#!/usr/bin/env python3
"""
Alpaca Paper Trading Dashboard — v2
Full-market scanner · 5-criteria signal detection · Backtest engine
"""

import json, math, os, queue, random, threading, time, logging
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import ta
from flask import Flask, Response, jsonify, request, render_template_string

# ── Configuration ──────────────────────────────────────────────────────────────

API_KEY    = os.environ.get("APCA_API_KEY_ID", "PKIDMLMR2MVR7465HJJKG6MKMM")
API_SECRET = os.environ.get("APCA_API_SECRET_KEY", "CXJHiFeYUH3jdKCuPRTh65mXKzCDhz6HxQ8MdSyxz4Lr")
BASE_URL   = os.environ.get("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
DATA_URL   = "https://data.alpaca.markets"

HEADERS = {
    "APCA-API-KEY-ID":     API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET,
    "Content-Type":        "application/json",
}

BATCH_SIZE       = 50
MIN_PRICE        = 5.0
MAX_PRICE        = 10_000.0
MIN_DAILY_VOL    = 500_000
RVOL_MIN         = 1.5
ATR_TARGET_MULT  = 3.0
MIN_RR           = 2.0
ACCOUNT_RISK_PCT = 0.01      # 1% account risk per trade
SCAN_INTERVAL    = 300       # seconds between live scan passes
LOOKBACK         = 80        # bars for live scan
BT_LOOKBACK      = 1000      # bars per symbol for backtest (~13 trading days)
TIMEFRAME        = "5Min"
SWING_ORDER      = 2         # bars each side for swing-point detection

# ── App & Global State ─────────────────────────────────────────────────────────

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

state = {
    "running":    False,
    "total":      0,
    "scanned":    0,
    "alerts":     [],
    "last_scan":  None,
    "auto_trade": False,
    "start_time": None,
    "status":     "idle",
}
state_lock = threading.Lock()

backtest_state = {
    "running":  False,
    "status":   "idle",
    "progress": 0,
    "total":    0,
    "results":  None,
    "trades":   [],
    "error":    None,
}
bt_lock = threading.Lock()

event_queues = []
eq_lock      = threading.Lock()


def broadcast(event_type: str, data: dict):
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with eq_lock:
        for q in list(event_queues):
            try:
                q.put_nowait(msg)
            except queue.Full:
                pass


# ── Alpaca API Helpers ─────────────────────────────────────────────────────────

def get_account():
    r = requests.get(f"{BASE_URL}/v2/account", headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_all_symbols():
    assets = []
    for exchange in ("NYSE", "NASDAQ"):
        r = requests.get(
            f"{BASE_URL}/v2/assets",
            headers=HEADERS,
            params={"status": "active", "asset_class": "us_equity", "exchange": exchange},
            timeout=30,
        )
        r.raise_for_status()
        assets.extend(r.json())
    symbols = [
        a["symbol"] for a in assets
        if a.get("tradable") and a.get("status") == "active"
        and "/" not in a["symbol"] and len(a["symbol"]) <= 5
    ]
    return sorted(set(symbols))


def fetch_bars_batch(symbols: list, limit: int = LOOKBACK) -> dict:
    params = {
        "symbols":   ",".join(symbols),
        "timeframe": TIMEFRAME,
        "limit":     limit,
        "feed":      "iex",
        "sort":      "asc",
    }
    try:
        r = requests.get(f"{DATA_URL}/v2/stocks/bars", headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        raw = r.json().get("bars", {})
    except Exception as e:
        log.warning(f"fetch_bars_batch error: {e}")
        return {}

    result = {}
    for sym, bars in raw.items():
        if not bars:
            continue
        df = pd.DataFrame(bars)
        df.rename(columns={"o": "open", "h": "high", "l": "low",
                            "c": "close", "v": "volume", "t": "time"}, inplace=True)
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].astype(float)
        result[sym] = df
    return result


def submit_bracket_order(symbol, qty, entry, stop, target):
    order = {
        "symbol":        symbol,
        "qty":           str(int(qty)),
        "side":          "buy",
        "type":          "limit",
        "time_in_force": "day",
        "limit_price":   str(round(entry, 2)),
        "order_class":   "bracket",
        "take_profit":   {"limit_price": str(round(target, 2))},
        "stop_loss":     {"stop_price": str(round(stop, 2))},
    }
    r = requests.post(f"{BASE_URL}/v2/orders", headers=HEADERS, json=order, timeout=10)
    r.raise_for_status()
    return r.json()


def get_positions():
    r = requests.get(f"{BASE_URL}/v2/positions", headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def get_orders():
    r = requests.get(
        f"{BASE_URL}/v2/orders",
        headers=HEADERS,
        params={"status": "all", "limit": 50},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def close_position(symbol):
    r = requests.delete(f"{BASE_URL}/v2/positions/{symbol}", headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def cancel_order(order_id):
    r = requests.delete(f"{BASE_URL}/v2/orders/{order_id}", headers=HEADERS, timeout=10)
    r.raise_for_status()


# ── Signal Utilities ───────────────────────────────────────────────────────────

def find_local_maxima(arr: np.ndarray, order: int = SWING_ORDER) -> list:
    """Return bar indices that are local maxima (swing highs)."""
    n, out = len(arr), []
    for i in range(order, n - order):
        if all(arr[i] > arr[i - j] for j in range(1, order + 1)) and \
           all(arr[i] > arr[i + j] for j in range(1, order + 1)):
            out.append(i)
    return out


def find_local_minima(arr: np.ndarray, order: int = SWING_ORDER) -> list:
    """Return bar indices that are local minima (swing lows)."""
    n, out = len(arr), []
    for i in range(order, n - order):
        if all(arr[i] < arr[i - j] for j in range(1, order + 1)) and \
           all(arr[i] < arr[i + j] for j in range(1, order + 1)):
            out.append(i)
    return out


# ── Signal Detection — ALL 5 criteria are hard filters ────────────────────────

def check_criteria(symbol: str, df: pd.DataFrame, equity: float):
    """
    Apply all 5 criteria to a bar DataFrame.
    Returns alert dict if ALL pass, else None.
    """
    if len(df) < 60:
        return None

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]
    open_  = df["open"]

    # ── Indicators ────────────────────────────────────────────────────────────
    ema20_s = ta.trend.EMAIndicator(close, window=20).ema_indicator()
    ema50_s = ta.trend.EMAIndicator(close, window=50).ema_indicator()
    atr_s   = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()

    ema20      = ema20_s.iloc[-1]
    ema50      = ema50_s.iloc[-1]
    atr        = atr_s.iloc[-1]
    last_close = close.iloc[-1]

    if any(pd.isna(x) for x in [ema20, ema50, atr]) or atr <= 0:
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 1 — Trend / Market Structure
    #   • Price > EMA20 > EMA50
    #   • Higher highs AND higher lows via swing-point detection
    #   • Most recent pullback holds above prior swing low (close-based)
    #   • No collapse back after breakout above prior swing high
    #   • If no clear HH/HL structure → skip (choppy)
    # ─────────────────────────────────────────────────────────────────────────
    if not (last_close > ema20 > ema50):
        return None

    h_arr = high.values
    l_arr = low.values
    c_arr = close.values

    sh_idxs = find_local_maxima(h_arr)
    sl_idxs = find_local_minima(l_arr)

    # Need at least 2 of each to confirm structure; fewer = choppy → skip
    if len(sh_idxs) < 2 or len(sl_idxs) < 2:
        return None

    sh_vals = [h_arr[i] for i in sh_idxs]
    sl_vals = [l_arr[i] for i in sl_idxs]

    # Higher highs: most recent swing high > prior swing high
    if sh_vals[-1] <= sh_vals[-2]:
        return None

    # Higher lows: most recent swing low > prior swing low
    if sl_vals[-1] <= sl_vals[-2]:
        return None

    # Pullback holds: no close below most recent swing low in last 5 bars
    if close.iloc[-5:].min() < sl_vals[-1]:
        return None

    # Follow-through: if we're above prior swing high, price shouldn't close back below it
    prior_sh = sh_vals[-2]
    if last_close > prior_sh:
        if close.iloc[-4:-1].min() < prior_sh:
            return None

    trend_note = (
        f"Price ${last_close:.2f} > EMA20 ${ema20:.2f} > EMA50 ${ema50:.2f} | "
        f"HH ${sh_vals[-2]:.2f}\u2192${sh_vals[-1]:.2f} | "
        f"HL ${sl_vals[-2]:.2f}\u2192${sl_vals[-1]:.2f}"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 2 — Entry Trigger (ONE must pass)
    #   A) Breakout + reclaim: close > prior 10-bar high with bullish candle
    #   B) EMA20 pullback: recent bar touched EMA20, current bar bullish close above
    #   C) Pivot reclaim: dipped below prior swing high, now closes back above it
    # ─────────────────────────────────────────────────────────────────────────
    last_bar = df.iloc[-1]
    prev_bar = df.iloc[-2]
    entry_type = None
    entry_note = None

    # A — Breakout
    prior_10_high = high.iloc[-11:-1].max()
    is_breakout  = last_close > prior_10_high * 1.001
    is_bullish   = (last_close > last_bar["open"]) or (last_close > prev_bar["close"] * 1.002)
    if is_breakout and is_bullish:
        entry_type = "Breakout"
        entry_note = (
            f"Close ${last_close:.2f} broke 10-bar high ${prior_10_high:.2f} "
            f"({'bullish candle' if last_close > last_bar['open'] else 'bullish close'})"
        )

    # B — EMA20 pullback
    if entry_type is None:
        recent_low = low.iloc[-5:-1].min()
        near_ema   = abs(recent_low - ema20) / ema20 < 0.015
        bull_react = (last_close > last_bar["open"]) and (last_close > ema20)
        if near_ema and bull_react:
            entry_type = "EMA20 Pullback"
            entry_note = (
                f"Pulled to EMA20 ${ema20:.2f} (low ${recent_low:.2f}), "
                f"bullish close ${last_close:.2f} above EMA"
            )

    # C — Pivot reclaim
    if entry_type is None and len(sh_idxs) >= 2:
        pivot    = sh_vals[-2]
        dipped   = low.iloc[-6:-1].min() < pivot
        reclaimed = last_close > pivot and last_close > last_bar["open"]
        if dipped and reclaimed:
            entry_type = "Pivot Reclaim"
            entry_note = (
                f"Dipped below pivot ${pivot:.2f}, "
                f"closed back above \u2192 ${last_close:.2f}"
            )

    if entry_type is None:
        return None

    entry_price = last_close

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 3 — Support & R / Invalidation
    #   • Find nearest valid support below entry (swing low or flipped resistance)
    #   • No 5m CLOSE below that support in recent 5 bars (already invalidated check)
    # ─────────────────────────────────────────────────────────────────────────
    support_level = None

    # Nearest swing low below entry within 5%
    sl_candidates = [v for v in sl_vals if v < entry_price and (entry_price - v) / entry_price < 0.05]
    if sl_candidates:
        support_level = max(sl_candidates)

    # Fallback: prior swing high acting as support (flipped resistance), within 4%
    if support_level is None:
        for v in reversed(sh_vals[:-1]):
            if v < entry_price and (entry_price - v) / entry_price < 0.04:
                support_level = v
                break

    if support_level is None:
        return None

    # Invalidation check: no close below support in last 5 bars
    if close.iloc[-5:].min() < support_level:
        return None

    pct_below = (entry_price - support_level) / entry_price * 100
    sr_note = (
        f"Support ${support_level:.2f} ({pct_below:.1f}% below entry) | "
        f"No close violation in last 5 bars"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 4 — Risk Management
    #   • Stop just below support (invalidation level)
    #   • Target = 3\u00d7 ATR above entry
    #   • R:R must be \u2265 2.0 — if not, skip
    #   • Position sized to 1% account risk
    # ─────────────────────────────────────────────────────────────────────────
    stop   = support_level * 0.998
    target = entry_price + ATR_TARGET_MULT * atr
    risk   = entry_price - stop
    reward = target - entry_price

    if risk <= 0:
        return None

    rr = reward / risk
    if rr < MIN_RR:
        return None

    dollar_risk  = equity * ACCOUNT_RISK_PCT
    qty          = max(1, math.floor(dollar_risk / risk))
    position_val = qty * entry_price

    risk_note = (
        f"ATR ${atr:.2f} | Stop ${stop:.2f} (\u2212{(entry_price-stop)/entry_price*100:.2f}%) | "
        f"Target ${target:.2f} | R:R {rr:.1f}:1 | "
        f"{qty}\u00d7${entry_price:.2f} = ${position_val:,.0f} notional"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 5 — Volume / Confirmation
    #   • RVOL \u2265 1.5\u00d7 20-bar average
    #   • Current bar volume > prior 5-bar average (expanding volume)
    #   • Estimated daily volume \u2265 500k
    # ─────────────────────────────────────────────────────────────────────────
    avg_vol_20 = volume.iloc[-21:-1].mean()
    avg_vol_5  = volume.iloc[-6:-1].mean()
    cur_vol    = volume.iloc[-1]
    rvol       = cur_vol / avg_vol_20 if avg_vol_20 > 0 else 0
    est_daily  = avg_vol_20 * 78        # 78 five-min bars per 6.5-hr session

    if rvol < RVOL_MIN:
        return None
    if cur_vol < avg_vol_5:
        return None
    if est_daily < MIN_DAILY_VOL:
        return None

    expanding     = cur_vol > avg_vol_5 * 1.2
    expand_label  = "Expanding \u2191" if expanding else "Rising"
    vol_note  = (
        f"RVOL {rvol:.1f}\u00d7 | Bar {cur_vol:,.0f} vs 5-bar avg {avg_vol_5:,.0f} | "
        f"Est daily {est_daily:,.0f} | {expand_label}"
    )

    # ── Price filter ──────────────────────────────────────────────────────────
    if not (MIN_PRICE <= entry_price <= MAX_PRICE):
        return None

    return {
        "symbol":     symbol,
        "entry":      round(entry_price, 2),
        "stop":       round(stop, 2),
        "target":     round(target, 2),
        "rr":         round(rr, 2),
        "qty":        qty,
        "atr":        round(atr, 2),
        "rvol":       round(rvol, 2),
        "entry_type": entry_type,
        "criteria": {
            "trend":  trend_note,
            "entry":  entry_note,
            "sr":     sr_note,
            "risk":   risk_note,
            "volume": vol_note,
        },
        "timestamp": datetime.now().isoformat(),
    }


# ── Scanner Thread ─────────────────────────────────────────────────────────────

def scanner_loop():
    while True:
        with state_lock:
            running = state["running"]
        if not running:
            time.sleep(1)
            continue
        try:
            run_scan()
        except Exception as e:
            log.error(f"Scanner crashed: {e}", exc_info=True)
            broadcast("error", {"message": str(e)})
        with state_lock:
            state["status"] = "waiting"
            still_running   = state["running"]
        if not still_running:
            continue
        broadcast("status", {"status": "waiting", "next_scan_in": SCAN_INTERVAL})
        for _ in range(SCAN_INTERVAL):
            time.sleep(1)
            with state_lock:
                if not state["running"]:
                    break


def run_scan():
    log.info("Starting market scan ...")
    broadcast("status", {"status": "scanning", "message": "Fetching symbol list..."})
    try:
        account = get_account()
        equity  = float(account.get("equity", 100_000))
    except Exception:
        equity = 100_000
        log.warning("Could not fetch account equity; defaulting to $100,000")
    try:
        symbols = get_all_symbols()
    except Exception as e:
        log.error(f"Failed to fetch symbols: {e}")
        broadcast("error", {"message": f"Failed to fetch symbol list: {e}"})
        return

    total = len(symbols)
    log.info(f"Scanning {total} symbols in batches of {BATCH_SIZE}")

    with state_lock:
        state["total"]      = total
        state["scanned"]    = 0
        state["start_time"] = time.time()
        state["status"]     = "scanning"

    broadcast("progress", {"scanned": 0, "total": total, "eta": 0, "alerts": 0})

    new_alerts = []
    batches    = [symbols[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        with state_lock:
            if not state["running"]:
                break

        bars_map = fetch_bars_batch(batch)

        for sym, df in bars_map.items():
            try:
                alert = check_criteria(sym, df, equity)
            except Exception as e:
                log.debug(f"Error analyzing {sym}: {e}")
                alert = None

            if alert:
                new_alerts.append(alert)
                broadcast("alert", alert)
                log.info(f"ALERT  {sym}  {alert['entry_type']}  R:R {alert['rr']}:1")

                with state_lock:
                    auto = state["auto_trade"]
                if auto:
                    try:
                        submit_bracket_order(sym, alert["qty"],
                                             alert["entry"], alert["stop"], alert["target"])
                        broadcast("trade", {"symbol": sym, "status": "submitted",
                                            "qty": alert["qty"], "entry": alert["entry"]})
                    except Exception as te:
                        log.warning(f"Auto-trade failed {sym}: {te}")
                        broadcast("trade", {"symbol": sym, "status": "failed", "error": str(te)})

        scanned = min((batch_idx + 1) * BATCH_SIZE, total)
        with state_lock:
            state["scanned"] = scanned
            elapsed = time.time() - state["start_time"]

        rate = scanned / elapsed if elapsed > 0 else 1
        eta  = int((total - scanned) / rate) if rate > 0 else 0
        broadcast("progress", {"scanned": scanned, "total": total, "eta": eta, "alerts": len(new_alerts)})
        time.sleep(0.04)

    with state_lock:
        existing_syms = {a["symbol"] for a in new_alerts}
        merged = new_alerts + [a for a in state["alerts"] if a["symbol"] not in existing_syms]
        state["alerts"]    = merged[:200]
        state["scanned"]   = total
        state["last_scan"] = datetime.now().isoformat()

    broadcast("scan_complete", {
        "total": total, "alerts": len(new_alerts), "timestamp": state["last_scan"]
    })
    log.info(f"Scan complete -- {len(new_alerts)} alerts from {total} symbols")


# ── Backtest Engine ────────────────────────────────────────────────────────────

def backtest_symbol(sym: str, df: pd.DataFrame, equity: float = 100_000) -> list:
    """
    Walk-forward backtest for a single symbol.
    Enters at next-bar open after signal fires; simulates up to 30 bars forward.
    """
    trades  = []
    warmup  = 65    # bars before we start checking
    max_hold = 30   # max bars in a simulated trade

    i = warmup
    n = len(df)
    while i < n - 1:
        try:
            alert = check_criteria(sym, df.iloc[:i], equity)
        except Exception:
            alert = None

        if not alert:
            i += 1
            continue

        # Enter at next bar's open
        entry_price = df.iloc[i]["open"]
        stop        = alert["stop"]
        risk        = entry_price - stop
        if risk <= 0:
            i += 1
            continue

        # Shift target to match actual entry (preserves ATR-based distance)
        target = entry_price + ATR_TARGET_MULT * alert["atr"]
        rr     = (target - entry_price) / risk

        # Simulate forward
        outcome    = "timeout"
        exit_price = df.iloc[min(i + max_hold, n - 1)]["close"]
        exit_bar   = min(i + max_hold, n - 1)

        for j in range(i + 1, min(i + max_hold + 1, n)):
            lo = df.iloc[j]["low"]
            hi = df.iloc[j]["high"]
            if lo <= stop:
                outcome    = "loss"
                exit_price = stop
                exit_bar   = j
                break
            if hi >= target:
                outcome    = "win"
                exit_price = target
                exit_bar   = j
                break

        pnl_r = (exit_price - entry_price) / risk

        trades.append({
            "symbol":     sym,
            "entry_type": alert["entry_type"],
            "entry":      round(entry_price, 2),
            "exit":       round(exit_price, 2),
            "stop":       round(stop, 2),
            "target":     round(target, 2),
            "rr":         round(rr, 2),
            "outcome":    outcome,
            "pnl_r":      round(pnl_r, 2),
        })

        i = exit_bar + 1  # skip past completed trade

    return trades


def run_backtest_job(n_symbols: int = 100):
    """Background thread: run full backtest."""
    try:
        with bt_lock:
            backtest_state.update({
                "running":  True,
                "status":   "fetching",
                "progress": 0,
                "results":  None,
                "trades":   [],
                "error":    None,
            })

        broadcast("backtest_status", {"status": "fetching", "message": "Fetching symbol list..."})

        try:
            all_syms = get_all_symbols()
        except Exception as e:
            raise RuntimeError(f"Could not fetch symbols: {e}")

        n      = min(n_symbols, len(all_syms))
        sample = random.sample(all_syms, n)

        with bt_lock:
            backtest_state["total"] = n

        broadcast("backtest_status", {
            "status": "fetching",
            "message": f"Downloading {n} symbols \u00d7 ~{BT_LOOKBACK} bars each..."
        })

        all_data = {}
        batches  = [sample[i:i + BATCH_SIZE] for i in range(0, n, BATCH_SIZE)]
        nb       = len(batches)

        for b_idx, batch in enumerate(batches):
            try:
                bars = fetch_bars_batch(batch, limit=BT_LOOKBACK)
                all_data.update(bars)
            except Exception as e:
                log.warning(f"Backtest fetch error batch {b_idx}: {e}")

            pct = (b_idx + 1) / nb * 40
            with bt_lock:
                backtest_state["progress"] = pct
            broadcast("backtest_progress", {
                "progress": pct, "phase": "fetching",
                "symbols_done": min((b_idx + 1) * BATCH_SIZE, n), "total": n
            })
            time.sleep(0.1)

        broadcast("backtest_status", {
            "status": "simulating",
            "message": f"Simulating strategy on {len(all_data)} symbols..."
        })

        try:
            equity = float(get_account().get("equity", 100_000))
        except Exception:
            equity = 100_000

        all_trades   = []
        syms_done    = 0
        total_syms   = len(all_data)

        for sym, df in all_data.items():
            try:
                trades = backtest_symbol(sym, df, equity)
                all_trades.extend(trades)
            except Exception as e:
                log.debug(f"Backtest sim error {sym}: {e}")

            syms_done += 1
            pct = 40 + (syms_done / total_syms) * 60
            with bt_lock:
                backtest_state["progress"] = pct

            if syms_done % 5 == 0 or syms_done == total_syms:
                broadcast("backtest_progress", {
                    "progress": pct, "phase": "simulating",
                    "symbols_done": syms_done, "total": total_syms,
                    "trades_found": len(all_trades),
                })

        # ── Aggregate stats ────────────────────────────────────────────────
        wins     = [t for t in all_trades if t["outcome"] == "win"]
        losses   = [t for t in all_trades if t["outcome"] == "loss"]
        timeouts = [t for t in all_trades if t["outcome"] == "timeout"]
        total_t  = len(all_trades)

        win_rate = len(wins) / total_t * 100 if total_t > 0 else 0
        avg_rr   = float(np.mean([t["rr"]    for t in all_trades])) if all_trades else 0
        avg_pnl  = float(np.mean([t["pnl_r"] for t in all_trades])) if all_trades else 0

        gross_win  = sum(t["pnl_r"] for t in wins)   if wins   else 0
        gross_loss = abs(sum(t["pnl_r"] for t in losses)) if losses else 0
        pf = gross_win / gross_loss if gross_loss > 0 else (9.99 if gross_win > 0 else 0.0)

        results = {
            "total_signals":  total_t,
            "wins":           len(wins),
            "losses":         len(losses),
            "timeouts":       len(timeouts),
            "win_rate":       round(win_rate, 1),
            "avg_rr":         round(avg_rr, 2),
            "avg_pnl_r":      round(avg_pnl, 2),
            "profit_factor":  round(float(pf), 2),
            "symbols_tested": total_syms,
            "completed_at":   datetime.now().isoformat(),
        }

        with bt_lock:
            backtest_state.update({
                "running":  False,
                "status":   "complete",
                "progress": 100,
                "results":  results,
                "trades":   all_trades[:500],
            })

        broadcast("backtest_complete", results)
        log.info(f"Backtest done: {total_t} trades, {win_rate:.1f}% win rate, PF {pf:.2f}")

    except Exception as e:
        log.error(f"Backtest job failed: {e}", exc_info=True)
        with bt_lock:
            backtest_state.update({"running": False, "status": "error", "error": str(e)})
        broadcast("backtest_error", {"message": str(e)})


# ── Flask Routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/stream")
def stream():
    q = queue.Queue(maxsize=500)
    with eq_lock:
        event_queues.append(q)

    def generate():
        with state_lock:
            snap = {k: v for k, v in state.items() if k != "alerts"}
        yield f"event: init\ndata: {json.dumps(snap)}\n\n"
        try:
            while True:
                try:
                    yield q.get(timeout=25)
                except queue.Empty:
                    yield ": heartbeat\n\n"
        except GeneratorExit:
            pass
        finally:
            with eq_lock:
                try:
                    event_queues.remove(q)
                except ValueError:
                    pass

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/start", methods=["POST"])
def api_start():
    auto = (request.json or {}).get("auto_trade", False)
    with state_lock:
        state.update({"running": True, "alerts": [], "status": "scanning", "auto_trade": auto})
    broadcast("status", {"status": "scanning", "auto_trade": auto})
    return jsonify({"ok": True})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    with state_lock:
        state.update({"running": False, "status": "stopped"})
    broadcast("status", {"status": "stopped"})
    return jsonify({"ok": True})


@app.route("/api/alerts")
def api_alerts():
    with state_lock:
        return jsonify(state["alerts"])


@app.route("/api/account")
def api_account():
    try:
        return jsonify(get_account())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/positions")
def api_positions():
    try:
        return jsonify(get_positions())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/orders")
def api_orders():
    try:
        return jsonify(get_orders())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/close/<symbol>", methods=["DELETE"])
def api_close_position(symbol):
    try:
        return jsonify(close_position(symbol))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cancel/<order_id>", methods=["DELETE"])
def api_cancel_order(order_id):
    try:
        cancel_order(order_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/auto_trade", methods=["POST"])
def api_auto_trade():
    enabled = (request.json or {}).get("enabled", False)
    with state_lock:
        state["auto_trade"] = enabled
    return jsonify({"auto_trade": enabled})


@app.route("/api/backtest/start", methods=["POST"])
def api_backtest_start():
    with bt_lock:
        if backtest_state["running"]:
            return jsonify({"error": "Backtest already running"}), 400
    n = int((request.json or {}).get("n_symbols", 100))
    n = max(10, min(500, n))
    threading.Thread(target=run_backtest_job, args=(n,), daemon=True, name="backtest").start()
    return jsonify({"ok": True, "n_symbols": n})


@app.route("/api/backtest/status")
def api_backtest_status():
    with bt_lock:
        return jsonify({k: v for k, v in backtest_state.items() if k != "trades"})


@app.route("/api/backtest/trades")
def api_backtest_trades():
    with bt_lock:
        return jsonify(backtest_state["trades"])


# ── HTML Template ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Market Scanner &middot; Alpaca</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:ital,wght@0,400;0,500;0,600;1,400&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg0:  #06060e;
      --bg1:  #0b0b18;
      --bg2:  #10101e;
      --bg3:  #181828;
      --bg4:  #202035;
      --brd:  #252540;
      --brd2: #343460;
      --txt:  #b8b8d8;
      --dim:  #50507a;
      --dim2: #7070a0;
      --hi:   #eeeeff;
      --grn:  #00e87c;
      --grn2: #00b860;
      --grn3: rgba(0,232,124,.08);
      --red:  #ff3d5a;
      --red2: rgba(255,61,90,.1);
      --yel:  #ffd600;
      --yel2: rgba(255,214,0,.1);
      --blu:  #3d9eff;
      --pur:  #9d5cff;
      --pur2: rgba(157,92,255,.1);
      --cyn:  #00d4ff;
      --cyn2: rgba(0,212,255,.1);
      --mono: 'Space Mono', monospace;
      --sans: 'DM Sans', sans-serif;
      --r2:   3px;
      --r3:   4px;
    }

    body { background: var(--bg0); color: var(--txt); font-family: var(--sans); font-size: 13px; min-height: 100vh; }

    /* ── HEADER ─────────────────────────────────────────────────────────── */
    header {
      position: sticky; top: 0; z-index: 100;
      background: var(--bg1); border-bottom: 1px solid var(--brd);
      padding: 0 20px; height: 52px;
      display: flex; align-items: center; gap: 20px;
    }
    .logo { font-family: var(--mono); font-size: 12px; font-weight: 700; color: var(--grn); letter-spacing: 2.5px; white-space: nowrap; flex-shrink: 0; }
    .logo em { color: var(--dim); font-style: normal; }

    .acct { display: flex; gap: 24px; align-items: center; flex: 1; overflow: hidden; }
    .kv   { display: flex; flex-direction: column; }
    .kv .k { font-size: 9px; color: var(--dim); text-transform: uppercase; letter-spacing: 1px; line-height: 1; margin-bottom: 3px; }
    .kv .v { font-family: var(--mono); font-size: 12px; color: var(--hi); line-height: 1; }
    .kv .v.g { color: var(--grn); }
    .kv .v.r { color: var(--red); }

    .hacts { display: flex; gap: 10px; align-items: center; flex-shrink: 0; }

    .btn {
      font-family: var(--mono); font-size: 10px; letter-spacing: 1px;
      padding: 7px 16px; border: 1px solid var(--brd2);
      background: transparent; color: var(--txt); cursor: pointer;
      border-radius: var(--r2); transition: all .15s; white-space: nowrap;
    }
    .btn:hover   { background: var(--bg4); color: var(--hi); border-color: var(--dim2); }
    .btn.primary { background: var(--grn2); border-color: var(--grn); color: #000; font-weight: 700; }
    .btn.primary:hover { background: var(--grn); box-shadow: 0 0 12px rgba(0,232,124,.3); }
    .btn.danger  { border-color: var(--red); color: var(--red); }
    .btn.danger:hover { background: var(--red2); }
    .btn:disabled { opacity: .3; cursor: not-allowed; }
    .btn.sm { padding: 4px 10px; font-size: 9px; }

    .tog   { display: flex; align-items: center; gap: 8px; cursor: pointer; user-select: none; }
    .tog-t { width: 34px; height: 17px; background: var(--bg4); border: 1px solid var(--brd2); border-radius: 9px; position: relative; transition: background .2s, border-color .2s; }
    .tog-t.on { background: var(--grn2); border-color: var(--grn); }
    .tog-k { width: 11px; height: 11px; border-radius: 50%; background: var(--dim); position: absolute; top: 2px; left: 2px; transition: left .2s, background .2s; }
    .tog-t.on .tog-k { left: 19px; background: #000; }
    .tog-l { font-size: 10px; color: var(--dim); letter-spacing: 1px; text-transform: uppercase; font-family: var(--mono); }

    /* ── PROGRESS BAR ───────────────────────────────────────────────────── */
    .prog-wrap { background: var(--bg1); border-bottom: 1px solid var(--brd); padding: 10px 20px; }
    .prog-meta { display: flex; justify-content: space-between; align-items: center; margin-bottom: 7px; font-family: var(--mono); font-size: 10px; color: var(--dim); }
    .prog-meta .hl  { color: var(--cyn); }
    .prog-meta .alc { color: var(--grn); }
    .prog-trk  { height: 2px; background: var(--bg4); border-radius: 2px; overflow: hidden; }
    .prog-fill { height: 100%; background: linear-gradient(90deg, var(--blu) 0%, var(--cyn) 100%); border-radius: 2px; width: 0%; transition: width .4s ease; }
    .prog-fill.done { background: linear-gradient(90deg, var(--grn2), var(--grn)); }

    .sdot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: var(--dim); margin-right: 7px; vertical-align: middle; transition: background .3s; }
    .sdot.scan { background: var(--cyn); animation: pulse 1.2s ease-in-out infinite; }
    .sdot.done { background: var(--grn); }
    .sdot.stop { background: var(--red); }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)}50%{opacity:.3;transform:scale(.7)} }

    /* ── TABS ───────────────────────────────────────────────────────────── */
    .tabs { display: flex; border-bottom: 1px solid var(--brd); padding: 0 20px; background: var(--bg1); }
    .tab  { font-family: var(--mono); font-size: 10px; letter-spacing: 1.5px; text-transform: uppercase; padding: 12px 18px; cursor: pointer; color: var(--dim); border-bottom: 2px solid transparent; transition: all .15s; }
    .tab:hover { color: var(--dim2); }
    .tab.active { color: var(--cyn); border-bottom-color: var(--cyn); }
    .badge { background: var(--grn); color: #000; font-size: 9px; font-weight: 700; padding: 1px 5px; border-radius: 8px; margin-left: 6px; vertical-align: middle; }

    /* ── CONTENT ────────────────────────────────────────────────────────── */
    .content { padding: 16px 20px; }
    .panel   { display: none; }
    .panel.active { display: block; }

    /* ── ALERT CARDS ────────────────────────────────────────────────────── */
    .alerts-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 12px; }
    .alert-card  {
      background: var(--bg2); border: 1px solid var(--brd); padding: 15px;
      border-radius: var(--r3); border-left: 3px solid var(--grn);
      animation: cardIn .35s cubic-bezier(.22,.68,0,1.2);
    }
    .alert-card.breakout { border-left-color: var(--cyn); }
    .alert-card.pullback { border-left-color: var(--pur); }
    .alert-card.pivot    { border-left-color: var(--yel); }
    @keyframes cardIn { from{opacity:0;transform:translateY(-8px) scale(.98)}to{opacity:1;transform:none} }

    .ah    { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; }
    .a-sym { font-family: var(--mono); font-size: 22px; font-weight: 700; color: var(--hi); letter-spacing: 1px; }
    .a-ts  { font-family: var(--mono); font-size: 9px; color: var(--dim); margin-top: 3px; }
    .a-typ { font-family: var(--mono); font-size: 9px; letter-spacing: 1.5px; text-transform: uppercase; padding: 4px 9px; border-radius: 2px; }
    .a-typ.breakout { background: var(--cyn2); color: var(--cyn); border: 1px solid rgba(0,212,255,.2); }
    .a-typ.pullback { background: var(--pur2); color: var(--pur); border: 1px solid rgba(157,92,255,.2); }
    .a-typ.pivot    { background: var(--yel2); color: var(--yel); border: 1px solid rgba(255,214,0,.2); }

    .a-px  { display: grid; grid-template-columns: repeat(3,1fr); gap: 8px; margin-bottom: 11px; }
    .pc    { background: var(--bg3); padding: 8px 10px; border-radius: var(--r2); border: 1px solid var(--brd); }
    .pc .l { font-size: 9px; color: var(--dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 3px; }
    .pc .v { font-family: var(--mono); font-size: 14px; }
    .pc .v.e { color: var(--cyn); }
    .pc .v.s { color: var(--red); }
    .pc .v.t { color: var(--grn); }

    .a-st { display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 12px; font-family: var(--mono); font-size: 11px; color: var(--dim); }
    .a-st .sv   { color: var(--yel); }
    .a-st .sv.g { color: var(--grn); font-weight: 700; }

    /* ── CRITERIA NUMBERED BADGES ───────────────────────────────────────── */
    .crit-list { border-top: 1px solid var(--brd); padding-top: 10px; display: flex; flex-direction: column; gap: 6px; }
    .crit      { display: flex; align-items: flex-start; gap: 8px; line-height: 1.5; }
    .cn {
      display: inline-flex; align-items: center; justify-content: center;
      width: 17px; height: 17px; border-radius: 50%;
      background: var(--grn); color: #000;
      font-family: var(--mono); font-size: 9px; font-weight: 700;
      flex-shrink: 0; margin-top: 1px;
    }
    .ck { font-family: var(--mono); font-size: 10px; color: var(--hi); flex-shrink: 0; min-width: 54px; }
    .cd { font-size: 10px; color: var(--dim); line-height: 1.5; }

    /* ── TABLES ─────────────────────────────────────────────────────────── */
    .dtbl    { width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 11px; }
    .dtbl th { text-align: left; padding: 8px 12px; background: var(--bg2); color: var(--dim); font-size: 9px; letter-spacing: 1.5px; text-transform: uppercase; border-bottom: 1px solid var(--brd); }
    .dtbl td { padding: 9px 12px; border-bottom: 1px solid var(--brd); color: var(--txt); }
    .dtbl tr:hover td { background: var(--bg2); }
    .dtbl .pos { color: var(--grn); }
    .dtbl .neg { color: var(--red); }
    .dtbl .sym { color: var(--hi); font-weight: 700; }
    .dtbl .win { color: var(--grn); }
    .dtbl .loss { color: var(--red); }
    .dtbl .timeout { color: var(--dim2); }

    .empty      { text-align: center; padding: 60px 20px; color: var(--dim); font-family: var(--mono); font-size: 11px; line-height: 2; }
    .empty .ico { font-size: 28px; margin-bottom: 12px; opacity: .4; }

    /* ── BACKTEST TAB ───────────────────────────────────────────────────── */
    .bt-ctrl { display: flex; align-items: flex-end; gap: 14px; margin-bottom: 20px; flex-wrap: wrap; padding-bottom: 20px; border-bottom: 1px solid var(--brd); }
    .bt-ctrl .kv { gap: 6px; }
    .bt-inp  { font-family: var(--mono); font-size: 12px; background: var(--bg3); border: 1px solid var(--brd2); color: var(--hi); padding: 7px 12px; width: 100px; border-radius: var(--r2); outline: none; }
    .bt-inp:focus { border-color: var(--cyn); }
    .bt-hint { font-size: 11px; color: var(--dim); font-style: italic; }

    .bt-prog-wrap { background: var(--bg2); border: 1px solid var(--brd); padding: 18px 20px; border-radius: var(--r3); margin-bottom: 20px; }
    .bt-prog-msg  { font-family: var(--mono); font-size: 11px; color: var(--cyn); margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
    .bt-prog-sub  { font-family: var(--mono); font-size: 10px; color: var(--dim); margin-top: 9px; }

    .bt-stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 10px; margin-bottom: 22px; }
    .bt-stat  { background: var(--bg2); border: 1px solid var(--brd); padding: 14px 16px; border-radius: var(--r3); }
    .bt-stat .k { font-size: 9px; color: var(--dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
    .bt-stat .v { font-family: var(--mono); font-size: 24px; font-weight: 700; color: var(--hi); line-height: 1; }
    .bt-stat .v.g { color: var(--grn); }
    .bt-stat .v.r { color: var(--red); }
    .bt-stat .v.y { color: var(--yel); }

    .bt-sec-title { font-family: var(--mono); font-size: 10px; color: var(--dim); text-transform: uppercase; letter-spacing: 2px; margin-bottom: 12px; }

    /* ── TOASTS ─────────────────────────────────────────────────────────── */
    #toasts { position: fixed; bottom: 20px; right: 20px; display: flex; flex-direction: column-reverse; gap: 8px; z-index: 9999; pointer-events: none; }
    .toast  { background: var(--bg3); border: 1px solid var(--brd2); border-left: 3px solid var(--grn); padding: 10px 16px; font-family: var(--mono); font-size: 11px; color: var(--txt); max-width: 340px; border-radius: var(--r2); animation: toastIn .2s ease; }
    .toast.err   { border-left-color: var(--red); }
    .toast.trade { border-left-color: var(--yel); }
    .toast.info  { border-left-color: var(--blu); }
    @keyframes toastIn { from{opacity:0;transform:translateX(16px)}to{opacity:1;transform:none} }

    #ph { text-align: center; padding: 70px 20px; color: var(--dim); font-family: var(--mono); font-size: 11px; line-height: 2; }
    #ph .ico { font-size: 32px; margin-bottom: 14px; opacity: .3; }

    ::-webkit-scrollbar { width: 4px; height: 4px; }
    ::-webkit-scrollbar-track { background: var(--bg0); }
    ::-webkit-scrollbar-thumb { background: var(--bg4); border-radius: 2px; }
  </style>
</head>
<body>

<!-- ── HEADER ──────────────────────────────────────────────────────────────── -->
<header>
  <div class="logo">&#9654; MARKET<em>&middot;</em>SCANNER</div>

  <div class="acct">
    <div class="kv"><span class="k">Equity</span>       <span class="v g" id="acc-eq">&#8212;</span></div>
    <div class="kv"><span class="k">Buying Power</span> <span class="v"   id="acc-bp">&#8212;</span></div>
    <div class="kv"><span class="k">Day P&amp;L</span>  <span class="v"   id="acc-pl">&#8212;</span></div>
    <div class="kv"><span class="k">Last Scan</span>    <span class="v"   id="acc-ls">&#8212;</span></div>
  </div>

  <div class="hacts">
    <label class="tog" id="auto-lbl" title="Auto-submit bracket orders on signal">
      <div class="tog-t" id="auto-trk"><div class="tog-k"></div></div>
      <span class="tog-l">Auto</span>
    </label>
    <button class="btn primary" id="btn-start" onclick="startScan()">&#9654; START SCAN</button>
    <button class="btn danger"  id="btn-stop"  onclick="stopScan()" disabled>&#9632; STOP</button>
  </div>
</header>

<!-- ── PROGRESS ────────────────────────────────────────────────────────────── -->
<div class="prog-wrap">
  <div class="prog-meta">
    <span>
      <span class="sdot" id="sdot"></span>
      <span id="stxt" style="font-family:var(--mono);font-size:10px;color:var(--dim)">IDLE</span>
    </span>
    <span>
      <span id="pnum" class="hl">0 / 0</span> symbols
      &nbsp;&middot;&nbsp; ETA <span id="peta" class="hl">&#8212;</span>
      &nbsp;&middot;&nbsp; <span class="alc"><span id="palerts">0</span> alerts</span>
    </span>
  </div>
  <div class="prog-trk"><div class="prog-fill" id="pfill"></div></div>
</div>

<!-- ── TABS ────────────────────────────────────────────────────────────────── -->
<div class="tabs">
  <div class="tab active" data-tab="alerts"    onclick="switchTab('alerts')">Alerts <span class="badge" id="bdg">0</span></div>
  <div class="tab"        data-tab="positions" onclick="switchTab('positions')">Positions</div>
  <div class="tab"        data-tab="orders"    onclick="switchTab('orders')">Orders</div>
  <div class="tab"        data-tab="backtest"  onclick="switchTab('backtest')">Backtest</div>
</div>

<!-- ── CONTENT ─────────────────────────────────────────────────────────────── -->
<div class="content">

  <!-- ALERTS -->
  <div class="panel active" id="panel-alerts">
    <div id="ph">
      <div class="ico">&#9672;</div>
      Start the scanner to detect high-probability setups.<br>
      Scanning NYSE &amp; NASDAQ using 5 hard-filter criteria.
    </div>
    <div class="alerts-grid" id="agrid"></div>
  </div>

  <!-- POSITIONS -->
  <div class="panel" id="panel-positions">
    <div id="pcont"><div class="empty"><div class="ico">&#9671;</div>Click Positions tab to load</div></div>
  </div>

  <!-- ORDERS -->
  <div class="panel" id="panel-orders">
    <div id="ocont"><div class="empty"><div class="ico">&#9671;</div>Click Orders tab to load</div></div>
  </div>

  <!-- BACKTEST -->
  <div class="panel" id="panel-backtest">
    <div class="bt-ctrl">
      <div class="kv">
        <span class="k">Symbols to test</span>
        <input id="bt-n" class="bt-inp" type="number" value="100" min="10" max="500">
      </div>
      <button class="btn primary" id="btn-bt" onclick="startBacktest()">&#9654; RUN BACKTEST</button>
      <span class="bt-hint">
        Walk-forward simulation &middot; ~13 days of historical 5-min bars<br>
        Enters at next-bar open &middot; 30-bar max hold &middot; all 5 criteria active
      </span>
    </div>

    <div id="bt-prog-wrap" style="display:none">
      <div class="bt-prog-msg">
        <span class="sdot scan" id="bt-sdot"></span>
        <span id="bt-msg">Initializing...</span>
      </div>
      <div class="prog-trk"><div class="prog-fill" id="bt-pfill"></div></div>
      <div class="bt-prog-sub" id="bt-sub"></div>
    </div>

    <div id="bt-results" style="display:none">
      <div class="bt-stats-grid" id="bt-stats"></div>
      <div class="bt-sec-title">Recent Simulated Trades</div>
      <div id="bt-tbl-wrap"></div>
    </div>
  </div>

</div>

<div id="toasts"></div>

<script>
var alerts    = [];
var autoTrade = false;
var es        = null;

// ── ACCOUNT ─────────────────────────────────────────────────────────────────
async function refreshAccount() {
  try {
    var a = await fetch('/api/account').then(function(r){return r.json();});
    if (a.error) return;
    var eq = parseFloat(a.equity       || 0);
    var bp = parseFloat(a.buying_power || 0);
    var pl = eq - parseFloat(a.last_equity || eq);
    var fmt = function(n){ return '$' + Math.abs(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}); };
    document.getElementById('acc-eq').textContent = fmt(eq);
    document.getElementById('acc-bp').textContent = fmt(bp);
    var plEl = document.getElementById('acc-pl');
    plEl.textContent = (pl >= 0 ? '+' : '-') + fmt(pl);
    plEl.className   = 'v ' + (pl >= 0 ? 'g' : 'r');
  } catch(e) {}
}

// ── SSE ──────────────────────────────────────────────────────────────────────
function connectSSE() {
  if (es) es.close();
  es = new EventSource('/stream');

  es.addEventListener('init', function(e) {
    var d = JSON.parse(e.data);
    setStatus(d.status || 'idle');
    if (d.scanned && d.total) updateProg(d.scanned, d.total, 0, 0);
  });

  es.addEventListener('progress', function(e) {
    var d = JSON.parse(e.data);
    updateProg(d.scanned, d.total, d.eta, d.alerts);
  });

  es.addEventListener('alert', function(e) {
    var a = JSON.parse(e.data);
    addCard(a);
    toast('\u26a1 ' + a.symbol + '  ' + a.entry_type + '  R:R ' + a.rr + ':1', 'alert');
  });

  es.addEventListener('status', function(e) {
    var d = JSON.parse(e.data);
    setStatus(d.status);
  });

  es.addEventListener('scan_complete', function(e) {
    var d = JSON.parse(e.data);
    updateProg(d.total, d.total, 0, d.alerts);
    setStatus('done');
    document.getElementById('acc-ls').textContent = new Date().toLocaleTimeString();
    toast('Scan complete \u2014 ' + d.alerts + ' alert' + (d.alerts !== 1 ? 's' : '') + ' found', 'info');
  });

  es.addEventListener('error', function(e) {
    if (e.data) { try { toast(JSON.parse(e.data).message, 'err'); } catch(_){} }
  });

  es.addEventListener('trade', function(e) {
    var d = JSON.parse(e.data);
    var m = d.status === 'submitted'
      ? '\u2714 Order: ' + d.symbol + ' \xd7' + d.qty + ' @ $' + d.entry
      : '\u2716 Trade failed: ' + d.symbol + ' \u2014 ' + d.error;
    toast(m, 'trade');
  });

  // Backtest events
  es.addEventListener('backtest_status', function(e) {
    var d = JSON.parse(e.data);
    document.getElementById('bt-msg').textContent = d.message || d.status;
  });

  es.addEventListener('backtest_progress', function(e) {
    var d = JSON.parse(e.data);
    var fill = document.getElementById('bt-pfill');
    fill.style.width = d.progress + '%';
    var sub = document.getElementById('bt-sub');
    if (d.phase === 'fetching') {
      sub.textContent = 'Downloading: ' + (d.symbols_done || 0) + ' / ' + (d.total || '?') + ' symbols';
    } else {
      sub.textContent = 'Simulating: ' + (d.symbols_done || 0) + ' / ' + (d.total || '?') + ' \u2014 ' + (d.trades_found || 0) + ' trades found';
    }
  });

  es.addEventListener('backtest_complete', function(e) {
    var d = JSON.parse(e.data);
    document.getElementById('bt-pfill').style.width = '100%';
    document.getElementById('bt-pfill').classList.add('done');
    document.getElementById('bt-sdot').className = 'sdot done';
    document.getElementById('bt-msg').textContent = 'Backtest complete';
    document.getElementById('bt-sub').textContent = d.symbols_tested + ' symbols tested \u00b7 ' + d.total_signals + ' signals';
    renderBacktestResults(d);
    document.getElementById('btn-bt').disabled = false;
    toast('Backtest done \u2014 ' + d.total_signals + ' trades, ' + d.win_rate + '% win rate', 'info');
  });

  es.addEventListener('backtest_error', function(e) {
    var d = JSON.parse(e.data);
    document.getElementById('bt-sdot').className = 'sdot stop';
    document.getElementById('bt-msg').textContent = 'Error: ' + d.message;
    document.getElementById('btn-bt').disabled = false;
    toast('Backtest error: ' + d.message, 'err');
  });

  es.onerror = function(){ setTimeout(connectSSE, 3000); };
}

// ── CONTROLS ─────────────────────────────────────────────────────────────────
async function startScan() {
  alerts = [];
  document.getElementById('agrid').innerHTML = '';
  document.getElementById('ph').style.display = 'none';
  document.getElementById('bdg').textContent = '0';
  await fetch('/api/start', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({auto_trade:autoTrade})});
  document.getElementById('btn-start').disabled = true;
  document.getElementById('btn-stop').disabled  = false;
  setStatus('scanning');
}

async function stopScan() {
  await fetch('/api/stop', {method:'POST'});
  document.getElementById('btn-start').disabled = false;
  document.getElementById('btn-stop').disabled  = true;
  setStatus('stopped');
}

document.getElementById('auto-lbl').addEventListener('click', async function() {
  autoTrade = !autoTrade;
  document.getElementById('auto-trk').classList.toggle('on', autoTrade);
  await fetch('/api/auto_trade', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({enabled:autoTrade})});
  toast('Auto-trade ' + (autoTrade ? 'ENABLED' : 'DISABLED'), 'info');
});

// ── PROGRESS ─────────────────────────────────────────────────────────────────
function updateProg(scanned, total, eta, alertCount) {
  var pct  = total > 0 ? scanned / total * 100 : 0;
  var fill = document.getElementById('pfill');
  fill.style.width = pct + '%';
  fill.classList.toggle('done', scanned >= total && total > 0);
  document.getElementById('pnum').textContent    = scanned.toLocaleString() + ' / ' + total.toLocaleString();
  document.getElementById('peta').textContent    = eta > 0 ? eta + 's' : '\u2014';
  document.getElementById('palerts').textContent = alertCount;
  document.getElementById('bdg').textContent     = alerts.length;
}

function setStatus(s) {
  var dot  = document.getElementById('sdot');
  var text = document.getElementById('stxt');
  var M = {
    idle:    ['', 'IDLE'],
    scanning:['scan','SCANNING'],
    waiting: ['','WAITING \u2014 NEXT SCAN IN 5 MIN'],
    done:    ['done','SCAN COMPLETE'],
    stopped: ['stop','STOPPED'],
  };
  var e = M[s] || ['', s.toUpperCase()];
  dot.className    = 'sdot ' + e[0];
  text.textContent = e[1];
}

// ── ALERT CARDS ──────────────────────────────────────────────────────────────
function addCard(a) {
  var old = document.getElementById('ac-' + a.symbol);
  if (old) old.remove();
  alerts.unshift(a);
  document.getElementById('bdg').textContent     = alerts.length;
  document.getElementById('palerts').textContent = alerts.length;

  var typeKey = a.entry_type === 'Breakout' ? 'breakout'
              : a.entry_type === 'Pivot Reclaim' ? 'pivot' : 'pullback';

  var card = document.createElement('div');
  card.className = 'alert-card ' + typeKey;
  card.id        = 'ac-' + a.symbol;

  var ts = new Date(a.timestamp).toLocaleTimeString();
  var c  = a.criteria;

  card.innerHTML =
    '<div class="ah">' +
      '<div>' +
        '<div class="a-sym">' + a.symbol + '</div>' +
        '<div class="a-ts">' + ts + '</div>' +
      '</div>' +
      '<div class="a-typ ' + typeKey + '">' + a.entry_type + '</div>' +
    '</div>' +
    '<div class="a-px">' +
      '<div class="pc"><div class="l">Entry</div><div class="v e">$' + a.entry.toFixed(2) + '</div></div>' +
      '<div class="pc"><div class="l">Stop</div><div class="v s">$' + a.stop.toFixed(2) + '</div></div>' +
      '<div class="pc"><div class="l">Target</div><div class="v t">$' + a.target.toFixed(2) + '</div></div>' +
    '</div>' +
    '<div class="a-st">' +
      '<span>R:R <span class="sv g">' + a.rr + ':1</span></span>' +
      '<span>ATR <span class="sv">$' + a.atr + '</span></span>' +
      '<span>RVOL <span class="sv">' + a.rvol + '\u00d7</span></span>' +
      '<span>Qty <span class="sv">' + a.qty + ' sh</span></span>' +
    '</div>' +
    '<div class="crit-list">' +
      mkCrit(1,'Trend', c.trend) +
      mkCrit(2,'Entry', c.entry) +
      mkCrit(3,'S/R',   c.sr)    +
      mkCrit(4,'Risk',  c.risk)  +
      mkCrit(5,'Vol',   c.volume) +
    '</div>';

  var grid = document.getElementById('agrid');
  grid.insertBefore(card, grid.firstChild);
}

function mkCrit(n, label, detail) {
  return '<div class="crit">' +
    '<span class="cn">' + n + '</span>' +
    '<span class="ck">' + label + '</span>' +
    '<span class="cd">' + escHtml(detail) + '</span>' +
  '</div>';
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── TABS ──────────────────────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.tab').forEach(function(t){ t.classList.toggle('active', t.dataset.tab === tab); });
  document.querySelectorAll('.panel').forEach(function(p){ p.classList.toggle('active', p.id === 'panel-' + tab); });
  if (tab === 'positions') loadPositions();
  if (tab === 'orders')    loadOrders();
}

// ── POSITIONS ─────────────────────────────────────────────────────────────────
async function loadPositions() {
  var el = document.getElementById('pcont');
  el.innerHTML = '<div class="empty">Loading\u2026</div>';
  try {
    var ps = await fetch('/api/positions').then(function(r){return r.json();});
    if (!ps.length) { el.innerHTML = '<div class="empty"><div class="ico">\u25c7</div>No open positions</div>'; return; }
    var rows = ps.map(function(p) {
      var pnl = parseFloat(p.unrealized_pl || 0);
      var ppc = parseFloat(p.unrealized_plpc || 0) * 100;
      var cls = pnl >= 0 ? 'pos' : 'neg';
      var sgn = pnl >= 0 ? '+' : '-';
      return '<tr>' +
        '<td class="sym">' + p.symbol + '</td>' +
        '<td>' + p.qty + '</td>' +
        '<td>$' + parseFloat(p.avg_entry_price || 0).toFixed(2) + '</td>' +
        '<td>$' + parseFloat(p.current_price   || 0).toFixed(2) + '</td>' +
        '<td class="' + cls + '">' + sgn + '$' + Math.abs(pnl).toFixed(2) + '</td>' +
        '<td class="' + cls + '">' + sgn + Math.abs(ppc).toFixed(2) + '%</td>' +
        '<td>$' + parseFloat(p.market_value || 0).toLocaleString('en-US',{minimumFractionDigits:2}) + '</td>' +
        '<td><button class="btn danger sm" onclick="closePos(\'' + p.symbol + '\')">CLOSE</button></td></tr>';
    }).join('');
    el.innerHTML = '<table class="dtbl"><thead><tr><th>Symbol</th><th>Qty</th><th>Avg Entry</th><th>Price</th><th>P&amp;L $</th><th>P&amp;L %</th><th>Mkt Val</th><th></th></tr></thead><tbody>' + rows + '</tbody></table>';
  } catch(e) { el.innerHTML = '<div class="empty">Error: ' + e.message + '</div>'; }
}

async function closePos(sym) {
  if (!confirm('Close entire position in ' + sym + '?')) return;
  await fetch('/api/close/' + sym, {method:'DELETE'});
  toast('Closing ' + sym + '\u2026', 'trade');
  loadPositions();
}

// ── ORDERS ────────────────────────────────────────────────────────────────────
async function loadOrders() {
  var el = document.getElementById('ocont');
  el.innerHTML = '<div class="empty">Loading\u2026</div>';
  try {
    var os = await fetch('/api/orders').then(function(r){return r.json();});
    if (!os.length) { el.innerHTML = '<div class="empty"><div class="ico">\u25c7</div>No recent orders</div>'; return; }
    var CLR = {filled:'var(--grn)',canceled:'var(--dim)',pending_new:'var(--yel)',new:'var(--yel)',partially_filled:'var(--cyn)'};
    var CAN = ['pending_new','new','partially_filled'];
    var rows = os.map(function(o) {
      var sc = CLR[o.status] || 'var(--txt)';
      var px = o.filled_avg_price ? '$' + parseFloat(o.filled_avg_price).toFixed(2)
               : o.limit_price    ? '$' + parseFloat(o.limit_price).toFixed(2) : '\u2014';
      var cb = CAN.indexOf(o.status) >= 0
        ? '<button class="btn sm" onclick="cancelOrd(\'' + o.id + '\')">CANCEL</button>' : '';
      return '<tr>' +
        '<td class="sym">' + o.symbol + '</td>' +
        '<td>' + o.side.toUpperCase() + '</td>' +
        '<td>' + o.qty + ' / ' + parseFloat(o.filled_qty||0) + '</td>' +
        '<td>' + px + '</td>' +
        '<td>' + o.type + '</td>' +
        '<td style="color:' + sc + '">' + o.status.toUpperCase().replace(/_/g,' ') + '</td>' +
        '<td style="color:var(--dim);font-size:9px">' + new Date(o.created_at).toLocaleTimeString() + '</td>' +
        '<td>' + cb + '</td></tr>';
    }).join('');
    el.innerHTML = '<table class="dtbl"><thead><tr><th>Symbol</th><th>Side</th><th>Qty/Fill</th><th>Price</th><th>Type</th><th>Status</th><th>Time</th><th></th></tr></thead><tbody>' + rows + '</tbody></table>';
  } catch(e) { el.innerHTML = '<div class="empty">Error: ' + e.message + '</div>'; }
}

async function cancelOrd(id) {
  await fetch('/api/cancel/' + id, {method:'DELETE'});
  toast('Order cancelled', 'info');
  loadOrders();
}

// ── BACKTEST ──────────────────────────────────────────────────────────────────
async function startBacktest() {
  var n = parseInt(document.getElementById('bt-n').value) || 100;
  var res = await fetch('/api/backtest/start', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({n_symbols: n}),
  });
  var data = await res.json();
  if (data.error) { toast(data.error, 'err'); return; }

  document.getElementById('btn-bt').disabled = true;
  document.getElementById('bt-results').style.display  = 'none';
  document.getElementById('bt-prog-wrap').style.display = 'block';
  document.getElementById('bt-pfill').style.width  = '0%';
  document.getElementById('bt-pfill').classList.remove('done');
  document.getElementById('bt-sdot').className = 'sdot scan';
  document.getElementById('bt-msg').textContent = 'Starting...';
  document.getElementById('bt-sub').textContent = '';

  toast('Backtest started \u2014 ' + n + ' symbols', 'info');
}

function renderBacktestResults(r) {
  var isGood = function(key) {
    if (key === 'win_rate')      return r.win_rate >= 50 ? 'g' : (r.win_rate >= 40 ? 'y' : 'r');
    if (key === 'profit_factor') return r.profit_factor >= 1.5 ? 'g' : (r.profit_factor >= 1.0 ? 'y' : 'r');
    if (key === 'avg_pnl_r')     return r.avg_pnl_r > 0 ? 'g' : 'r';
    return '';
  };

  var stats = [
    {k:'Total Signals',   v: r.total_signals, cls:''},
    {k:'Wins',            v: r.wins,          cls:'g'},
    {k:'Losses',          v: r.losses,        cls:'r'},
    {k:'Timeouts',        v: r.timeouts,      cls:''},
    {k:'Win Rate',        v: r.win_rate + '%', cls: isGood('win_rate')},
    {k:'Avg R:R Setup',   v: r.avg_rr + ':1', cls:''},
    {k:'Avg P&amp;L (R)', v: (r.avg_pnl_r >= 0 ? '+' : '') + r.avg_pnl_r + 'R', cls: isGood('avg_pnl_r')},
    {k:'Profit Factor',   v: r.profit_factor, cls: isGood('profit_factor')},
    {k:'Symbols Tested',  v: r.symbols_tested, cls:''},
  ];

  document.getElementById('bt-stats').innerHTML = stats.map(function(s){
    return '<div class="bt-stat"><div class="k">' + s.k + '</div><div class="v ' + s.cls + '">' + s.v + '</div></div>';
  }).join('');

  // Load trades table
  fetch('/api/backtest/trades').then(function(r2){return r2.json();}).then(function(trades){
    if (!trades.length) {
      document.getElementById('bt-tbl-wrap').innerHTML = '<div class="empty" style="padding:30px">No simulated trades</div>';
    } else {
      var rows = trades.slice(0, 200).map(function(t) {
        var ocls = t.outcome === 'win' ? 'win' : (t.outcome === 'loss' ? 'loss' : 'timeout');
        var pnlCls = t.pnl_r > 0 ? 'pos' : (t.pnl_r < 0 ? 'neg' : '');
        var pnlStr = (t.pnl_r >= 0 ? '+' : '') + t.pnl_r + 'R';
        return '<tr>' +
          '<td class="sym">' + t.symbol + '</td>' +
          '<td style="color:var(--dim)">' + t.entry_type + '</td>' +
          '<td>$' + t.entry.toFixed(2) + '</td>' +
          '<td>$' + t.exit.toFixed(2) + '</td>' +
          '<td style="color:var(--red)">$' + t.stop.toFixed(2) + '</td>' +
          '<td style="color:var(--grn)">$' + t.target.toFixed(2) + '</td>' +
          '<td>' + t.rr.toFixed(1) + ':1</td>' +
          '<td class="' + ocls + '">' + t.outcome.toUpperCase() + '</td>' +
          '<td class="' + pnlCls + '">' + pnlStr + '</td>' +
        '</tr>';
      }).join('');
      document.getElementById('bt-tbl-wrap').innerHTML =
        '<table class="dtbl"><thead><tr>' +
        '<th>Symbol</th><th>Type</th><th>Entry</th><th>Exit</th><th>Stop</th><th>Target</th><th>R:R</th><th>Outcome</th><th>PnL</th>' +
        '</tr></thead><tbody>' + rows + '</tbody></table>';
    }
    document.getElementById('bt-results').style.display = 'block';
  }).catch(function(){ });
}

// ── TOASTS ────────────────────────────────────────────────────────────────────
function toast(msg, type) {
  var c = document.getElementById('toasts');
  var t = document.createElement('div');
  t.className   = 'toast ' + (type || 'info');
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(function(){ t.remove(); }, 5500);
}

// ── INIT ──────────────────────────────────────────────────────────────────────
connectSSE();
refreshAccount();
setInterval(refreshAccount, 30000);
</script>
</body>
</html>"""


# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    t = threading.Thread(target=scanner_loop, daemon=True, name="scanner")
    t.start()
    log.info("Scanner thread started — open http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=os.environ.get("FLASK_ENV") == "development", threaded=True)
