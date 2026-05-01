#!/usr/bin/env python3
"""
Alpaca Paper Trading Dashboard — v2
Full-market scanner · 5-criteria signal detection · Backtest engine
"""

import json, math, os, queue, random, sqlite3, threading, time, logging
from datetime import datetime, timedelta, timezone
import pytz

import numpy as np
import pandas as pd
import requests
import ta
from flask import Flask, Response, jsonify, request, render_template_string, session

# ── Auth / Payments layer (requires: pip install authlib stripe flask-login flask-sqlalchemy) ──
from auth import (
    auth_bp, login_required, init_users_db, get_user,
    oauth, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
)
from landing import landing_bp
from legal import legal_bp

ET_TZ = pytz.timezone("America/New_York")

# ── Configuration ──────────────────────────────────────────────────────────────

API_KEY    = os.environ.get("APCA_API_KEY_ID", "PK6EL5DWC4LRRX7LMMHMIGL5YF")
API_SECRET = os.environ.get("APCA_API_SECRET_KEY", "9ReJZu61ANVBwT5wVacXrTYtFWtnTkfy93NHkVgQNbP7")
BASE_URL   = os.environ.get("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
DATA_URL   = "https://data.alpaca.markets"

HEADERS = {
    "APCA-API-KEY-ID":     API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET,
    "Content-Type":        "application/json",
}
ALPACA_HEADERS = HEADERS   # alias used in 2-pass scanner

BATCH_SIZE       = 50
MIN_PRICE        = 5.0
MAX_PRICE        = 10_000.0
MIN_DAILY_VOL    = 200_000   # loosened: SIP vol is real consolidated tape vol
RVOL_MIN         = 8.0       # Warrior Trading: minimum relative volume (raised to 8x for high-conviction only)
GAP_MIN          = 10.0      # minimum gap % for Gap-and-Go signals (raised from 5% to 10%)
PRICE_MIN        = 2.0       # minimum stock price: below $2 too volatile/manipulated
PRICE_MAX        = 20.0      # maximum stock price: above $20 percentage moves shrink
MAX_STOP_PCT     = 5.0       # maximum allowed stop distance as % of entry (skip if risk too large)
MIN_GAIN_PCT     = 10.0      # minimum +10% daily gain vs prev close
ATR_TARGET_MULT  = 3.0
MIN_RR           = 2.0
ACCOUNT_RISK_PCT = 0.01      # 1% account risk per trade
SCAN_INTERVAL    = 300       # seconds between live scan passes
PASS1_BATCH_SIZE = 200       # symbols per batch in Pass 1 RVOL screen
LOOKBACK         = 80        # bars for live scan
BT_LOOKBACK      = 1000      # bars per symbol for backtest (~13 trading days)
TIMEFRAME        = "5Min"
SWING_ORDER      = 2         # bars each side for swing-point detection

# Curated high-volatility symbols for backtest runs
CURATED_SYMBOLS = [
    "AMC","GME","BBBY","CLOV","MVIS","EXPR","RKT","WISH","WKHS","OCGN","SNDL",
    "BB","NOK","KOSS","SPCE","TLRY","RIDE","NKLA","HYLN","BLNK","GOEV","FSR",
    "ARVL","XPEV","NIO","PLTR","CTRM","GNUS","MARK","ZOM","IDEX","FCEL","SENS",
    "ATER","BBIG","PHUN","IMPP","TLGA","MULN","PTRA","ZEV","SOLO","WATT","GREE",
    "CLSK","MARA","RIOT","COIN","HOOD","LCID","RIVN","SOFI","OPEN","UPST","AFRM",
]

# ── Sector ETFs for Heatmap ────────────────────────────────────────────────────
SECTOR_ETFS = {
    "XLK": "Tech",
    "XLF": "Finance",
    "XLV": "Health",
    "XLE": "Energy",
    "XLI": "Industrl",
    "XLY": "ConDisc",
    "XLP": "ConStapl",
    "XLB": "Matls",
    "XLRE": "RealEst",
    "XLU": "Utility",
    "XLC": "Comm",
}

# ── App & Global State ─────────────────────────────────────────────────────────

app = Flask(__name__)

# ── Auth / Subscription setup ──────────────────────────────────────────────────
app.secret_key = os.environ.get("FLASK_SECRET_KEY",
                                "change-this-to-a-random-secret-key-before-launch")

oauth.init_app(app)
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

app.register_blueprint(auth_bp)
app.register_blueprint(landing_bp)
app.register_blueprint(legal_bp)
init_users_db()
# ───────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

state = {
    "running":       False,
    "total":         0,
    "scanned":       0,
    "alerts":        [],
    "last_scan":     None,
    "auto_trade":    False,
    "start_time":    None,
    "status":        "idle",
    "gapper_count":  0,
    "market_cycle":  "neutral",
}
state_lock = threading.Lock()

backtest_state = {
    "running":   False,
    "status":    "idle",
    "progress":  0,
    "total":     0,
    "results":   None,
    "trades":    [],
    "error":     None,
    "run_id":    None,
    "runs_done": 0,
}
bt_lock = threading.Lock()

event_queues = []
eq_lock      = threading.Lock()

_float_cache: dict = {}   # symbol → shares_outstanding (None if unavailable); persists across scans
_news_cache:  dict = {}   # symbol → (headlines_list, fetch_epoch)
_heatmap_cache: dict = {}  # ticker → pct_change (SPY + sector ETFs); updated by api_sector_heatmap()
_trailing_status: dict = {}   # symbol → {"phase": "be"|"trail", "stop": float}

watchlist_alerts: list = []   # last alert dicts from watchlist-only scan loop
watchlist_lock   = threading.Lock()

premarket_gappers: list = []  # gapper dicts from pre-market scan
premarket_lock    = threading.Lock()

DB_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db")
WATCHLIST_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchlist.json")
NEWS_CACHE_TTL = 300   # seconds

# ── Daily Risk Limits ──────────────────────────────────────────────────────────
DAILY_MAX_LOSS   = 500.0
DAILY_MAX_TRADES = 6
daily_loss_total  = 0.0
daily_trade_count = 0
trading_halted    = False
daily_risk_lock   = threading.Lock()
_last_risk_reset_date: str = ""   # ET date string "YYYY-MM-DD"

# ── 2-Pass Scanner State ────────────────────────────────────────────────────────
# daily_watchlist: symbols that passed Pass 1 RVOL screen today → {symbol: rvol}
# Resets each trading day at 4 AM ET (keyed by ET date when hour >= 4).
daily_watchlist: dict      = {}
daily_watchlist_date: str  = None
daily_watchlist_lock       = threading.Lock()


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


def fetch_bars_batch(symbols: list, limit: int = LOOKBACK,
                     days_back: int = 14) -> dict:
    """
    Fetch recent bars for a batch of symbols using the SIP consolidated feed.
    Uses a start-date window so data is available even when markets are closed.
    The multi-symbol endpoint has a per-page cap; we paginate once and take the
    most recent `limit` bars per symbol.
    """
    start_dt = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
    params = {
        "symbols":   ",".join(symbols),
        "timeframe": TIMEFRAME,
        "start":     start_dt,
        "limit":     10000,          # request max; API pages internally
        "feed":      "sip",
        "sort":      "asc",
    }
    all_raw: dict = {}
    for _page in range(4):          # at most 4 pages per batch
        try:
            r = requests.get(f"{DATA_URL}/v2/stocks/bars", headers=HEADERS,
                             params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"fetch_bars_batch error: {e}")
            break
        for sym, bars in (data.get("bars") or {}).items():
            all_raw.setdefault(sym, []).extend(bars or [])
        tok = data.get("next_page_token")
        if not tok:
            break
        params["page_token"] = tok

    result = {}
    for sym, bars in all_raw.items():
        if not bars:
            continue
        # Filter to regular market hours only (13:30–20:00 UTC = 9:30–16:00 ET)
        rh = [b for b in bars
              if 13 * 60 + 30 <= int(b["t"][11:13]) * 60 + int(b["t"][14:16]) < 20 * 60]
        bars_to_use = rh if len(rh) >= 30 else bars   # fall back if no RH data
        # Take the most recent `limit` bars
        bars_to_use = bars_to_use[-limit:]
        df = pd.DataFrame(bars_to_use)
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
        "type":          "market",
        "time_in_force": "day",
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


def submit_market_sell(symbol: str, qty: int):
    """Submit a market sell order for an open position."""
    order = {
        "symbol":        symbol,
        "qty":           str(int(qty)),
        "side":          "sell",
        "type":          "market",
        "time_in_force": "day",
    }
    r = requests.post(f"{BASE_URL}/v2/orders", headers=HEADERS, json=order, timeout=10)
    r.raise_for_status()
    return r.json()


def _log_auto_exit(symbol: str, exit_price: float, reason: str, qty: int = 0):
    """Update the most recent open journal entry for symbol with auto-exit info."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM trade_journal WHERE symbol=? AND status='open' ORDER BY id DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if row:
            ep  = row["entry_price"] or 0
            sh  = row["shares"] or qty
            pnl = round((exit_price - ep) * sh, 2)
            con.execute(
                "UPDATE trade_journal SET status='closed', exit_price=?, pnl=?, notes=? WHERE id=?",
                (exit_price, pnl, f"auto-exit: {reason}", row["id"]),
            )
            con.commit()
        con.close()
    except Exception as e:
        log.warning(f"_log_auto_exit {symbol}: {e}")


# ── Trade Journal (SQLite) ──────────────────────────────────────────────────────

def init_db():
    """Create trades.db and all tables if they don't exist."""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trade_journal (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp        TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            entry_price      REAL,
            stop_price       REAL,
            target_price     REAL,
            shares           INTEGER,
            risk_dollars     REAL,
            reward_dollars   REAL,
            rr_ratio         REAL,
            atr              REAL,
            entry_type       TEXT,
            rvol             REAL,
            gap_pct          REAL,
            pct_change_today REAL,
            float_shares     REAL,
            status           TEXT DEFAULT 'open',
            exit_price       REAL,
            pnl              REAL,
            notes            TEXT,
            order_id         TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS backtest_results (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id        TEXT NOT NULL,
            symbol        TEXT NOT NULL,
            entry_dt      TEXT,
            exit_dt       TEXT,
            entry_price   REAL,
            stop_price    REAL,
            target_price  REAL,
            exit_price    REAL,
            pnl           REAL,
            r_multiple    REAL,
            hold_bars     INTEGER,
            exit_reason   TEXT,
            criteria_fired TEXT,
            run_ts        TEXT
        )
    """)
    con.commit()
    con.close()
    log.info(f"Trade journal DB ready: {DB_PATH}")


# ── Watchlist helpers ─────────────────────────────────────────────────────────

def load_watchlist() -> list:
    """Return list of symbols from watchlist.json (creates file if missing)."""
    if not os.path.exists(WATCHLIST_PATH):
        return []
    try:
        with open(WATCHLIST_PATH, "r") as f:
            data = json.load(f)
            return [s.upper() for s in data if isinstance(s, str)]
    except Exception:
        return []


def save_watchlist(symbols: list):
    """Persist list of symbols to watchlist.json."""
    try:
        with open(WATCHLIST_PATH, "w") as f:
            json.dump([s.upper() for s in symbols], f)
    except Exception as e:
        log.warning(f"save_watchlist: {e}")


# ── MTF bar helper ────────────────────────────────────────────────────────────

def fetch_mtf_bars(symbol: str, timeframe: str, days_back: int, limit: int = 100):
    """Fetch bars for a single symbol at a given timeframe (e.g. '15Min', '1Hour')."""
    start_dt = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
    params = {
        "symbols":   symbol,
        "timeframe": timeframe,
        "start":     start_dt,
        "limit":     1000,
        "feed":      "sip",
        "sort":      "asc",
    }
    try:
        r = requests.get(f"{DATA_URL}/v2/stocks/bars", headers=HEADERS,
                         params=params, timeout=20)
        r.raise_for_status()
        bars = (r.json().get("bars") or {}).get(symbol, [])
        if not bars:
            return None
        bars = bars[-limit:]
        df = pd.DataFrame(bars)
        df.rename(columns={"o": "open", "h": "high", "l": "low",
                            "c": "close", "v": "volume", "t": "time"}, inplace=True)
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)
        df = df[["open", "high", "low", "close", "volume"]].astype(float)
        return df
    except Exception as e:
        log.debug(f"fetch_mtf_bars {symbol} {timeframe}: {e}")
        return None


def log_trade(data: dict, order_id: str = None):
    """Insert one trade into the journal. data may be the full alert dict."""
    entry  = float(data.get("entry") or data.get("entry_price") or 0)
    stop   = float(data.get("stop")  or data.get("stop_price")  or 0)
    target = float(data.get("target") or data.get("target_price") or 0)
    shares = int(data.get("qty") or data.get("shares") or 0)
    rps    = entry - stop
    risk_d = round(rps * shares, 2)
    rew_d  = round((target - entry) * shares, 2)
    rr     = data.get("rr") or data.get("rr_ratio") or (rew_d / risk_d if risk_d else 0)
    sym    = data.get("symbol", "")
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO trade_journal
            (timestamp, symbol, entry_price, stop_price, target_price, shares,
             risk_dollars, reward_dollars, rr_ratio, atr, entry_type, rvol, gap_pct,
             pct_change_today, float_shares, status, order_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        datetime.now().isoformat(), sym,
        entry, stop, target, shares, risk_d, rew_d, round(float(rr), 2),
        data.get("atr"),
        data.get("entry_type"),
        data.get("rvol"),
        data.get("gap_pct"),
        data.get("pct_change_today"),
        _float_cache.get(sym),
        "open", order_id,
    ))
    con.commit()
    con.close()
    log.info(f"Journal: logged {sym} {data.get('entry_type')} entry=${entry:.2f}")


def get_journal_entries() -> list:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM trade_journal ORDER BY id DESC").fetchall()
    con.close()
    return [dict(r) for r in rows]


# ── Backtest DB helpers ────────────────────────────────────────────────────────

def save_bt_result(run_id: str, trade: dict, run_ts: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO backtest_results
            (run_id, symbol, entry_dt, exit_dt, entry_price, stop_price, target_price,
             exit_price, pnl, r_multiple, hold_bars, exit_reason, criteria_fired, run_ts)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        run_id,
        trade.get("symbol"),
        trade.get("entry_dt"),
        trade.get("exit_dt"),
        trade.get("entry"),
        trade.get("stop"),
        trade.get("target"),
        trade.get("exit"),
        trade.get("pnl_dollar", 0),
        trade.get("pnl_r", 0),
        trade.get("hold_bars", 0),
        trade.get("outcome"),
        trade.get("entry_type"),
        run_ts,
    ))
    con.commit()
    con.close()


def get_bt_results(run_id: str = None) -> list:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    if run_id:
        rows = con.execute(
            "SELECT * FROM backtest_results WHERE run_id=? ORDER BY id DESC",
            (run_id,)
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM backtest_results ORDER BY id DESC LIMIT 500"
        ).fetchall()
    con.close()
    return [dict(r) for r in rows]


def get_bt_summary() -> dict:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM backtest_results ORDER BY id ASC").fetchall()
    con.close()
    if not rows:
        return {"total": 0, "runs": 0}

    trades  = [dict(r) for r in rows]
    run_ids = list({t["run_id"] for t in trades})

    wins     = [t for t in trades if t["exit_reason"] == "win"]
    losses   = [t for t in trades if t["exit_reason"] == "loss"]
    timeouts = [t for t in trades if t["exit_reason"] == "timeout"]
    total    = len(trades)

    win_rate = len(wins) / total * 100 if total else 0
    r_vals   = [t["r_multiple"] or 0 for t in trades]
    avg_r    = float(np.mean(r_vals)) if r_vals else 0

    gross_win  = sum(t["r_multiple"] for t in wins   if t["r_multiple"])
    gross_loss = abs(sum(t["r_multiple"] for t in losses if t["r_multiple"]))
    pf = gross_win / gross_loss if gross_loss > 0 else (9.99 if gross_win > 0 else 0.0)

    # Max drawdown (in R)
    cum_r = 0.0; peak = 0.0; max_dd = 0.0
    for t in trades:
        cum_r += t["r_multiple"] or 0
        if cum_r > peak: peak = cum_r
        dd = peak - cum_r
        if dd > max_dd: max_dd = dd

    # Sharpe-like
    sharpe = 0.0
    if len(r_vals) > 1:
        std    = float(np.std(r_vals))
        sharpe = round(avg_r / std, 2) if std > 0 else 0.0

    # Best / worst trade
    best_trade  = max(r_vals) if r_vals else 0
    worst_trade = min(r_vals) if r_vals else 0

    # Average hold duration in minutes
    hold_minutes = []
    for t in trades:
        try:
            if t.get("entry_dt") and t.get("exit_dt"):
                dt_entry = datetime.fromisoformat(str(t["entry_dt"]).replace("Z", "+00:00").split("+")[0])
                dt_exit  = datetime.fromisoformat(str(t["exit_dt"]).replace("Z", "+00:00").split("+")[0])
                diff_min = (dt_exit - dt_entry).total_seconds() / 60
                if 0 < diff_min < 10000:
                    hold_minutes.append(diff_min)
        except Exception:
            pass
    avg_hold_minutes = round(float(np.mean(hold_minutes)), 1) if hold_minutes else 0

    # Exit reason breakdown %
    pct_target = len([t for t in trades if (t["exit_reason"] or "").lower() == "win"]) / total * 100 if total else 0
    pct_stop   = len([t for t in trades if (t["exit_reason"] or "").lower() == "loss"]) / total * 100 if total else 0
    pct_eod    = len([t for t in trades if (t["exit_reason"] or "").lower() in ("eod", "timeout")]) / total * 100 if total else 0

    # Win streaks
    cur_streak = 0; max_streak = 0; running = 0
    for t in trades:
        if (t["exit_reason"] or "").lower() == "win":
            running += 1
            if running > max_streak: max_streak = running
        else:
            running = 0
    # Current streak from the tail
    cur_streak = 0
    for t in reversed(trades):
        if (t["exit_reason"] or "").lower() == "win":
            cur_streak += 1
        else:
            break

    return {
        "total":            total,
        "wins":             len(wins),
        "losses":           len(losses),
        "timeouts":         len(timeouts),
        "win_rate":         round(win_rate, 1),
        "avg_r":            round(avg_r, 3),
        "profit_factor":    round(float(pf), 2),
        "max_drawdown_r":   round(max_dd, 2),
        "sharpe":           sharpe,
        "runs":             len(run_ids),
        "run_ids":          run_ids,
        "best_trade":       round(best_trade, 2),
        "worst_trade":      round(worst_trade, 2),
        "avg_hold_minutes": avg_hold_minutes,
        "pct_target_hit":   round(pct_target, 1),
        "pct_stop_hit":     round(pct_stop, 1),
        "pct_eod_exit":     round(pct_eod, 1),
        "current_streak":   cur_streak,
        "max_streak":       max_streak,
    }


def fetch_daily_bars_batch(symbols: list) -> dict:
    """
    Fetch daily bars for a batch of symbols (SIP feed).
    Returns {symbol: {"prev_close": float, "today_open": float|None}}.
    prev_close = most recent completed session close (i.e. yesterday).
    today_open = today's daily open if a bar for today exists, else None.
    """
    start_dt = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
    params = {
        "symbols":   ",".join(symbols),
        "timeframe": "1Day",
        "start":     start_dt,
        "limit":     1000,
        "feed":      "sip",
        "sort":      "asc",
    }
    try:
        r = requests.get(f"{DATA_URL}/v2/stocks/bars", headers=HEADERS,
                         params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning(f"fetch_daily_bars_batch error: {e}")
        return {}

    today_str = datetime.now(ET_TZ).strftime("%Y-%m-%d")
    result    = {}
    for sym, bars in (data.get("bars") or {}).items():
        if not bars:
            continue
        prev_bar  = None
        today_bar = None
        for b in reversed(bars):
            bar_date = b["t"][:10]
            if bar_date == today_str:
                today_bar = b
            elif prev_bar is None and bar_date < today_str:
                prev_bar = b
            if prev_bar:
                break
        if prev_bar:
            result[sym] = {
                "prev_close": float(prev_bar["c"]),
                "today_open": float(today_bar["o"]) if today_bar else None,
            }
    return result


def fetch_daily_rvol_batch(symbols: list) -> dict:
    """
    Pass 1 helper: fetch last 51 daily bars for a batch of symbols (SIP feed).
    Returns {symbol: rvol} where rvol = latest_bar_volume / mean(prior 50 bars volume).
    Uses timeframe=1Day, limit=51. Works both during market hours (building daily bar)
    and outside market hours (last completed daily bar).
    """
    # 80 calendar days covers ~55 trading days comfortably
    start_dt = (datetime.now(timezone.utc) - timedelta(days=80)).strftime("%Y-%m-%dT00:00:00Z")
    params = {
        "symbols":   ",".join(symbols),
        "timeframe": "1Day",
        "start":     start_dt,
        "limit":     10000,
        "feed":      "sip",
        "sort":      "asc",
    }
    try:
        r = requests.get(f"{DATA_URL}/v2/stocks/bars", headers=ALPACA_HEADERS,
                         params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.warning(f"fetch_daily_rvol_batch error: {e}")
        return {}

    result = {}
    for sym, bars in (data.get("bars") or {}).items():
        if not bars or len(bars) < 2:
            continue
        # Take last 51 bars (or fewer if limited history)
        recent = bars[-51:]
        if len(recent) < 2:
            continue
        latest_vol = float(recent[-1].get("v", 0))
        prior_vols = [float(b.get("v", 0)) for b in recent[:-1]]
        avg_vol    = float(np.mean(prior_vols)) if prior_vols else 0
        if avg_vol <= 0:
            continue
        result[sym] = round(latest_vol / avg_vol, 2)
    return result


def fetch_asset_float(symbol: str):
    """
    Return shares_outstanding for symbol from /v2/assets (cached).
    Returns float or None if field is absent / request fails.
    """
    if symbol in _float_cache:
        return _float_cache[symbol]
    try:
        r = requests.get(f"{BASE_URL}/v2/assets/{symbol}",
                         headers=HEADERS, timeout=5)
        r.raise_for_status()
        so = r.json().get("shares_outstanding")
        val = float(so) if so else None
    except Exception:
        val = None
    _float_cache[symbol] = val
    return val


def fetch_premarket_bars(symbol: str):
    """
    Fetch 5-min bars for the premarket session today (4:00–9:29 AM ET).
    Returns a DataFrame or None if no data / called outside the window.
    """
    now_et   = datetime.now(ET_TZ)
    start_et = now_et.replace(hour=4, minute=0, second=0, microsecond=0)
    end_et   = now_et.replace(hour=9, minute=29, second=59, microsecond=0)
    if now_et < start_et:
        return None
    fetch_end = min(end_et, now_et)
    if fetch_end <= start_et:
        return None
    params = {
        "timeframe": "5Min",
        "start":     start_et.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end":       fetch_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "feed":      "sip",
        "sort":      "asc",
        "limit":     100,
    }
    try:
        r = requests.get(f"{DATA_URL}/v2/stocks/{symbol}/bars",
                         headers=HEADERS, params=params, timeout=10)
        r.raise_for_status()
        bars = r.json().get("bars") or []
        if not bars:
            return None
        df = pd.DataFrame(bars)
        df.rename(columns={"o": "open", "h": "high", "l": "low",
                            "c": "close", "v": "volume", "t": "time"}, inplace=True)
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)
        return df[["open", "high", "low", "close", "volume"]].astype(float)
    except Exception as e:
        log.debug(f"fetch_premarket_bars {symbol}: {e}")
        return None


# ── Trailing Stop Manager ───────────────────────────────────────────────────────

def get_open_orders_for_symbol(symbol: str) -> list:
    r = requests.get(
        f"{BASE_URL}/v2/orders", headers=HEADERS,
        params={"status": "open", "symbols": symbol, "limit": 20},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def patch_order_stop(order_id: str, stop_price: float):
    r = requests.patch(
        f"{BASE_URL}/v2/orders/{order_id}", headers=HEADERS,
        json={"stop_price": str(round(stop_price, 2))},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def _update_stop(symbol: str, new_stop: float, phase: str):
    """Find the open sell-stop child order for symbol and patch its stop price."""
    try:
        orders = get_open_orders_for_symbol(symbol)
        stop_orders = [
            o for o in orders
            if o.get("type") in ("stop", "stop_limit") and o.get("side") == "sell"
        ]
        if not stop_orders:
            log.debug(f"Trailing [{phase}] {symbol}: no open stop order found")
            return
        stop_order = sorted(stop_orders, key=lambda o: o.get("created_at", ""), reverse=True)[0]
        patch_order_stop(stop_order["id"], new_stop)
        log.info(f"Trailing stop [{phase.upper()}] {symbol}: stop → ${new_stop:.2f}")
        broadcast("trailing_stop", {"symbol": symbol, "phase": phase, "new_stop": round(new_stop, 2)})
    except Exception as e:
        log.warning(f"_update_stop {symbol} [{phase}]: {e}")


def _run_trailing_check():
    global daily_loss_total, trading_halted
    try:
        positions = get_positions()
    except Exception:
        return
    if not positions:
        return
    journal = {r["symbol"]: r for r in get_journal_entries() if r.get("status") == "open"}

    # ── Auto-exit: EOD close at 3:55 PM ET ───────────────────────────────────
    now_et    = datetime.now(ET_TZ)
    eod_close = now_et.hour == 15 and now_et.minute >= 55

    for pos in positions:
        sym    = pos.get("symbol", "")
        qty    = abs(int(pos.get("qty", 0)))
        entry  = float(pos.get("avg_entry_price", 0))
        curr   = float(pos.get("current_price") or entry)
        unreal = float(pos.get("unrealized_pl") or 0)

        if pos.get("side", "long") != "long" or qty == 0 or entry == 0:
            continue

        jrow = journal.get(sym)
        if jrow and jrow.get("stop_price"):
            stop_orig      = float(jrow["stop_price"])
            risk_per_share = entry - stop_orig
            atr_est        = float(jrow.get("atr") or 0) or (risk_per_share / 1.5)
        else:
            risk_per_share = entry * 0.02
            atr_est        = risk_per_share / 1.5

        if risk_per_share <= 0:
            continue

        risk_dollars = risk_per_share * qty

        # ── Auto-exit: EOD 3:55 PM ET ─────────────────────────────────────
        if eod_close:
            log.info(f"Auto-exit EOD: {sym} qty={qty} curr=${curr:.2f}")
            try:
                submit_market_sell(sym, qty)
                _log_auto_exit(sym, curr, "EOD 3:55 PM ET", qty)
                broadcast("auto_exit", {"symbol": sym, "reason": "EOD", "price": round(curr, 2)})
                _trailing_status.pop(sym, None)
                if unreal < 0:
                    with daily_risk_lock:
                        daily_loss_total += abs(unreal)
                        if daily_loss_total >= DAILY_MAX_LOSS and not trading_halted:
                            trading_halted = True
                            broadcast("trading_halted", {"type": "trading_halted", "reason": "daily_loss_limit"})
                            log.info(f"Trading HALTED: daily loss ${daily_loss_total:.2f} >= ${DAILY_MAX_LOSS:.2f}")
            except Exception as e:
                log.warning(f"Auto-exit EOD {sym}: {e}")
            continue

        # ── Auto-exit: catastrophic stop 3×risk ──────────────────────────
        if unreal <= -3 * risk_dollars:
            log.info(f"Auto-exit catastrophic: {sym} PnL={unreal:.2f} < -3×risk={-3*risk_dollars:.2f}")
            try:
                submit_market_sell(sym, qty)
                _log_auto_exit(sym, curr, "catastrophic 3×risk stop", qty)
                broadcast("auto_exit", {"symbol": sym, "reason": "catastrophic_stop", "price": round(curr, 2)})
                _trailing_status.pop(sym, None)
                if unreal < 0:
                    with daily_risk_lock:
                        daily_loss_total += abs(unreal)
                        if daily_loss_total >= DAILY_MAX_LOSS and not trading_halted:
                            trading_halted = True
                            broadcast("trading_halted", {"type": "trading_halted", "reason": "daily_loss_limit"})
                            log.info(f"Trading HALTED: daily loss ${daily_loss_total:.2f} >= ${DAILY_MAX_LOSS:.2f}")
            except Exception as e:
                log.warning(f"Auto-exit catastrophic {sym}: {e}")
            continue

        ts_now    = _trailing_status.get(sym, {})
        phase_now = ts_now.get("phase")
        stop_now  = ts_now.get("stop", 0)

        # Phase TRAIL: PnL >= 2×risk — trail 1.5×ATR below current
        if unreal >= 2 * risk_dollars:
            new_stop = curr - 1.5 * atr_est
            if new_stop > stop_now + 0.01 and new_stop > entry:
                _update_stop(sym, new_stop, "trail")
                _trailing_status[sym] = {"phase": "trail", "stop": new_stop}

        # Phase BE: PnL >= 1×risk — move stop to breakeven
        elif unreal >= risk_dollars and phase_now not in ("be", "trail"):
            _update_stop(sym, entry, "be")
            _trailing_status[sym] = {"phase": "be", "stop": entry}


def trailing_stop_manager():
    """Daemon thread: checks all open positions every 30 s for trailing stop adjustments."""
    global daily_loss_total, daily_trade_count, trading_halted, _last_risk_reset_date
    log.info("Trailing stop manager started")
    while True:
        # ── Midnight ET reset of daily risk counters ──────────────────────
        today_et = datetime.now(ET_TZ).strftime("%Y-%m-%d")
        if today_et != _last_risk_reset_date:
            with daily_risk_lock:
                daily_loss_total  = 0.0
                daily_trade_count = 0
                trading_halted    = False
            _last_risk_reset_date = today_et
            log.info(f"Daily risk counters reset for {today_et}")
        try:
            _run_trailing_check()
        except Exception as e:
            log.warning(f"Trailing stop check error: {e}")
        time.sleep(30)


# ── News Feed ──────────────────────────────────────────────────────────────────

def fetch_news(symbol: str) -> list:
    """Return up to 5 recent headlines for symbol via Alpaca v1beta1/news. Cached 5 min."""
    now = time.time()
    if symbol in _news_cache:
        headlines, ts = _news_cache[symbol]
        if now - ts < NEWS_CACHE_TTL:
            return headlines
    try:
        r = requests.get(
            f"{DATA_URL}/v1beta1/news",
            headers=HEADERS,
            params={"symbols": symbol, "limit": 5, "sort": "desc"},
            timeout=10,
        )
        r.raise_for_status()
        articles = r.json().get("news", [])
        headlines = [
            {
                "headline":   a.get("headline", ""),
                "url":        a.get("url", ""),
                "source":     a.get("source", ""),
                "created_at": a.get("created_at", ""),
                "summary":    (a.get("summary") or "")[:120],
            }
            for a in articles[:5]
        ]
    except Exception as e:
        log.debug(f"fetch_news {symbol}: {e}")
        headlines = []
    _news_cache[symbol] = (headlines, now)
    return headlines


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

def check_criteria(symbol: str, df: pd.DataFrame, equity: float,
                   prev_close: float = None, today_open: float = None):
    """
    Apply all 5 criteria + Warrior Trading filters to a bar DataFrame.
    Returns alert dict if ALL pass, else None.
    New params (optional — gracefully skipped if None):
      prev_close : yesterday's daily close (for gain% and gap%)
      today_open : today's daily open (for gap%)
    """
    def _dbg(msg):
        log.debug("[%s] %s", symbol, msg)

    if len(df) < 60:
        _dbg(f"SKIP: only {len(df)} bars")
        return None

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]
    open_  = df["open"]

    last_close = close.iloc[-1]

    # ── Daily gain filter (+10% vs prev close) ────────────────────────────────
    if prev_close is not None and prev_close > 0:
        pct_change_today = (last_close - prev_close) / prev_close * 100
        if pct_change_today < MIN_GAIN_PCT:
            _dbg(f"GAIN FAIL: {pct_change_today:.1f}% < {MIN_GAIN_PCT}%")
            return None
    else:
        pct_change_today = 0.0

    # ── Gap % (today's open vs prev close) ────────────────────────────────────
    if prev_close is not None and prev_close > 0 and today_open is not None:
        gap_pct = (today_open - prev_close) / prev_close * 100
    else:
        gap_pct = 0.0

    # ── Indicators ────────────────────────────────────────────────────────────
    ema20_s = ta.trend.EMAIndicator(close, window=20).ema_indicator()
    ema50_s = ta.trend.EMAIndicator(close, window=50).ema_indicator()
    atr_s   = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()

    ema20 = ema20_s.iloc[-1]
    ema50 = ema50_s.iloc[-1]
    atr   = atr_s.iloc[-1]

    if any(pd.isna(x) for x in [ema20, ema50, atr]) or atr <= 0:
        _dbg("SKIP: NaN indicators")
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 1 — Trend / Market Structure
    # ─────────────────────────────────────────────────────────────────────────
    if not (last_close > ema20 > ema50):
        _dbg(f"C1 FAIL: trend not aligned "
             f"({last_close:.2f} > {ema20:.2f} > {ema50:.2f} = False)")
        return None

    h_arr = high.values
    l_arr = low.values

    sh_idxs = find_local_maxima(h_arr)
    sl_idxs = find_local_minima(l_arr)

    if len(sh_idxs) < 2 or len(sl_idxs) < 2:
        _dbg(f"C1 FAIL: insufficient swings (sh={len(sh_idxs)} sl={len(sl_idxs)})")
        return None

    sh_vals = [h_arr[i] for i in sh_idxs]
    sl_vals = [l_arr[i] for i in sl_idxs]

    if sh_vals[-1] <= sh_vals[-2]:
        _dbg(f"C1 FAIL: no HH ({sh_vals[-2]:.2f}->{sh_vals[-1]:.2f})")
        return None

    if sl_vals[-1] <= sl_vals[-2]:
        _dbg(f"C1 FAIL: no HL ({sl_vals[-2]:.2f}->{sl_vals[-1]:.2f})")
        return None

    if close.iloc[-5:].min() < sl_vals[-1]:
        _dbg(f"C1 FAIL: pullback closed below swing low {sl_vals[-1]:.2f}")
        return None

    prior_sh = sh_vals[-2]
    if last_close > prior_sh:
        if close.iloc[-4:-1].min() < prior_sh:
            _dbg(f"C1 FAIL: collapsed back below prior swing high {prior_sh:.2f}")
            return None

    trend_note = (
        f"Price ${last_close:.2f} > EMA20 ${ema20:.2f} > EMA50 ${ema50:.2f} | "
        f"HH ${sh_vals[-2]:.2f}\u2192${sh_vals[-1]:.2f} | "
        f"HL ${sl_vals[-2]:.2f}\u2192${sl_vals[-1]:.2f}"
    )

    # ─────────────────────────────────────────────────────────────────────────

    # ── Relative Strength Check (cached heatmap — no live fetch) ──────────────────────────────
    # Skip if SPY down >0.5% AND average sector ETF is also negative.
    # Gracefully skipped when _heatmap_cache is empty or stale.
    try:
        if _heatmap_cache:
            _spy_pct = _heatmap_cache.get("SPY")
            if _spy_pct is not None and _spy_pct < -0.5:
                _sect_vals = [v for k, v in _heatmap_cache.items() if k != "SPY"]
                if _sect_vals and (sum(_sect_vals) / len(_sect_vals)) < 0:
                    _dbg(f"RS FAIL: SPY {_spy_pct:.2f}% and avg sector negative — weak tape")
                    return None
    except Exception:
        pass  # never block a signal on cache errors

    # Criterion 2 — Entry Trigger (ONE must pass)
    #   A) Breakout: close > prior 10-bar high with bullish candle
    #   B) EMA20 pullback: recent bar touched EMA20, current bar bullish
    #   C) Pivot reclaim: dipped below prior swing high, now closes back above
    #   D) Gap and Go: gap ≥5%, close > PM high -OR- PM bull flag breakout
    # ─────────────────────────────────────────────────────────────────────────
    last_bar   = df.iloc[-1]
    prev_bar   = df.iloc[-2]
    entry_type = None
    entry_note = None

    # A — Breakout
    prior_10_high = high.iloc[-11:-1].max()
    is_breakout   = last_close > prior_10_high * 1.001
    is_bullish    = (last_close > last_bar["open"]) or (last_close > prev_bar["close"] * 1.002)
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
        pivot     = sh_vals[-2]
        dipped    = low.iloc[-6:-1].min() < pivot
        reclaimed = last_close > pivot and last_close > last_bar["open"]
        if dipped and reclaimed:
            entry_type = "Pivot Reclaim"
            entry_note = (
                f"Dipped below pivot ${pivot:.2f}, "
                f"closed back above \u2192 ${last_close:.2f}"
            )

    # D — Gap and Go (requires gap ≥10% and premarket data)
    pm_high   = None   # track premarket high for extended check later
    extended  = False  # True if price >2% above PM high (chasing)
    if entry_type is None and gap_pct >= GAP_MIN:
        pm_df = fetch_premarket_bars(symbol)
        if pm_df is not None and len(pm_df) >= 3:
            pm_high = pm_df["high"].max()
            # PM High Breakout: current close > premarket high
            if last_close > pm_high * 1.001:
                entry_type = "Gap and Go — PM High Breakout"
                entry_note = (
                    f"Gap {gap_pct:.1f}% | PM high ${pm_high:.2f}, "
                    f"close ${last_close:.2f} broke above"
                )
            # PM Bull Flag: 3 tight consolidation bars (<2% range) then breakout
            if entry_type is None and len(pm_df) >= 4:
                consol_3   = pm_df.iloc[-4:-1]
                bar_ranges = (consol_3["high"] - consol_3["low"]) / consol_3["close"]
                if (bar_ranges < 0.02).all():
                    consol_high = consol_3["high"].max()
                    if last_close > consol_high:
                        entry_type = "Gap and Go — PM Bull Flag"
                        entry_note = (
                            f"Gap {gap_pct:.1f}% | PM bull flag (<2% range), "
                            f"breakout close ${last_close:.2f} > ${consol_high:.2f}"
                        )
    # Clean breakout check: if PM high known, flag entries >2% above it as extended
    if pm_high is not None:
        if last_close > pm_high * 1.02:
            extended = True
            _dbg(f"EXTENDED: price ${last_close:.2f} is >{((last_close/pm_high)-1)*100:.1f}% above PM high ${pm_high:.2f}")
        elif last_close < pm_high * 0.997 and entry_type and entry_type.startswith("Gap and Go"):
            # Price hasn’t yet cleanly broken above PM high — not yet a valid breakout
            _dbg(f"PM HIGH BREAKOUT NOT CLEAN: close ${last_close:.2f} < PM high ${pm_high:.2f}")

    if entry_type is None:
        _dbg("C2 FAIL: no entry trigger (breakout / pullback / pivot / gap-and-go)")
        return None

    entry_price = last_close

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 3 — Support & R / Invalidation
    # ─────────────────────────────────────────────────────────────────────────
    support_level = None

    sl_candidates = [v for v in sl_vals if v < entry_price and (entry_price - v) / entry_price < 0.08]
    if sl_candidates:
        support_level = max(sl_candidates)

    if support_level is None:
        for v in reversed(sh_vals[:-1]):
            if v < entry_price and (entry_price - v) / entry_price < 0.06:
                support_level = v
                break

    if support_level is None:
        _dbg(f"C3 FAIL: no support level within 8% below {entry_price:.2f}")
        return None

    if close.iloc[-5:].min() < support_level:
        _dbg(f"C3 FAIL: closed below support {support_level:.2f}")
        return None

    pct_below = (entry_price - support_level) / entry_price * 100
    if pct_below > MAX_STOP_PCT:
        _dbg(f"C3 FAIL: stop distance {pct_below:.1f}% > MAX_STOP_PCT {MAX_STOP_PCT}% — risk too large")
        return None
    sr_note = (
        f"Support ${support_level:.2f} ({pct_below:.1f}% below entry) | "
        f"No close violation in last 5 bars"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Criterion 4 — Risk Management
    #   • Stop just below support; target = max(3×ATR, 2×risk) above entry
    #   • Position sized to 1% account risk
    # ─────────────────────────────────────────────────────────────────────────
    stop = support_level * 0.998
    risk = entry_price - stop

    if risk <= 0:
        _dbg(f"C4 FAIL: risk <= 0 (entry={entry_price:.2f} stop={stop:.2f})")
        return None

    target       = entry_price + max(ATR_TARGET_MULT * atr, MIN_RR * risk)
    reward       = target - entry_price
    rr           = reward / risk
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
    #   • RVOL ≥ 5× 20-bar average
    #   • Current bar volume > prior 5-bar average (expanding)
    #   • Estimated daily volume ≥ MIN_DAILY_VOL
    #   • Float filter: RVOL≥10 → <20M shares; RVOL 5-10 → <50M shares
    # ─────────────────────────────────────────────────────────────────────────
    avg_vol_20 = volume.iloc[-21:-1].mean()
    avg_vol_5  = volume.iloc[-6:-1].mean()
    cur_vol    = volume.iloc[-1]
    rvol       = cur_vol / avg_vol_20 if avg_vol_20 > 0 else 0
    est_daily  = avg_vol_20 * 78        # 78 five-min bars per 6.5-hr session

    if rvol < RVOL_MIN:
        _dbg(f"C5 FAIL: RVOL {rvol:.2f}x < {RVOL_MIN}x")
        return None
    if cur_vol < avg_vol_5:
        _dbg(f"C5 FAIL: cur_vol {cur_vol:,.0f} < 5-bar avg {avg_vol_5:,.0f}")
        return None
    if est_daily < MIN_DAILY_VOL:
        _dbg(f"C5 FAIL: est_daily {est_daily:,.0f} < {MIN_DAILY_VOL:,}")
        return None

    # Float filter (on-demand, cached)
    float_shares = fetch_asset_float(symbol)
    if float_shares is not None:
        if rvol > 15 and float_shares > 15_000_000:
            _dbg(f"FLOAT FAIL: RVOL {rvol:.1f}x (>15x) requires <15M float, got {float_shares/1e6:.1f}M")
            return None
        elif 8 <= rvol <= 15 and float_shares > 30_000_000:
            _dbg(f"FLOAT FAIL: RVOL {rvol:.1f}x (8-15x) requires <30M float, got {float_shares/1e6:.1f}M")
            return None

    expanding    = cur_vol > avg_vol_5 * 1.2
    expand_label = "Expanding \u2191" if expanding else "Rising"
    float_label  = f" | Float {float_shares/1e6:.1f}M" if float_shares else ""
    vol_note = (
        f"RVOL {rvol:.1f}\u00d7 | Bar {cur_vol:,.0f} vs 5-bar avg {avg_vol_5:,.0f} | "
        f"Est daily {est_daily:,.0f} | {expand_label}{float_label}"
    )

    # ── Price range filter ($2–$20): below $2 too manipulated, above $20 moves shrink ────────────────
    if not (PRICE_MIN <= entry_price <= PRICE_MAX):
        _dbg(f"PRICE FAIL: {entry_price:.2f} not in [${PRICE_MIN}, ${PRICE_MAX}]")
        return None

    # ── Time-of-day flag (7–11 AM ET is prime; alerts outside are flagged) ───
    # Gap-and-Go prime window: 9:30–11:00 AM ET (first 90 min of regular session)
    now_et        = datetime.now(ET_TZ)
    hour_et       = now_et.hour + now_et.minute / 60.0
    outside_prime = not (7.0 <= hour_et <= 11.0)
    is_gap_and_go = entry_type is not None and entry_type.startswith("Gap and Go")
    prime_window  = not (is_gap_and_go and hour_et > 11.0)

    _dbg(f"ALL PASS: {entry_type}  R:R {rr:.1f}  RVOL {rvol:.1f}x  "
         f"gap {gap_pct:.1f}%  chg {pct_change_today:.1f}%"
         + ("  [OUTSIDE PRIME]" if outside_prime else ""))

    # ── Multi-Timeframe Confirmation (non-blocking — still emits alert) ───────
    mtf_confirmed = False
    mtf_note      = "MTF check skipped"
    try:
        df15 = fetch_mtf_bars(symbol, "15Min", days_back=5,  limit=50)
        df1h = fetch_mtf_bars(symbol, "1Hour", days_back=10, limit=50)
        ok15 = ok1h = False
        notes15 = notes1h = "no data"
        if df15 is not None and len(df15) >= 20:
            e9_15  = ta.trend.EMAIndicator(df15["close"], window=9).ema_indicator().iloc[-1]
            e20_15 = ta.trend.EMAIndicator(df15["close"], window=20).ema_indicator().iloc[-1]
            ok15   = bool(e9_15 > e20_15)
            notes15 = f"15m EMA9 {'>' if ok15 else '<='} EMA20 (${e9_15:.2f} vs ${e20_15:.2f})"
        if df1h is not None and len(df1h) >= 20:
            e9_1h  = ta.trend.EMAIndicator(df1h["close"], window=9).ema_indicator().iloc[-1]
            e20_1h = ta.trend.EMAIndicator(df1h["close"], window=20).ema_indicator().iloc[-1]
            ok1h   = bool(e9_1h > e20_1h)
            notes1h = f"1h EMA9 {'>' if ok1h else '<='} EMA20 (${e9_1h:.2f} vs ${e20_1h:.2f})"
        mtf_confirmed = ok15 and ok1h
        mtf_note      = notes15 + " | " + notes1h
    except Exception as e:
        mtf_note = f"MTF error: {e}"

    return {
        "symbol":            symbol,
        "entry":             round(entry_price, 2),
        "stop":              round(stop, 2),
        "target":            round(target, 2),
        "rr":                round(rr, 2),
        "qty":               qty,
        "atr":               round(atr, 2),
        "rvol":              round(rvol, 2),
        "gap_pct":           round(gap_pct, 2),
        "pct_change_today":  round(pct_change_today, 2),
        "outside_prime":     outside_prime,
        "prime_window":      prime_window,
        "extended":          extended,
        "entry_type":        entry_type,
        "mtf_confirmed":     mtf_confirmed,
        "mtf_note":          mtf_note,
        "float_shares":      float_shares,
        "criteria": {
            "trend":  trend_note,
            "entry":  entry_note,
            "sr":     sr_note,
            "risk":   risk_note,
            "volume": vol_note,
        },
        "timestamp": datetime.now().isoformat(),
    }


# ── Watchlist Scan Loop ────────────────────────────────────────────────────────

def watchlist_scan_loop():
    """Dedicated 60-second loop scanning only user-pinned watchlist symbols."""
    log.info("Watchlist scan loop started")
    while True:
        time.sleep(60)
        symbols = load_watchlist()
        if not symbols:
            continue
        try:
            account = get_account()
            equity  = float(account.get("equity", 100_000))
        except Exception:
            equity = 100_000

        found = []
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            try:
                bars_map = fetch_bars_batch(batch, limit=LOOKBACK, days_back=7)
            except Exception as e:
                log.warning(f"watchlist fetch_bars error: {e}")
                continue
            for sym in batch:
                df = bars_map.get(sym)
                if df is None or len(df) < 60:
                    continue
                try:
                    alert = check_criteria(sym, df, equity)
                    if alert:
                        alert["source"] = "watchlist"
                        found.append(alert)
                        broadcast("watchlist_alert", alert)
                        log.info(f"[watchlist] alert: {sym} {alert['entry_type']}")
                except Exception as e:
                    log.debug(f"[watchlist] {sym}: {e}")

        with watchlist_lock:
            watchlist_alerts.clear()
            watchlist_alerts.extend(found)


# ── Pre-Market Gapper Scanner ─────────────────────────────────────────────────

def premarket_scan_loop():
    """Daemon thread: scans for pre-market gappers between 4:00–9:30 AM ET."""
    log.info("Pre-market gapper scan loop started")
    while True:
        now_et = datetime.now(ET_TZ)
        # Only run between 4:00 AM and 9:30 AM ET
        start_window = now_et.replace(hour=4,  minute=0,  second=0, microsecond=0)
        end_window   = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)

        if start_window <= now_et < end_window:
            try:
                _run_premarket_scan()
            except Exception as e:
                log.warning(f"premarket_scan_loop error: {e}")
            time.sleep(300)  # 5 minutes between scans
        else:
            # Sleep until 4:00 AM next window or just idle
            time.sleep(60)


def _run_premarket_scan():
    """Fetch yesterday's close and today's 1-min PM bars; store qualifying gappers."""
    watchlist = load_watchlist()
    symbols   = list(set(list(CURATED_SYMBOLS) + watchlist))

    # Fetch yesterday's daily close for all symbols
    daily_data = {}
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        daily_data.update(fetch_daily_bars_batch(batch))

    found = []
    now_et   = datetime.now(ET_TZ)
    start_et = now_et.replace(hour=4, minute=0, second=0, microsecond=0)

    for sym in symbols:
        daily = daily_data.get(sym)
        if not daily or not daily.get("prev_close"):
            continue
        prev_close = daily["prev_close"]
        if prev_close <= 0:
            continue

        # Fetch 1-Min bars from 4:00 AM ET today (extended hours)
        fetch_end = now_et
        if fetch_end <= start_et:
            continue
        params = {
            "timeframe": "1Min",
            "start":     start_et.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end":       fetch_end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "feed":      "sip",
            "sort":      "asc",
            "limit":     1000,
        }
        try:
            r = requests.get(f"{DATA_URL}/v2/stocks/{sym}/bars",
                             headers=HEADERS, params=params, timeout=10)
            r.raise_for_status()
            bars = r.json().get("bars") or []
        except Exception as e:
            log.debug(f"premarket bars {sym}: {e}")
            continue

        if not bars:
            continue

        pm_price  = float(bars[-1]["c"])
        pm_volume = sum(float(b.get("v", 0)) for b in bars)
        gap_pct   = (pm_price - prev_close) / prev_close * 100

        if gap_pct >= 5.0 and pm_volume >= 50_000:
            found.append({
                "symbol":     sym,
                "prev_close": round(prev_close, 2),
                "pm_price":   round(pm_price, 2),
                "gap_pct":    round(gap_pct, 2),
                "pm_volume":  int(pm_volume),
                "timestamp":  datetime.now().isoformat(),
            })

    found.sort(key=lambda x: x["gap_pct"], reverse=True)
    with premarket_lock:
        premarket_gappers.clear()
        premarket_gappers.extend(found)
    log.info(f"Pre-market scan: {len(found)} gappers from {len(symbols)} symbols")


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
    """
    2-pass scanner:
      Pass 1 — Fast RVOL screen over full symbol universe using daily bars only.
               Populates daily_watchlist (resets per trading day at 4 AM ET).
      Pass 2 — Full check_criteria() only over daily_watchlist symbols.
               Only these symbols get the expensive 5-min bar / premarket / float calls.
    """
    global daily_watchlist, daily_watchlist_date

    log.info("Starting 2-pass market scan ...")
    broadcast("status", {"status": "scanning", "message": "Initialising scan..."})

    try:
        account = get_account()
        equity  = float(account.get("equity", 100_000))
    except Exception:
        equity = 100_000
        log.warning("Could not fetch account equity; defaulting to $100,000")

    # ── Determine today's trading-day key (resets at 4 AM ET) ────────────────
    now_et    = datetime.now(ET_TZ)
    today_key = now_et.strftime("%Y-%m-%d") if now_et.hour >= 4 else (
        (now_et - timedelta(days=1)).strftime("%Y-%m-%d"))

    with daily_watchlist_lock:
        needs_refresh = (daily_watchlist_date != today_key) or (len(daily_watchlist) == 0)

    # ── PASS 1 — Fast RVOL screen (daily bars only) ───────────────────────────
    if needs_refresh:
        broadcast("status", {"status": "scanning",
                              "message": "Pass 1: RVOL screening full symbol universe..."})
        try:
            universe = get_all_symbols()
        except Exception as e:
            log.warning(f"get_all_symbols failed, falling back to CURATED_SYMBOLS: {e}")
            universe = list(CURATED_SYMBOLS)

        # Always include user watchlist symbols in the universe
        universe = list(set(universe + load_watchlist()))
        log.info(f"Pass 1: screening {len(universe)} symbols for RVOL >= {RVOL_MIN}x "
                 f"using daily bars (batch size {PASS1_BATCH_SIZE}) ...")

        p1_results: dict = {}
        p1_batches = [universe[i:i + PASS1_BATCH_SIZE]
                      for i in range(0, len(universe), PASS1_BATCH_SIZE)]

        for b_idx, batch in enumerate(p1_batches):
            with state_lock:
                if not state["running"]:
                    return
            rvol_map = fetch_daily_rvol_batch(batch)
            for sym, rvol in rvol_map.items():
                if rvol >= RVOL_MIN:
                    p1_results[sym] = rvol
            time.sleep(0.1)   # light throttle between batches

        log.info(f"Pass 1 complete: {len(p1_results)} / {len(universe)} symbols passed "
                 f"RVOL >= {RVOL_MIN}x")

        with daily_watchlist_lock:
            daily_watchlist      = dict(p1_results)
            daily_watchlist_date = today_key

        broadcast("daily_watchlist_update", {"count": len(p1_results), "date": today_key})
    else:
        with daily_watchlist_lock:
            p1_results = dict(daily_watchlist)
        log.info(f"Pass 1 skipped — daily_watchlist already fresh for {today_key} "
                 f"({len(p1_results)} symbols)")

    # ── PASS 2 — Full criteria check (watchlist symbols only) ─────────────────
    with daily_watchlist_lock:
        pass2_symbols = list(daily_watchlist.keys())

    total = len(pass2_symbols)
    if total == 0:
        log.info("Pass 2 skipped — daily_watchlist is empty")
        return

    log.info(f"Pass 2: full criteria check on {total} watchlist symbols ...")
    broadcast("status", {"status": "scanning",
                         "message": f"Pass 2: checking {total} watchlist symbols..."})

    with state_lock:
        state["total"]      = total
        state["scanned"]    = 0
        state["start_time"] = time.time()
        state["status"]     = "scanning"

    broadcast("progress", {"scanned": 0, "total": total, "eta": 0, "alerts": 0})

    new_alerts   = []
    gapper_count = 0
    batches      = [pass2_symbols[i:i + BATCH_SIZE]
                    for i in range(0, total, BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        with state_lock:
            if not state["running"]:
                break

        bars_map  = fetch_bars_batch(batch)
        daily_map = fetch_daily_bars_batch(batch)

        for sym, df in bars_map.items():
            daily      = daily_map.get(sym, {})
            prev_close = daily.get("prev_close")
            today_open = daily.get("today_open")

            # Track gapper count independently of full criteria pass
            if prev_close and prev_close > 0 and len(df) >= 21:
                lc     = float(df["close"].iloc[-1])
                gain_q = (lc - prev_close) / prev_close * 100
                avg_v  = df["volume"].iloc[-21:-1].mean()
                rv_q   = float(df["volume"].iloc[-1]) / avg_v if avg_v > 0 else 0
                if rv_q >= RVOL_MIN and gain_q >= MIN_GAIN_PCT:
                    gapper_count += 1

            try:
                alert = check_criteria(sym, df, equity,
                                       prev_close=prev_close, today_open=today_open)
            except Exception as e:
                log.debug(f"Error analyzing {sym}: {e}")
                alert = None

            if alert:
                new_alerts.append(alert)
                broadcast("alert", alert)
                log.info(
                    f"ALERT  {sym}  {alert['entry_type']}  R:R {alert['rr']}:1  "
                    f"RVOL {alert['rvol']}x  gap {alert['gap_pct']}%  "
                    f"chg {alert['pct_change_today']}%"
                    + ("  [OUTSIDE PRIME]" if alert.get("outside_prime") else "")
                )

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
        broadcast("progress", {"scanned": scanned, "total": total, "eta": eta,
                                "alerts": len(new_alerts)})
        time.sleep(0.04)

    cycle = "hot" if gapper_count >= 5 else ("cold" if gapper_count <= 2 else "neutral")

    with state_lock:
        existing_syms = {a["symbol"] for a in new_alerts}
        merged = new_alerts + [a for a in state["alerts"] if a["symbol"] not in existing_syms]
        state["alerts"]       = merged[:200]
        state["scanned"]      = total
        state["last_scan"]    = datetime.now().isoformat()
        state["gapper_count"] = gapper_count
        state["market_cycle"] = cycle

    broadcast("scan_complete", {
        "total": total, "alerts": len(new_alerts), "timestamp": state["last_scan"]
    })
    broadcast("market_cycle", {"cycle": cycle, "gapper_count": gapper_count})
    log.info(
        f"Scan complete — Pass 1: {len(p1_results)} symbols in daily_watchlist | "
        f"Pass 2: {len(new_alerts)} alerts from {total} watchlist symbols | "
        f"gappers={gapper_count} cycle={cycle}"
    )


# ── Backtest Engine ────────────────────────────────────────────────────────────

def backtest_symbol(sym: str, df: pd.DataFrame, equity: float = 100_000,
                    run_id: str = None, run_ts: str = None) -> list:
    """
    Walk-forward backtest for a single symbol.
    Enters at next-bar open after signal fires; simulates up to 30 bars forward.
    Records entry_dt, exit_dt, hold_bars, pnl_dollar per trade.
    Optionally persists each trade to backtest_results table if run_id is provided.
    """
    trades   = []
    warmup   = 65    # bars before we start checking
    max_hold = 30    # max bars in a simulated trade

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

        # Target: ATR_TARGET_MULT from actual entry
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

        pnl_r       = (exit_price - entry_price) / risk
        hold_bars   = exit_bar - i
        qty_est     = max(1, math.floor(equity * ACCOUNT_RISK_PCT / max(risk, 0.01)))
        pnl_dollar  = round((exit_price - entry_price) * qty_est, 2)

        # Safe datetime extraction
        try:
            entry_dt = str(df.index[i])
            exit_dt  = str(df.index[exit_bar])
        except Exception:
            entry_dt = exit_dt = None

        trade = {
            "symbol":     sym,
            "entry_type": alert["entry_type"],
            "entry":      round(entry_price, 2),
            "exit":       round(exit_price, 2),
            "stop":       round(stop, 2),
            "target":     round(target, 2),
            "rr":         round(rr, 2),
            "outcome":    outcome,
            "pnl_r":      round(pnl_r, 2),
            "pnl_dollar": pnl_dollar,
            "hold_bars":  hold_bars,
            "entry_dt":   entry_dt,
            "exit_dt":    exit_dt,
        }
        trades.append(trade)

        if run_id:
            try:
                save_bt_result(run_id, trade, run_ts or datetime.now().isoformat())
            except Exception as dbe:
                log.debug(f"save_bt_result error {sym}: {dbe}")

        i = exit_bar + 1  # skip past completed trade

    return trades


def run_enhanced_backtest(run_id: str = None):
    """
    Background thread: 10-pass walk-forward backtest on CURATED_SYMBOLS.
    Each pass = one complete scan of all curated symbols.
    Results persist to backtest_results table; live state in backtest_state.
    """
    import uuid
    if run_id is None:
        run_id = str(uuid.uuid4())[:8]
    run_ts = datetime.now().isoformat()

    try:
        with bt_lock:
            backtest_state.update({
                "running":   True,
                "status":    "fetching",
                "progress":  0,
                "results":   None,
                "trades":    [],
                "error":     None,
                "run_id":    run_id,
                "runs_done": 0,
            })

        broadcast("backtest_status", {
            "status": "fetching",
            "message": f"Run {run_id}: downloading {len(CURATED_SYMBOLS)} curated symbols…",
        })

        # ── Fetch bar data ──────────────────────────────────────────────────
        all_data: dict = {}
        batches  = [CURATED_SYMBOLS[i:i + BATCH_SIZE] for i in range(0, len(CURATED_SYMBOLS), BATCH_SIZE)]
        nb       = len(batches)

        for b_idx, batch in enumerate(batches):
            # Filter to tradable symbols
            tradable = [s for s in batch
                        if not any(c in s for c in ('/', '.')) and len(s) <= 5]
            try:
                bars = fetch_bars_batch(tradable, limit=BT_LOOKBACK, days_back=60)
                all_data.update(bars)
            except Exception as e:
                log.warning(f"BT fetch error batch {b_idx}: {e}")

            pct = (b_idx + 1) / nb * 40
            with bt_lock:
                backtest_state["progress"] = pct
            broadcast("backtest_progress", {
                "progress": pct, "phase": "fetching",
                "symbols_done": min((b_idx + 1) * BATCH_SIZE, len(CURATED_SYMBOLS)),
                "total": len(CURATED_SYMBOLS),
            })
            time.sleep(0.05)

        broadcast("backtest_status", {
            "status": "simulating",
            "message": f"Simulating on {len(all_data)} symbols × 10 walk-forward passes…",
        })

        try:
            equity = float(get_account().get("equity", 100_000))
        except Exception:
            equity = 100_000

        # ── 10 walk-forward passes ──────────────────────────────────────────
        all_trades   = []
        n_passes     = 10
        total_syms   = len(all_data)

        for pass_idx in range(n_passes):
            # Each pass uses a different (shuffled) ordering to vary entry timing
            pass_syms = list(all_data.keys())
            random.shuffle(pass_syms)
            pass_run_id = f"{run_id}-p{pass_idx+1}"
            pass_ts     = datetime.now().isoformat()

            for sym_idx, sym in enumerate(pass_syms):
                df = all_data[sym]
                try:
                    trades = backtest_symbol(sym, df, equity,
                                             run_id=pass_run_id, run_ts=pass_ts)
                    all_trades.extend(trades)
                except Exception as e:
                    log.debug(f"BT sim error {sym} pass {pass_idx}: {e}")

            with bt_lock:
                backtest_state["runs_done"] = pass_idx + 1

            pct = 40 + (pass_idx + 1) / n_passes * 60
            with bt_lock:
                backtest_state["progress"] = pct
            broadcast("backtest_progress", {
                "progress": pct, "phase": "simulating",
                "symbols_done": (pass_idx + 1) * total_syms,
                "total": n_passes * total_syms,
                "trades_found": len(all_trades),
                "pass": pass_idx + 1,
                "n_passes": n_passes,
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

        # Max drawdown in R
        cum_r = 0.0; peak = 0.0; max_dd = 0.0
        for t in all_trades:
            cum_r += t["pnl_r"]
            if cum_r > peak: peak = cum_r
            dd = peak - cum_r
            if dd > max_dd: max_dd = dd

        r_vals = [t["pnl_r"] for t in all_trades]
        std    = float(np.std(r_vals)) if len(r_vals) > 1 else 0
        sharpe = round(avg_pnl / std, 2) if std > 0 else 0.0

        results = {
            "total_signals":  total_t,
            "wins":           len(wins),
            "losses":         len(losses),
            "timeouts":       len(timeouts),
            "win_rate":       round(win_rate, 1),
            "avg_rr":         round(avg_rr, 2),
            "avg_pnl_r":      round(avg_pnl, 3),
            "profit_factor":  round(float(pf), 2),
            "max_drawdown_r": round(max_dd, 2),
            "sharpe":         sharpe,
            "symbols_tested": total_syms,
            "n_passes":       n_passes,
            "run_id":         run_id,
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
        log.info(
            f"Backtest run {run_id} done: {total_t} trades / "
            f"{total_syms} syms × {n_passes} passes | "
            f"WR {win_rate:.1f}% PF {pf:.2f} Sharpe {sharpe:.2f}"
        )

    except Exception as e:
        log.error(f"Backtest job failed: {e}", exc_info=True)
        with bt_lock:
            backtest_state.update({"running": False, "status": "error", "error": str(e)})
        broadcast("backtest_error", {"message": str(e)})


def run_backtest_job(n_symbols: int = 100):
    """Legacy wrapper kept for backward compat with existing /api/backtest/start route."""
    run_enhanced_backtest()


# ── Flask Routes ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
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


def is_market_open() -> bool:
    """True if NYSE/NASDAQ regular session is approximately open (ET 9:30–16:00)."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:   # Saturday=5, Sunday=6
        return False
    # EDT: UTC-4  |  EST: UTC-5.  April is always EDT.
    et_offset = 4  # hours behind UTC
    et_hour   = (now.hour - et_offset) % 24
    et_min    = now.minute
    et_mins   = et_hour * 60 + et_min
    return 9 * 60 + 30 <= et_mins < 16 * 60


@app.route("/api/market_cycle")
def api_market_cycle():
    with state_lock:
        return jsonify({
            "cycle":         state.get("market_cycle", "neutral"),
            "gapper_count":  state.get("gapper_count", 0),
        })


@app.route("/api/market_status")
def api_market_status():
    open_ = is_market_open()
    now   = datetime.now(timezone.utc)
    return jsonify({
        "open": open_,
        "utc":  now.isoformat(),
        "msg":  "Market open" if open_ else "Market closed — showing last cached data",
    })


@app.route("/api/trade", methods=["POST"])
def api_place_trade():
    """Place a single bracket order from the confirmation modal and log it."""
    global daily_trade_count, trading_halted
    data = request.json or {}

    # ── Daily risk limit gate ─────────────────────────────────────────────
    with daily_risk_lock:
        if trading_halted:
            return jsonify({"error": "Trading halted — daily risk limit reached"}), 403
        if daily_trade_count >= DAILY_MAX_TRADES:
            trading_halted = True
            return jsonify({"error": f"Trading halted — max {DAILY_MAX_TRADES} trades reached"}), 403

    try:
        result   = submit_bracket_order(
            data["symbol"], data["qty"], data["entry"], data["stop"], data["target"]
        )
        order_id = result.get("id")
        log.info(f"Trade placed: {data['symbol']} qty={data['qty']} "
                 f"entry={data['entry']} stop={data['stop']} target={data['target']}")

        with daily_risk_lock:
            daily_trade_count += 1

        # Enrich with full alert metadata from scanner state (rvol, gap%, atr, etc.)
        with state_lock:
            alert_meta = next(
                (a for a in state["alerts"] if a["symbol"] == data["symbol"]), {}
            )
        try:
            log_trade({**alert_meta, **data}, order_id)
        except Exception as je:
            log.warning(f"Journal log failed for {data.get('symbol')}: {je}")
        return jsonify({"ok": True, "order_id": order_id,
                        "status": result.get("status", "submitted")})
    except Exception as e:
        log.warning(f"Trade placement failed ({data.get('symbol')}): {e}")
        return jsonify({"error": str(e)}), 500


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


@app.route("/api/journal")
def api_journal():
    try:
        return jsonify(get_journal_entries())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/journal/<int:trade_id>/close", methods=["POST"])
def api_journal_close(trade_id):
    data       = request.json or {}
    exit_price = float(data.get("exit_price", 0))
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM trade_journal WHERE id=?", (trade_id,)).fetchone()
        pnl = 0.0
        if row:
            ep  = row["entry_price"] or 0
            sh  = row["shares"] or 0
            pnl = round((exit_price - ep) * sh, 2)
            con.execute(
                "UPDATE trade_journal SET status='closed', exit_price=?, pnl=? WHERE id=?",
                (exit_price, pnl, trade_id),
            )
            con.commit()
        con.close()
        return jsonify({"ok": True, "pnl": pnl})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/news/<symbol>")
def api_news(symbol):
    try:
        return jsonify(fetch_news(symbol.upper()))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/quote/<symbol>")
def api_quote(symbol):
    """Return last trade price for a symbol via Alpaca latest trades (SIP feed)."""
    sym = symbol.upper()
    try:
        r = requests.get(
            f"{DATA_URL}/v2/stocks/{sym}/trades/latest",
            headers=HEADERS,
            params={"feed": "sip"},
            timeout=8,
        )
        r.raise_for_status()
        trade = r.json().get("trade", {})
        price = float(trade.get("p", 0))
        return jsonify({"symbol": sym, "price": price, "size": trade.get("s"), "ts": trade.get("t")})
    except Exception as e:
        # Fallback: try the bars endpoint for last close
        try:
            r2 = requests.get(
                f"{DATA_URL}/v2/stocks/{sym}/bars/latest",
                headers=HEADERS,
                params={"feed": "iex"},
                timeout=8,
            )
            r2.raise_for_status()
            bar = r2.json().get("bar", {})
            return jsonify({"symbol": sym, "price": float(bar.get("c", 0)), "source": "bar"})
        except Exception as e2:
            return jsonify({"error": str(e2), "symbol": sym}), 500


@app.route("/api/bars/<symbol>")
def api_bars(symbol):
    """Return recent 5-min bars for the symbol detail mini-chart."""
    sym       = symbol.upper()
    timeframe = request.args.get("timeframe", "5Min")
    limit     = min(int(request.args.get("limit", 78)), 500)
    days_back = int(request.args.get("days_back", 5))
    try:
        data = fetch_bars_batch([sym], limit=limit, days_back=days_back)
        if sym not in data:
            return jsonify([])
        df   = data[sym].reset_index()
        rows = [
            {"t": str(r["time"]), "open": r["open"], "high": r["high"],
             "low": r["low"],  "close": r["close"], "volume": r["volume"]}
            for _, r in df.iterrows()
        ]
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/trailing_status")
def api_trailing_status():
    return jsonify(_trailing_status)


@app.route("/api/risk_status")
def api_risk_status():
    with daily_risk_lock:
        return jsonify({
            "trading_halted":    trading_halted,
            "daily_loss_total":  round(daily_loss_total, 2),
            "daily_trade_count": daily_trade_count,
            "daily_max_loss":    DAILY_MAX_LOSS,
            "daily_max_trades":  DAILY_MAX_TRADES,
        })


@app.route("/api/risk_status/resume", methods=["POST"])
def api_risk_resume():
    global trading_halted
    with daily_risk_lock:
        trading_halted = False
    log.info("Trading resumed by user")
    return jsonify({"ok": True})


@app.route("/api/backtest/run", methods=["POST"])
def api_backtest_run():
    """Start an enhanced backtest run on curated volatile symbols (10 walk-forward passes)."""
    import uuid
    with bt_lock:
        if backtest_state["running"]:
            return jsonify({"error": "Backtest already running"}), 400
    run_id = str(uuid.uuid4())[:8]
    threading.Thread(
        target=run_enhanced_backtest,
        args=(run_id,),
        daemon=True,
        name=f"backtest-{run_id}",
    ).start()
    return jsonify({"ok": True, "run_id": run_id, "symbols": len(CURATED_SYMBOLS), "passes": 10})


@app.route("/api/backtest/results")
def api_backtest_results():
    """Return saved backtest trades. Optional ?run_id=X filter."""
    run_id = request.args.get("run_id")
    try:
        return jsonify(get_bt_results(run_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/backtest/summary")
def api_backtest_summary():
    """Return aggregate stats across all saved backtest runs."""
    try:
        return jsonify(get_bt_summary())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Watchlist Routes ───────────────────────────────────────────────────────────

@app.route("/api/watchlist", methods=["GET"])
def api_watchlist_get():
    syms = load_watchlist()
    with watchlist_lock:
        last = list(watchlist_alerts)
    return jsonify({"symbols": syms, "alerts": last})


@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_add():
    sym = ((request.json or {}).get("symbol") or "").upper().strip()
    if not sym:
        return jsonify({"error": "symbol required"}), 400
    syms = load_watchlist()
    if sym not in syms:
        syms.append(sym)
        save_watchlist(syms)
    return jsonify({"symbols": syms})


@app.route("/api/watchlist/<symbol>", methods=["DELETE"])
def api_watchlist_remove(symbol):
    sym  = symbol.upper().strip()
    syms = load_watchlist()
    syms = [s for s in syms if s != sym]
    save_watchlist(syms)
    return jsonify({"symbols": syms})


# ── Sector Heatmap Route ───────────────────────────────────────────────────────

@app.route("/api/sector_heatmap")
def api_sector_heatmap():
    """Return pct_change vs prior close for 11 SPDR sector ETFs + SPY (also caches into _heatmap_cache)."""
    global _heatmap_cache
    tickers = list(SECTOR_ETFS.keys()) + ["SPY"]
    start_dt = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT00:00:00Z")
    params = {
        "symbols":   ",".join(tickers),
        "timeframe": "1Day",
        "start":     start_dt,
        "limit":     500,
        "feed":      "sip",
        "sort":      "asc",
    }
    result = []
    try:
        r = requests.get(f"{DATA_URL}/v2/stocks/bars", headers=HEADERS,
                         params=params, timeout=20)
        r.raise_for_status()
        bars_map = r.json().get("bars") or {}
        new_cache: dict = {}
        for ticker, label in SECTOR_ETFS.items():
            bars = bars_map.get(ticker, [])
            if len(bars) >= 2:
                prev  = float(bars[-2].get("c", 0))
                last  = float(bars[-1].get("c", 0))
                pct   = round((last - prev) / prev * 100, 2) if prev > 0 else 0.0
            elif len(bars) == 1:
                pct  = 0.0
                last = float(bars[0].get("c", 0))
            else:
                pct = last = 0.0
            result.append({"ticker": ticker, "label": label, "pct": pct, "price": last})
            new_cache[ticker] = pct
        # Cache SPY for relative-strength check in check_criteria()
        spy_bars = bars_map.get("SPY", [])
        if len(spy_bars) >= 2:
            spy_prev = float(spy_bars[-2].get("c", 0))
            spy_last = float(spy_bars[-1].get("c", 0))
            new_cache["SPY"] = round((spy_last - spy_prev) / spy_prev * 100, 2) if spy_prev > 0 else 0.0
        elif len(spy_bars) == 1:
            new_cache["SPY"] = 0.0
        _heatmap_cache = new_cache
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── New Routes: Feature 1–4 ───────────────────────────────────────────────────

@app.route("/api/premarket_gappers")
def api_premarket_gappers():
    with premarket_lock:
        return jsonify(list(premarket_gappers))


@app.route("/api/account_summary")
def api_account_summary():
    """Return combined account + positions summary for the Positions tab upgrade."""
    try:
        acct = get_account()
        equity       = float(acct.get("equity",       0))
        buying_power = float(acct.get("buying_power", 0))
        cash         = float(acct.get("cash",         0))
        last_equity  = float(acct.get("last_equity",  equity))
        daily_pnl    = round(equity - last_equity, 2)

        raw_positions = get_positions()
        positions = []
        for p in raw_positions:
            positions.append({
                "symbol":          p.get("symbol"),
                "qty":             p.get("qty"),
                "avg_entry_price": float(p.get("avg_entry_price") or 0),
                "current_price":   float(p.get("current_price")   or 0),
                "market_value":    float(p.get("market_value")    or 0),
                "unrealized_pl":   float(p.get("unrealized_pl")   or 0),
                "unrealized_plpc": float(p.get("unrealized_plpc") or 0),
            })

        return jsonify({
            "equity":       round(equity, 2),
            "buying_power": round(buying_power, 2),
            "cash":         round(cash, 2),
            "daily_pnl":    daily_pnl,
            "positions":    positions,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/journal/export")
def api_journal_export():
    """Return all trade_journal rows as a downloadable CSV."""
    import csv, io
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute("SELECT * FROM trade_journal ORDER BY id DESC").fetchall()
        con.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Date", "Symbol", "Entry Type", "Entry $", "Stop $", "Target $",
            "Shares", "Risk $", "Reward $", "R:R", "Exit $", "P&L",
            "R-Multiple", "Exit Reason", "Float", "RVOL", "Gap %",
        ])
        for r in rows:
            entry  = r["entry_price"]  or 0
            stop   = r["stop_price"]   or 0
            target = r["target_price"] or 0
            exit_p = r["exit_price"]   or ""
            pnl    = r["pnl"]          or ""
            risk   = r["risk_dollars"] or 0
            reward = r["reward_dollars"] if "reward_dollars" in r.keys() else (
                round((target - entry) * (r["shares"] or 0), 2) if entry and target else "")
            rr     = r["rr_ratio"]     or ""
            r_mult = round((r["pnl"] / r["risk_dollars"]), 2) if (r["pnl"] and r["risk_dollars"]) else ""
            writer.writerow([
                r["timestamp"],
                r["symbol"],
                r["entry_type"]  or "",
                f"${entry:.2f}" if entry else "",
                f"${stop:.2f}"  if stop  else "",
                f"${target:.2f}" if target else "",
                r["shares"]  or "",
                f"${risk:.2f}" if risk else "",
                f"${reward:.2f}" if isinstance(reward, float) else reward,
                f"{rr:.2f}:1"  if isinstance(rr, float) else rr,
                f"${exit_p:.2f}" if isinstance(exit_p, float) and exit_p else "",
                f"${pnl:.2f}"  if isinstance(pnl, float) else pnl,
                r_mult,
                r["notes"]   or "",
                r["float_shares"] or "",
                r["rvol"]    or "",
                r["gap_pct"] or "",
            ])

        csv_bytes = output.getvalue().encode("utf-8")
        return Response(
            csv_bytes,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=journal.csv"},
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/alerts", methods=["DELETE"])
def api_alerts_clear():
    """Clear all live scanner alerts and watchlist alerts."""
    with state_lock:
        state["alerts"][:] = []
    with watchlist_lock:
        watchlist_alerts[:] = []
    return jsonify({"ok": True})


@app.route("/api/scan_status")
def api_scan_status():
    """Return last scan timestamp as unix epoch for the scanner status dot."""
    with state_lock:
        ls = state.get("last_scan")
    if ls:
        try:
            ts = datetime.fromisoformat(ls).timestamp()
        except Exception:
            ts = None
    else:
        ts = None
    return jsonify({"last_scan": ts})


@app.route("/api/daily_watchlist")
def api_daily_watchlist():
    """Return sorted list of {symbol, rvol} from the daily Pass-1 watchlist."""
    with daily_watchlist_lock:
        snapshot = dict(daily_watchlist)
        wl_date  = daily_watchlist_date
    items = sorted(
        [{"symbol": sym, "rvol": rvol} for sym, rvol in snapshot.items()],
        key=lambda x: x["rvol"], reverse=True
    )
    return jsonify({"date": wl_date, "items": items, "count": len(items)})




# ── HTML Template ──────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Market Scanner &middot; Alpaca</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      /* New design tokens */
      --bg-primary:   #0a0a12;
      --bg-secondary: #111120;
      --bg-tertiary:  #181828;
      --bg-hover:     #1e1e32;
      --border:       #252540;
      --border-light: #1c1c30;
      --text-primary: #eeeeff;
      --text-secondary: #b8b8d8;
      --text-muted:   #50507a;
      --accent:       #3d9eff;
      --accent-hover: #5aadff;
      --green:        #00e87c;
      --green-bg:     rgba(0,232,124,.1);
      --red:          #ff3d5a;
      --red-bg:       rgba(255,61,90,.1);
      --amber:        #ffd600;
      --amber-bg:     rgba(255,214,0,.1);
      --purple:       #9d5cff;
      --purple-bg:    rgba(157,92,255,.1);
      --radius-sm:    4px;
      --radius-md:    8px;
      --radius-lg:    12px;
      --shadow:       0 4px 24px rgba(0,0,0,.5);
      --transition:   all .15s ease;
      /* Legacy aliases — keep JS references working */
      --bg0:  #0a0a12;
      --bg1:  #111120;
      --bg2:  #181828;
      --bg3:  #1e1e32;
      --bg4:  #252538;
      --brd:  #252540;
      --brd2: #343460;
      --txt:  #b8b8d8;
      --dim:  #50507a;
      --dim2: #7070a0;
      --hi:   #eeeeff;
      --grn:  #00e87c;
      --grn2: #00b860;
      --grn3: rgba(0,232,124,.08);
      --red2: rgba(255,61,90,.1);
      --yel:  #ffd600;
      --yel2: rgba(255,214,0,.1);
      --blu:  #3d9eff;
      --pur:  #9d5cff;
      --pur2: rgba(157,92,255,.1);
      --cyn:  #3d9eff;
      --cyn2: rgba(61,158,255,.12);
      --mono: 'Inter', sans-serif;
      --sans: 'Inter', sans-serif;
      --r2:   4px;
      --r3:   8px;
    }

    body {
      background: var(--bg-primary);
      color: var(--text-secondary);
      font-family: 'Inter', sans-serif;
      font-size: 13px;
      min-height: 100vh;
      display: flex;
    }

    ::-webkit-scrollbar { width: 4px; height: 4px; }
    ::-webkit-scrollbar-track { background: var(--bg-primary); }
    ::-webkit-scrollbar-thumb { background: var(--bg-hover); border-radius: 2px; }

    /* ── SIDEBAR ─────────────────────────────────────────────────────────── */
    .sidebar {
      position: fixed; top: 0; left: 0; height: 100vh;
      width: 64px; background: var(--bg-secondary);
      border-right: 1px solid var(--border);
      display: flex; flex-direction: column;
      z-index: 200; overflow: hidden;
      transition: width .22s cubic-bezier(.4,0,.2,1);
    }
    .sidebar:hover { width: 200px; }
    .sidebar-logo {
      height: 56px; display: flex; align-items: center; gap: 12px;
      padding: 0 18px; border-bottom: 1px solid var(--border);
      flex-shrink: 0; overflow: hidden; white-space: nowrap;
    }
    .sidebar-logo .logo-icon { font-size: 16px; flex-shrink: 0; color: var(--green); }
    .sidebar-logo .logo-text { font-size: 11px; font-weight: 700; color: var(--green); letter-spacing: 1.5px; opacity: 0; transition: opacity .2s; white-space: nowrap; }
    .sidebar:hover .logo-text { opacity: 1; }

    .nav-items { flex: 1; padding: 8px 0; overflow: hidden; }
    .nav-item {
      display: flex; align-items: center; gap: 14px;
      padding: 11px 18px; cursor: pointer; white-space: nowrap;
      position: relative; transition: background .15s;
      border-left: 3px solid transparent;
      overflow: hidden;
    }
    .nav-item:hover { background: var(--bg-hover); }
    .nav-item.active {
      background: rgba(61,158,255,.08);
      border-left-color: var(--accent);
    }
    .nav-item .nav-icon { font-size: 15px; flex-shrink: 0; width: 22px; text-align: center; line-height: 1; }
    .nav-item .nav-label {
      font-size: 12px; font-weight: 500; color: var(--text-secondary);
      opacity: 0; transition: opacity .18s; white-space: nowrap;
    }
    .nav-item.active .nav-label { color: var(--accent); }
    .sidebar:hover .nav-label { opacity: 1; }
    .nav-item .nav-badge {
      display: none; background: var(--green); color: #000;
      font-size: 9px; font-weight: 700; padding: 1px 5px;
      border-radius: 8px; margin-left: auto; flex-shrink: 0;
    }
    .sidebar:hover .nav-item .nav-badge { display: inline; }

    .sidebar-bottom { padding: 12px 0; border-top: 1px solid var(--border); flex-shrink: 0; }

    /* ── APP SHELL ───────────────────────────────────────────────────────── */
    .app-shell {
      margin-left: 64px;
      flex: 1; display: flex; flex-direction: column;
      min-height: 100vh; min-width: 0;
    }

    /* ── HEADER ─────────────────────────────────────────────────────────── */
    header {
      position: sticky; top: 0; z-index: 100;
      background: var(--bg-secondary); border-bottom: 1px solid var(--border);
      padding: 0 20px; height: 56px;
      display: flex; align-items: center; gap: 16px;
    }
    .acct { display: flex; gap: 20px; align-items: center; flex: 1; overflow: hidden; min-width: 0; }
    .kv   { display: flex; flex-direction: column; }
    .kv .k { font-size: 9px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 1px; line-height: 1; margin-bottom: 3px; }
    .kv .v { font-size: 12px; font-weight: 600; color: var(--text-primary); line-height: 1; }
    .kv .v.g { color: var(--green); }
    .kv .v.r { color: var(--red); }

    .hacts { display: flex; gap: 8px; align-items: center; flex-shrink: 0; }

    /* ── BUTTONS ────────────────────────────────────────────────────────── */
    .btn {
      font-size: 11px; font-weight: 500; letter-spacing: .3px;
      padding: 7px 14px; border: 1px solid var(--border);
      background: transparent; color: var(--text-secondary); cursor: pointer;
      border-radius: var(--radius-sm); transition: var(--transition); white-space: nowrap;
    }
    .btn:hover   { background: var(--bg-hover); color: var(--text-primary); border-color: var(--text-muted); }
    .btn.primary { background: rgba(0,184,96,.15); border-color: var(--green); color: var(--green); font-weight: 600; }
    .btn.primary:hover { background: rgba(0,184,96,.3); box-shadow: 0 0 12px rgba(0,232,124,.2); }
    .btn.danger  { border-color: var(--red); color: var(--red); }
    .btn.danger:hover { background: var(--red-bg); }
    .btn:disabled { opacity: .35; cursor: not-allowed; }
    .btn.sm { padding: 4px 9px; font-size: 10px; }
    .btn.amber { border-color: rgba(255,159,28,.35); color: #ff9f1c; }
    .btn.amber:hover { background: rgba(255,159,28,.1); border-color: #ff9f1c; }

    .tog   { display: flex; align-items: center; gap: 8px; cursor: pointer; user-select: none; }
    .tog-t { width: 34px; height: 18px; background: var(--bg-hover); border: 1px solid var(--border); border-radius: 9px; position: relative; transition: background .2s, border-color .2s; }
    .tog-t.on { background: rgba(0,184,96,.25); border-color: var(--green); }
    .tog-k { width: 12px; height: 12px; border-radius: 50%; background: var(--text-muted); position: absolute; top: 2px; left: 2px; transition: left .2s, background .2s; }
    .tog-t.on .tog-k { left: 18px; background: var(--green); }
    .tog-l { font-size: 10px; color: var(--text-muted); letter-spacing: .5px; }

    /* ── PROGRESS BAR ───────────────────────────────────────────────────── */
    .prog-wrap { background: var(--bg-secondary); border-bottom: 1px solid var(--border); padding: 10px 20px; }
    .prog-meta { display: flex; justify-content: space-between; align-items: center; margin-bottom: 7px; font-size: 10px; color: var(--text-muted); }
    .prog-meta .hl  { color: var(--accent); }
    .prog-meta .alc { color: var(--green); }
    .prog-trk  { height: 2px; background: var(--bg-hover); border-radius: 2px; overflow: hidden; }
    .prog-fill { height: 100%; background: linear-gradient(90deg, var(--accent) 0%, #7dd3fc 100%); border-radius: 2px; width: 0%; transition: width .4s ease; }
    .prog-fill.done { background: linear-gradient(90deg, var(--grn2), var(--green)); }

    .sdot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; background: var(--text-muted); margin-right: 7px; vertical-align: middle; transition: background .3s; }
    .sdot.scan { background: var(--accent); animation: pulse 1.2s ease-in-out infinite; }
    .sdot.done { background: var(--green); }
    .sdot.stop { background: var(--red); }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)}50%{opacity:.3;transform:scale(.7)} }

    /* ── CONTENT ────────────────────────────────────────────────────────── */
    .content { padding: 16px 20px; flex: 1; }
    .panel   { display: none; }
    .panel.active { display: block; }

    /* ── ALERT CARDS ────────────────────────────────────────────────────── */
    .alerts-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); gap: 12px; }
    .alert-card {
      background: var(--bg-secondary); border: 1px solid var(--border);
      border-radius: var(--radius-md); border-left: 3px solid var(--green);
      padding: 16px; position: relative; overflow: hidden;
      animation: cardIn .3s cubic-bezier(.22,.68,0,1.2);
      transition: box-shadow .2s;
    }
    .alert-card:hover { box-shadow: var(--shadow); }
    .alert-card.breakout { border-left-color: var(--accent); }
    .alert-card.pullback { border-left-color: var(--purple); }
    .alert-card.pivot    { border-left-color: var(--amber); }
    .alert-card.gap_go   { border-left-color: var(--green); }
    .alert-card.watchlist { border-left-color: #ff9f1c; }
    @keyframes cardIn { from{opacity:0;transform:translateY(-6px) scale(.99)}to{opacity:1;transform:none} }

    .ah    { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; gap: 8px; }
    .a-sym { font-size: 20px; font-weight: 700; color: var(--text-primary); letter-spacing: -.3px; }
    .a-ts  { font-size: 10px; color: var(--text-muted); margin-top: 3px; }

    /* Price pill badge */
    .a-price-pill {
      background: var(--bg-tertiary); border: 1px solid var(--border);
      border-radius: 20px; padding: 4px 10px;
      font-size: 12px; font-weight: 600; color: var(--text-primary);
      white-space: nowrap;
    }

    .a-badges { display: flex; flex-direction: column; align-items: flex-end; gap: 6px; }
    .a-typ {
      font-size: 10px; font-weight: 600; letter-spacing: .3px;
      padding: 3px 8px; border-radius: var(--radius-sm);
      white-space: nowrap;
    }
    .a-typ.breakout { background: rgba(61,158,255,.12); color: var(--accent); border: 1px solid rgba(61,158,255,.25); }
    .a-typ.pullback { background: var(--purple-bg); color: var(--purple); border: 1px solid rgba(157,92,255,.25); }
    .a-typ.pivot    { background: var(--amber-bg); color: var(--amber); border: 1px solid rgba(255,214,0,.25); }
    .a-typ.gap_go   { background: var(--green-bg); color: var(--green); border: 1px solid rgba(0,232,124,.25); }
    .a-typ.watchlist { background: rgba(255,159,28,.1); color: #ff9f1c; border: 1px solid rgba(255,159,28,.25); }

    .a-rr-badge {
      font-size: 10px; font-weight: 700; padding: 3px 8px;
      border-radius: var(--radius-sm);
    }
    .a-rr-badge.rr-hi  { background: var(--green-bg); color: var(--green); border: 1px solid rgba(0,232,124,.25); }
    .a-rr-badge.rr-mid { background: var(--amber-bg); color: var(--amber); border: 1px solid rgba(255,214,0,.25); }
    .a-rr-badge.rr-lo  { background: var(--red-bg);   color: var(--red);   border: 1px solid rgba(255,61,90,.25); }

    .a-px  { display: grid; grid-template-columns: repeat(3,1fr); gap: 8px; margin-bottom: 11px; }
    .pc    { background: var(--bg-tertiary); padding: 8px 10px; border-radius: var(--radius-sm); border: 1px solid var(--border-light); }
    .pc .l { font-size: 9px; color: var(--text-muted); text-transform: uppercase; letter-spacing: .8px; margin-bottom: 3px; }
    .pc .v { font-size: 13px; font-weight: 600; }
    .pc .v.e { color: var(--accent); }
    .pc .v.s { color: var(--red); }
    .pc .v.t { color: var(--green); }

    .a-st { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 11px; font-size: 11px; color: var(--text-muted); }
    .a-st .sv   { color: var(--amber); font-weight: 600; }
    .a-st .sv.g { color: var(--green); font-weight: 700; }

    /* Place Trade button */
    .a-trade-btn {
      display: block; width: 100%; padding: 9px;
      background: rgba(61,158,255,.1); border: 1px solid rgba(61,158,255,.25);
      border-radius: var(--radius-sm); color: var(--accent);
      font-size: 11px; font-weight: 600; cursor: pointer;
      text-align: center; transition: var(--transition); margin-top: 10px;
      letter-spacing: .3px;
    }
    .a-trade-btn:hover { background: rgba(61,158,255,.2); border-color: var(--accent); }

    /* ── CRITERIA NUMBERED BADGES ───────────────────────────────────────── */
    .crit-list { border-top: 1px solid var(--border-light); padding-top: 10px; display: flex; flex-direction: column; gap: 5px; }
    .crit      { display: flex; align-items: flex-start; gap: 8px; line-height: 1.5; }
    .cn {
      display: inline-flex; align-items: center; justify-content: center;
      width: 16px; height: 16px; border-radius: 50%;
      background: var(--green); color: #000;
      font-size: 9px; font-weight: 700; flex-shrink: 0; margin-top: 2px;
    }
    .ck { font-size: 10px; font-weight: 500; color: var(--text-primary); flex-shrink: 0; min-width: 50px; }
    .cd { font-size: 10px; color: var(--text-muted); line-height: 1.5; }

    /* ── SKELETON LOADING ───────────────────────────────────────────────── */
    .skeleton-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); gap: 12px; }
    .skeleton-card {
      background: var(--bg-secondary); border: 1px solid var(--border);
      border-radius: var(--radius-md); border-left: 3px solid var(--border);
      padding: 16px; height: 200px;
    }
    .skel-line {
      height: 12px; border-radius: 4px;
      background: linear-gradient(90deg, var(--bg-tertiary) 25%, var(--bg-hover) 50%, var(--bg-tertiary) 75%);
      background-size: 200% 100%;
      animation: shimmer 1.5s infinite;
      margin-bottom: 10px;
    }
    .skel-line.w-50 { width: 50%; }
    .skel-line.w-30 { width: 30%; }
    .skel-line.w-70 { width: 70%; }
    .skel-line.h-24 { height: 24px; }
    .skel-line.h-8  { height: 8px; }
    @keyframes shimmer {
      0%   { background-position: 200% 0; }
      100% { background-position: -200% 0; }
    }

    /* ── STAT CARDS (glassmorphism) ─────────────────────────────────────── */
    .bt-stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 10px; margin-bottom: 22px; }
    .bt-stat {
      background: var(--bg-secondary); border: 1px solid var(--border);
      padding: 16px; border-radius: var(--radius-md); position: relative; overflow: hidden;
    }
    .bt-stat::before {
      content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
      background: linear-gradient(90deg, var(--accent), var(--purple));
    }
    .bt-stat .k { font-size: 10px; font-weight: 600; color: var(--text-muted); text-transform: uppercase; letter-spacing: .8px; margin-bottom: 10px; }
    .bt-stat .v { font-size: 24px; font-weight: 700; color: var(--text-primary); line-height: 1; }
    .bt-stat .v.g { color: var(--green); }
    .bt-stat .v.r { color: var(--red); }
    .bt-stat .v.y { color: var(--amber); }

    /* ── SECTOR HEATMAP ─────────────────────────────────────────────────── */
    .heatmap-strip { display:flex; gap:6px; flex-wrap:wrap; padding:10px 0 14px; border-bottom:1px solid var(--border); margin-bottom:14px; align-items:center; }
    .heatmap-lbl   { font-size:9px; color:var(--text-muted); text-transform:uppercase; letter-spacing:1.5px; flex-shrink:0; margin-right:4px; }
    .hm-tile {
      display:flex; flex-direction:column; align-items:center; justify-content:center;
      padding:7px 10px; border-radius:var(--radius-sm); min-width:68px;
      border:1px solid transparent; transition: transform .2s, box-shadow .2s; cursor: default;
    }
    .hm-tile:hover { transform: scale(1.04); box-shadow: var(--shadow); }
    .hm-tile .ht-lbl  { font-size:8px; color:rgba(255,255,255,.55); letter-spacing:.8px; margin-bottom:2px; text-transform:uppercase; }
    .hm-tile .ht-etf  { font-size:7px; color:rgba(255,255,255,.35); margin-bottom:3px; }
    .hm-tile .ht-pct  { font-size:13px; font-weight:700; }
    .hm-tile .ht-arr  { font-size:9px; margin-top:1px; }
    .hm-tile.pos-3 { background: var(--green-bg); border-color:rgba(0,232,124,.3); }
    .hm-tile.pos-2 { background:rgba(0,232,124,.07); border-color:rgba(0,232,124,.2); }
    .hm-tile.pos-1 { background:rgba(0,232,124,.04); border-color:rgba(0,232,124,.12); }
    .hm-tile.zero  { background:rgba(255,255,255,.02); border-color:var(--border); }
    .hm-tile.neg-1 { background: var(--red-bg); border-color:rgba(255,61,90,.12); }
    .hm-tile.neg-2 { background:rgba(255,61,90,.07); border-color:rgba(255,61,90,.2); }
    .hm-tile.neg-3 { background:rgba(255,61,90,.14); border-color:rgba(255,61,90,.3); }
    .hm-tile.pos-3 .ht-pct,.hm-tile.pos-2 .ht-pct,.hm-tile.pos-1 .ht-pct { color:var(--green); }
    .hm-tile.pos-3 .ht-arr,.hm-tile.pos-2 .ht-arr,.hm-tile.pos-1 .ht-arr { color:var(--green); }
    .hm-tile.zero  .ht-pct { color:var(--text-muted); }
    .hm-tile.neg-1 .ht-pct,.hm-tile.neg-2 .ht-pct,.hm-tile.neg-3 .ht-pct { color:var(--red); }
    .hm-tile.neg-1 .ht-arr,.hm-tile.neg-2 .ht-arr,.hm-tile.neg-3 .ht-arr { color:var(--red); }

    /* ── TABLES ─────────────────────────────────────────────────────────── */
    .dtbl    { width: 100%; border-collapse: collapse; font-size: 12px; }
    .dtbl thead { position: sticky; top: 0; z-index: 10; }
    .dtbl th {
      text-align: left; padding: 9px 12px;
      background: var(--bg-tertiary); color: var(--text-muted);
      font-size: 10px; font-weight: 600; letter-spacing: .8px; text-transform: uppercase;
      border-bottom: 1px solid var(--border);
    }
    .dtbl td { padding: 9px 12px; border-bottom: 1px solid var(--border-light); color: var(--text-secondary); }
    .dtbl tbody tr:nth-child(even) td { background: rgba(255,255,255,.015); }
    .dtbl tbody tr:hover td { background: var(--bg-hover); }
    .dtbl .pos { color: var(--green); }
    .dtbl .neg { color: var(--red); }
    .dtbl .sym { color: var(--text-primary); font-weight: 700; }
    .dtbl .win { color: var(--green); }
    .dtbl .loss { color: var(--red); }
    .dtbl .timeout { color: var(--text-muted); }
    th.sortable { cursor:pointer; user-select:none; }
    th.sortable:hover { color:var(--text-primary); }
    th.sort-asc::after  { content:' \u25b2'; color:var(--accent); font-size:8px; }
    th.sort-desc::after { content:' \u25bc'; color:var(--accent); font-size:8px; }

    /* P&L pill badges in tables */
    .pnl-pill {
      display: inline-block; padding: 2px 7px; border-radius: 10px;
      font-size: 10px; font-weight: 600;
    }
    .pnl-pill.pos { background: var(--green-bg); color: var(--green); }
    .pnl-pill.neg { background: var(--red-bg); color: var(--red); }
    .r-badge {
      display: inline-block; padding: 2px 6px; border-radius: 4px;
      font-size: 10px; font-weight: 700;
    }
    .r-badge.pos { color: var(--green); }
    .r-badge.neg { color: var(--red); }

    /* ── EMPTY STATES ───────────────────────────────────────────────────── */
    .empty-state {
      display: flex; flex-direction: column; align-items: center; justify-content: center;
      padding: 64px 20px; text-align: center;
    }
    .empty-state .es-icon { font-size: 36px; margin-bottom: 16px; opacity: .4; }
    .empty-state .es-title { font-size: 15px; font-weight: 600; color: var(--text-primary); margin-bottom: 8px; }
    .empty-state .es-desc  { font-size: 12px; color: var(--text-muted); line-height: 1.6; max-width: 280px; }
    /* Legacy .empty still works */
    .empty { text-align:center; padding:60px 20px; color:var(--text-muted); font-size:11px; line-height:2; }
    .empty .ico { font-size:28px; margin-bottom:12px; opacity:.4; }

    /* ── TOASTS ─────────────────────────────────────────────────────────── */
    #toasts {
      position: fixed; bottom: 20px; right: 20px;
      display: flex; flex-direction: column; gap: 8px;
      z-index: 9999; pointer-events: none;
      width: 320px;
    }
    .toast {
      background: var(--bg-tertiary);
      border: 1px solid var(--border);
      backdrop-filter: blur(12px);
      border-radius: var(--radius-md);
      padding: 0; overflow: hidden;
      box-shadow: var(--shadow);
      pointer-events: all;
      animation: toastSlide .25s cubic-bezier(.22,.68,0,1.2);
      position: relative;
    }
    @keyframes toastSlide {
      from { opacity:0; transform: translateX(110%); }
      to   { opacity:1; transform: translateX(0); }
    }
    .toast-inner {
      display: flex; align-items: flex-start; gap: 10px;
      padding: 11px 14px; border-left: 3px solid var(--green);
    }
    .toast.err   .toast-inner { border-left-color: var(--red); }
    .toast.trade .toast-inner { border-left-color: var(--amber); }
    .toast.info  .toast-inner { border-left-color: var(--accent); }
    .toast.alert .toast-inner { border-left-color: var(--purple); }
    .toast-icon  { font-size: 14px; flex-shrink: 0; line-height: 1.4; }
    .toast-msg   { font-size: 11px; color: var(--text-secondary); line-height: 1.5; flex: 1; }
    .toast-prog  {
      height: 2px; background: var(--green);
      animation: toastProg linear forwards;
    }
    .toast.err   .toast-prog { background: var(--red); }
    .toast.trade .toast-prog { background: var(--amber); }
    .toast.info  .toast-prog { background: var(--accent); }
    @keyframes toastProg {
      from { width: 100%; }
      to   { width: 0%; }
    }

    /* ── COMMAND PALETTE ────────────────────────────────────────────────── */
    #cmd-palette {
      display: none; position: fixed; inset: 0; z-index: 10000;
      background: rgba(6,6,18,.75);
      backdrop-filter: blur(8px);
      align-items: flex-start; justify-content: center;
      padding-top: 18vh;
    }
    #cmd-palette.open { display: flex; }
    .cmd-box {
      background: var(--bg-secondary); border: 1px solid var(--border);
      border-radius: var(--radius-lg); width: 560px; max-width: 95vw;
      box-shadow: 0 24px 64px rgba(0,0,0,.7);
      overflow: hidden;
      animation: cmdIn .18s cubic-bezier(.22,.68,0,1.2);
    }
    @keyframes cmdIn { from{opacity:0;transform:scale(.97) translateY(-8px)}to{opacity:1;transform:none} }
    .cmd-input-wrap {
      display: flex; align-items: center; gap: 10px;
      padding: 14px 16px; border-bottom: 1px solid var(--border);
    }
    .cmd-icon { font-size: 14px; color: var(--text-muted); flex-shrink: 0; }
    #cmd-input {
      flex: 1; background: none; border: none; outline: none;
      font-size: 14px; color: var(--text-primary); font-family: 'Inter', sans-serif;
    }
    #cmd-input::placeholder { color: var(--text-muted); }
    .cmd-hint { font-size: 10px; color: var(--text-muted); flex-shrink: 0; }
    .cmd-results { max-height: 320px; overflow-y: auto; padding: 6px; }
    .cmd-item {
      display: flex; align-items: center; gap: 12px;
      padding: 9px 10px; border-radius: var(--radius-sm);
      cursor: pointer; transition: background .1s;
    }
    .cmd-item:hover, .cmd-item.focused { background: var(--bg-hover); }
    .cmd-item .ci-icon { font-size: 14px; flex-shrink: 0; width: 22px; text-align: center; }
    .cmd-item .ci-label { font-size: 12px; font-weight: 500; color: var(--text-primary); flex: 1; }
    .cmd-item .ci-kbd { font-size: 10px; color: var(--text-muted); }
    .cmd-empty { text-align: center; padding: 32px; color: var(--text-muted); font-size: 12px; }

    /* ── TRADE MODAL ────────────────────────────────────────────────────── */
    .modal-overlay {
      position: fixed; bottom: 24px; right: 24px; z-index: 1000;
      pointer-events: none;
    }
    .modal-box {
      pointer-events: all;
      background: var(--bg-secondary); border: 1px solid var(--border);
      border-top: 2px solid var(--accent);
      padding: 20px 22px; width: 380px;
      border-radius: var(--radius-md);
      box-shadow: 0 8px 40px rgba(0,0,0,.65);
      animation: slideIn .24s cubic-bezier(.22,.68,0,1.2);
    }
    @keyframes slideIn { from{opacity:0;transform:translateX(32px)} to{opacity:1;transform:none} }
    .modal-close-btn {
      background: none; border: none; color: var(--text-muted); font-size: 15px;
      cursor: pointer; padding: 4px 7px; line-height: 1; border-radius: 3px;
      align-self: flex-start; margin-top: 2px;
    }
    .modal-close-btn:hover { color: var(--text-primary); background: var(--bg-hover); }
    .mkt-closed-badge {
      margin-top: 8px; padding: 3px 8px; display: inline-block;
      background: var(--amber-bg); border: 1px solid rgba(255,190,0,.3);
      border-radius: 3px; font-size: 9px; letter-spacing: 1px; color: var(--amber);
    }
    .outside-prime-badge {
      margin-top: 6px; padding: 3px 8px; display: inline-block;
      background: var(--amber-bg); border: 1px solid rgba(255,214,0,.25);
      border-radius: 3px; font-size: 9px; letter-spacing: 1px; color: var(--amber);
    }
    .not-prime-badge {
      margin-top: 6px; padding: 3px 8px; display: inline-block;
      background: rgba(220,50,50,.12); border: 1px solid rgba(220,50,50,.35);
      border-radius: 3px; font-size: 9px; letter-spacing: 1px; color: #e05050; font-weight: 600;
    }
    .extended-badge {
      margin-top: 6px; padding: 3px 8px; display: inline-block;
      background: rgba(220,100,0,.12); border: 1px solid rgba(220,100,0,.35);
      border-radius: 3px; font-size: 9px; letter-spacing: 1px; color: #e07030; font-weight: 600;
    }
    .modal-hdr { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; }
    .modal-sym  { font-size: 28px; font-weight: 700; color: var(--text-primary); letter-spacing: -.5px; }
    .modal-type-badge {
      font-size: 10px; font-weight: 600; letter-spacing: .3px; padding: 4px 9px;
      border-radius: var(--radius-sm); margin-top: 5px; display: inline-block;
      background: rgba(61,158,255,.12); color: var(--accent); border: 1px solid rgba(61,158,255,.25);
    }
    .modal-countdown-ring { position: relative; flex-shrink: 0; }
    .modal-cd-num { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 700; color: var(--accent); }
    .modal-prices { display: grid; grid-template-columns: repeat(3,1fr); gap: 10px; margin-bottom: 14px; }
    .mpc    { background: var(--bg-tertiary); padding: 9px 11px; border-radius: var(--radius-sm); border: 1px solid var(--border-light); }
    .mpc-l  { font-size: 9px; color: var(--text-muted); text-transform: uppercase; letter-spacing: .8px; margin-bottom: 4px; }
    .mpc-v  { font-size: 16px; font-weight: 700; }
    .mpc-v.e { color: var(--accent); }
    .mpc-v.s { color: var(--red); }
    .mpc-v.t { color: var(--green); }
    .modal-meta { display: flex; flex-wrap: wrap; gap: 14px; font-size: 11px; color: var(--text-muted); margin-bottom: 20px; }
    .modal-meta .hi-val   { color: var(--text-primary); font-weight: 600; }
    .modal-meta .hi-val.g { color: var(--green); }
    .modal-actions { display: flex; gap: 10px; }
    .modal-place   { flex: 1; padding: 10px; font-size: 11px; }
    .modal-skip    { flex-shrink: 0; padding: 10px 20px; font-size: 11px; }
    .modal-queue-info { margin-top: 12px; text-align: center; font-size: 10px; color: var(--text-muted); }
    .mod-inp { width:100%; background:var(--bg-tertiary); border:1px solid var(--border); border-radius:var(--radius-sm); color:var(--text-primary); font-size:12px; padding:5px 8px; outline:none; }
    .mod-inp:focus { border-color:var(--accent); box-shadow:0 0 0 2px rgba(61,158,255,.15); }
    .mf label { display:block; }

    /* ── BACKTEST ───────────────────────────────────────────────────────── */
    .bt-ctrl { display: flex; align-items: flex-end; gap: 14px; margin-bottom: 20px; flex-wrap: wrap; padding-bottom: 20px; border-bottom: 1px solid var(--border); }
    .bt-ctrl .kv { gap: 6px; }
    .bt-inp  { font-size: 12px; background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 7px 12px; width: 100px; border-radius: var(--radius-sm); outline: none; }
    .bt-inp:focus { border-color: var(--accent); }
    .bt-hint { font-size: 11px; color: var(--text-muted); font-style: italic; }
    .bt-prog-wrap { background: var(--bg-secondary); border: 1px solid var(--border); padding: 18px 20px; border-radius: var(--radius-md); margin-bottom: 20px; }
    .bt-prog-msg  { font-size: 11px; color: var(--accent); margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
    .bt-prog-sub  { font-size: 10px; color: var(--text-muted); margin-top: 9px; }
    .bt-sec-title { font-size: 10px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 2px; margin-bottom: 12px; }

    /* ── JOURNAL ───────────────────────────────────────────────────────── */
    .jrnl-summary { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
    .jrnl-kv { background:var(--bg-secondary); border:1px solid var(--border); padding:14px 16px; border-radius:var(--radius-md); min-width:90px; position:relative; overflow:hidden; }
    .jrnl-kv::before { content:''; position:absolute; top:0; left:0; right:0; height:2px; background:linear-gradient(90deg,var(--accent),var(--purple)); }
    .jrnl-kv .k { font-size:10px; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:.8px; margin-bottom:8px; }
    .jrnl-kv .v { font-size:20px; font-weight:700; color:var(--text-primary); line-height:1; }
    .jrnl-kv .v.g { color:var(--green); }
    .jrnl-kv .v.r { color:var(--red); }

    /* ── NEWS ──────────────────────────────────────────────────────────── */
    .news-section  { margin-top:10px; border-top:1px solid var(--border-light); padding-top:8px; }
    .news-hdr      { font-size:9px; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:1px; margin-bottom:6px; }
    .news-item     { padding:4px 0; border-bottom:1px solid var(--border-light); }
    .news-item:last-child { border-bottom:none; }
    .news-headline { font-size:11px; color:var(--text-secondary); line-height:1.4; }
    .news-meta     { font-size:9px; color:var(--text-muted); margin-top:2px; }
    .news-loading  { font-size:10px; color:var(--text-muted); }

    /* ── MISC ──────────────────────────────────────────────────────────── */
    .cycle-bdg {
      font-size: 9px; font-weight: 600; letter-spacing: 1px;
      text-transform: uppercase; padding: 3px 9px; border-radius: 3px; border: 1px solid;
    }
    .cycle-bdg.hot     { background: var(--green-bg); color: var(--green); border-color: rgba(0,232,124,.35); }
    .cycle-bdg.cold    { background: rgba(110,110,180,.12); color: #9999cc; border-color: rgba(110,110,180,.35); }
    .cycle-bdg.neutral { background: var(--amber-bg); color: var(--amber); border-color: rgba(255,214,0,.3); }

    .trail-be    { font-size:9px; padding:2px 6px; border-radius:2px; background:rgba(61,158,255,.1); color:var(--accent); border:1px solid rgba(61,158,255,.3); }
    .trail-trail { font-size:9px; padding:2px 6px; border-radius:2px; background:var(--green-bg); color:var(--green); border:1px solid rgba(0,232,124,.3); }

    .mtf-yes { display:inline-flex; align-items:center; gap:4px; font-size:9px; padding:2px 7px; border-radius:2px; background:var(--green-bg); color:var(--green); border:1px solid rgba(0,232,124,.25); margin-top:5px; }
    .mtf-no  { display:inline-flex; align-items:center; gap:4px; font-size:9px; padding:2px 7px; border-radius:2px; background:var(--amber-bg); color:var(--amber); border:1px solid rgba(255,214,0,.2); margin-top:5px; }

    .badge { background: var(--green); color: #000; font-size: 9px; font-weight: 700; padding: 1px 5px; border-radius: 8px; margin-left: 5px; vertical-align: middle; }

    .sdot-wrap { display:inline-flex; align-items:center; gap:6px; }
    .dot { width:8px; height:8px; border-radius:50%; display:inline-block; margin-left:6px; vertical-align:middle; }
    .dot-green { background:#22c55e; animation:dotpulse 1.5s infinite; }
    .dot-red   { background:#ef4444; }
    @keyframes dotpulse { 0%,100%{opacity:1} 50%{opacity:.4} }
    #scan-age  { font-size:9px; color:var(--text-muted); margin-left:4px; }
    #mute-btn { font-size:14px; background:none; border:1px solid var(--border); border-radius:var(--radius-sm); padding:5px 9px; cursor:pointer; color:var(--text-muted); transition:var(--transition); }
    #mute-btn:hover { color:var(--text-primary); border-color:var(--text-muted); background:var(--bg-hover); }
    #mute-btn.muted { color:var(--red); border-color:rgba(255,61,90,.4); }

    .wl-section { margin-bottom:16px; border:1px solid rgba(255,159,28,.25); border-radius:var(--radius-md); background:rgba(255,159,28,.04); padding:12px 14px; }
    .wl-title   { font-size:10px; font-weight:600; color:#ff9f1c; text-transform:uppercase; letter-spacing:1.5px; flex:1; }
    .wl-input-row { display:flex; gap:8px; margin-bottom:10px; }
    .wl-inp     { flex:1; font-size:12px; background:var(--bg-tertiary); border:1px solid rgba(255,159,28,.25); border-radius:var(--radius-sm); color:var(--text-primary); padding:6px 10px; outline:none; text-transform:uppercase; }
    .wl-inp:focus { border-color:#ff9f1c; }
    .wl-pills   { display:flex; flex-wrap:wrap; gap:6px; margin-bottom:8px; }
    .wl-pill    { display:inline-flex; align-items:center; gap:5px; background:rgba(255,159,28,.1); border:1px solid rgba(255,159,28,.25); border-radius:12px; padding:3px 10px; font-size:10px; color:#ff9f1c; }
    .wl-pill button { background:none; border:none; color:#ff9f1c; cursor:pointer; font-size:11px; line-height:1; padding:0 1px; opacity:.7; }
    .wl-pill button:hover { opacity:1; }
    .wl-no-alerts { font-size:10px; color:var(--text-muted); padding:4px 0; }

    .pm-section { margin-bottom:14px; border:1px solid rgba(157,92,255,.3); border-radius:var(--radius-md); background:rgba(157,92,255,.05); }
    .pm-header  { display:flex; align-items:center; gap:10px; padding:10px 14px; cursor:pointer; user-select:none; }
    .pm-title   { font-size:10px; font-weight:600; color:var(--purple); text-transform:uppercase; letter-spacing:1.5px; flex:1; }
    .pm-count   { font-size:9px; background:var(--purple); color:#fff; border-radius:8px; padding:1px 7px; }
    .pm-body    { padding:0 14px 12px; }
    .pm-grid    { display:grid; grid-template-columns:repeat(auto-fill, minmax(240px, 1fr)); gap:8px; }
    .pm-card    { background:var(--bg-secondary); border:1px solid rgba(157,92,255,.2); border-left:3px solid var(--purple); border-radius:var(--radius-sm); padding:10px 12px; }
    .pm-sym     { font-size:16px; font-weight:700; color:var(--text-primary); margin-bottom:4px; display:inline; }
    .pm-gap-bdg { display:inline-block; background:var(--purple-bg); color:var(--purple); border:1px solid rgba(157,92,255,.35); font-size:10px; padding:2px 7px; border-radius:3px; margin-left:6px; }
    .pm-meta    { font-size:10px; color:var(--text-muted); margin-top:4px; }
    .pm-watch   { font-size:9px; background:var(--purple-bg); border:1px solid rgba(157,92,255,.35); color:var(--purple); border-radius:3px; padding:3px 9px; cursor:pointer; margin-top:6px; }
    .pm-watch:hover { background:rgba(157,92,255,.3); color:var(--text-primary); }
    .pm-empty   { font-size:10px; color:var(--text-muted); padding:8px 0; }

    .pos-stat-bar { display:grid; grid-template-columns:repeat(auto-fill, minmax(160px, 1fr)); gap:10px; margin-bottom:18px; }
    .pos-stat     { background:var(--bg-secondary); border:1px solid var(--border); padding:14px 16px; border-radius:var(--radius-md); }
    .pos-stat .k  { font-size:10px; font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:.8px; margin-bottom:6px; }
    .pos-stat .v  { font-size:20px; font-weight:700; color:var(--text-primary); line-height:1; }
    .pos-stat .v.g { color:var(--green); }
    .pos-stat .v.r { color:var(--red); }

    .settings-footer { background: var(--bg-secondary); border-top: 1px solid var(--border); }
    .settings-toggle { width:100%; background:none; border:none; color:var(--text-muted); font-size:10px; letter-spacing:1px; padding:10px 20px; text-align:left; cursor:pointer; display:flex; align-items:center; gap:8px; }
    .settings-toggle:hover { color:var(--text-secondary); background:var(--bg-hover); }
    .settings-body { padding:14px 20px 18px; border-top:1px solid var(--border); }
    .settings-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(300px, 1fr)); gap:7px 32px; }
    .setting-row { display:flex; gap:10px; align-items:baseline; font-size:10px; }
    .setting-row .sk { color:var(--text-muted); min-width:88px; text-transform:uppercase; letter-spacing:1px; flex-shrink:0; }
    .setting-row .sv { color:var(--text-primary); }

    .kb-legend { display:flex; gap:16px; flex-wrap:wrap; padding:8px 20px; font-size:9px; color:var(--text-muted); border-top:1px solid var(--border); background:var(--bg-secondary); }
    .kb-key { display:inline-flex; align-items:center; justify-content:center; background:var(--bg-tertiary); border:1px solid var(--border); border-radius:3px; padding:1px 6px; color:var(--text-secondary); margin-right:4px; font-size:9px; }

    #ph { text-align:center; padding:70px 20px; color:var(--text-muted); font-size:11px; line-height:2; }
    #ph .ico { font-size:32px; margin-bottom:14px; opacity:.3; }
  </style>
</head>
<body>

<!-- ── SIDEBAR ─────────────────────────────────────────────────────────────── -->
<nav class="sidebar">
  <div class="sidebar-logo">
    <span class="logo-icon">&#9654;</span>
    <span class="logo-text">SCANNER</span>
  </div>
  <div class="nav-items">
    <div class="nav-item active" data-tab="alerts" onclick="switchTab('alerts')">
      <span class="nav-icon">&#128288;</span>
      <span class="nav-label">Alerts</span>
      <span class="nav-badge" id="bdg">0</span>
    </div>
    <div class="nav-item" data-tab="watchlist" onclick="switchTab('watchlist')">
      <span class="nav-icon">&#11088;</span>
      <span class="nav-label">Watchlist</span>
    </div>
    <div class="nav-item" data-tab="positions" onclick="switchTab('positions')">
      <span class="nav-icon">&#128200;</span>
      <span class="nav-label">Positions</span>
    </div>
    <div class="nav-item" data-tab="orders" onclick="switchTab('orders')">
      <span class="nav-icon">&#128203;</span>
      <span class="nav-label">Orders</span>
    </div>
    <div class="nav-item" data-tab="backtest" onclick="switchTab('backtest')">
      <span class="nav-icon">&#128202;</span>
      <span class="nav-label">Backtest</span>
    </div>
    <div class="nav-item" data-tab="journal" onclick="switchTab('journal')">
      <span class="nav-icon">&#128211;</span>
      <span class="nav-label">Journal</span>
      <span class="nav-badge" id="jrnl-bdg" style="display:none">0</span>
    </div>
  </div>
  <div class="sidebar-bottom">
    <div class="nav-item" data-tab="settings-nav" onclick="toggleSettings()">
      <span class="nav-icon">&#9881;</span>
      <span class="nav-label">Settings</span>
    </div>
  </div>
</nav>

<!-- ── APP SHELL ────────────────────────────────────────────────────────────── -->
<div class="app-shell">

<!-- ── HEADER ──────────────────────────────────────────────────────────────── -->
<header>
  <div style="display:flex;align-items:center;gap:8px;flex-shrink:0">
    <span id="scan-dot" class="dot dot-red"></span>
    <small id="scan-age"></small>
    <span id="cycle-wrap"><span class="cycle-bdg neutral" id="cycle-bdg">NEUTRAL</span></span>
  </div>

  <div class="acct">
    <div class="kv"><span class="k">Equity</span>       <span class="v g" id="acc-eq">&#8212;</span></div>
    <div class="kv"><span class="k">Buying Power</span> <span class="v"   id="acc-bp">&#8212;</span></div>
    <div class="kv"><span class="k">Day P&amp;L</span>  <span class="v"   id="acc-pl">&#8212;</span></div>
    <div class="kv"><span class="k">Last Scan</span>    <span class="v"   id="acc-ls">&#8212;</span></div>
  </div>

  <div class="hacts">
    <button onclick="openCmdPalette()" title="Command palette (Ctrl+K / ⌘K)"
      style="font-size:11px;background:var(--bg-tertiary);border:1px solid var(--border);border-radius:var(--radius-sm);padding:5px 10px;cursor:pointer;color:var(--text-muted);display:flex;align-items:center;gap:6px">
      &#128269; <span style="font-size:10px;opacity:.7">&#8984;K</span>
    </button>
    <button id="mute-btn" onclick="toggleMute()" title="Toggle alert sound">&#128276;</button>
    <label class="tog" id="auto-lbl" title="Auto-submit bracket orders on signal">
      <div class="tog-t" id="auto-trk"><div class="tog-k"></div></div>
      <span class="tog-l">Auto</span>
    </label>
    <button class="btn primary" id="btn-start" onclick="startScan()">&#9654; START</button>
    <button class="btn danger"  id="btn-stop"  onclick="stopScan()" disabled>&#9632; STOP</button>
    <span id="risk-gauge" style="font-size:11px;color:var(--text-muted);white-space:nowrap">
      <span id="rg-trades">0</span>/<span id="rg-max-t">6</span> trades
      &nbsp;|&nbsp;$<span id="rg-loss">0</span>/$<span id="rg-max-l">500</span>
    </span>
    <button id="resume-btn" onclick="resumeTrading()" style="display:none;background:var(--red);color:#fff;border:none;border-radius:var(--radius-sm);padding:4px 10px;font-size:10px;cursor:pointer;font-weight:600">HALTED &mdash; RESUME</button>

    <!-- User info bar (populated by /auth/me via JS) -->
    <div id="user-bar" style="display:flex;align-items:center;gap:8px;border-left:1px solid var(--border);padding-left:12px;flex-shrink:0">
      <img id="user-avatar" src="" alt="" width="26" height="26"
           style="border-radius:50%;display:none;object-fit:cover;border:1px solid var(--border)">
      <span id="user-name" style="font-size:11px;color:var(--text-secondary);max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"></span>
      <a href="/auth/billing"
         style="font-size:10px;color:var(--accent);text-decoration:none;opacity:.85;flex-shrink:0"
         title="Manage subscription">Billing</a>
      <a href="/auth/logout"
         style="font-size:10px;color:var(--text-muted);text-decoration:none;opacity:.75;flex-shrink:0"
         title="Sign out">Logout</a>
    </div>
  </div>
</header>

<!-- ── PROGRESS ────────────────────────────────────────────────────────────── -->
<div class="prog-wrap">
  <div class="prog-meta">
    <span>
      <span class="sdot" id="sdot"></span>
      <span id="stxt" style="font-size:10px;color:var(--text-muted)">IDLE</span>
    </span>
    <span>
      <span id="pnum" class="hl">0 / 0</span> symbols
      &nbsp;&middot;&nbsp; ETA <span id="peta" class="hl">&#8212;</span>
      &nbsp;&middot;&nbsp; <span class="alc"><span id="palerts">0</span> alerts</span>
    </span>
  </div>
  <div class="prog-trk"><div class="prog-fill" id="pfill"></div></div>
</div>

<!-- ── CONTENT ─────────────────────────────────────────────────────────────── -->
<div class="content">

  <!-- ── ALERTS PANEL ──────────────────────────────────────────────────────── -->
  <div class="panel active" id="panel-alerts">

    <!-- Daily Watchlist badge + filter bar -->
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap">
      <span id="wl-count" onclick="showWatchlistModal()" style="cursor:pointer;background:#6366f1;color:#fff;padding:3px 12px;border-radius:999px;font-size:11px;font-weight:600">Daily WL: 0</span>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-left:auto">
        <label style="font-size:10px;color:var(--text-muted)">RVOL&ge;
          <input id="f-rvol" type="range" min="5" max="30" step="1" value="5" style="width:80px;vertical-align:middle" oninput="applyFilters()">
          <span id="f-rvol-val" style="color:var(--accent);font-weight:600">5&#215;</span>
        </label>
        <label style="font-size:10px;color:var(--text-muted)">Gap&ge;
          <input id="f-gap" type="number" min="0" step="1" value="0" style="width:42px;background:var(--bg-tertiary);border:1px solid var(--border);border-radius:4px;color:var(--text-primary);padding:2px 4px;font-size:10px" oninput="applyFilters()">%
        </label>
        <label style="font-size:10px;color:var(--text-muted);display:flex;align-items:center;gap:4px">
          <input id="f-mtf" type="checkbox" onchange="applyFilters()"> MTF only
        </label>
        <select id="f-type" onchange="applyFilters()" style="font-size:10px;background:var(--bg-tertiary);border:1px solid var(--border);border-radius:4px;color:var(--text-secondary);padding:3px 6px">
          <option value="">All Types</option>
          <option value="Gap and Go">Gap and Go</option>
          <option value="Breakout">Breakout</option>
          <option value="EMA20 Pullback">EMA Pullback</option>
          <option value="Pivot Reclaim">Pivot Reclaim</option>
        </select>
        <span id="f-count" style="font-size:10px;color:var(--text-muted)"></span>
        <button class="btn sm" onclick="clearFilters()">Reset</button>
        <button class="btn sm danger" onclick="clearAllAlerts()">Clear All</button>
      </div>
    </div>

    <!-- Daily Watchlist Modal -->
    <div id="wl-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);z-index:9000;align-items:center;justify-content:center">
      <div style="background:var(--bg-secondary);border:1px solid var(--border);border-radius:var(--radius-lg);padding:24px;min-width:340px;max-width:520px;width:90%;max-height:70vh;display:flex;flex-direction:column;gap:12px">
        <div style="display:flex;justify-content:space-between;align-items:center">
          <span style="font-size:13px;font-weight:700;color:var(--text-primary)">&#128204; Daily Watchlist (Pass 1 RVOL &ge; 5&times;)</span>
          <button onclick="document.getElementById('wl-modal').style.display='none'" style="background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer">&times;</button>
        </div>
        <div id="wl-date-label" style="font-size:10px;color:var(--text-muted)">&#8212;</div>
        <div style="overflow-y:auto;flex:1">
          <table style="width:100%;border-collapse:collapse;font-size:12px">
            <thead>
              <tr style="color:var(--text-muted);text-align:left;border-bottom:1px solid var(--border)">
                <th style="padding:4px 8px">#</th>
                <th style="padding:4px 8px">Symbol</th>
                <th style="padding:4px 8px;text-align:right">RVOL</th>
              </tr>
            </thead>
            <tbody id="wl-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Sector heatmap strip -->
    <div id="heatmap-strip" class="heatmap-strip">
      <span class="heatmap-lbl">Sectors</span>
    </div>

    <!-- Pre-market gappers -->
    <div class="pm-section">
      <div class="pm-header" onclick="togglePmSection()">
        <span class="pm-title">&#9728; Pre-Market Gappers</span>
        <span class="pm-count" id="pm-count">0</span>
        <span id="pm-arrow" style="color:var(--text-muted);font-size:10px">&#9656;</span>
      </div>
      <div id="pm-body" class="pm-body" style="display:none">
        <div id="pm-grid" class="pm-grid">
          <div class="pm-empty">No gappers yet &mdash; runs 4:00&ndash;9:30 AM ET</div>
        </div>
      </div>
    </div>

    <!-- Skeleton placeholders (shown when no alerts yet) -->
    <div id="skeleton-wrap" class="skeleton-grid">
      <div class="skeleton-card">
        <div class="skel-line h-24 w-30"></div>
        <div class="skel-line w-50"></div>
        <div class="skel-line w-70"></div>
        <div class="skel-line h-8 w-50"></div>
      </div>
      <div class="skeleton-card">
        <div class="skel-line h-24 w-30"></div>
        <div class="skel-line w-70"></div>
        <div class="skel-line w-50"></div>
        <div class="skel-line h-8 w-50"></div>
      </div>
      <div class="skeleton-card">
        <div class="skel-line h-24 w-30"></div>
        <div class="skel-line w-50"></div>
        <div class="skel-line w-70"></div>
        <div class="skel-line h-8 w-50"></div>
      </div>
    </div>

    <!-- Alert grid -->
    <div id="agrid" class="alerts-grid" style="margin-top:12px"></div>

    <!-- Empty state (shown after scan with zero results) -->
    <div id="ph" style="display:none">
      <div class="empty-state">
        <div class="es-icon">&#128225;</div>
        <div class="es-title">No alerts yet</div>
        <div class="es-desc">Click START SCAN to begin scanning the market for momentum setups.</div>
      </div>
    </div>
  </div>

  <!-- ── WATCHLIST PANEL ────────────────────────────────────────────────────── -->
  <div class="panel" id="panel-watchlist">
    <div class="wl-section">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
        <span class="wl-title">&#11088; My Watchlist</span>
      </div>
      <div class="wl-input-row">
        <input id="wl-inp" class="wl-inp" type="text" placeholder="ADD SYMBOL" maxlength="10">
        <button class="btn amber sm" onclick="addToWatchlist()">+ ADD</button>
      </div>
      <div id="wl-pills" class="wl-pills"></div>
      <div id="wl-no-alerts" class="wl-no-alerts">No watchlist alerts &mdash; signals will appear here when triggered.</div>
      <div id="wl-agrid" class="alerts-grid" style="display:none;margin-top:8px"></div>
    </div>
  </div>

  <!-- ── POSITIONS PANEL ────────────────────────────────────────────────────── -->
  <div class="panel" id="panel-positions">
    <div id="pos-stat-bar" class="pos-stat-bar" style="display:none">
      <div class="pos-stat"><div class="k">Equity</div><div class="v" id="ps-equity">&#8212;</div></div>
      <div class="pos-stat"><div class="k">Day P&amp;L</div><div class="v" id="ps-dpnl">&#8212;</div></div>
      <div class="pos-stat"><div class="k">Buying Power</div><div class="v" id="ps-bp">&#8212;</div></div>
      <div class="pos-stat"><div class="k">Open Positions</div><div class="v" id="ps-count">&#8212;</div></div>
    </div>
    <div id="pcont">
      <div class="empty-state">
        <div class="es-icon">&#9671;</div>
        <div class="es-title">No open positions</div>
        <div class="es-desc">Your open positions will appear here once trades are placed.</div>
      </div>
    </div>
  </div>

  <!-- ── ORDERS PANEL ───────────────────────────────────────────────────────── -->
  <div class="panel" id="panel-orders">
    <div id="ocont">
      <div class="empty-state">
        <div class="es-icon">&#128203;</div>
        <div class="es-title">No recent orders</div>
        <div class="es-desc">Orders placed through the scanner will appear here.</div>
      </div>
    </div>
  </div>

  <!-- ── BACKTEST PANEL ─────────────────────────────────────────────────────── -->
  <div class="panel" id="panel-backtest">
    <div class="bt-ctrl">
      <div class="kv" style="gap:6px">
        <span class="k" style="font-size:9px;color:var(--text-muted)">LOOKBACK DAYS</span>
        <input id="bt-days" class="bt-inp" type="number" value="30" min="5" max="365">
      </div>
      <div class="kv" style="gap:6px">
        <span class="k" style="font-size:9px;color:var(--text-muted)">PASSES</span>
        <input id="bt-passes" class="bt-inp" type="number" value="10" min="1" max="50">
      </div>
      <button id="btn-bt" class="btn primary" onclick="startBacktest()">&#9654; RUN BACKTEST</button>
      <span class="bt-hint">Walk-forward simulation across real market data</span>
    </div>

    <div id="bt-prog-wrap" class="bt-prog-wrap" style="display:none">
      <div class="bt-prog-msg">
        <span class="sdot scan" id="bt-sdot"></span>
        <span id="bt-msg">Starting&hellip;</span>
      </div>
      <div class="prog-trk"><div class="prog-fill" id="bt-pfill" style="width:0%"></div></div>
      <div class="bt-prog-sub" id="bt-sub"></div>
      <div style="font-size:9px;color:var(--text-muted);margin-top:6px" id="bt-run-id-lbl"></div>
    </div>

    <div id="bt-results" style="display:none">
      <div class="bt-sec-title">PERFORMANCE SUMMARY</div>
      <div id="bt-stats" class="bt-stats-grid"></div>

      <div class="bt-sec-title">EQUITY CURVE</div>
      <div style="background:var(--bg-secondary);border:1px solid var(--border);border-radius:var(--radius-md);padding:16px;margin-bottom:20px;overflow:hidden">
        <svg width="100%" viewBox="0 0 800 130" style="display:block">
          <line id="ec-zero" x1="0" y1="65" x2="800" y2="65" stroke="var(--border)" stroke-width="1" stroke-dasharray="4,4"/>
          <polyline id="ec-line" points="" fill="none" stroke="var(--green)" stroke-width="1.5"/>
        </svg>
      </div>

      <div class="bt-sec-title">TRADE LOG</div>
      <div style="overflow-x:auto" id="bt-tbl-wrap"></div>
    </div>

    <div id="bt-empty" style="display:none">
      <div class="empty-state">
        <div class="es-icon">&#128202;</div>
        <div class="es-title">No backtest runs yet</div>
        <div class="es-desc">Click RUN BACKTEST to simulate the scanner strategy across historical data.</div>
      </div>
    </div>
  </div>

  <!-- ── JOURNAL PANEL ──────────────────────────────────────────────────────── -->
  <div class="panel" id="panel-journal">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
      <span style="font-size:14px;font-weight:700;color:var(--text-primary)">Trade Journal</span>
      <button class="btn sm" onclick="exportJournal()">&#8595; Export CSV</button>
    </div>
    <div id="jrnl-summary" class="jrnl-summary"></div>
    <div id="jcont">
      <div class="empty-state">
        <div class="es-icon">&#128211;</div>
        <div class="es-title">No trades logged yet</div>
        <div class="es-desc">Trades placed via the modal are automatically logged here with full metrics.</div>
      </div>
    </div>
  </div>

</div><!-- /.content -->

<!-- ── SCANNER SETTINGS FOOTER ──────────────────────────────────────────────── -->
<div class="settings-footer">
  <button class="settings-toggle" onclick="toggleSettings()">
    <span id="settings-arrow">&#9656;</span> SCANNER SETTINGS
  </button>
  <div id="settings-body" class="settings-body" style="display:none">
    <div class="settings-grid">
      <div class="setting-row"><span class="sk">RVOL</span><span class="sv">&ge; 5&times; 20-bar average</span></div>
      <div class="setting-row"><span class="sk">Price</span><span class="sv">$5 &ndash; $10,000</span></div>
      <div class="setting-row"><span class="sk">Daily Gain</span><span class="sv">+10% minimum vs prev close</span></div>
      <div class="setting-row"><span class="sk">Float</span><span class="sv">&lt;20M shares (RVOL &ge;10&times;) &middot; &lt;50M (RVOL 5&ndash;10&times;)</span></div>
      <div class="setting-row"><span class="sk">Prime Time</span><span class="sv">7:00&ndash;11:00 AM ET</span></div>
      <div class="setting-row"><span class="sk">Patterns</span><span class="sv">Gap and Go &middot; Breakout &middot; EMA20 Pullback &middot; Pivot Reclaim</span></div>
    </div>
  </div>
  <div class="kb-legend">
    <span><span class="kb-key">B</span> Alerts</span>
    <span><span class="kb-key">W</span> Watchlist</span>
    <span><span class="kb-key">S</span> Positions</span>
    <span><span class="kb-key">J</span> Journal</span>
    <span><span class="kb-key">&#8984;K</span> Command Palette</span>
    <span><span class="kb-key">Enter</span> Place Trade</span>
    <span><span class="kb-key">Esc</span> Skip / Close</span>
  </div>
</div>

</div><!-- /.app-shell -->

<!-- ── TOASTS ───────────────────────────────────────────────────────────────── -->
<div id="toasts"></div>

<!-- ── COMMAND PALETTE ──────────────────────────────────────────────────────── -->
<div id="cmd-palette">
  <div class="cmd-box">
    <div class="cmd-input-wrap">
      <span class="cmd-icon">&#128269;</span>
      <input id="cmd-input" type="text" placeholder="Type a command or search&hellip;" autocomplete="off" oninput="filterCmds()" onkeydown="cmdKeydown(event)">
      <span class="cmd-hint">ESC to close</span>
    </div>
    <div id="cmd-results" class="cmd-results"></div>
  </div>
</div>

<!-- ── TRADE CONFIRMATION MODAL ─────────────────────────────────────────────── -->
<div class="modal-overlay" id="trade-modal" style="display:none">
  <div class="modal-box">
    <div class="modal-hdr">
      <div>
        <div class="modal-sym" id="m-sym"></div>
        <div class="modal-type-badge" id="m-type-badge"></div>
      </div>
      <div style="display:flex;align-items:center;gap:8px">
        <div class="modal-countdown-ring">
          <svg width="44" height="44" viewBox="0 0 44 44">
            <circle cx="22" cy="22" r="18" fill="none" stroke="var(--border)" stroke-width="3"/>
            <circle id="m-ring" cx="22" cy="22" r="18" fill="none" stroke="var(--accent)" stroke-width="3"
              stroke-dasharray="113.1" stroke-dashoffset="0"
              transform="rotate(-90 22 22)" style="transition:stroke-dashoffset 1s linear,stroke .5s"/>
          </svg>
          <div class="modal-cd-num" id="m-cd">60</div>
        </div>
        <button class="modal-close-btn" onclick="skipTrade()">&#10005;</button>
      </div>
    </div>

    <div class="modal-prices">
      <div class="mpc"><div class="mpc-l">Entry</div><div class="mpc-v e" id="m-entry">&#8212;</div></div>
      <div class="mpc"><div class="mpc-l">Stop</div><div class="mpc-v s" id="m-stop">&#8212;</div></div>
      <div class="mpc"><div class="mpc-l">Target</div><div class="mpc-v t" id="m-target">&#8212;</div></div>
    </div>

    <div class="modal-meta">
      <span>R:R <span class="hi-val g" id="m-rr">&#8212;</span></span>
      <span>Qty <span class="hi-val" id="m-qty">&#8212;</span> sh</span>
      <span>Risk <span class="hi-val" id="m-risk">&#8212;</span></span>
      <span>ATR <span class="hi-val" id="m-atr">&#8212;</span></span>
    </div>

    <!-- Optional modify row -->
    <div id="m-modify-row" style="display:none;margin-bottom:14px;background:var(--bg-tertiary);padding:12px;border-radius:var(--radius-sm);border:1px solid var(--border)">
      <div style="font-size:10px;font-weight:600;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:.8px">Adjust Order</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px" class="mf">
        <label style="font-size:9px;color:var(--text-muted)">Entry<input id="m-mod-entry" class="mod-inp" type="number" step="0.01"></label>
        <label style="font-size:9px;color:var(--text-muted)">Stop<input id="m-mod-stop" class="mod-inp" type="number" step="0.01"></label>
        <label style="font-size:9px;color:var(--text-muted)">Target<input id="m-mod-target" class="mod-inp" type="number" step="0.01"></label>
        <label style="font-size:9px;color:var(--text-muted)">Qty<input id="m-mod-qty" class="mod-inp" type="number" step="1" min="1"></label>
      </div>
      <div id="m-mod-rr-preview" style="font-size:10px;color:var(--accent);margin-top:6px"></div>
    </div>

    <div class="modal-actions">
      <button id="m-place-btn" class="btn primary modal-place" onclick="placeOrder()">&#9654; PLACE TRADE</button>
      <button class="btn modal-skip" onclick="skipTrade()">SKIP</button>
    </div>
    <div class="modal-queue-info" id="m-queue-info" style="display:none">
      <span id="m-queue-txt"></span>
    </div>
  </div>
</div>

<!-- ── SYMBOL DETAIL MODAL ───────────────────────────────────────────────────── -->
<div id="sym-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.75);backdrop-filter:blur(6px);z-index:5000;align-items:center;justify-content:center">
  <div style="background:var(--bg-secondary);border:1px solid var(--border);border-radius:var(--radius-lg);padding:24px;width:560px;max-width:95vw;max-height:85vh;overflow-y:auto;box-shadow:var(--shadow)">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px">
      <div>
        <div style="font-size:28px;font-weight:700;color:var(--text-primary);letter-spacing:-.5px" id="sym-title"></div>
        <div style="font-size:11px;color:var(--text-muted);margin-top:4px" id="sym-stats"></div>
      </div>
      <button onclick="closeSymModal()" style="background:none;border:none;color:var(--text-muted);font-size:20px;cursor:pointer;padding:4px">&times;</button>
    </div>
    <canvas id="sym-chart" width="520" height="100" style="width:100%;border-radius:6px;background:var(--bg-tertiary);display:block;margin-bottom:16px"></canvas>
    <div style="font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Recent News</div>
    <div id="sym-news" style="font-size:12px"></div>
    <div style="display:flex;gap:10px;margin-top:16px">
      <button id="sym-watch-btn" onclick="symAddWatch()" style="flex:1;padding:9px;border-radius:var(--radius-sm);background:var(--amber-bg);border:1px solid rgba(255,214,0,.3);color:var(--amber);cursor:pointer;font-weight:600;font-size:11px">+ WATCHLIST</button>
      <button id="sym-clone-btn" onclick="symClone()" style="flex:1;padding:9px;border-radius:var(--radius-sm);background:var(--green-bg);border:1px solid rgba(0,232,124,.3);color:var(--green);cursor:pointer;font-weight:600;font-size:11px">CLONE TRADE</button>
    </div>
  </div>
</div>

<script>
var alerts        = [];
var autoTrade     = false;
var es            = null;
var marketOpen    = false;
var accountEquity = 0;
var cloneMode     = false;

// ── ACCOUNT ──────────────────────────────────────────────────────────────────
async function refreshAccount() {
  try {
    var a = await fetch('/api/account').then(function(r){return r.json();});
    if (a.error) return;
    var eq = parseFloat(a.equity       || 0);
    var bp = parseFloat(a.buying_power || 0);
    var pl = eq - parseFloat(a.last_equity || eq);
    accountEquity = eq;
    var fmt = function(n){ return '$' + Math.abs(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}); };
    document.getElementById('acc-eq').textContent = fmt(eq);
    document.getElementById('acc-bp').textContent = fmt(bp);
    var plEl = document.getElementById('acc-pl');
    plEl.textContent = (pl >= 0 ? '+' : '-') + fmt(pl);
    plEl.className   = 'v ' + (pl >= 0 ? 'g' : 'r');
  } catch(e) {}
}

// ── SSE ───────────────────────────────────────────────────────────────────────
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
    hideSkeleton();
    addCard(a, !marketOpen);
    showTradeModal(a);
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
    if (!d.alerts) {
      hideSkeleton();
      document.getElementById('ph').style.display = 'block';
    }
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
    var runId = d.run_id || _btRunId;
    var url   = '/api/backtest/results' + (runId ? '?run_id=' + encodeURIComponent(runId) : '');
    fetch(url).then(function(r){ return r.json(); }).then(function(trades){
      renderBacktestTable(trades);
      renderEquityCurve(trades);
      document.getElementById('bt-results').style.display = 'block';
    }).catch(function(){});
    loadBtSummary();
    toast('Backtest done \u2014 ' + d.total_signals + ' trades, ' + d.win_rate + '% win rate', 'info');
  });

  es.addEventListener('backtest_error', function(e) {
    var d = JSON.parse(e.data);
    document.getElementById('bt-sdot').className = 'sdot stop';
    document.getElementById('bt-msg').textContent = 'Error: ' + d.message;
    document.getElementById('btn-bt').disabled = false;
    toast('Backtest error: ' + d.message, 'err');
  });

  es.addEventListener('market_cycle', function(e) {
    var d = JSON.parse(e.data);
    updateCycleBadge(d.cycle, d.gapper_count);
  });

  es.addEventListener('watchlist_alert', function(e) {
    var a = JSON.parse(e.data);
    renderWatchlistAlert(a);
    toast('\u2605 Watchlist signal: ' + a.symbol + ' \u2014 ' + a.entry_type, 'trade');
  });

  es.addEventListener('trailing_stop', function(e) {
    var d = JSON.parse(e.data);
    var label = d.phase === 'be' ? 'BREAKEVEN' : 'TRAILING';
    toast('\u21bb ' + d.symbol + ' stop [' + label + '] \u2192 $' + d.new_stop, 'trade');
    if (document.getElementById('panel-positions').classList.contains('active')) loadPositions();
  });

  es.onerror = function(){ setTimeout(connectSSE, 3000); };
}

// ── CONTROLS ─────────────────────────────────────────────────────────────────
async function startScan() {
  alerts = [];
  document.getElementById('agrid').innerHTML = '';
  document.getElementById('ph').style.display = 'none';
  document.getElementById('skeleton-wrap').style.display = 'grid';
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

function hideSkeleton() {
  var s = document.getElementById('skeleton-wrap');
  if (s) s.style.display = 'none';
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
function addCard(a, marketClosed) {
  var old = document.getElementById('ac-' + a.symbol);
  if (old) old.remove();
  alerts.unshift(a);
  document.getElementById('bdg').textContent     = alerts.length;
  document.getElementById('palerts').textContent = alerts.length;

  var typeKey = (a.entry_type && a.entry_type.indexOf('Gap and Go') === 0) ? 'gap_go'
              : a.entry_type === 'Breakout' ? 'breakout'
              : a.entry_type === 'Pivot Reclaim' ? 'pivot' : 'pullback';

  var rr    = parseFloat(a.rr) || 0;
  var rrCls = rr >= 3 ? 'rr-hi' : rr >= 2 ? 'rr-mid' : 'rr-lo';

  var card = document.createElement('div');
  card.className             = 'alert-card ' + typeKey;
  card.id                    = 'ac-' + a.symbol;
  card.dataset.rvol          = a.rvol || 0;
  card.dataset.gap           = a.gap_pct || 0;
  card.dataset.type          = a.entry_type || '';
  card.dataset.outsidePrime  = a.outside_prime ? 'true' : 'false';
  card.dataset.mtf           = a.mtf_confirmed ? 'true' : 'false';

  var ts  = new Date(a.timestamp).toLocaleTimeString();
  var c   = a.criteria;
  var chg = (a.pct_change_today != null && a.pct_change_today !== 0)
            ? (a.pct_change_today >= 0 ? '+' : '') + a.pct_change_today.toFixed(1) + '%' : '';
  var gap = (a.gap_pct != null && a.gap_pct !== 0)
            ? (a.gap_pct >= 0 ? '+' : '') + a.gap_pct.toFixed(1) + '%' : '';

  card.innerHTML =
    '<div class="ah">' +
      '<div>' +
        '<div class="a-sym" onclick="openSymModal(' + JSON.stringify(a) + ')" style="cursor:pointer;text-decoration:underline dotted">' + escHtml(a.symbol) + '</div>' +
        '<div class="a-ts">' + ts + (chg ? ' \u00b7 ' + chg + ' today' : '') + '</div>' +
      '</div>' +
      '<div class="a-badges">' +
        '<span class="a-price-pill">$' + a.entry.toFixed(2) + '</span>' +
        '<span class="a-typ ' + typeKey + '">' + a.entry_type + '</span>' +
        '<span class="a-rr-badge ' + rrCls + '">' + rr.toFixed(1) + ':1 R:R</span>' +
      '</div>' +
    '</div>' +
    '<div class="a-px">' +
      '<div class="pc"><div class="l">Entry</div><div class="v e">$' + a.entry.toFixed(2) + '</div></div>' +
      '<div class="pc"><div class="l">Stop</div><div class="v s">$' + a.stop.toFixed(2) + '</div></div>' +
      '<div class="pc"><div class="l">Target</div><div class="v t">$' + a.target.toFixed(2) + '</div></div>' +
    '</div>' +
    '<div class="a-st">' +
      '<span>RVOL <span class="sv">' + a.rvol + '\u00d7</span></span>' +
      (gap ? '<span>Gap <span class="sv g">' + gap + '</span></span>' : '') +
      (chg ? '<span>Chg <span class="sv g">' + chg + '</span></span>' : '') +
      '<span>Qty <span class="sv">' + a.qty + ' sh</span></span>' +
    '</div>' +
    '<div class="crit-list">' +
      mkCrit(1,'Trend', c.trend) +
      mkCrit(2,'Entry', c.entry) +
      mkCrit(3,'S/R',   c.sr)    +
      mkCrit(4,'Risk',  c.risk)  +
      mkCrit(5,'Vol',   c.volume) +
    '</div>' +
    (a.mtf_confirmed != null
      ? '<div class="' + (a.mtf_confirmed ? 'mtf-yes' : 'mtf-no') + '" title="' + escHtml(a.mtf_note || '') + '">' +
          (a.mtf_confirmed ? '\u2714 MTF Confirmed' : '\u26a0 MTF Unconfirmed') + '</div>'
      : '') +
    (a.outside_prime ? '<div class="outside-prime-badge">\u26a0 Outside 7\u201311AM ET</div>' : '') +
    (!a.prime_window ? '<div class="not-prime-badge">\u26d4 Post-11AM \u2014 Gap\u202f&\u202fGo edge degraded</div>' : '') +
    (a.extended ? '<div class="extended-badge">\u26a0 Extended \u2014 may be chasing</div>' : '') +
    (marketClosed ? '<div class="mkt-closed-badge">\u26a0 Market Closed \u2014 historical signal</div>' : '') +
    '<button class="a-trade-btn" onclick="openSymModal(' + JSON.stringify(a) + ')">Place Trade &#8599;</button>';

  var grid = document.getElementById('agrid');
  grid.insertBefore(card, grid.firstChild);
  fetchAndInjectNews(a.symbol, 'ac-' + a.symbol);
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

// ── TABS / SIDEBAR ────────────────────────────────────────────────────────────
function switchTab(tab) {
  document.querySelectorAll('.nav-item[data-tab]').forEach(function(t){
    t.classList.toggle('active', t.dataset.tab === tab);
  });
  document.querySelectorAll('.panel').forEach(function(p){
    p.classList.toggle('active', p.id === 'panel-' + tab);
  });
  if (tab === 'positions') { loadPositions(); loadAccountSummary(); }
  if (tab === 'orders')    loadOrders();
  if (tab === 'journal')   loadJournal();
  if (tab === 'backtest')  loadBtSummary();
}

// ── POSITIONS ─────────────────────────────────────────────────────────────────
async function loadPositions() {
  var el = document.getElementById('pcont');
  el.innerHTML = '<div class="empty">Loading\u2026</div>';
  try {
    var ps = await fetch('/api/positions').then(function(r){return r.json();});
    if (!ps.length) {
      el.innerHTML = '<div class="empty-state"><div class="es-icon">\u25c7</div><div class="es-title">No open positions</div><div class="es-desc">Open positions will appear here once trades are placed.</div></div>';
      return;
    }
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
        '<td><span class="pnl-pill ' + cls + '">' + sgn + '$' + Math.abs(pnl).toFixed(2) + '</span></td>' +
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
    if (!os.length) {
      el.innerHTML = '<div class="empty-state"><div class="es-icon">\u25c7</div><div class="es-title">No recent orders</div><div class="es-desc">Orders placed through the scanner will appear here.</div></div>';
      return;
    }
    var CLR = {filled:'var(--green)',canceled:'var(--text-muted)',pending_new:'var(--amber)',new:'var(--amber)',partially_filled:'var(--accent)'};
    var CAN = ['pending_new','new','partially_filled'];
    var rows = os.map(function(o) {
      var sc = CLR[o.status] || 'var(--text-secondary)';
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
        '<td style="color:var(--text-muted);font-size:9px">' + new Date(o.created_at).toLocaleTimeString() + '</td>' +
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
var _btPoll  = null;
var _btRunId = null;

async function startBacktest() {
  document.getElementById('btn-bt').disabled = true;
  document.getElementById('bt-results').style.display   = 'none';
  document.getElementById('bt-prog-wrap').style.display = 'block';
  document.getElementById('bt-pfill').style.width = '0%';
  document.getElementById('bt-pfill').classList.remove('done');
  document.getElementById('bt-sdot').className = 'sdot scan';
  document.getElementById('bt-msg').textContent = 'Starting enhanced backtest\u2026';
  document.getElementById('bt-sub').textContent = '';
  document.getElementById('bt-run-id-lbl').textContent = '';
  try {
    var res  = await fetch('/api/backtest/run', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
    var data = await res.json();
    if (data.error) { toast(data.error, 'err'); document.getElementById('btn-bt').disabled = false; return; }
    _btRunId = data.run_id;
    document.getElementById('bt-run-id-lbl').textContent = 'run ' + _btRunId + ' \u00b7 ' + data.symbols + ' symbols \u00d7 ' + data.passes + ' passes';
    toast('Backtest started \u2014 run ' + _btRunId, 'info');
    if (_btPoll) clearInterval(_btPoll);
    _btPoll = setInterval(_pollBacktest, 3000);
  } catch(e) {
    toast('Backtest launch error: ' + e.message, 'err');
    document.getElementById('btn-bt').disabled = false;
  }
}

async function _pollBacktest() {
  try {
    var s = await fetch('/api/backtest/status').then(function(r){ return r.json(); });
    var pct = s.progress || 0;
    document.getElementById('bt-pfill').style.width = pct + '%';
    if (s.status === 'fetching') {
      document.getElementById('bt-msg').textContent = 'Downloading bar data\u2026';
    } else if (s.status === 'simulating') {
      document.getElementById('bt-msg').textContent = 'Running walk-forward simulation\u2026';
      if (s.runs_done != null) document.getElementById('bt-sub').textContent = 'Pass ' + s.runs_done + ' / 10';
    }
    if (s.status === 'complete') {
      clearInterval(_btPoll); _btPoll = null;
      document.getElementById('bt-pfill').style.width = '100%';
      document.getElementById('bt-pfill').classList.add('done');
      document.getElementById('bt-sdot').className = 'sdot done';
      document.getElementById('btn-bt').disabled = false;
      var runId   = s.run_id || _btRunId;
      var url     = '/api/backtest/results' + (runId ? '?run_id=' + encodeURIComponent(runId) : '');
      var trades  = await fetch(url).then(function(r){ return r.json(); });
      renderBacktestResults(s.results || s);
      renderBacktestTable(trades);
      renderEquityCurve(trades);
      loadBtSummary();
      document.getElementById('bt-results').style.display = 'block';
      toast('Backtest complete \u2014 ' + (s.results || s).total_signals + ' trades', 'info');
    } else if (s.status === 'error') {
      clearInterval(_btPoll); _btPoll = null;
      document.getElementById('bt-sdot').className = 'sdot stop';
      document.getElementById('bt-msg').textContent = 'Error: ' + (s.error || 'unknown');
      document.getElementById('btn-bt').disabled = false;
      toast('Backtest error: ' + (s.error || ''), 'err');
    }
  } catch(e) {}
}

function renderBacktestResults(r) {
  if (!r || !r.total_signals) return;
  var isGood = function(key) {
    if (key === 'win_rate')      return r.win_rate >= 50 ? 'g' : (r.win_rate >= 40 ? 'y' : 'r');
    if (key === 'profit_factor') return r.profit_factor >= 1.5 ? 'g' : (r.profit_factor >= 1.0 ? 'y' : 'r');
    if (key === 'avg_pnl_r')     return r.avg_pnl_r > 0 ? 'g' : 'r';
    return '';
  };
  var stats = [
    {k:'Total Signals',   v: r.total_signals,  cls:''},
    {k:'Wins',            v: r.wins,            cls:'g'},
    {k:'Losses',          v: r.losses,          cls:'r'},
    {k:'Timeouts',        v: r.timeouts,        cls:''},
    {k:'Win Rate',        v: r.win_rate + '%',  cls: isGood('win_rate')},
    {k:'Avg R:R Setup',   v: r.avg_rr + ':1',  cls:''},
    {k:'Avg P&amp;L (R)', v: (r.avg_pnl_r >= 0 ? '+' : '') + r.avg_pnl_r + 'R', cls: isGood('avg_pnl_r')},
    {k:'Profit Factor',   v: r.profit_factor,  cls: isGood('profit_factor')},
    {k:'Max DD (R)',       v: '\u2212' + r.max_drawdown_r + 'R', cls: r.max_drawdown_r > 5 ? 'r' : ''},
    {k:'Sharpe',          v: r.sharpe,         cls: r.sharpe > 1 ? 'g' : (r.sharpe > 0 ? '' : 'r')},
    {k:'Symbols Tested',  v: r.symbols_tested, cls:''},
    {k:'Passes',          v: r.n_passes,       cls:''},
  ];
  document.getElementById('bt-stats').innerHTML = stats.map(function(s){
    return '<div class="bt-stat"><div class="k">' + s.k + '</div><div class="v ' + s.cls + '">' + s.v + '</div></div>';
  }).join('');
}

function renderBacktestTable(trades) {
  var wrap = document.getElementById('bt-tbl-wrap');
  if (!trades || !trades.length) {
    wrap.innerHTML = '<div class="empty-state"><div class="es-icon">\u25c7</div><div class="es-title">No trades in this run</div></div>';
    return;
  }
  var rows = trades.slice(0, 300).map(function(t) {
    var ocls   = t.outcome === 'win' ? 'win' : (t.outcome === 'loss' ? 'loss' : 'timeout');
    var pnlCls = t.pnl_r > 0 ? 'pos' : (t.pnl_r < 0 ? 'neg' : '');
    var pnlStr = (t.pnl_r >= 0 ? '+' : '') + Number(t.pnl_r).toFixed(2) + 'R';
    var eSym   = escHtml(t.symbol   || '');
    var eType  = escHtml(t.entry_type || '');
    return '<tr>' +
      '<td class="sym">' + eSym + '</td>' +
      '<td style="color:var(--text-muted)">' + eType + '</td>' +
      '<td>$' + Number(t.entry).toFixed(2) + '</td>' +
      '<td>$' + Number(t.exit).toFixed(2) + '</td>' +
      '<td style="color:var(--red)">$' + Number(t.stop).toFixed(2) + '</td>' +
      '<td style="color:var(--green)">$' + Number(t.target).toFixed(2) + '</td>' +
      '<td>' + Number(t.rr).toFixed(1) + ':1</td>' +
      '<td class="' + ocls + '">' + t.outcome.toUpperCase() + '</td>' +
      '<td><span class="r-badge ' + pnlCls + '">' + pnlStr + '</span></td>' +
      '<td><button class="btn sm" onclick="cloneTrade(' +
        JSON.stringify(eSym) + ',' + Number(t.entry).toFixed(4) + ',' +
        Number(t.stop).toFixed(4) + ',' + Number(t.target).toFixed(4) + ',' +
        JSON.stringify(eType) + ')">CLONE</button></td></tr>';
  }).join('');
  wrap.innerHTML =
    '<table class="dtbl"><thead><tr>' +
    '<th>Symbol</th><th>Type</th><th>Entry</th><th>Exit</th><th>Stop</th>' +
    '<th>Target</th><th>R:R</th><th>Outcome</th><th>PnL (R)</th><th></th>' +
    '</tr></thead><tbody>' + rows + '</tbody></table>';
}

function renderEquityCurve(trades) {
  var line   = document.getElementById('ec-line');
  var zeroEl = document.getElementById('ec-zero');
  if (!line || !trades || !trades.length) return;
  var cum = 0;
  var pts = [{i:0, y:0}];
  trades.forEach(function(t, idx) { cum += (t.pnl_r || 0); pts.push({i: idx + 1, y: cum}); });
  var n = pts.length;
  var ys = pts.map(function(p){ return p.y; });
  var minR = Math.min.apply(null, ys);
  var maxR = Math.max.apply(null, ys);
  var rng  = maxR - minR || 1;
  var W = 800, H = 130, PAD = 10;
  var sx = function(i){ return PAD + (i / (n - 1)) * (W - 2 * PAD); };
  var sy = function(r){ return PAD + (1 - (r - minR) / rng) * (H - 2 * PAD); };
  line.setAttribute('points', pts.map(function(p){ return sx(p.i).toFixed(1) + ',' + sy(p.y).toFixed(1); }).join(' '));
  line.setAttribute('stroke', cum >= 0 ? 'var(--green)' : 'var(--red)');
  if (zeroEl) { var zy = sy(0).toFixed(1); zeroEl.setAttribute('y1', zy); zeroEl.setAttribute('y2', zy); }
}

async function loadBtSummary() {
  var el = document.getElementById('bt-stats');
  if (!el) return;
  try {
    var s = await fetch('/api/backtest/summary').then(function(r){ return r.json(); });
    if (!s || s.error || !s.total) {
      el.innerHTML = '<div style="color:var(--text-muted);font-size:11px;padding:8px 0">No backtest runs yet \u2014 click RUN BACKTEST to start.</div>';
      return;
    }
    var g  = function(v, hi, lo){ return v >= hi ? 'g' : (v >= lo ? '' : 'r'); };
    var items = [
      {k:'Total Trades',   v: s.total,                                              cls:''},
      {k:'Runs',           v: s.runs,                                               cls:''},
      {k:'Win Rate',       v: s.win_rate + '%',                                     cls: g(s.win_rate, 50, 40)},
      {k:'Profit Factor',  v: s.profit_factor,                                      cls: g(s.profit_factor, 1.5, 1.0)},
      {k:'Avg R',          v: (s.avg_r >= 0 ? '+' : '') + s.avg_r + 'R',           cls: s.avg_r > 0 ? 'g' : 'r'},
      {k:'Max Drawdown',   v: '\u2212' + s.max_drawdown_r + 'R',                   cls: s.max_drawdown_r > 5 ? 'r' : ''},
      {k:'Sharpe',         v: s.sharpe,                                             cls: s.sharpe >= 1 ? 'g' : (s.sharpe >= 0 ? '' : 'r')},
      {k:'Best Trade',     v: '+' + s.best_trade + 'R',                             cls:'g'},
      {k:'Worst Trade',    v: s.worst_trade + 'R',                                  cls:'r'},
      {k:'Avg Hold',       v: s.avg_hold_minutes + ' min',                          cls:''},
      {k:'% Target Hit',   v: s.pct_target_hit + '%',                               cls: g(s.pct_target_hit, 45, 30)},
      {k:'% Stop Hit',     v: s.pct_stop_hit + '%',                                 cls: s.pct_stop_hit > 60 ? 'r' : ''},
      {k:'Cur Streak',     v: s.current_streak + ' W',                              cls: s.current_streak >= 3 ? 'g' : ''},
      {k:'Max Streak',     v: s.max_streak + ' W',                                  cls: s.max_streak >= 5 ? 'g' : ''},
    ];
    el.innerHTML = items.map(function(it){
      return '<div class="bt-stat"><div class="k">' + it.k + '</div><div class="v ' + it.cls + '">' + it.v + '</div></div>';
    }).join('');
    document.getElementById('bt-results').style.display = 'block';
  } catch(e) {}
}

// ── CLONE TRADE ───────────────────────────────────────────────────────────────
async function cloneTrade(symbol, entryPrice, stopPrice, targetPrice, entryType) {
  var livePrice = entryPrice;
  try {
    var qt = await fetch('/api/quote/' + encodeURIComponent(symbol)).then(function(r){ return r.json(); });
    if (qt && qt.price && qt.price > 0) livePrice = qt.price;
  } catch(e) {}
  var equity = 100000;
  try {
    var acc = await fetch('/api/account').then(function(r){ return r.json(); });
    if (acc && acc.equity) equity = parseFloat(acc.equity);
  } catch(e) {}
  var risk = livePrice - stopPrice;
  if (risk <= 0) risk = livePrice * 0.02;
  var shares  = Math.max(1, Math.floor(equity * 0.02 / risk));
  var reward  = targetPrice - livePrice;
  var rr      = reward / risk;
  var a = {
    symbol: symbol, entry: parseFloat(livePrice.toFixed(2)), stop: parseFloat(stopPrice.toFixed(2)),
    target: parseFloat(targetPrice.toFixed(2)), qty: shares, rr: parseFloat(rr.toFixed(2)),
    atr: parseFloat((risk / 1.5).toFixed(2)), entry_type: entryType || 'Clone',
  };
  _openModal(a);
  document.getElementById('m-mod-entry').value  = a.entry;
  document.getElementById('m-mod-stop').value   = a.stop;
  document.getElementById('m-mod-target').value = a.target;
  document.getElementById('m-mod-qty').value    = a.qty;
  document.getElementById('m-modify-row').style.display = 'block';
  _recalcModify();
}

function _recalcModify() {
  var entry  = parseFloat(document.getElementById('m-mod-entry').value)  || 0;
  var stop   = parseFloat(document.getElementById('m-mod-stop').value)   || 0;
  var target = parseFloat(document.getElementById('m-mod-target').value) || 0;
  var qty    = parseInt(document.getElementById('m-mod-qty').value, 10)  || 0;
  if (entry <= 0 || stop <= 0 || qty <= 0) {
    document.getElementById('m-mod-rr-preview').textContent = '';
    return;
  }
  var risk   = entry - stop;
  var reward = target > 0 ? target - entry : 0;
  var rr     = risk > 0 ? reward / risk : 0;
  var riskD  = (risk * qty).toFixed(2);
  var rewD   = (reward * qty).toFixed(2);
  document.getElementById('m-mod-rr-preview').textContent =
    'R:R ' + rr.toFixed(2) + ':1  \u00b7  Risk $' + riskD + '  \u00b7  Reward $' + rewD;
  document.getElementById('m-entry').textContent  = '$' + entry.toFixed(2);
  document.getElementById('m-stop').textContent   = '$' + stop.toFixed(2);
  if (target > 0) document.getElementById('m-target').textContent = '$' + target.toFixed(2);
  var rrEl = document.getElementById('m-rr');
  rrEl.textContent = rr.toFixed(2) + ':1';
  rrEl.className   = 'hi-val ' + (rr >= 3 ? 'g' : '');
  document.getElementById('m-qty').textContent  = qty;
  document.getElementById('m-risk').textContent = '$' + riskD;
  if (modalAlert) {
    modalAlert.entry  = entry;
    modalAlert.stop   = stop;
    modalAlert.target = target > 0 ? target : modalAlert.target;
    modalAlert.qty    = qty;
    modalAlert.rr     = parseFloat(rr.toFixed(2));
  }
}

// ── SYMBOL DETAIL MODAL ───────────────────────────────────────────────────────
window._symAlert = null;

async function openSymModal(a) {
  window._symAlert = a;
  var modal = document.getElementById('sym-modal');
  modal.style.display = 'flex';
  document.getElementById('sym-title').textContent = a.symbol || '';
  var chgStr = (a.pct_change_today != null && a.pct_change_today !== 0)
    ? (a.pct_change_today >= 0 ? '+' : '') + a.pct_change_today.toFixed(1) + '% today' : '';
  document.getElementById('sym-stats').textContent =
    'Entry $' + (a.entry||0).toFixed(2) +
    (chgStr ? ' \u00b7 ' + chgStr : '') +
    ' \u00b7 Gap ' + (a.gap_pct||0).toFixed(1) + '%' +
    ' \u00b7 RVOL ' + (a.rvol||0) + '\u00d7';
  document.getElementById('sym-news').innerHTML = '<span style="color:var(--text-muted)">Loading\u2026</span>';
  var canvas = document.getElementById('sym-chart');
  var ctx    = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  modal.onclick = function(e) { if (e.target === modal) closeSymModal(); };
  try {
    var qt = await fetch('/api/quote/' + encodeURIComponent(a.symbol)).then(function(r){ return r.json(); });
    if (qt && qt.price && qt.price > 0) {
      var livePrice  = qt.price;
      var floatVal   = (a.float_shares && a.float_shares > 0) ? a.float_shares : null;
      var mktCapStr  = floatVal ? '$' + (livePrice * floatVal / 1e6).toFixed(0) + 'M cap' : '';
      var floatStr   = floatVal ? (floatVal / 1e6).toFixed(1) + 'M float' : '';
      document.getElementById('sym-stats').textContent =
        'Live $' + livePrice.toFixed(2) + (chgStr ? ' \u00b7 ' + chgStr : '') +
        ' \u00b7 Gap ' + (a.gap_pct||0).toFixed(1) + '%' +
        ' \u00b7 RVOL ' + (a.rvol||0) + '\u00d7' +
        (floatStr ? ' \u00b7 ' + floatStr : '') + (mktCapStr ? ' \u00b7 ' + mktCapStr : '');
    }
  } catch(_) {}
  try {
    var start = new Date(Date.now() - 2 * 24 * 3600 * 1000).toISOString().replace(/\.\d+Z$/, 'Z');
    var barsUrl = '/api/bars/' + encodeURIComponent(a.symbol) + '?timeframe=5Min&limit=78&start=' + encodeURIComponent(start);
    var barsData = await fetch(barsUrl).then(function(r){ return r.json(); });
    var closes = Array.isArray(barsData) ? barsData.map(function(b){ return b.close || b.c || 0; }) : [];
    if (closes.length > 2) _drawMiniChart(canvas, closes);
  } catch(e) {}
  try {
    var newsItems = await fetch('/api/news/' + encodeURIComponent(a.symbol)).then(function(r){ return r.json(); });
    var newsEl    = document.getElementById('sym-news');
    if (!newsItems || !newsItems.length) {
      newsEl.innerHTML = '<span style="color:var(--text-muted)">No recent news</span>';
    } else {
      newsEl.innerHTML = newsItems.slice(0, 3).map(function(n) {
        var ago = n.created_at ? timeAgo(new Date(n.created_at)) : '';
        return '<div style="border-bottom:1px solid var(--border-light);padding:6px 0">' +
          '<div style="color:var(--text-primary);margin-bottom:2px;font-size:12px">' + escHtml(n.headline) + '</div>' +
          '<div style="color:var(--text-muted);font-size:10px">' + escHtml(n.source||'') + (ago ? ' \u00b7 ' + ago : '') + '</div>' +
          '</div>';
      }).join('');
    }
  } catch(e) {
    document.getElementById('sym-news').innerHTML = '<span style="color:var(--text-muted)">News unavailable</span>';
  }
}

function _drawMiniChart(canvas, closes) {
  var ctx  = canvas.getContext('2d');
  var W    = canvas.width, H = canvas.height;
  var minC = Math.min.apply(null, closes);
  var maxC = Math.max.apply(null, closes);
  var rng  = maxC - minC || 1;
  var pad  = 6;
  var sx   = function(i){ return pad + (i / (closes.length - 1)) * (W - 2 * pad); };
  var sy   = function(c){ return pad + (1 - (c - minC) / rng) * (H - 2 * pad); };
  ctx.clearRect(0, 0, W, H);
  ctx.strokeStyle = closes[closes.length-1] >= closes[0] ? '#00e87c' : '#ff3d5a';
  ctx.lineWidth   = 1.5;
  ctx.beginPath();
  closes.forEach(function(c, i){
    if (i === 0) ctx.moveTo(sx(i), sy(c)); else ctx.lineTo(sx(i), sy(c));
  });
  ctx.stroke();
}

function closeSymModal() {
  document.getElementById('sym-modal').style.display = 'none';
  window._symAlert = null;
}
function symAddWatch() {
  if (!window._symAlert) return;
  if (typeof addToWatchlistSym === 'function') addToWatchlistSym(window._symAlert.symbol);
  closeSymModal();
}
function symClone() {
  if (!window._symAlert) return;
  var a = window._symAlert;
  closeSymModal();
  cloneTrade(a.symbol, a.entry, a.stop, a.target, a.entry_type);
}

// ── ALERT FILTERS ─────────────────────────────────────────────────────────────
function applyFilters() {
  var minRvol    = parseFloat(document.getElementById('f-rvol').value) || 5;
  var minGap     = parseFloat(document.getElementById('f-gap').value)  || 0;
  var mtfOnly    = document.getElementById('f-mtf').checked;
  var typeFilter = document.getElementById('f-type').value;
  document.getElementById('f-rvol-val').textContent = minRvol.toFixed(1) + '\u00d7';
  var cards   = document.querySelectorAll('#agrid .alert-card');
  var visible = 0;
  cards.forEach(function(c) {
    var rvol = parseFloat(c.dataset.rvol) || 0;
    var gap  = parseFloat(c.dataset.gap)  || 0;
    var mtf  = c.dataset.mtf === 'true';
    var type = c.dataset.type || '';
    var show = true;
    if (rvol < minRvol)                   show = false;
    if (gap  < minGap)                    show = false;
    if (mtfOnly && !mtf)                  show = false;
    if (typeFilter && type !== typeFilter) show = false;
    c.style.display = show ? '' : 'none';
    if (show) visible++;
  });
  var fc = document.getElementById('f-count');
  if (fc) fc.textContent = visible + ' / ' + cards.length + ' shown';
}

function clearFilters() {
  document.getElementById('f-rvol').value   = 5;
  document.getElementById('f-gap').value    = 0;
  document.getElementById('f-mtf').checked  = false;
  document.getElementById('f-type').value   = '';
  applyFilters();
}

// ── TOASTS (enhanced — max 4, progress bar, icons) ───────────────────────────
var _toastCount = 0;
var TOAST_ICONS = {info: '\u2139\ufe0f', err: '\u274c', trade: '\u2705', alert: '\ud83d\udce1', '': '\u2139\ufe0f'};

function toast(msg, type) {
  var c = document.getElementById('toasts');
  // Auto-dismiss oldest if 4 already showing
  while (c.children.length >= 4) c.firstElementChild.remove();

  var t    = document.createElement('div');
  t.className = 'toast ' + (type || 'info');

  var icon  = TOAST_ICONS[type] || TOAST_ICONS[''];
  var dur   = 5000;

  t.innerHTML =
    '<div class="toast-inner">' +
      '<span class="toast-icon">' + icon + '</span>' +
      '<span class="toast-msg">' + escHtml(msg) + '</span>' +
    '</div>' +
    '<div class="toast-prog" style="animation-duration:' + dur + 'ms"></div>';

  c.appendChild(t);
  setTimeout(function(){
    t.style.transition = 'opacity .3s, transform .3s';
    t.style.opacity    = '0';
    t.style.transform  = 'translateX(110%)';
    setTimeout(function(){ t.remove(); }, 350);
  }, dur);
}

// ── TRADE CONFIRMATION MODAL ──────────────────────────────────────────────────
var modalQueue   = [];
var modalActive  = false;
var modalAlert   = null;
var cdInterval   = null;
var cdRemaining  = 60;
var RING_CIRC    = 113.1;

function playBeep() {
  if (localStorage.getItem('muted') === '1') return;
  try {
    var ctx  = new (window.AudioContext || window.webkitAudioContext)();
    var osc  = ctx.createOscillator();
    var gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.type = 'sine';
    osc.frequency.setValueAtTime(880, ctx.currentTime);
    osc.frequency.setValueAtTime(1109, ctx.currentTime + 0.12);
    gain.gain.setValueAtTime(0, ctx.currentTime);
    gain.gain.linearRampToValueAtTime(0.28, ctx.currentTime + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.45);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.5);
    ctx.close();
  } catch(e) {}
}

function showTradeModal(a) {
  if (!marketOpen) return;
  if (a.rr < 5.0 || a.rr > 10.0) return;
  if (autoTrade) {
    toast('\u26a1 ' + a.symbol + '  ' + a.entry_type + '  R:R ' + a.rr + ':1', 'alert');
    return;
  }
  if (modalActive) {
    modalQueue.push(a);
    var qi = document.getElementById('m-queue-info');
    var qt = document.getElementById('m-queue-txt');
    if (qi && qt) { qi.style.display = 'block'; qt.textContent = modalQueue.length + ' more signal' + (modalQueue.length > 1 ? 's' : '') + ' queued'; }
    return;
  }
  _openModal(a);
}

function _openModal(a) {
  modalActive = true;
  modalAlert  = a;
  var typeKey = (a.entry_type && a.entry_type.indexOf('Gap and Go') === 0) ? 'gap_go'
              : a.entry_type === 'Breakout' ? 'breakout'
              : a.entry_type === 'Pivot Reclaim' ? 'pivot' : 'pullback';
  document.getElementById('m-sym').textContent         = a.symbol;
  document.getElementById('m-type-badge').textContent  = a.entry_type;
  document.getElementById('m-type-badge').className    = 'modal-type-badge ' + typeKey;
  document.getElementById('m-entry').textContent       = '$' + a.entry.toFixed(2);
  document.getElementById('m-stop').textContent        = '$' + a.stop.toFixed(2);
  document.getElementById('m-target').textContent      = '$' + a.target.toFixed(2);
  var rr = document.getElementById('m-rr');
  rr.textContent = a.rr + ':1';
  rr.className   = 'hi-val ' + (a.rr >= 3 ? 'g' : '');
  document.getElementById('m-qty').textContent  = a.qty;
  var dollarRisk = ((a.entry - a.stop) * a.qty).toFixed(2);
  document.getElementById('m-risk').textContent = '$' + dollarRisk;
  document.getElementById('m-atr').textContent  = '$' + a.atr;
  var qi = document.getElementById('m-queue-info');
  if (qi) qi.style.display = 'none';
  var mr = document.getElementById('m-modify-row');
  if (mr) mr.style.display = 'none';
  var overlay = document.getElementById('trade-modal');
  overlay.style.display = 'block';
  playBeep();
  _startCountdown(60);
}

function _startCountdown(secs) {
  cdRemaining = secs;
  _updateRing(secs);
  document.getElementById('m-cd').textContent = secs;
  if (cdInterval) clearInterval(cdInterval);
  cdInterval = setInterval(function() {
    cdRemaining--;
    _updateRing(cdRemaining);
    document.getElementById('m-cd').textContent = cdRemaining;
    if (cdRemaining <= 0) { clearInterval(cdInterval); _dismissModal(false); }
  }, 1000);
}

function _updateRing(secs) {
  var ring = document.getElementById('m-ring');
  if (!ring) return;
  var pct    = secs / 60;
  var offset = RING_CIRC * (1 - pct);
  ring.style.strokeDashoffset = offset;
  ring.style.stroke = secs > 20 ? 'var(--accent)' : (secs > 8 ? 'var(--amber)' : 'var(--red)');
}

function _dismissModal(placed) {
  if (cdInterval) { clearInterval(cdInterval); cdInterval = null; }
  document.getElementById('trade-modal').style.display = 'none';
  var mr = document.getElementById('m-modify-row');
  if (mr) mr.style.display = 'none';
  modalActive = false;
  modalAlert  = null;
  if (modalQueue.length > 0) { setTimeout(function() { _openModal(modalQueue.shift()); }, 400); }
}

async function placeOrder() {
  if (!modalAlert) return;
  var a   = modalAlert;
  var btn = document.getElementById('m-place-btn');
  btn.disabled    = true;
  btn.textContent = 'Placing\u2026';
  try {
    var res = await fetch('/api/trade', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({symbol: a.symbol, qty: a.qty, entry: a.entry, stop: a.stop, target: a.target}),
    });
    var data = await res.json();
    if (data.error) throw new Error(data.error);
    toast('\u2714 Order placed: ' + a.qty + ' shares of ' + a.symbol + ' (bracket)', 'trade');
  } catch(err) {
    toast('\u2716 Order failed: ' + err.message, 'err');
  }
  btn.disabled    = false;
  btn.textContent = '\u25b6 PLACE TRADE';
  _dismissModal(true);
}

function skipTrade() {
  toast('Skipped ' + (modalAlert ? modalAlert.symbol : ''), 'info');
  _dismissModal(false);
}

// ── COMMAND PALETTE ───────────────────────────────────────────────────────────
var CMD_LIST = [
  {icon:'\ud83d\udce1', label:'Go to Alerts',           kbd:'B',     fn: function(){ switchTab('alerts');    closeCmdPalette(); }},
  {icon:'\u2605',       label:'Go to Watchlist',         kbd:'W',     fn: function(){ switchTab('watchlist'); closeCmdPalette(); }},
  {icon:'\ud83d\udcc3', label:'Go to Journal',           kbd:'J',     fn: function(){ switchTab('journal');  closeCmdPalette(); }},
  {icon:'\ud83d\udcca', label:'Run Backtest',            kbd:'',      fn: function(){ switchTab('backtest'); closeCmdPalette(); setTimeout(startBacktest, 200); }},
  {icon:'\u274c',       label:'Clear All Alerts',        kbd:'',      fn: function(){ clearAllAlerts();      closeCmdPalette(); }},
  {icon:'\ud83d\udd07', label:'Toggle Mute',             kbd:'',      fn: function(){ toggleMute();          closeCmdPalette(); }},
  {icon:'\u25b6',       label:'Start Scan',              kbd:'',      fn: function(){ startScan();           closeCmdPalette(); }},
  {icon:'\u25a0',       label:'Stop Scan',               kbd:'',      fn: function(){ stopScan();            closeCmdPalette(); }},
  {icon:'\u21bb',       label:'Resume Trading',          kbd:'',      fn: function(){ resumeTrading();       closeCmdPalette(); }},
  {icon:'\ud83d\udcc8', label:'Go to Positions',         kbd:'S',     fn: function(){ switchTab('positions');closeCmdPalette(); }},
];
var _cmdFiltered = CMD_LIST.slice();
var _cmdFocused  = 0;

function openCmdPalette() {
  _cmdFiltered = CMD_LIST.slice();
  _cmdFocused  = 0;
  document.getElementById('cmd-palette').classList.add('open');
  document.getElementById('cmd-input').value = '';
  renderCmdResults();
  setTimeout(function(){ document.getElementById('cmd-input').focus(); }, 50);
}

function closeCmdPalette() {
  document.getElementById('cmd-palette').classList.remove('open');
}

function filterCmds() {
  var q = (document.getElementById('cmd-input').value || '').toLowerCase().trim();
  _cmdFiltered = q
    ? CMD_LIST.filter(function(c){ return c.label.toLowerCase().indexOf(q) >= 0; })
    : CMD_LIST.slice();
  _cmdFocused = 0;
  renderCmdResults();
}

function renderCmdResults() {
  var el = document.getElementById('cmd-results');
  if (!_cmdFiltered.length) {
    el.innerHTML = '<div class="cmd-empty">No commands found</div>';
    return;
  }
  el.innerHTML = _cmdFiltered.map(function(c, i){
    return '<div class="cmd-item' + (i === _cmdFocused ? ' focused' : '') + '" onclick="execCmd(' + i + ')">' +
      '<span class="ci-icon">' + c.icon + '</span>' +
      '<span class="ci-label">' + escHtml(c.label) + '</span>' +
      (c.kbd ? '<span class="ci-kbd">' + c.kbd + '</span>' : '') +
    '</div>';
  }).join('');
}

function execCmd(idx) {
  var c = _cmdFiltered[idx];
  if (c && c.fn) c.fn();
}

function cmdKeydown(e) {
  if (e.key === 'Escape') { closeCmdPalette(); return; }
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    _cmdFocused = Math.min(_cmdFocused + 1, _cmdFiltered.length - 1);
    renderCmdResults();
    return;
  }
  if (e.key === 'ArrowUp') {
    e.preventDefault();
    _cmdFocused = Math.max(_cmdFocused - 1, 0);
    renderCmdResults();
    return;
  }
  if (e.key === 'Enter') {
    e.preventDefault();
    execCmd(_cmdFocused);
    return;
  }
}

// Close on backdrop click
document.getElementById('cmd-palette').addEventListener('click', function(e) {
  if (e.target === this) closeCmdPalette();
});

// ── MARKET CYCLE ─────────────────────────────────────────────────────────────
function updateCycleBadge(cycle, count) {
  var el = document.getElementById('cycle-bdg');
  if (!el) return;
  el.className   = 'cycle-bdg ' + cycle;
  var label      = cycle === 'hot' ? 'HOT' : (cycle === 'cold' ? 'COLD' : 'NEUTRAL');
  el.textContent = label + ' \u00b7 ' + count;
}

async function fetchMarketCycle() {
  try {
    var d = await fetch('/api/market_cycle').then(function(r){ return r.json(); });
    updateCycleBadge(d.cycle, d.gapper_count);
  } catch(e) {}
}

function toggleSettings() {
  var body  = document.getElementById('settings-body');
  var arrow = document.getElementById('settings-arrow');
  var vis   = body.style.display !== 'none';
  body.style.display = vis ? 'none' : 'block';
  arrow.textContent  = vis ? '\u25b6' : '\u25bc';
}

// ── JOURNAL ───────────────────────────────────────────────────────────────────
var jSort = {col: 'id', dir: 'desc'};

async function loadJournal() {
  var el = document.getElementById('jcont');
  el.innerHTML = '<div class="empty">Loading\u2026</div>';
  try {
    var rows = await fetch('/api/journal').then(function(r){ return r.json(); });
    renderJournal(rows);
  } catch(e) { el.innerHTML = '<div class="empty">Error: ' + e.message + '</div>'; }
}

function renderJournal(rows) {
  var el    = document.getElementById('jcont');
  var sumEl = document.getElementById('jrnl-summary');
  var open   = rows.filter(function(r){ return r.status === 'open'; }).length;
  var closed = rows.filter(function(r){ return r.status === 'closed'; });
  var wins   = closed.filter(function(r){ return (r.pnl || 0) > 0; }).length;
  var totPnl = closed.reduce(function(s, r){ return s + (r.pnl || 0); }, 0);
  var wr     = closed.length > 0 ? (wins / closed.length * 100).toFixed(0) + '%' : '\u2014';
  sumEl.innerHTML =
    mkJKV('Total', rows.length, '') + mkJKV('Open', open, '') +
    mkJKV('Closed', closed.length, '') +
    mkJKV('Win Rate', wr, parseFloat(wr) >= 50 ? 'g' : (closed.length ? 'r' : '')) +
    mkJKV('Total P&L', totPnl !== 0 ? (totPnl >= 0 ? '+$' : '-$') + Math.abs(totPnl).toFixed(2) : '$0.00', totPnl >= 0 ? 'g' : 'r');
  var bdg = document.getElementById('jrnl-bdg');
  if (rows.length) { bdg.style.display = ''; bdg.textContent = rows.length; }
  if (!rows.length) {
    el.innerHTML = '<div class="empty-state"><div class="es-icon">\ud83d\udcd3</div><div class="es-title">No trades logged yet</div><div class="es-desc">Trades placed via the modal are auto-logged here.</div></div>';
    return;
  }
  var sorted = rows.slice().sort(function(a, b) {
    var va = a[jSort.col], vb = b[jSort.col];
    if (va == null) va = ''; if (vb == null) vb = '';
    if (typeof va === 'string') return jSort.dir === 'asc' ? va.localeCompare(String(vb)) : String(vb).localeCompare(String(va));
    return jSort.dir === 'asc' ? va - vb : vb - va;
  });
  var cols = [
    {k:'id',lbl:'#',s:true},{k:'timestamp',lbl:'Time',s:true},{k:'symbol',lbl:'Symbol',s:true},
    {k:'entry_type',lbl:'Type',s:false},{k:'entry_price',lbl:'Entry',s:true},{k:'stop_price',lbl:'Stop',s:false},
    {k:'target_price',lbl:'Target',s:false},{k:'shares',lbl:'Qty',s:true},{k:'risk_dollars',lbl:'Risk $',s:true},
    {k:'rr_ratio',lbl:'R:R',s:true},{k:'rvol',lbl:'RVOL',s:true},{k:'gap_pct',lbl:'Gap%',s:true},
    {k:'status',lbl:'Status',s:true},{k:'exit_price',lbl:'Exit',s:false},{k:'pnl',lbl:'P&L',s:true},
  ];
  var thd = cols.map(function(c) {
    var sc = jSort.col === c.k ? (' sort-' + jSort.dir) : '';
    var at = c.s ? ' class="sortable' + sc + '" onclick="jSortBy(\'' + c.k + '\')"' : '';
    return '<th' + at + '>' + c.lbl + '</th>';
  }).join('') + '<th></th>';
  var tbody = sorted.map(function(r) {
    var ts  = r.timestamp ? new Date(r.timestamp).toLocaleString() : '\u2014';
    var pnl = r.pnl != null ? ((r.pnl >= 0 ? '+$' : '-$') + Math.abs(r.pnl).toFixed(2)) : '\u2014';
    var pc  = r.pnl > 0 ? 'pos' : (r.pnl < 0 ? 'neg' : '');
    var sc  = r.status === 'open' ? 'style="color:var(--amber)"' : (r.pnl > 0 ? 'class="pos"' : 'class="neg"');
    var cb  = r.status === 'open' ? '<button class="btn sm" onclick="closeJournalTrade(' + r.id + ')">CLOSE</button>' : '';
    return '<tr>' +
      '<td style="color:var(--text-muted)">' + r.id + '</td>' +
      '<td style="color:var(--text-muted);font-size:9px">' + ts + '</td>' +
      '<td class="sym">' + escHtml(r.symbol || '') + '</td>' +
      '<td style="color:var(--text-muted);font-size:10px">' + escHtml(r.entry_type || '') + '</td>' +
      '<td style="color:var(--accent)">' + (r.entry_price  ? '$' + r.entry_price.toFixed(2)  : '\u2014') + '</td>' +
      '<td style="color:var(--red)">'    + (r.stop_price   ? '$' + r.stop_price.toFixed(2)   : '\u2014') + '</td>' +
      '<td style="color:var(--green)">'  + (r.target_price ? '$' + r.target_price.toFixed(2) : '\u2014') + '</td>' +
      '<td>' + (r.shares || '\u2014') + '</td>' +
      '<td style="color:var(--red)">'    + (r.risk_dollars ? '$' + r.risk_dollars.toFixed(2) : '\u2014') + '</td>' +
      '<td>' + (r.rr_ratio ? r.rr_ratio.toFixed(1) + ':1' : '\u2014') + '</td>' +
      '<td>' + (r.rvol ? r.rvol.toFixed(1) + 'x' : '\u2014') + '</td>' +
      '<td>' + (r.gap_pct != null ? (r.gap_pct >= 0 ? '+' : '') + r.gap_pct.toFixed(1) + '%' : '\u2014') + '</td>' +
      '<td ' + sc + '>' + (r.status || '').toUpperCase() + '</td>' +
      '<td>' + (r.exit_price ? '$' + r.exit_price.toFixed(2) : '\u2014') + '</td>' +
      '<td><span class="pnl-pill ' + pc + '">' + pnl + '</span></td>' +
      '<td>' + cb + '</td></tr>';
  }).join('');
  el.innerHTML = '<div style="overflow-x:auto"><table class="dtbl"><thead><tr>' + thd + '</tr></thead><tbody>' + tbody + '</tbody></table></div>';
}

function mkJKV(label, val, cls) {
  return '<div class="jrnl-kv"><div class="k">' + label + '</div><div class="v ' + cls + '">' + val + '</div></div>';
}

function jSortBy(col) {
  jSort.dir = (jSort.col === col && jSort.dir === 'desc') ? 'asc' : 'desc';
  jSort.col = col;
  loadJournal();
}

async function closeJournalTrade(id) {
  var exit = prompt('Exit price for this trade:');
  if (!exit) return;
  var ep = parseFloat(exit);
  if (isNaN(ep) || ep <= 0) { toast('Invalid price', 'err'); return; }
  try {
    var res  = await fetch('/api/journal/' + id + '/close', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({exit_price: ep}),
    });
    var data = await res.json();
    if (data.error) throw new Error(data.error);
    var pnlStr = data.pnl >= 0 ? '+$' + data.pnl.toFixed(2) : '-$' + Math.abs(data.pnl).toFixed(2);
    toast('Trade closed: ' + pnlStr, data.pnl >= 0 ? 'trade' : 'err');
    loadJournal();
  } catch(e) { toast('Error: ' + e.message, 'err'); }
}

async function exportJournal() {
  window.location.href = '/api/journal/export';
}

// ── NEWS ──────────────────────────────────────────────────────────────────────
var _newsClientCache = {};

async function fetchAndInjectNews(symbol, cardId) {
  var card = document.getElementById(cardId);
  if (!card) return;
  var stub = document.createElement('div');
  stub.className = 'news-section';
  stub.innerHTML = '<div class="news-hdr">News</div><div class="news-loading">Loading\u2026</div>';
  card.appendChild(stub);
  var now = Date.now();
  if (_newsClientCache[symbol] && now - _newsClientCache[symbol].ts < 300000) {
    injectNews(card, _newsClientCache[symbol].data);
    return;
  }
  try {
    var items = await fetch('/api/news/' + symbol).then(function(r){ return r.json(); });
    _newsClientCache[symbol] = {data: items, ts: now};
    injectNews(card, items);
  } catch(e) {
    var s = card.querySelector('.news-section');
    if (s) s.innerHTML = '<div class="news-hdr">News</div><div class="news-loading">Unavailable</div>';
  }
}

function injectNews(card, items) {
  var existing = card.querySelector('.news-section');
  if (existing) existing.remove();
  if (!items || !items.length) return;
  var top2 = items.slice(0, 2);
  var html = '<div class="news-section"><div class="news-hdr">News</div>' +
    top2.map(function(n) {
      var ago = n.created_at ? timeAgo(new Date(n.created_at)) : '';
      return '<div class="news-item">' +
        '<div class="news-headline">' + escHtml(n.headline) + '</div>' +
        '<div class="news-meta">' + escHtml(n.source || '') + (ago ? ' \u00b7 ' + ago : '') + '</div>' +
      '</div>';
    }).join('') + '</div>';
  card.insertAdjacentHTML('beforeend', html);
}

function timeAgo(date) {
  var diff = Math.floor((Date.now() - date.getTime()) / 1000);
  if (diff < 60)    return diff + 's ago';
  if (diff < 3600)  return Math.floor(diff / 60) + 'm ago';
  if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
  return Math.floor(diff / 86400) + 'd ago';
}

// ── SECTOR HEATMAP ────────────────────────────────────────────────────────────
var SECTOR_ETFS = {
  'Tech':'XLK','Fin':'XLF','Health':'XLV','Energy':'XLE','Util':'XLU',
  'Cons D':'XLY','Cons S':'XLP','Ind':'XLI','Mat':'XLB','RE':'XLRE','Comm':'XLC'
};

async function loadSectorHeatmap() {
  var strip = document.getElementById('heatmap-strip');
  if (!strip) return;
  try {
    var data = await fetch('/api/sector_heatmap').then(function(r){ return r.json(); });
    if (!data || data.error || !data.length) return;
    var html = '<span class="heatmap-lbl">Sectors</span>';
    data.forEach(function(s) {
      var pct = s.pct || 0;
      var cls = pct >= 2 ? 'pos-3' : pct >= 1 ? 'pos-2' : pct >= 0.2 ? 'pos-1'
              : pct <= -2 ? 'neg-3' : pct <= -1 ? 'neg-2' : pct <= -0.2 ? 'neg-1' : 'zero';
      var sign = pct >= 0 ? '+' : '';
      var arr  = pct > 0 ? '\u25b2' : (pct < 0 ? '\u25bc' : '');
      html += '<div class="hm-tile ' + cls + '" title="' + escHtml(s.ticker || s.label) + ': ' + sign + pct.toFixed(2) + '%">' +
        '<div class="ht-lbl">' + escHtml(s.label) + '</div>' +
        '<div class="ht-etf">' + escHtml(s.ticker || '') + '</div>' +
        '<div class="ht-pct">' + sign + pct.toFixed(1) + '%</div>' +
        '<div class="ht-arr">' + arr + '</div>' +
      '</div>';
    });
    strip.innerHTML = html;
  } catch(e) {}
}

// ── WATCHLIST ─────────────────────────────────────────────────────────────────
async function loadWatchlist() {
  try {
    var d = await fetch('/api/watchlist').then(function(r){ return r.json(); });
    renderWatchlistPills(d.symbols || []);
    (d.alerts || []).forEach(function(a){ renderWatchlistAlert(a); });
  } catch(e) {}
}

function renderWatchlistPills(syms) {
  var el = document.getElementById('wl-pills');
  if (!el) return;
  el.innerHTML = syms.map(function(s) {
    return '<span class="wl-pill">' + escHtml(s) +
      '<button onclick="removeFromWatchlist(\'' + escHtml(s) + '\')" title="Remove">&#10005;</button>' +
    '</span>';
  }).join('');
}

async function addToWatchlist() {
  var inp = document.getElementById('wl-inp');
  var sym = (inp.value || '').toUpperCase().trim();
  if (!sym) return;
  try {
    var d = await fetch('/api/watchlist', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({symbol: sym}),
    }).then(function(r){ return r.json(); });
    if (d.error) { toast(d.error, 'err'); return; }
    inp.value = '';
    renderWatchlistPills(d.symbols || []);
    toast('\u2605 Added ' + sym + ' to watchlist', 'info');
  } catch(e) { toast('Error: ' + e.message, 'err'); }
}

async function removeFromWatchlist(sym) {
  try {
    var d = await fetch('/api/watchlist/' + encodeURIComponent(sym), {method:'DELETE'}).then(function(r){ return r.json(); });
    renderWatchlistPills(d.symbols || []);
    toast('Removed ' + sym + ' from watchlist', 'info');
  } catch(e) {}
}

document.getElementById('wl-inp').addEventListener('keydown', function(e) {
  if (e.key === 'Enter') addToWatchlist();
});

function renderWatchlistAlert(a) {
  var grid = document.getElementById('wl-agrid');
  var noAlerts = document.getElementById('wl-no-alerts');
  if (!grid) return;
  grid.style.display = 'grid';
  if (noAlerts) noAlerts.style.display = 'none';
  var old = document.getElementById('wl-ac-' + a.symbol);
  if (old) old.remove();
  var typeKey = (a.entry_type && a.entry_type.indexOf('Gap and Go') === 0) ? 'gap_go'
              : a.entry_type === 'Breakout' ? 'breakout'
              : a.entry_type === 'Pivot Reclaim' ? 'pivot' : 'pullback';
  var card = document.createElement('div');
  card.className = 'alert-card watchlist ' + typeKey;
  card.id        = 'wl-ac-' + a.symbol;
  var ts = new Date(a.timestamp).toLocaleTimeString();
  card.innerHTML =
    '<div class="ah"><div><div class="a-sym">' + escHtml(a.symbol) + '</div><div class="a-ts">' + ts + '</div></div>' +
    '<div class="a-badges"><span class="a-price-pill">$' + a.entry.toFixed(2) + '</span>' +
    '<span class="a-typ ' + typeKey + '">' + escHtml(a.entry_type) + '</span></div></div>' +
    '<div class="a-px">' +
      '<div class="pc"><div class="l">Entry</div><div class="v e">$' + a.entry.toFixed(2) + '</div></div>' +
      '<div class="pc"><div class="l">Stop</div><div class="v s">$' + a.stop.toFixed(2) + '</div></div>' +
      '<div class="pc"><div class="l">Target</div><div class="v t">$' + a.target.toFixed(2) + '</div></div>' +
    '</div>' +
    '<div class="a-st"><span>R:R <span class="sv g">' + a.rr + ':1</span></span><span>RVOL <span class="sv">' + a.rvol + '\u00d7</span></span></div>' +
    (a.mtf_confirmed != null
      ? '<div class="' + (a.mtf_confirmed ? 'mtf-yes' : 'mtf-no') + '">' +
          (a.mtf_confirmed ? '\u2714 MTF Confirmed' : '\u26a0 MTF Unconfirmed') + '</div>' : '');
  grid.insertBefore(card, grid.firstChild);
}

// ── KEYBOARD SHORTCUTS ────────────────────────────────────────────────────────
document.addEventListener('keydown', function(e) {
  var tag = (document.activeElement || {}).tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
  if ((e.metaKey || e.ctrlKey) && e.key === 'k') { e.preventDefault(); openCmdPalette(); return; }
  switch (e.key) {
    case 'Enter':  if (modalActive) { e.preventDefault(); placeOrder(); } break;
    case 'Escape':
      if (modalActive) { e.preventDefault(); skipTrade(); }
      else closeCmdPalette();
      break;
    case 'b': case 'B': switchTab('alerts');    break;
    case 'w': case 'W': switchTab('watchlist'); break;
    case 's': case 'S': switchTab('positions'); break;
    case 'j': case 'J': switchTab('journal');   break;
  }
});

// ── MARKET STATUS ─────────────────────────────────────────────────────────────
async function checkMarketStatus() {
  try {
    var ms = await fetch('/api/market_status').then(function(r){return r.json();});
    marketOpen = !!ms.open;
    var el = document.getElementById('stxt');
    if (!ms.open && !document.getElementById('sdot').classList.contains('scan')) {
      el.textContent = 'MARKET CLOSED \u2014 ' + (ms.msg || '');
      el.style.color = 'var(--amber)';
    } else {
      el.style.color = '';
    }
  } catch(e) {}
}

// ── MUTE ─────────────────────────────────────────────────────────────────────
function toggleMute() {
  var muted = localStorage.getItem('muted') === '1';
  muted = !muted;
  localStorage.setItem('muted', muted ? '1' : '0');
  var btn = document.getElementById('mute-btn');
  if (btn) { btn.textContent = muted ? '\uD83D\uDD07' : '\uD83D\uDD14'; btn.classList.toggle('muted', muted); }
  toast('Sound ' + (muted ? 'muted' : 'enabled'), 'info');
}
(function initMuteState() {
  var muted = localStorage.getItem('muted') === '1';
  var btn = document.getElementById('mute-btn');
  if (btn) { btn.textContent = muted ? '\uD83D\uDD07' : '\uD83D\uDD14'; btn.classList.toggle('muted', muted); }
})();

// ── CLEAR ALERTS ─────────────────────────────────────────────────────────────
async function clearAllAlerts() {
  try {
    await fetch('/api/alerts', {method: 'DELETE'});
    alerts = [];
    document.getElementById('agrid').innerHTML = '';
    document.getElementById('bdg').textContent = '0';
    document.getElementById('palerts').textContent = '0';
    document.getElementById('wl-agrid').innerHTML = '';
    document.getElementById('wl-no-alerts').style.display = '';
    document.getElementById('wl-agrid').style.display = 'none';
    document.getElementById('skeleton-wrap').style.display = 'grid';
    document.getElementById('ph').style.display = 'none';
    toast('Alerts cleared', 'info');
  } catch(e) { toast('Error clearing alerts: ' + e.message, 'err'); }
}

// ── SCAN DOT ──────────────────────────────────────────────────────────────────
async function updateScanDot() {
  try {
    var d = await fetch('/api/scan_status').then(function(r){ return r.json(); });
    var dot   = document.getElementById('scan-dot');
    var ageEl = document.getElementById('scan-age');
    if (!dot) return;
    var ts = d.last_scan;
    if (!ts) { dot.className = 'dot dot-red'; if (ageEl) ageEl.textContent = ''; return; }
    var ageMin = Math.floor((Date.now() / 1000 - ts) / 60);
    if (ageMin < 10) { dot.className = 'dot dot-green'; if (ageEl) ageEl.textContent = ageMin + 'm ago'; }
    else             { dot.className = 'dot dot-red';   if (ageEl) ageEl.textContent = ageMin + 'm ago'; }
  } catch(e) {}
}

// ── ACCOUNT SUMMARY (Positions Tab) ──────────────────────────────────────────
async function loadAccountSummary() {
  var bar = document.getElementById('pos-stat-bar');
  if (bar) bar.style.display = 'grid';
  try {
    var d = await fetch('/api/account_summary').then(function(r){ return r.json(); });
    if (d.error) return;
    var fmt = function(n){ return '$' + Math.abs(n).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}); };
    var eqEl = document.getElementById('ps-equity'); if (eqEl) eqEl.textContent = fmt(d.equity);
    var dpEl = document.getElementById('ps-dpnl');
    if (dpEl) { var dp = d.daily_pnl || 0; dpEl.textContent = (dp >= 0 ? '+' : '-') + fmt(dp); dpEl.className = 'v ' + (dp >= 0 ? 'g' : 'r'); }
    var bpEl = document.getElementById('ps-bp'); if (bpEl) bpEl.textContent = fmt(d.buying_power);
    var cntEl = document.getElementById('ps-count'); if (cntEl) cntEl.textContent = (d.positions || []).length;
    var el = document.getElementById('pcont');
    var ps = d.positions || [];
    if (!ps.length) {
      el.innerHTML = '<div class="empty-state"><div class="es-icon">\u25c7</div><div class="es-title">No open positions</div></div>';
      return;
    }
    var trailMap = {};
    try {
      var ts = await fetch('/api/trailing_status').then(function(r){ return r.json(); });
      trailMap = ts || {};
    } catch(e) {}
    var rows = ps.map(function(p) {
      var pnl = p.unrealized_pl || 0;
      var ppc = (p.unrealized_plpc || 0) * 100;
      var cls = pnl >= 0 ? 'pos' : 'neg';
      var sgn = pnl >= 0 ? '+' : '-';
      var trail = trailMap[p.symbol];
      var stopPhase = trail ? '<span class="trail-' + trail.phase + '">' + trail.phase.toUpperCase() + '</span>' : '\u2014';
      return '<tr>' +
        '<td class="sym">' + escHtml(p.symbol) + '</td>' +
        '<td>' + p.qty + '</td>' +
        '<td>$' + p.avg_entry_price.toFixed(2) + '</td>' +
        '<td>$' + p.current_price.toFixed(2) + '</td>' +
        '<td>$' + p.market_value.toLocaleString('en-US',{minimumFractionDigits:2}) + '</td>' +
        '<td><span class="pnl-pill ' + cls + '">' + sgn + '$' + Math.abs(pnl).toFixed(2) + '</span></td>' +
        '<td class="' + cls + '">' + sgn + Math.abs(ppc).toFixed(2) + '%</td>' +
        '<td>' + stopPhase + '</td>' +
        '<td><button class="btn danger sm" onclick="closePos(\'' + escHtml(p.symbol) + '\')">CLOSE</button></td></tr>';
    }).join('');
    el.innerHTML = '<table class="dtbl"><thead><tr>' +
      '<th>Symbol</th><th>Qty</th><th>Avg Cost</th><th>Current</th>' +
      '<th>Mkt Value</th><th>Unreal P&amp;L</th><th>P&amp;L %</th><th>Stop Phase</th><th></th>' +
      '</tr></thead><tbody>' + rows + '</tbody></table>';
  } catch(e) {}
}

// ── PRE-MARKET GAPPERS ───────────────────────────────────────────────────────
var _pmExpanded = false;

function togglePmSection() {
  _pmExpanded = !_pmExpanded;
  var body  = document.getElementById('pm-body');
  var arrow = document.getElementById('pm-arrow');
  if (body)  body.style.display  = _pmExpanded ? 'block' : 'none';
  if (arrow) arrow.textContent   = _pmExpanded ? '\u25bc' : '\u25b6';
}

async function fetchPremarket() {
  try {
    var data = await fetch('/api/premarket_gappers').then(function(r){ return r.json(); });
    var grid  = document.getElementById('pm-grid');
    var count = document.getElementById('pm-count');
    if (!grid) return;
    if (count) count.textContent = data.length;
    if (!data.length) { grid.innerHTML = '<div class="pm-empty">No gappers yet &mdash; runs 4:00&ndash;9:30 AM ET</div>'; return; }
    grid.innerHTML = data.map(function(g) {
      var sign = g.gap_pct >= 0 ? '+' : '';
      var vol  = (g.pm_volume / 1000).toFixed(0) + 'K';
      return '<div class="pm-card">' +
        '<div><span class="pm-sym">' + escHtml(g.symbol) + '</span>' +
        '<span class="pm-gap-bdg">' + sign + g.gap_pct.toFixed(1) + '% gap</span></div>' +
        '<div class="pm-meta">PM Price: $' + g.pm_price.toFixed(2) +
          ' &nbsp;&middot;&nbsp; Prev Close: $' + g.prev_close.toFixed(2) +
          ' &nbsp;&middot;&nbsp; Vol: ' + vol + '</div>' +
        '<button class="pm-watch" onclick="addToWatchlistSym(\'' + escHtml(g.symbol) + '\')">+ Watch</button>' +
      '</div>';
    }).join('');
  } catch(e) {}
}

async function addToWatchlistSym(sym) {
  try {
    var d = await fetch('/api/watchlist', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({symbol: sym}),
    }).then(function(r){ return r.json(); });
    if (d.error) { toast(d.error, 'err'); return; }
    renderWatchlistPills(d.symbols || []);
    toast('\u2605 Added ' + sym + ' to watchlist', 'info');
  } catch(e) { toast('Error: ' + e.message, 'err'); }
}

// ── DAILY WATCHLIST ───────────────────────────────────────────────────────────
async function loadDailyWatchlist() {
  try {
    var d = await fetch('/api/daily_watchlist').then(function(r){ return r.json(); });
    var count = d.count || 0;
    var badge = document.getElementById('wl-count');
    if (badge) badge.textContent = 'Daily WL: ' + count;
    window._wlData = d;
  } catch(e) {}
}

function showWatchlistModal() {
  var d = window._wlData || {items:[], date: null, count: 0};
  var lbl = document.getElementById('wl-date-label');
  if (lbl) lbl.textContent = d.date ? ('Date: ' + d.date + ' \u2014 ' + d.count + ' symbols') : 'No data yet';
  var tbody = document.getElementById('wl-tbody');
  if (!tbody) return;
  if (!d.items || d.items.length === 0) {
    tbody.innerHTML = '<tr><td colspan="3" style="color:var(--text-muted);padding:12px 8px;font-size:11px">No symbols in daily watchlist yet.</td></tr>';
  } else {
    tbody.innerHTML = d.items.map(function(row, i) {
      return '<tr style="border-bottom:1px solid var(--border)">'
        + '<td style="padding:4px 8px;color:var(--text-muted)">' + (i+1) + '</td>'
        + '<td style="padding:4px 8px;color:var(--text-primary);font-weight:700">' + row.symbol + '</td>'
        + '<td style="padding:4px 8px;text-align:right;color:#a5b4fc">' + row.rvol.toFixed(2) + '\u00d7</td>'
        + '</tr>';
    }).join('');
  }
  document.getElementById('wl-modal').style.display = 'flex';
}

document.addEventListener('click', function(e) {
  var modal = document.getElementById('wl-modal');
  if (modal && e.target === modal) modal.style.display = 'none';
});

// ── RISK STATUS ───────────────────────────────────────────────────────────────
async function loadRiskStatus() {
  try {
    var s = await fetch('/api/risk_status').then(function(r){ return r.json(); });
    document.getElementById('rg-trades').textContent = s.daily_trade_count;
    document.getElementById('rg-max-t').textContent  = s.daily_max_trades;
    document.getElementById('rg-loss').textContent   = s.daily_loss_total.toFixed(0);
    document.getElementById('rg-max-l').textContent  = s.daily_max_loss.toFixed(0);
    var halted    = !!s.trading_halted;
    var gauge     = document.getElementById('risk-gauge');
    var resumeBtn = document.getElementById('resume-btn');
    if (gauge)     gauge.style.color     = halted ? 'var(--red)' : 'var(--text-muted)';
    if (resumeBtn) resumeBtn.style.display = halted ? 'inline-block' : 'none';
  } catch(e) {}
}

async function resumeTrading() {
  try {
    await fetch('/api/risk_status/resume', {method:'POST'});
    toast('Trading resumed', 'info');
    loadRiskStatus();
  } catch(e) { toast('Error resuming: ' + e.message, 'err'); }
}

// ── INIT ──────────────────────────────────────────────────────────────────────
connectSSE();
refreshAccount();
checkMarketStatus();
fetchMarketCycle();
loadBtSummary();
loadSectorHeatmap();
loadWatchlist();
loadRiskStatus();
setInterval(refreshAccount, 30000);
setInterval(checkMarketStatus, 60000);
setInterval(fetchMarketCycle, 300000);
setInterval(loadSectorHeatmap, 300000);
fetchPremarket();
setInterval(fetchPremarket, 300000);
updateScanDot();
setInterval(updateScanDot, 30000);
setInterval(loadAccountSummary, 30000);
loadDailyWatchlist();
setInterval(loadDailyWatchlist, 60000);
setInterval(loadRiskStatus, 30000);

['m-mod-entry','m-mod-stop','m-mod-target','m-mod-qty'].forEach(function(id) {
  var el = document.getElementById(id);
  if (el) el.addEventListener('input', _recalcModify);
});

// ── Load user info bar ────────────────────────────────────────────────────────
(function loadUserBar() {
  fetch('/auth/me').then(function(r){ return r.ok ? r.json() : null; }).then(function(u) {
    if (!u || u.error) return;
    var nameEl   = document.getElementById('user-name');
    var avatarEl = document.getElementById('user-avatar');
    if (nameEl)   nameEl.textContent = u.name || u.email || '';
    if (avatarEl && u.picture) {
      avatarEl.src = u.picture;
      avatarEl.style.display = 'block';
    }
  }).catch(function(){});
})();
</script>
</body>
</html>"""

# ── Entry Point ──────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    t = threading.Thread(target=scanner_loop, daemon=True, name="scanner")
    t.start()
    ts_thread = threading.Thread(target=trailing_stop_manager, daemon=True, name="trailing_stop")
    ts_thread.start()
    wl_thread = threading.Thread(target=watchlist_scan_loop, daemon=True, name="watchlist_scan")
    wl_thread.start()
    pm_thread = threading.Thread(target=premarket_scan_loop, daemon=True, name="premarket_scan")
    pm_thread.start()
    log.info("Scanner + trailing-stop + watchlist + premarket threads started — open http://localhost:5001")
    app.run(host="0.0.0.0", port=5001, debug=os.environ.get("FLASK_ENV") == "development", threaded=True)
