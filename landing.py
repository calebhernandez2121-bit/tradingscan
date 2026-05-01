"""
landing.py — Public marketing / sign-in page
"""
from flask import Blueprint, render_template_string, session, redirect

landing_bp = Blueprint("landing", __name__)

_LANDING_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>TradingAlerts &mdash; Institutional-grade stock scanning</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg0:   #05050f;
      --bg1:   #0a0a1a;
      --bg2:   #0f0f22;
      --bg3:   #161630;
      --brd:   #1e1e40;
      --brd2:  #2a2a55;
      --txt:   #b0b0d0;
      --hi:    #eeeeff;
      --dim:   #50507a;
      --grn:   #00e87c;
      --grn2:  #00b860;
      --red:   #ff3d5a;
      --cyn:   #00d4ff;
      --yel:   #ffd600;
      --pur:   #9d5cff;
      --mono:  'Space Mono', monospace;
      --sans:  'Inter', sans-serif;
    }
    body {
      background: var(--bg0);
      color: var(--txt);
      font-family: var(--sans);
      min-height: 100vh;
      line-height: 1.6;
    }

    /* ── NAV ── */
    nav {
      position: sticky; top: 0; z-index: 50;
      background: rgba(5,5,15,.85);
      backdrop-filter: blur(12px);
      border-bottom: 1px solid var(--brd);
      padding: 0 32px;
      height: 60px;
      display: flex; align-items: center; justify-content: space-between;
    }
    .nav-logo {
      font-size: 18px; font-weight: 800;
      background: linear-gradient(135deg, var(--grn), var(--cyn));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
      letter-spacing: -0.5px;
    }
    .nav-sign-in {
      display: flex; align-items: center; gap: 8px;
      background: #fff; color: #333;
      border: none; border-radius: 6px;
      padding: 8px 16px; font-size: 14px; font-weight: 600;
      cursor: pointer; text-decoration: none;
      transition: box-shadow .15s;
    }
    .nav-sign-in:hover { box-shadow: 0 0 0 3px rgba(255,255,255,.2); }
    .g-icon { width: 18px; height: 18px; flex-shrink: 0; }

    /* ── HERO ── */
    .hero {
      max-width: 860px; margin: 0 auto;
      padding: 100px 24px 80px;
      text-align: center;
    }
    .hero-badge {
      display: inline-block;
      background: rgba(0,232,124,.1);
      border: 1px solid rgba(0,232,124,.3);
      border-radius: 20px;
      padding: 4px 14px; font-size: 12px;
      color: var(--grn); letter-spacing: 1px;
      text-transform: uppercase; margin-bottom: 24px;
    }
    .hero h1 {
      font-size: clamp(36px, 6vw, 72px);
      font-weight: 800; line-height: 1.08;
      color: var(--hi); letter-spacing: -1.5px;
      margin-bottom: 20px;
    }
    .hero h1 span { background: linear-gradient(135deg, var(--grn), var(--cyn));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .hero p {
      font-size: clamp(16px, 2.5vw, 20px);
      color: var(--txt); max-width: 560px; margin: 0 auto 40px;
    }
    .cta-group { display: flex; gap: 14px; justify-content: center; flex-wrap: wrap; }
    .cta-primary {
      background: linear-gradient(135deg, var(--grn2), var(--grn));
      color: #000; font-weight: 700; font-size: 16px;
      padding: 14px 32px; border-radius: 8px;
      border: none; cursor: pointer; text-decoration: none;
      letter-spacing: .3px;
      box-shadow: 0 0 28px rgba(0,232,124,.25);
      transition: box-shadow .2s, transform .1s;
      display: inline-block;
    }
    .cta-primary:hover { box-shadow: 0 0 40px rgba(0,232,124,.4); transform: translateY(-1px); }
    .cta-secondary {
      background: transparent; color: var(--txt);
      border: 1px solid var(--brd2); border-radius: 8px;
      padding: 14px 28px; font-size: 16px; cursor: pointer;
      text-decoration: none; transition: border-color .15s, color .15s;
      display: inline-block;
    }
    .cta-secondary:hover { border-color: var(--dim); color: var(--hi); }
    .hero-sub { margin-top: 16px; font-size: 13px; color: var(--dim); }

    /* ── FEATURES ── */
    .section { max-width: 1040px; margin: 0 auto; padding: 80px 24px; }
    .section-title { text-align: center; font-size: 28px; font-weight: 700;
      color: var(--hi); margin-bottom: 48px; letter-spacing: -.5px; }
    .features { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px,1fr)); gap: 20px; }
    .feat-card {
      background: var(--bg2); border: 1px solid var(--brd);
      border-radius: 12px; padding: 28px 24px;
      transition: border-color .2s, transform .15s;
    }
    .feat-card:hover { border-color: var(--brd2); transform: translateY(-2px); }
    .feat-icon { font-size: 32px; margin-bottom: 14px; }
    .feat-card h3 { font-size: 18px; font-weight: 700; color: var(--hi); margin-bottom: 10px; }
    .feat-card p { font-size: 14px; color: var(--txt); line-height: 1.65; }
    .feat-tag {
      display: inline-block; margin-top: 14px;
      font-size: 11px; font-weight: 600; letter-spacing: .8px;
      text-transform: uppercase; padding: 3px 10px;
      border-radius: 4px;
    }
    .tag-green  { background: rgba(0,232,124,.1); color: var(--grn); border: 1px solid rgba(0,232,124,.25); }
    .tag-cyan   { background: rgba(0,212,255,.1); color: var(--cyn); border: 1px solid rgba(0,212,255,.25); }
    .tag-purple { background: rgba(157,92,255,.1); color: var(--pur); border: 1px solid rgba(157,92,255,.25); }

    /* ── SOCIAL PROOF ── */
    .metrics-row {
      display: flex; gap: 0; justify-content: center;
      border: 1px solid var(--brd); border-radius: 12px;
      overflow: hidden; max-width: 640px; margin: 0 auto 80px;
    }
    .metric { flex: 1; text-align: center; padding: 24px 16px; border-right: 1px solid var(--brd); }
    .metric:last-child { border-right: none; }
    .metric .num { font-size: 28px; font-weight: 800; color: var(--hi); margin-bottom: 4px; }
    .metric .lbl { font-size: 12px; color: var(--dim); text-transform: uppercase; letter-spacing: .8px; }
    .num.g { color: var(--grn); }

    /* ── PRICING ── */
    .pricing-wrap { max-width: 400px; margin: 0 auto 80px; }
    .pricing-card {
      background: var(--bg2); border: 1px solid var(--brd2);
      border-radius: 16px; padding: 36px 32px; text-align: center;
      position: relative; overflow: hidden;
    }
    .pricing-card::before {
      content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
      background: linear-gradient(90deg, var(--grn), var(--cyn));
    }
    .pricing-badge {
      display: inline-block; background: rgba(0,232,124,.12);
      border: 1px solid rgba(0,232,124,.3); border-radius: 20px;
      padding: 4px 14px; font-size: 11px; color: var(--grn);
      text-transform: uppercase; letter-spacing: 1px; margin-bottom: 20px;
    }
    .price-main { font-size: 56px; font-weight: 800; color: var(--hi);
      letter-spacing: -2px; line-height: 1; }
    .price-main sup { font-size: 24px; vertical-align: super; margin-right: 2px; }
    .price-period { font-size: 14px; color: var(--dim); margin-bottom: 8px; margin-top: 4px; }
    .trial-pill {
      display: inline-block;
      background: linear-gradient(135deg, rgba(0,212,255,.15), rgba(157,92,255,.15));
      border: 1px solid rgba(0,212,255,.25);
      border-radius: 20px; padding: 6px 18px;
      font-size: 14px; font-weight: 600; color: var(--cyn);
      margin: 12px 0 24px;
    }
    .pricing-features { text-align: left; margin-bottom: 28px; }
    .pricing-features li {
      list-style: none; padding: 7px 0;
      border-bottom: 1px solid var(--brd);
      font-size: 14px; color: var(--txt);
      display: flex; align-items: center; gap: 10px;
    }
    .pricing-features li:last-child { border-bottom: none; }
    .chk { color: var(--grn); font-size: 16px; flex-shrink: 0; }

    /* ── SIGN-IN SECTION ── */
    .signin-section { text-align: center; padding: 60px 24px 80px; }
    .signin-section h2 { font-size: 28px; font-weight: 700; color: var(--hi); margin-bottom: 10px; }
    .signin-section p  { color: var(--dim); margin-bottom: 28px; }
    .google-btn {
      display: inline-flex; align-items: center; gap: 10px;
      background: #fff; color: #333;
      border: none; border-radius: 8px;
      padding: 12px 24px; font-size: 15px; font-weight: 600;
      cursor: pointer; text-decoration: none;
      box-shadow: 0 2px 10px rgba(0,0,0,.35);
      transition: box-shadow .15s, transform .1s;
    }
    .google-btn:hover { box-shadow: 0 4px 20px rgba(0,0,0,.45); transform: translateY(-1px); }

    /* ── FOOTER ── */
    footer {
      border-top: 1px solid var(--brd);
      padding: 28px 24px; text-align: center;
      font-size: 12px; color: var(--dim);
    }
    footer a { color: var(--dim); text-decoration: none; margin: 0 10px; }
    footer a:hover { color: var(--txt); }
    .legal-links { margin-top: 8px; }
  </style>
</head>
<body>

<!-- NAV -->
<nav>
  <span class="nav-logo">TradingAlerts</span>
  <a href="/auth/login" class="nav-sign-in">
    <svg class="g-icon" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/>
      <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
      <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z" fill="#FBBC05"/>
      <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
    </svg>
    Sign in with Google
  </a>
</nav>

<!-- HERO -->
<div class="hero">
  <div class="hero-badge">&#9679; Live Market Scanner</div>
  <h1>Institutional-grade scanning<br>for <span>independent traders</span></h1>
  <p>Apply Warrior Trading criteria to every NYSE &amp; NASDAQ stock in real time. Get alerts before the crowd.</p>
  <div class="cta-group">
    <a href="/auth/login" class="cta-primary">Start Free Trial &mdash; $2 for 3 Days &rarr;</a>
    <a href="#features" class="cta-secondary">See How It Works</a>
  </div>
  <p class="hero-sub">No credit card required to sign in &middot; Cancel any time</p>
</div>

<!-- METRICS -->
<div class="section" style="padding-top:0">
  <div class="metrics-row">
    <div class="metric"><div class="num g">8,000+</div><div class="lbl">Symbols Scanned</div></div>
    <div class="metric"><div class="num g">5</div><div class="lbl">Hard Criteria</div></div>
    <div class="metric"><div class="num g">5&times;</div><div class="lbl">Min RVOL Filter</div></div>
    <div class="metric"><div class="num g">5-min</div><div class="lbl">Bar Resolution</div></div>
  </div>
</div>

<!-- FEATURES -->
<div class="section" id="features">
  <div class="section-title">Everything you need to trade the open</div>
  <div class="features">
    <div class="feat-card">
      <div class="feat-icon">&#9889;</div>
      <h3>Real-Time Market Scanner</h3>
      <p>Continuously scans NYSE and NASDAQ for momentum setups every 5 minutes. Get alerts the moment a stock clears all criteria.</p>
      <span class="feat-tag tag-green">Live</span>
    </div>
    <div class="feat-card">
      <div class="feat-icon">&#9876;</div>
      <h3>Warrior Trading Criteria</h3>
      <p>Five hard filters: trend alignment (EMA20/50), entry trigger, support/resistance, 1%&#8209;risk management, and RVOL &ge;5&times; with float filter.</p>
      <span class="feat-tag tag-cyan">5 Criteria</span>
    </div>
    <div class="feat-card">
      <div class="feat-icon">&#128200;</div>
      <h3>Auto Trade Execution</h3>
      <p>One-click bracket orders with pre-calculated entry, stop, and target. Supports auto-trade mode with Alpaca paper or live accounts.</p>
      <span class="feat-tag tag-purple">Alpaca API</span>
    </div>
    <div class="feat-card">
      <div class="feat-icon">&#128202;</div>
      <h3>Walk-Forward Backtest Engine</h3>
      <p>10-pass walk-forward backtests on 55 curated high-volatility symbols. Equity curves, Sharpe ratio, max drawdown, and streak tracking.</p>
      <span class="feat-tag tag-green">Backtested</span>
    </div>
    <div class="feat-card">
      <div class="feat-icon">&#128212;</div>
      <h3>Trade Journal</h3>
      <p>Every order auto-logged to SQLite. Track open &amp; closed trades, P&amp;L, R&#8209;multiples, and win rate across sessions.</p>
      <span class="feat-tag tag-cyan">Journal</span>
    </div>
    <div class="feat-card">
      <div class="feat-icon">&#128737;</div>
      <h3>Daily Risk Limits</h3>
      <p>Configurable daily max&#8209;loss and max&#8209;trades guard. Auto-halt with one-click resume. Trailing stop manager with BE and trail phases.</p>
      <span class="feat-tag tag-purple">Risk Mgmt</span>
    </div>
  </div>
</div>

<!-- PRICING -->
<div class="section" style="padding-top:0">
  <div class="section-title" id="pricing">Simple, transparent pricing</div>
  <div class="pricing-wrap">
    <div class="pricing-card">
      <div class="pricing-badge">Most Popular</div>
      <div class="price-main"><sup>$</sup>20</div>
      <div class="price-period">per month &mdash; billed monthly</div>
      <div class="trial-pill">&#9733; Start with $2 for 3 days</div>
      <ul class="pricing-features">
        <li><span class="chk">&#10003;</span> Full scanner (NYSE &amp; NASDAQ)</li>
        <li><span class="chk">&#10003;</span> All 5 Warrior Trading criteria</li>
        <li><span class="chk">&#10003;</span> Auto-trade &amp; bracket orders</li>
        <li><span class="chk">&#10003;</span> Walk-forward backtest engine</li>
        <li><span class="chk">&#10003;</span> Trade journal &amp; P&amp;L tracking</li>
        <li><span class="chk">&#10003;</span> Trailing stop manager</li>
        <li><span class="chk">&#10003;</span> News feed per alert</li>
        <li><span class="chk">&#10003;</span> Cancel any time, no lock-in</li>
      </ul>
      <a href="/auth/login" class="cta-primary" style="display:block;width:100%;text-align:center">
        Get Started &rarr;
      </a>
      <p style="margin-top:14px;font-size:12px;color:var(--dim)">
        3-day trial for $2 &middot; then $20/mo &middot; cancel any time
      </p>
    </div>
  </div>
</div>

<!-- SIGN IN -->
<div class="signin-section">
  <h2>Ready to find your next setup?</h2>
  <p>Sign in with Google to start your 3-day trial for $2</p>
  <a href="/auth/login" class="google-btn">
    <svg width="20" height="20" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/>
      <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
      <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z" fill="#FBBC05"/>
      <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
    </svg>
    Continue with Google
  </a>
</div>

<!-- FOOTER -->
<footer>
  <div>&copy; 2026 TradingAlerts. Not financial advice. Educational purposes only.</div>
  <div class="legal-links">
    <a href="/legal/terms">Terms of Service</a>
    <a href="/legal/privacy">Privacy Policy</a>
    <a href="/legal/disclaimer">Risk Disclaimer</a>
  </div>
</footer>

</body>
</html>"""


@landing_bp.route("/landing")
def landing_page():
    """Public landing page. Logged-in subscribers are sent straight to the app."""
    uid = session.get("user_id")
    if uid:
        from auth import get_user
        user = get_user(uid)
        if user and user.get("subscription_status") in ("trialing", "active"):
            return redirect("/")
    return render_template_string(_LANDING_HTML)
