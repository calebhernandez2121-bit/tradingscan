"""
legal.py — Terms of Service, Privacy Policy, and Financial Disclaimer pages
Replace [OWNER_NAME] and [OWNER_EMAIL] before launch.
"""

from flask import Blueprint, render_template_string

legal_bp = Blueprint("legal", __name__)

# ── Shared CSS ────────────────────────────────────────────────────────────────
_CSS = """
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg0: #05050f; --bg1: #0a0a1a; --bg2: #0f0f22;
    --brd: #1e1e40; --txt: #b0b0d0; --hi: #eeeeff; --dim: #50507a;
    --grn: #00e87c; --cyn: #00d4ff;
  }
  body {
    background: var(--bg0); color: var(--txt);
    font-family: 'Inter', sans-serif; line-height: 1.75;
    padding-bottom: 80px;
  }
  nav {
    background: rgba(5,5,15,.9); border-bottom: 1px solid var(--brd);
    padding: 0 32px; height: 56px;
    display: flex; align-items: center; justify-content: space-between;
  }
  .nav-logo {
    font-size: 17px; font-weight: 800;
    background: linear-gradient(135deg, var(--grn), var(--cyn));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }
  .nav-back { color: var(--dim); text-decoration: none; font-size: 13px; }
  .nav-back:hover { color: var(--txt); }
  .container { max-width: 760px; margin: 0 auto; padding: 48px 24px 0; }
  .doc-title { font-size: 30px; font-weight: 800; color: var(--hi);
    letter-spacing: -.5px; margin-bottom: 6px; }
  .doc-updated { font-size: 12px; color: var(--dim); margin-bottom: 36px; }
  h2 { font-size: 18px; font-weight: 700; color: var(--hi);
    margin: 36px 0 12px; padding-bottom: 8px;
    border-bottom: 1px solid var(--brd); }
  p { margin-bottom: 14px; font-size: 15px; }
  ul { padding-left: 20px; margin-bottom: 14px; }
  li { font-size: 15px; margin-bottom: 6px; }
  a { color: var(--cyn); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .highlight {
    background: var(--bg2); border: 1px solid var(--brd);
    border-radius: 8px; padding: 16px 20px; margin: 16px 0;
    font-size: 14px;
  }
  footer { text-align: center; margin-top: 60px;
    font-size: 12px; color: var(--dim); }
  footer a { color: var(--dim); margin: 0 8px; }
  footer a:hover { color: var(--txt); }
</style>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
"""

_NAV = """
<nav>
  <span class="nav-logo">TradingAlerts</span>
  <a href="/landing" class="nav-back">← Back to Home</a>
</nav>
"""

_FOOTER = """
<footer>
  <div>© 2026 TradingAlerts. All rights reserved.</div>
  <div style="margin-top:6px">
    <a href="/legal/terms">Terms</a>
    <a href="/legal/privacy">Privacy</a>
    <a href="/legal/disclaimer">Disclaimer</a>
  </div>
</footer>
"""


# ── Terms of Service ──────────────────────────────────────────────────────────

_TERMS_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Terms of Service — TradingAlerts</title>
{_CSS}
</head>
<body>
{_NAV}
<div class="container">
  <div class="doc-title">Terms of Service</div>
  <div class="doc-updated">Last updated: April 29, 2026</div>

  <p>Please read these Terms of Service ("Terms") carefully before using TradingAlerts
  (the "Service") operated by <strong>[OWNER_NAME]</strong> ("we," "us," or "our").
  By accessing or using the Service, you agree to be bound by these Terms.</p>

  <h2>1. Acceptance of Terms</h2>
  <p>By creating an account or using TradingAlerts, you confirm that you are at least
  18 years old, have read and understood these Terms, and agree to be bound by them
  and our Privacy Policy and Risk Disclaimer.</p>

  <h2>2. Description of Service</h2>
  <p>TradingAlerts provides a real-time stock market scanning and alerting dashboard
  powered by the Alpaca trading API. The Service is intended for <strong>educational
  and informational purposes only</strong>. We do not provide investment advice,
  financial planning services, or securities brokerage services.</p>

  <h2>3. Subscription &amp; Billing</h2>
  <ul>
    <li>A <strong>3-day trial</strong> is available for a one-time fee of <strong>$2.00</strong>.</li>
    <li>After the trial period, your subscription automatically renews at
        <strong>$20.00 per month</strong> until cancelled.</li>
    <li>Payments are processed securely by Stripe. We never store your payment card details.</li>
    <li>You may cancel at any time via the Billing Portal. Cancellation takes effect at the end
        of the current billing period; no refunds are issued for partial months.</li>
    <li>We reserve the right to change pricing with 30 days' notice.</li>
  </ul>

  <h2>4. User Accounts</h2>
  <p>You are responsible for maintaining the confidentiality of your account credentials
  and for all activity that occurs under your account. You must notify us immediately at
  <a href="mailto:[OWNER_EMAIL]">[OWNER_EMAIL]</a> of any unauthorized use.</p>

  <h2>5. Acceptable Use</h2>
  <p>You agree not to:</p>
  <ul>
    <li>Use the Service for any unlawful purpose or in violation of these Terms.</li>
    <li>Attempt to reverse-engineer, scrape, or copy the Service or its underlying algorithms.</li>
    <li>Share your account credentials with any third party.</li>
    <li>Use automated scripts to access the Service except through documented APIs.</li>
    <li>Distribute or resell access to the Service or any alerts generated by it.</li>
  </ul>

  <h2>6. Intellectual Property</h2>
  <p>All content, code, algorithms, and materials on the Service are the exclusive property
  of [OWNER_NAME] and are protected by applicable copyright and intellectual property laws.
  Your subscription grants you a limited, non-exclusive, non-transferable licence to use the
  Service for your personal, non-commercial use.</p>

  <h2>7. Disclaimer of Warranties</h2>
  <div class="highlight">
    THE SERVICE IS PROVIDED "AS IS" AND "AS AVAILABLE" WITHOUT WARRANTIES OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. WE DO NOT WARRANT THAT THE
    SERVICE WILL BE UNINTERRUPTED, ERROR-FREE, OR FREE OF VIRUSES.
  </div>

  <h2>8. Limitation of Liability</h2>
  <p>TO THE FULLEST EXTENT PERMITTED BY LAW, [OWNER_NAME] SHALL NOT BE LIABLE FOR ANY
  INDIRECT, INCIDENTAL, SPECIAL, CONSEQUENTIAL, OR PUNITIVE DAMAGES, INCLUDING LOSS OF
  PROFITS OR TRADING LOSSES, ARISING FROM YOUR USE OF THE SERVICE, EVEN IF WE HAVE BEEN
  ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.</p>

  <h2>9. Indemnification</h2>
  <p>You agree to indemnify and hold harmless [OWNER_NAME] and its affiliates from any
  claim, loss, liability, or expense (including legal fees) arising from your use of the
  Service or violation of these Terms.</p>

  <h2>10. Governing Law</h2>
  <p>These Terms are governed by the laws of the United States. Any disputes arising
  under these Terms shall be resolved by binding arbitration in accordance with the
  American Arbitration Association rules.</p>

  <h2>11. Changes to Terms</h2>
  <p>We may update these Terms at any time. Continued use of the Service after changes
  are posted constitutes your acceptance of the revised Terms. Material changes will be
  communicated via email.</p>

  <h2>12. Contact</h2>
  <p>For questions about these Terms, please contact us at
  <a href="mailto:[OWNER_EMAIL]">[OWNER_EMAIL]</a>.</p>
</div>
{_FOOTER}
</body>
</html>"""


# ── Privacy Policy ────────────────────────────────────────────────────────────

_PRIVACY_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Privacy Policy — TradingAlerts</title>
{_CSS}
</head>
<body>
{_NAV}
<div class="container">
  <div class="doc-title">Privacy Policy</div>
  <div class="doc-updated">Last updated: April 29, 2026</div>

  <p>This Privacy Policy describes how <strong>[OWNER_NAME]</strong> ("we," "us," or "our")
  collects, uses, and shares information when you use TradingAlerts (the "Service").</p>

  <h2>1. Information We Collect</h2>
  <p><strong>Information you provide:</strong></p>
  <ul>
    <li><strong>Google Account data</strong> — name, email address, and profile picture
        obtained when you sign in via Google OAuth.</li>
    <li><strong>Payment data</strong> — Stripe processes payments on our behalf. We receive
        only a customer ID and subscription status; we never see or store your card number.</li>
  </ul>
  <p><strong>Information collected automatically:</strong></p>
  <ul>
    <li>Server logs including IP address, browser type, pages visited, and timestamps.</li>
    <li>Session cookies required to keep you logged in.</li>
  </ul>

  <h2>2. How We Use Your Information</h2>
  <ul>
    <li>To provide, operate, and improve the Service.</li>
    <li>To verify your identity and manage your subscription.</li>
    <li>To communicate with you about your account, billing, and Service updates.</li>
    <li>To enforce our Terms of Service and detect fraudulent activity.</li>
  </ul>

  <h2>3. Data Storage</h2>
  <p>User data is stored in a SQLite database on our server. Payment data is stored
  exclusively by Stripe under their
  <a href="https://stripe.com/privacy" target="_blank">Privacy Policy</a>.
  We do not sell your personal data to third parties.</p>

  <h2>4. Data Sharing</h2>
  <p>We share your data only with:</p>
  <ul>
    <li><strong>Stripe</strong> — for payment processing.</li>
    <li><strong>Google</strong> — only to authenticate your identity via OAuth.</li>
    <li><strong>Law enforcement</strong> — when required by applicable law or valid legal process.</li>
  </ul>

  <h2>5. Cookies</h2>
  <p>We use a single session cookie to maintain your login state. This cookie expires
  when you log out or close your browser (or after 30 days of inactivity). We do not
  use tracking cookies or third-party advertising cookies.</p>

  <h2>6. Your Rights</h2>
  <ul>
    <li><strong>Access</strong> — you may request a copy of the data we hold about you.</li>
    <li><strong>Deletion</strong> — you may request deletion of your account and associated data.</li>
    <li><strong>Correction</strong> — you may ask us to correct inaccurate data.</li>
    <li><strong>Portability</strong> — you may request your data in a machine-readable format.</li>
  </ul>
  <p>To exercise any of these rights, email
  <a href="mailto:[OWNER_EMAIL]">[OWNER_EMAIL]</a>.</p>

  <h2>7. Data Retention</h2>
  <p>We retain your account data for as long as your account is active, plus 90 days
  after cancellation for accounting purposes. Server logs are purged after 30 days.</p>

  <h2>8. Security</h2>
  <p>We use industry-standard practices to protect your data including HTTPS encryption,
  hashed session tokens, and restricted database access. However, no method of
  transmission over the Internet is 100% secure.</p>

  <h2>9. Children's Privacy</h2>
  <p>The Service is not directed to children under the age of 18. We do not knowingly
  collect personal information from minors.</p>

  <h2>10. Changes to This Policy</h2>
  <p>We may update this Privacy Policy at any time. We will notify you of material
  changes via email or a prominent notice on the Service.</p>

  <h2>11. Contact</h2>
  <p>Privacy questions or data requests:
  <a href="mailto:[OWNER_EMAIL]">[OWNER_EMAIL]</a></p>
</div>
{_FOOTER}
</body>
</html>"""


# ── Financial Risk Disclaimer ─────────────────────────────────────────────────

_DISCLAIMER_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Risk Disclaimer — TradingAlerts</title>
{_CSS}
</head>
<body>
{_NAV}
<div class="container">
  <div class="doc-title">Financial Risk Disclaimer</div>
  <div class="doc-updated">Last updated: April 29, 2026</div>

  <div class="highlight" style="border-color: rgba(255,61,90,.4); margin-bottom: 28px;">
    <strong style="color:#ff3d5a">⚠ Important:</strong> Trading stocks involves substantial
    risk of loss. Read this disclaimer in full before using TradingAlerts.
  </div>

  <h2>1. Not Financial Advice</h2>
  <p>TradingAlerts and its operators (<strong>[OWNER_NAME]</strong>) are <strong>not</strong>
  registered investment advisers, broker-dealers, or financial planners. Nothing on the
  Service — including scanner alerts, backtest results, trade signals, watchlists, news
  summaries, or any other content — constitutes investment advice, a solicitation to buy
  or sell any security, or a recommendation for any specific investment strategy.</p>
  <p>All content is provided for <strong>educational and informational purposes only</strong>.
  You should consult a qualified financial adviser before making any investment decision.</p>

  <h2>2. Risk of Loss</h2>
  <p>Trading equities, especially low-float and high-momentum stocks, carries a <strong>high
  risk of loss</strong> and is not appropriate for all investors. Past performance — including
  any backtest results displayed on the Service — is <strong>not indicative of future
  results</strong>. You may lose some or all of your invested capital.</p>
  <ul>
    <li>Momentum and gap-and-go strategies can fail without warning.</li>
    <li>Low-float stocks are subject to extreme volatility and potential manipulation.</li>
    <li>Automated order execution can result in fills at prices significantly different
        from signals due to slippage, gaps, or system latency.</li>
    <li>Paper trading results do not reflect real-world execution costs, slippage,
        or market impact.</li>
  </ul>

  <h2>3. No Guarantee of Accuracy</h2>
  <p>Market data is sourced from third-party providers (Alpaca Markets). While we strive
  for accuracy, we cannot guarantee that data is complete, timely, or error-free.
  Scanner alerts may be delayed, missed, or triggered incorrectly due to API limitations,
  server load, or data provider outages.</p>

  <h2>4. Backtesting Limitations</h2>
  <p>Backtest results shown on the Service are <strong>hypothetical</strong> and subject to
  significant limitations:</p>
  <ul>
    <li>Hypothetical performance does not account for real-world execution costs.</li>
    <li>Results may be affected by look-ahead bias or curve-fitting to historical data.</li>
    <li>Market conditions in the backtest period may not repeat in the future.</li>
    <li>Walk-forward analysis reduces but does not eliminate these limitations.</li>
  </ul>

  <h2>5. Alpaca Integration</h2>
  <p>If you connect a live Alpaca account to TradingAlerts and enable auto-trade mode,
  <strong>you do so entirely at your own risk</strong>. [OWNER_NAME] is not responsible
  for any trades executed, order rejections, margin calls, or financial losses arising
  from the use of the auto-trade feature.</p>

  <h2>6. Your Responsibility</h2>
  <p>By using the Service, you acknowledge and agree that:</p>
  <ul>
    <li>You are solely responsible for all trading and investment decisions.</li>
    <li>You have read, understood, and agree to this disclaimer in full.</li>
    <li>You will not hold [OWNER_NAME] liable for any losses arising from use of the Service.</li>
    <li>You have verified that trading is legal and compliant with regulations in your jurisdiction.</li>
  </ul>

  <h2>7. SEC Rule 10b-5 Notice</h2>
  <p>TradingAlerts does not engage in market manipulation, front-running, or any activity
  prohibited by SEC Rule 10b-5 or any other applicable securities law. Scanner alerts are
  generated algorithmically based on publicly available market data.</p>

  <h2>8. Contact</h2>
  <p>Questions about this disclaimer:
  <a href="mailto:[OWNER_EMAIL]">[OWNER_EMAIL]</a></p>
</div>
{_FOOTER}
</body>
</html>"""


# ── Routes ────────────────────────────────────────────────────────────────────

@legal_bp.route("/legal/terms")
def terms():
    return render_template_string(_TERMS_HTML)


@legal_bp.route("/legal/privacy")
def privacy():
    return render_template_string(_PRIVACY_HTML)


@legal_bp.route("/legal/disclaimer")
def disclaimer():
    return render_template_string(_DISCLAIMER_HTML)
