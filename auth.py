"""
auth.py — Google OAuth + Stripe subscription layer for TradingAlerts
Replace all YOUR_* placeholder values with real credentials before launch,
or set the corresponding environment variables.
"""

import os, sqlite3, stripe
from functools import wraps
from flask import (
    Blueprint, redirect, session, request,
    jsonify, render_template_string, url_for,
)
from authlib.integrations.flask_client import OAuth

# ── Credentials (env vars override placeholders) ──────────────────────────────
GOOGLE_CLIENT_ID      = os.environ.get("GOOGLE_CLIENT_ID",      "")
GOOGLE_CLIENT_SECRET  = os.environ.get("GOOGLE_CLIENT_SECRET",  "")
STRIPE_SECRET_KEY     = os.environ.get("STRIPE_SECRET_KEY",     "")
STRIPE_PRICE_ID       = os.environ.get("STRIPE_PRICE_ID",       "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "YOUR_STRIPE_WEBHOOK_SECRET")

stripe.api_key = STRIPE_SECRET_KEY

# Blueprint — registered on the Flask app in dashboard.py
auth_bp = Blueprint("auth", __name__)

# OAuth client — call oauth.init_app(app) in dashboard.py after app creation
oauth = OAuth()

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")


# ── Database helpers ──────────────────────────────────────────────────────────

def init_users_db():
    """Create the users table if it does not already exist."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                google_id               TEXT UNIQUE NOT NULL,
                email                   TEXT NOT NULL,
                name                    TEXT,
                picture                 TEXT,
                stripe_customer_id      TEXT,
                stripe_subscription_id  TEXT,
                subscription_status     TEXT DEFAULT 'none',
                trial_end               TEXT,
                created_at              TEXT DEFAULT (datetime('now'))
            )
        """)
        con.commit()


def get_user(google_id: str):
    """Return the user row as a dict, or None if not found."""
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT * FROM users WHERE google_id = ?", (google_id,)
        ).fetchone()
        return dict(row) if row else None


def upsert_user(google_id: str, email: str, name: str, picture: str):
    """Insert a new user or refresh their name / picture / email."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            INSERT INTO users (google_id, email, name, picture)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(google_id) DO UPDATE SET
                email   = excluded.email,
                name    = excluded.name,
                picture = excluded.picture
        """, (google_id, email, name, picture))
        con.commit()


def update_subscription(google_id: str, customer_id: str,
                        sub_id: str, status: str, trial_end=None):
    """Write Stripe subscription fields for a user identified by google_id."""
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
            UPDATE users
            SET stripe_customer_id     = ?,
                stripe_subscription_id = ?,
                subscription_status    = ?,
                trial_end              = ?
            WHERE google_id = ?
        """, (customer_id, sub_id, status, trial_end, google_id))
        con.commit()


# ── Auth decorator ────────────────────────────────────────────────────────────

def login_required(f):
    """
    Guard a Flask route: redirect unauthenticated users to /landing,
    and users without an active/trialing subscription to /auth/subscribe.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        uid = session.get("user_id")
        if not uid:
            return redirect("/landing")
        user = get_user(uid)
        if not user:
            session.clear()
            return redirect("/landing")
        # PAYWALL DISABLED FOR TESTING — re-enable before launch
        # if user["subscription_status"] not in ("trialing", "active"):
        #     return redirect("/auth/subscribe")
        return f(*args, **kwargs)
    return decorated


# ── OAuth routes ──────────────────────────────────────────────────────────────

@auth_bp.route("/auth/login")
def login():
    """Redirect to Google's OAuth consent screen."""
    redirect_uri = request.host_url.rstrip("/") + "/auth/callback"
    return oauth.google.authorize_redirect(redirect_uri)


@auth_bp.route("/auth/callback")
def callback():
    """Handle the Google redirect, upsert the user, then route appropriately."""
    token    = oauth.google.authorize_access_token()
    userinfo = token.get("userinfo") or oauth.google.userinfo()

    google_id = userinfo["sub"]
    email     = userinfo.get("email", "")
    name      = userinfo.get("name", "")
    picture   = userinfo.get("picture", "")

    upsert_user(google_id, email, name, picture)
    session.permanent = True
    session["user_id"]      = google_id
    session["user_name"]    = name
    session["user_picture"] = picture
    session["user_email"]   = email

    user = get_user(google_id)
    if user and user["subscription_status"] in ("trialing", "active"):
        return redirect("/")
    return redirect("/auth/subscribe")


@auth_bp.route("/auth/logout")
def logout():
    session.clear()
    return redirect("/landing")


# ── Stripe routes ─────────────────────────────────────────────────────────────

@auth_bp.route("/auth/subscribe")
def subscribe():
    """
    Create a Stripe Checkout session for the $2 / 3-day trial → $20/mo plan
    and redirect the user to the Stripe-hosted checkout page.
    """
    uid = session.get("user_id")
    if not uid:
        return redirect("/landing")
    user = get_user(uid)
    if not user:
        return redirect("/landing")

    # Already subscribed — go straight to the dashboard
    if user["subscription_status"] in ("trialing", "active"):
        return redirect("/")

    base = request.host_url.rstrip("/")
    try:
        checkout = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            subscription_data={"trial_period_days": 3},
            customer_email=user["email"],
            success_url=base + "/auth/subscription_success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=base + "/landing",
        )
        return redirect(checkout.url)
    except Exception as exc:
        # Stripe not yet configured — show a holding page so dev flow isn't broken
        return render_template_string(_SUBSCRIBE_PLACEHOLDER_HTML,
                                      name=user.get("name", ""),
                                      error=str(exc))


@auth_bp.route("/auth/subscription_success")
def subscription_success():
    """Stripe redirects here after successful checkout."""
    uid = session.get("user_id")
    if uid:
        # Optimistically mark trialing; webhook will confirm
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "UPDATE users SET subscription_status='trialing' WHERE google_id=?",
                (uid,)
            )
            con.commit()
    return redirect("/")


@auth_bp.route("/auth/billing")
def billing():
    """Open the Stripe Customer Portal so the user can manage / cancel."""
    uid = session.get("user_id")
    if not uid:
        return redirect("/landing")
    user = get_user(uid)
    if not user or not user.get("stripe_customer_id"):
        return redirect("/")
    try:
        portal = stripe.billing_portal.Session.create(
            customer=user["stripe_customer_id"],
            return_url=request.host_url.rstrip("/") + "/",
        )
        return redirect(portal.url)
    except Exception:
        return redirect("/")


# ── /auth/me  ─────────────────────────────────────────────────────────────────

@auth_bp.route("/auth/me")
def me():
    """Return the logged-in user's profile + subscription status as JSON."""
    uid = session.get("user_id")
    if not uid:
        return jsonify({"error": "not logged in"}), 401
    user = get_user(uid)
    if not user:
        return jsonify({"error": "user not found"}), 404
    return jsonify({
        "name":                user.get("name"),
        "email":               user.get("email"),
        "picture":             user.get("picture"),
        "subscription_status": user.get("subscription_status"),
    })


# ── Stripe Webhook ────────────────────────────────────────────────────────────

@auth_bp.route("/auth/webhook", methods=["POST"])
def webhook():
    """
    Receive and verify Stripe webhook events.
    Wire this endpoint as the webhook URL in your Stripe Dashboard.
    """
    payload = request.get_data()
    sig     = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        return jsonify({"error": "invalid signature"}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    obj = event["data"]["object"]

    # ── subscription.updated / .created ──────────────────────────────────────
    if event["type"] in ("customer.subscription.updated",
                         "customer.subscription.created"):
        stripe_status = obj.get("status", "none")
        # Map Stripe statuses to our four internal states
        status_map = {
            "trialing":           "trialing",
            "active":             "active",
            "past_due":           "active",     # brief grace period
            "canceled":           "canceled",
            "unpaid":             "canceled",
            "paused":             "canceled",
            "incomplete":         "none",
            "incomplete_expired": "none",
        }
        our_status = status_map.get(stripe_status, "none")
        cid        = obj.get("customer")
        sub_id     = obj.get("id")
        trial_end  = str(obj.get("trial_end") or "") or None
        with sqlite3.connect(DB_PATH) as con:
            con.execute("""
                UPDATE users
                SET stripe_subscription_id = ?,
                    subscription_status    = ?,
                    trial_end              = ?
                WHERE stripe_customer_id = ?
            """, (sub_id, our_status, trial_end, cid))
            con.commit()

    # ── subscription.deleted ─────────────────────────────────────────────────
    elif event["type"] == "customer.subscription.deleted":
        cid = obj.get("customer")
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "UPDATE users SET subscription_status='canceled' WHERE stripe_customer_id=?",
                (cid,)
            )
            con.commit()

    # ── checkout.session.completed  (link customer ID to user row) ────────────
    elif event["type"] == "checkout.session.completed":
        cid    = obj.get("customer")
        sub_id = obj.get("subscription")
        email  = (obj.get("customer_email")
                  or (obj.get("customer_details") or {}).get("email"))
        if email:
            with sqlite3.connect(DB_PATH) as con:
                con.execute("""
                    UPDATE users
                    SET stripe_customer_id     = ?,
                        stripe_subscription_id = ?,
                        subscription_status    = 'trialing'
                    WHERE email = ?
                """, (cid, sub_id, email))
                con.commit()

    return jsonify({"ok": True})


# ── Placeholder HTML shown when Stripe keys are still placeholder values ───────

_SUBSCRIBE_PLACEHOLDER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Subscribe — TradingAlerts</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:#0d1117;color:#e6edf3;font-family:'Inter',sans-serif;
       display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}
  .card{background:#161b22;border:1px solid #30363d;border-radius:12px;
        padding:48px 40px;max-width:480px;width:100%;text-align:center}
  h2{font-size:1.7rem;margin-bottom:12px;color:#58a6ff}
  p{color:#8b949e;margin-bottom:24px;line-height:1.6}
  .badge{display:inline-block;background:#1f2937;border:1px solid #374151;
         color:#60a5fa;padding:4px 12px;border-radius:20px;font-size:.85rem;margin-bottom:20px}
  .btn{display:inline-block;padding:13px 32px;background:#238636;color:#fff;
       border-radius:8px;text-decoration:none;font-weight:700;font-size:1rem;
       transition:background .2s}
  .btn:hover{background:#2ea043}
  .error{background:#2d1117;border:1px solid #f8514966;border-radius:6px;
         color:#f85149;font-size:.82rem;margin-top:20px;padding:12px;
         text-align:left;word-break:break-all}
  .links{margin-top:20px;font-size:.82rem;color:#6e7681}
  .links a{color:#58a6ff;text-decoration:none}
</style>
</head>
<body>
<div class="card">
  <span class="badge">⚡ TradingAlerts</span>
  <h2>Welcome, {{ name or "Trader" }}!</h2>
  <p>Access TradingAlerts with a <strong>3-day trial for just $2</strong>,
     then only <strong>$20/month</strong>. Cancel anytime.</p>
  <a href="/auth/subscribe" class="btn">Start My 3-Day Trial — $2</a>
  {% if error %}
  <div class="error"><strong>⚠ Stripe not yet configured:</strong><br>{{ error }}<br><br>
  Set your <code>STRIPE_SECRET_KEY</code> and <code>STRIPE_PRICE_ID</code> environment
  variables or edit auth.py to enable payments.</div>
  {% endif %}
  <div class="links">
    <a href="/landing">← Back to landing page</a> &nbsp;·&nbsp;
    <a href="/legal/terms">Terms</a> &nbsp;·&nbsp;
    <a href="/legal/privacy">Privacy</a>
  </div>
</div>
</body>
</html>"""
