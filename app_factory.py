"""
app_factory.py — Authlib OAuth initialisation helper for TradingAlerts.
Import oauth from here when you want a single shared OAuth instance
(e.g., if you split dashboard.py into an application factory pattern).

Usage in your app entry-point:
    from app_factory import oauth, init_oauth
    init_oauth(app)
"""

from authlib.integrations.flask_client import OAuth

oauth = OAuth()


def init_oauth(app):
    """Register the Google OAuth provider on *app*."""
    from auth import GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET  # noqa: PLC0415

    oauth.init_app(app)
    oauth.register(
        name="google",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )
