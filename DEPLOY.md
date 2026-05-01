# Deploying TradingAlerts to Render

## Step 1 — Push to GitHub
1. Go to github.com and create a new repository called `tradingalerts` (set to Private)
2. Copy the repo URL (e.g. `https://github.com/yourusername/tradingalerts.git`)
3. In Terminal, run:
   ```bash
   cd ~/Downloads/blueprint-app
   git remote add origin https://github.com/yourusername/tradingalerts.git
   git branch -M main
   git push -u origin main
   ```

## Step 2 — Deploy on Render
1. Go to render.com and sign up (free)
2. Click "New +" → "Web Service"
3. Connect your GitHub account and select the `tradingalerts` repo
4. Render will auto-detect the render.yaml — click "Create Web Service"
5. Wait ~3 minutes for the build to complete
6. Your app will be live at `https://tradingalerts.onrender.com` (or similar)

## Step 3 — Update OAuth Redirect URI
1. Go to console.cloud.google.com → APIs & Services → Credentials
2. Click your OAuth 2.0 Client
3. Under Authorized redirect URIs, add: `https://tradingalerts.onrender.com/auth/callback`
4. Save

## Step 4 — Set FLASK_SECRET_KEY on Render
1. In Render dashboard → your service → Environment
2. Add key `FLASK_SECRET_KEY` with value from running: `python3 -c "import secrets; print(secrets.token_hex(32))"`

## Step 5 — Add Stripe Webhook
1. Go to dashboard.stripe.com → Developers → Webhooks → Add endpoint
2. URL: `https://tradingalerts.onrender.com/auth/webhook`
3. Events: `customer.subscription.created`, `customer.subscription.updated`, `customer.subscription.deleted`, `checkout.session.completed`
4. Copy the signing secret → add as `STRIPE_WEBHOOK_SECRET` in Render environment variables
