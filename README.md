# ARDA Fertilizer Distribution System
### Katsina State — Blockchain-Backed Fertilizer Tracking

---

## Project Structure

```
your-repo/
├── app.py                  ← Single unified Flask backend (production entry point)
├── main.html               ← Landing page
├── index.html              ← Main application dashboard
├── landing.html            ← (optional alternate landing)
├── requirements.txt        ← Python dependencies
├── render.yaml             ← Render.com deployment config
├── Procfile                ← Gunicorn start command (fallback)
├── runtime.txt             ← Python version pin
└── .gitignore              ← Excludes .env, *.db, blockchain files
```

> `main.py` is no longer used in production. The two-process proxy architecture
> has been consolidated into a single `app.py` compatible with Render's
> one-process-per-service model.

---

## Deploying to Render.com

### Prerequisites
- Code pushed to a GitHub repository
- A free account at https://render.com

---

### Step 1 — Push all files to GitHub

```bash
git add app.py requirements.txt render.yaml Procfile runtime.txt .gitignore
git commit -m "Production ready: unified app for Render deployment"
git push origin main
```

Make sure `main.html`, `index.html`, and `landing.html` are also committed.

---

### Step 2 — Create a Web Service on Render

1. Log in to https://render.com
2. Click **New → Web Service**
3. Connect your GitHub account if not already linked
4. Select your repository
5. Fill in the form:

| Field | Value |
|---|---|
| Name | `arda-fertilizer` |
| Region | `Frankfurt (EU)` — closest to Nigeria |
| Branch | `main` |
| Runtime | `Python 3` |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120` |
| Instance Type | `Free` |

6. Under **Environment Variables**, add:

| Key | Value |
|---|---|
| `SECRET_KEY` | Click **Generate** for a secure random value |

7. Click **Create Web Service**

Render will install dependencies, build, and deploy. You'll receive a live URL like:
`https://arda-fertilizer.onrender.com`

---

### Step 3 — Initialize the Database (First Deploy Only)

After the service shows **Live**, open the **Shell** tab in Render and run:

```bash
python -c "from app import init_db, init_blockchain; init_db(); init_blockchain()"
```

This creates all SQLite tables and the genesis blockchain blocks.

Alternatively, you can use the Flask CLI:
```bash
flask init-db
```

---

### Step 4 — Verify Deployment

Open the following URLs to confirm everything works:

| URL | Expected Result |
|---|---|
| `https://your-app.onrender.com/` | Landing page loads |
| `https://your-app.onrender.com/app` | Main dashboard loads |
| `https://your-app.onrender.com/health` | `{"status": "ok", ...}` |
| `https://your-app.onrender.com/api/blockchain/verify` | `{"valid": true, ...}` |

---

## ⚠️ Important: SQLite Data Persistence

Render's free tier uses an **ephemeral filesystem**. This means:

- The SQLite database (`fertilizer.db`) and blockchain JSON files
  **are deleted every time the service is redeployed or restarted**.
- For a production system holding real farmer data, you **must** migrate
  to a persistent database.

### Recommended: Upgrade to PostgreSQL (Free on Render)

1. In Render dashboard → **New → PostgreSQL**
2. Create a free database, copy the **Internal Database URL**
3. Add it as an environment variable: `DATABASE_URL=postgresql://...`
4. Install `psycopg2-binary` and rewrite `get_db()` to use SQLAlchemy or psycopg2

This is the recommended path before going live with real users.

---

## Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run locally
python app.py
# App available at http://localhost:5000
```

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `SECRET_KEY` | Yes | Flask secret key — set to a long random string in production |
| `PORT` | Auto (Render sets it) | Port the server listens on |
| `DATA_DIR` | No | Override path for DB and blockchain files |

---

## API Endpoints Summary

### Auth
- `POST /api/register/farmer` — Register farmer
- `POST /api/register/admin` — Register admin
- `POST /api/register/officer` — Register store officer
- `POST /api/login` — Login (all user types)

### Inventory
- `GET /api/inventory` — List inventory
- `POST /api/inventory` — Add stock

### Sessions
- `GET /api/sessions` — All sessions
- `GET /api/sessions/active` — Active sessions only
- `POST /api/sessions` — Create session

### Requests & Allocation
- `POST /api/requests` — Farmer submits request
- `GET /api/requests/farmer/<id>` — Farmer's requests
- `GET /api/requests/session/<id>` — All requests in a session
- `POST /api/allocate/<session_id>` — Admin allocates bags

### Distribution
- `POST /api/verify_qr` — Verify QR code
- `POST /api/distribute` — Mark as distributed
- `POST /api/acknowledge` — Farmer acknowledges receipt

### Blockchain
- `GET /api/blockchain` — Full chain
- `GET /api/blockchain/verify` — Integrity check

### Dashboard
- `GET /api/stats/admin` — Admin statistics
- `GET /api/farmers` — All farmers
- `GET /api/officers` — All store officers
- `GET /api/audit_logs` — Audit log (last 100)
- `GET /api/distributions/pending` — Pending distributions
- `GET /api/distributions/officer/<id>` — Officer's distribution history

### Locations
- `POST/GET /api/locations/lga`
- `POST /api/locations/ward` / `GET /api/locations/ward/<lga_id>`
- `POST /api/locations/polling_unit` / `GET /api/locations/polling_unit/<ward_id>`

### System
- `GET /health` — Health check (used by Render)
