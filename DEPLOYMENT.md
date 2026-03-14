# Massive Brain — Cloud Deployment Checklist

Use this checklist to deploy Massive Brain so it runs from any device without your MacBook.

## Current product reality

- Live active Scout-Brain app: `https://web-production-61047.up.railway.app/`
- MixedMakerShop admin is currently separate from Scout-Brain.
- This deployment guide targets improving and operating the standalone Scout-Brain app first.

## Dual-mode product direction

- One Scout-Brain core app supports both:
  - internal MixedMakerShop usage
  - standalone SaaS usage for external users
- Keep one codebase and one data model; do not fork into separate apps.
- Billing/subscriptions are intentionally out of scope for now.

## Architecture

- **Frontend**: Vercel (standalone Scout-Brain frontend)
- **Backend**: Railway (standalone Scout-Brain backend)
- **Database / Auth**: Supabase (already set up)

---

## 1. Supabase (already done)

- [ ] Tables applied: `profiles`, `opportunities`, `case_files`, `notes`, `scout_runs`
- [ ] RLS policies enabled
- [ ] Note: **JWT Secret** — Supabase Dashboard → Project Settings → API → **JWT Secret** (needed for backend to verify user tokens)

---

## 2. Backend (Railway)

### 2.1 Create project

- [ ] Go to [railway.app](https://railway.app), create a new project
- [ ] Connect your GitHub repo (or deploy from CLI)
- [ ] Root directory: repo root (where `app.py` and `requirements.txt` live)

### 2.2 Environment variables (Railway → Variables)

Set in the Railway dashboard:

For MixedMakerShop admin proxy auth to work, these three backend values must come from the exact same Supabase project as MixedMakerShop (`https://zwdsnwvuhaesbllzbfmt.supabase.co`): `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `SUPABASE_JWT_SECRET`.

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_MAPS_API_KEY` | Yes (for Run Scout) | Google Cloud API key with Places API (New) + Geocoding enabled |
| `SUPABASE_URL` | Yes (for sync/auth verification context) | `https://zwdsnwvuhaesbllzbfmt.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | Yes (for sync) | From Supabase → Settings → API → service_role (secret) |
| `SUPABASE_JWT_SECRET` | Yes (for sync) | From Supabase → Settings → API → JWT Secret |
| `PORT` | No | Railway sets this automatically |

### 2.3 Deploy

- [ ] Railway Docker deploy uses root `Dockerfile` (Python-only runtime).
- [ ] Service uses Dockerfile deploy mode (not Nixpacks) for backend-only runtime.
- [ ] Repo Railway config (`railway.json`) uses:
  - Build: `pip install -r requirements.txt`
  - Start: `python3 app.py`
- [ ] Dockerfile does not run frontend build commands (`npm install`, `npm ci`, `npm run build`).
- [ ] Build command: `pip install -r requirements.txt`
- [ ] Start command: `python3 app.py`
- [ ] `app.py` reads `PORT` from env and binds host `0.0.0.0`
- [ ] Railway uses backend-only mode (`SERVE_FRONTEND=0` by default)
- [ ] Deploy; note the generated URL (e.g. `https://your-app.up.railway.app`)

### 2.4 Custom domain (later)

- [ ] Railway → Settings → Domains → Add custom domain: `brain-api.mixedmakershop.com`
- [ ] Add the CNAME record at your DNS provider

---

## 3. Frontend (Vercel)

### 3.1 Create project

- [ ] Go to [vercel.com](https://vercel.com), import your GitHub repo
- [ ] Framework preset: Vite
- [ ] Root directory: leave default (repo root)
- [ ] Build command: `npm run build`
- [ ] Output directory: `dist`

### 3.2 Environment variables (Vercel → Settings → Environment Variables)

| Variable | Value |
|----------|--------|
| `VITE_SUPABASE_URL` | `https://zwdsnwvuhaesbllzbfmt.supabase.co` |
| `VITE_SUPABASE_ANON_KEY` | Your Supabase anon (public) key |
| `VITE_API_BASE_URL` | Your **backend** URL, e.g. `https://brain-api.mixedmakershop.com` or Railway URL (no trailing slash) |

### 3.3 Deploy

- [ ] Deploy; note the Vercel URL

### 3.4 Custom domain (later)

- [ ] Vercel → Settings → Domains → Add: `brain.mixedmakershop.com`
- [ ] Add the A/CNAME record as shown by Vercel

---

## 4. Scout config (backend)

The backend runs the scout using config from the **repo** (e.g. `scout/config.json`). On Railway the filesystem is ephemeral.

- [ ] Commit a `scout/config.json` in the repo with your default `home_city`, `categories`, etc., so each deploy has a valid config
- [ ] Or rely on env-based config later (not implemented yet)

---

## 5. Post-deploy checks

- [ ] Open frontend URL → login screen loads
- [ ] Sign in with Supabase account
- [ ] Dashboard loads; opportunities (if any) come from Supabase
- [ ] Click **Run Scout** → request goes to backend URL; backend runs scout and syncs results to Supabase
- [ ] Refresh or open app on another device → same data appears

---

## 6. Local development

- **Frontend**: `npm run dev` (Vite). Use Vite proxy: leave `VITE_API_BASE_URL` unset so `/run-scout` etc. proxy to `localhost:8760`.
- **Backend**: `python app.py` (runs on port 8760; set `PORT=8760` if needed).
- **Optional monolith local mode**: `SERVE_FRONTEND=1 python app.py` to serve frontend from FastAPI.
- **.env**: Copy `.env.example` and `backend.env.example`, fill in keys for local runs.

---

## Summary

| Component | URL (example) | Purpose |
|-----------|----------------|---------|
| Frontend | https://brain.mixedmakershop.com | UI; auth; loads data from Supabase; calls backend for Run Scout / Audit |
| Backend | https://brain-api.mixedmakershop.com | Run Scout, Audit; syncs scout results to Supabase when user is logged in |
| Supabase | (dashboard) | Auth, opportunities, case_files, notes, scout_runs |

## Roadmap

### Phase 1 - standalone Scout-Brain polish (current)

- Improve dashboard usefulness, lead refresh timing, dossier quality, outreach flow, and prioritization.
- Treat Railway-hosted Scout-Brain as the primary environment.

### Phase 2 - MixedMakerShop integration (later)

- Integrate or link Scout-Brain into MixedMakerShop admin once standalone polish targets are met.
- Surface lightweight integration widgets first (dashboard card, top leads, quick Run Scout, top opportunities, deep link to full app).
