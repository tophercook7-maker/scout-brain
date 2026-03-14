# Massive Brain (Scout-Brain)

Massive Brain is a split-deploy full-stack app:
- Vite frontend (deploy to Vercel)
- FastAPI backend (deploy to Railway)
- Supabase for auth and shared data

## Project structure (current)

This repo is intentionally a single-root project (no file moves required).

- **Frontend root:** repo root with Vite config in `vite.config.js` and UI entry at `ui/index.html`
- **Backend root:** repo root with FastAPI entry at `app.py` and Python deps in `requirements.txt`
- **Backend data/runtime folders:** `scout/`, `data/`

## Local setup

```bash
cp .env.example .env
npm install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run locally

Frontend (Vite dev server):

```bash
npm run dev
```

Backend (FastAPI):

```bash
source .venv/bin/activate
python3 app.py
```

## Frontend environment variables (Vercel)

- `VITE_SUPABASE_URL`
- `VITE_SUPABASE_ANON_KEY`
- `VITE_API_BASE_URL`

`VITE_API_BASE_URL` must be your hosted Railway backend URL (not localhost).

## Backend environment variables (Railway)

- `GOOGLE_MAPS_API_KEY`
- `SUPABASE_URL` (needed for scout result sync)
- `SUPABASE_SERVICE_ROLE_KEY` (needed for scout result sync)
- `SUPABASE_JWT_SECRET` (needed for Bearer JWT verification in sync flow)
- `ALLOWED_ORIGINS` (comma-separated explicit frontend origins)
- `PORT` (set automatically by Railway)

`SUPABASE_ANON_KEY` is not required by the backend for current server flows.

## Backend API routes used by frontend

- `GET /scout-data`
- `POST /run-scout`
- `POST /audit`
- `GET /case/{slug}`
- `POST /case/{slug}/update`
- `GET /scout/config.json`

## Vercel deployment

Use repo root as project root.

```bash
npm install
npm run build
```

`vercel.json` is already configured for Vite output (`dist`).

## Railway deployment

Use repo root as service root.

Start command:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

`Procfile` already includes the same start command.

Railway production should serve the built Vite frontend from `dist` through FastAPI:

- `GET /` serves `dist/index.html`
- `GET /assets/*` serves `dist/assets/*`
- Non-API frontend routes use SPA fallback to `dist/index.html`
- API routes (`/run-scout`, `/scout-data`, `/audit`, `/case/*`, `/scout/*`) remain backend endpoints

Because `dist` is ignored in git, ensure Railway builds frontend before start:

```bash
npm install
npm run build
pip install -r requirements.txt
```

Then start FastAPI with:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## CORS configuration

Backend CORS is environment-driven:

- `ALLOWED_ORIGINS` supports comma-separated explicit origins (recommended).
- `ALLOWED_ORIGIN_REGEX` defaults to `^https://.*\.vercel\.app$` for preview/production Vercel URLs.
- Localhost origins are allowed by default for local development.

Example:

```bash
ALLOWED_ORIGINS=https://your-app.vercel.app,https://www.yourdomain.com
```

## Deployment wiring summary

1. Deploy backend to Railway and copy the public backend URL.
2. Set Vercel `VITE_API_BASE_URL` to that Railway URL.
3. Set Supabase env vars on frontend and backend as listed above.
4. Redeploy Vercel so frontend calls Railway for `/run-scout`, `/audit`, and `/scout-data`.
