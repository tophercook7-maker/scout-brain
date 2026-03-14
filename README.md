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

`vercel.json` includes SPA rewrite support so client routes like `/admin/scout` resolve to `index.html` instead of returning 404.

Important: frontend API calls must continue to use `VITE_API_BASE_URL` (do not point frontend routes to Railway directly).

### Immediate Vercel setup (live Railway backend)

- Framework: `Vite`
- Build command: `npm run build`
- Output directory: `dist`
- Required Vercel environment variables:
  - `VITE_SUPABASE_URL`
  - `VITE_SUPABASE_ANON_KEY`
  - `VITE_API_BASE_URL=https://web-production-61047.up.railway.app`

## Railway deployment

Use repo root as service root.

Railway is backend-only for this project (frontend runs on Vercel).

Docker deploy mode (recommended for Railway):

- Root `Dockerfile` uses `python:3.11-slim` and installs from `requirements.txt`.
- Container start command runs FastAPI via:
  - `uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}`
- `SERVE_FRONTEND=0` is set in the container (API-only mode).
- Do not run frontend build commands in Railway Docker builds (`npm install`, `npm ci`, `npm run build`).

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

`Procfile` already includes the same start command.

Do **not** set Railway build steps to `npm install` or `npm run build`.

Backend API-only behavior in production:

- `SERVE_FRONTEND` defaults to `false`, so Railway serves API only.
- `GET /` returns backend status JSON in API-only mode.
- API routes stay active:
  - `POST /run-scout`
  - `GET /scout-data`
  - `POST /audit`
  - `GET /case/{slug}`
  - `POST /case/{slug}/update`
  - `GET /scout/config.json`

If you ever want monolith mode (FastAPI also serves frontend), set `SERVE_FRONTEND=1`.

## Deployment and domains

Frontend:

- Use the Vercel project URL first.
- Add custom domain later: `brain.mixedmakershop.com`.

Backend:

- Use the Railway public URL first.
- Add custom domain later: `brain-api.mixedmakershop.com`.

## Custom domain setup

Vercel frontend custom domain:

- Add `brain.mixedmakershop.com` in Vercel Domains.
- Configure DNS at your domain provider exactly as instructed by Vercel.

Railway backend custom domain:

- Add `brain-api.mixedmakershop.com` in Railway Public Networking / Custom Domain.
- Configure DNS at your domain provider exactly as instructed by Railway.

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
