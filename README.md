# Massive Brain

Massive Brain is a full-stack opportunity scouting and analysis app with a Vite frontend, a Python backend, and Supabase-powered sync.

## Local setup

1. Clone the repo.
2. Create a local environment file:

   ```bash
   cp .env.example .env
   ```

3. Fill real values in `.env` (do not commit secrets).
4. Install dependencies:

   ```bash
   npm install
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

## Frontend run steps

```bash
npm run dev
```

Frontend runs on Vite (typically `http://localhost:5173`).

## Backend run steps

```bash
source .venv/bin/activate
python3 app.py
```

Backend runs on `http://localhost:8760`.

## Required environment variables

Set these in local `.env`:

- `VITE_SUPABASE_URL`
- `VITE_SUPABASE_ANON_KEY`
- `GOOGLE_MAPS_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Use real secrets only in `.env` or platform environment settings. Never commit real credentials to GitHub.

## Deployment plan (Vercel + Railway + Supabase)

- **Supabase:** host Postgres/Auth and run SQL migrations from `supabase/migrations`.
- **Vercel:** deploy the frontend (`npm run build` output from Vite).
- **Railway:** deploy the Python backend (`app.py`) and set backend secrets.
- **Environment wiring:** point frontend env vars to Supabase + Railway endpoints, and point backend vars to Supabase service credentials.
