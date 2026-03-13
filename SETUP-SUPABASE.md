# Massive Brain — Connect to Supabase

Use your existing project: [jtqbcryjzjtlhsllhpvp](https://supabase.com/dashboard/project/jtqbcryjzjtlhsllhpvp)

## 1. Run the schema

1. Open [Supabase SQL Editor](https://supabase.com/dashboard/project/jtqbcryjzjtlhsllhpvp/sql/new)
2. Paste the contents of `supabase/migrations/001_initial.sql`
3. Run

## 2. Enable auth

1. Go to **Authentication** → **Providers**
2. Ensure **Email** is enabled

## 3. Get API keys

1. Go to **Settings** → **API**
2. Copy **Project URL** and **anon public** key

## 4. Configure local env

```bash
cp .env.example .env
```

Edit `.env`:

```
VITE_SUPABASE_URL=https://jtqbcryjzjtlhsllhpvp.supabase.co
VITE_SUPABASE_ANON_KEY=your-anon-key-here
```

## 5. Run the app

```bash
npm install
npm run dev
```

Open http://localhost:5173. Sign up with email/password. Data syncs to Supabase.

## 6. Deploy to Vercel

Set env vars in Vercel project settings:

- `VITE_SUPABASE_URL` = your Supabase URL
- `VITE_SUPABASE_ANON_KEY` = your anon key
