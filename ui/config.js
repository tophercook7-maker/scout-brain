/**
 * Massive Brain — Supabase config
 * Set in .env or Vercel/hosting env:
 *
 *   VITE_SUPABASE_URL=https://jtqbcryjzjtlhsllhpvp.supabase.co
 *   VITE_SUPABASE_ANON_KEY=your-anon-key
 *
 * Or: SUPABASE_URL, SUPABASE_ANON_KEY (if build injects them)
 */
export const SUPABASE_URL = import.meta.env?.VITE_SUPABASE_URL || import.meta.env?.SUPABASE_URL;
export const SUPABASE_ANON_KEY = import.meta.env?.VITE_SUPABASE_ANON_KEY || import.meta.env?.SUPABASE_ANON_KEY;
