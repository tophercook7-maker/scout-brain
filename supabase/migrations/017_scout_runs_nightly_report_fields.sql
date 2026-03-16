alter table if exists public.scout_runs
  add column if not exists job_name text;

alter table if exists public.scout_runs
  add column if not exists cities_scanned integer default 0;

alter table if exists public.scout_runs
  add column if not exists industries_scanned integer default 0;

alter table if exists public.scout_runs
  add column if not exists businesses_found integer default 0;

alter table if exists public.scout_runs
  add column if not exists opportunities_scored integer default 0;

alter table if exists public.scout_runs
  add column if not exists leads_created integer default 0;

alter table if exists public.scout_runs
  add column if not exists email_drafts_generated integer default 0;

alter table if exists public.scout_runs
  add column if not exists nightly_report jsonb;

create index if not exists idx_scout_runs_workspace_run_time_job
  on public.scout_runs(workspace_id, run_time desc, job_name);
