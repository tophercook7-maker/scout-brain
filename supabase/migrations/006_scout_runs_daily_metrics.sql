-- Add daily scouting metrics for scheduler summaries.

alter table public.scout_runs
  add column if not exists run_time timestamptz default now();

alter table public.scout_runs
  add column if not exists leads_found integer default 0;

alter table public.scout_runs
  add column if not exists strong_opportunities integer default 0;

alter table public.scout_runs
  add column if not exists weak_websites integer default 0;

alter table public.scout_runs
  add column if not exists no_website integer default 0;

create index if not exists idx_scout_runs_workspace_run_time
  on public.scout_runs(workspace_id, run_time desc);
