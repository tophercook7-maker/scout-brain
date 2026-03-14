-- Add daily morning scout summary fields.

alter table public.scout_runs
  add column if not exists run_date date;

alter table public.scout_runs
  add column if not exists businesses_discovered integer default 0;

alter table public.scout_runs
  add column if not exists analyzed_total integer default 0;

alter table public.scout_runs
  add column if not exists high_opportunity_total integer default 0;

create index if not exists idx_scout_runs_workspace_run_date
  on public.scout_runs(workspace_id, run_date desc);
