-- Background jobs table for async scout execution.

create table if not exists public.jobs (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid references public.workspaces(id) on delete cascade,
  type text not null,
  status text not null default 'queued',
  progress integer not null default 0,
  payload jsonb,
  result_summary text,
  error text,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz
);

create index if not exists idx_jobs_workspace_created_at
  on public.jobs(workspace_id, created_at desc);

create index if not exists idx_jobs_status
  on public.jobs(status);
