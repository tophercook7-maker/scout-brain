-- Cross-device job visibility fields for workspace-global Scout runs.

alter table public.jobs
  add column if not exists job_type text;

alter table public.jobs
  add column if not exists message text;

update public.jobs
set job_type = coalesce(job_type, type)
where job_type is null;

update public.jobs
set message = coalesce(message, result_summary)
where message is null;

create index if not exists idx_jobs_workspace_job_type_status_created
  on public.jobs(workspace_id, job_type, status, created_at desc);
