alter table if exists public.case_files
  add column if not exists activity_summary jsonb;

create index if not exists idx_case_files_activity_summary
  on public.case_files
  using gin (activity_summary);
