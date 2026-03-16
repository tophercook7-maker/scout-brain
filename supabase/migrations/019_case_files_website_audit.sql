alter table if exists public.case_files
  add column if not exists website_audit jsonb;

create index if not exists idx_case_files_website_audit
  on public.case_files
  using gin (website_audit);
