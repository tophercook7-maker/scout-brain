alter table if exists public.case_files
  add column if not exists website_issues jsonb;

create index if not exists idx_case_files_website_issues
  on public.case_files
  using gin (website_issues);
