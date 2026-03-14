-- Website audit fields for lead dossier scoring.

alter table public.case_files
  add column if not exists website_score integer;

alter table public.case_files
  add column if not exists mobile_score integer;

alter table public.case_files
  add column if not exists design_score integer;

alter table public.case_files
  add column if not exists navigation_score integer;

alter table public.case_files
  add column if not exists conversion_score integer;

alter table public.case_files
  add column if not exists audit_issues jsonb;

alter table public.case_files
  add column if not exists high_opportunity boolean default false;
