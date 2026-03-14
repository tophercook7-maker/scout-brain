-- Store detected owner/decision-maker metadata from site investigator.

alter table public.case_files
  add column if not exists owner_name text;

alter table public.case_files
  add column if not exists owner_title text;

alter table public.case_files
  add column if not exists owner_source_page text;
