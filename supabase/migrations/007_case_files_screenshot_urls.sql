-- Store investigator screenshot URLs in case files.

alter table public.case_files
  add column if not exists desktop_screenshot_url text;

alter table public.case_files
  add column if not exists mobile_screenshot_url text;

alter table public.case_files
  add column if not exists internal_screenshot_url text;
