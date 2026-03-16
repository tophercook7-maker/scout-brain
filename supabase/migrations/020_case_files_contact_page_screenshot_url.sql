alter table if exists public.case_files
  add column if not exists contact_page_screenshot_url text;
