-- Massive Brain — Supabase schema
-- Apply in: Supabase Dashboard → SQL Editor
-- Project: jtqbcryjzjtlhsllhpvp

-- Profiles (extends auth.users)
create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text,
  display_name text,
  created_at timestamptz default now()
);

-- Opportunities (Scout leads)
create table if not exists public.opportunities (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  business_name text not null,
  category text,
  lane text,
  distance_miles numeric,
  address text,
  phone text,
  website text,
  maps_link text,
  rating numeric,
  review_count integer,
  hours jsonb,
  no_website boolean default false,
  recommended_contact_method text,
  backup_contact_method text,
  strongest_pitch_angle text,
  best_service_to_offer text,
  demo_to_show text,
  internal_score numeric,
  priority text,
  status text default 'New',
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Case files (outreach/audit per opportunity)
create table if not exists public.case_files (
  id uuid primary key default gen_random_uuid(),
  opportunity_id uuid not null references public.opportunities(id) on delete cascade,
  email text,
  contact_page text,
  phone_from_site text,
  facebook text,
  instagram text,
  owner_manager_name text,
  platform_used text,
  homepage_title text,
  meta_description text,
  viewport_ok boolean,
  tap_to_call_present boolean,
  menu_visibility text,
  hours_visibility text,
  directions_visibility text,
  contact_form_present boolean,
  text_heavy_clues text,
  outdated_design_clues text,
  strongest_problems jsonb,
  short_email text,
  longer_email text,
  contact_form_version text,
  follow_up_note text,
  outreach_notes text,
  follow_up_due text,
  outcome text,
  status text default 'New',
  raw_json jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique(opportunity_id)
);

-- Notes (user notes on opportunities)
create table if not exists public.notes (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  opportunity_id uuid references public.opportunities(id) on delete cascade,
  body text not null,
  created_at timestamptz default now()
);

-- Scout runs (history)
create table if not exists public.scout_runs (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  summary text,
  processed_count integer default 0,
  saved_count integer default 0,
  skipped_count integer default 0,
  created_at timestamptz default now()
);

-- Indexes
create index if not exists idx_opportunities_user_id on public.opportunities(user_id);
create index if not exists idx_opportunities_status on public.opportunities(status);
create index if not exists idx_case_files_opportunity_id on public.case_files(opportunity_id);
create index if not exists idx_notes_user_id on public.notes(user_id);
create index if not exists idx_notes_opportunity_id on public.notes(opportunity_id);
create index if not exists idx_scout_runs_user_id on public.scout_runs(user_id);

-- Auto-update updated_at
create or replace function public.set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists opportunities_updated_at on public.opportunities;
create trigger opportunities_updated_at
  before update on public.opportunities
  for each row execute function public.set_updated_at();

drop trigger if exists case_files_updated_at on public.case_files;
create trigger case_files_updated_at
  before update on public.case_files
  for each row execute function public.set_updated_at();

-- Row Level Security
alter table public.profiles enable row level security;
alter table public.opportunities enable row level security;
alter table public.case_files enable row level security;
alter table public.notes enable row level security;
alter table public.scout_runs enable row level security;

-- RLS policies (user can only access own rows)
drop policy if exists "Users can manage own profile" on public.profiles;
create policy "Users can manage own profile"
  on public.profiles for all using (auth.uid() = id);

drop policy if exists "Users can manage own opportunities" on public.opportunities;
create policy "Users can manage own opportunities"
  on public.opportunities for all using (auth.uid() = user_id);

drop policy if exists "Users can manage case files for own opportunities" on public.case_files;
create policy "Users can manage case files for own opportunities"
  on public.case_files for all using (
    exists (select 1 from public.opportunities o where o.id = opportunity_id and o.user_id = auth.uid())
  );

drop policy if exists "Users can manage own notes" on public.notes;
create policy "Users can manage own notes"
  on public.notes for all using (auth.uid() = user_id);

drop policy if exists "Users can manage own scout runs" on public.scout_runs;
create policy "Users can manage own scout runs"
  on public.scout_runs for all using (auth.uid() = user_id);

-- Profile hook: create profile when user signs up
create or replace function public.handle_new_user()
returns trigger as $$
begin
  insert into public.profiles (id, email, display_name)
  values (new.id, new.email, coalesce(new.raw_user_meta_data->>'display_name', new.email));
  return new;
end;
$$ language plpgsql security definer;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();
