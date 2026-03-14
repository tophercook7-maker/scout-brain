-- Multi-city discovery fields for opportunities.

alter table public.opportunities
  add column if not exists place_id text;

alter table public.opportunities
  add column if not exists city text;

alter table public.opportunities
  add column if not exists state text;

alter table public.opportunities
  add column if not exists industry text;

alter table public.opportunities
  add column if not exists website_score integer;

create index if not exists idx_opportunities_workspace_place_id
  on public.opportunities(workspace_id, place_id);

create index if not exists idx_opportunities_city_state
  on public.opportunities(city, state);

create index if not exists idx_opportunities_industry
  on public.opportunities(industry);
