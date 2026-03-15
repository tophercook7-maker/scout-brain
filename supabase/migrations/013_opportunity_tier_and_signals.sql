-- Store explicit website-priority tier and detected issue signals on opportunities.
alter table public.opportunities
  add column if not exists tier text;

alter table public.opportunities
  add column if not exists opportunity_signals jsonb default '[]'::jsonb;

create index if not exists idx_opportunities_tier
  on public.opportunities(tier);
