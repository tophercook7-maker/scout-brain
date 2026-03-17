alter table if exists public.opportunities
  add column if not exists opportunity_reason text;

create index if not exists idx_opportunities_opportunity_reason
  on public.opportunities(opportunity_reason);
