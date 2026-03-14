-- Add explicit scoring fields for prioritized lead ranking.

alter table public.opportunities
  add column if not exists opportunity_score numeric;

alter table public.opportunities
  add column if not exists lead_tier text;

create index if not exists idx_opportunities_opportunity_score
  on public.opportunities(opportunity_score desc);
