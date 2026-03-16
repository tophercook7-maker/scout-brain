-- Store short human-readable reason for redesign opportunity ranking.
alter table public.opportunities
  add column if not exists opportunity_reason text;

create index if not exists idx_opportunities_reason
  on public.opportunities(opportunity_reason);
