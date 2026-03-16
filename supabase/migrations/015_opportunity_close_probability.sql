alter table if exists public.opportunities
  add column if not exists close_probability text;

create index if not exists idx_opportunities_close_probability
  on public.opportunities(close_probability);

