do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public' and table_name = 'leads'
  ) then
    alter table public.leads
      add column if not exists is_hot_lead boolean not null default false;
  end if;
end
$$;

do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public' and table_name = 'opportunities'
  ) then
    alter table public.opportunities
      add column if not exists opportunity_signals jsonb;
  end if;
end
$$;
