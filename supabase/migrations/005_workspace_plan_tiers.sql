-- Prepare workspace plans for future billing tiers.

alter table public.workspaces
  alter column plan set default 'free';

update public.workspaces
set plan = 'free'
where plan is null;

-- Preserve internal owner workspace as internal tier.
update public.workspaces
set plan = 'internal'
where lower(coalesce(name, '')) = 'mixedmakershop';

alter table public.workspaces
  drop constraint if exists workspaces_plan_check;

alter table public.workspaces
  add constraint workspaces_plan_check
  check (plan in ('internal', 'free', 'pro', 'agency'));
