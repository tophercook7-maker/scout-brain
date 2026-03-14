-- Align multi-tenant schema naming with workspace_users and enforce workspace RLS.

-- 1) Core tables
create table if not exists public.workspaces (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz default now(),
  plan text
);

create table if not exists public.workspace_users (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null default 'member',
  created_at timestamptz default now(),
  unique(workspace_id, user_id)
);

-- 2) Ensure workspace_id exists on tenant-scoped tables
alter table public.opportunities add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.case_files add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.notes add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.scout_runs add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;

create index if not exists idx_workspace_users_workspace_id on public.workspace_users(workspace_id);
create index if not exists idx_workspace_users_user_id on public.workspace_users(user_id);

-- 3) Copy legacy workspace_memberships if present
do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public' and table_name = 'workspace_memberships'
  ) then
    insert into public.workspace_users (workspace_id, user_id, role, created_at)
    select m.workspace_id, m.user_id, coalesce(m.role, 'member'), coalesce(m.created_at, now())
    from public.workspace_memberships m
    on conflict (workspace_id, user_id) do nothing;
  end if;
end $$;

-- 4) Ensure each owner belongs to own workspace
insert into public.workspace_users (workspace_id, user_id, role)
select w.id, w.owner_user_id, 'owner'
from public.workspaces w
on conflict (workspace_id, user_id) do nothing;

-- 5) MixedMakerShop owner bootstrap workspace
do $$
declare owner_uuid uuid;
declare mms_workspace uuid;
begin
  select p.id into owner_uuid
  from public.profiles p
  where lower(coalesce(p.email, '')) = 'topher@mixedmakershop.com'
  limit 1;

  if owner_uuid is not null then
    insert into public.workspaces (name, owner_user_id, plan)
    values ('MixedMakerShop', owner_uuid, null)
    on conflict do nothing;

    select w.id into mms_workspace
    from public.workspaces w
    where w.owner_user_id = owner_uuid and w.name = 'MixedMakerShop'
    order by w.created_at asc
    limit 1;

    if mms_workspace is not null then
      insert into public.workspace_users (workspace_id, user_id, role)
      values (mms_workspace, owner_uuid, 'owner')
      on conflict (workspace_id, user_id) do nothing;

      update public.opportunities set workspace_id = mms_workspace
      where user_id = owner_uuid and workspace_id is null;

      update public.notes set workspace_id = mms_workspace
      where user_id = owner_uuid and workspace_id is null;

      update public.scout_runs set workspace_id = mms_workspace
      where user_id = owner_uuid and workspace_id is null;

      update public.case_files c
      set workspace_id = mms_workspace
      from public.opportunities o
      where c.opportunity_id = o.id
        and o.user_id = owner_uuid
        and c.workspace_id is null;
    end if;
  end if;
end $$;

-- 6) Generic backfill for any rows still missing workspace_id
insert into public.workspaces (name, owner_user_id, plan)
select distinct 'Personal Workspace', u.user_id, null
from (
  select user_id from public.opportunities
  union
  select user_id from public.notes
  union
  select user_id from public.scout_runs
) u
left join public.workspaces w on w.owner_user_id = u.user_id
where w.id is null;

insert into public.workspace_users (workspace_id, user_id, role)
select w.id, w.owner_user_id, 'owner'
from public.workspaces w
on conflict (workspace_id, user_id) do nothing;

update public.opportunities o
set workspace_id = w.id
from public.workspaces w
where o.workspace_id is null
  and w.owner_user_id = o.user_id;

update public.notes n
set workspace_id = w.id
from public.workspaces w
where n.workspace_id is null
  and w.owner_user_id = n.user_id;

update public.scout_runs s
set workspace_id = w.id
from public.workspaces w
where s.workspace_id is null
  and w.owner_user_id = s.user_id;

update public.case_files c
set workspace_id = o.workspace_id
from public.opportunities o
where c.workspace_id is null
  and c.opportunity_id = o.id;

-- 7) Workspace helper and RLS
create or replace function public.is_workspace_user(ws_id uuid)
returns boolean
language sql
stable
as $$
  select exists (
    select 1 from public.workspace_users wu
    where wu.workspace_id = ws_id
      and wu.user_id = auth.uid()
  );
$$;

alter table public.workspaces enable row level security;
alter table public.workspace_users enable row level security;

drop policy if exists "Users can read own workspaces" on public.workspaces;
create policy "Users can read own workspaces"
  on public.workspaces for select
  using (public.is_workspace_user(id) or owner_user_id = auth.uid());

drop policy if exists "Users can manage own workspaces" on public.workspaces;
create policy "Users can manage own workspaces"
  on public.workspaces for all
  using (owner_user_id = auth.uid());

drop policy if exists "Users can read own workspace users" on public.workspace_users;
create policy "Users can read own workspace users"
  on public.workspace_users for select
  using (user_id = auth.uid() or public.is_workspace_user(workspace_id));

drop policy if exists "Users can manage own workspace users" on public.workspace_users;
create policy "Users can manage own workspace users"
  on public.workspace_users for all
  using (public.is_workspace_user(workspace_id));

drop policy if exists "Workspace members can manage opportunities" on public.opportunities;
create policy "Workspace members can manage opportunities"
  on public.opportunities for all
  using ((workspace_id is not null and public.is_workspace_user(workspace_id)) or auth.uid() = user_id);

drop policy if exists "Workspace members can manage case files" on public.case_files;
create policy "Workspace members can manage case files"
  on public.case_files for all
  using (
    (workspace_id is not null and public.is_workspace_user(workspace_id))
    or exists (select 1 from public.opportunities o where o.id = opportunity_id and o.user_id = auth.uid())
  );

drop policy if exists "Workspace members can manage notes" on public.notes;
create policy "Workspace members can manage notes"
  on public.notes for all
  using ((workspace_id is not null and public.is_workspace_user(workspace_id)) or auth.uid() = user_id);

drop policy if exists "Workspace members can manage scout runs" on public.scout_runs;
create policy "Workspace members can manage scout runs"
  on public.scout_runs for all
  using ((workspace_id is not null and public.is_workspace_user(workspace_id)) or auth.uid() = user_id);
