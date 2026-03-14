-- Scout-Brain multi-tenant workspace foundation
-- Phase 1: adds workspace model while keeping legacy user_id access compatibility.

create table if not exists public.workspaces (
  id uuid primary key default gen_random_uuid(),
  slug text unique,
  name text not null,
  owner_user_id uuid not null references auth.users(id) on delete cascade,
  is_internal boolean default false,
  created_at timestamptz default now()
);

create table if not exists public.workspace_memberships (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null default 'member',
  created_at timestamptz default now(),
  unique(workspace_id, user_id)
);

alter table public.opportunities add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.case_files add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.notes add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;
alter table public.scout_runs add column if not exists workspace_id uuid references public.workspaces(id) on delete cascade;

create index if not exists idx_workspaces_owner_user_id on public.workspaces(owner_user_id);
create index if not exists idx_workspace_memberships_workspace_id on public.workspace_memberships(workspace_id);
create index if not exists idx_workspace_memberships_user_id on public.workspace_memberships(user_id);
create index if not exists idx_opportunities_workspace_id on public.opportunities(workspace_id);
create index if not exists idx_case_files_workspace_id on public.case_files(workspace_id);
create index if not exists idx_notes_workspace_id on public.notes(workspace_id);
create index if not exists idx_scout_runs_workspace_id on public.scout_runs(workspace_id);

-- Backfill: create one personal workspace per existing user.
insert into public.workspaces (owner_user_id, name, slug, is_internal)
select distinct u.user_id, 'Personal Workspace', null, false
from (
  select user_id from public.opportunities
  union
  select user_id from public.notes
  union
  select user_id from public.scout_runs
) u
left join public.workspaces w on w.owner_user_id = u.user_id
where w.id is null;

insert into public.workspace_memberships (workspace_id, user_id, role)
select w.id, w.owner_user_id, 'owner'
from public.workspaces w
left join public.workspace_memberships m
  on m.workspace_id = w.id and m.user_id = w.owner_user_id
where m.id is null;

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

-- Workspace access helper for RLS.
create or replace function public.is_workspace_member(ws_id uuid)
returns boolean
language sql
stable
as $$
  select exists (
    select 1 from public.workspace_memberships m
    where m.workspace_id = ws_id
      and m.user_id = auth.uid()
  );
$$;

alter table public.workspaces enable row level security;
alter table public.workspace_memberships enable row level security;

drop policy if exists "Users can read own workspace memberships" on public.workspace_memberships;
create policy "Users can read own workspace memberships"
  on public.workspace_memberships for select
  using (user_id = auth.uid());

drop policy if exists "Users can read own workspaces" on public.workspaces;
create policy "Users can read own workspaces"
  on public.workspaces for select
  using (public.is_workspace_member(id) or owner_user_id = auth.uid());

drop policy if exists "Users can manage own workspaces" on public.workspaces;
create policy "Users can manage own workspaces"
  on public.workspaces for all
  using (owner_user_id = auth.uid());

-- Keep existing user-based policies in 001_initial; add workspace-aware policies for forward compatibility.
drop policy if exists "Workspace members can manage opportunities" on public.opportunities;
create policy "Workspace members can manage opportunities"
  on public.opportunities for all
  using (
    (workspace_id is not null and public.is_workspace_member(workspace_id))
    or auth.uid() = user_id
  );

drop policy if exists "Workspace members can manage case files" on public.case_files;
create policy "Workspace members can manage case files"
  on public.case_files for all
  using (
    (workspace_id is not null and public.is_workspace_member(workspace_id))
    or exists (select 1 from public.opportunities o where o.id = opportunity_id and o.user_id = auth.uid())
  );

drop policy if exists "Workspace members can manage notes" on public.notes;
create policy "Workspace members can manage notes"
  on public.notes for all
  using (
    (workspace_id is not null and public.is_workspace_member(workspace_id))
    or auth.uid() = user_id
  );

drop policy if exists "Workspace members can manage scout runs" on public.scout_runs;
create policy "Workspace members can manage scout runs"
  on public.scout_runs for all
  using (
    (workspace_id is not null and public.is_workspace_member(workspace_id))
    or auth.uid() = user_id
  );
