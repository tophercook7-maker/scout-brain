-- User email notification preferences per workspace.

create table if not exists public.user_settings (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  email_notifications_enabled boolean not null default true,
  email_frequency text not null default 'daily',
  include_new_leads boolean not null default true,
  include_followups boolean not null default true,
  include_top_opportunities boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, workspace_id),
  constraint user_settings_email_frequency_check check (email_frequency in ('daily', 'weekly', 'off'))
);

create index if not exists idx_user_settings_workspace_id on public.user_settings(workspace_id);
create index if not exists idx_user_settings_user_id on public.user_settings(user_id);

drop trigger if exists user_settings_updated_at on public.user_settings;
create trigger user_settings_updated_at
  before update on public.user_settings
  for each row execute function public.set_updated_at();

alter table public.user_settings enable row level security;

drop policy if exists "Workspace members can read own user settings" on public.user_settings;
create policy "Workspace members can read own user settings"
  on public.user_settings for select
  using (
    user_id = auth.uid()
    and (
      (workspace_id is not null and public.is_workspace_user(workspace_id))
      or exists (
        select 1 from public.workspaces w
        where w.id = workspace_id and w.owner_user_id = auth.uid()
      )
    )
  );

drop policy if exists "Workspace members can manage own user settings" on public.user_settings;
create policy "Workspace members can manage own user settings"
  on public.user_settings for all
  using (
    user_id = auth.uid()
    and (
      (workspace_id is not null and public.is_workspace_user(workspace_id))
      or exists (
        select 1 from public.workspaces w
        where w.id = workspace_id and w.owner_user_id = auth.uid()
      )
    )
  )
  with check (
    user_id = auth.uid()
    and (
      (workspace_id is not null and public.is_workspace_user(workspace_id))
      or exists (
        select 1 from public.workspaces w
        where w.id = workspace_id and w.owner_user_id = auth.uid()
      )
    )
  );
