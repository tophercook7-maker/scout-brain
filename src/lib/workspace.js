import { supabase } from "./supabaseClient.js";

let _workspaceCtx = null;

function parseOwnerEmails() {
  const raw = (import.meta.env.VITE_OWNER_EMAILS || "").trim();
  if (!raw) return [];
  return raw.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
}

export function getPresentationMode() {
  const raw = (import.meta.env.VITE_APP_PRESENTATION || "standalone").trim().toLowerCase();
  return raw === "internal" ? "internal" : "standalone";
}

export function isMissingWorkspaceSchemaError(error) {
  const code = String(error?.code || "");
  const msg = String(error?.message || "").toLowerCase();
  return (
    code === "42703" ||
    code === "42p01" ||
    code === "PGRST204" ||
    msg.includes("workspace_id") ||
    msg.includes("workspace_users") ||
    msg.includes("workspace_memberships") ||
    msg.includes("workspaces")
  );
}

export function clearWorkspaceContext() {
  _workspaceCtx = null;
}

export async function resolveWorkspaceContext() {
  if (_workspaceCtx) return _workspaceCtx;
  if (!supabase) return null;

  const { data, error } = await supabase.auth.getUser();
  if (error) throw error;
  const user = data?.user || null;
  if (!user) return null;

  const ownerEmails = parseOwnerEmails();
  const isOwnerInternal = ownerEmails.includes(String(user.email || "").toLowerCase());

  const fallback = {
    workspaceId: user.id,
    workspaceSlug: null,
    workspaceName: "Personal Workspace",
    role: isOwnerInternal ? "owner" : "member",
    isOwnerInternal,
    source: "fallback-user",
  };

  try {
    let membership = await supabase
      .from("workspace_users")
      .select("workspace_id, role, workspaces:workspace_id(id, name)")
      .eq("user_id", user.id)
      .order("created_at", { ascending: true })
      .limit(1)
      .single();

    if (membership.error && isMissingWorkspaceSchemaError(membership.error)) {
      membership = await supabase
        .from("workspace_memberships")
        .select("workspace_id, role, workspaces:workspace_id(id, slug, name)")
        .eq("user_id", user.id)
        .order("created_at", { ascending: true })
        .limit(1)
        .single();
    }

    if (membership.error) {
      if (isMissingWorkspaceSchemaError(membership.error)) {
        _workspaceCtx = fallback;
        return _workspaceCtx;
      }
      throw membership.error;
    }

    const row = membership.data || {};
    const ws = row.workspaces || {};
    _workspaceCtx = {
      workspaceId: row.workspace_id || fallback.workspaceId,
      workspaceSlug: ws.slug || null,
      workspaceName: ws.name || fallback.workspaceName,
      role: row.role || fallback.role,
      isOwnerInternal,
      source: "workspace-membership",
    };
    return _workspaceCtx;
  } catch (err) {
    if (isMissingWorkspaceSchemaError(err)) {
      _workspaceCtx = fallback;
      return _workspaceCtx;
    }
    throw err;
  }
}

export function withWorkspaceId(row, workspaceId) {
  if (!workspaceId) return row;
  return { ...row, workspace_id: workspaceId };
}
