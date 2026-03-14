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

  console.log("workspace lookup start");
  const { data, error } = await supabase.auth.getUser();
  if (error) throw error;
  const user = data?.user || null;
  if (!user) return null;

  const ownerEmails = parseOwnerEmails();
  const isOwnerInternal = ownerEmails.includes(String(user.email || "").toLowerCase());

  const fallback = {
    workspaceId: null,
    workspaceSlug: null,
    workspaceName: "No Workspace",
    role: isOwnerInternal ? "owner" : "member",
    isOwnerInternal,
    source: "workspace-missing",
  };

  try {
    let membership = await supabase
      .from("workspace_users")
      .select("workspace_id, role, workspaces:workspace_id(id, name)")
      .eq("user_id", user.id)
      .order("created_at", { ascending: true })
      .limit(1)
      .maybeSingle();

    if (membership.error && isMissingWorkspaceSchemaError(membership.error)) {
      membership = await supabase
        .from("workspace_memberships")
        .select("workspace_id, role, workspaces:workspace_id(id, slug, name)")
        .eq("user_id", user.id)
        .order("created_at", { ascending: true })
        .limit(1)
        .maybeSingle();
    }

    if (membership.error) {
      if (isMissingWorkspaceSchemaError(membership.error)) {
        _workspaceCtx = { ...fallback, source: "schema-fallback" };
        console.log("workspace missing");
        console.log("workspace missing for user", { user_id: user.id, reason: "schema-fallback" });
        return _workspaceCtx;
      }
      throw membership.error;
    }

    const row = membership.data || null;
    if (!row) {
      _workspaceCtx = fallback;
      console.log("workspace missing");
      console.log("workspace missing for user", { user_id: user.id, reason: "no-membership-row" });
      return _workspaceCtx;
    }
    const ws = row.workspaces || {};
    _workspaceCtx = {
      workspaceId: row.workspace_id || fallback.workspaceId,
      workspaceSlug: ws.slug || null,
      workspaceName: ws.name || fallback.workspaceName,
      role: row.role || fallback.role,
      isOwnerInternal,
      source: "workspace-membership",
    };
    console.log("workspace found", {
      user_id: user.id,
      workspace_id: _workspaceCtx.workspaceId,
      source: _workspaceCtx.source,
    });
    return _workspaceCtx;
  } catch (err) {
    if (isMissingWorkspaceSchemaError(err)) {
      _workspaceCtx = { ...fallback, source: "schema-fallback" };
      console.log("workspace missing");
      console.log("workspace missing for user", { user_id: user.id, reason: "schema-fallback-exception" });
      return _workspaceCtx;
    }
    throw err;
  }
}

export function withWorkspaceId(row, workspaceId) {
  if (!workspaceId) return row;
  return { ...row, workspace_id: workspaceId };
}
