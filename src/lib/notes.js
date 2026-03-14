import { supabase } from "./supabaseClient.js";
import {
  isMissingWorkspaceSchemaError,
  resolveWorkspaceContext,
  withWorkspaceId,
} from "./workspace.js";

export async function addNote(opportunityId, body) {
  if (!supabase) throw new Error("Supabase not configured");
  const { data: authData, error: authError } = await supabase.auth.getUser();
  if (authError) throw authError;
  const user = authData?.user || null;
  if (!user) throw new Error("Not authenticated");

  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }

  let { data, error } = await supabase
    .from("notes")
    .insert({
      ...withWorkspaceId({ user_id: user.id }, workspaceId),
      opportunity_id: opportunityId,
      body: body || "",
    })
    .select()
    .single();

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase
      .from("notes")
      .insert({
        user_id: user.id,
        opportunity_id: opportunityId,
        body: body || "",
      })
      .select()
      .single();
    data = retry.data;
    error = retry.error;
  }

  if (error) throw error;

  return data;
}

export async function loadNotes(opportunityId) {
  if (!supabase) return [];
  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }

  let query = supabase
    .from("notes")
    .select("id, body, created_at")
    .eq("opportunity_id", opportunityId)
    .order("created_at", { ascending: true });
  if (workspaceId) query = query.eq("workspace_id", workspaceId);
  let { data, error } = await query;

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase
      .from("notes")
      .select("id, body, created_at")
      .eq("opportunity_id", opportunityId)
      .order("created_at", { ascending: true });
    data = retry.data;
    error = retry.error;
  }

  if (error) throw error;

  return data || [];
}
