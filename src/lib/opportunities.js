import { supabase } from "./supabaseClient.js";
import {
  isMissingWorkspaceSchemaError,
  resolveWorkspaceContext,
  withWorkspaceId,
} from "./workspace.js";

export async function loadOpportunities() {
  if (!supabase) return [];
  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }

  let query = supabase.from("opportunities").select("*").order("created_at", { ascending: false });
  if (workspaceId) query = query.eq("workspace_id", workspaceId);
  let { data, error } = await query;

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase
      .from("opportunities")
      .select("*")
      .order("created_at", { ascending: false });
    data = retry.data;
    error = retry.error;
  }

  if (error) throw error;

  return data || [];
}

export async function saveOpportunity(opportunity) {
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

  const row = withWorkspaceId({ ...opportunity, user_id: user.id }, workspaceId);

  let { data, error } = await supabase
    .from("opportunities")
    .insert(row)
    .select()
    .single();

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase
      .from("opportunities")
      .insert({ ...opportunity, user_id: user.id })
      .select()
      .single();
    data = retry.data;
    error = retry.error;
  }

  if (error) throw error;

  return data;
}
