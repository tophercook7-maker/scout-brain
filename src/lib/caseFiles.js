import { supabase } from "./supabaseClient.js";
import {
  isMissingWorkspaceSchemaError,
  resolveWorkspaceContext,
  withWorkspaceId,
} from "./workspace.js";

export async function loadCaseFile(opportunityId) {
  if (!supabase) return null;
  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }

  let query = supabase.from("case_files").select("*").eq("opportunity_id", opportunityId);
  if (workspaceId) query = query.eq("workspace_id", workspaceId);
  let { data, error } = await query.single();

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase
      .from("case_files")
      .select("*")
      .eq("opportunity_id", opportunityId)
      .single();
    data = retry.data;
    error = retry.error;
  }

  if (error && error.code !== "PGRST116") throw error;

  return data;
}

export async function saveCaseFile(opportunityId, updates) {
  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }

  let { data, error } = await supabase
    .from("case_files")
    .upsert(
      withWorkspaceId({ opportunity_id: opportunityId, ...updates }, workspaceId),
      { onConflict: "opportunity_id" }
    )
    .select()
    .single();

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase
      .from("case_files")
      .upsert(
        { opportunity_id: opportunityId, ...updates },
        { onConflict: "opportunity_id" }
      )
      .select()
      .single();
    data = retry.data;
    error = retry.error;
  }

  if (error) throw error;

  return data;
}
