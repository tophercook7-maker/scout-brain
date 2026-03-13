import { supabase } from "./supabaseClient.js";

export async function loadCaseFile(opportunityId) {
  if (!supabase) return null;
  const { data, error } = await supabase
    .from("case_files")
    .select("*")
    .eq("opportunity_id", opportunityId)
    .single();

  if (error && error.code !== "PGRST116") throw error;

  return data;
}

export async function saveCaseFile(opportunityId, updates) {
  const { data, error } = await supabase
    .from("case_files")
    .upsert(
      { opportunity_id: opportunityId, ...updates },
      { onConflict: "opportunity_id" }
    )
    .select()
    .single();

  if (error) throw error;

  return data;
}
