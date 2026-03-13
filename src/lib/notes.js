import { supabase } from "./supabaseClient.js";

export async function addNote(opportunityId, body) {
  if (!supabase) throw new Error("Supabase not configured");
  const { data: { user } } = await supabase.auth.getUser();
  if (!user) throw new Error("Not authenticated");

  const { data, error } = await supabase
    .from("notes")
    .insert({
      user_id: user.id,
      opportunity_id: opportunityId,
      body: body || "",
    })
    .select()
    .single();

  if (error) throw error;

  return data;
}

export async function loadNotes(opportunityId) {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from("notes")
    .select("id, body, created_at")
    .eq("opportunity_id", opportunityId)
    .order("created_at", { ascending: true });

  if (error) throw error;

  return data || [];
}
