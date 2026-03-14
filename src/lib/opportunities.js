import { supabase } from "./supabaseClient.js";

export async function loadOpportunities() {
  if (!supabase) return [];
  const { data, error } = await supabase
    .from("opportunities")
    .select("*")
    .order("created_at", { ascending: false });

  if (error) throw error;

  return data || [];
}

export async function saveOpportunity(opportunity) {
  const { data: authData, error: authError } = await supabase.auth.getUser();
  if (authError) throw authError;
  const user = authData?.user || null;
  if (!user) throw new Error("Not authenticated");

  const row = { ...opportunity, user_id: user.id };

  const { data, error } = await supabase
    .from("opportunities")
    .insert(row)
    .select()
    .single();

  if (error) throw error;

  return data;
}
