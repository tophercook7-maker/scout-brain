import { supabase } from "./supabaseClient.js";

export function initAuth(callback) {
  if (!supabase) return;
  if (typeof callback !== "function") return;
  supabase.auth.onAuthStateChange((_event, session) => callback?.(session));
  supabase.auth.getSession().then(({ data: { session } }) => callback?.(session));
}

export async function signIn(email, password) {
  return supabase.auth.signInWithPassword({ email, password });
}

export async function signUp(email, password) {
  return supabase.auth.signUp({ email, password });
}

export async function signOut() {
  return supabase.auth.signOut();
}

export async function getUser() {
  const { data } = await supabase.auth.getUser();
  return data?.user || null;
}
