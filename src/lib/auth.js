import { supabase } from "./supabaseClient.js";

export function initAuth(callback) {
  if (!supabase) return;
  if (typeof callback !== "function") return;
  supabase.auth.onAuthStateChange((_event, session) => {
    try {
      callback?.(session || null);
    } catch (err) {
      console.error("auth state callback failed:", err);
    }
  });
  supabase.auth
    .getSession()
    .then(({ data, error }) => {
      if (error) {
        console.error("auth getSession failed:", error);
        callback?.(null);
        return;
      }
      callback?.(data?.session || null);
    })
    .catch((err) => {
      console.error("auth getSession exception:", err);
      callback?.(null);
    });
}

export async function signIn(email, password) {
  if (!supabase) return { data: null, error: new Error("Supabase not configured") };
  try {
    return await supabase.auth.signInWithPassword({ email, password });
  } catch (err) {
    return { data: null, error: err };
  }
}

export async function signUp(email, password) {
  if (!supabase) return { data: null, error: new Error("Supabase not configured") };
  try {
    return await supabase.auth.signUp({ email, password });
  } catch (err) {
    return { data: null, error: err };
  }
}

export async function signOut() {
  if (!supabase) return { error: null };
  return supabase.auth.signOut();
}

export async function getUser() {
  if (!supabase) return null;
  try {
    const { data, error } = await supabase.auth.getUser();
    if (error) {
      console.error("auth getUser failed:", error);
      return null;
    }
    return data?.user || null;
  } catch (err) {
    console.error("auth getUser exception:", err);
    return null;
  }
}
