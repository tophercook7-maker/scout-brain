/**
 * Bootstrap: init Supabase auth, set cloud data bridge, load app.
 */
import "./styles.css";
import { initDebugOverlay, setDebugValue } from "../src/debug-overlay.js";
console.log("frontend loaded");

// API base URL for backend (Vercel frontend -> Railway/backend host). Empty = same-origin calls.
const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
window.MB_API_BASE = apiBase;

if (!apiBase && window.location.hostname.includes("vercel.app")) {
  console.warn("VITE_API_BASE_URL is not set. Configure it in Vercel to point at your hosted backend.");
}

import { supabase, isCloudMode } from "../src/lib/supabaseClient.js";
import { initAuth, signIn, signUp, signOut } from "../src/lib/auth.js";
import {
  fetchScoutDataFromSupabase,
  getCaseFromSupabase,
  updateCaseInSupabase,
} from "./cloud-data.js";

let modulesLoaded = false;

async function loadAppModules() {
  if (modulesLoaded) return;
  await import("./analyzer.js");
  await import("./app.js");
  modulesLoaded = true;
  setDebugValue("dbg-init", "complete");
}

function setAuthEmailDisplay(session) {
  const email = session?.user?.email || "";
  const el = document.getElementById("auth-email-display");
  if (el) el.textContent = email;
}

async function checkBackend() {
  try {
    const backendBase = import.meta.env.VITE_API_BASE_URL || "";
    const res = await fetch(`${backendBase}/scout-data`);
    if (res.ok) {
      setDebugValue("dbg-backend", "reachable");
    } else {
      setDebugValue("dbg-backend", "error");
    }
  } catch (e) {
    setDebugValue("dbg-backend", "offline");
  }
}

function renderAuthUI(container, onLoggedIn) {
  const root = document.createElement("div");
  root.id = "auth-root";
  root.innerHTML = `
    <div class="auth-card" style="max-width:400px;margin:40px auto;padding:24px;background:var(--panel, #222);border-radius:16px;border:1px solid var(--line, rgba(255,255,255,0.08));">
      <h2 style="margin:0 0 8px;">Massive Brain</h2>
      <p style="color:var(--muted,#999);margin:0 0 20px;">Sign in to sync across devices</p>
      <form id="auth-form">
        <input type="email" id="auth-email" placeholder="Email" required style="width:100%;padding:12px;margin-bottom:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--text);">
        <input type="password" id="auth-password" placeholder="Password" required style="width:100%;padding:12px;margin-bottom:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--text);">
        <button type="submit" class="primary-btn" style="width:100%;padding:12px;">Sign In</button>
      </form>
      <p id="auth-error" style="color:#e08080;margin-top:12px;font-size:14px;"></p>
      <p style="margin-top:16px;font-size:13px;color:var(--muted);">
        New? <a href="#" id="auth-toggle-signup" style="color:var(--accent);">Sign up</a>
      </p>
    </div>
  `;
  container.appendChild(root);

  let isSignUp = false;
  const form = root.querySelector("#auth-form");
  const emailEl = root.querySelector("#auth-email");
  const passwordEl = root.querySelector("#auth-password");
  const errorEl = root.querySelector("#auth-error");
  const toggleEl = root.querySelector("#auth-toggle-signup");

  form.onsubmit = async (e) => {
    e.preventDefault();
    errorEl.textContent = "";
    console.log("sign in clicked");
    try {
      if (isSignUp) {
        const { error } = await signUp(emailEl.value, passwordEl.value);
        if (error) {
          console.error("sign in failed", error);
          errorEl.textContent = error.message || "Login failed. Check email and password.";
          return;
        }
        errorEl.textContent = "Check your email to confirm.";
      } else {
        const { data, error } = await signIn(emailEl.value, passwordEl.value);
        if (error) {
          console.error("sign in failed", error);
          errorEl.textContent = error.message || "Login failed. Check email and password.";
          return;
        }
        const user = data?.user || data?.session?.user || null;
        if (!user) {
          console.error("sign in failed", { message: "No user returned from Supabase auth response." });
          errorEl.textContent = "Login failed. Check email and password.";
          return;
        }
        console.log("sign in success");
        root.remove();
        onLoggedIn();
      }
    } catch (err) {
      console.error("sign in failed", err);
      errorEl.textContent = err.message || "Login failed. Check email and password.";
    }
  };

  toggleEl.onclick = (e) => {
    e.preventDefault();
    isSignUp = !isSignUp;
    form.querySelector('button[type="submit"]').textContent = isSignUp ? "Sign Up" : "Sign In";
    toggleEl.textContent = isSignUp ? "Sign in instead" : "Sign up";
  };
}

function renderLoggedInBar(onLogout) {
  const existing = document.getElementById("auth-bar");
  if (existing) existing.remove();

  const bar = document.createElement("div");
  bar.id = "auth-bar";
  bar.style.cssText = "display:flex;align-items:center;gap:12px;padding:8px 0;";
  bar.innerHTML = `
    <span id="auth-email-display" style="font-size:13px;color:var(--muted);"></span>
    <button type="button" class="ghost-btn" id="auth-logout">Sign Out</button>
  `;
  document.querySelector(".topbar-actions")?.prepend(bar);
  document.getElementById("auth-logout").onclick = () => signOut().then(onLogout);
}

async function boot() {
  console.log("auth bootstrap start");
  setDebugValue("dbg-init", "booting");
  const cssOk = document.styleSheets.length > 0 && getComputedStyle(document.documentElement).getPropertyValue("--bg").trim();
  console.log("[Massive Brain] Boot | stylesheets:", document.styleSheets.length, "| --bg set:", !!cssOk);
  void checkBackend();
  try {
    if (!isCloudMode() || !supabase) {
      window.MB_USE_CLOUD = false;
      setDebugValue("dbg-auth", "no user");
      await loadAppModules();
      return;
    }

    let session = null;
    try {
      const { data, error } = await supabase.auth.getSession();
      if (error) {
        console.error("auth bootstrap getSession failed:", error);
      }
      session = data?.session || null;
    } catch (err) {
      console.error("auth bootstrap getSession exception:", err);
    }

    console.log(`auth bootstrap user: ${session?.user ? "present" : "missing"}`);
    setDebugValue("dbg-auth", session?.user ? "signed-in" : "no user");

    if (!session?.user) {
      window.MB_USE_CLOUD = false;
      const container = document.getElementById("auth-container");
      if (container) {
        document.querySelector("main.dashboard")?.classList.add("showing-auth");
        renderAuthUI(container, async () => {
          document.querySelector("main.dashboard")?.classList.remove("showing-auth");
          let freshSession = null;
          try {
            const { data, error } = await supabase.auth.getSession();
            if (error) console.error("post-login getSession failed:", error);
            freshSession = data?.session || null;
          } catch (err) {
            console.error("post-login getSession exception:", err);
          }

          if (freshSession?.user) {
            window.MB_SESSION = freshSession;
            setCloudBridge();
            renderLoggedInBar(handleLogout);
            setAuthEmailDisplay(freshSession);
            setDebugValue("dbg-auth", "signed-in");
            await window.refreshScoutData?.();
          } else {
            window.MB_USE_CLOUD = false;
            setDebugValue("dbg-auth", "no user");
          }
        });
      }
      await loadAppModules();
      return;
    }

    window.MB_SESSION = session;
    setCloudBridge();
    renderLoggedInBar(handleLogout);
    setAuthEmailDisplay(session);
    setDebugValue("dbg-auth", "signed-in");
    await loadAppModules();
  } catch (err) {
    console.error("auth bootstrap failed:", err);
    window.MB_USE_CLOUD = false;
    setDebugValue("dbg-auth", "no user");
    setDebugValue("dbg-init", "error");
    await loadAppModules();
  }
}

function setCloudBridge() {
  window.MB_USE_CLOUD = true;
  window.MB_FETCH_SCOUT_DATA = fetchScoutDataFromSupabase;
  window.MB_GET_CASE = getCaseFromSupabase;
  window.MB_UPDATE_CASE = updateCaseInSupabase;
}

function handleLogout() {
  window.MB_USE_CLOUD = false;
  window.MB_SESSION = null;
  window.MB_FETCH_SCOUT_DATA = null;
  window.MB_GET_CASE = null;
  window.MB_UPDATE_CASE = null;
  document.getElementById("auth-bar")?.remove();
  location.reload();
}

initAuth?.((session) => {
  if (session?.user && window.MB_USE_CLOUD) {
    setAuthEmailDisplay(session);
    setDebugValue("dbg-auth", "signed-in");
  } else if (!session?.user) {
    setDebugValue("dbg-auth", "no user");
  }
});

window.addEventListener("error", (e) => {
  console.error("Global JS error:", e.error || e.message);
});

initDebugOverlay();
setDebugValue("dbg-api", import.meta.env.VITE_API_BASE_URL || "missing");
setDebugValue("dbg-supabase", import.meta.env.VITE_SUPABASE_URL ? "configured" : "missing");
setDebugValue("dbg-auth", "...");
setDebugValue("dbg-init", "starting");
setDebugValue("dbg-backend", "checking");

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
