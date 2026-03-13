/**
 * Bootstrap: init Supabase auth, set cloud data bridge, load app.
 */
import "./styles.css";
console.log("frontend loaded");

// API base URL for backend (Vercel frontend -> Railway backend). Empty = same origin.
const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
window.MB_API_BASE = apiBase;

import { supabase, isCloudMode } from "../src/lib/supabaseClient.js";
import { initAuth, signIn, signUp, signOut } from "../src/lib/auth.js";
import {
  fetchScoutDataFromSupabase,
  getCaseFromSupabase,
  updateCaseInSupabase,
} from "./cloud-data.js";

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
    try {
      if (isSignUp) {
        await signUp(emailEl.value, passwordEl.value);
        errorEl.textContent = "Check your email to confirm.";
      } else {
        await signIn(emailEl.value, passwordEl.value);
        root.remove();
        onLoggedIn();
      }
    } catch (err) {
      errorEl.textContent = err.message || "Sign in failed";
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
  const cssOk = document.styleSheets.length > 0 && getComputedStyle(document.documentElement).getPropertyValue("--bg").trim();
  console.log("[Massive Brain] Boot | stylesheets:", document.styleSheets.length, "| --bg set:", !!cssOk);
  if (!isCloudMode()) {
    window.MB_USE_CLOUD = false;
    await import("./analyzer.js");
    await import("./app.js");
    return;
  }

  if (!supabase) {
  console.warn("Supabase URL/key missing, running in local mode");
  window.MB_USE_CLOUD = false;
  await import("./analyzer.js");
  await import("./app.js");
  return;
  }

  const { data: { session } } = await supabase.auth.getSession();
  if (!session) {
    const container = document.getElementById("auth-container");
    if (container) {
      document.querySelector("main.dashboard")?.classList.add("showing-auth");
      renderAuthUI(container, async () => {
        document.querySelector("main.dashboard")?.classList.remove("showing-auth");
        window.MB_SESSION = (await supabase.auth.getSession()).data.session;
        setCloudBridge();
        renderLoggedInBar(handleLogout);
        document.getElementById("auth-email-display").textContent = window.MB_SESSION.user.email;
        await import("./analyzer.js");
        await import("./app.js");
      });
    } else {
      await import("./analyzer.js");
      await import("./app.js");
    }
    return;
  }

  window.MB_SESSION = session;
  setCloudBridge();
  renderLoggedInBar(handleLogout);
  document.getElementById("auth-email-display").textContent = session.user.email;
  await import("./analyzer.js");
  await import("./app.js");
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
  if (session && window.MB_USE_CLOUD) {
    document.getElementById("auth-email-display").textContent = session.user.email;
  }
});

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot);
} else {
  boot();
}
