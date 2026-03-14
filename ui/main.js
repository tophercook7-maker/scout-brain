/**
 * Bootstrap: init Supabase auth, set cloud data bridge, load app.
 * Current product reality: this UI runs as the standalone Scout-Brain app.
 * MixedMakerShop admin integration is a later roadmap phase.
 */
import "./styles.css";
import { initDebugOverlay, setDebugValue } from "../src/debug-overlay.js";
import {
  clearWorkspaceContext,
  getPresentationMode,
  resolveWorkspaceContext,
} from "../src/lib/workspace.js";
console.log("frontend loaded");

// API base URL for standalone Railway-hosted Scout-Brain backend.
// Empty means same-origin calls for local development.
const apiBase = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
window.MB_API_BASE = apiBase;

if (!apiBase && window.location.hostname.includes("vercel.app")) {
  console.warn("VITE_API_BASE_URL is not set. Configure it in Vercel to point at your hosted backend.");
}

import { supabase, isCloudMode } from "../src/lib/supabaseClient.js";
import { initAuth, signIn, signUp, signOut, requestPasswordReset } from "../src/lib/auth.js";
import {
  fetchScoutDataFromSupabase,
  getCaseFromSupabase,
  regenerateOutreachForCaseInSupabase,
  updateCaseInSupabase,
} from "./cloud-data.js";

let modulesLoaded = false;

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function applyPresentationBranding(session, workspaceCtx = null) {
  const mode = getPresentationMode();
  const isOwnerInternal = !!workspaceCtx?.isOwnerInternal;
  const effectiveMode = isOwnerInternal ? "internal" : mode;
  document.body.setAttribute("data-presentation-mode", effectiveMode);

  if (effectiveMode === "internal") {
    setText("publicBrandEyebrow", "MixedMakerShop Internal");
    setText("publicBrandTitle", "Scout-Brain");
    setText("publicBrandSubtext", "Internal sales workspace powered by Scout-Brain.");
    setText("adminBrandTitle", "Scout-Brain Internal CRM");
  } else {
    setText("publicBrandEyebrow", "Scout-Brain");
    setText("publicBrandTitle", "Scout-Brain");
    setText("publicBrandSubtext", "Standalone lead intelligence and outreach workspace.");
    setText("adminBrandTitle", "Scout-Brain Workspace");
  }

  const workspaceName = workspaceCtx?.workspaceName || "Personal";
  setText("workspaceBadge", `Workspace: ${workspaceName}`);
  window.MB_WORKSPACE = {
    workspaceId: workspaceCtx?.workspaceId || null,
    workspaceName,
    role: workspaceCtx?.role || "member",
    isOwnerInternal,
    presentationMode: effectiveMode,
    email: session?.user?.email || "",
  };
}

function normalizedPath() {
  const p = window.location.pathname || "/";
  if (p.length > 1 && p.endsWith("/")) return p.slice(0, -1);
  return p;
}

function isAdminRoute(path = normalizedPath()) {
  return path === "/admin" || path.startsWith("/admin/");
}

function currentPublicRoute(path = normalizedPath()) {
  if (path === "/" || path === "/services" || path === "/portfolio" || path === "/contact") return path;
  return "/";
}

function applyRouteShell() {
  const path = normalizedPath();
  const admin = isAdminRoute(path);
  const publicSite = document.getElementById("public-site");
  const adminApp = document.getElementById("admin-app");
  if (publicSite) publicSite.classList.toggle("hidden", admin);
  if (adminApp) adminApp.classList.toggle("hidden", !admin);

  const publicRoute = currentPublicRoute(path);
  document.querySelectorAll("[data-public-page]").forEach((el) => {
    el.classList.toggle("hidden", el.getAttribute("data-public-page") !== publicRoute);
  });
  document.querySelectorAll(".public-nav-link").forEach((el) => {
    const href = el.getAttribute("data-public-route") || "";
    el.classList.toggle("active", href === (admin ? "/admin" : publicRoute));
  });

  const routeLabel = document.getElementById("adminRouteLabel");
  if (routeLabel && admin) routeLabel.textContent = path;
}

function setAdminAccess(authed) {
  const protectedEl = document.getElementById("admin-protected");
  if (protectedEl) protectedEl.hidden = !authed;
}

async function loadAppModules() {
  if (modulesLoaded) return;
  await import("./analyzer.js");
  await import("./app.js");
  modulesLoaded = true;
  setDebugValue("dbg-init", "complete");
  window.MB_APPLY_ADMIN_ROUTE_VIEW?.();
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
        <input type="text" id="auth-display-name" placeholder="Display name" style="display:none;width:100%;padding:12px;margin-bottom:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--text);">
        <input type="email" id="auth-email" placeholder="Email" required style="width:100%;padding:12px;margin-bottom:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--text);">
        <input type="password" id="auth-password" placeholder="Password" required style="width:100%;padding:12px;margin-bottom:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--text);">
        <button type="submit" class="primary-btn" style="width:100%;padding:12px;">Sign In</button>
      </form>
      <p id="auth-error" style="color:#e08080;margin-top:12px;font-size:14px;"></p>
      <p style="margin-top:16px;font-size:13px;color:var(--muted);">
        New? <a href="#" id="auth-toggle-signup" style="color:var(--accent);">Sign up</a>
      </p>
      <p style="margin-top:8px;font-size:13px;color:var(--muted);">
        <a href="#" id="auth-forgot-password" style="color:var(--accent);">Forgot Password?</a>
      </p>
    </div>
  `;
  container.appendChild(root);

  let isSignUp = false;
  const form = root.querySelector("#auth-form");
  const displayNameEl = root.querySelector("#auth-display-name");
  const emailEl = root.querySelector("#auth-email");
  const passwordEl = root.querySelector("#auth-password");
  const errorEl = root.querySelector("#auth-error");
  const toggleEl = root.querySelector("#auth-toggle-signup");
  const forgotEl = root.querySelector("#auth-forgot-password");

  form.onsubmit = async (e) => {
    e.preventDefault();
    errorEl.style.color = "#e08080";
    errorEl.textContent = "";
    console.log("sign in clicked");
    try {
      if (isSignUp) {
        console.log("sign up clicked");
        const displayName = (displayNameEl?.value || "").trim();
        if (!displayName) {
          console.error("sign up failed", { message: "Display name is required." });
          errorEl.textContent = "Display name is required for sign up.";
          return;
        }
        const metadata = {
          display_name: displayName,
          workspace_name: `${displayName}'s Workspace`,
        };
        console.log("sign up metadata sent", metadata);
        const { error } = await signUp(emailEl.value, passwordEl.value, metadata);
        if (error) {
          console.error("sign up failed", error);
          errorEl.textContent = error.message || "Sign up failed. Please check your details and try again.";
          return;
        }
        console.log("sign up success");
        errorEl.style.color = "#7ef5b7";
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
      if (isSignUp) {
        console.error("sign up failed", err);
        errorEl.textContent = err.message || "Sign up failed. Please check your details and try again.";
      } else {
        console.error("sign in failed", err);
        errorEl.textContent = err.message || "Login failed. Check email and password.";
      }
    }
  };

  toggleEl.onclick = (e) => {
    e.preventDefault();
    isSignUp = !isSignUp;
    if (displayNameEl) {
      displayNameEl.style.display = isSignUp ? "block" : "none";
      displayNameEl.required = isSignUp;
      if (!isSignUp) displayNameEl.value = "";
    }
    form.querySelector('button[type="submit"]').textContent = isSignUp ? "Sign Up" : "Sign In";
    toggleEl.textContent = isSignUp ? "Sign in instead" : "Sign up";
    if (!isSignUp) {
      errorEl.style.color = "#e08080";
      errorEl.textContent = "";
    }
  };

  forgotEl.onclick = async (e) => {
    e.preventDefault();
    console.log("forgot password clicked");

    const defaultEmail = emailEl.value?.trim() || "";
    const requestedEmail = window.prompt("Enter your email address to reset your password:", defaultEmail);
    const email = (requestedEmail || "").trim();
    if (!email) {
      errorEl.style.color = "#e08080";
      errorEl.textContent = "Unable to send reset email.";
      return;
    }

    console.log("password reset requested");
    try {
      const { error } = await requestPasswordReset(email);
      if (error) {
        const msg = String(error.message || "").toLowerCase();
        if (msg.includes("not found") || msg.includes("no user")) {
          errorEl.style.color = "#e08080";
          errorEl.textContent = "No account found for that email.";
        } else {
          errorEl.style.color = "#e08080";
          errorEl.textContent = "Unable to send reset email.";
        }
        console.error("password reset failed", error);
        return;
      }
      errorEl.style.color = "#7ef5b7";
      errorEl.textContent = "Reset email sent. Check your inbox.";
      console.log("password reset success");
      setTimeout(() => {
        errorEl.style.color = "#e08080";
      }, 3000);
    } catch (err) {
      errorEl.style.color = "#e08080";
      errorEl.textContent = "Unable to send reset email.";
      console.error("password reset failed", err);
    }
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
  applyRouteShell();
  console.log("auth bootstrap start");
  setDebugValue("dbg-init", "booting");
  const cssOk = document.styleSheets.length > 0 && getComputedStyle(document.documentElement).getPropertyValue("--bg").trim();
  console.log("[Massive Brain] Boot | stylesheets:", document.styleSheets.length, "| --bg set:", !!cssOk);
  if (!isAdminRoute()) {
    setDebugValue("dbg-init", "public");
    return;
  }
  void checkBackend();
  try {
    if (!isCloudMode() || !supabase) {
      window.MB_USE_CLOUD = false;
      setDebugValue("dbg-auth", "no user");
      setAdminAccess(false);
      const container = document.getElementById("auth-container");
      if (container) {
        container.innerHTML = `<div class="panel" style="max-width:520px;margin:24px auto;">
          <h3>Admin login unavailable</h3>
          <p>Supabase auth is not configured. Set Vercel env vars and reload.</p>
        </div>`;
      }
      applyPresentationBranding(null, null);
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
      setAdminAccess(false);
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
            const workspaceCtx = await resolveWorkspaceContext();
            applyPresentationBranding(freshSession, workspaceCtx);
            setAdminAccess(true);
            renderLoggedInBar(handleLogout);
            setAuthEmailDisplay(freshSession);
            setDebugValue("dbg-auth", "signed-in");
            await loadAppModules();
            await window.refreshScoutData?.();
          } else {
            window.MB_USE_CLOUD = false;
            setAdminAccess(false);
            setDebugValue("dbg-auth", "no user");
          }
        });
      }
      return;
    }

    window.MB_SESSION = session;
    setCloudBridge();
    const workspaceCtx = await resolveWorkspaceContext();
    applyPresentationBranding(session, workspaceCtx);
    setAdminAccess(true);
    renderLoggedInBar(handleLogout);
    setAuthEmailDisplay(session);
    setDebugValue("dbg-auth", "signed-in");
    await loadAppModules();
  } catch (err) {
    console.error("auth bootstrap failed:", err);
    window.MB_USE_CLOUD = false;
    setAdminAccess(false);
    applyPresentationBranding(null, null);
    setDebugValue("dbg-auth", "no user");
    setDebugValue("dbg-init", "error");
  }
}

function setCloudBridge() {
  window.MB_USE_CLOUD = true;
  window.MB_FETCH_SCOUT_DATA = fetchScoutDataFromSupabase;
  window.MB_GET_CASE = getCaseFromSupabase;
  window.MB_UPDATE_CASE = updateCaseInSupabase;
  window.MB_REGENERATE_OUTREACH = regenerateOutreachForCaseInSupabase;
}

function handleLogout() {
  clearWorkspaceContext();
  window.MB_USE_CLOUD = false;
  window.MB_SESSION = null;
  window.MB_FETCH_SCOUT_DATA = null;
  window.MB_GET_CASE = null;
  window.MB_UPDATE_CASE = null;
  window.MB_REGENERATE_OUTREACH = null;
  setAdminAccess(false);
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
window.addEventListener("popstate", () => {
  applyRouteShell();
  if (isAdminRoute()) {
    window.MB_APPLY_ADMIN_ROUTE_VIEW?.();
  }
});

initDebugOverlay();
setDebugValue("dbg-api", import.meta.env.VITE_API_BASE_URL || "missing");
setDebugValue("dbg-supabase", import.meta.env.VITE_SUPABASE_URL ? "configured" : "missing");
setDebugValue("dbg-auth", "...");
setDebugValue("dbg-init", "starting");
setDebugValue("dbg-backend", "checking");

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    applyPresentationBranding(null, null);
    applyRouteShell();
    boot();
  });
} else {
  applyPresentationBranding(null, null);
  applyRouteShell();
  boot();
}
