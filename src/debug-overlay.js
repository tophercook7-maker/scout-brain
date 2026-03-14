export function initDebugOverlay() {
  const existing = document.getElementById("debug-overlay");
  if (existing) return;

  const overlay = document.createElement("div");
  overlay.id = "debug-overlay";

  overlay.style.position = "fixed";
  overlay.style.bottom = "10px";
  overlay.style.right = "10px";
  overlay.style.padding = "10px";
  overlay.style.background = "rgba(0,0,0,0.8)";
  overlay.style.color = "#00ff88";
  overlay.style.fontSize = "12px";
  overlay.style.fontFamily = "monospace";
  overlay.style.zIndex = "9999";
  overlay.style.borderRadius = "6px";
  overlay.style.maxWidth = "300px";
  overlay.style.lineHeight = "1.4";

  overlay.innerHTML = `
  <b>Scout-Brain Debug</b><br/>
  API: <span id="dbg-api">...</span><br/>
  Supabase: <span id="dbg-supabase">...</span><br/>
  Auth: <span id="dbg-auth">...</span><br/>
  App Init: <span id="dbg-init">...</span><br/>
  Backend: <span id="dbg-backend">...</span>
  `;

  if (document.body) {
    document.body.appendChild(overlay);
  } else {
    document.addEventListener("DOMContentLoaded", () => {
      document.body?.appendChild(overlay);
    }, { once: true });
  }
}

export function setDebugValue(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}
