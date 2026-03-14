/* eslint-disable no-console */
console.log("app.js loaded");

const STORAGE_KEY = "massive-brain-v0.4";

const starterData = {
  opportunities: [],
  ideas: [],
  projects: [],
  memory: []
};

function loadState() {
  const v4 = localStorage.getItem("massive-brain-v0.4");
  const v3 = localStorage.getItem("massive-brain-v0.3");
  const v2 = localStorage.getItem("massive-brain-v0.2");

  if (v4) return normalizeItemFields(JSON.parse(v4));
  if (v3) {
    const data = normalizeItemFields(JSON.parse(v3));
    localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    return data;
  }
  if (v2) {
    const data = normalizeItemFields(JSON.parse(v2));
    localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
    return data;
  }

  return structuredClone(starterData);
}

function normalizeItemFields(data) {
  for (const bucket of ["opportunities", "ideas", "projects", "memory"]) {
    data[bucket] = (data[bucket] || []).map(item => ({
      title: "",
      description: "",
      pitch: "",
      build: "",
      notes: "",
      ...item,
      pitch: item.pitch ?? item.why ?? "",
      build: item.build ?? item.nextAction ?? ""
    }));
  }
  return data;
}

let state;
try {
  state = loadState();
} catch (e) {
  console.error("loadState failed:", e);
  state = { opportunities: [], ideas: [], projects: [], memory: [] };
}

/** Scout / Morning Runner config from scout/config.json. Loaded at runtime. */
let scoutConfig = null;

function saveState() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function exportBrain() {
  const data = JSON.stringify(state, null, 2);
  const blob = new Blob([data], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "massive-brain-backup.json";
  a.click();
  URL.revokeObjectURL(url);
}

function importBrain(file) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function (e) {
    try {
      const data = JSON.parse(e.target.result);
      if (!data.opportunities || !data.ideas || !data.projects || !data.memory) {
        alert("Invalid backup: missing opportunities, ideas, projects, or memory.");
        return;
      }
      state = normalizeItemFields(data);
      saveState();
      renderAll();
    } catch (err) {
      alert("Could not parse backup file: " + err.message);
    }
  };
  reader.readAsText(file);
}

function createCard(item, bucket, index) {
  const card = document.createElement("div");
  card.className = "item-card";

  card.innerHTML = `
    <h3>${item.title || "Untitled"}</h3>
    <p>${item.description || ""}</p>
    <button class="open-btn" type="button">Open</button>
  `;

  card.querySelector(".open-btn").onclick = () => openModal(bucket, index);

  return card;
}

function renderList(container, bucket) {
  const el = document.getElementById(container);
  el.innerHTML = "";

  state[bucket].forEach((item, i) => {
    el.appendChild(createCard(item, bucket, i));
  });

  const countId = container.replace("-list", "-count");
  const countEl = document.getElementById(countId);
  if (countEl) countEl.textContent = state[bucket].length;
}

function renderAll() {
  renderList("opportunities-list", "opportunities");
  renderList("ideas-list", "ideas");
  renderList("projects-list", "projects");
  renderList("memory-list", "memory");
}

function openModal(bucket, index) {
  const item = state[bucket][index];

  document.getElementById("modalTitle").value = item.title || "";
  document.getElementById("modalDescription").value = item.description || "";
  document.getElementById("modalPitch").value = item.pitch || item.why || "";
  document.getElementById("modalBuild").value = item.build || item.nextAction || "";
  document.getElementById("modalNotes").value = item.notes || "";

  showTab("overview");

  document.getElementById("modalSave").onclick = () => {
    item.title = document.getElementById("modalTitle").value.trim();
    item.description = document.getElementById("modalDescription").value.trim();
    item.pitch = document.getElementById("modalPitch").value.trim();
    item.build = document.getElementById("modalBuild").value.trim();
    item.notes = document.getElementById("modalNotes").value.trim();

    saveState();
    renderAll();
    document.getElementById("modal").classList.add("hidden");
    document.getElementById("modal").setAttribute("aria-hidden", "true");
  };

  document.getElementById("modal").classList.remove("hidden");
  document.getElementById("modal").setAttribute("aria-hidden", "false");
}

function closeModal() {
  document.getElementById("modal").classList.add("hidden");
  document.getElementById("modal").setAttribute("aria-hidden", "true");
}

function showTab(tabName) {
  document.querySelectorAll(".tab").forEach((el) => el.classList.remove("active"));
  document.querySelectorAll(".tabs button").forEach((el) => el.classList.remove("active"));
  const panel = document.getElementById("tab-" + tabName);
  const btn = document.querySelector('.tabs button[data-tab="' + tabName + '"]');
  if (panel) panel.classList.add("active");
  if (btn) btn.classList.add("active");
}

function showMainTab(tabName) {
  document.querySelectorAll(".tab-panel").forEach((el) => el.classList.remove("active"));
  document.querySelectorAll('.main-nav-btn[data-main-tab]').forEach((el) => el.classList.remove("active"));
  const panel = document.getElementById(tabName);
  const btn = document.querySelector('.main-nav-btn[data-main-tab="' + tabName + '"]');
  if (panel) panel.classList.add("active");
  if (btn) btn.classList.add("active");

  if (tabName === "scout" && !scoutConfig) loadScoutConfig();
}

function currentAdminModule() {
  const path = (window.location.pathname || "").replace(/\/$/, "");
  if (path === "/admin" || path === "/admin/dashboard") return "dashboard";
  if (path === "/admin/scout") return "scout";
  if (path === "/admin/leads") return "leads";
  if (path === "/admin/cases") return "cases";
  if (path === "/admin/outreach") return "outreach";
  if (path === "/admin/notes") return "notes";
  return "dashboard";
}

function setActiveAdminModuleNav(module) {
  document.querySelectorAll(".admin-module-link").forEach((el) => {
    el.classList.toggle("active", el.getAttribute("data-admin-module") === module);
  });
}

function setVisible(el, show) {
  if (!el) return;
  el.classList.toggle("hidden", !show);
}

function applyAdminRouteView() {
  const path = (window.location.pathname || "").replace(/\/$/, "");
  if (!(path === "/admin" || path.startsWith("/admin/"))) return;
  const module = currentAdminModule();
  if (module === "dashboard") {
    console.log("dashboard route active");
  }
  setActiveAdminModuleNav(module);
  const routeLabel = document.getElementById("adminRouteLabel");
  if (routeLabel) routeLabel.textContent = path;
  const moduleDescription = document.getElementById("adminModuleDescription");
  const moduleIntro = document.getElementById("adminModuleIntro");
  const moduleTitle = document.getElementById("adminModuleTitle");
  const moduleHelp = document.getElementById("adminModuleHelp");
  const dashboardSummary = document.getElementById("adminDashboardSummary");
  const commandCenter = document.getElementById("adminCommandCenter");
  const dashboardGrid = document.getElementById("dashboardGrid");
  const analyzeConsole = document.getElementById("analyzeConsole");
  const morningRunnerPanel = document.getElementById("morningRunnerPanel");
  const moduleCards = document.querySelectorAll("[data-module-card]");
  if (moduleDescription) {
    const labels = {
      dashboard: "Dashboard",
      scout: "Scout engine",
      leads: "Leads list",
      cases: "Business dossiers",
      outreach: "Outreach queue",
      notes: "Notes",
    };
    moduleDescription.textContent = labels[module] || "Dashboard";
  }

  const moduleGuidance = {
    dashboard: "Overview of pipeline counts, discovered leads, follow-ups, and top opportunities.",
    scout: "Run Scout and review latest scans with the location used for distance calculations.",
    leads: "Sortable lead list with status tags and one-click access to full case dossiers.",
    cases: "Full business dossier review with deep research, contact matrix, and outreach pack.",
    outreach: "Contact queue focused on follow-ups and rapid status changes.",
    notes: "Central notes workspace for CRM context and follow-up memory.",
  };
  if (moduleTitle) moduleTitle.textContent = moduleDescription?.textContent || "Dashboard";
  if (moduleHelp) moduleHelp.textContent = moduleGuidance[module] || moduleGuidance.dashboard;

  moduleCards.forEach((card) => card.classList.remove("hidden"));
  setVisible(moduleIntro, module !== "dashboard");
  setVisible(dashboardSummary, module === "dashboard");
  setVisible(commandCenter, module === "dashboard");
  setVisible(analyzeConsole, module === "scout");

  if (module === "dashboard") {
    _runnerStatusFilter = "all";
    _runnerSort = "priority";
    setVisible(dashboardGrid, true);
    setVisible(morningRunnerPanel, false);
    console.log("rendering dashboard view");
    const fallbackToday = {
      generated_at: null,
      summary: "No scout run yet.",
      top_opportunities: [],
      case_slugs: [],
    };
    const today = _runnerLastPayload?.today || fallbackToday;
    const opportunities = _runnerLastPayload?.opportunities || [];
    updateAdminSummary(today, opportunities);
    console.log("dashboard data loaded");
    console.log("dashboard render complete");
  } else if (module === "scout") {
    _runnerStatusFilter = "all";
    _runnerSort = "priority";
    setVisible(dashboardGrid, false);
    setVisible(morningRunnerPanel, true);
  } else if (module === "leads") {
    _runnerStatusFilter = "all";
    _runnerSort = "name";
    setVisible(dashboardGrid, false);
    setVisible(morningRunnerPanel, true);
  } else if (module === "cases") {
    _runnerStatusFilter = "all";
    _runnerSort = "priority";
    setVisible(dashboardGrid, false);
    setVisible(morningRunnerPanel, true);
  } else if (module === "outreach") {
    _runnerStatusFilter = "Follow up";
    _runnerSort = "priority";
    setVisible(dashboardGrid, false);
    setVisible(morningRunnerPanel, true);
  } else if (module === "notes") {
    setVisible(dashboardGrid, true);
    setVisible(morningRunnerPanel, false);
    moduleCards.forEach((card) => {
      card.classList.toggle("hidden", card.getAttribute("data-module-card") !== "memory");
    });
  }

  if (_runnerLastPayload) {
    renderMorningRunner(
      _runnerLastPayload.today,
      _runnerLastPayload.opportunities,
      _runnerLastPayload.stdout,
      _runnerLastPayload.stderr
    );
  }
}

function loadScoutConfig() {
  const statusEl = document.getElementById("scoutConfigStatus");
  const cityInput = document.getElementById("scoutCity");
  fetch(`${(window.MB_API_BASE || "").replace(/\/$/, "")}/scout/config.json`)
    .then((r) => {
      if (!r.ok) throw new Error(r.statusText);
      return r.json();
    })
    .then((data) => {
      scoutConfig = data;
      if (cityInput && data.home_city) {
        cityInput.placeholder = data.home_city;
        if (!cityInput.value.trim()) cityInput.value = data.home_city;
      }
      if (statusEl) {
        statusEl.textContent = `Config loaded: ${data.home_city}, ${data.search_radius_miles} mi, ${(data.categories || []).join(", ")} (max ${data.max_results_per_category || 5} per category${data.ignore_chains ? ", chains ignored" : ""}).`;
        statusEl.classList.remove("scout-config-error");
      }
    })
    .catch(() => {
      scoutConfig = null;
      if (statusEl) {
        statusEl.textContent = "No scout/config.json found. Using form values only.";
        statusEl.classList.add("scout-config-error");
      }
    });
}

function runScout() {
  console.log("runScout (Scout tab) called");
  const cityEl = document.getElementById("scoutCity");
  const cityInput = cityEl ? cityEl.value.trim() : "";
  const typeEl = document.getElementById("scoutType");
  const typeOverride = typeEl ? typeEl.value.trim() : "";

  if (scoutConfig) {
    const city = cityInput || scoutConfig.home_city || "City";
    const categories = typeOverride
      ? [typeOverride]
      : (scoutConfig.categories || ["diner", "coffee shop", "church"]);
    const maxPer = Math.max(1, scoutConfig.max_results_per_category || 5);
    const ignoreChains = !!scoutConfig.ignore_chains;

    const allLeads = [];
    categories.forEach((category) => {
      const raw = generateLeads(city, category);
      const limited = raw.slice(0, maxPer);
      limited.forEach((lead) => {
        if (ignoreChains && isLikelyChain(lead.name)) return;
        allLeads.push(lead);
      });
    });
    renderScout(allLeads);
    return;
  }

  const leads = generateLeads(cityInput || "City", typeOverride || "business");
  renderScout(leads);
}

function isLikelyChain(name) {
  if (!name) return false;
  const lower = name.toLowerCase();
  const chainClues = ["chain", "franchise", "mcdonald", "starbucks", "subway", "dunkin", "walmart", "target"];
  return chainClues.some((c) => lower.includes(c));
}

function generateLeads(city, type) {
  const templates = [
    {
      name: `${type} House`,
      city: city,
      problem: "Outdated or basic website",
      pitch: "Modern mobile-friendly site with menu updates and online directions",
      next: "Show demo on iPad"
    },
    {
      name: `Downtown ${type}`,
      city: city,
      problem: "No online menu system",
      pitch: "Add a simple menu updater so staff can change prices easily",
      next: "Offer $900 website rebuild"
    },
    {
      name: `Family ${type}`,
      city: city,
      problem: "Weak mobile experience",
      pitch: "Fast mobile design + click-to-call + Google Maps",
      next: "Invite owner to see demo"
    },
    {
      name: `Main Street ${type}`,
      city: city,
      problem: "Hard to find hours and location on phone",
      pitch: "Clear hours, map, and tap-to-call on one screen",
      next: "Send a quick mockup"
    },
    {
      name: `Local ${type}`,
      city: city,
      problem: "Site looks dated compared to social presence",
      pitch: "Match your website to the quality of your socials",
      next: "Book a short call"
    }
  ];
  return templates;
}

function renderScout(leads) {
  const container = document.getElementById("scoutResults");
  container.innerHTML = "";

  leads.forEach((lead) => {
    const card = document.createElement("div");
    card.className = "scout-card";
    card.innerHTML = `
      <h3>${lead.name}</h3>
      <p><strong>City:</strong> ${lead.city}</p>
      <p><strong>Problem:</strong> ${lead.problem}</p>
      <p><strong>Pitch:</strong> ${lead.pitch}</p>
      <p><strong>Next Step:</strong> ${lead.next}</p>
      <button type="button" class="scout-add-btn">Add to Opportunities</button>
    `;
    card.querySelector(".scout-add-btn").onclick = () => addOpportunity(lead);
    container.appendChild(card);
  });
}

function addOpportunity(lead) {
  state.opportunities.push({
    title: lead.name,
    description: lead.problem,
    pitch: lead.pitch,
    build: lead.next,
    notes: ""
  });
  saveState();
  renderAll();
  alert("Opportunity added to Brain");
}

function loadPitch(type) {
  const pitches = {
    diner: {
      title: "Modern Diner Website",
      problem: "Most diners have outdated websites or menus that are hard to read on phones.",
      solution: "I build simple modern diner sites that show menus clearly, load fast, and help customers find you.",
      price: "$900 build + $89/month support",
      demo: "Show the Southern Diner concept page on iPad"
    },
    coffee: {
      title: "Coffee Shop Website",
      problem: "Many coffee shops rely only on Instagram or Facebook.",
      solution: "A clean website helps people see your menu, hours, and location instantly.",
      price: "$900 build + $89/month support",
      demo: "Show Coffee Shop Starter Kit demo"
    },
    church: {
      title: "Church Website",
      problem: "Many churches have confusing or outdated sites.",
      solution: "Simple site with events, sermons, contact info, and optional donations.",
      price: "$900 build + optional support",
      demo: "Show church template demo"
    }
  };

  const p = pitches[type];
  if (!p) return;

  const container = document.getElementById("pitchDisplay");
  container.innerHTML = `
    <div class="pitch-card">
      <h3>${p.title}</h3>
      <p><strong>Problem:</strong> ${p.problem}</p>
      <p><strong>Solution:</strong> ${p.solution}</p>
      <p><strong>Price:</strong> ${p.price}</p>
      <p><strong>Demo to Show:</strong> ${p.demo}</p>
    </div>
  `;
}

function renderBulletList(containerId, items) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = "";
  (items || []).forEach((item) => {
    const row = document.createElement("div");
    row.className = "tag analysis-bullet-item";
    row.textContent = item;
    el.appendChild(row);
  });
}

async function runAnalyzeDraft() {
  console.log("runAnalyzeDraft called");
  const nameEl = document.getElementById("analyzeName");
  const g = (id) => { const el = document.getElementById(id); return el ? el.value.trim() : ""; };
  const name = g("analyzeName");
  const type = g("analyzeType");
  const website = g("analyzeWebsite");
  const city = g("analyzeCity");

  if (!name || !type || !website) {
    alert("Add at least business name, type, and website.");
    return;
  }

  let url = website;
  if (!/^https?:\/\//i.test(url)) url = "https://" + url;

  document.getElementById("analysisProblems").innerHTML = '<div class="tag analysis-bullet-item">Auditing site...</div>';
  document.getElementById("analysisPitch").innerHTML = '<div class="tag analysis-bullet-item">Thinking...</div>';
  const factsEl = document.getElementById("analysisFacts");
  if (factsEl) factsEl.innerHTML = "";

  try {
    const audit = await auditWebsiteLive(url);
    window.lastAuditResult = { name, type, website, city, audit };

    renderBulletList("analysisProblems", audit.problems || []);
    renderBulletList("analysisPitch", audit.pitch || []);

    if (factsEl) {
      (audit.facts || []).forEach((f) => {
        const row = document.createElement("div");
        row.className = "tag analysis-bullet-item";
        row.textContent = f;
        factsEl.appendChild(row);
      });
    }

    const email = buildEmailFromAudit({ name, type, city, audit });
    document.getElementById("analysisSubject").value = email.emailSubject;
    document.getElementById("analysisEmail").value = email.emailBody;
  } catch (err) {
    console.error(err);
    alert("Audit failed. Make sure your backend API is reachable and VITE_API_BASE_URL is set correctly.");
  }
}

function copyEmailDraft() {
  console.log("copyEmailDraft called");
  const subjEl = document.getElementById("analysisSubject");
  const bodyEl = document.getElementById("analysisEmail");
  const subject = subjEl ? subjEl.value.trim() : "";
  const body = bodyEl ? bodyEl.value.trim() : "";
  const full = `Subject: ${subject}\n\n${body}`;
  navigator.clipboard.writeText(full).then(() => {
    alert("Email copied.");
  });
}

function saveAnalyzeAsOpportunity() {
  const name = document.getElementById("analyzeName").value.trim();
  const type = document.getElementById("analyzeType").value.trim();
  const website = document.getElementById("analyzeWebsite").value.trim();
  const city = document.getElementById("analyzeCity").value.trim();
  const body = document.getElementById("analysisEmail").value.trim();

  if (!name) return alert("Run an analysis first.");

  const audit = window.lastAuditResult?.audit || { problems: [], pitch: [], facts: [] };

  state.opportunities.unshift({
    title: name,
    description: `${type}${city ? ` in ${city}` : ""} — ${website}`,
    pitch: (audit.pitch || []).join("\n"),
    build: (audit.facts || []).join("\n"),
    notes: body
  });

  saveState();
  renderAll();
  alert("Saved to Opportunities.");
}

function saveAnalyzeAsIdea() {
  const name = document.getElementById("analyzeName").value.trim();
  const type = document.getElementById("analyzeType").value.trim();

  if (!name) return alert("Run an analysis first.");

  const audit = window.lastAuditResult?.audit || { problems: [], pitch: [], facts: [] };

  state.ideas.unshift({
    title: `${type} pitch for ${name}`,
    description: "Generated from Analyze + Draft",
    pitch: (audit.pitch || []).join("\n"),
    build: (audit.problems || []).join("\n"),
    notes: document.getElementById("analysisEmail").value.trim()
  });

  saveState();
  renderAll();
  alert("Saved to Ideas.");
}

async function fetchScoutData(options = {}) {
  const cacheBust = !!options.cacheBust;
  if (window.MB_FETCH_SCOUT_DATA) {
    console.log("fetchScoutData: using Supabase");
    return window.MB_FETCH_SCOUT_DATA(options);
  }
  const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
  const query = cacheBust ? `?t=${Date.now()}` : "";
  const url = `${apiBase}/scout-data${query}`;
  console.log("fetchScoutData: calling GET", url);
  const response = await fetch(url, { cache: "no-store" });
  console.log("fetchScoutData: status", response.status);
  if (!response.ok) throw new Error("Could not load scout data");
  return response.json();
}

async function updateCaseStatus(slug, updates) {
  if (window.MB_UPDATE_CASE) {
    await window.MB_UPDATE_CASE(slug, updates);
    return {};
  }
  const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
  const res = await fetch(`${apiBase}/case/${encodeURIComponent(slug)}/update`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(updates),
  });
  if (!res.ok) throw new Error("Could not update case");
  return res.json();
}

async function fetchCase(slug) {
  if (window.MB_GET_CASE) {
    const data = await window.MB_GET_CASE(slug);
    if (!data) throw new Error("Case not found");
    return data;
  }
  const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
  const res = await fetch(`${apiBase}/case/${encodeURIComponent(slug)}`);
  if (!res.ok) throw new Error("Could not load case");
  return res.json();
}

function defaultEmailAlertSettings() {
  return {
    email_notifications_enabled: true,
    email_frequency: "daily",
    include_new_leads: true,
    include_followups: true,
    include_top_opportunities: true,
  };
}

function collectEmailAlertSettingsFromUI() {
  const enabledEl = document.getElementById("emailAlertsEnabled");
  const frequencyEl = document.getElementById("emailAlertsFrequency");
  const newLeadsEl = document.getElementById("emailAlertsIncludeNewLeads");
  const followupsEl = document.getElementById("emailAlertsIncludeFollowups");
  const topEl = document.getElementById("emailAlertsIncludeTopOpportunities");
  return {
    email_notifications_enabled: !!enabledEl?.checked,
    email_frequency: (frequencyEl?.value || "daily").toLowerCase(),
    include_new_leads: !!newLeadsEl?.checked,
    include_followups: !!followupsEl?.checked,
    include_top_opportunities: !!topEl?.checked,
  };
}

function applyEmailAlertSettingsToUI(settings) {
  const merged = { ...defaultEmailAlertSettings(), ...(settings || {}) };
  const enabledEl = document.getElementById("emailAlertsEnabled");
  const frequencyEl = document.getElementById("emailAlertsFrequency");
  const newLeadsEl = document.getElementById("emailAlertsIncludeNewLeads");
  const followupsEl = document.getElementById("emailAlertsIncludeFollowups");
  const topEl = document.getElementById("emailAlertsIncludeTopOpportunities");
  if (enabledEl) enabledEl.checked = !!merged.email_notifications_enabled;
  if (frequencyEl) frequencyEl.value = merged.email_frequency || "daily";
  if (newLeadsEl) newLeadsEl.checked = !!merged.include_new_leads;
  if (followupsEl) followupsEl.checked = !!merged.include_followups;
  if (topEl) topEl.checked = !!merged.include_top_opportunities;
}

function setEmailAlertsStatus(text, isError = false) {
  const el = document.getElementById("emailAlertsStatus");
  if (!el) return;
  el.textContent = text;
  el.classList.toggle("status-failed", !!isError);
}

async function loadEmailAlertSettings() {
  try {
    let settings = null;
    if (window.MB_GET_USER_SETTINGS) {
      settings = await window.MB_GET_USER_SETTINGS();
    } else {
      const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
      const headers = {};
      if (window.MB_SESSION?.access_token) headers.Authorization = `Bearer ${window.MB_SESSION.access_token}`;
      if (window.MB_WORKSPACE?.workspaceId) headers["X-Workspace-Id"] = window.MB_WORKSPACE.workspaceId;
      const res = await fetch(`${apiBase}/user-settings`, { headers });
      if (!res.ok) throw new Error("Could not load email settings");
      settings = await res.json();
    }
    applyEmailAlertSettingsToUI(settings || defaultEmailAlertSettings());
    setEmailAlertsStatus("Loaded");
  } catch (err) {
    console.error("email settings load failed", err);
    applyEmailAlertSettingsToUI(defaultEmailAlertSettings());
    setEmailAlertsStatus("Unavailable", true);
  }
}

async function saveEmailAlertSettings() {
  const saveBtn = document.getElementById("saveEmailAlertsBtn");
  const original = saveBtn?.textContent || "Save Email Alerts";
  try {
    const payload = collectEmailAlertSettingsFromUI();
    if (saveBtn) {
      saveBtn.disabled = true;
      saveBtn.textContent = "Saving...";
    }
    let saved = null;
    if (window.MB_SAVE_USER_SETTINGS) {
      saved = await window.MB_SAVE_USER_SETTINGS(payload);
    } else {
      const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
      const headers = { "Content-Type": "application/json" };
      if (window.MB_SESSION?.access_token) headers.Authorization = `Bearer ${window.MB_SESSION.access_token}`;
      if (window.MB_WORKSPACE?.workspaceId) headers["X-Workspace-Id"] = window.MB_WORKSPACE.workspaceId;
      const res = await fetch(`${apiBase}/user-settings`, {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error("Could not save email settings");
      saved = await res.json();
    }
    applyEmailAlertSettingsToUI(saved || payload);
    setEmailAlertsStatus("Saved");
  } catch (err) {
    console.error("email settings save failed", err);
    setEmailAlertsStatus("Save failed", true);
  } finally {
    if (saveBtn) {
      saveBtn.disabled = false;
      saveBtn.textContent = original;
    }
  }
}

let _runnerPreviousCount = null;
let _runnerStatusFilter = "all";
let _runnerSort = "priority";
let _runnerOpportunities = [];
let _runnerLastPayload = null;

const LEAD_STATUSES = ["New", "Ready to contact", "Contacted", "Follow up", "Closed", "Skip"];

function setRunnerStatus(status) {
  const el = document.getElementById("runnerStatus");
  if (!el) return;
  el.textContent = status;
  const slug = status.replace(/\.\.\./g, "").toLowerCase();
  el.className = "runner-status status-" + (slug === "running" ? "running" : slug);
}

function setRunnerLoading(show) {
  const el = document.getElementById("runnerLoadingIndicator");
  if (!el) return;
  el.classList.toggle("hidden", !show);
  el.setAttribute("aria-hidden", show ? "false" : "true");
}

function flashRunnerPanel() {
  const panel = document.getElementById("morningRunnerPanel");
  if (!panel) return;
  panel.classList.remove("runner-panel-success-flash");
  void panel.offsetWidth;
  panel.classList.add("runner-panel-success-flash");
  setTimeout(() => panel.classList.remove("runner-panel-success-flash"), 800);
}

async function runScoutNow() {
  console.log("runScoutNow called");
  const btn = document.getElementById("runMorningScoutBtn");
  const summary = document.getElementById("morningRunnerSummary");
  const generated = document.getElementById("scoutGeneratedAt");
  const locationStatus = document.getElementById("scoutLocationStatus");

  const prevCount = _runnerPreviousCount;

  if (btn) {
    btn.disabled = true;
    btn.textContent = "Running Scout...";
    console.log("Button disabled, text changed to Running Scout...");
  }
  setRunnerStatus("Running...");
  setRunnerLoading(true);
  const errorBanner = document.getElementById("scoutErrorBanner");
  const errorBody = document.getElementById("scoutErrorBody");
  const errorDetailsPre = document.getElementById("scoutErrorDetailsPre");
  if (errorBanner) {
    errorBanner.classList.add("hidden");
    if (errorBody) errorBody.textContent = "";
    if (errorDetailsPre) errorDetailsPre.textContent = "";
  }
  if (summary) summary.textContent = "Scout is running... searching and investigating businesses.";
  if (generated) generated.textContent = "Scout is running...";

  async function resolveDeviceLocation() {
    const savedHome = scoutConfig?.home_city ? `Saved location: ${scoutConfig.home_city}` : "Using saved home location";
    console.log("requesting device location");
    if (!("geolocation" in navigator)) {
      console.log("device location denied");
      if (locationStatus) locationStatus.textContent = "Location unavailable, using saved home location";
      if (summary) summary.textContent = savedHome;
      return null;
    }
    return new Promise((resolve) => {
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          const coords = {
            current_lat: pos.coords.latitude,
            current_lng: pos.coords.longitude,
          };
          console.log("device location received", coords);
          if (locationStatus) {
            locationStatus.textContent = `Current location: ${coords.current_lat.toFixed(4)}, ${coords.current_lng.toFixed(4)}`;
          }
          resolve(coords);
        },
        () => {
          console.log("device location denied");
          if (locationStatus) locationStatus.textContent = "Location unavailable, using saved home location";
          if (summary) summary.textContent = savedHome;
          resolve(null);
        },
        {
          enableHighAccuracy: true,
          timeout: 8000,
          maximumAge: 300000,
        }
      );
    });
  }

  function showScoutError(userMsg, errType, errMsg, fullData) {
    setRunnerStatus("Failed");
    if (summary) summary.textContent = userMsg;
    if (generated) generated.textContent = "Scout failed";
    if (errorBanner) {
      if (errorBody) errorBody.textContent = userMsg;
      if (errorDetailsPre) {
        const detailText = [errType && `error_type: ${errType}`, errMsg && `error_message: ${errMsg}`].filter(Boolean).join("\n");
        errorDetailsPre.textContent = detailText || (fullData ? JSON.stringify(fullData, null, 2) : "");
      }
      errorBanner.classList.remove("hidden");
    }
    console.log("Scout failure reason:", userMsg);
  }

  try {
    const deviceLocation = await resolveDeviceLocation();
    const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
    const runScoutUrl = `${apiBase}/run-scout`;
    console.log("Calling", runScoutUrl);
    const headers = { "Content-Type": "application/json" };
    if (window.MB_SESSION?.access_token) {
      headers["Authorization"] = `Bearer ${window.MB_SESSION.access_token}`;
    }
    if (window.MB_WORKSPACE?.workspaceId) {
      headers["X-Workspace-Id"] = window.MB_WORKSPACE.workspaceId;
    }
    const payload = deviceLocation || {};
    if (deviceLocation) {
      console.log("run scout using current location");
      if (summary) summary.textContent = "Using current location";
    } else {
      console.log("run scout using saved config location");
      const savedHome = scoutConfig?.home_city ? `Saved location: ${scoutConfig.home_city}` : "Using saved home location";
      if (locationStatus) locationStatus.textContent = savedHome;
      if (summary) summary.textContent = savedHome;
    }
    const response = await fetch(runScoutUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    console.log("Scout response status:", response.status);

    let data;
    try {
      const text = await response.text();
      try {
        data = JSON.parse(text);
      } catch (parseErr) {
        console.error("Scout response JSON parse failed. Response may be HTML or non-JSON:", text.slice(0, 200));
        data = { success: false, user_friendly_message: `Server returned non-JSON (${response.status}). Is the scout server running?` };
      }
    } catch (readErr) {
      console.error("Scout response read error:", readErr);
      data = { success: false, user_friendly_message: "Could not read scout response. Network or server error." };
    }

    console.log("Scout response JSON:", data);

    if (!response.ok || data.ok === false || data.success === false) {
      const errType = data.error_type || "scout_error";
      const errMsg = data.error_message || data.stderr || "Unknown error";
      const userMsg = data.user_friendly_message || "Scout run failed. Check the app and try again.";
      console.error("Scout failed:", { error_type: errType, error_message: errMsg, full: data });
      showScoutError(userMsg, errType, errMsg, data);
      return;
    }

    const opportunities = data.opportunities || [];
    const newCount = opportunities.length;

    renderMorningRunner(data.today, opportunities, data.stdout, data.stderr);
    updateAdminSummary(data.today, opportunities);
    console.log("run scout success");

    setRunnerStatus("Complete");
    if (summary) {
      if (prevCount !== null && prevCount === newCount && newCount > 0) {
        summary.textContent = "Scout complete — no major changes found.";
      } else {
        summary.textContent = `Scout complete — ${newCount} opportunities refreshed.`;
      }
    }
    if (generated && data.today?.generated_at) {
      generated.textContent = "Generated: " + data.today.generated_at;
    }

    _runnerPreviousCount = newCount;
    flashRunnerPanel();

    console.log("refreshing leads after scout");
    const refreshed = await refreshScoutData({
      reason: "post-run",
      cacheBust: true,
      minExpectedCount: newCount,
      retries: window.MB_FETCH_SCOUT_DATA ? 4 : 1,
      retryDelayMs: 1200,
    });
    const refreshedCount = refreshed?.opportunities?.length ?? newCount;
    const refreshedMetrics = summarizeRunMetrics(refreshed?.today, refreshed?.opportunities || opportunities);
    console.log(`lead count after refresh: ${refreshedCount}`);
    if (summary) {
      summary.textContent = `Scout complete — ${refreshedCount} leads refreshed. Processed ${refreshedMetrics.processed}, saved ${refreshedMetrics.saved}, skipped ${refreshedMetrics.skipped}, location ${refreshedMetrics.location}.`;
    }
  } catch (err) {
    console.error("Scout run exception:", err);
    const deployMsg = window.MB_USE_CLOUD
      ? "Run Scout failed. Check that VITE_API_BASE_URL points to a hosted backend with /run-scout enabled."
      : "Scout run failed. Check that the backend is running and reachable.";
    const errMsg = err.message || String(err);
    showScoutError(deployMsg, "network_or_exception", errMsg, null);
  } finally {
    setRunnerLoading(false);
    if (btn) {
      btn.disabled = false;
      btn.textContent = "Run Scout";
    }
  }
}
window.runScoutNow = runScoutNow;
console.log("runScoutNow defined");

function createRunnerTag(text) {
  const el = document.createElement("span");
  el.className = "tag";
  el.textContent = text;
  return el;
}

function valOrMissing(v) {
  if (v == null || v === "") return "Missing";
  return String(v);
}

function escapeHtml(s) {
  if (s == null || s === "") return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function buildCardDataSection(opp) {
  const wrap = document.createElement("div");
  wrap.className = "runner-card-data";
  const dataTitle = document.createElement("div");
  dataTitle.className = "runner-section-title";
  dataTitle.textContent = "Research data";
  wrap.appendChild(dataTitle);
  const email = opp.contact?.email || (opp.contact?.emails?.length ? opp.contact.emails[0] : null);
  const phone = opp.phone || opp.contact?.phone_from_site || (opp.contact?.phones?.length ? opp.contact.phones[0] : null);
  const rows = [
    ["Website", opp.website],
    ["Phone", phone],
    ["Email", email],
    ["Contact page", opp.contact?.contact_page],
    ["Facebook", opp.contact?.facebook],
    ["Instagram", opp.contact?.instagram],
    ["Recommended contact", opp.recommended_contact],
    ["Backup contact", opp.backup_contact_method],
    ["Rating", opp.rating != null ? String(opp.rating) : null],
    ["Review count", opp.review_count != null ? String(opp.review_count) : null],
    ["Hours", opp.hours],
    ["No website", opp.no_website ? "Yes" : (opp.website ? "No" : null)],
    ["Platform", opp.website_analysis?.platform],
  ];
  rows.forEach(([label, val]) => {
    const row = document.createElement("div");
    row.className = "runner-data-row";
    row.innerHTML = `<span class="runner-data-label">${label}:</span> <span class="runner-data-val ${!val && val !== 0 ? "missing" : ""}">${valOrMissing(val)}</span>`;
    if (val && (label === "Website" || label === "Contact page" || label === "Facebook" || label === "Instagram")) {
      const link = document.createElement("a");
      link.href = val;
      link.textContent = val.length > 45 ? val.slice(0, 42) + "…" : val;
      link.className = "runner-data-link";
      row.querySelector(".runner-data-val").innerHTML = "";
      row.querySelector(".runner-data-val").appendChild(link);
    }
    wrap.appendChild(row);
  });
  return wrap;
}

function buildContactMatrixSection(opp) {
  const wrap = document.createElement("div");
  wrap.className = "runner-contact-matrix";
  const cm = opp.contact_matrix || {};
  const email = opp.contact?.email || (opp.contact?.emails?.length ? opp.contact.emails[0] : null);
  const phone = opp.phone || opp.contact?.phone_from_site || (opp.contact?.phones?.length ? opp.contact.phones[0] : null);
  const rows = [
    ["Best contact", cm.best_contact || opp.recommended_contact],
    ["Backup contact", cm.backup_contact || opp.backup_contact_method],
    ["Email", email],
    ["Phone", phone],
    ["Contact page", opp.contact?.contact_page],
    ["Facebook", opp.contact?.facebook],
    ["Instagram", opp.contact?.instagram],
  ];
  const title = document.createElement("div");
  title.className = "runner-section-title";
  title.textContent = "Contact Matrix";
  wrap.appendChild(title);
  rows.forEach(([label, val]) => {
    const row = document.createElement("div");
    row.className = "runner-matrix-row";
    const isUrl = val && ["Contact page", "Facebook", "Instagram"].includes(label);
    row.innerHTML = `<span class="runner-matrix-label">${label}:</span> `;
    const valSpan = document.createElement("span");
    valSpan.className = "runner-matrix-val" + (!val ? " missing" : "");
    if (isUrl && val) {
      const a = document.createElement("a");
      a.href = val;
      a.textContent = val.length > 50 ? val.slice(0, 47) + "…" : val;
      valSpan.appendChild(a);
    } else {
      valSpan.textContent = valOrMissing(val);
    }
    row.appendChild(valSpan);
    wrap.appendChild(row);
  });
  return wrap;
}

function buildContactLinks(opp) {
  const wrap = document.createElement("div");
  wrap.className = "runner-links";
  const links = [];
  if (opp.website) links.push(["Website", opp.website]);
  if (opp.maps_url) links.push(["Maps", opp.maps_url]);
  if (opp.contact?.contact_page) links.push(["Contact Page", opp.contact.contact_page]);
  if (opp.contact?.facebook) links.push(["Facebook", opp.contact.facebook]);
  if (opp.contact?.instagram) links.push(["Instagram", opp.contact.instagram]);
  links.forEach(([label, href]) => {
    const a = document.createElement("a");
    a.href = href;
    a.className = "tag";
    a.textContent = label;
    wrap.appendChild(a);
  });
  const email = opp.contact?.email || (opp.contact?.emails?.length ? opp.contact.emails[0] : null);
  const phone = opp.phone || opp.contact?.phone_from_site || (opp.contact?.phones?.length ? opp.contact.phones[0] : null);
  if (email) {
    const el = document.createElement("span");
    el.className = "tag";
    el.textContent = `Email: ${email}`;
    wrap.appendChild(el);
  }
  if (phone) {
    const el = document.createElement("span");
    el.className = "tag";
    el.textContent = `Phone: ${phone}`;
    wrap.appendChild(el);
  }
  return wrap;
}

function copyAndFeedback(btn, text, label) {
  navigator.clipboard.writeText(text).then(() => {
    const orig = btn.textContent;
    btn.textContent = "Copied";
    setTimeout(() => { btn.textContent = orig; }, 1200);
  });
}

function applyCaseUpdateToOpportunity(target, updated) {
  if (!target || !updated) return target;
  Object.assign(target, updated);
  if (!target.contact) target.contact = {};
  if (updated.contact) Object.assign(target.contact, updated.contact);
  if (!target.website_analysis) target.website_analysis = {};
  if (updated.website_analysis) Object.assign(target.website_analysis, updated.website_analysis);
  if (!target.email_draft) target.email_draft = {};
  if (updated.email_draft) Object.assign(target.email_draft, updated.email_draft);
  return target;
}

function openCaseDetail(opp) {
  const modal = document.getElementById("caseDetailModal");
  const content = document.getElementById("caseDetailContent");
  const titleEl = document.getElementById("caseDetailTitle");
  if (!modal || !content || !titleEl) return;

  titleEl.textContent = opp.name || "Opportunity";
  const s = (v) => (v ?? "");
  const m = (v) => (v != null && v !== "" ? String(v) : "Missing");
  const arr = (v) => (Array.isArray(v) ? v : v ? [v] : []);
  const issues = arr(opp.website_analysis?.issues);

  const emails = opp.contact?.emails?.length ? opp.contact.emails : (opp.contact?.email ? [opp.contact.email] : []);
  const phones = opp.contact?.phones?.length ? opp.contact.phones : (opp.phone || opp.contact?.phone_from_site ? [opp.phone || opp.contact.phone_from_site] : []);

  const email = opp.contact?.email || (opp.contact?.emails?.length ? opp.contact.emails[0] : null);
  const phone = opp.phone || opp.contact?.phone_from_site || (opp.contact?.phones?.length ? opp.contact.phones[0] : null);

  let html = `<div class="case-detail-section"><h4>Business Snapshot</h4><div class="case-field-list">`;
  html += `<div class="case-field"><span class="case-field-label">Name:</span> ${m(opp.name)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Category:</span> ${m(opp.category)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Address:</span> ${m(opp.address)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Distance:</span> ${opp.distance_miles != null ? opp.distance_miles + " mi" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Maps link:</span> ${opp.maps_url ? `<a href="${opp.maps_url}">${opp.maps_url}</a>` : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Website:</span> ${opp.website ? `<a href="${opp.website}">${opp.website}</a>` : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Rating:</span> ${m(opp.rating)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Review count:</span> ${m(opp.review_count)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Hours:</span> ${m(opp.hours)}</div>`;
  html += `</div></div>`;

  html += `<div class="case-detail-section"><h4>Contact Matrix</h4><div class="case-field-list">`;
  html += `<div class="case-field"><span class="case-field-label">Best contact:</span> ${m(opp.contact_matrix?.best_contact_method || opp.contact_matrix?.best_contact || opp.recommended_contact)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Backup contact:</span> ${m(opp.contact_matrix?.backup_contact_method || opp.contact_matrix?.backup_contact || opp.backup_contact_method)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Email:</span> ${email || "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Phone:</span> ${m(phone)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Contact page:</span> ${opp.contact?.contact_page ? `<a href="${opp.contact.contact_page}">${opp.contact.contact_page}</a>` : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Facebook:</span> ${opp.contact?.facebook ? `<a href="${opp.contact.facebook}">${opp.contact.facebook}</a>` : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Instagram:</span> ${opp.contact?.instagram ? `<a href="${opp.contact.instagram}">${opp.contact.instagram}</a>` : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">LinkedIn:</span> ${opp.contact?.linkedin ? `<a href="${opp.contact.linkedin}">${opp.contact.linkedin}</a>` : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Owner/founder/manager:</span> ${m(opp.owner_manager_name || opp.contact_matrix?.owner_name || (opp.owner_names && opp.owner_names[0]))}</div>`;
  const soc = opp.social_links || {};
  Object.entries(soc).filter(([k]) => !["facebook","instagram"].includes(k)).forEach(([k, v]) => {
    if (v) html += `<div class="case-field"><span class="case-field-label">${k}:</span> <a href="${v}">${v}</a></div>`;
  });
  html += `</div></div>`;

  html += `<div class="case-detail-section"><h4>Website Intelligence</h4><div class="case-field-list">`;
  html += `<div class="case-field"><span class="case-field-label">Platform:</span> ${m(opp.website_analysis?.platform)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Homepage title:</span> ${m(opp.homepage_title)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Meta description:</span> ${m(opp.meta_description)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Viewport/mobile:</span> ${opp.viewport_ok === true ? "OK" : opp.viewport_ok === false ? "Issues" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Tap-to-call:</span> ${opp.tap_to_call_present === true ? "Yes" : opp.tap_to_call_present === false ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Contact form:</span> ${opp.contact_form_present === true ? "Yes" : opp.contact_form_present === false ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Menu found:</span> ${opp.menu_found === true || opp.menu_visibility === true ? "Yes" : (opp.menu_found === false || opp.menu_visibility === false) ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Hours found:</span> ${opp.hours_found === true || opp.hours_visibility === true ? "Yes" : (opp.hours_found === false || opp.hours_visibility === false) ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Directions found:</span> ${opp.directions_found === true || opp.directions_visibility === true ? "Yes" : (opp.directions_found === false || opp.directions_visibility === false) ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Outdated design clues:</span> ${opp.outdated_design_clues === true ? "Yes" : opp.outdated_design_clues === false ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Text-heavy clues:</span> ${opp.text_heavy_clues === true ? "Yes" : opp.text_heavy_clues === false ? "No" : "Missing"}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Navigation items:</span> ${((opp.navigation_items || opp.page_navigation_items || []).length ? (opp.navigation_items || opp.page_navigation_items).join(", ") : "Missing")}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Important internal links:</span> ${(Object.keys(opp.important_internal_links || opp.internal_links_found || {}).length ? Object.entries(opp.important_internal_links || opp.internal_links_found).map(([k, v]) => `${k}: ${v}`).join(" | ") : "Missing")}</div>`;
  const disc = opp.discovered_pages || [];
  html += `<div class="case-field"><span class="case-field-label">Internal pages found:</span> ${disc.length ? disc.length + " pages" : "Missing"}</div>`;
  html += `</div><p><strong>Strongest problems:</strong></p><pre class="case-pre">${escapeHtml(issues.length ? issues.join("\n") : "None identified")}</pre></div>`;

  const reviewSnippets = Array.isArray(opp.review_snippets) ? opp.review_snippets : [];
  const reviewThemes = Array.isArray(opp.review_themes) ? opp.review_themes : [];
  html += `<div class="case-detail-section"><h4>Review Intelligence</h4><div class="case-field-list">`;
  html += `<div class="case-field"><span class="case-field-label">Rating:</span> ${m(opp.rating)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Review count:</span> ${m(opp.review_count)}</div>`;
  html += `<div class="case-field"><span class="case-field-label">Review themes:</span> ${reviewThemes.length ? reviewThemes.join(", ") : "Missing"}</div>`;
  html += `</div>`;
  html += `<p><strong>Review snippets:</strong></p><pre class="case-pre">${escapeHtml(reviewSnippets.length ? reviewSnippets.join("\n\n") : "Missing")}</pre></div>`;

  html += `<div class="case-detail-section"><h4>Pitch angle</h4><p>${s(opp.pitch_angle) || "Missing"}</p>`;
  if (opp.demo_to_show) html += `<p><strong>Demo:</strong> ${s(opp.demo_to_show)}</p>`;
  html += `</div>`;

  html += `<div class="case-detail-section"><h4>Outreach Pack</h4>`;
  const shortEmail = opp.email_draft?.body || opp.short_email || opp.longer_email;
  html += `<p><strong>Short email:</strong></p><pre class="case-pre case-outreach-pre">${escapeHtml(shortEmail || "Missing")}</pre>`;
  html += `<p><strong>Long email:</strong></p><pre class="case-pre case-outreach-pre">${escapeHtml(opp.longer_email || "Missing")}</pre>`;
  html += `<p><strong>Contact form message:</strong></p><pre class="case-pre case-outreach-pre">${escapeHtml(opp.contact_form_version || "Missing")}</pre>`;
  html += `<p><strong>Social DM:</strong></p><pre class="case-pre case-outreach-pre">${escapeHtml(opp.social_dm_version || "Missing")}</pre>`;
  html += `<p><strong>Follow-up:</strong></p><pre class="case-pre case-outreach-pre">${escapeHtml(opp.follow_up_note || opp.follow_up_line || "Missing")}</pre>`;
  html += `<div class="case-copy-buttons">`;
  if (shortEmail) html += `<button type="button" class="ghost-btn case-copy-btn" data-copy="short">Copy short</button>`;
  if (opp.longer_email) html += `<button type="button" class="ghost-btn case-copy-btn" data-copy="long">Copy long</button>`;
  if (opp.follow_up_note || opp.follow_up_line) html += `<button type="button" class="ghost-btn case-copy-btn" data-copy="followup">Copy follow-up</button>`;
  if (opp.contact_form_version) html += `<button type="button" class="ghost-btn case-copy-btn" data-copy="form">Copy form msg</button>`;
  if (opp.social_dm_version) html += `<button type="button" class="ghost-btn case-copy-btn" data-copy="social">Copy social DM</button>`;
  html += `<button type="button" class="primary-btn" id="caseRegenerateOutreachBtn">Regenerate Outreach</button>`;
  html += `</div></div>`;
  if (opp.why_worth_pursuing || opp.next_action) {
    html += `<div class="case-detail-section"><h4>${opp.no_website ? "Why this is a strong lead" : "Internal notes"}</h4>`;
    if (opp.why_worth_pursuing) html += `<p>${s(opp.why_worth_pursuing)}</p>`;
    if (opp.next_action) html += `<p><strong>Next:</strong> ${s(opp.next_action)}</p>`;
    if (opp.follow_up_suggestion) html += `<p><strong>Follow-up:</strong> ${s(opp.follow_up_suggestion)}</p>`;
    html += `</div>`;
  }

  html += `<div class="case-detail-section"><h4>Outreach queue</h4>`;
  html += `<p><strong>Status:</strong> ${s(opp.status) || "New"}</p>`;
  html += `<label class="modal-label">Outreach notes</label><textarea id="caseOutreachNotes" class="modal-textarea case-outreach-field">${s(opp.outreach_notes)}</textarea>`;
  html += `<label class="modal-label">Follow-up due (date)</label><input type="text" id="caseFollowUpDue" class="modal-input case-outreach-field" placeholder="e.g. 2025-03-15" value="${s(opp.follow_up_due)}" />`;
  html += `<label class="modal-label">Outcome</label><input type="text" id="caseOutcome" class="modal-input case-outreach-field" placeholder="e.g. No response, Meeting scheduled" value="${s(opp.outcome)}" />`;
  html += `<div class="case-outreach-quick">`;
  LEAD_STATUSES.forEach((st) => {
    html += `<button type="button" class="ghost-btn case-status-btn" data-status="${st}">${st}</button>`;
  });
  html += `</div>`;
  html += `<button type="button" id="caseOutreachSaveBtn" class="primary-btn">Save outreach</button>`;
  html += `<button type="button" id="caseViewRawBtn" class="ghost-btn">View raw case JSON</button>`;
  html += `</div>`;

  html += `<div class="case-detail-section case-raw-section"><button type="button" id="caseRawToggle" class="ghost-btn case-raw-toggle">Raw Case File ▾</button>`;
  html += `<pre id="caseRawPre" class="case-pre case-raw-pre hidden"></pre></div>`;

  content.innerHTML = html;

  document.getElementById("caseViewRawBtn").onclick = () => openRawCaseModal(opp.slug);
  document.getElementById("caseRawToggle").onclick = async () => {
    const pre = document.getElementById("caseRawPre");
    const btn = document.getElementById("caseRawToggle");
    if (pre.classList.contains("hidden")) {
      if (!pre.textContent) {
        try {
          const data = await fetchCase(opp.slug);
          pre.textContent = JSON.stringify(data, null, 2);
        } catch (e) {
          pre.textContent = "Error loading: " + (e.message || "Unknown");
        }
      }
      pre.classList.remove("hidden");
      btn.textContent = "Raw Case File ▴";
    } else {
      pre.classList.add("hidden");
      btn.textContent = "Raw Case File ▾";
    }
  };

  document.getElementById("caseOutreachSaveBtn").onclick = async () => {
    const notes = document.getElementById("caseOutreachNotes")?.value ?? "";
    const followUpDue = document.getElementById("caseFollowUpDue")?.value?.trim() || null;
    const outcome = document.getElementById("caseOutcome")?.value?.trim() || null;
    try {
      await updateCaseStatus(opp.slug, { outreach_notes: notes, follow_up_due: followUpDue, outcome });
      opp.outreach_notes = notes;
      opp.follow_up_due = followUpDue;
      opp.outcome = outcome;
      document.getElementById("caseOutreachSaveBtn").textContent = "Saved";
      setTimeout(() => { document.getElementById("caseOutreachSaveBtn").textContent = "Save outreach"; }, 1200);
    } catch (e) {
      console.error(e);
    }
  };
  content.querySelectorAll(".case-status-btn").forEach((btn) => {
    btn.onclick = async () => {
      const st = btn.getAttribute("data-status");
      try {
        await updateCaseStatus(opp.slug, { status: st });
        opp.status = st;
        await refreshScoutData();
        openCaseDetail(opp);
      } catch (e) {
        console.error(e);
      }
    };
  });

  content.querySelectorAll(".case-copy-btn").forEach((btn) => {
    btn.onclick = () => {
      const k = btn.getAttribute("data-copy");
      let text = "";
      if (k === "short") text = opp.email_draft?.body || opp.short_email || "";
      else if (k === "long") text = opp.longer_email || "";
      else if (k === "followup") text = opp.follow_up_note || opp.follow_up_line || "";
      else if (k === "form") text = opp.contact_form_version || "";
      else text = opp.social_dm_version || "";
      copyAndFeedback(btn, text, btn.textContent);
    };
  });
  const regenBtn = document.getElementById("caseRegenerateOutreachBtn");
  if (regenBtn) {
    regenBtn.onclick = async () => {
      const original = regenBtn.textContent;
      regenBtn.disabled = true;
      regenBtn.textContent = "Regenerating...";
      try {
        let updatedCase = null;
        if (window.MB_REGENERATE_OUTREACH) {
          updatedCase = await window.MB_REGENERATE_OUTREACH(opp.slug, opp);
        } else {
          const apiBase = (window.MB_API_BASE || "").replace(/\/$/, "");
          const res = await fetch(`${apiBase}/case/${encodeURIComponent(opp.slug)}/regenerate-outreach`, {
            method: "POST",
          });
          if (!res.ok) throw new Error("Could not regenerate outreach");
          const data = await res.json();
          updatedCase = data?.case || null;
        }
        if (updatedCase) {
          applyCaseUpdateToOpportunity(opp, updatedCase);
          await refreshScoutData({ reason: "outreach-regenerate", cacheBust: true });
          openCaseDetail(opp);
        }
      } catch (e) {
        console.error("outreach regeneration failed", e);
        regenBtn.disabled = false;
        regenBtn.textContent = "Regenerate failed";
        setTimeout(() => {
          regenBtn.disabled = false;
          regenBtn.textContent = original;
        }, 1500);
      }
    };
  }
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
}

function closeCaseDetail() {
  const modal = document.getElementById("caseDetailModal");
  if (modal) { modal.classList.add("hidden"); modal.setAttribute("aria-hidden", "true"); }
}

async function openRawCaseModal(slug) {
  const modal = document.getElementById("rawCaseModal");
  const content = document.getElementById("rawCaseContent");
  if (!modal || !content) return;
  content.textContent = "Loading…";
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  try {
    const data = await fetchCase(slug);
    content.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    content.textContent = "Error: " + (e.message || "Could not load case");
  }
}

function closeRawCaseModal() {
  const modal = document.getElementById("rawCaseModal");
  if (modal) { modal.classList.add("hidden"); modal.setAttribute("aria-hidden", "true"); }
}

function renderMorningRunner(today, opportunities, stdout, stderr) {
  _runnerLastPayload = { today, opportunities, stdout, stderr };
  const list = document.getElementById("morningRunnerList");
  const count = document.getElementById("morningRunnerCount");
  const summary = document.getElementById("morningRunnerSummary");
  const generated = document.getElementById("scoutGeneratedAt");
  const filterEl = document.getElementById("runnerStatusFilters");
  const sortEl = document.getElementById("runnerSortSelect");
  const todaySection = document.getElementById("runnerTodaySection");
  const todayList = document.getElementById("runnerTodayList");

  if (!list || !count || !summary || !generated) return;

  _runnerOpportunities = (today && today.top_opportunities) ? today.top_opportunities : (opportunities || []);
  const top = _runnerOpportunities;

  const filtered = _runnerStatusFilter === "all"
    ? top
    : top.filter(o => (o.status || "New") === _runnerStatusFilter);

  const sorted = [...filtered].sort((a, b) => {
    if (_runnerSort === "distance") {
      return (a.distance_miles ?? 9999) - (b.distance_miles ?? 9999);
    }
    if (_runnerSort === "name") {
      return String(a.name || "").localeCompare(String(b.name || ""));
    }
    if (_runnerSort === "status") {
      return String(a.status || "New").localeCompare(String(b.status || "New"));
    }
    const pScore = { high: 3, medium: 2, low: 1 };
    return (pScore[b.priority] || 0) - (pScore[a.priority] || 0);
  });

  const noWebsite = sorted.filter(o => o.no_website || o.lane === "no_website");
  const weakWebsite = sorted.filter(o => !o.no_website && o.lane !== "no_website");

  if (filterEl) {
    filterEl.innerHTML = "";
    ["All", ...LEAD_STATUSES].forEach((st) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ghost-btn runner-filter-btn" + (_runnerStatusFilter === (st === "All" ? "all" : st) ? " active" : "");
      btn.textContent = st;
      btn.onclick = () => {
        _runnerStatusFilter = st === "All" ? "all" : st;
        renderMorningRunner(today, opportunities, stdout, stderr);
      };
      filterEl.appendChild(btn);
    });
  }

  if (sortEl) {
    sortEl.value = _runnerSort;
    sortEl.onchange = () => {
      _runnerSort = sortEl.value || "priority";
      renderMorningRunner(today, opportunities, stdout, stderr);
    };
  }

  const todayLeads = top.filter(o => {
    const s = o.status || "New";
    return s === "Ready to contact" || s === "Follow up";
  });
  if (todaySection && todayList) {
    todaySection.classList.toggle("hidden", todayLeads.length === 0);
    todayList.innerHTML = "";
    todayLeads.forEach((o) => {
      const a = document.createElement("a");
      a.href = "#";
      a.className = "runner-today-item";
      a.textContent = `${o.name || "?"} — ${o.status}`;
      a.onclick = (e) => { e.preventDefault(); openCaseDetail(o); };
      todayList.appendChild(a);
    });
  }

  list.innerHTML = "";
  count.textContent = sorted.length;
  summary.textContent = today?.summary || "No scout summary available.";
  generated.textContent = today?.generated_at ? `Generated: ${today.generated_at}` : "No scout run yet";

  function renderOppCard(opp) {
    const card = document.createElement("div");
    card.className = "runner-opp";

    const topRow = document.createElement("div");
    topRow.className = "runner-opp-top";

    const left = document.createElement("div");
    const title = document.createElement("h4");
    title.textContent = `${opp.name} • ${opp.distance_miles != null ? opp.distance_miles : "?"} miles`;
    const desc = document.createElement("p");
    desc.textContent = `${opp.category} — ${opp.address || "No address found"}`;
    left.appendChild(title);
    left.appendChild(desc);

    const right = document.createElement("div");
    const statusBadge = document.createElement("span");
    statusBadge.className = "tag runner-status-badge status-" + (opp.status || "New").toLowerCase().replace(/\s+/g, "-");
    statusBadge.textContent = opp.status || "New";
    right.appendChild(statusBadge);
    if (opp.no_website) right.appendChild(createRunnerTag("No website"));
    right.appendChild(createRunnerTag(`Best: ${opp.recommended_contact || "unknown"}`));
    if (opp.backup_contact_method) right.appendChild(createRunnerTag(`Backup: ${opp.backup_contact_method}`));
    if (!opp.no_website && opp.website_analysis?.platform) right.appendChild(createRunnerTag(opp.website_analysis.platform));
    if (opp.rating != null) right.appendChild(createRunnerTag(`⭐ ${opp.rating} (${opp.review_count ?? "?"} reviews)`));
    if (opp.priority) right.appendChild(createRunnerTag(opp.priority));

    topRow.appendChild(left);
    topRow.appendChild(right);
    card.appendChild(topRow);

    card.appendChild(buildCardDataSection(opp));
    card.appendChild(buildContactMatrixSection(opp));
    card.appendChild(buildContactLinks(opp));

    if (opp.best_service_to_offer || opp.demo_to_show) {
      const recRow = document.createElement("div");
      recRow.className = "runner-recommendations";
      if (opp.best_service_to_offer) {
        const s = document.createElement("span");
        s.className = "tag";
        s.textContent = `Offer: ${opp.best_service_to_offer}`;
        recRow.appendChild(s);
      }
      if (opp.demo_to_show) {
        const d = document.createElement("span");
        d.className = "tag";
        d.textContent = `Demo: ${opp.demo_to_show}`;
        recRow.appendChild(d);
      }
      card.appendChild(recRow);
    }

    const issuesTitle = document.createElement("div");
    issuesTitle.className = "runner-section-title";
    issuesTitle.textContent = opp.no_website ? "Opportunity" : "Issues found";
    card.appendChild(issuesTitle);
    const issuesBox = document.createElement("div");
    issuesBox.className = "runner-pre";
    const issues = opp.no_website ? ["No website — strong opportunity for first-site build"] : (opp.website_analysis?.issues || []);
    issuesBox.textContent = issues.length ? issues.join("\n") : "No strong issues detected.";
    card.appendChild(issuesBox);

    const pitchTitle = document.createElement("div");
    pitchTitle.className = "runner-section-title";
    pitchTitle.textContent = "Pitch angle";
    card.appendChild(pitchTitle);
    const pitchBox = document.createElement("div");
    pitchBox.className = "runner-pre";
    pitchBox.textContent = opp.pitch_angle || "No pitch angle generated.";
    card.appendChild(pitchBox);

    const emailTitle = document.createElement("div");
    emailTitle.className = "runner-section-title";
    emailTitle.textContent = "Ready email draft";
    card.appendChild(emailTitle);
    const emailBox = document.createElement("div");
    emailBox.className = "runner-pre";
    emailBox.textContent = `Subject: ${opp.email_draft?.subject || ""}\n\n${opp.email_draft?.body || ""}`;
    card.appendChild(emailBox);

    const statusRow = document.createElement("div");
    statusRow.className = "runner-status-row";
    statusRow.appendChild(document.createTextNode("Status: "));
    const statusSelect = document.createElement("select");
    statusSelect.className = "runner-status-select";
    LEAD_STATUSES.forEach((st) => {
      const opt = document.createElement("option");
      opt.value = st;
      opt.textContent = st;
      if ((opp.status || "New") === st) opt.selected = true;
      statusSelect.appendChild(opt);
    });
    statusSelect.onchange = async () => {
      const newStatus = statusSelect.value;
      try {
        await updateCaseStatus(opp.slug, { status: newStatus });
        await refreshScoutData();
      } catch (e) {
        console.error(e);
        statusSelect.value = opp.status || "New";
      }
    };
    statusRow.appendChild(statusSelect);
    const quickActions = document.createElement("span");
    quickActions.className = "runner-quick-actions";
    ["Ready to contact", "Contacted", "Follow up", "Closed", "Skip"].forEach((st) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ghost-btn runner-quick-btn";
      btn.textContent = st === "Ready to contact" ? "Ready" : st === "Follow up" ? "Follow up" : st;
      btn.onclick = async () => {
        try {
          await updateCaseStatus(opp.slug, { status: st });
          opp.status = st;
          statusBadge.textContent = st;
          statusBadge.className = "tag runner-status-badge status-" + st.toLowerCase().replace(/\s+/g, "-");
          statusSelect.value = st;
          await refreshScoutData();
        } catch (e) {
          console.error(e);
        }
      };
      quickActions.appendChild(btn);
    });
    statusRow.appendChild(quickActions);
    card.appendChild(statusRow);

    const copyRow = document.createElement("div");
    copyRow.className = "runner-copy-row";
    const copyBtns = [
      ["Copy short", opp.email_draft?.body || opp.short_email || opp.longer_email, `Subject: ${opp.email_draft?.subject || ""}\n\n`],
      ["Copy long", opp.longer_email, ""],
      ["Copy follow-up", opp.follow_up_note || opp.follow_up_line, ""],
      ["Copy form msg", opp.contact_form_version, ""],
      ["Copy social DM", opp.social_dm_version, ""],
    ];
    copyBtns.forEach(([label, text, prefix]) => {
      if (!text) return;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ghost-btn runner-copy-btn";
      btn.textContent = label;
      btn.onclick = () => {
        copyAndFeedback(btn, prefix ? prefix + text : text, label);
      };
      copyRow.appendChild(btn);
    });
    const viewBtn = document.createElement("button");
    viewBtn.type = "button";
    viewBtn.className = "primary-btn";
    viewBtn.textContent = "View full research";
    viewBtn.onclick = () => openCaseDetail(opp);
    copyRow.appendChild(viewBtn);
    const rawBtn = document.createElement("button");
    rawBtn.type = "button";
    rawBtn.className = "ghost-btn";
    rawBtn.textContent = "View raw case JSON";
    rawBtn.onclick = () => openRawCaseModal(opp.slug);
    copyRow.appendChild(rawBtn);
    const saveBtn = document.createElement("button");
    saveBtn.type = "button";
    saveBtn.className = "ghost-btn";
    saveBtn.textContent = "Save to Opportunities";
    saveBtn.onclick = () => {
      const body = opp.email_draft?.body || opp.short_email || opp.longer_email || "";
      state.opportunities.unshift({
        title: opp.name,
        description: `${opp.category} — ${opp.address || ""}`,
        pitch: opp.pitch_angle || "",
        build: (opp.website_analysis?.facts || []).join("\n"),
        notes: body
      });
      saveState();
      renderAll();
      saveBtn.textContent = "Saved";
      setTimeout(() => { saveBtn.textContent = "Save to Opportunities"; }, 1200);
    };
    copyRow.appendChild(saveBtn);
    card.appendChild(copyRow);
    return card;
  }

  if (noWebsite.length > 0) {
    const sectionTitle = document.createElement("h4");
    sectionTitle.className = "runner-lane-title";
    sectionTitle.textContent = `No Website Opportunities (${noWebsite.length})`;
    list.appendChild(sectionTitle);
    noWebsite.forEach(opp => list.appendChild(renderOppCard(opp)));
  }
  if (weakWebsite.length > 0) {
    const sectionTitle2 = document.createElement("h4");
    sectionTitle2.className = "runner-lane-title";
    sectionTitle2.textContent = `Website Redesign Opportunities (${weakWebsite.length})`;
    list.appendChild(sectionTitle2);
    weakWebsite.forEach(opp => list.appendChild(renderOppCard(opp)));
  }
  if (noWebsite.length === 0 && weakWebsite.length === 0) {
    const emptyMsg = document.createElement("p");
    emptyMsg.className = "runner-empty";
    emptyMsg.textContent = "No opportunities yet. Run Scout to find local businesses.";
    list.appendChild(emptyMsg);
  }

  if (stdout || stderr) {
    const log = document.createElement("div");
    log.className = "scout-log";
    log.textContent = [stdout, stderr].filter(Boolean).join("\n");
    list.appendChild(log);
  }
}

function updateAdminSummary(today, opportunities) {
  const total = opportunities?.length || 0;
  const todayLeads = today?.top_opportunities?.length || total;
  const followUps = (opportunities || []).filter((o) => {
    const status = o.status || "New";
    return status === "Follow up" || !!o.follow_up_due;
  }).length;
  const topNames = (opportunities || []).slice(0, 3).map((o) => o.name).filter(Boolean);

  const setText = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  };
  setText("adm-total-leads", String(total));
  setText("adm-today-leads", String(todayLeads));
  setText("adm-followups-due", String(followUps));
  setText("adm-top-opps", topNames.length ? topNames.join(", ") : "None");

  renderDashboardCommandCenter(today, opportunities || []);
}

function parseDateAsLocal(value) {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function isOverdueFollowUp(opp) {
  const due = parseDateAsLocal(opp.follow_up_due);
  if (!due) return false;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return due < today;
}

function getLeadScore(opp) {
  const raw = opp.internal_score ?? opp.score ?? 0;
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

function hasReachableContact(opp) {
  const email = opp.contact?.email || (opp.contact?.emails && opp.contact.emails[0]);
  const phone = opp.phone || opp.contact?.phone_from_site || (opp.contact?.phones && opp.contact.phones[0]);
  const reachableMethod = opp.recommended_contact || opp.backup_contact_method || email || phone || opp.contact?.contact_page;
  return !!reachableMethod;
}

function statusPriority(status) {
  if (status === "Ready to contact") return 4;
  if (status === "Follow up") return 3;
  if (status === "New") return 2;
  if (status === "Contacted") return 1;
  return 0;
}

function isIgnoredTopContactStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  return normalized === "closed" || normalized === "not interested" || normalized === "do not contact" || normalized === "skip";
}

function rankTopOpportunity(opp) {
  let rank = 0;
  const isNoWebsite = opp.no_website || opp.lane === "no_website";
  const issues = opp.website_analysis?.issues || opp.strongest_problems || [];
  const reviewCount = Number(opp.review_count || 0) || 0;
  const rating = Number(opp.rating || 0) || 0;
  const distance = Number(opp.distance_miles ?? 9999);
  const contacted = (opp.status || "New") === "Contacted";

  if (isNoWebsite) rank += 600;
  else if (opp.lane === "weak_website") rank += 420;
  if (issues.length) rank += 140;
  if (hasReachableContact(opp)) rank += 120;
  if (!contacted) rank += 110;
  rank += Math.max(0, getLeadScore(opp)) * 10;
  rank += Math.min(reviewCount, 200) * 0.4;
  rank += rating * 3;
  rank -= Math.min(Number.isFinite(distance) ? distance : 9999, 200) * 1.5;
  rank += statusPriority(opp.status || "New") * 25;
  return rank;
}

function inferRunLocation(today) {
  const explicit = today?.location_used || today?.search_location || today?.location || today?.city;
  if (explicit) return String(explicit);
  const summary = String(today?.summary || "");
  const m = summary.match(/using\s+([^.,;]+)/i);
  if (m && m[1]) return m[1].trim();
  if (summary.toLowerCase().includes("current location")) return "Current location";
  return "Saved home location";
}

function summarizeRunMetrics(today, opportunities) {
  const list = opportunities || [];
  const processed = Number(today?.processed_count ?? today?.processed ?? list.length);
  const saved = Number(today?.saved_count ?? today?.saved ?? list.length);
  const skipped = Number(today?.skipped_count ?? today?.skipped ?? Math.max(0, processed - saved));
  const noWebsite = list.filter((o) => o.no_website || o.lane === "no_website").length;
  const weakWebsite = Math.max(0, list.length - noWebsite);
  const location = inferRunLocation(today);
  const runTime = today?.generated_at || today?.timestamp || today?.generatedAt || null;
  return {
    processed: Number.isFinite(processed) ? processed : list.length,
    saved: Number.isFinite(saved) ? saved : list.length,
    skipped: Number.isFinite(skipped) ? skipped : 0,
    noWebsite,
    weakWebsite,
    location,
    runTime,
  };
}

function renderDashboardSimpleList(containerId, items, emptyLabel) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = "";
  if (!items.length) {
    const row = document.createElement("div");
    row.className = "runner-today-item";
    row.textContent = emptyLabel;
    el.appendChild(row);
    return;
  }
  items.slice(0, 5).forEach((opp) => {
    const row = document.createElement("a");
    row.href = "#";
    row.className = "runner-today-item";
    row.textContent = `${opp.name || "Unknown"} — ${opp.status || "New"}`;
    row.onclick = (e) => {
      e.preventDefault();
      openCaseDetail(opp);
    };
    el.appendChild(row);
  });
}

function renderDashboardCommandCenter(today, opportunities) {
  console.log("computing top opportunities");
  const active = opportunities.filter((o) => !["Closed", "Skip"].includes(o.status || ""));
  const ready = opportunities.filter((o) => (o.status || "New") === "Ready to contact");
  const followUp = opportunities.filter((o) => (o.status || "New") === "Follow up");
  const overdue = opportunities.filter((o) => isOverdueFollowUp(o));
  const dueCount = opportunities.filter((o) => (o.status || "New") === "Follow up" || isOverdueFollowUp(o)).length;
  const leadsToday = today?.top_opportunities?.length || opportunities.length;
  const topRanked = opportunities
    .filter((o) => !isIgnoredTopContactStatus(o.status))
    .sort((a, b) => rankTopOpportunity(b) - rankTopOpportunity(a))
    .slice(0, 5);
  if (topRanked.length) {
    console.log("top opportunities loaded", { count: topRanked.length });
  } else {
    console.log("top opportunities empty");
  }

  const setText = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
  };

  setText("kpiLeadsToday", String(leadsToday));
  setText("kpiReadyToContact", String(ready.length));
  setText("kpiFollowUpsDue", String(dueCount));
  setText("kpiActiveCases", String(active.length));
  setText("adminCommandCenterCount", String(opportunities.length));

  renderDashboardSimpleList("dashboardReadyList", ready, "No leads ready to contact.");
  renderDashboardSimpleList("dashboardFollowUpList", followUp, "No follow-ups queued.");
  renderDashboardSimpleList("dashboardOverdueList", overdue, "No overdue follow-ups.");

  const topEl = document.getElementById("dashboardTopOpps");
  if (topEl) {
    topEl.innerHTML = "";
    if (!topRanked.length) {
      const empty = document.createElement("div");
      empty.className = "runner-today-item";
      empty.textContent = "No opportunities yet.";
      topEl.appendChild(empty);
    } else {
      topRanked.forEach((opp) => {
        const row = document.createElement("div");
        row.className = "dashboard-top-opp";
        const noWebsite = opp.no_website || opp.lane === "no_website" ? "No Website" : "Weak Website";
        const score = getLeadScore(opp);
        const contact = opp.recommended_contact || opp.backup_contact_method || "Contact unclear";
        const meta = `${opp.category || "Unknown category"} • ${opp.distance_miles ?? "?"} mi • ${noWebsite} • Score ${score}`;
        row.innerHTML = `
          <div class="dashboard-top-opp-head">
            <strong>${escapeHtml(opp.name || "Unknown")}</strong>
            <span class="tag">${escapeHtml(opp.status || "New")}</span>
          </div>
          <div class="dashboard-top-opp-meta">${escapeHtml(meta)}</div>
          <div class="dashboard-top-opp-meta">Recommended contact: ${escapeHtml(contact)}</div>
        `;
        const openBtn = document.createElement("button");
        openBtn.type = "button";
        openBtn.className = "ghost-btn";
        openBtn.textContent = "Open case";
        openBtn.onclick = (e) => {
          e.preventDefault();
          openCaseDetail(opp);
        };
        row.appendChild(openBtn);
        const copyBtn = document.createElement("button");
        copyBtn.type = "button";
        copyBtn.className = "ghost-btn";
        copyBtn.textContent = "Copy Outreach Message";
        copyBtn.onclick = () => {
          const text = opp.email_draft?.body || opp.short_email || opp.longer_email || "";
          if (!text) {
            copyBtn.textContent = "No outreach";
            setTimeout(() => {
              copyBtn.textContent = "Copy Outreach Message";
            }, 1200);
            return;
          }
          copyAndFeedback(copyBtn, text, "Copy Outreach Message");
        };
        row.appendChild(copyBtn);
        topEl.appendChild(row);
      });
    }
  }

  const metrics = summarizeRunMetrics(today, opportunities);
  setText("dashboardRunSummary", today?.summary || "No scout run yet.");
  setText("dashboardRunLocation", metrics.location);
  setText("dashboardRunTime", metrics.runTime || "No run yet");
  setText("dashboardRunProcessed", String(metrics.processed));
  setText("dashboardRunSaved", String(metrics.saved));
  setText("dashboardRunSkipped", String(metrics.skipped));
  setText("dashboardRunNoWebsite", String(metrics.noWebsite));
  setText("dashboardRunWeakWebsite", String(metrics.weakWebsite));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function refreshScoutData(options = {}) {
  const summary = document.getElementById("morningRunnerSummary");
  const retries = Math.max(1, Number(options.retries || 1));
  const retryDelayMs = Math.max(0, Number(options.retryDelayMs || 0));
  const minExpectedCount = Number(options.minExpectedCount || 0);
  const reason = options.reason || "manual";
  const cacheBust = !!options.cacheBust;
  try {
    let data = null;
    for (let attempt = 1; attempt <= retries; attempt += 1) {
      data = await fetchScoutData({ cacheBust });
      console.log("leads fetch complete", { reason, attempt, count: data?.opportunities?.length || 0 });
      if ((data?.opportunities?.length || 0) >= minExpectedCount || attempt === retries) break;
      if (retryDelayMs > 0) await sleep(retryDelayMs);
    }
    console.log("dashboard data loaded");
    renderMorningRunner(data.today, data.opportunities);
    updateAdminSummary(data.today, data.opportunities);
    await loadEmailAlertSettings();
    console.log("dashboard data refreshed");
    return data;
  } catch (err) {
    console.error(err);
    if (summary) summary.textContent = "Could not load scout data. Check that the backend API is reachable.";
    updateAdminSummary(null, []);
    return null;
  }
}
window.refreshScoutData = refreshScoutData;
window.runScoutNow = runScoutNow;

window.addEventListener("error", (e) => {
  console.error("Global JS error:", e.error || e.message);
});

function navigateAdminRoute(href) {
  if (!href) return;
  const current = (window.location.pathname || "").replace(/\/$/, "");
  const target = href.replace(/\/$/, "");
  if (current !== target) {
    history.pushState({}, "", href);
  }
  window.dispatchEvent(new PopStateEvent("popstate"));
  applyAdminRouteView();
}

function bindButtons() {
  console.log("frontend loaded - buttons binding started");

  const exportBtn = document.getElementById("exportBrain");
  if (exportBtn) exportBtn.addEventListener("click", exportBrain);

  const importInput = document.getElementById("importBrain");
  if (importInput) {
    importInput.addEventListener("change", (e) => {
      importBrain(e.target.files[0]);
      e.target.value = "";
    });
  }

  const modalBackdrop = document.getElementById("modalBackdrop");
  const closeModalBtn = document.getElementById("closeModalBtn");
  if (modalBackdrop) modalBackdrop.addEventListener("click", closeModal);
  if (closeModalBtn) closeModalBtn.addEventListener("click", closeModal);

  const caseDetailBackdrop = document.getElementById("caseDetailBackdrop");
  const closeCaseDetailBtn = document.getElementById("closeCaseDetailBtn");
  if (caseDetailBackdrop) caseDetailBackdrop.addEventListener("click", closeCaseDetail);
  if (closeCaseDetailBtn) closeCaseDetailBtn.addEventListener("click", closeCaseDetail);

  const rawCaseBackdrop = document.getElementById("rawCaseBackdrop");
  const closeRawCaseBtn = document.getElementById("closeRawCaseBtn");
  if (rawCaseBackdrop) rawCaseBackdrop.addEventListener("click", closeRawCaseModal);
  if (closeRawCaseBtn) closeRawCaseBtn.addEventListener("click", closeRawCaseModal);

  document.querySelectorAll(".tabs button").forEach((btn) => {
    btn.addEventListener("click", () => showTab(btn.getAttribute("data-tab")));
  });

  document.querySelectorAll(".main-nav-btn[data-main-tab]").forEach((btn) => {
    btn.addEventListener("click", () => showMainTab(btn.getAttribute("data-main-tab")));
  });

  document.querySelectorAll(".admin-module-link").forEach((link) => {
    link.addEventListener("click", (e) => {
      e.preventDefault();
      const href = link.getAttribute("href");
      if (link.getAttribute("data-admin-module") === "dashboard") {
        console.log("dashboard nav clicked");
      }
      navigateAdminRoute(href);
    });
  });

  const quickRun = document.getElementById("dashboardQuickRunScout");
  if (quickRun) {
    quickRun.addEventListener("click", () => {
      navigateAdminRoute("/admin/scout");
      runScoutNow();
    });
  }
  const quickLeads = document.getElementById("dashboardQuickLeads");
  if (quickLeads) quickLeads.addEventListener("click", () => navigateAdminRoute("/admin/leads"));
  const quickOutreach = document.getElementById("dashboardQuickOutreach");
  if (quickOutreach) quickOutreach.addEventListener("click", () => navigateAdminRoute("/admin/outreach"));
  const saveEmailAlertsBtn = document.getElementById("saveEmailAlertsBtn");
  if (saveEmailAlertsBtn) saveEmailAlertsBtn.addEventListener("click", saveEmailAlertSettings);

  const runScoutBtn = document.getElementById("runScoutBtn");
  console.log("runScoutBtn element:", runScoutBtn);
  if (runScoutBtn) {
    runScoutBtn.addEventListener("click", () => {
      console.log("Run Scout clicked (Scout tab)");
      runScout();
    });
  }

  const runMorningScoutBtn = document.getElementById("runMorningScoutBtn");
  console.log("runMorningScoutBtn element:", runMorningScoutBtn);
  if (runMorningScoutBtn) {
    runMorningScoutBtn.addEventListener("click", () => {
      console.log("Run Scout clicked");
      runScoutNow();
    });
    console.log("Run Scout handler attached");
  }

  const refreshScoutBtn = document.getElementById("refreshScoutBtn");
  console.log("refreshScoutBtn element:", refreshScoutBtn);
  if (refreshScoutBtn) {
    refreshScoutBtn.addEventListener("click", () => {
      console.log("Refresh clicked");
      refreshScoutData();
    });
  }

  const runAnalyzeBtn = document.getElementById("runAnalyze");
  console.log("runAnalyze element:", runAnalyzeBtn);
  if (runAnalyzeBtn) {
    runAnalyzeBtn.addEventListener("click", () => {
      console.log("Analyze clicked");
      runAnalyzeDraft();
    });
  }

  const copyEmailBtn = document.getElementById("copyEmailBtn");
  console.log("copyEmailBtn element:", copyEmailBtn);
  if (copyEmailBtn) {
    copyEmailBtn.addEventListener("click", () => {
      console.log("Copy Email clicked");
      copyEmailDraft();
    });
  }

  const saveOpportunityBtn = document.getElementById("saveOpportunityBtn");
  console.log("saveOpportunityBtn element:", saveOpportunityBtn);
  if (saveOpportunityBtn) {
    saveOpportunityBtn.addEventListener("click", saveAnalyzeAsOpportunity);
  }

  const saveIdeaBtn = document.getElementById("saveIdeaBtn");
  console.log("saveIdeaBtn element:", saveIdeaBtn);
  if (saveIdeaBtn) {
    saveIdeaBtn.addEventListener("click", saveAnalyzeAsIdea);
  }

  document.querySelectorAll(".pitch-buttons button").forEach((btn) => {
    btn.addEventListener("click", () => loadPitch(btn.getAttribute("data-pitch")));
  });

  document.querySelectorAll(".add-item-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const bucket = btn.getAttribute("data-bucket");
      state[bucket].push({
        title: "",
        description: "",
        pitch: "",
        build: "",
        notes: ""
      });
      saveState();
      renderAll();
      openModal(bucket, state[bucket].length - 1);
    });
  });

  renderAll();
  applyAdminRouteView();
  refreshScoutData();
  console.log("app init complete");
}

window.MB_APPLY_ADMIN_ROUTE_VIEW = applyAdminRouteView;

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bindButtons);
} else {
  bindButtons();
}
console.log("app.js finished loading");
