/**
 * Supabase data layer for opportunities and case files.
 * Uses src/lib loaders; maps DB rows to app shape (case_to_ui compatible).
 */
import { supabase } from "../src/lib/supabaseClient.js";
import { loadOpportunities } from "../src/lib/opportunities.js";
import { loadCaseFile, saveCaseFile } from "../src/lib/caseFiles.js";
import {
  isMissingWorkspaceSchemaError,
  resolveWorkspaceContext,
  withWorkspaceId,
} from "../src/lib/workspace.js";

function asList(v) {
  if (!v) return [];
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  const text = String(v).trim();
  return text ? [text] : [];
}

function first(...vals) {
  for (const v of vals) {
    if (v === null || v === undefined) continue;
    const text = String(v).trim();
    if (text) return text;
  }
  return null;
}

function formatHours(h) {
  if (h == null) return null;
  if (typeof h === "string") return h;
  if (Array.isArray(h)) return h.join(" | ");
  if (typeof h === "object" && h.weekdayDescriptions) return h.weekdayDescriptions.join(" | ");
  return String(h);
}

function oppToUI(row) {
  return {
    slug: row.id,
    id: row.id,
    name: row.business_name,
    distance_miles: row.distance_miles,
    category: row.category,
    address: row.address,
    phone: row.phone,
    website: row.website,
    maps_url: row.maps_link,
    recommended_contact: row.recommended_contact_method,
    website_analysis: { platform: null, issues: [], facts: [] },
    score: row.internal_score,
    pitch_angle: row.strongest_pitch_angle,
    email_draft: { subject: "", body: row.strongest_pitch_angle || "" },
    contact: { email: null, emails: [], phones: [], contact_page: null, phone_from_site: null, facebook: null, instagram: null, linkedin: null },
    hours: formatHours(row.hours),
    rating: row.rating,
    review_count: row.review_count,
    review_snippets: [],
    review_themes: [],
    owner_manager_name: null,
    lane: row.lane || "weak_website",
    no_website: row.no_website || false,
    backup_contact_method: row.backup_contact_method,
    best_service_to_offer: row.best_service_to_offer,
    demo_to_show: row.demo_to_show,
    priority: row.priority,
    status: row.status || "New",
  };
}

function mergeCaseIntoUI(ui, cf) {
  if (!cf) return ui;
  const problems = Array.isArray(cf.strongest_problems) ? cf.strongest_problems : [];
  return {
    ...ui,
    website_analysis: {
      platform: cf.platform_used,
      issues: problems,
      facts: cf.homepage_title ? [`Title: ${cf.homepage_title.slice(0, 100)}`] : [],
    },
    email_draft: {
      subject: `Quick idea for ${ui.name || "your business"}'s website`,
      body: cf.short_email || cf.longer_email || "",
    },
    contact: {
      email: cf.email,
      emails: [],
      phones: [],
      contact_page: cf.contact_page,
      phone_from_site: cf.phone_from_site,
      facebook: cf.facebook,
      instagram: cf.instagram,
      linkedin: cf.linkedin,
    },
    homepage_title: cf.homepage_title,
    meta_description: cf.meta_description,
    viewport_ok: cf.viewport_ok,
    tap_to_call_present: cf.tap_to_call_present,
    menu_found: cf.menu_found,
    hours_found: cf.hours_found,
    directions_found: cf.directions_found,
    menu_visibility: cf.menu_visibility,
    hours_visibility: cf.hours_visibility,
    directions_visibility: cf.directions_visibility,
    contact_form_present: cf.contact_form_present,
    navigation_items: cf.navigation_items || [],
    important_internal_links: cf.important_internal_links || {},
    outdated_design_clues: cf.outdated_design_clues,
    text_heavy_clues: cf.text_heavy_clues,
    best_service_to_offer: cf.best_service_to_offer ?? ui.best_service_to_offer,
    demo_to_show: cf.demo_to_show ?? ui.demo_to_show,
    longer_email: cf.longer_email,
    contact_form_version: cf.contact_form_version,
    social_dm_version: cf.social_dm_version,
    follow_up_note: cf.follow_up_note,
    follow_up_line: cf.follow_up_line,
    why_this_lead_is_worth_pursuing: cf.why_this_lead_is_worth_pursuing,
    review_snippets: cf.review_snippets || [],
    review_themes: cf.review_themes || [],
    outreach_notes: cf.outreach_notes,
    follow_up_due: cf.follow_up_due,
    outcome: cf.outcome,
    status: cf.status || "New",
  };
}

function generateOutreachPackFromOpportunity(opp) {
  console.log("generating outreach pack");
  const businessName = first(opp?.name, "your business");
  const lane = (first(opp?.lane) || (opp?.no_website ? "no_website" : "weak_website")).toLowerCase();
  const ownerName = first(opp?.owner_manager_name, (opp?.owner_names || [])[0]);
  const category = first(opp?.category);
  const strongestPitch = first(opp?.pitch_angle, opp?.strongest_pitch_angle);
  const bestService = first(opp?.best_service_to_offer);
  const demo = first(opp?.best_demo_to_show, opp?.demo_to_show);
  const recommendedContact = first(opp?.recommended_contact, opp?.backup_contact_method);
  const strongestProblems = asList(opp?.website_analysis?.issues || opp?.strongest_problems);
  const reviewThemes = asList(opp?.review_themes);

  const greeting = ownerName ? `Hi ${ownerName},` : "Hi there,";
  const categoryLine = category ? ` (${category})` : "";
  const observation = lane === "no_website"
    ? `I noticed ${businessName}${categoryLine} does not seem to have a website yet.`
    : (strongestProblems[0] || `I noticed a few easy website improvements for ${businessName}${categoryLine}.`);
  const valueAngle = strongestPitch || (
    lane === "no_website"
      ? "A simple, professional site can help local customers find your hours and contact info."
      : "A cleaner mobile experience and clearer calls to action can make outreach and bookings easier."
  );
  const proof = [];
  if (strongestProblems.length) proof.push(...strongestProblems.slice(0, 2));
  if (reviewThemes.length) proof.push(`Customers often mention: ${reviewThemes.slice(0, 2).join(", ")}.`);
  if (!proof.length && opp?.address) proof.push(`I was reviewing businesses around ${opp.address}.`);

  const offerLine = bestService || "I can share one quick, practical improvement idea.";
  const demoLine = demo ? `I can also show a quick demo: ${demo}.` : "";
  const contactLine = recommendedContact
    ? `Best way to reach you seems to be ${recommendedContact}.`
    : "Happy to use whichever contact method works best for your team.";

  const shortEmail = [
    greeting,
    "",
    observation,
    valueAngle,
    "",
    "If helpful, I can send one quick idea you can review in a few minutes.",
    "",
    "Thanks,",
    "Topher",
    "topher@mixedmakershop.com",
  ].join("\n");

  const longerEmail = [
    greeting,
    "",
    `I took a quick look at ${businessName}${categoryLine}.`,
    observation,
    valueAngle,
    "",
    ...(proof.length ? ["What stood out:", ...proof.map((p) => `- ${p}`), ""] : []),
    `Offer: ${offerLine}`,
    demoLine,
    contactLine,
    "",
    "If you're open to it, I can send a short walkthrough and keep it low-pressure.",
    "",
    "Thanks,",
    "Topher",
    "topher@mixedmakershop.com",
  ].filter(Boolean).join("\n");

  const contactFormVersion = `${observation} ${valueAngle} Offer: ${offerLine}. If helpful, I can send one quick idea today. Topher — topher@mixedmakershop.com`;
  const socialDmVersion = `Hey — quick note about ${businessName}. ${observation} ${valueAngle} Happy to send one simple idea if useful.`;
  const followUpNote = `Quick follow-up on my note about ${businessName}. If you'd like, I can send that one-page idea and keep it brief.`;

  const missing = [];
  if (!ownerName) missing.push("owner_manager_name");
  if (!strongestProblems.length) missing.push("strongest_problems");
  if (!strongestPitch) missing.push("strongest_pitch_angle");
  if (!bestService) missing.push("best_service_to_offer");
  if (missing.length) console.log("missing dossier fields handled", missing);

  console.log("outreach pack generated");
  return {
    short_email: shortEmail,
    longer_email: longerEmail,
    contact_form_version: contactFormVersion,
    social_dm_version: socialDmVersion,
    follow_up_note: followUpNote,
    follow_up_line: followUpNote,
  };
}

export async function fetchScoutDataFromSupabase() {
  if (!supabase) return null;
  let workspaceCtx = null;
  try {
    workspaceCtx = await resolveWorkspaceContext();
  } catch (err) {
    console.warn("workspace context fallback for cloud scout data:", err?.message || err);
  }

  const opps = await loadOpportunities();
  const opportunities = [];
  for (const o of opps || []) {
    let ui = oppToUI(o);
    const cf = await loadCaseFile(o.id);
    ui = mergeCaseIntoUI(ui, cf);
    opportunities.push(ui);
  }

  return {
    today: {
      generated_at: null,
      summary: `From cloud: ${opportunities.length} opportunities`,
      top_opportunities: opportunities,
      case_slugs: opps?.map((o) => o.id) || [],
      workspace_id: workspaceCtx?.workspaceId || null,
      workspace_name: workspaceCtx?.workspaceName || null,
    },
    opportunities,
  };
}

export async function getCaseFromSupabase(slugOrId) {
  if (!supabase) return null;

  const opps = await loadOpportunities();
  const opp = opps?.find((o) => o.id === slugOrId);
  if (!opp) return null;

  let ui = oppToUI(opp);
  const cf = await loadCaseFile(opp.id);
  return mergeCaseIntoUI(ui, cf);
}

export async function updateCaseInSupabase(slugOrId, updates) {
  if (!supabase) throw new Error("Not authenticated");

  const opps = await loadOpportunities();
  const opp = opps?.find((o) => o.id === slugOrId);
  if (!opp) throw new Error("Case not found");

  const { error: e1 } = await supabase
    .from("opportunities")
    .update({
      status: updates.status ?? undefined,
      updated_at: new Date().toISOString(),
    })
    .eq("id", opp.id);

  if (e1) throw e1;

  const caseUpdates = {
    outreach_notes: updates.outreach_notes,
    follow_up_due: updates.follow_up_due,
    outcome: updates.outcome,
    status: updates.status,
    updated_at: new Date().toISOString(),
  };
  Object.keys(caseUpdates).forEach((k) => caseUpdates[k] === undefined && delete caseUpdates[k]);

  await saveCaseFile(opp.id, caseUpdates);
}

export async function regenerateOutreachForCaseInSupabase(slugOrId, existingOpp = null) {
  if (!supabase) throw new Error("Not authenticated");
  const opps = await loadOpportunities();
  const oppRow = opps?.find((o) => o.id === slugOrId);
  if (!oppRow) throw new Error("Case not found");

  let ui = existingOpp || oppToUI(oppRow);
  if (!existingOpp) {
    const cf = await loadCaseFile(oppRow.id);
    ui = mergeCaseIntoUI(ui, cf);
  }

  const pack = generateOutreachPackFromOpportunity(ui);
  try {
    await saveCaseFile(oppRow.id, { ...pack, updated_at: new Date().toISOString() });
  } catch (err) {
    const msg = String(err?.message || err || "").toLowerCase();
    if (msg.includes("social_dm_version")) {
      const legacyPack = { ...pack };
      delete legacyPack.social_dm_version;
      delete legacyPack.follow_up_line;
      await saveCaseFile(oppRow.id, { ...legacyPack, updated_at: new Date().toISOString() });
    } else {
      throw err;
    }
  }
  console.log("outreach pack regenerated");
  return mergeCaseIntoUI(ui, pack);
}

export { loadNotes as fetchNotesFromSupabase } from "../src/lib/notes.js";
import { addNote } from "../src/lib/notes.js";

export async function saveNoteToSupabase({ opportunity_id, body }) {
  return addNote(opportunity_id, body);
}

export async function saveScoutRunToSupabase({ summary, processed_count, saved_count, skipped_count }) {
  if (!supabase) return;
  const { data: authData, error: authError } = await supabase.auth.getUser();
  if (authError) {
    console.error("saveScoutRunToSupabase auth error:", authError);
    return;
  }
  const user = authData?.user || null;
  if (!user) return;

  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }

  let { error } = await supabase.from("scout_runs").insert(withWorkspaceId({
    user_id: user.id,
    summary: summary || "",
    processed_count: processed_count ?? 0,
    saved_count: saved_count ?? 0,
    skipped_count: skipped_count ?? 0,
  }, workspaceId));

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    const retry = await supabase.from("scout_runs").insert({
      user_id: user.id,
      summary: summary || "",
      processed_count: processed_count ?? 0,
      saved_count: saved_count ?? 0,
      skipped_count: skipped_count ?? 0,
    });
    error = retry.error;
  }
  if (error) throw error;
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

export async function getUserEmailSettingsFromSupabase() {
  if (!supabase) return defaultEmailAlertSettings();
  const { data: authData, error: authError } = await supabase.auth.getUser();
  if (authError) throw authError;
  const user = authData?.user || null;
  if (!user) throw new Error("Not authenticated");

  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }
  if (!workspaceId) return defaultEmailAlertSettings();

  let query = supabase
    .from("user_settings")
    .select("*")
    .eq("user_id", user.id)
    .eq("workspace_id", workspaceId)
    .limit(1);

  let { data, error } = await query;
  if (error && isMissingWorkspaceSchemaError(error)) {
    return defaultEmailAlertSettings();
  }
  if (error) throw error;
  if (!data || !data.length) return defaultEmailAlertSettings();
  const row = data[0];
  return {
    email_notifications_enabled: !!row.email_notifications_enabled,
    email_frequency: row.email_frequency || "daily",
    include_new_leads: !!row.include_new_leads,
    include_followups: !!row.include_followups,
    include_top_opportunities: !!row.include_top_opportunities,
  };
}

export async function saveUserEmailSettingsToSupabase(settings) {
  if (!supabase) throw new Error("Not authenticated");
  const { data: authData, error: authError } = await supabase.auth.getUser();
  if (authError) throw authError;
  const user = authData?.user || null;
  if (!user) throw new Error("Not authenticated");

  let workspaceId = null;
  try {
    const ctx = await resolveWorkspaceContext();
    workspaceId = ctx?.workspaceId || null;
  } catch {
    workspaceId = null;
  }
  if (!workspaceId) throw new Error("Workspace context unavailable");

  const payload = withWorkspaceId({
    user_id: user.id,
    email_notifications_enabled: !!settings.email_notifications_enabled,
    email_frequency: settings.email_frequency || "daily",
    include_new_leads: !!settings.include_new_leads,
    include_followups: !!settings.include_followups,
    include_top_opportunities: !!settings.include_top_opportunities,
    updated_at: new Date().toISOString(),
  }, workspaceId);

  let { error } = await supabase
    .from("user_settings")
    .upsert(payload, { onConflict: "user_id,workspace_id" });

  if (error && workspaceId && isMissingWorkspaceSchemaError(error)) {
    throw new Error("user_settings table not available yet. Run latest migration.");
  }
  if (error) throw error;

  return getUserEmailSettingsFromSupabase();
}
