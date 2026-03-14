/**
 * Supabase data layer for opportunities and case files.
 * Uses src/lib loaders; maps DB rows to app shape (case_to_ui compatible).
 */
import { supabase } from "../src/lib/supabaseClient.js";
import { loadOpportunities } from "../src/lib/opportunities.js";
import { loadCaseFile, saveCaseFile } from "../src/lib/caseFiles.js";

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

export async function fetchScoutDataFromSupabase() {
  if (!supabase) return null;

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

  await supabase.from("scout_runs").insert({
    user_id: user.id,
    summary: summary || "",
    processed_count: processed_count ?? 0,
    saved_count: saved_count ?? 0,
    skipped_count: skipped_count ?? 0,
  });
}
