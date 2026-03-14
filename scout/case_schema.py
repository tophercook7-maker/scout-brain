"""
Case file schema for Massive Brain v2 — one detailed JSON per lead.

Each opportunity is stored as a full case with identity, contact, site audit,
and outreach fields. Used by Morning Runner and the single app.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# All fields we want per opportunity (detailed case file)
CASE_FIELDS = [
    # Identity & location
    "business_name",
    "category",
    "industry",
    "city",
    "state",
    "place_id",
    "distance_miles",
    "address",
    "phone",
    "website",
    "maps_link",
    "hours",
    "rating",
    "review_count",
    # Contact
    "email",
    "contact_page",
    "phone_from_site",
    "facebook",
    "instagram",
    "linkedin",
    "owner_manager_name",
    "owner_name",
    "owner_title",
    "owner_source_page",
    "recommended_contact_method",
    "backup_contact_method",
    # Site / tech (from audit)
    "platform_used",
    "homepage_title",
    "meta_description",
    "navigation_items",
    "important_internal_links",
    "viewport_ok",
    "tap_to_call_present",
    "menu_found",
    "hours_found",
    "directions_found",
    "menu_visibility",
    "hours_visibility",
    "directions_visibility",
    "contact_form_present",
    "text_heavy_clues",
    "outdated_design_clues",
    "website_score",
    "website_status",
    "website_speed",
    "mobile_ready",
    "seo_score",
    "website_quality_score",
    "fetch_ok",
    "homepage_http_status",
    "homepage_load_seconds",
    "missing_meta_title",
    "missing_meta_description",
    "text_content_length",
    "image_count",
    "broken_links_count",
    "mobile_score",
    "design_score",
    "navigation_score",
    "conversion_score",
    "audit_issues",
    "high_opportunity",
    # Outreach
    "strongest_problems",
    "strongest_pitch_angle",
    "best_service_to_offer",
    "best_demo_to_show",
    "demo_to_show",
    "short_email",
    "longer_email",
    "contact_form_version",
    "social_dm_version",
    "follow_up_note",
    "review_snippets",
    "review_themes",
    "why_this_lead_is_worth_pursuing",
    "opportunity_score",
    "internal_score",
    "lead_tier",
    "priority",
    "opportunity_signals",
    "desktop_screenshot_url",
    "mobile_screenshot_url",
    "internal_screenshot_url",
]

# Legacy UI shape: some keys are nested (e.g. website_analysis, email_draft, contact).
# We keep a flat case file on disk and convert to/from UI shape in the app.


def empty_case(slug: str = "") -> dict[str, Any]:
    """Return one case dict with all fields set to None or empty."""
    return {
        "slug": slug,
        "business_name": None,
        "category": None,
        "industry": None,
        "city": None,
        "state": None,
        "place_id": None,
        "distance_miles": None,
        "address": None,
        "phone": None,
        "website": None,
        "maps_link": None,
        "hours": None,
        "rating": None,
        "review_count": None,
        "business_status": None,
        "email": None,
        "contact_page": None,
        "phone_from_site": None,
        "facebook": None,
        "instagram": None,
        "linkedin": None,
        "owner_manager_name": None,
        "owner_name": None,
        "owner_title": None,
        "owner_source_page": None,
        "lane": "weak_website",
        "no_website": False,
        "owner_names": [],
        "emails": [],
        "phones": [],
        "reservation_link": None,
        "order_link": None,
        "contact_matrix": {},
        "discovered_pages": [],
        "recommended_contact_method": None,
        "backup_contact_method": None,
        "platform_used": None,
        "homepage_title": None,
        "meta_description": None,
        "navigation_items": [],
        "important_internal_links": {},
        "viewport_ok": None,
        "tap_to_call_present": None,
        "menu_found": None,
        "hours_found": None,
        "directions_found": None,
        "menu_visibility": None,
        "hours_visibility": None,
        "directions_visibility": None,
        "contact_form_present": None,
        "text_heavy_clues": None,
        "outdated_design_clues": None,
        "website_score": None,
        "website_status": None,
        "website_speed": None,
        "mobile_ready": None,
        "seo_score": None,
        "website_quality_score": None,
        "fetch_ok": None,
        "homepage_http_status": None,
        "homepage_load_seconds": None,
        "missing_meta_title": None,
        "missing_meta_description": None,
        "text_content_length": None,
        "image_count": None,
        "broken_links_count": None,
        "mobile_score": None,
        "design_score": None,
        "navigation_score": None,
        "conversion_score": None,
        "audit_issues": [],
        "high_opportunity": False,
        "social_links": {},
        "internal_links_found": {},
        "page_navigation_items": [],
        "ssl_ok": None,
        "strongest_problems": [],
        "strongest_pitch_angle": None,
        "best_service_to_offer": None,
        "best_demo_to_show": None,
        "demo_to_show": None,
        "short_email": None,
        "longer_email": None,
        "contact_form_version": None,
        "social_dm_version": None,
        "follow_up_note": None,
        "follow_up_line": None,
        "why_problems_matter_customers": None,
        "why_problems_matter_owner": None,
        "why_worth_pursuing": None,
        "why_this_lead_is_worth_pursuing": None,
        "opportunity_score": None,
        "what_stood_out": None,
        "next_action": None,
        "follow_up_suggestion": None,
        "review_snippets": [],
        "review_themes": [],
        "social_links": {},
        "internal_links_found": {},
        "page_navigation_items": [],
        "ssl_ok": None,
        "internal_score": None,
        "lead_tier": None,
        "priority": None,
        "opportunity_signals": [],
        "desktop_screenshot_path": None,
        "mobile_screenshot_path": None,
        "internal_screenshot_path": None,
        "desktop_screenshot_url": None,
        "mobile_screenshot_url": None,
        "internal_screenshot_url": None,
        "screenshot_failed": False,
        # Outreach queue
        "status": "New",
        "first_contacted_at": None,
        "last_contacted_at": None,
        "follow_up_due": None,
        "outcome": None,
        "outreach_notes": None,
    }


def slug_from_name(name: str, index: int = 0) -> str:
    """Generate a filesystem-safe slug from business name + index."""
    base = re.sub(r"[^\w\s-]", "", (name or "lead").lower())
    base = re.sub(r"[-\s]+", "-", base).strip("-") or "lead"
    return f"{base}-{index}" if index else base


def case_to_ui(c: dict[str, Any]) -> dict[str, Any]:
    """Convert flat case file to legacy UI shape (name, website_analysis, email_draft, etc.)."""
    problems = c.get("strongest_problems") or []
    return {
        "slug": c.get("slug"),
        "name": c.get("business_name"),
        "distance_miles": c.get("distance_miles"),
        "category": c.get("category"),
        "industry": c.get("industry") or c.get("category"),
        "city": c.get("city"),
        "state": c.get("state"),
        "place_id": c.get("place_id"),
        "address": c.get("address"),
        "phone": c.get("phone"),
        "website": c.get("website"),
        "maps_url": c.get("maps_link"),
        "recommended_contact": c.get("recommended_contact_method"),
        "website_analysis": {
            "platform": c.get("platform_used"),
            "issues": problems if isinstance(problems, list) else [problems],
            "facts": _facts_from_case(c),
        },
        "website_audit": {
            "website_score": c.get("website_score"),
            "website_status": c.get("website_status"),
            "website_speed": c.get("website_speed"),
            "mobile_ready": c.get("mobile_ready"),
            "seo_score": c.get("seo_score"),
            "website_quality_score": c.get("website_quality_score"),
            "mobile_score": c.get("mobile_score"),
            "design_score": c.get("design_score"),
            "navigation_score": c.get("navigation_score"),
            "conversion_score": c.get("conversion_score"),
            "audit_issues": c.get("audit_issues") or [],
        },
        "score": c.get("opportunity_score") if c.get("opportunity_score") is not None else c.get("internal_score"),
        "opportunity_score": c.get("opportunity_score") if c.get("opportunity_score") is not None else c.get("internal_score"),
        "lead_tier": c.get("lead_tier"),
        "pitch_angle": c.get("strongest_pitch_angle"),
        "email_draft": {
            "subject": _email_subject(c),
            "body": c.get("short_email") or c.get("longer_email") or "",
        },
        "contact": {
            "email": c.get("email"),
            "emails": c.get("emails") or [],
            "phones": c.get("phones") or [],
            "contact_page": c.get("contact_page"),
            "phone_from_site": c.get("phone_from_site"),
            "facebook": c.get("facebook"),
            "instagram": c.get("instagram"),
            "linkedin": c.get("linkedin"),
        },
        "hours": c.get("hours"),
        "rating": c.get("rating"),
        "review_count": c.get("review_count"),
        "owner_manager_name": c.get("owner_manager_name"),
        "owner_name": c.get("owner_name"),
        "owner_title": c.get("owner_title"),
        "owner_source_page": c.get("owner_source_page"),
        "lane": c.get("lane", "weak_website"),
        "no_website": c.get("no_website", False),
        "backup_contact_method": c.get("backup_contact_method"),
        "homepage_title": c.get("homepage_title"),
        "meta_description": c.get("meta_description"),
        "viewport_ok": c.get("viewport_ok"),
        "tap_to_call_present": c.get("tap_to_call_present"),
        "menu_found": c.get("menu_found") if c.get("menu_found") is not None else c.get("menu_visibility"),
        "hours_found": c.get("hours_found") if c.get("hours_found") is not None else c.get("hours_visibility"),
        "directions_found": c.get("directions_found") if c.get("directions_found") is not None else c.get("directions_visibility"),
        "menu_visibility": c.get("menu_visibility"),
        "hours_visibility": c.get("hours_visibility"),
        "directions_visibility": c.get("directions_visibility"),
        "contact_form_present": c.get("contact_form_present"),
        "website_score": c.get("website_score"),
        "website_status": c.get("website_status"),
        "website_speed": c.get("website_speed"),
        "mobile_ready": c.get("mobile_ready"),
        "seo_score": c.get("seo_score"),
        "website_quality_score": c.get("website_quality_score"),
        "fetch_ok": c.get("fetch_ok"),
        "homepage_http_status": c.get("homepage_http_status"),
        "homepage_load_seconds": c.get("homepage_load_seconds"),
        "missing_meta_title": c.get("missing_meta_title"),
        "missing_meta_description": c.get("missing_meta_description"),
        "text_content_length": c.get("text_content_length"),
        "image_count": c.get("image_count"),
        "broken_links_count": c.get("broken_links_count"),
        "mobile_score": c.get("mobile_score"),
        "design_score": c.get("design_score"),
        "navigation_score": c.get("navigation_score"),
        "conversion_score": c.get("conversion_score"),
        "audit_issues": c.get("audit_issues") or [],
        "high_opportunity": bool(c.get("high_opportunity")),
        "best_service_to_offer": c.get("best_service_to_offer"),
        "best_demo_to_show": c.get("best_demo_to_show") or c.get("demo_to_show"),
        "demo_to_show": c.get("demo_to_show"),
        "longer_email": c.get("longer_email"),
        "contact_form_version": c.get("contact_form_version"),
        "social_dm_version": c.get("social_dm_version"),
        "follow_up_note": c.get("follow_up_note"),
        "follow_up_line": c.get("follow_up_line"),
        "why_problems_matter_customers": c.get("why_problems_matter_customers"),
        "why_problems_matter_owner": c.get("why_problems_matter_owner"),
        "why_worth_pursuing": c.get("why_worth_pursuing"),
        "why_this_lead_is_worth_pursuing": c.get("why_this_lead_is_worth_pursuing") or c.get("why_worth_pursuing"),
        "what_stood_out": c.get("what_stood_out"),
        "next_action": c.get("next_action"),
        "follow_up_suggestion": c.get("follow_up_suggestion"),
        "social_links": c.get("social_links") or {},
        "internal_links_found": c.get("internal_links_found") or c.get("important_internal_links") or {},
        "important_internal_links": c.get("important_internal_links") or c.get("internal_links_found") or {},
        "page_navigation_items": c.get("page_navigation_items") or c.get("navigation_items") or [],
        "navigation_items": c.get("navigation_items") or c.get("page_navigation_items") or [],
        "owner_names": c.get("owner_names") or [],
        "emails": c.get("emails") or [],
        "phones": c.get("phones") or [],
        "reservation_link": c.get("reservation_link"),
        "order_link": c.get("order_link"),
        "contact_matrix": c.get("contact_matrix") or {},
        "discovered_pages": c.get("discovered_pages") or [],
        "ssl_ok": c.get("ssl_ok"),
        "review_snippets": c.get("review_snippets") or [],
        "review_themes": c.get("review_themes") or [],
        "priority": c.get("priority"),
        "opportunity_score": c.get("opportunity_score") if c.get("opportunity_score") is not None else c.get("internal_score"),
        "lead_tier": c.get("lead_tier"),
        "opportunity_signals": c.get("opportunity_signals") or [],
        "desktop_screenshot_url": c.get("desktop_screenshot_url"),
        "mobile_screenshot_url": c.get("mobile_screenshot_url"),
        "internal_screenshot_url": c.get("internal_screenshot_url"),
        "status": c.get("status") or "New",
        "first_contacted_at": c.get("first_contacted_at"),
        "last_contacted_at": c.get("last_contacted_at"),
        "follow_up_due": c.get("follow_up_due"),
        "outcome": c.get("outcome"),
        "outreach_notes": c.get("outreach_notes"),
    }


def _facts_from_case(c: dict[str, Any]) -> list[str]:
    facts = []
    if c.get("homepage_title"):
        facts.append(f"Title: {c['homepage_title'][:100]}")
    if c.get("platform_used"):
        facts.append(f"Platform: {c['platform_used']}")
    if c.get("viewport_ok") is False:
        facts.append("Viewport/mobile: issues")
    if c.get("tap_to_call_present"):
        facts.append("Tap-to-call present")
    return facts


def _email_subject(c: dict[str, Any]) -> str:
    name = c.get("business_name") or "your business"
    return f"Quick idea for {name}'s website"


def load_cases_dir(cases_dir: Path) -> list[dict]:
    """Load all case JSON files from scout/cases/ and return as UI-shaped list."""
    if not cases_dir.is_dir():
        return []
    cases = []
    for path in sorted(cases_dir.glob("*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                c = json.load(f)
            cases.append(case_to_ui(c))
        except Exception:
            continue
    return cases


def save_case(cases_dir: Path, case: dict[str, Any]) -> Path:
    """Write one case to scout/cases/{slug}.json. Creates dir if needed."""
    cases_dir.mkdir(parents=True, exist_ok=True)
    slug = case.get("slug") or slug_from_name(case.get("business_name"), 0)
    case["slug"] = slug
    path = cases_dir / f"{slug}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(case, f, indent=2)
    return path
