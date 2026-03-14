#!/usr/bin/env python3
"""
Morning Runner — automated one-button local client finder.

Prioritizes businesses with NO website, secondary lane for weak websites.
Uses config: home_city, search_radius_miles, categories, max_results_per_category, ignore_chains.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    from .case_schema import empty_case, slug_from_name, save_case
    from .investigator import investigate
    from .outreach_generator import generate_outreach_pack
    from .errors import ScoutRunError
except ImportError:
    from case_schema import empty_case, slug_from_name, save_case
    from investigator import investigate
    from outreach_generator import generate_outreach_pack
    from errors import ScoutRunError

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
CITY_DATASET_PATH = SCRIPT_DIR / "cities_dataset.json"
CASES_DIR = SCRIPT_DIR / "cases"
CASE_FILES_DIR = SCRIPT_DIR / "case_files"
TODAY_PATH = SCRIPT_DIR / "today.json"
OPPORTUNITIES_PATH = SCRIPT_DIR / "opportunities.json"

CHAIN_CLUES = ["mcdonald", "starbucks", "subway", "dunkin", "walmart", "target", "chain", "franchise"]

# Template-like prefixes from mock data; skip these (not real Places results)
WEAK_NAME_PREFIXES = ("family ", "main street ", "local ", "downtown ")


def _is_weak_name(name: str, category: str) -> bool:
    """Skip generic/template names that suggest mock data or unusable results."""
    if not name or len(name.strip()) < 3:
        return True
    lower = name.lower().strip()
    if (category or "") and lower == category.lower():
        return True
    return any(lower.startswith(p) for p in WEAK_NAME_PREFIXES)


def _validate_case(case: dict) -> tuple[bool, str]:
    """
    Validate case before saving. Requires:
    - real business_name
    - category
    - address OR maps_link
    - at least one of: phone, website, maps_link
    Returns (valid, reason).
    """
    name = (case.get("business_name") or "").strip()
    if not name:
        return False, "missing business_name"
    cat = case.get("category")
    if not cat:
        return False, "missing category"
    has_location = bool((case.get("address") or "").strip()) or bool((case.get("maps_link") or "").strip())
    if not has_location:
        return False, "missing address and maps_link"
    has_contact = bool((case.get("phone") or "").strip()) or bool((case.get("website") or "").strip()) or bool((case.get("maps_link") or "").strip())
    if not has_contact:
        return False, "missing phone, website, and maps_link"
    return True, ""


def _log_place_fields(place: dict, log: list) -> None:
    """Log found/missing for each key field."""
    for field, key in [
        ("address", "address"), ("phone", "phone"), ("website", "website"),
        ("maps_link", "maps_url"), ("rating", "rating"), ("review_count", "review_count"),
        ("hours", "hours"),
    ]:
        val = place.get(key)
        status = "found" if (val is not None and str(val).strip()) else "missing"
        log.append(f"  {field}: {status}")


def _is_chain(name: str) -> bool:
    if not name:
        return False
    lower = name.lower()
    return any(c in lower for c in CHAIN_CLUES)


def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def load_city_dataset() -> list[dict]:
    if not CITY_DATASET_PATH.exists():
        return []
    with open(CITY_DATASET_PATH, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    out = []
    for row in data:
        if not isinstance(row, dict):
            continue
        name = str(row.get("city_name") or "").strip()
        state = str(row.get("state") or "").strip()
        if not name:
            continue
        try:
            lat = float(row.get("latitude")) if row.get("latitude") is not None else None
            lng = float(row.get("longitude")) if row.get("longitude") is not None else None
        except Exception:
            lat = None
            lng = None
        out.append(
            {
                "city_name": name,
                "state": state,
                "latitude": lat,
                "longitude": lng,
                "population": int(row.get("population") or 0),
            }
        )
    return out


def _resolve_target_cities(config: dict) -> list[dict]:
    multi_city_enabled = bool(config.get("multi_city_enabled", False))
    home_city = str(config.get("home_city") or "City").strip()
    if not multi_city_enabled:
        return [{"city_name": home_city, "state": "", "latitude": None, "longitude": None, "population": 0}]

    dataset = load_city_dataset()
    if not dataset:
        return [{"city_name": home_city, "state": "", "latitude": None, "longitude": None, "population": 0}]

    explicit_targets = config.get("target_cities") or []
    if explicit_targets:
        selected = []
        dataset_by_key = {
            f"{r['city_name'].lower()}|{r['state'].lower()}": r for r in dataset
        }
        for t in explicit_targets:
            if isinstance(t, str):
                city_name = t.strip()
                state = ""
            elif isinstance(t, dict):
                city_name = str(t.get("city_name") or t.get("city") or "").strip()
                state = str(t.get("state") or "").strip()
            else:
                continue
            if not city_name:
                continue
            row = dataset_by_key.get(f"{city_name.lower()}|{state.lower()}")
            selected.append(row or {"city_name": city_name, "state": state, "latitude": None, "longitude": None, "population": 0})
        return selected

    max_cities = max(1, int(config.get("max_cities_per_run", 5)))
    ranked = sorted(dataset, key=lambda r: int(r.get("population") or 0), reverse=True)
    return ranked[:max_cities]


def _fetch_places(
    city: str,
    categories: list,
    max_per: int,
    radius: float,
    current_lat: float | None = None,
    current_lng: float | None = None,
    radii_miles: list[float] | None = None,
    max_total_results: int = 120,
):
    def log(msg: str) -> None:
        print(f"  {msg}")

    try:
        from .places_client import search_places
        return search_places(
            city,
            categories,
            max_per,
            radii_miles if radii_miles else radius,
            log=log,
            current_lat=current_lat,
            current_lng=current_lng,
            max_total_results=max_total_results,
        )
    except ImportError:
        from places_client import search_places
        return search_places(
            city,
            categories,
            max_per,
            radii_miles if radii_miles else radius,
            log=log,
            current_lat=current_lat,
            current_lng=current_lng,
            max_total_results=max_total_results,
        )


def _as_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _as_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def calculateOpportunityScore(lead: dict) -> tuple[int, list[str], str]:
    """
    Opportunity score engine (0-100).
    Returns: (score, scoring_signals, lead_tier).
    """
    score = 35.0
    score_signals: list[str] = []

    rating = _as_float(lead.get("rating"), 0.0)
    review_count = _as_int(lead.get("review_count"), 0)
    lane = str(lead.get("lane") or "").strip().lower()

    outdated_design = lead.get("outdated_design_clues") is True
    mobile_layout_problem = lead.get("viewport_ok") is False
    text_heavy = lead.get("text_heavy_clues") is True
    missing_cta = lead.get("tap_to_call_present") is False and lead.get("contact_form_present") is False
    business_closed = str(lead.get("business_status") or "").strip().lower() in {"closed", "permanently_closed"}

    modern_website_detected = (
        lane == "weak_website"
        and lead.get("viewport_ok") is True
        and lead.get("tap_to_call_present") is True
        and lead.get("contact_form_present") is True
        and not outdated_design
        and not text_heavy
    )

    if lane == "no_website" or lead.get("no_website"):
        score += 20
        score_signals.append("+20 no website present")

    if rating >= 4.2:
        score += 15
        score_signals.append("+15 review rating >= 4.2")
    if review_count >= 50:
        score += 15
        score_signals.append("+15 review count >= 50")
    if outdated_design:
        score += 15
        score_signals.append("+15 outdated design clues")
    if mobile_layout_problem:
        score += 15
        score_signals.append("+15 mobile layout problem")
    if text_heavy:
        score += 10
        score_signals.append("+10 text-heavy homepage")
    if missing_cta:
        score += 10
        score_signals.append("+10 missing CTA")

    if modern_website_detected:
        score -= 30
        score_signals.append("-30 modern website detected")
    if rating > 0 and rating < 3.5:
        score -= 20
        score_signals.append("-20 review rating < 3.5")
    if business_closed:
        score -= 50
        score_signals.append("-50 business closed")

    if lead.get("phone") or lead.get("email") or lead.get("contact_page"):
        score += 5
        score_signals.append("+5 reachable contact available")

    clamped = max(0, min(100, int(round(score))))
    if clamped >= 85:
        tier = "Hot Lead"
    elif clamped >= 70:
        tier = "Strong Lead"
    elif clamped >= 50:
        tier = "Possible Lead"
    else:
        tier = "Low Priority"
    return clamped, score_signals, tier


def generateOpportunitySignals(caseData: dict) -> list[str]:
    print("generating opportunity signals")
    signals: list[str] = []
    lane = (caseData.get("lane") or "").strip().lower()
    if lane == "no_website" or caseData.get("no_website"):
        signals.append("No website detected")
    elif lane == "weak_website":
        signals.append("Website needs improvement")

    if caseData.get("outdated_design_clues") is True:
        signals.append("Website appears outdated")
    if caseData.get("viewport_ok") is False:
        signals.append("Website not optimized for mobile")
    if caseData.get("tap_to_call_present") is False:
        signals.append("Missing tap-to-call on mobile")
    if caseData.get("tap_to_call_present") is False and caseData.get("contact_form_present") is False:
        signals.append("Missing clear CTA on homepage")
    if caseData.get("menu_found") is False or caseData.get("menu_visibility") is False:
        signals.append("Key service/menu info is hard to find")

    review_count = caseData.get("review_count")
    try:
        review_count = int(review_count) if review_count is not None else 0
    except Exception:
        review_count = 0
    if review_count > 50:
        signals.append("Active customer reviews")
    elif review_count > 10:
        signals.append("Consistent customer review activity")

    rating = caseData.get("rating")
    try:
        rating = float(rating) if rating is not None else None
    except Exception:
        rating = None
    if rating is not None and rating >= 4.2 and review_count > 10:
        signals.append("Strong review reputation")

    distance = caseData.get("distance_miles")
    try:
        distance = float(distance) if distance is not None else None
    except Exception:
        distance = None
    if distance is not None and distance < 5:
        signals.append("Very close proximity")
    elif distance is not None and distance < 12:
        signals.append("Local business within target distance")

    # Keep the list concise and unique.
    deduped: list[str] = []
    for signal in signals:
        if signal not in deduped:
            deduped.append(signal)
    final_signals = deduped[:6]
    print("opportunity signals generated")
    return final_signals


def _outreach_no_website(name: str, city: str) -> dict:
    """Backward-compatible wrapper; prefer generate_outreach_pack."""
    return generate_outreach_pack(
        {
            "business_name": name,
            "lane": "no_website",
            "no_website": True,
            "strongest_pitch_angle": "Get your first simple website so customers can find you online",
            "best_service_to_offer": "First website — simple, mobile-friendly, with hours and contact",
        },
        city_hint=city,
    )


def _outreach_weak_website(name: str, city: str, problems: list, pitch_lines: list) -> dict:
    """Backward-compatible wrapper; prefer generate_outreach_pack."""
    return generate_outreach_pack(
        {
            "business_name": name,
            "lane": "weak_website",
            "strongest_problems": problems or [],
            "strongest_pitch_angle": (pitch_lines or [None])[0],
            "best_service_to_offer": "Modern mobile-friendly website with clear menu, hours, and contact",
        },
        city_hint=city,
    )


def _build_no_website_case(place: dict, home_city: str, categories: list, index: int, log: list, category: str = "") -> dict | None:
    """Build case from enriched Place Details. No website lane."""
    name = (place.get("name") or "").strip()
    if not name:
        log.append("  SKIP: missing business name")
        return None
    if _is_weak_name(name, category):
        log.append(f"  SKIP: weak/generic name '{name}'")
        return None
    website = (place.get("website") or "").strip()
    if website:
        log.append(f"  SKIP: has website (use weak-website lane)")
        return None

    log.append(f"  Found: {name}")
    log.append("  place_details: success (enriched)")
    _log_place_fields(place, log)
    log.append("  lane: no_website")

    slug = slug_from_name(name, index)
    case = empty_case(slug)
    case["lane"] = "no_website"
    case["no_website"] = True

    case["business_name"] = name
    case["category"] = place.get("category") or (categories[0] if categories else None)
    case["industry"] = case["category"]
    case["city"] = place.get("city")
    case["state"] = place.get("state")
    case["place_id"] = place.get("place_id")
    case["address"] = place.get("address") or place.get("vicinity")
    case["distance_miles"] = place.get("distance_miles")
    case["phone"] = place.get("phone")
    case["website"] = None
    case["maps_link"] = place.get("maps_url")
    case["hours"] = place.get("hours")
    case["rating"] = place.get("rating")
    case["review_count"] = place.get("review_count")
    case["business_status"] = place.get("business_status")
    case["review_snippets"] = place.get("review_snippets") or []
    case["review_themes"] = place.get("review_themes") or []

    case["email"] = None
    case["contact_page"] = None
    case["phone_from_site"] = None
    case["facebook"] = None
    case["instagram"] = None
    case["linkedin"] = None
    case["social_links"] = {}
    case["emails"] = []
    case["phones"] = [place["phone"]] if place.get("phone") else []

    if case["phone"]:
        case["recommended_contact_method"] = "Phone"
        case["backup_contact_method"] = "Maps / visit"
    else:
        case["recommended_contact_method"] = "Maps / visit in person"
        case["backup_contact_method"] = None

    case["strongest_problems"] = ["No website — missing online presence"]
    case["website_score"] = 0
    case["mobile_score"] = 0
    case["design_score"] = 0
    case["navigation_score"] = 0
    case["conversion_score"] = 0
    case["audit_issues"] = ["No website detected"]
    case["high_opportunity"] = bool((case.get("rating") or 0) >= 4.2 and (case.get("website_score") or 100) <= 60)
    case["strongest_pitch_angle"] = "Get your first simple website so customers can find you online"
    case["best_service_to_offer"] = "First website — simple, mobile-friendly, with hours and contact"
    case["best_demo_to_show"] = "Show example of similar local business site"
    case["demo_to_show"] = "Show example of similar local business site"
    case["why_worth_pursuing"] = f"{name} — independent business with no website. High intent for first-site build."
    case["why_this_lead_is_worth_pursuing"] = case["why_worth_pursuing"]
    case["what_stood_out"] = "No website"
    case["next_action"] = "Call or visit with short pitch"
    case["follow_up_suggestion"] = "Follow up in 3–5 days"
    score, score_signals, lead_tier = calculateOpportunityScore(case)
    case["opportunity_score"] = score
    case["internal_score"] = score
    case["lead_tier"] = lead_tier
    case["priority"] = "high" if score >= 70 else "medium" if score >= 50 else "low"
    case["contact_matrix"] = {
        "best_contact": "phone" if case["phone"] else "visit",
        "best_contact_method": "phone" if case["phone"] else "visit",
        "backup_contact": "maps",
        "backup_contact_method": "maps",
        "email": None,
        "phone": case["phone"],
        "contact_page": None,
        "facebook": None,
        "instagram": None,
        "linkedin": None,
        "owner_name": None,
        "phone_available": bool(case["phone"]),
        "contact_form_available": False,
        "social_available": False,
        "email_available": False,
    }
    base_signals = generateOpportunitySignals(case)
    merged_signals = list(dict.fromkeys(base_signals + score_signals))
    case["opportunity_signals"] = merged_signals[:8]

    pack = generate_outreach_pack(case, city_hint=home_city, logger=log.append)
    case["short_email"] = pack["short_email"]
    case["longer_email"] = pack["longer_email"]
    case["contact_form_version"] = pack["contact_form_version"]
    case["social_dm_version"] = pack["social_dm_version"]
    case["follow_up_note"] = pack["follow_up_note"]
    case["follow_up_line"] = pack["follow_up_line"]

    valid, reason = _validate_case(case)
    if not valid:
        log.append(f"  SKIP validation: {reason}")
        return None
    save_case(CASES_DIR, case)
    log.append(f"  case_file_written: {case['slug']}.json")
    return case


def _build_weak_website_case(place: dict, home_city: str, categories: list, index: int, log: list, category: str = "") -> dict | None:
    """Build case from enriched Place Details. Has website; run investigator."""
    name = (place.get("name") or "").strip()
    website = (place.get("website") or "").strip()
    if not name:
        log.append("  SKIP: missing business name")
        return None
    if _is_weak_name(name, category):
        log.append(f"  SKIP: weak/generic name '{name}'")
        return None
    if not website:
        log.append("  SKIP: no website (use no-website lane)")
        return None

    log.append(f"  Found: {name}")
    log.append("  place_details: success (enriched)")
    _log_place_fields(place, log)
    log.append("  lane: weak_website")

    slug = slug_from_name(name, index)
    case = empty_case(slug)
    case["lane"] = "weak_website"
    case["no_website"] = False

    case["business_name"] = name
    case["category"] = place.get("category") or (categories[0] if categories else None)
    case["industry"] = case["category"]
    case["city"] = place.get("city")
    case["state"] = place.get("state")
    case["place_id"] = place.get("place_id")
    case["address"] = place.get("address") or place.get("vicinity")
    case["distance_miles"] = place.get("distance_miles")
    case["phone"] = place.get("phone")
    case["website"] = website
    case["maps_link"] = place.get("maps_url")
    case["hours"] = place.get("hours")
    case["rating"] = place.get("rating")
    case["review_count"] = place.get("review_count")
    case["business_status"] = place.get("business_status")
    case["review_snippets"] = place.get("review_snippets") or []
    case["review_themes"] = place.get("review_themes") or []

    log.append(f"  Investigating: {website}")
    screenshot_dir = CASE_FILES_DIR / slug
    inv = None
    try:
        inv = investigate(website, crawl_internal=True, timeout=14, screenshot_dir=str(screenshot_dir))
    except Exception as e:
        log.append(f"  INVESTIGATION FAILED: {e}")

    if inv and inv.get("fetch_ok"):
        case["platform_used"] = inv.get("platform_used")
        case["homepage_title"] = inv.get("homepage_title")
        case["meta_description"] = inv.get("meta_description")
        case["viewport_ok"] = inv.get("viewport_ok")
        case["tap_to_call_present"] = inv.get("tap_to_call_present")
        case["menu_found"] = inv.get("menu_visibility")
        case["hours_found"] = inv.get("hours_visibility")
        case["directions_found"] = inv.get("directions_visibility")
        case["menu_visibility"] = inv.get("menu_visibility")
        case["hours_visibility"] = inv.get("hours_visibility")
        case["directions_visibility"] = inv.get("directions_visibility")
        case["contact_form_present"] = inv.get("contact_form_present")
        case["text_heavy_clues"] = inv.get("text_heavy_clues")
        case["outdated_design_clues"] = inv.get("outdated_design_clues")
        case["website_score"] = inv.get("website_score")
        case["mobile_score"] = inv.get("mobile_score")
        case["design_score"] = inv.get("design_score")
        case["navigation_score"] = inv.get("navigation_score")
        case["conversion_score"] = inv.get("conversion_score")
        case["audit_issues"] = inv.get("audit_issues") or inv.get("detected_issues") or []
        case["ssl_ok"] = inv.get("ssl_ok")
        case["internal_links_found"] = inv.get("internal_links_found") or {}
        case["important_internal_links"] = inv.get("important_internal_links") or inv.get("internal_links_found") or {}
        case["page_navigation_items"] = inv.get("page_navigation_items") or []
        case["navigation_items"] = inv.get("navigation_items") or inv.get("page_navigation_items") or []
        case["emails"] = inv.get("emails") or []
        case["phones"] = inv.get("phones") or []
        case["owner_names"] = inv.get("owner_names") or []
        case["owner_name"] = inv.get("owner_name")
        case["owner_title"] = inv.get("owner_title")
        case["owner_source_page"] = inv.get("owner_source_page")
        case["contact_matrix"] = inv.get("contact_matrix") or {}
        case["discovered_pages"] = inv.get("discovered_pages") or []
        case["reservation_link"] = inv.get("reservation_link")
        case["order_link"] = inv.get("order_link")
        case["desktop_screenshot_path"] = inv.get("desktop_homepage_path")
        case["mobile_screenshot_path"] = inv.get("mobile_homepage_path")
        case["internal_screenshot_path"] = inv.get("internal_page_path")
        case["desktop_screenshot_url"] = f"/case/{slug}/screenshot/desktop_homepage" if inv.get("desktop_homepage_path") else None
        case["mobile_screenshot_url"] = f"/case/{slug}/screenshot/mobile_homepage" if inv.get("mobile_homepage_path") else None
        case["internal_screenshot_url"] = f"/case/{slug}/screenshot/key_internal_page" if inv.get("internal_page_path") else None

        emails = inv.get("emails") or []
        social = inv.get("social") or {}
        case["email"] = emails[0] if emails else None
        case["contact_page"] = inv.get("contact_page")
        case["phone_from_site"] = (inv.get("phones") or [None])[0]
        case["facebook"] = social.get("facebook")
        case["instagram"] = social.get("instagram")
        case["linkedin"] = social.get("linkedin")
        case["social_links"] = social
        case["owner_manager_name"] = case.get("owner_name") or (case.get("owner_names") or [None])[0]

        problems = inv.get("problems") or []
        pitch_lines = inv.get("pitch") or []
        for line in inv.get("debug_log") or []:
            log.append(f"    {line}")
    else:
        problems = ["Website could not be fully investigated."]
        pitch_lines = ["Manual review recommended."]
        log.append("    website investigation: failed")

    case["strongest_problems"] = problems
    if not case.get("audit_issues"):
        case["audit_issues"] = list(problems) if isinstance(problems, list) else [str(problems)]
    website_score = case.get("website_score")
    case["high_opportunity"] = bool((case.get("rating") or 0) >= 4.2 and (website_score if website_score is not None else 100) <= 60)
    case["strongest_pitch_angle"] = pitch_lines[0] if pitch_lines else None
    case["best_service_to_offer"] = "Modern mobile-friendly website with clear menu, hours, and contact"
    case["best_demo_to_show"] = "Show demo on iPad"
    case["demo_to_show"] = "Show demo on iPad"

    if case["email"]:
        case["recommended_contact_method"] = "Email"
    elif case["phone"] or case["phone_from_site"]:
        case["recommended_contact_method"] = "Phone"
    elif case["contact_page"]:
        case["recommended_contact_method"] = "Contact form"
    elif case["facebook"] or case["instagram"]:
        case["recommended_contact_method"] = "Social DM"
    else:
        case["recommended_contact_method"] = "Website or phone"
    case["backup_contact_method"] = "Phone" if case["email"] else "Email" if (case["phone"] or case["phone_from_site"]) else None

    pack = generate_outreach_pack(case, city_hint=home_city, logger=log.append)
    case["short_email"] = pack["short_email"]
    case["longer_email"] = pack["longer_email"]
    case["contact_form_version"] = pack["contact_form_version"]
    case["social_dm_version"] = pack["social_dm_version"]
    case["follow_up_note"] = pack["follow_up_note"]
    case["follow_up_line"] = pack["follow_up_line"]

    case["why_worth_pursuing"] = f"{name} — real business with website, worth outreach."
    case["why_this_lead_is_worth_pursuing"] = case["why_worth_pursuing"]
    case["what_stood_out"] = problems[0] if problems else None
    case["next_action"] = "Send short email or try contact form"
    case["follow_up_suggestion"] = "Follow up in 5–7 days"
    score, score_signals, lead_tier = calculateOpportunityScore(case)
    case["opportunity_score"] = score
    case["internal_score"] = score
    case["lead_tier"] = lead_tier
    case["priority"] = "high" if score >= 70 else "medium" if score >= 50 else "low"
    base_signals = generateOpportunitySignals(case)
    merged_signals = list(dict.fromkeys(base_signals + score_signals))
    case["opportunity_signals"] = merged_signals[:8]

    valid, reason = _validate_case(case)
    if not valid:
        log.append(f"  SKIP validation: {reason}")
        return None
    save_case(CASES_DIR, case)
    log.append(f"  case_file_written: {case['slug']}.json")
    return case


def run(current_lat: float | None = None, current_lng: float | None = None):
    try:
        from dotenv import load_dotenv
        load_dotenv(SCRIPT_DIR / ".env")
        load_dotenv(SCRIPT_DIR.parent / ".env")
    except ImportError:
        pass

    config = load_config()
    home_city = config.get("home_city", "City")
    target_cities = _resolve_target_cities(config)
    radius = config.get("search_radius_miles", 25)
    categories = config.get("categories", [
        "restaurant",
        "cafe",
        "bakery",
        "auto repair",
        "dentist",
        "plumber",
        "lawyer",
        "gym",
        "church",
        "salon",
        "barber shop",
        "chiropractor",
        "contractor",
    ])
    radii_miles = config.get("search_radii_miles", [2, 5, 10, 15])
    max_total_results = int(config.get("max_total_results_per_run", 120))
    max_per = max(1, config.get("max_results_per_category", 5))
    ignore_chains = config.get("ignore_chains", True)
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()

    print("Morning Runner — automated local client finder")
    print(f"  home_city: {home_city}, radius: {radius} mi")
    print(f"  target cities: {len(target_cities)}")
    print(f"  search_radii_miles: {radii_miles}")
    print(f"  categories: {categories}, max_per: {max_per}, ignore_chains: {ignore_chains}")
    print(f"  max_total_results_per_run: {max_total_results}")
    if current_lat is not None and current_lng is not None:
        print(f"  location mode: current ({current_lat}, {current_lng})")
    else:
        print(f"  location mode: saved config ({home_city})")
    if not api_key:
        print()
        print("  ERROR: GOOGLE_MAPS_API_KEY not set. Add to scout/.env")
        raise ScoutRunError(
            "api_key_missing",
            "GOOGLE_MAPS_API_KEY not set",
            "Scout failed: Google Maps API key not configured. Add GOOGLE_MAPS_API_KEY to scout/.env",
        )
    print()

    places = []
    seen_place_ids: set[str] = set()
    total_duplicates_skipped = 0
    try:
        for target in target_cities:
            city_name = target.get("city_name") or home_city
            state = target.get("state") or ""
            city_label = f"{city_name}, {state}" if state else city_name
            city_lat = target.get("latitude")
            city_lng = target.get("longitude")
            scan_lat = current_lat if (current_lat is not None and current_lng is not None and len(target_cities) == 1) else city_lat
            scan_lng = current_lng if (current_lat is not None and current_lng is not None and len(target_cities) == 1) else city_lng
            print(f"  city being scanned: {city_label}")
            city_places = _fetch_places(
                city_label,
                categories,
                max_per,
                radius,
                current_lat=scan_lat,
                current_lng=scan_lng,
                radii_miles=radii_miles,
                max_total_results=max_total_results,
            )
            city_added = 0
            city_dupes = 0
            for p in city_places:
                pid = str(p.get("place_id") or "").strip()
                if pid and pid in seen_place_ids:
                    city_dupes += 1
                    total_duplicates_skipped += 1
                    continue
                if pid:
                    seen_place_ids.add(pid)
                p["city"] = city_name
                p["state"] = state
                places.append(p)
                city_added += 1
                if len(places) >= max_total_results:
                    break
            print(f"  businesses discovered: {city_added}")
            print(f"  duplicates skipped: {city_dupes}")
            if len(places) >= max_total_results:
                break
    except ScoutRunError:
        raise
    except Exception as e:
        err_str = str(e).upper()
        if "REQUEST_DENIED" in err_str or "LEGACY" in err_str:
            raise ScoutRunError(
                "request_denied",
                str(e),
                "Scout failed: Places API returned REQUEST_DENIED. "
                "Enable 'Places API (New)' and 'Geocoding API' in your Google Cloud project. "
                "You may be calling a legacy API that is not enabled.",
            ) from e
        err_lower = err_str.lower()
        if "certificate" in err_lower or "ssl" in err_lower:
            raise ScoutRunError(
                "ssl_verify_failed",
                str(e),
                "Scout failed: Python SSL certificate verification failed while calling Google APIs.",
            ) from e
        if "geocode" in err_lower or "resolve" in err_lower:
            raise ScoutRunError(
                "geocode_failed",
                str(e),
                "Scout could not resolve the configured city. Check SSL certificates or API access.",
            ) from e
        raise ScoutRunError(
            "places_api_failed",
            str(e),
            f"Scout failed: {str(e)}",
        ) from e

    if not places:
        _write_empty("No businesses from Google Places. Check API key and config.")
        return

    if ignore_chains:
        places = [p for p in places if not _is_chain(p.get("name") or "")]
        print(f"  Filtered chains: {len(places)} remaining")

    no_website = [p for p in places if not (p.get("website") or "").strip()]
    weak_website = [p for p in places if (p.get("website") or "").strip()]
    print(f"  total businesses discovered: {len(places)}")
    print(f"  duplicates skipped: {total_duplicates_skipped}")

    print(f"  No website: {len(no_website)} | Weak website: {len(weak_website)}")
    print()

    case_slugs = []
    no_website_slugs = []
    weak_website_slugs = []
    debug_log = []
    processed = 0
    saved = 0
    skipped = 0

    for i, place in enumerate(no_website):
        cat = place.get("category") or (categories[0] if categories else "")
        print(f"  [{cat}] ", end="")
        processed += 1
        case = _build_no_website_case(place, home_city, categories, i, debug_log, category=cat)
        for line in debug_log:
            print(line)
        debug_log.clear()
        if case:
            saved += 1
            no_website_slugs.append(case["slug"])
            case_slugs.append(case["slug"])
        else:
            skipped += 1

    for i, place in enumerate(weak_website):
        cat = place.get("category") or (categories[0] if categories else "")
        print(f"  [{cat}] ", end="")
        processed += 1
        case = _build_weak_website_case(place, home_city, categories, len(no_website) + i, debug_log, category=cat)
        for line in debug_log:
            print(line)
        debug_log.clear()
        if case:
            saved += 1
            weak_website_slugs.append(case["slug"])
            case_slugs.append(case["slug"])
        else:
            skipped += 1

    print()
    print(f"  Processed: {processed}")
    print(f"  Saved valid case files: {saved}")
    print(f"  Skipped invalid/incomplete leads: {skipped}")
    print()

    if not case_slugs:
        _write_empty("No case files written. Check Places API and config.")
        return

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    location_summary = f"{current_lat:.4f},{current_lng:.4f}" if (current_lat is not None and current_lng is not None) else home_city
    summary = f"Found {len(no_website_slugs)} no-website + {len(weak_website_slugs)} weak-website opportunities near {location_summary}."
    today = {
        "generated_at": generated_at,
        "summary": summary,
        "case_slugs": case_slugs,
        "no_website_slugs": no_website_slugs,
        "weak_website_slugs": weak_website_slugs,
        "top_opportunities": case_slugs,
        "businesses_discovered": len(places),
        "duplicates_skipped": total_duplicates_skipped,
        "unique_leads_created": len(case_slugs),
        "processed_count": processed,
        "saved_count": saved,
        "skipped_count": skipped,
    }
    with open(TODAY_PATH, "w", encoding="utf-8") as f:
        json.dump(today, f, indent=2)

    try:
        from .case_schema import case_to_ui
    except ImportError:
        from case_schema import case_to_ui
    ui_list = []
    for slug in case_slugs:
        p = CASES_DIR / f"{slug}.json"
        if p.exists():
            with open(p, encoding="utf-8") as f:
                c = json.load(f)
            ui_obj = case_to_ui(c)
            ui_obj["lane"] = c.get("lane", "weak_website")
            ui_obj["no_website"] = c.get("no_website", False)
            ui_list.append(ui_obj)
    with open(OPPORTUNITIES_PATH, "w", encoding="utf-8") as f:
        json.dump(ui_list, f, indent=2)

    print()
    print(f"  Wrote {len(case_slugs)} cases. {len(no_website_slugs)} no-website (priority), {len(weak_website_slugs)} weak-website.")
    print(f"  unique leads created: {len(case_slugs)}")


def _write_empty(summary: str | None = None):
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    today = {
        "generated_at": generated_at,
        "summary": summary or "No opportunities found.",
        "case_slugs": [],
        "no_website_slugs": [],
        "weak_website_slugs": [],
        "top_opportunities": [],
    }
    with open(SCRIPT_DIR / "today.json", "w", encoding="utf-8") as f:
        json.dump(today, f, indent=2)
    with open(SCRIPT_DIR / "opportunities.json", "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)


if __name__ == "__main__":
    run()
