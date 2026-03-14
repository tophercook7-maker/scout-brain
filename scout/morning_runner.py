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
    from .errors import ScoutRunError
except ImportError:
    from case_schema import empty_case, slug_from_name, save_case
    from investigator import investigate
    from errors import ScoutRunError

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "config.json"
CASES_DIR = SCRIPT_DIR / "cases"
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


def _fetch_places(city: str, categories: list, max_per: int, radius: float, current_lat: float | None = None, current_lng: float | None = None):
    def log(msg: str) -> None:
        print(f"  {msg}")

    try:
        from .places_client import search_places
        return search_places(city, categories, max_per, radius, log=log, current_lat=current_lat, current_lng=current_lng)
    except ImportError:
        from places_client import search_places
        return search_places(city, categories, max_per, radius, log=log, current_lat=current_lat, current_lng=current_lng)


def _score_no_website(case: dict) -> float:
    """Higher = better. Prioritize: phone, rating, review activity, address."""
    score = 50.0  # base for no-website (high priority)
    if case.get("phone"):
        score += 15
    if case.get("rating") is not None:
        score += 5
    if (case.get("review_count") or 0) > 0:
        score += 5
    if case.get("address"):
        score += 5
    dist = case.get("distance_miles")
    if dist is not None and dist < 10:
        score += 5
    return score


def _score_weak_website(case: dict) -> float:
    """Higher = better. Phone, rating, investigation success."""
    score = 20.0
    if case.get("phone") or case.get("phone_from_site"):
        score += 10
    if case.get("email"):
        score += 10
    if case.get("rating") is not None:
        score += 3
    if case.get("platform_used") and "weebly" in (case.get("platform_used") or "").lower():
        score += 5
    return score


def _outreach_no_website(name: str, city: str) -> dict:
    """Outreach drafts for businesses with no website."""
    short = f"""Hi there,

My name is Topher and I run MixedMakerShop.

I noticed {name} in {city} doesn't have a website yet — a simple site can help local customers find you, your hours, and how to reach you.

I build straightforward websites for small businesses. Happy to send a quick example if you're interested.

Thanks,
Topher
topher@mixedmakershop.com
MixedMakerShop.com"""

    long_body = f"""Hi there,

My name is Topher and I run MixedMakerShop.

I noticed {name} in {city} doesn't have a website yet. A simple site can make it easier for customers to:
- Find your hours and location
- Call or contact you
- See what you offer

I build straightforward websites for local businesses — nothing fancy, just clear and useful. Would love to send a quick example if you're open to it.

No pressure — just wanted to reach out.

Thanks,
Topher
topher@mixedmakershop.com
MixedMakerShop.com"""

    follow_up = f"Following up — still happy to send that quick website example for {name} whenever works for you."

    return {
        "short_email": short,
        "longer_email": long_body,
        "contact_form_version": f"Quick note — I help local businesses get their first website. {name} could benefit from a simple site. Happy to share an example. Topher — topher@mixedmakershop.com",
        "social_dm_version": f"Hey! I noticed {name} doesn't have a website yet. I help local businesses get simple sites — would love to send a quick example if you're interested.",
        "follow_up_note": follow_up,
        "follow_up_line": f"Quick follow-up — still happy to send that website example for {name}.",
    }


def _outreach_weak_website(name: str, city: str, problems: list, pitch_lines: list) -> dict:
    """Outreach drafts from website investigation."""
    bullets = "\n".join(f"- {p}" for p in (problems or [])[:4])
    pitch_bullets = "\n".join(f"- {p}" for p in (pitch_lines or [])[:4])

    short = f"""Hi there,

My name is Topher and I run MixedMakerShop.

I came across {name} in {city} and had a quick idea for your website.

{problems[0] if problems else "Your site could be clearer on phones."}

I build simple modern websites for small businesses. Happy to send a quick example if you're interested.

Thanks,
Topher
topher@mixedmakershop.com
MixedMakerShop.com"""

    long_body = f"""Hi there,

My name is Topher and I run MixedMakerShop.

I came across {name} in {city} and had a quick idea that might help your website feel cleaner and easier to use on phones.
"""
    if bullets:
        long_body += f"\nA few things stood out:\n{bullets}\n\n"
    long_body += "I build simple modern websites, and I think a better version could:\n"
    long_body += pitch_bullets or "- create a cleaner mobile-friendly layout"
    long_body += """

Happy to send over a quick example. No pressure — just wanted to reach out.

Thanks,
Topher
topher@mixedmakershop.com
MixedMakerShop.com"""

    contact_form = f"Quick idea for {name}'s website — I help local businesses get cleaner, mobile-friendly sites. Would love to share an example if you're open to it. Topher — topher@mixedmakershop.com"
    social_dm = f"Hey! I run MixedMakerShop and help local businesses with websites. Came across {name} — would love to send a quick example of a cleaner mobile-friendly version if you're interested."
    follow_up = f"Following up — still happy to send that quick website example for {name} whenever works for you."

    return {
        "short_email": short,
        "longer_email": long_body,
        "contact_form_version": contact_form,
        "social_dm_version": social_dm,
        "follow_up_note": follow_up,
        "follow_up_line": follow_up,
    }


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
    case["address"] = place.get("address") or place.get("vicinity")
    case["distance_miles"] = place.get("distance_miles")
    case["phone"] = place.get("phone")
    case["website"] = None
    case["maps_link"] = place.get("maps_url")
    case["hours"] = place.get("hours")
    case["rating"] = place.get("rating")
    case["review_count"] = place.get("review_count")
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

    pack = _outreach_no_website(name, home_city)
    case["short_email"] = pack["short_email"]
    case["longer_email"] = pack["longer_email"]
    case["contact_form_version"] = pack["contact_form_version"]
    case["social_dm_version"] = pack["social_dm_version"]
    case["follow_up_note"] = pack["follow_up_note"]
    case["follow_up_line"] = pack["follow_up_line"]

    case["strongest_problems"] = ["No website — missing online presence"]
    case["strongest_pitch_angle"] = "Get your first simple website so customers can find you online"
    case["best_service_to_offer"] = "First website — simple, mobile-friendly, with hours and contact"
    case["best_demo_to_show"] = "Show example of similar local business site"
    case["demo_to_show"] = "Show example of similar local business site"
    case["why_worth_pursuing"] = f"{name} — independent business with no website. High intent for first-site build."
    case["why_this_lead_is_worth_pursuing"] = case["why_worth_pursuing"]
    case["what_stood_out"] = "No website"
    case["next_action"] = "Call or visit with short pitch"
    case["follow_up_suggestion"] = "Follow up in 3–5 days"
    case["internal_score"] = int(_score_no_website(case))
    case["priority"] = "high" if case["phone"] else "medium"
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
    case["address"] = place.get("address") or place.get("vicinity")
    case["distance_miles"] = place.get("distance_miles")
    case["phone"] = place.get("phone")
    case["website"] = website
    case["maps_link"] = place.get("maps_url")
    case["hours"] = place.get("hours")
    case["rating"] = place.get("rating")
    case["review_count"] = place.get("review_count")
    case["review_snippets"] = place.get("review_snippets") or []
    case["review_themes"] = place.get("review_themes") or []

    log.append(f"  Investigating: {website}")
    inv = None
    try:
        inv = investigate(website, crawl_internal=True, timeout=14)
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
        case["ssl_ok"] = inv.get("ssl_ok")
        case["internal_links_found"] = inv.get("internal_links_found") or {}
        case["important_internal_links"] = inv.get("important_internal_links") or inv.get("internal_links_found") or {}
        case["page_navigation_items"] = inv.get("page_navigation_items") or []
        case["navigation_items"] = inv.get("navigation_items") or inv.get("page_navigation_items") or []
        case["emails"] = inv.get("emails") or []
        case["phones"] = inv.get("phones") or []
        case["owner_names"] = inv.get("owner_names") or []
        case["contact_matrix"] = inv.get("contact_matrix") or {}
        case["discovered_pages"] = inv.get("discovered_pages") or []
        case["reservation_link"] = inv.get("reservation_link")
        case["order_link"] = inv.get("order_link")

        emails = inv.get("emails") or []
        social = inv.get("social") or {}
        case["email"] = emails[0] if emails else None
        case["contact_page"] = inv.get("contact_page")
        case["phone_from_site"] = (inv.get("phones") or [None])[0]
        case["facebook"] = social.get("facebook")
        case["instagram"] = social.get("instagram")
        case["linkedin"] = social.get("linkedin")
        case["social_links"] = social
        case["owner_manager_name"] = (case.get("owner_names") or [None])[0]

        problems = inv.get("problems") or []
        pitch_lines = inv.get("pitch") or []
        for line in inv.get("debug_log") or []:
            log.append(f"    {line}")
    else:
        problems = ["Website could not be fully investigated."]
        pitch_lines = ["Manual review recommended."]
        log.append("    website investigation: failed")

    case["strongest_problems"] = problems
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

    pack = _outreach_weak_website(name, home_city, problems, pitch_lines)
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
    case["internal_score"] = int(_score_weak_website(case))
    case["priority"] = "medium"

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
    radius = config.get("search_radius_miles", 25)
    categories = config.get("categories", ["coffee shop", "diner", "church"])
    max_per = max(1, config.get("max_results_per_category", 5))
    ignore_chains = config.get("ignore_chains", True)
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()

    print("Morning Runner — automated local client finder")
    print(f"  home_city: {home_city}, radius: {radius} mi")
    print(f"  categories: {categories}, max_per: {max_per}, ignore_chains: {ignore_chains}")
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
    try:
        places = _fetch_places(home_city, categories, max_per, radius, current_lat=current_lat, current_lng=current_lng)
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
