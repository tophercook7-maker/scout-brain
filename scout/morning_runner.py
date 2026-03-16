#!/usr/bin/env python3
"""
Morning Runner — automated one-button local client finder.

Prioritizes businesses with NO website, secondary lane for weak websites.
Uses config: home_city, search_radius_miles, categories, max_results_per_category, ignore_chains.
"""

import json
import math
import os
import sys
import builtins as _builtins
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, TimeoutError as FutureTimeoutError, wait
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urllib_error
from urllib import request as urllib_request

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
SCOUT_VERBOSE_LOGS = str(os.environ.get("SCOUT_VERBOSE_LOGS", "") or "").strip().lower() in {"1", "true", "yes", "on"}


def _should_emit_runner_log(message: str, stream) -> bool:
    if stream is sys.stderr:
        return True
    if SCOUT_VERBOSE_LOGS:
        return True
    msg = str(message or "")
    lowered = msg.lower()
    important_fragments = (
        "morning runner",
        "error:",
        "failed",
        "processed:",
        "saved valid case files:",
        "skipped invalid/incomplete leads:",
        "wrote ",
        "no website:",
        "weak website:",
        "total businesses discovered:",
    )
    return any(fragment in lowered for fragment in important_fragments)


def print(*args, **kwargs):  # type: ignore[override]
    message = " ".join(str(a) for a in args)
    stream = kwargs.get("file")
    if _should_emit_runner_log(message, stream):
        _builtins.print(*args, **kwargs)

CHAIN_CLUES = ["mcdonald", "starbucks", "subway", "dunkin", "walmart", "target", "chain", "franchise"]

# Template-like prefixes from mock data; skip these (not real Places results)
WEAK_NAME_PREFIXES = ("family ", "main street ", "local ", "downtown ")
DEFAULT_TARGET_INDUSTRIES = (
    "plumber,roofing,hvac,electrician,landscaping,cleaning service,pressure washing,"
    "auto repair,church,small restaurant"
)
DEFAULT_DISCOVERY_CATEGORIES = [
    "small restaurant",
    "plumber",
    "HVAC",
    "roofing",
    "electrician",
    "landscaping",
    "cleaning service",
    "pressure washing",
    "auto repair",
    "church",
]
DEFAULT_MULTI_CITY_SEQUENCE = [
    "Hot Springs",
    "Hot Springs Village",
    "Malvern",
    "Benton",
    "Bryant",
    "Little Rock",
    "Arkadelphia",
    "Sheridan",
]
ARKANSAS_CITIES_STATEWIDE = [
    "Little Rock",
    "Fort Smith",
    "Fayetteville",
    "Springdale",
    "Rogers",
    "Conway",
    "Jonesboro",
    "North Little Rock",
    "Bentonville",
    "Pine Bluff",
    "Hot Springs",
    "Texarkana",
    "Benton",
    "Sherwood",
    "Jacksonville",
    "Russellville",
    "Bryant",
    "Cabot",
    "Van Buren",
    "Searcy",
    "Hot Springs Village",
    "Malvern",
    "Arkadelphia",
    "El Dorado",
    "Magnolia",
    "Mountain Home",
    "Harrison",
    "Batesville",
    "Forrest City",
    "Paragould",
    "West Memphis",
    "Stuttgart",
    "Camden",
    "Hope",
    "Clarksville",
    "Monticello",
    "Helena",
    "Dumas",
    "Morrilton",
    "Greenwood",
]
ARKANSAS_REGION_CITIES = {
    "northwest": [
        "Fayetteville",
        "Springdale",
        "Rogers",
        "Bentonville",
        "Harrison",
        "Mountain Home",
    ],
    "central": [
        "Little Rock",
        "North Little Rock",
        "Conway",
        "Benton",
        "Sherwood",
        "Jacksonville",
        "Bryant",
        "Cabot",
        "Searcy",
        "Morrilton",
    ],
    "river_valley": [
        "Fort Smith",
        "Van Buren",
        "Russellville",
        "Clarksville",
        "Greenwood",
    ],
    "delta": [
        "Jonesboro",
        "Forrest City",
        "Paragould",
        "West Memphis",
        "Helena",
        "Stuttgart",
    ],
    "south": [
        "Pine Bluff",
        "Texarkana",
        "El Dorado",
        "Magnolia",
        "Camden",
        "Hope",
        "Monticello",
        "Dumas",
    ],
    "ouachita": [
        "Hot Springs",
        "Hot Springs Village",
        "Malvern",
        "Arkadelphia",
        "Batesville",
    ],
}
HOT_SPRINGS_NEARBY_CITIES = [
    "Hot Springs Village",
    "Malvern",
    "Benton",
    "Bryant",
    "Little Rock",
    "Arkadelphia",
    "Sheridan",
]

LOW_PRIORITY_INDUSTRIES = {
    "law firms",
    "marketing agencies",
    "software companies",
    "consultants",
    "large franchises",
}

HIGH_CLOSE_CATEGORIES = {
    "dentists",
    "chiropractors",
    "restaurants",
    "cafes",
    "gyms",
    "hair salons",
    "auto repair",
    "mechanics",
    "body shops",
    "tire shops",
    "plumbers",
    "roofing",
    "electricians",
    "landscaping",
    "cleaning services",
    "pressure washing",
    "contractors",
}
PRIORITY_INDUSTRY_CATEGORIES = {
    "roofing",
    "hvac",
    "electricians",
    "plumbers",
    "landscaping",
    "cleaning services",
    "pressure washing",
    "auto repair",
    "mechanics",
    "churches",
    "restaurants",
}
EASY_CLOSE_CATEGORIES = {
    "plumbers",
    "roofing",
    "hvac",
    "electricians",
    "landscaping",
    "cleaning services",
    "pressure washing",
    "auto repair",
    "mechanics",
    "churches",
    "restaurants",
    "cafes",
}


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


def _preferred_industry_terms() -> list[str]:
    raw = (
        os.environ.get("SCOUT_TARGET_INDUSTRIES")
        or DEFAULT_TARGET_INDUSTRIES
    )
    return [part.strip().lower() for part in str(raw).split(",") if part.strip()]


def _normalize_industry(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    aliases = {
        "restaurant": "restaurants",
        "restaurants": "restaurants",
        "cafe": "cafes",
        "cafes": "cafes",
        "coffee shop": "cafes",
        "bakery": "bakeries",
        "bakeries": "bakeries",
        "plumber": "plumbers",
        "plumbers": "plumbers",
        "hvac": "hvac",
        "hvac service": "hvac",
        "hvac contractor": "hvac",
        "heating and air": "hvac",
        "heating and cooling": "hvac",
        "electrician": "electricians",
        "electricians": "electricians",
        "roofing contractor": "roofing",
        "roofing contractors": "roofing",
        "roofer": "roofing",
        "roofing": "roofing",
        "auto repair": "auto repair",
        "mechanic": "mechanics",
        "mechanics": "mechanics",
        "body shop": "body shops",
        "body shops": "body shops",
        "tire shop": "tire shops",
        "tire shops": "tire shops",
        "landscaper": "landscaping",
        "landscaping": "landscaping",
        "cleaning service": "cleaning services",
        "cleaning services": "cleaning services",
        "pressure washing": "pressure washing",
        "pressure washer": "pressure washing",
        "pressure washers": "pressure washing",
        "boutique": "boutiques",
        "boutiques": "boutiques",
        "florist": "florists",
        "florists": "florists",
        "hair salon": "hair salons",
        "hair salons": "hair salons",
        "salon": "hair salons",
        "dentist": "dentists",
        "dentists": "dentists",
        "chiropractor": "chiropractors",
        "chiropractors": "chiropractors",
        "small law firm": "law firms",
        "lawyer": "law firms",
        "law firms": "law firms",
        "med spa": "med spa",
        "medical spa": "med spa",
        "med spas": "med spa",
        "medical spas": "med spa",
        "marketing agency": "marketing agencies",
        "marketing agencies": "marketing agencies",
        "software company": "software companies",
        "software companies": "software companies",
        "consultant": "consultants",
        "consultants": "consultants",
        "franchise": "large franchises",
        "large franchise": "large franchises",
        "large franchises": "large franchises",
        "church": "churches",
        "churches": "churches",
        "small restaurant": "restaurants",
        "local retail shops": "local retail shops",
        "retail": "local retail shops",
    }
    return aliases.get(text, text)


def _industry_is_preferred(value: str) -> bool:
    normalized = _normalize_industry(value)
    preferred = {_normalize_industry(term) for term in _preferred_industry_terms()}
    if not normalized:
        return False
    return normalized in preferred


def _industry_is_lower_priority(value: str) -> bool:
    normalized = _normalize_industry(value)
    return normalized in {_normalize_industry(v) for v in LOW_PRIORITY_INDUSTRIES}


def _industry_is_high_close_probability(value: str) -> bool:
    normalized = _normalize_industry(value)
    if normalized in HIGH_CLOSE_CATEGORIES:
        return True
    return "contractor" in normalized


def _is_easy_close_category(value: str) -> bool:
    normalized = _normalize_industry(value)
    if not normalized:
        return False
    return normalized in EASY_CLOSE_CATEGORIES


def _derive_easy_target_reasons(lead: dict, website_quality: dict | None = None) -> list[str]:
    website_quality = website_quality or {}
    website = str(lead.get("website") or "").strip()
    facebook_url = str(lead.get("facebook") or lead.get("facebook_url") or "").strip()
    has_website = bool(website) and not bool(lead.get("no_website"))
    fetch_ok = lead.get("fetch_ok")
    ssl_ok = lead.get("ssl_ok")
    viewport_ok = lead.get("viewport_ok")
    mobile_score = _as_int(lead.get("mobile_score"), 100)
    contact_page = str(lead.get("contact_page") or "").strip()
    contact_form_present = bool(lead.get("contact_form_present"))
    contact_page_present = bool(contact_page or contact_form_present)
    contact_depth = _as_int(lead.get("contact_link_depth"), 1)
    website_status = str(website_quality.get("website_status") or lead.get("website_status") or "").strip().lower()
    reasons: list[str] = []
    if not has_website and facebook_url:
        reasons.append("Facebook-only presence")
    elif not has_website:
        reasons.append("No website found")
    if has_website and (fetch_ok is False or website_status in {"unreachable", "broken_website"}):
        reasons.append("Website unreachable")
    if has_website and (website.lower().startswith("http://") or ssl_ok is False):
        reasons.append("Website uses insecure HTTP")
    if has_website and (not contact_page_present or contact_depth >= 3):
        reasons.append("Contact page missing")
    if has_website and (viewport_ok is False or mobile_score < 50):
        reasons.append("Mobile layout broken")
    return list(dict.fromkeys(reasons))


def _resolve_discovery_categories(config: dict) -> list[str]:
    configured = config.get("categories", DEFAULT_DISCOVERY_CATEGORIES)
    configured_list = [str(c).strip() for c in configured if str(c).strip()]
    preferred = [term for term in _preferred_industry_terms() if term]
    ordered: list[str] = []
    seen: set[str] = set()

    # Preferred industries first.
    for term in preferred:
        norm = _normalize_industry(term)
        if not norm:
            continue
        if norm not in seen:
            ordered.append(term)
            seen.add(norm)

    # Include any configured categories not already covered.
    for category in configured_list:
        norm = _normalize_industry(category)
        if not norm:
            continue
        if norm not in seen:
            ordered.append(category)
            seen.add(norm)

    return ordered or configured_list


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_miles = 3958.8
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(d_lng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_miles * c


def _quick_site_precheck(url: str, timeout: int = 3) -> dict:
    result = {
        "unreachable": False,
        "http_status": None,
        "ssl_issue": str(url or "").strip().lower().startswith("http://"),
    }
    target = str(url or "").strip()
    if not target:
        result["unreachable"] = True
        return result
    try:
        req = urllib_request.Request(
            target,
            headers={"User-Agent": "Mozilla/5.0 Scout-Brain/1.0"},
            method="GET",
        )
        with urllib_request.urlopen(req, timeout=max(1, timeout)) as resp:
            result["http_status"] = int(getattr(resp, "status", 200) or 200)
    except urllib_error.HTTPError as e:
        result["http_status"] = int(getattr(e, "code", 0) or 0)
        result["unreachable"] = result["http_status"] >= 400 or result["http_status"] == 0
    except Exception:
        result["unreachable"] = True
    return result


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
    city_radius = float(os.environ.get("SCOUT_CITY_RADIUS", "80") or "80")

    explicit_targets = config.get("target_cities") or []
    requested_region = str(os.environ.get("SCOUT_REGION_OVERRIDE", "") or "").strip().lower()
    if requested_region == "all_arkansas":
        explicit_targets = [{"city_name": city, "state": "AR"} for city in ARKANSAS_CITIES_STATEWIDE]
    elif requested_region in ARKANSAS_REGION_CITIES:
        explicit_targets = [
            {"city_name": city, "state": "AR"} for city in ARKANSAS_REGION_CITIES.get(requested_region, [])
        ]
    if not explicit_targets and bool(config.get("scan_statewide_arkansas", False)):
        explicit_targets = [{"city_name": city, "state": "AR"} for city in ARKANSAS_CITIES_STATEWIDE]
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

    nearby_seed = str(config.get("nearby_city_seed") or "").strip()
    if not selected and home_city.strip().lower().startswith("hot springs"):
        for city_name in DEFAULT_MULTI_CITY_SEQUENCE:
            row = next((r for r in dataset if str(r.get("city_name") or "").strip().lower() == city_name.lower()), None)
            selected.append(
                row
                or {
                    "city_name": city_name,
                    "state": "AR",
                    "latitude": None,
                    "longitude": None,
                    "population": 0,
                }
            )

    if not selected:
        max_cities = max(1, int(config.get("max_cities_per_run", 5)))
        ranked = sorted(dataset, key=lambda r: int(r.get("population") or 0), reverse=True)
        selected = ranked[:max_cities]

    # Expand nearby cities when home city is Hot Springs (legacy behavior).
    if home_city.strip().lower().startswith("hot springs"):
        seed = None
        for row in selected:
            if str(row.get("city_name") or "").strip().lower() == "hot springs":
                seed = row
                break
        if seed is None:
            seed = next((r for r in dataset if str(r.get("city_name") or "").strip().lower() == "hot springs"), None)
        seen = {str(r.get("city_name") or "").strip().lower() for r in selected}
        for city_name in HOT_SPRINGS_NEARBY_CITIES:
            match = next((r for r in dataset if str(r.get("city_name") or "").strip().lower() == city_name.lower()), None)
            if (
                match
                and seed
                and seed.get("latitude") is not None
                and seed.get("longitude") is not None
                and match.get("latitude") is not None
                and match.get("longitude") is not None
            ):
                try:
                    distance = _haversine_miles(
                        float(seed.get("latitude")),
                        float(seed.get("longitude")),
                        float(match.get("latitude")),
                        float(match.get("longitude")),
                    )
                    if distance > city_radius:
                        continue
                except Exception:
                    pass
            if city_name.lower() in seen:
                continue
            selected.append(
                match
                or {
                    "city_name": city_name,
                    "state": "AR",
                    "latitude": None,
                    "longitude": None,
                    "population": 0,
                }
            )
            seen.add(city_name.lower())

    # Generic nearby-city expansion when explicitly requested by scan settings.
    if nearby_seed:
        seed = next((r for r in dataset if str(r.get("city_name") or "").strip().lower() == nearby_seed.lower()), None)
        if seed:
            seed_lat = seed.get("latitude")
            seed_lng = seed.get("longitude")
            seen = {str(r.get("city_name") or "").strip().lower() for r in selected}
            candidates: list[tuple[float, dict]] = []
            for row in dataset:
                city_name = str(row.get("city_name") or "").strip()
                if not city_name or city_name.lower() in seen or city_name.lower() == nearby_seed.lower():
                    continue
                if seed_lat is None or seed_lng is None or row.get("latitude") is None or row.get("longitude") is None:
                    continue
                try:
                    distance = _haversine_miles(float(seed_lat), float(seed_lng), float(row.get("latitude")), float(row.get("longitude")))
                except Exception:
                    continue
                if distance <= city_radius:
                    candidates.append((distance, row))
            candidates.sort(key=lambda item: item[0])
            max_cities = max(1, int(config.get("max_cities_per_run", 8)))
            if not any(str(r.get("city_name") or "").strip().lower() == nearby_seed.lower() for r in selected):
                selected.insert(0, seed)
            for _, row in candidates:
                if len(selected) >= max_cities:
                    break
                selected.append(row)

    return selected


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


def _calculate_base_business_score(lead: dict) -> tuple[int, list[str]]:
    score = 35.0
    signals: list[str] = []
    rating = _as_float(lead.get("rating"), 0.0)
    review_count = _as_int(lead.get("review_count"), 0)
    business_closed = str(lead.get("business_status") or "").strip().lower() in {"closed", "permanently_closed"}

    if rating > 4.3:
        score += 16
        signals.append("+16 rating above 4.3")
    elif rating >= 4.2:
        score += 12
        signals.append("+12 strong review rating")
    if review_count >= 80:
        score += 18
        signals.append("+18 very strong review volume")
    elif review_count > 40:
        score += 14
        signals.append("+14 reviews above 40")
    elif review_count >= 20:
        score += 6
        signals.append("+6 moderate review volume")
    if lead.get("phone") or lead.get("email") or lead.get("contact_page"):
        score += 6
        signals.append("+6 reachable contact available")
    industry_value = lead.get("category") or lead.get("industry") or ""
    if _industry_is_preferred(industry_value):
        score += 20
        signals.append("+20 preferred industry")
    if _industry_is_high_close_probability(industry_value):
        score += 18
        signals.append("+18 category depends on new customers")
    if _industry_is_lower_priority(industry_value):
        score -= 20
        signals.append("-20 lower-priority industry")
    business_name = str(lead.get("business_name") or lead.get("name") or "").strip().lower()
    if any(token in business_name for token in ["franchise", "group", "corporate"]):
        score -= 12
        signals.append("-12 likely larger chain/franchise fit")

    distance = _as_float(lead.get("distance_miles"), 9999.0)
    if distance <= 8:
        score += 5
        signals.append("+5 close local proximity")

    if rating > 0 and rating < 3.5:
        score -= 15
        signals.append("-15 low review rating")
    if business_closed:
        score -= 60
        signals.append("-60 business closed")

    return max(0, min(100, int(round(score)))), signals


def calculateWebsiteQualityScore(lead: dict) -> dict:
    print("website quality check started")
    issues: list[str] = []
    boosts: list[str] = []
    website_quality_score = 0

    website = str(lead.get("website") or "").strip()
    facebook_url = str(lead.get("facebook_url") or lead.get("facebook") or "").strip()
    instagram_url = str(lead.get("instagram_url") or lead.get("instagram") or "").strip()
    contact_page = str(lead.get("contact_page") or "").strip()
    email = str(lead.get("email") or "").strip()
    phone = str(lead.get("phone") or "").strip()
    if not facebook_url:
        print("facebook_url missing, continuing without social link")
    has_website = bool(website) and not bool(lead.get("no_website"))
    fetch_ok = lead.get("fetch_ok")
    ssl_ok = lead.get("ssl_ok")
    viewport_ok = lead.get("viewport_ok")
    homepage_title = str(lead.get("homepage_title") or "").strip()
    meta_description = str(lead.get("meta_description") or "").strip()
    missing_meta_title = bool(lead.get("missing_meta_title")) or not bool(homepage_title)
    missing_meta_description = bool(lead.get("missing_meta_description")) or not bool(meta_description)
    homepage_load_seconds = lead.get("homepage_load_seconds")
    load_seconds = _as_float(homepage_load_seconds, 0.0) if homepage_load_seconds is not None else None
    text_content_length = _as_int(lead.get("text_content_length"), 0) if lead.get("text_content_length") is not None else None
    image_count = _as_int(lead.get("image_count"), 0) if lead.get("image_count") is not None else None
    broken_links_count = _as_int(lead.get("broken_links_count"), 0) if lead.get("broken_links_count") is not None else 0
    platform_used = str(lead.get("platform_used") or "").strip().lower()
    category = _normalize_industry(lead.get("category") or lead.get("industry") or "")
    has_contact_path = bool(
        phone
        or email
        or contact_page
        or bool(lead.get("tap_to_call_present"))
        or bool(lead.get("contact_form_present"))
        or facebook_url
        or instagram_url
    )

    facebook_only = bool((has_website and "facebook.com" in website.lower()) or ((not has_website) and facebook_url))
    no_website = (not has_website) and (not facebook_only)
    unreachable = has_website and (fetch_ok is False)
    very_slow = bool(load_seconds is not None and load_seconds > 3.5)
    no_mobile = viewport_ok is False
    no_ssl = bool(has_website and ((ssl_ok is False) or website.lower().startswith("http://")))
    missing_seo_basics = missing_meta_title or missing_meta_description
    poor_seo = missing_seo_basics or broken_links_count > 0 or bool(text_content_length is not None and text_content_length < 300)
    missing_contact = not has_contact_path
    contact_page_missing = not bool(contact_page) and not bool(lead.get("contact_form_present"))
    outdated_cms = any(token in platform_used for token in ["weebly", "editmysite", "joomla", "drupal 7", "magento 1"])
    outdated_design = bool(lead.get("outdated_design_clues"))
    outdated_wordpress_theme = bool("wordpress" in platform_used and outdated_design)
    builder_lock_in = any(token in platform_used for token in ["wix", "godaddy"])
    very_low_text = bool(text_content_length is not None and text_content_length < 300)
    missing_images = bool(image_count is not None and image_count == 0)
    broken_links = broken_links_count > 0
    broken_layout = bool(
        broken_links
        or str(lead.get("menu_visibility") or "").strip().lower() in {"false", "0", "none"}
        or str(lead.get("hours_visibility") or "").strip().lower() in {"false", "0", "none"}
    )
    is_restaurant = category in {"restaurants", "cafes", "bakeries"}
    is_service_business = category in {
        "plumbers",
        "electricians",
        "roofing",
        "landscaping",
        "cleaning services",
        "pressure washing",
        "auto repair",
        "mechanics",
        "body shops",
        "tire shops",
    }
    missing_online_ordering = bool(
        is_restaurant
        and has_website
        and not str(lead.get("order_link") or "").strip()
        and not str(lead.get("reservation_link") or "").strip()
    )
    missing_booking = bool(
        is_service_business
        and has_website
        and not str(lead.get("reservation_link") or "").strip()
        and not bool(lead.get("contact_form_present"))
    )

    if no_website:
        website_quality_score += 100
        issues.append("No website found")
        boosts.append("+100 no website")
    if facebook_only:
        website_quality_score += 88
        issues.append("Facebook-only presence")
        boosts.append("+88 facebook-only presence")
    if unreachable:
        website_quality_score += 95
        issues.append("Website does not load")
        boosts.append("+95 website unreachable")
    if no_ssl:
        website_quality_score += 85
        issues.append("Website uses insecure HTTP")
        boosts.append("+85 missing HTTPS")
    if contact_page_missing:
        website_quality_score += 75
        issues.append("Contact page is missing")
        boosts.append("+75 contact page missing")
    if no_mobile:
        website_quality_score += 75
        issues.append("Mobile layout is hard to use")
        boosts.append("+75 mobile layout broken")
    if very_slow:
        website_quality_score += 8
        issues.append("Homepage loads slowly")
        boosts.append("+8 slow load")
    if poor_seo:
        website_quality_score += 4
        issues.append("missing SEO title/description")
        boosts.append("+4 poor SEO")
    if missing_contact:
        website_quality_score += 6
        issues.append("Contact info is hard to find")
        boosts.append("+6 missing contact info")
    if missing_online_ordering:
        website_quality_score += 24
        issues.append("no booking or ordering system")
        boosts.append("+24 missing online ordering")
    if missing_booking:
        website_quality_score += 22
        issues.append("no booking or ordering system")
        boosts.append("+22 missing booking")
    if outdated_wordpress_theme:
        website_quality_score += 18
        issues.append("outdated WordPress theme")
        boosts.append("+18 outdated WordPress theme")
    if builder_lock_in:
        website_quality_score += 14
        issues.append("Wix or GoDaddy builder")
        boosts.append("+14 builder lock-in")
    if broken_layout:
        website_quality_score += 20
        issues.append("difficult navigation")
        boosts.append("+20 broken layout")
    if outdated_cms or outdated_design:
        issues.append("Site looks outdated")
    if very_low_text:
        issues.append("text heavy homepage")
    if missing_images:
        issues.append("images not optimized")
    if broken_links:
        issues.append("broken links detected")

    for issue in issues:
        print(f"website issue detected: {issue}")

    seo_score = 100
    if missing_meta_title:
        seo_score -= 25
    if missing_meta_description:
        seo_score -= 25
    if very_low_text:
        seo_score -= 20
    if missing_images:
        seo_score -= 15
    if broken_links:
        seo_score -= 15
    seo_score = max(0, min(100, seo_score))

    website_quality_score = max(0, min(100, int(round(website_quality_score))))
    if no_website:
        website_status = "no_website"
    elif facebook_only:
        website_status = "facebook_only"
    elif unreachable:
        website_status = "broken_website"
    elif no_ssl:
        website_status = "http_only"
    elif contact_page_missing:
        website_status = "missing_contact_page"
    elif no_mobile:
        website_status = "mobile_layout_issue"
    elif outdated_cms or outdated_design:
        website_status = "outdated_website"
    else:
        website_status = "unclear"

    result = {
        "website_status": website_status,
        "website_speed": round(load_seconds, 2) if load_seconds is not None else None,
        "mobile_ready": not no_mobile,
        "seo_score": seo_score,
        "website_quality_score": website_quality_score,
        "website_issues": issues,
        "website_boost_signals": boosts,
        "missing_contact_info": missing_contact,
        "contact_page_missing": contact_page_missing,
        "outdated_cms_indicators": outdated_cms or outdated_design,
        "missing_online_ordering": missing_online_ordering,
        "missing_booking_system": missing_booking,
        "facebook_only": facebook_only,
        "outdated_wordpress_theme": outdated_wordpress_theme,
        "builder_lock_in": builder_lock_in,
        "broken_layout": broken_layout,
    }
    easy_target_reasons = _derive_easy_target_reasons(lead, result)
    result["easy_target_reasons"] = easy_target_reasons
    result["easy_target"] = bool(easy_target_reasons)
    print(
        "website scoring applied: "
        f"status={website_status}, quality={website_quality_score}, seo={seo_score}, easy_target={bool(easy_target_reasons)}"
    )
    return result


def _derive_opportunity_reason(website_quality: dict, lead: dict) -> list[str]:
    easy_reasons = _derive_easy_target_reasons(lead, website_quality)
    if easy_reasons:
        return easy_reasons[:3]
    return ["No immediate breakage detected"]


def _derive_close_probability(
    total_score: int,
    website_quality: dict,
    lead: dict,
) -> str:
    rating = _as_float(lead.get("rating"), 0.0)
    review_count = _as_int(lead.get("review_count"), 0)
    industry_value = lead.get("category") or lead.get("industry") or ""
    weak_design = bool(lead.get("outdated_design_clues")) or bool(website_quality.get("website_status") == "weak")
    missing_capture = bool(
        website_quality.get("missing_online_ordering")
        or website_quality.get("missing_booking_system")
        or not bool(lead.get("contact_form_present"))
        or not bool(lead.get("order_link"))
    )
    strong_reviews = review_count > 40
    strong_rating = rating > 4.3
    high_close_category = _industry_is_high_close_probability(industry_value)

    buyer_signals = sum(
        [
            1 if weak_design else 0,
            1 if high_close_category else 0,
            1 if strong_reviews else 0,
            1 if strong_rating else 0,
            1 if missing_capture else 0,
            1 if bool(lead.get("outdated_design_clues")) else 0,
        ]
    )
    if total_score >= 85 or buyer_signals >= 4:
        return "high"
    if total_score >= 60 or buyer_signals >= 2:
        return "medium"
    return "low"


def _score_to_lead_bucket(score: int) -> str:
    if score >= 90:
        return "Easy Win"
    if score >= 75:
        return "High Value"
    if score >= 60:
        return "Good Prospect"
    if score >= 40:
        return "Needs Review"
    return "Low Priority"


def _derive_lead_assessment(case: dict) -> dict:
    website = str(case.get("website") or "").strip().lower()
    reasons = case.get("opportunity_reason") or []
    reason_text = " | ".join([str(v or "").strip() for v in reasons if str(v or "").strip()]).lower()
    website_status = str(case.get("website_status") or "").strip().lower()
    no_website = bool(case.get("no_website")) or not bool(website)
    broken_website = website_status == "unreachable" or "unreachable" in reason_text
    insecure_http = website.startswith("http://") or "insecure http" in reason_text or bool(case.get("ssl_ok") is False)
    missing_contact_page = (
        not bool(str(case.get("contact_page") or "").strip()) and not bool(case.get("contact_form_present"))
    ) or "contact page missing" in reason_text
    mobile_issue = bool(case.get("viewport_ok") is False) or _as_int(case.get("mobile_score"), 100) < 55 or "mobile layout" in reason_text
    slow_site = _as_float(case.get("homepage_load_seconds"), 0.0) > 4.0 or "loads slowly" in reason_text
    outdated = bool(case.get("outdated_design_clues"))
    reviews_last_30 = _as_int(case.get("reviews_last_30_days"), 0)
    review_count = _as_int(case.get("google_review_count"), _as_int(case.get("review_count"), 0))
    active_business = (
        reviews_last_30 > 3
        or review_count > 50
        or bool(case.get("owner_post_detected"))
        or bool(case.get("new_photos_detected"))
        or bool(case.get("listing_recently_updated"))
    )
    score = _as_int(case.get("opportunity_score"), 0)
    lead_bucket = _score_to_lead_bucket(score)

    is_church = "church" in str(case.get("category") or case.get("industry") or "").lower()
    lead_type = "Needs Review"
    if no_website or broken_website or insecure_http or missing_contact_page:
        lead_type = "Easy Win"
    elif is_church and (mobile_issue or slow_site or outdated):
        lead_type = "Church Website Opportunity"
    elif active_business and (mobile_issue or slow_site or outdated):
        lead_type = "Active Business, Weak Website"
    elif score < 50:
        lead_type = "Low Priority"

    close_probability = str(case.get("close_probability") or "medium").lower()
    if lead_type == "Easy Win":
        close_probability = "high"
    elif lead_type == "Low Priority":
        close_probability = "low"
    elif close_probability not in {"low", "medium", "high"}:
        close_probability = "medium"

    if no_website:
        best_pitch = "Looks like you may not currently have a website."
    elif broken_website:
        best_pitch = "I noticed your site may not be loading correctly."
    elif insecure_http:
        best_pitch = "Your website appears to use insecure HTTP instead of HTTPS."
    elif missing_contact_page:
        best_pitch = "Your contact page appears to be missing, which can cost leads."
    elif mobile_issue:
        best_pitch = "Your mobile layout could make it harder for customers to contact you."
    elif slow_site:
        best_pitch = "Homepage loads slowly, which can reduce calls and form submissions."
    else:
        best_pitch = "Your business has strong reviews but the website may be holding you back."

    if str(case.get("email") or "").strip():
        best_contact_method = "email"
    elif str(case.get("contact_page") or case.get("contact_form_url") or "").strip():
        best_contact_method = "contact_page"
    elif str(case.get("phone_from_site") or case.get("phone") or "").strip():
        best_contact_method = "phone"
    elif str(case.get("facebook_url") or case.get("facebook") or "").strip():
        best_contact_method = "facebook"
    else:
        best_contact_method = "phone"

    if lead_type == "Low Priority":
        recommended_next_action = "Skip For Now"
    elif not bool(str(case.get("email") or case.get("phone_from_site") or case.get("phone") or case.get("contact_page") or "").strip()):
        recommended_next_action = "Review Site Manually"
    elif str(case.get("status") or "new").strip().lower() in {"new", "new_lead"}:
        recommended_next_action = "Send First Touch"
    else:
        recommended_next_action = "Generate Email"

    return {
        "lead_bucket": lead_bucket,
        "lead_type": lead_type,
        "close_probability": close_probability,
        "best_contact_method": best_contact_method,
        "best_pitch_angle": best_pitch,
        "recommended_next_action": recommended_next_action,
    }


def calculateOpportunityScore(lead: dict) -> tuple[int, list[str], str, dict, list[str], str]:
    """
    Opportunity score engine (0-100).
    Returns: (score, scoring_signals, lead_tier, website_quality_payload, opportunity_reason_list, close_probability).
    """
    base_score, base_signals = _calculate_base_business_score(lead)
    website_quality = calculateWebsiteQualityScore(lead)
    website_issue_score = int(website_quality.get("website_quality_score") or 0)
    # Weighted scoring model requested by product requirements.
    website_issues_weight = 40
    review_activity_weight = 30
    industry_weight = 20
    contact_info_weight = 10

    review_count = _as_int(lead.get("google_review_count"), _as_int(lead.get("review_count"), 0))
    rating = _as_float(lead.get("rating"), 0.0)
    reviews_last_30_days = _as_int(lead.get("reviews_last_30_days"), 0)
    recent_review_detected = reviews_last_30_days > 0
    owner_post_detected = bool(lead.get("owner_post_detected"))
    new_photos_detected = bool(lead.get("new_photos_detected"))
    listing_recently_updated = bool(lead.get("listing_recently_updated"))
    website_exists = bool(str(lead.get("website") or "").strip()) and not bool(lead.get("no_website"))
    website_grade = _as_int(lead.get("website_score"), 100)
    mobile_performance = _as_int(lead.get("mobile_score"), 100)
    contact_page_present = bool(str(lead.get("contact_page") or "").strip()) or bool(lead.get("contact_form_present"))
    phone_or_email_detected = bool(str(lead.get("phone") or "").strip() or str(lead.get("email") or "").strip())
    normalized_category = _normalize_industry(lead.get("category") or lead.get("industry") or "")

    website_status = str(website_quality.get("website_status") or lead.get("website_status") or "").strip().lower()
    business_activity_score = 0
    if review_count >= 100:
        business_activity_score = 40
    elif review_count >= 50:
        business_activity_score = 30
    elif review_count >= 20:
        business_activity_score = 20
    elif review_count >= 10:
        business_activity_score = 10

    website_problem_score = 0
    if website_status == "no_website":
        website_problem_score = 40
    elif website_status == "broken_website":
        website_problem_score = 35
    elif website_status == "facebook_only":
        website_problem_score = 30
    elif website_status == "http_only":
        website_problem_score = 25
    elif website_status == "missing_contact_page":
        website_problem_score = 20
    elif website_status == "mobile_layout_issue":
        website_problem_score = 15
    elif website_status == "outdated_website":
        website_problem_score = 15
    elif bool(homepage_load_seconds := lead.get("homepage_load_seconds")) and _as_float(homepage_load_seconds, 0.0) > 4:
        website_problem_score = 10
    elif website_issue_score >= 30:
        website_problem_score = 10

    activity_score = 0
    activity_summary: list[str] = []
    if reviews_last_30_days > 3:
        activity_score += 30
        activity_summary.append(f"{reviews_last_30_days} new reviews this month")
    elif reviews_last_30_days > 0:
        activity_score += 12
        activity_summary.append(f"{reviews_last_30_days} new reviews this month")
    if owner_post_detected:
        activity_score += 20
        activity_summary.append("Owner posted update recently")
    if new_photos_detected:
        activity_score += 15
        activity_summary.append("New photos detected")
    if listing_recently_updated:
        activity_score += 20
        activity_summary.append("Listing recently updated")
    activity_score = max(0, min(100, activity_score))

    contact_info_score = 0
    if bool(str(lead.get("email") or "").strip()):
        contact_info_score = 20
    elif contact_page_present:
        contact_info_score = 15
    elif bool(str(lead.get("phone") or "").strip() or str(lead.get("phone_from_site") or "").strip()):
        contact_info_score = 10
    elif bool(str(lead.get("facebook") or lead.get("facebook_url") or "").strip()):
        contact_info_score = 5

    total = max(0, min(100, int(round(business_activity_score + website_problem_score + contact_info_score))))

    if website_exists:
        base_signals.append("+website exists")
    if website_exists and website_grade < 70:
        base_signals.append("+weak website quality")
    if mobile_performance < 65:
        base_signals.append("+mobile performance issues")
    if contact_page_present:
        base_signals.append("+contact page present")
    if phone_or_email_detected:
        base_signals.append("+phone or email detected")
    if recent_review_detected:
        base_signals.append("+recent review detected")
    base_signals.append(f"+business activity score {business_activity_score}")
    base_signals.append(f"+website problem score {website_problem_score}")
    base_signals.append(f"+contact availability score {contact_info_score}")

    score_signals = list(base_signals) + list(website_quality.get("website_boost_signals") or [])
    if total >= 80:
        tier = "hot_lead"
    elif total >= 60:
        tier = "warm_lead"
    else:
        tier = "low_priority"
    opportunity_reason = _derive_opportunity_reason(website_quality, lead)
    print(f"opportunity_reason generated: {' | '.join(opportunity_reason)}")
    easy_target_reasons = _derive_easy_target_reasons(lead, website_quality)
    is_easy_target = bool(easy_target_reasons)
    website_quality["easy_target"] = is_easy_target
    website_quality["easy_target_reasons"] = easy_target_reasons
    if activity_summary:
        website_quality["activity_summary"] = list(dict.fromkeys(activity_summary))[:4]
    else:
        website_quality["activity_summary"] = []
    website_quality["activity_score"] = activity_score
    website_quality["reviews_last_30_days"] = reviews_last_30_days
    website_quality["google_review_count"] = review_count
    website_quality["owner_post_detected"] = owner_post_detected
    website_quality["new_photos_detected"] = new_photos_detected
    website_quality["listing_recently_updated"] = listing_recently_updated
    close_probability = _derive_close_probability(total, website_quality, lead)
    if not is_easy_target:
        total = min(total, 59)
        close_probability = "low"
        base_signals.append("downranked: non-easy target")
    else:
        total = max(total, 80)
    if total >= 80:
        tier = "hot_lead"
    elif total >= 60:
        tier = "warm_lead"
    else:
        tier = "low_priority"
    print(
        "opportunity score updated: "
        f"base={base_score}, website_issue_score={website_issue_score}, total={total}, "
        f"business_activity={business_activity_score}, website_problem={website_problem_score}, "
        f"contact_availability={contact_info_score}, activity_score={activity_score}, tier={tier}, close_probability={close_probability}"
    )
    return total, score_signals, tier, website_quality, opportunity_reason, close_probability


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
    reviews_last_30_days = _as_int(caseData.get("reviews_last_30_days"), 0)
    if reviews_last_30_days > 0:
        signals.append(f"{reviews_last_30_days} recent reviews")
    if bool(caseData.get("owner_post_detected")):
        signals.append("Owner posted update recently")
    if bool(caseData.get("new_photos_detected")):
        signals.append("New photos detected")

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
    case["google_review_count"] = place.get("google_review_count") or place.get("review_count")
    case["reviews_last_30_days"] = int(place.get("reviews_last_30_days") or 0)
    case["owner_post_detected"] = bool(place.get("owner_post_detected"))
    case["new_photos_detected"] = bool(place.get("new_photos_detected"))
    case["listing_recently_updated"] = bool(place.get("listing_recently_updated"))
    case["activity_summary"] = list(place.get("activity_summary") or [])
    case["business_status"] = place.get("business_status")
    case["review_snippets"] = place.get("review_snippets") or []
    case["review_themes"] = place.get("review_themes") or []

    case["email"] = None
    case["contact_page"] = None
    case["contact_form_url"] = None
    case["phone_from_site"] = None
    case["facebook"] = None
    case["facebook_url"] = None
    case["instagram"] = None
    case["instagram_url"] = None
    case["linkedin"] = None
    case["social_links"] = {}
    case["emails"] = []
    case["phones"] = [place["phone"]] if place.get("phone") else []

    if case["email"]:
        case["recommended_contact_method"] = "Email"
        case["backup_contact_method"] = "Contact page"
    elif case["contact_page"] or case["contact_form_url"]:
        case["recommended_contact_method"] = "Contact page"
        case["backup_contact_method"] = "Phone" if case["phone"] else None
    elif case["phone"]:
        case["recommended_contact_method"] = "Phone"
        case["backup_contact_method"] = "Maps / visit"
    else:
        case["recommended_contact_method"] = "Maps / visit in person"
        case["backup_contact_method"] = None

    case["strongest_problems"] = ["No website — missing online presence"]
    case["website_issues"] = [
        {
            "category": "Site Structure",
            "issue": "No website present",
        }
    ]
    case["website_score"] = 0
    case["fetch_ok"] = False
    case["website_status"] = "none"
    case["website_speed"] = None
    case["mobile_ready"] = False
    case["seo_score"] = 0
    case["website_quality_score"] = 40
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
    score, score_signals, lead_tier, website_quality, opportunity_reason, close_probability = calculateOpportunityScore(case)
    case["opportunity_score"] = score
    case["internal_score"] = score
    case["lead_tier"] = lead_tier
    case["tier"] = lead_tier
    case["website_status"] = website_quality.get("website_status")
    case["website_speed"] = website_quality.get("website_speed")
    case["mobile_ready"] = website_quality.get("mobile_ready")
    case["seo_score"] = website_quality.get("seo_score")
    case["website_quality_score"] = website_quality.get("website_quality_score")
    case["priority"] = "high" if score >= 70 else "medium" if score >= 50 else "low"
    case["opportunity_reason"] = opportunity_reason
    case["activity_summary"] = website_quality.get("activity_summary") or case.get("activity_summary") or []
    case["close_probability"] = close_probability
    lead_assessment = _derive_lead_assessment(case)
    case["lead_bucket"] = lead_assessment.get("lead_bucket")
    case["lead_type"] = lead_assessment.get("lead_type")
    case["close_probability"] = lead_assessment.get("close_probability")
    case["best_contact_method"] = lead_assessment.get("best_contact_method")
    case["best_pitch_angle"] = lead_assessment.get("best_pitch_angle")
    case["recommended_next_action"] = lead_assessment.get("recommended_next_action")
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

    # Outreach is generated on demand (case open / explicit regenerate), not during main scout run.
    case["short_email"] = None
    case["longer_email"] = None
    case["contact_form_version"] = None
    case["social_dm_version"] = None
    case["follow_up_note"] = None
    case["follow_up_line"] = None
    case["screenshot_failed"] = False

    valid, reason = _validate_case(case)
    if not valid:
        log.append(f"  SKIP validation: {reason}")
        return None
    save_case(CASES_DIR, case)
    log.append(f"  case_file_written: {case['slug']}.json")
    return case


def _build_weak_website_case(
    place: dict,
    home_city: str,
    categories: list,
    index: int,
    log: list,
    category: str = "",
    *,
    deep_scan: bool = False,
    capture_screenshots: bool = False,
    website_fetch_timeout: int = 6,
    screenshot_timeout: int = 14,
) -> dict | None:
    """Build case from Place Details. Light scan by default, optional deep scan."""
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
    case["google_review_count"] = place.get("google_review_count") or place.get("review_count")
    case["reviews_last_30_days"] = int(place.get("reviews_last_30_days") or 0)
    case["owner_post_detected"] = bool(place.get("owner_post_detected"))
    case["new_photos_detected"] = bool(place.get("new_photos_detected"))
    case["listing_recently_updated"] = bool(place.get("listing_recently_updated"))
    case["activity_summary"] = list(place.get("activity_summary") or [])
    case["business_status"] = place.get("business_status")
    case["review_snippets"] = place.get("review_snippets") or []
    case["review_themes"] = place.get("review_themes") or []
    case["performance_score"] = None

    if not deep_scan:
        precheck = _quick_site_precheck(website, timeout=min(3, max(1, website_fetch_timeout)))
        case["homepage_http_status"] = precheck.get("http_status")
        case["ssl_ok"] = not bool(precheck.get("ssl_issue"))
        if precheck.get("unreachable"):
            case["fetch_ok"] = False
            case["website_status"] = "unreachable"
            case["strongest_problems"] = ["Website appears unreachable"]
            case["website_issues"] = [
                {
                    "category": "Site Structure",
                    "issue": "Website unreachable",
                }
            ]
            case["audit_issues"] = ["Website appears unreachable"]
            case["strongest_pitch_angle"] = "Fix reliability and get the site loading for customers"
            case["best_service_to_offer"] = "Stability and performance recovery plan"
            score, score_signals, lead_tier, website_quality, opportunity_reason, close_probability = calculateOpportunityScore(case)
            score = max(score, 90)
            case["opportunity_score"] = score
            case["internal_score"] = score
            lead_tier = "hot_lead" if score >= 80 else "warm_lead" if score >= 60 else "low_priority"
            case["lead_tier"] = lead_tier
            case["tier"] = lead_tier
            case["website_status"] = website_quality.get("website_status") or "unreachable"
            case["website_speed"] = website_quality.get("website_speed")
            case["mobile_ready"] = website_quality.get("mobile_ready")
            case["seo_score"] = website_quality.get("seo_score")
            case["website_quality_score"] = website_quality.get("website_quality_score")
            case["priority"] = "high" if score >= 70 else "medium" if score >= 50 else "low"
            case["opportunity_reason"] = opportunity_reason
            case["activity_summary"] = website_quality.get("activity_summary") or case.get("activity_summary") or []
            case["close_probability"] = close_probability
            lead_assessment = _derive_lead_assessment(case)
            case["lead_bucket"] = lead_assessment.get("lead_bucket")
            case["lead_type"] = lead_assessment.get("lead_type")
            case["close_probability"] = lead_assessment.get("close_probability")
            case["best_contact_method"] = lead_assessment.get("best_contact_method")
            case["best_pitch_angle"] = lead_assessment.get("best_pitch_angle")
            case["recommended_next_action"] = lead_assessment.get("recommended_next_action")
            base_signals = generateOpportunitySignals(case)
            merged_signals = list(dict.fromkeys(base_signals + score_signals))
            case["opportunity_signals"] = merged_signals[:8]
            log.append("  fast precheck short-circuit: website unreachable")
            valid, reason = _validate_case(case)
            if not valid:
                log.append(f"  SKIP validation: {reason}")
                return None
            save_case(CASES_DIR, case)
            log.append(f"  case_file_written: {case['slug']}.json")
            return case
        if precheck.get("ssl_issue"):
            case["missing_ssl"] = True
            case["ssl_ok"] = False
            case["strongest_problems"] = ["Website uses HTTP instead of HTTPS"]
            case["website_issues"] = [
                {
                    "category": "Site Structure",
                    "issue": "Broken SSL / HTTP site",
                }
            ]
            case["audit_issues"] = ["Website uses HTTP instead of HTTPS"]
            case["strongest_pitch_angle"] = "Secure the site with HTTPS to improve trust and conversions"
            case["best_service_to_offer"] = "HTTPS/security and conversion-focused refresh"
            score, score_signals, lead_tier, website_quality, opportunity_reason, close_probability = calculateOpportunityScore(case)
            score = max(score, 80)
            case["opportunity_score"] = score
            case["internal_score"] = score
            lead_tier = "hot_lead" if score >= 80 else "warm_lead" if score >= 60 else "low_priority"
            case["lead_tier"] = lead_tier
            case["tier"] = lead_tier
            case["website_status"] = website_quality.get("website_status") or "weak"
            case["website_speed"] = website_quality.get("website_speed")
            case["mobile_ready"] = website_quality.get("mobile_ready")
            case["seo_score"] = website_quality.get("seo_score")
            case["website_quality_score"] = website_quality.get("website_quality_score")
            case["priority"] = "high" if score >= 70 else "medium" if score >= 50 else "low"
            case["opportunity_reason"] = opportunity_reason
            case["activity_summary"] = website_quality.get("activity_summary") or case.get("activity_summary") or []
            case["close_probability"] = close_probability
            lead_assessment = _derive_lead_assessment(case)
            case["lead_bucket"] = lead_assessment.get("lead_bucket")
            case["lead_type"] = lead_assessment.get("lead_type")
            case["close_probability"] = lead_assessment.get("close_probability")
            case["best_contact_method"] = lead_assessment.get("best_contact_method")
            case["best_pitch_angle"] = lead_assessment.get("best_pitch_angle")
            case["recommended_next_action"] = lead_assessment.get("recommended_next_action")
            base_signals = generateOpportunitySignals(case)
            merged_signals = list(dict.fromkeys(base_signals + score_signals))
            case["opportunity_signals"] = merged_signals[:8]
            log.append("  fast precheck short-circuit: website uses HTTP only")
            valid, reason = _validate_case(case)
            if not valid:
                log.append(f"  SKIP validation: {reason}")
                return None
            save_case(CASES_DIR, case)
            log.append(f"  case_file_written: {case['slug']}.json")
            return case

    log.append(f"  Investigating ({'deep' if deep_scan else 'light'}): {website}")
    screenshot_dir = CASE_FILES_DIR / slug if (deep_scan and capture_screenshots) else None
    inv = None
    try:
        inv = investigate(
            website,
            crawl_internal=deep_scan,
            timeout=screenshot_timeout if deep_scan else website_fetch_timeout,
            screenshot_dir=str(screenshot_dir) if screenshot_dir else None,
            google_profile_url=str(place.get("maps_url") or place.get("maps_link") or "").strip() or None,
        )
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
        case["fetch_ok"] = inv.get("fetch_ok")
        case["homepage_http_status"] = inv.get("homepage_http_status")
        case["homepage_load_seconds"] = inv.get("homepage_load_seconds")
        case["missing_meta_title"] = inv.get("missing_meta_title")
        case["missing_meta_description"] = inv.get("missing_meta_description")
        case["text_content_length"] = inv.get("text_content_length")
        case["image_count"] = inv.get("image_count")
        case["broken_links_count"] = inv.get("broken_links_count")
        case["mobile_score"] = inv.get("mobile_score")
        case["design_score"] = inv.get("design_score")
        case["navigation_score"] = inv.get("navigation_score")
        case["conversion_score"] = inv.get("conversion_score")
        case["website_audit"] = inv.get("website_audit") or {}
        case["audit_issues"] = inv.get("audit_issues") or inv.get("detected_issues") or []
        case["website_issues"] = inv.get("website_issues") or []
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
        case["contact_page_screenshot_path"] = inv.get("contact_page_path")
        case["internal_screenshot_path"] = inv.get("internal_page_path")
        case["desktop_screenshot_url"] = f"/case/{slug}/screenshot/desktop_homepage" if inv.get("desktop_homepage_path") else None
        case["mobile_screenshot_url"] = f"/case/{slug}/screenshot/mobile_homepage" if inv.get("mobile_homepage_path") else None
        case["contact_page_screenshot_url"] = f"/case/{slug}/screenshot/contact_page" if inv.get("contact_page_path") else None
        # Keep legacy field populated for backward compatibility.
        case["internal_screenshot_url"] = (
            f"/case/{slug}/screenshot/contact_page"
            if inv.get("contact_page_path")
            else f"/case/{slug}/screenshot/key_internal_page"
            if inv.get("internal_page_path")
            else None
        )

        emails = inv.get("emails") or []
        social = inv.get("social") or {}
        case["email"] = emails[0] if emails else None
        case["contact_page"] = inv.get("contact_page")
        case["contact_form_url"] = inv.get("contact_form_url")
        case["phone_from_site"] = (inv.get("phones") or [None])[0]
        case["facebook"] = social.get("facebook")
        case["facebook_url"] = social.get("facebook")
        case["instagram"] = social.get("instagram")
        case["instagram_url"] = social.get("instagram")
        case["linkedin"] = social.get("linkedin")
        case["social_links"] = social
        case["owner_manager_name"] = case.get("owner_name") or (case.get("owner_names") or [None])[0]

        problems = inv.get("problems") or []
        pitch_lines = inv.get("pitch") or []
        for line in inv.get("debug_log") or []:
            log.append(f"    {line}")
    else:
        case["fetch_ok"] = False
        problems = ["Website could not be fully investigated."]
        pitch_lines = ["Manual review recommended."]
        log.append("    website investigation: failed")

    case["strongest_problems"] = problems
    if not case.get("audit_issues"):
        case["audit_issues"] = list(problems) if isinstance(problems, list) else [str(problems)]
    if not case.get("website_issues"):
        fallback_structured = []
        for issue in case.get("audit_issues") or []:
            text = str(issue or "").strip()
            if not text:
                continue
            fallback_structured.append({"category": "Website", "issue": text})
        case["website_issues"] = fallback_structured
    if not isinstance(case.get("website_audit"), dict):
        case["website_audit"] = {}
    website_score = case.get("website_score")
    case["high_opportunity"] = bool((case.get("rating") or 0) >= 4.2 and (website_score if website_score is not None else 100) <= 60)
    case["strongest_pitch_angle"] = pitch_lines[0] if pitch_lines else None
    case["best_service_to_offer"] = "Modern mobile-friendly website with clear menu, hours, and contact"
    case["best_demo_to_show"] = "Show demo on iPad"
    case["demo_to_show"] = "Show demo on iPad"

    if case["email"]:
        case["recommended_contact_method"] = "Email"
    elif case["contact_page"] or case.get("contact_form_url"):
        case["recommended_contact_method"] = "Contact page"
    elif case["phone"] or case["phone_from_site"]:
        case["recommended_contact_method"] = "Phone"
    elif case["facebook"] or case["instagram"]:
        case["recommended_contact_method"] = "Social DM"
    else:
        case["recommended_contact_method"] = "Website or phone"
    case["backup_contact_method"] = (
        "Contact page" if case["email"] and (case["contact_page"] or case.get("contact_form_url"))
        else "Phone" if case["email"] and (case["phone"] or case["phone_from_site"])
        else "Phone" if (case["contact_page"] or case.get("contact_form_url")) and (case["phone"] or case["phone_from_site"])
        else "Email" if (case["phone"] or case["phone_from_site"]) and case["email"]
        else None
    )

    # Outreach is generated on demand (case open / explicit regenerate), not during main scout run.
    case["short_email"] = None
    case["longer_email"] = None
    case["contact_form_version"] = None
    case["social_dm_version"] = None
    case["follow_up_note"] = None
    case["follow_up_line"] = None
    case["screenshot_failed"] = False

    case["why_worth_pursuing"] = f"{name} — real business with website, worth outreach."
    case["why_this_lead_is_worth_pursuing"] = case["why_worth_pursuing"]
    case["what_stood_out"] = problems[0] if problems else None
    case["next_action"] = "Send short email or try contact form"
    case["follow_up_suggestion"] = "Follow up in 5–7 days"
    score, score_signals, lead_tier, website_quality, opportunity_reason, close_probability = calculateOpportunityScore(case)
    case["opportunity_score"] = score
    case["internal_score"] = score
    case["lead_tier"] = lead_tier
    case["tier"] = lead_tier
    case["website_status"] = website_quality.get("website_status")
    case["website_speed"] = website_quality.get("website_speed")
    case["mobile_ready"] = website_quality.get("mobile_ready")
    case["seo_score"] = website_quality.get("seo_score")
    case["website_quality_score"] = website_quality.get("website_quality_score")
    case["performance_score"] = case.get("website_score")
    case["priority"] = "high" if score >= 70 else "medium" if score >= 50 else "low"
    case["opportunity_reason"] = opportunity_reason
    case["activity_summary"] = website_quality.get("activity_summary") or case.get("activity_summary") or []
    case["close_probability"] = close_probability
    lead_assessment = _derive_lead_assessment(case)
    case["lead_bucket"] = lead_assessment.get("lead_bucket")
    case["lead_type"] = lead_assessment.get("lead_type")
    case["close_probability"] = lead_assessment.get("close_probability")
    case["best_contact_method"] = lead_assessment.get("best_contact_method")
    case["best_pitch_angle"] = lead_assessment.get("best_pitch_angle")
    case["recommended_next_action"] = lead_assessment.get("recommended_next_action")
    base_signals = generateOpportunitySignals(case)
    merged_signals = list(dict.fromkeys(base_signals + score_signals))
    # Early exit for obviously modern, fast sites during lightweight scans.
    if (
        not deep_scan
        and _as_int(case.get("mobile_score"), 0) >= 85
        and _as_int(case.get("performance_score"), 0) >= 85
    ):
        case["opportunity_score"] = min(int(case.get("opportunity_score") or 0), 39)
        case["internal_score"] = case["opportunity_score"]
        case["lead_tier"] = "low_priority"
        case["tier"] = "low_priority"
        case["close_probability"] = "low"
        merged_signals.append("Early exit: modern fast site")
        case["priority"] = "low"
    case["opportunity_signals"] = merged_signals[:8]

    valid, reason = _validate_case(case)
    if not valid:
        log.append(f"  SKIP validation: {reason}")
        return None
    save_case(CASES_DIR, case)
    log.append(f"  case_file_written: {case['slug']}.json")
    return case


def _deep_priority_rank(case: dict) -> tuple[int, float]:
    easy_reasons = _derive_easy_target_reasons(case, case.get("website_quality") if isinstance(case.get("website_quality"), dict) else {})
    if easy_reasons:
        first = str(easy_reasons[0]).lower()
        if "no website found" in first:
            return (0, -float(case.get("opportunity_score") or 0))
        if "website unreachable" in first:
            return (1, -float(case.get("opportunity_score") or 0))
        if "insecure http" in first:
            return (2, -float(case.get("opportunity_score") or 0))
        if "contact page missing" in first:
            return (3, -float(case.get("opportunity_score") or 0))
        if "mobile layout broken" in first:
            return (4, -float(case.get("opportunity_score") or 0))
    lane = str(case.get("lane") or "").strip().lower()
    if lane == "no_website" or bool(case.get("no_website")):
        return (0, -float(case.get("opportunity_score") or 0))
    mobile_score = _as_int(case.get("mobile_score"), 100)
    website_speed = _as_float(case.get("website_speed"), 0.0)
    outdated = bool(case.get("outdated_design_clues"))
    if mobile_score <= 45 or case.get("mobile_ready") is False:
        return (1, -float(case.get("opportunity_score") or 0))
    if website_speed > 3.0:
        return (2, -float(case.get("opportunity_score") or 0))
    if outdated:
        return (3, -float(case.get("opportunity_score") or 0))
    return (5, -float(case.get("opportunity_score") or 0))


def _scan_depth_limit(depth: str | None, default_limit: int) -> int:
    normalized = str(depth or "").strip().lower()
    mapping = {
        "quick": 25,
        "normal": 100,
        "deep": 300,
    }
    if normalized in mapping:
        return int(mapping[normalized])
    return int(default_limit)


def _matches_issue_filters(case: dict, selected_filters: list[str]) -> bool:
    if not selected_filters:
        return True
    filters = {str(v or "").strip().lower() for v in selected_filters if str(v or "").strip()}
    website = str(case.get("website") or "").strip().lower()
    facebook_url = str(case.get("facebook") or case.get("facebook_url") or "").strip().lower()
    facebook_only = bool((website and "facebook.com" in website) or (not website and facebook_url))
    no_website = bool(case.get("no_website")) or not bool(website)
    unreachable = str(case.get("website_status") or "").strip().lower() == "unreachable" or bool(case.get("fetch_ok") is False)
    insecure_http = website.startswith("http://") or bool(case.get("ssl_ok") is False)
    contact_missing = not bool(str(case.get("contact_page") or "").strip()) and not bool(case.get("contact_form_present"))
    mobile_broken = bool(case.get("viewport_ok") is False) or _as_int(case.get("mobile_score"), 100) < 50
    easy_win = no_website or unreachable or insecure_http or contact_missing or mobile_broken
    missing_online_ordering = bool((case.get("website_audit") or {}).get("checks", {}).get("booking_or_ordering_missing"))
    weak_menu_contact_flow = contact_missing or bool(case.get("menu_found") is False) or bool(case.get("tap_to_call_present") is False)
    redesign_opportunity = mobile_broken or bool(case.get("outdated_design_clues")) or _as_int(case.get("website_score"), 100) < 65
    checks = {
        "no website": no_website,
        "facebook only": facebook_only,
        "broken website": unreachable,
        "insecure http": insecure_http,
        "missing contact page": contact_missing,
        "mobile layout issues": mobile_broken,
        "mobile issues": mobile_broken,
        "outdated website": bool(case.get("outdated_design_clues")) or _as_int(case.get("design_score"), 100) < 60,
        "missing online ordering": missing_online_ordering,
        "weak menu/contact flow": weak_menu_contact_flow,
        "redesign opportunities": redesign_opportunity,
        "easy wins only": easy_win,
    }
    return any(checks.get(f, False) for f in filters)


def run(
    current_lat: float | None = None,
    current_lng: float | None = None,
    scan_settings: dict | None = None,
    progress_callback=None,
    cancel_callback=None,
):
    def is_cancelled() -> bool:
        if not cancel_callback:
            return False
        try:
            return bool(cancel_callback())
        except Exception:
            return False

    def ensure_not_cancelled() -> None:
        if is_cancelled():
            raise ScoutRunError(
                "cancelled",
                "Scout cancelled by user",
                "Scout cancelled",
            )

    def report_progress(stage: str, progress: int, message: str, **extra):
        if not progress_callback:
            return
        payload = {
            "stage": stage,
            "progress": max(0, min(100, int(progress))),
            "message": message,
        }
        if extra:
            payload.update(extra)
        try:
            progress_callback(payload)
        except Exception:
            # Progress telemetry should never break the scout run.
            pass

    try:
        from dotenv import load_dotenv
        load_dotenv(SCRIPT_DIR / ".env")
        load_dotenv(SCRIPT_DIR.parent / ".env")
    except ImportError:
        pass

    config = load_config()
    settings = scan_settings if isinstance(scan_settings, dict) else {}
    scan_scope = str(settings.get("scope") or "").strip().lower()
    selected_city = str(settings.get("single_city") or "").strip()
    selected_region = str(settings.get("region") or "").strip().lower()
    selected_categories = [
        str(v or "").strip()
        for v in (settings.get("categories") or [])
        if str(v or "").strip()
    ]
    selected_issue_filters = [
        str(v or "").strip().lower()
        for v in (settings.get("issue_filters") or [])
        if str(v or "").strip()
    ]
    selected_depth = str(settings.get("depth") or "").strip().lower()
    if selected_categories:
        config["categories"] = selected_categories
    if scan_scope == "single_city" and selected_city:
        config["multi_city_enabled"] = True
        config["max_cities_per_run"] = 1
        config["target_cities"] = [{"city_name": selected_city, "state": "AR"}]
    elif scan_scope == "nearby_cities" and selected_city:
        config["multi_city_enabled"] = True
        config["max_cities_per_run"] = max(8, int(config.get("max_cities_per_run", 8)))
        config["target_cities"] = [{"city_name": selected_city, "state": "AR"}]
        config["home_city"] = selected_city
        config["nearby_city_seed"] = selected_city
    elif scan_scope == "arkansas_region" and selected_region:
        region_key = selected_region.replace(" ", "_").lower()
        if region_key in ARKANSAS_REGION_CITIES:
            config["multi_city_enabled"] = True
            config["target_cities"] = [{"city_name": city, "state": "AR"} for city in ARKANSAS_REGION_CITIES[region_key]]
            config["max_cities_per_run"] = max(
                len(ARKANSAS_REGION_CITIES[region_key]),
                int(config.get("max_cities_per_run", len(ARKANSAS_REGION_CITIES[region_key]))),
            )
    elif scan_scope == "all_arkansas":
        config["multi_city_enabled"] = True
        config["target_cities"] = [{"city_name": city, "state": "AR"} for city in ARKANSAS_CITIES_STATEWIDE]
        config["max_cities_per_run"] = max(len(ARKANSAS_CITIES_STATEWIDE), int(config.get("max_cities_per_run", 40)))
    home_city = config.get("home_city", "City")
    target_cities = _resolve_target_cities(config)
    radius = config.get("search_radius_miles", 25)
    categories = _resolve_discovery_categories(config)
    base_radii_miles = config.get("search_radii_miles", [10])
    expansion_radii_miles = config.get("expansion_radii_miles", [10, 25, 50])
    city_expansion_threshold = max(1, int(config.get("city_expansion_threshold", 100)))
    max_total_results = int(
        config.get(
            "DISCOVERY_MAX_PER_RUN",
            config.get("discovery_max_per_run", config.get("max_total_results_per_run", 600)),
        )
    )
    max_total_results = max(200, max_total_results)
    max_total_results = min(max_total_results, int(config.get("max_businesses_per_run", 600) or 600))
    max_total_results = _scan_depth_limit(selected_depth, max_total_results)
    max_results_per_city = max(100, int(config.get("max_results_per_city", 250)))
    max_per = int(config.get("max_results_per_query", config.get("max_results_per_category", 60)))
    max_per = max(1, min(60, max_per))
    ignore_chains = config.get("ignore_chains", True)
    deep_scan_max_per_run = max(
        1, int(config.get("DEEP_SCAN_MAX_PER_RUN", config.get("deep_scan_max_per_run", 15)))
    )
    max_concurrency = max(
        1,
        min(
            12,
            int(
                os.environ.get(
                    "SCOUT_MAX_CONCURRENCY",
                    config.get("SCOUT_MAX_CONCURRENCY", config.get("max_concurrency", 10)),
                )
            ),
        ),
    )
    screenshot_score_threshold = max(
        0,
        int(
            os.environ.get(
                "SCOUT_SCREENSHOT_SCORE_THRESHOLD",
                config.get("SCOUT_SCREENSHOT_SCORE_THRESHOLD", 70),
            )
        ),
    )
    deep_audit_score_threshold = max(
        0,
        int(
            os.environ.get(
                "SCOUT_DEEP_AUDIT_SCORE_THRESHOLD",
                config.get("SCOUT_DEEP_AUDIT_SCORE_THRESHOLD", 80),
            )
        ),
    )
    screenshot_max_per_run = max(
        1, int(config.get("SCREENSHOT_MAX_PER_RUN", config.get("screenshot_max_per_run", 10)))
    )
    screenshot_concurrency = max(
        1, min(5, int(config.get("SCREENSHOT_CONCURRENCY", config.get("screenshot_concurrency", 3))))
    )
    deep_scan_concurrency = max(
        1,
        min(
            5,
            int(
                config.get(
                    "DEEP_SCAN_CONCURRENCY",
                    config.get("deep_scan_concurrency", screenshot_concurrency),
                )
            ),
        ),
    )
    website_fetch_timeout = max(
        2, int(config.get("WEBSITE_FETCH_TIMEOUT_SECONDS", config.get("website_fetch_timeout_seconds", 6)))
    )
    screenshot_timeout = max(
        5, int(config.get("SCREENSHOT_TIMEOUT_SECONDS", config.get("screenshot_timeout_seconds", 12)))
    )
    deep_analysis_timeout = max(10, int(config.get("deep_analysis_timeout_seconds", 45)))
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()

    print("Morning Runner — automated local client finder")
    print(f"  home_city: {home_city}, radius: {radius} mi")
    print(f"  target cities: {len(target_cities)}")
    active_region = str(os.environ.get("SCOUT_REGION_OVERRIDE", "") or "").strip().lower()
    if active_region:
        print(f"  active region override: {active_region}")
    print(f"  search_radii_miles: {base_radii_miles}")
    print(f"  expansion_radii_miles: {expansion_radii_miles}")
    print(f"  city_expansion_threshold: {city_expansion_threshold}")
    print(f"  categories: {categories}, max_results_per_query: {max_per}, ignore_chains: {ignore_chains}")
    print(f"  discovery_max_per_run: {max_total_results}")
    if settings:
        print(
            "  scan settings applied: "
            f"scope={scan_scope or 'default'}, city={selected_city or 'n/a'}, region={selected_region or 'n/a'}, "
            f"categories={len(selected_categories)}, issue_filters={len(selected_issue_filters)}, depth={selected_depth or 'default'}"
        )
    print(f"  deep_scan_max_per_run: {deep_scan_max_per_run}")
    print(f"  max_concurrency: {max_concurrency}")
    print(f"  screenshot_max_per_run: {screenshot_max_per_run}")
    print(f"  screenshot_score_threshold: {screenshot_score_threshold}")
    print(f"  deep_audit_score_threshold: {deep_audit_score_threshold}")
    print(f"  screenshot_concurrency: {screenshot_concurrency}")
    print(f"  deep_scan_concurrency: {deep_scan_concurrency}")
    print(
        "  timeouts (sec): "
        f"website_fetch={website_fetch_timeout}, "
        f"screenshot={screenshot_timeout}, "
        f"deep_analysis={deep_analysis_timeout}"
    )
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
    report_progress("discovering_businesses", 10, "Discovery started")

    places = []
    seen_place_ids: set[str] = set()
    total_duplicates_skipped = 0
    cities_scanned_count = 0
    try:
        total_cities = max(1, len(target_cities))
        for city_idx, target in enumerate(target_cities, start=1):
            ensure_not_cancelled()
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
                radii_miles=base_radii_miles,
                max_total_results=max_results_per_city,
            )
            if len(city_places) < city_expansion_threshold:
                print(
                    f"  city under threshold ({len(city_places)}<{city_expansion_threshold}), "
                    "expanding radius tiers"
                )
                expanded_places = _fetch_places(
                    city_label,
                    categories,
                    max_per,
                    radius,
                    current_lat=scan_lat,
                    current_lng=scan_lng,
                    radii_miles=expansion_radii_miles,
                    max_total_results=max_results_per_city,
                )
                expanded_existing_ids = {
                    str(p.get("place_id") or "").strip() for p in city_places if str(p.get("place_id") or "").strip()
                }
                for p in expanded_places:
                    pid = str(p.get("place_id") or "").strip()
                    if pid and pid in expanded_existing_ids:
                        continue
                    city_places.append(p)
                    if pid:
                        expanded_existing_ids.add(pid)
                    if len(city_places) >= max_results_per_city:
                        break
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
            cities_scanned_count += 1
            print(f"  businesses discovered: {city_added}")
            print(f"  duplicates skipped: {city_dupes}")
            discovery_progress = 10 + int((city_idx / total_cities) * 15)
            report_progress(
                "discovering_businesses",
                discovery_progress,
                f"Discovered {len(places)} businesses ({city_idx}/{total_cities} cities scanned)",
                discovered_count=len(places),
                cities_scanned=cities_scanned_count,
                total_cities=total_cities,
            )
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

    # Focus on easy-close local service models for now.
    places = [
        p for p in places
        if _is_easy_close_category(p.get("category") or p.get("industry") or "")
    ]
    print(f"  Filtered to easy-close categories: {len(places)} remaining")
    if not places:
        _write_empty("No businesses matched easy-close category focus.")
        return

    # Prioritize businesses with visible phone/contact and simple service model.
    def _place_priority(place: dict) -> tuple[int, int, int]:
        has_phone = 1 if str(place.get("phone") or "").strip() else 0
        has_contact_hint = 1 if str(place.get("website") or "").strip() or has_phone else 0
        simple_service = 1 if _is_easy_close_category(place.get("category") or place.get("industry") or "") else 0
        return (-has_phone, -has_contact_hint, -simple_service)

    places = sorted(places, key=_place_priority)

    no_website = [p for p in places if not (p.get("website") or "").strip()]
    weak_website = [p for p in places if (p.get("website") or "").strip()]
    print(f"  total businesses discovered: {len(places)}")
    print(f"  duplicates skipped: {total_duplicates_skipped}")

    print(f"  No website: {len(no_website)} | Weak website: {len(weak_website)}")
    print()
    total_businesses = len(no_website) + len(weak_website)
    report_progress(
        "businesses_discovered",
        25,
        f"Websites collected for {total_businesses} businesses",
        total_businesses=total_businesses,
    )
    report_progress(
        "quick_scans_running",
        45,
        f"Quick scans running for {total_businesses} businesses",
        total_businesses=total_businesses,
    )

    case_slugs = []
    no_website_slugs = []
    weak_website_slugs = []
    debug_log = []
    processed = 0
    saved = 0
    skipped = 0
    weak_light_cases: list[dict] = []
    weak_place_by_slug: dict[str, dict] = {}

    total_to_analyze = max(1, total_businesses)
    for i, place in enumerate(no_website):
        ensure_not_cancelled()
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
        fetch_progress = 25 + int((processed / total_to_analyze) * 20)
        report_progress(
            "quick_scans_running",
            fetch_progress,
            f"Quick scans running {processed} of {total_to_analyze}",
            analyzed_count=processed,
            total_businesses=total_to_analyze,
        )

    def _run_light_scan(position: int, place: dict):
        cat = place.get("category") or (categories[0] if categories else "")
        local_log: list[str] = [f"  [{cat}] "]
        case = _build_weak_website_case(
            place,
            home_city,
            categories,
            len(no_website) + position,
            local_log,
            category=cat,
            deep_scan=False,
            website_fetch_timeout=website_fetch_timeout,
            screenshot_timeout=screenshot_timeout,
        )
        return position, place, case, local_log

    if weak_website:
        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            futures = {
                executor.submit(_run_light_scan, i, place): (i, place)
                for i, place in enumerate(weak_website)
            }
            pending = set(futures.keys())
            try:
                while pending:
                    ensure_not_cancelled()
                    done, pending = wait(pending, timeout=2, return_when=FIRST_COMPLETED)
                    if not done:
                        report_progress(
                            "quick_scans_running",
                            25 + int((processed / max(1, total_to_analyze)) * 20),
                            f"Quick scans running {processed} of {total_to_analyze}",
                            analyzed_count=processed,
                            total_businesses=total_to_analyze,
                        )
                        continue
                    for future in done:
                        try:
                            _, place, case, local_log = future.result(timeout=website_fetch_timeout + 5)
                        except Exception as e:
                            local_log = [f"  light scan failed: {e}"]
                            place = futures[future][1]
                            case = None
                        for line in local_log:
                            print(line)
                        processed += 1
                        if case:
                            saved += 1
                            weak_website_slugs.append(case["slug"])
                            case_slugs.append(case["slug"])
                            weak_light_cases.append(case)
                            weak_place_by_slug[case["slug"]] = place
                        else:
                            skipped += 1
                        report_progress(
                            "quick_scans_running",
                            25 + int((processed / total_to_analyze) * 20),
                            f"Quick scans running {processed} of {total_to_analyze}",
                            fetched_count=processed,
                            total_businesses=total_to_analyze,
                        )
            except ScoutRunError:
                executor.shutdown(wait=False, cancel_futures=True)
                raise

    # Phase 2: deep scan top candidates only (priority: no website, mobile, speed, outdated).
    no_website_cases = []
    for slug in no_website_slugs:
        ensure_not_cancelled()
        p = CASES_DIR / f"{slug}.json"
        if not p.exists():
            continue
        try:
            with open(p, encoding="utf-8") as f:
                no_website_cases.append(json.load(f))
        except Exception:
            continue

    all_candidates = no_website_cases + weak_light_cases
    ranked_candidates = sorted(all_candidates, key=_deep_priority_rank)
    deep_eligible = [
        c for c in ranked_candidates if float(c.get("opportunity_score") or 0) >= deep_audit_score_threshold
    ]
    deep_candidates = deep_eligible[:deep_scan_max_per_run]
    for c in deep_candidates:
        print(f"  deep scan candidate selected: {c.get('slug') or c.get('business_name')}")
    for c in ranked_candidates:
        score = float(c.get("opportunity_score") or 0)
        if score < deep_audit_score_threshold:
            print(f"  deep scan skipped below threshold ({score:.0f}): {c.get('slug') or c.get('business_name')}")
    for c in deep_eligible[deep_scan_max_per_run:]:
        print(f"  deep scan skipped due to run cap: {c.get('slug') or c.get('business_name')}")
    deep_total = len(deep_candidates)
    if deep_total:
        report_progress(
            "capturing_screenshots",
            65,
            f"Capturing screenshots for 0 of {deep_total} businesses",
            screenshots_count=0,
            screenshot_targets=deep_total,
        )

        screenshot_candidate_slugs = [
            c.get("slug")
            for c in deep_candidates
            if not bool(c.get("no_website"))
        ]
        screenshot_candidate_set = {str(s) for s in screenshot_candidate_slugs if s}

        def _run_deep(case_slug: str, position: int):
            place = weak_place_by_slug.get(case_slug) or {}
            cat = place.get("category") or (categories[0] if categories else "")
            local_log: list[str] = []
            capture = str(case_slug or "") in screenshot_candidate_set
            if capture:
                print(f"  screenshot capture started: {case_slug}")
            deep_case = _build_weak_website_case(
                place,
                home_city,
                categories,
                len(no_website) + position,
                local_log,
                category=cat,
                deep_scan=True,
                capture_screenshots=capture,
                website_fetch_timeout=website_fetch_timeout,
                screenshot_timeout=screenshot_timeout,
            )
            return deep_case, local_log

        completed_deep = 0
        with ThreadPoolExecutor(max_workers=min(deep_scan_concurrency, screenshot_concurrency)) as executor:
            futures_by_future = {}
            for idx, case in enumerate(deep_candidates, start=1):
                ensure_not_cancelled()
                slug = case.get("slug")
                if not slug or bool(case.get("no_website")):
                    completed_deep += 1
                    continue
                future = executor.submit(_run_deep, slug, idx)
                futures_by_future[future] = (idx, slug)

            pending = set(futures_by_future.keys())
            try:
                while pending:
                    ensure_not_cancelled()
                    done, pending = wait(pending, timeout=2, return_when=FIRST_COMPLETED)
                    if not done:
                        report_progress(
                            "capturing_screenshots",
                            65 + int((completed_deep / max(1, deep_total)) * 10),
                            f"Capturing screenshots {completed_deep} of {deep_total}",
                            screenshots_count=completed_deep,
                            screenshot_targets=deep_total,
                        )
                        continue
                    for future in done:
                        idx, slug = futures_by_future[future]
                        try:
                            deep_case, local_log = future.result(timeout=deep_analysis_timeout)
                            for line in local_log:
                                print(line)
                            if deep_case and slug:
                                print(f"  deep scan saved: {slug}")
                        except FutureTimeoutError:
                            print(f"  screenshot capture timed out: {slug}")
                            print(f"  deep scan timeout: {slug}")
                            try:
                                path = CASES_DIR / f"{slug}.json"
                                if path.exists():
                                    with open(path, encoding="utf-8") as f:
                                        timed_out_case = json.load(f)
                                    timed_out_case["screenshot_failed"] = True
                                    save_case(CASES_DIR, timed_out_case)
                            except Exception:
                                pass
                        except Exception as e:
                            print(f"  deep scan failed: {slug} ({e})")
                        completed_deep += 1
                        report_progress(
                            "capturing_screenshots",
                            65 + int((completed_deep / max(1, deep_total)) * 10),
                            f"Capturing screenshots {completed_deep} of {deep_total}",
                            screenshots_count=completed_deep,
                            screenshot_targets=deep_total,
                        )
                        report_progress(
                            "analyzing_websites",
                            85 + int((completed_deep / max(1, deep_total)) * 10),
                            f"Analyzing website {completed_deep} of {deep_total}",
                            analyzed_count=completed_deep,
                            total_businesses=deep_total,
                        )
            except ScoutRunError:
                executor.shutdown(wait=False, cancel_futures=True)
                raise

    print()
    print(f"  Processed: {processed}")
    print(f"  Saved valid case files: {saved}")
    print(f"  Skipped invalid/incomplete leads: {skipped}")
    print()

    ensure_not_cancelled()
    if not case_slugs:
        _write_empty("No case files written. Check Places API and config.")
        report_progress("generating_dossiers", 95, "No valid leads to generate dossiers")
        return

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    location_summary = f"{current_lat:.4f},{current_lng:.4f}" if (current_lat is not None and current_lng is not None) else home_city
    high_score_opportunities = 0
    for slug in case_slugs:
        p = CASES_DIR / f"{slug}.json"
        if not p.exists():
            continue
        try:
            with open(p, encoding="utf-8") as f:
                c = json.load(f)
            if int(c.get("opportunity_score") or 0) >= 80:
                high_score_opportunities += 1
        except Exception:
            continue
    filtered_case_slugs = []
    filtered_no_website_slugs = []
    filtered_weak_website_slugs = []
    for slug in case_slugs:
        p = CASES_DIR / f"{slug}.json"
        if not p.exists():
            continue
        try:
            with open(p, encoding="utf-8") as f:
                c = json.load(f)
        except Exception:
            continue
        if _matches_issue_filters(c, selected_issue_filters):
            filtered_case_slugs.append(slug)
            if slug in no_website_slugs:
                filtered_no_website_slugs.append(slug)
            if slug in weak_website_slugs:
                filtered_weak_website_slugs.append(slug)
    case_slugs = filtered_case_slugs
    no_website_slugs = filtered_no_website_slugs
    weak_website_slugs = filtered_weak_website_slugs

    summary = f"Found {len(no_website_slugs)} no-website + {len(weak_website_slugs)} weak-website opportunities near {location_summary}."
    today = {
        "generated_at": generated_at,
        "summary": summary,
        "scan_setup": {
            "scope": scan_scope or "default",
            "single_city": selected_city or None,
            "region": selected_region or None,
            "categories": categories,
            "issue_filters": selected_issue_filters,
            "depth": selected_depth or None,
            "max_businesses_per_run": max_total_results,
        },
        "case_slugs": case_slugs,
        "no_website_slugs": no_website_slugs,
        "weak_website_slugs": weak_website_slugs,
        "top_opportunities": case_slugs[:10],
        "total_businesses_scanned": len(places),
        "businesses_without_websites": len(no_website),
        "weak_websites_detected": len(weak_website),
        "businesses_discovered": len(places),
        "duplicates_skipped": total_duplicates_skipped,
        "unique_leads_created": len(case_slugs),
        "processed_count": processed,
        "saved_count": saved,
        "skipped_count": skipped,
        "cities_scanned": cities_scanned_count,
        "industries_scanned": len(categories),
        "businesses_found": len(places),
        "high_score_opportunities": high_score_opportunities,
        "leads_created": len(case_slugs),
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
    report_progress(
        "generating_dossiers",
        95,
        "Generating dossiers",
        generated_count=len(case_slugs),
    )

    print()
    print(f"  Wrote {len(case_slugs)} cases. {len(no_website_slugs)} no-website (priority), {len(weak_website_slugs)} weak-website.")
    print(f"  unique leads created: {len(case_slugs)}")
    report_progress("saving_results", 98, "Saving results")


def _write_empty(summary: str | None = None):
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    today = {
        "generated_at": generated_at,
        "summary": summary or "No opportunities found.",
        "case_slugs": [],
        "no_website_slugs": [],
        "weak_website_slugs": [],
        "top_opportunities": [],
        "cities_scanned": 0,
        "industries_scanned": 0,
        "businesses_found": 0,
        "high_score_opportunities": 0,
        "leads_created": 0,
    }
    with open(SCRIPT_DIR / "today.json", "w", encoding="utf-8") as f:
        json.dump(today, f, indent=2)
    with open(SCRIPT_DIR / "opportunities.json", "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)


if __name__ == "__main__":
    run()
