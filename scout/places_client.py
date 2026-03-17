"""
Google Places client — fetches real businesses for Morning Runner.

Uses Places API (New). Geocoding is optional and only used to improve
location bias / distance calculations when available.
Backend only.
"""
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Callable

# Geocoding API (still supported, not legacy)
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# Places API (New) — replaces legacy nearbysearch and details
TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACE_DETAILS_URL = "https://places.googleapis.com/v1/places"

# Field mask for Text Search — matches our output needs
TEXT_SEARCH_FIELDS = (
    "places.id,places.name,places.displayName,places.formattedAddress,"
    "places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri,"
    "places.rating,places.userRatingCount,places.regularOpeningHours,"
    "places.googleMapsUri,places.location"
)

DETAILS_FIELDS = (
    "id,displayName,formattedAddress,nationalPhoneNumber,internationalPhoneNumber,websiteUri,"
    "rating,userRatingCount,regularOpeningHours,googleMapsUri,location,reviews"
)


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


GEOCODING_OPTIONAL = _truthy_env("SCOUT_ENABLE_GEOCODING", True)
PLACES_ENABLED = _truthy_env("SCOUT_ENABLE_PLACES", True)
PLACES_REDUCED_MODE_MESSAGE = (
    "Google Places discovery is unavailable because billing is not enabled. "
    "Scout is running in reduced mode."
)
_places_reduced_mode_notice: str | None = None


def _set_places_reduced_mode_notice(message: str) -> None:
    global _places_reduced_mode_notice
    if not _places_reduced_mode_notice:
        _places_reduced_mode_notice = str(message or "").strip() or PLACES_REDUCED_MODE_MESSAGE


def get_places_reduced_mode_notice(*, clear: bool = False) -> str | None:
    global _places_reduced_mode_notice
    notice = _places_reduced_mode_notice
    if clear:
        _places_reduced_mode_notice = None
    return notice


def _maps_search_link(name: str | None, address: str | None, city: str | None = None) -> str | None:
    query = ", ".join([str(v or "").strip() for v in [name, address, city] if str(v or "").strip()])
    if not query:
        return None
    return f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(query)}"


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Approximate distance in miles between two points."""
    R = 3959
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return R * c


def _geocode_get(url: str, params: dict, log: Callable[[str], None] | None = None) -> dict:
    """GET request to Geocoding API. Legacy-style endpoint still supported."""
    key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not key:
        if log:
            log("GOOGLE_MAPS_API_KEY not set")
        return {}
    params["key"] = key
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"
    if log:
        log(f"Calling endpoint: {url}")
    req = urllib.request.Request(full_url, headers={"User-Agent": "MassiveBrainPlaces/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            status = data.get("status", "")
            if status == "REQUEST_DENIED":
                err = data.get("error_message", status)
                if log:
                    log(f"Soft warning: Geocoding API REQUEST_DENIED ({err}) — continuing without coordinates")
                return {}
            if status != "OK" and status != "ZERO_RESULTS":
                err = data.get("error_message", status)
                if log:
                    log(f"Geocoding status: {status} — {err}")
            return data
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        if log:
            log(f"Soft warning: Geocoding HTTP {e.code} ({body}) — continuing without coordinates")
        return {}
    except Exception as e:
        if log:
            log(f"Soft warning: Geocoding request failed ({e}) — continuing without coordinates")
        return {}


def _places_post(url: str, body: dict, field_mask: str, log: Callable[[str], None] | None = None) -> dict:
    """POST request to Places API (New). Uses X-Goog-Api-Key header."""
    key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not key:
        _set_places_reduced_mode_notice(
            "Google Places discovery is unavailable because API key is missing. Scout is running in reduced mode."
        )
        if log:
            log("Soft warning: GOOGLE_MAPS_API_KEY not set — running reduced mode without Places discovery")
        return {}
    if log:
        log(f"Calling endpoint: {url}")
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": key,
            "X-Goog-FieldMask": field_mask,
            "User-Agent": "MassiveBrainPlaces/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        body_str = body_bytes.decode("utf-8", errors="ignore")
        try:
            err_data = json.loads(body_str)
            err_msg = err_data.get("error", {}).get("message", body_str)
            status = err_data.get("error", {}).get("status", "UNKNOWN")
        except Exception:
            err_msg = body_str
            status = str(e.code)
        if "PERMISSION_DENIED" in str(status).upper() or "BILLING" in str(err_msg).upper() or "BILLING" in str(status).upper():
            _set_places_reduced_mode_notice(PLACES_REDUCED_MODE_MESSAGE)
        raise RuntimeError(f"Places API error ({status}): {err_msg}") from e
    except Exception as e:
        if log:
            log(f"Places API request failed: {e}")
        raise


def _place_from_new_api(p: dict, center_lat: float | None, center_lng: float | None) -> dict:
    """Map Places API (New) Place object to our internal format."""
    name = None
    dn = p.get("displayName") or {}
    if isinstance(dn, dict):
        name = dn.get("text")
    elif isinstance(dn, str):
        name = dn

    hours = None
    roh = p.get("regularOpeningHours") or {}
    if isinstance(roh, dict):
        wd = roh.get("weekdayDescriptions") or roh.get("weekdayText") or []
        if wd:
            hours = " | ".join(str(x) for x in wd[:7])

    phone = p.get("nationalPhoneNumber") or p.get("internationalPhoneNumber")

    lat, lng = None, None
    loc = p.get("location") or {}
    if isinstance(loc, dict):
        lat = loc.get("latitude")
        lng = loc.get("longitude")

    distance_miles = None
    if lat is not None and lng is not None and center_lat is not None and center_lng is not None:
        distance_miles = round(_haversine_miles(center_lat, center_lng, float(lat), float(lng)), 1)

    return {
        "name": name,
        "address": p.get("formattedAddress"),
        "phone": phone,
        "website": p.get("websiteUri"),
        "rating": p.get("rating"),
        "review_count": p.get("userRatingCount"),
        "hours": hours,
        "maps_url": p.get("googleMapsUri") or _maps_search_link(name, p.get("formattedAddress")),
        "place_id": p.get("id"),
        "distance_miles": distance_miles,
    }


def _extract_review_intelligence(reviews: list[dict] | None) -> tuple[list[str], list[str], dict[str, Any]]:
    snippets: list[str] = []
    themes: dict[str, int] = {}
    activity = {
        "reviews_last_30_days": 0,
        "owner_post_detected": False,
        "activity_summary": [],
    }
    if not reviews:
        return snippets, [], activity

    keyword_map = {
        "service": ["service", "staff", "friendly", "rude", "slow"],
        "speed": ["slow", "wait", "line", "quick", "fast"],
        "quality": ["quality", "fresh", "taste", "delicious", "bland"],
        "pricing": ["price", "expensive", "cheap", "value", "overpriced"],
        "cleanliness": ["clean", "dirty", "hygiene", "bathroom"],
        "website_or_ordering": ["website", "online", "order", "booking", "reservation"],
    }

    now_utc = datetime.now(timezone.utc)
    for r in reviews[:5]:
        text = (r.get("text") or {}).get("text") if isinstance(r.get("text"), dict) else r.get("text")
        if not text:
            continue
        text = str(text).strip()
        if not text:
            continue
        snippets.append(text[:220])
        lower = text.lower()
        for theme, keywords in keyword_map.items():
            if any(k in lower for k in keywords):
                themes[theme] = themes.get(theme, 0) + 1
        publish_time = str(r.get("publishTime") or "").strip()
        if publish_time:
            try:
                parsed = datetime.fromisoformat(publish_time.replace("Z", "+00:00"))
                if parsed >= now_utc - timedelta(days=30):
                    activity["reviews_last_30_days"] = int(activity.get("reviews_last_30_days") or 0) + 1
            except Exception:
                pass
        if r.get("reviewReply"):
            activity["owner_post_detected"] = True

    top_themes = sorted(themes.items(), key=lambda x: x[1], reverse=True)
    themed = [f"{name} ({count})" for name, count in top_themes[:4]]
    if int(activity.get("reviews_last_30_days") or 0) > 0:
        activity["activity_summary"].append(
            f"{int(activity.get('reviews_last_30_days') or 0)} new reviews this month"
        )
    if bool(activity.get("owner_post_detected")):
        activity["activity_summary"].append("Owner posted update recently")
    return snippets[:3], themed, activity


def place_details_new(
    place_id: str,
    center_lat: float | None,
    center_lng: float | None,
    log: Callable[[str], None] | None = None,
) -> dict | None:
    """Fetch Place Details (New) for richer dossier fields, including reviews."""
    if not place_id:
        return None
    url = f"{PLACE_DETAILS_URL}/{urllib.parse.quote(place_id)}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "X-Goog-Api-Key": os.environ.get("GOOGLE_MAPS_API_KEY", "").strip(),
            "X-Goog-FieldMask": DETAILS_FIELDS,
            "User-Agent": "MassiveBrainPlaces/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        mapped = _place_from_new_api(raw, center_lat, center_lng)
        snippets, themes, activity = _extract_review_intelligence(raw.get("reviews"))
        mapped["review_snippets"] = snippets
        mapped["review_themes"] = themes
        mapped["google_review_count"] = mapped.get("review_count")
        mapped["reviews_last_30_days"] = int(activity.get("reviews_last_30_days") or 0)
        mapped["owner_post_detected"] = bool(activity.get("owner_post_detected"))
        photos = raw.get("photos") or []
        mapped["new_photos_detected"] = bool(isinstance(photos, list) and len(photos) > 0)
        mapped["listing_recently_updated"] = bool(
            mapped.get("reviews_last_30_days")
            or mapped.get("owner_post_detected")
            or mapped.get("new_photos_detected")
        )
        activity_summary = list(activity.get("activity_summary") or [])
        if mapped.get("new_photos_detected"):
            activity_summary.append("New photos detected on listing")
        if mapped.get("listing_recently_updated"):
            activity_summary.append("Listing recently updated")
        mapped["activity_summary"] = list(dict.fromkeys(activity_summary))[:4]
        return mapped
    except Exception as e:
        if log:
            log(f"Place Details fetch failed for {place_id}: {e}")
        return None


def geocode(address: str, log: Callable[[str], None] | None = None) -> tuple[float, float] | None:
    """Geocode address to (lat, lng). Uses Geocoding API."""
    if not GEOCODING_OPTIONAL:
        if log:
            log("Geocoding disabled by SCOUT_ENABLE_GEOCODING flag — continuing without coordinates")
        return None
    data = _geocode_get(GEOCODE_URL, {"address": address}, log=log)
    results = data.get("results") or []
    if not results:
        return None
    loc = results[0].get("geometry", {}).get("location", {})
    lat = loc.get("lat")
    lng = loc.get("lng")
    if lat is not None and lng is not None:
        return float(lat), float(lng)
    return None


def text_search_new(
    text_query: str,
    lat: float | None,
    lng: float | None,
    radius_meters: float,
    max_results: int,
    log: Callable[[str], None] | None = None,
) -> list[dict]:
    """
    Text Search (New) — search places by text query.
    Returns list of place dicts in our internal format.
    """
    radius = min(50000.0, max(1.0, radius_meters))
    requested = max(1, min(60, int(max_results or 1)))
    out = []
    next_token = None
    pages_fetched = 0

    while len(out) < requested:
        pages_fetched += 1
        if pages_fetched > 12:
            break
        body = {
            "textQuery": text_query,
            "pageSize": min(20, max(1, requested - len(out))),
        }
        if lat is not None and lng is not None:
            body["locationBias"] = {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": radius,
                }
            }
        if next_token:
            body["pageToken"] = next_token

        data = _places_post(TEXT_SEARCH_URL, body, TEXT_SEARCH_FIELDS, log=log)
        places_raw = data.get("places") or []
        if not places_raw:
            break
        for p in places_raw:
            if len(out) >= requested:
                break
            pid = p.get("id")
            if not pid:
                continue
            mapped = _place_from_new_api(p, lat, lng)
            mapped["place_id"] = pid
            out.append(mapped)

        next_token = data.get("nextPageToken")
        if not next_token or len(out) >= requested:
            break
        # nextPageToken can take a moment to become valid.
        time.sleep(1.0)

    return out


def search_places(
    city: str,
    categories: list[str],
    max_per_category: int = 60,
    radius_miles: float | list[float] = 25,
    current_lat: float | None = None,
    current_lng: float | None = None,
    max_total_results: int = 120,
    log: Callable[[str], None] | None = None,
) -> list[dict]:
    """
    Search for businesses by category in a city.
    Uses Places API (New) Text Search; Geocoding is optional.
    Returns place dicts: name, address, phone, website, rating, review_count,
    hours, maps_url, distance_miles, category.
    """
    if not PLACES_ENABLED:
        _set_places_reduced_mode_notice(
            "Google Places discovery disabled by SCOUT_ENABLE_PLACES=false. Scout is running in reduced mode."
        )
        if log:
            log("Soft warning: SCOUT_ENABLE_PLACES=false — skipping Google Places discovery")
        return []

    if current_lat is not None and current_lng is not None:
        lat, lng = float(current_lat), float(current_lng)
        if log:
            log(f"run scout using current location ({lat}, {lng})")
    else:
        coords = geocode(city, log=log)
        if not coords:
            if log:
                log("Soft warning: city geocode unavailable — continuing with text search only")
            lat, lng = None, None
        else:
            lat, lng = coords
        if log:
            if lat is not None and lng is not None:
                log("run scout using saved config location")
            else:
                log("run scout without coordinate bias")

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg)

    if isinstance(radius_miles, list):
        radii = [float(r) for r in radius_miles if float(r) > 0]
    else:
        radii = [float(radius_miles)] if float(radius_miles) > 0 else []
    if not radii:
        radii = [2.0, 5.0, 10.0, 15.0]

    all_places = []
    seen_ids: set[str] = set()

    for cat in categories:
        _log(f"category search started: {cat}")
        for radius in radii:
            if len(all_places) >= max_total_results:
                break
            radius_m = radius * 1609.34
            query = f"{cat} in {city}"
            _log(f"radius search started: {radius} miles")
            try:
                results = text_search_new(query, lat, lng, radius_m, max_per_category, log=log)
            except RuntimeError as e:
                err_str = str(e).upper()
                if (
                    "REQUEST_DENIED" in err_str
                    or "LEGACY" in err_str
                    or "PERMISSION_DENIED" in err_str
                    or "BILLING" in err_str
                    or "FORBIDDEN" in err_str
                ):
                    _set_places_reduced_mode_notice(PLACES_REDUCED_MODE_MESSAGE)
                    _log(f"Soft warning: {PLACES_REDUCED_MODE_MESSAGE}")
                    return []
                raise

            for r in results:
                if len(all_places) >= max_total_results:
                    break
                pid = r.get("place_id") or r.get("id")
                name = r.get("name") or "?"
                if not pid:
                    continue
                if pid in seen_ids:
                    _log(f"duplicate business skipped: {name}")
                    continue
                seen_ids.add(pid)
                _log(f"Places result found: {name}")
                _log(f"Website found: {'yes' if r.get('website') else 'no'}")
                _log(f"Phone found: {'yes' if r.get('phone') else 'no'}")
                r["category"] = cat
                details = place_details_new(pid, lat, lng, log=log)
                if details:
                    # Prefer richer details payload when available.
                    r.update({k: v for k, v in details.items() if v is not None})
                all_places.append(r)

    _log(f"total businesses discovered: {len(all_places)}")

    return all_places
