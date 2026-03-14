"""
Google Places client — fetches real businesses for Morning Runner.

Uses Places API (New) and Geocoding API. GOOGLE_MAPS_API_KEY from environment.
Backend only.
"""
import json
import math
import os
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
                raise RuntimeError(f"Geocoding API REQUEST_DENIED: {err}")
            if status != "OK" and status != "ZERO_RESULTS":
                err = data.get("error_message", status)
                if log:
                    log(f"Geocoding status: {status} — {err}")
            return data
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Geocoding HTTP {e.code}: {body}") from e
    except Exception as e:
        if log:
            log(f"Geocoding request failed: {e}")
        raise


def _places_post(url: str, body: dict, field_mask: str, log: Callable[[str], None] | None = None) -> dict:
    """POST request to Places API (New). Uses X-Goog-Api-Key header."""
    key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not key:
        if log:
            log("GOOGLE_MAPS_API_KEY not set")
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
        raise RuntimeError(f"Places API error ({status}): {err_msg}") from e
    except Exception as e:
        if log:
            log(f"Places API request failed: {e}")
        raise


def _place_from_new_api(p: dict, center_lat: float, center_lng: float) -> dict:
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
        "maps_url": p.get("googleMapsUri"),
        "place_id": p.get("id"),
        "distance_miles": distance_miles,
    }


def _extract_review_intelligence(reviews: list[dict] | None) -> tuple[list[str], list[str]]:
    snippets: list[str] = []
    themes: dict[str, int] = {}
    if not reviews:
        return snippets, []

    keyword_map = {
        "service": ["service", "staff", "friendly", "rude", "slow"],
        "speed": ["slow", "wait", "line", "quick", "fast"],
        "quality": ["quality", "fresh", "taste", "delicious", "bland"],
        "pricing": ["price", "expensive", "cheap", "value", "overpriced"],
        "cleanliness": ["clean", "dirty", "hygiene", "bathroom"],
        "website_or_ordering": ["website", "online", "order", "booking", "reservation"],
    }

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

    top_themes = sorted(themes.items(), key=lambda x: x[1], reverse=True)
    themed = [f"{name} ({count})" for name, count in top_themes[:4]]
    return snippets[:3], themed


def place_details_new(place_id: str, center_lat: float, center_lng: float, log: Callable[[str], None] | None = None) -> dict | None:
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
        snippets, themes = _extract_review_intelligence(raw.get("reviews"))
        mapped["review_snippets"] = snippets
        mapped["review_themes"] = themes
        return mapped
    except Exception as e:
        if log:
            log(f"Place Details fetch failed for {place_id}: {e}")
        return None


def geocode(address: str, log: Callable[[str], None] | None = None) -> tuple[float, float] | None:
    """Geocode address to (lat, lng). Uses Geocoding API."""
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
    lat: float,
    lng: float,
    radius_meters: float,
    max_results: int,
    log: Callable[[str], None] | None = None,
) -> list[dict]:
    """
    Text Search (New) — search places by text query.
    Returns list of place dicts in our internal format.
    """
    radius = min(50000.0, max(1.0, radius_meters))
    body = {
        "textQuery": text_query,
        "locationBias": {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": radius,
            }
        },
        "pageSize": min(20, max(1, max_results)),
    }
    data = _places_post(TEXT_SEARCH_URL, body, TEXT_SEARCH_FIELDS, log=log)

    places_raw = data.get("places") or []
    out = []
    for p in places_raw:
        pid = p.get("id")
        if not pid:
            continue
        mapped = _place_from_new_api(p, lat, lng)
        mapped["place_id"] = pid
        out.append(mapped)
    return out


def search_places(
    city: str,
    categories: list[str],
    max_per_category: int = 5,
    radius_miles: float | list[float] = 25,
    current_lat: float | None = None,
    current_lng: float | None = None,
    max_total_results: int = 120,
    log: Callable[[str], None] | None = None,
) -> list[dict]:
    """
    Search for businesses by category in a city.
    Uses Geocoding API + Places API (New) Text Search.
    Returns place dicts: name, address, phone, website, rating, review_count,
    hours, maps_url, distance_miles, category.
    """
    if current_lat is not None and current_lng is not None:
        lat, lng = float(current_lat), float(current_lng)
        if log:
            log(f"run scout using current location ({lat}, {lng})")
    else:
        coords = geocode(city, log=log)
        if not coords:
            if log:
                log("Geocode failed for city")
            raise RuntimeError("Geocode failed for city")
        lat, lng = coords
        if log:
            log("run scout using saved config location")

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
                if "REQUEST_DENIED" in err_str or "LEGACY" in err_str:
                    raise RuntimeError(
                        f"Places API REQUEST_DENIED or legacy API. "
                        f"Enable 'Places API (New)' and 'Geocoding API' in your Google Cloud project. Original: {e}"
                    ) from e
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
