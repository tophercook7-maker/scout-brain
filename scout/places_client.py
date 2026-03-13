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
    radius_miles: float = 25,
    log: Callable[[str], None] | None = None,
) -> list[dict]:
    """
    Search for businesses by category in a city.
    Uses Geocoding API + Places API (New) Text Search.
    Returns place dicts: name, address, phone, website, rating, review_count,
    hours, maps_url, distance_miles, category.
    """
    coords = geocode(city, log=log)
    if not coords:
        if log:
            log("Geocode failed for city")
        raise RuntimeError("Geocode failed for city")
    lat, lng = coords

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg)

    radius_m = radius_miles * 1609.34
    all_places = []
    seen_ids: set[str] = set()

    for cat in categories:
        query = f"{cat} in {city}"
        _log(f"Searching: {query}")
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
            pid = r.get("place_id") or r.get("id")
            name = r.get("name") or "?"
            if not pid:
                continue
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            _log(f"Places result found: {name}")
            _log(f"Website found: {'yes' if r.get('website') else 'no'}")
            _log(f"Phone found: {'yes' if r.get('phone') else 'no'}")
            r["category"] = cat
            all_places.append(r)

    return all_places
