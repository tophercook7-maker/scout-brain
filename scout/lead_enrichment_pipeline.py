"""
Lead enrichment pipeline — Places match + website investigation + web-design scoring.

Insertion point: called from FastAPI POST /api/enrich-lead (and optionally morning runner later).
Reuses scout.places_client, scout.investigator, scout.web_design_classify.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from .enriched_lead_schema import EnrichLeadRequest, EnrichedLead
from .web_design_classify import (
    build_web_design_tags,
    classify_local_service,
    classify_polished_site,
    classify_weak_website,
    is_facebook_url,
    is_standalone_website,
)

LogFn = Callable[[str], None] | None


def _log(log: LogFn, msg: str) -> None:
    if log:
        log(msg)
    elif os.environ.get("SCOUT_VERBOSE_LOGS", "").strip().lower() in {"1", "true", "yes"}:
        print(f"[enrich] {msg}")


def _trim(s: str | None) -> str:
    return str(s or "").strip()


def _digits(s: str | None) -> str:
    return re.sub(r"\D", "", str(s or ""))


def _normalize_name_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _trim(s).lower()).strip()


def normalized_website_key(url: str | None) -> str | None:
    raw = _trim(url)
    if not raw:
        return None
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        p = urlparse(raw)
        host = (p.hostname or "").lower().replace("www.", "")
        path = (p.path or "").rstrip("/")
        if not host:
            return None
        return f"{host}{path}" if path and path != "/" else host
    except Exception:
        return None


def _website_host(url: str | None) -> str | None:
    raw = _trim(url)
    if not raw:
        return None
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        return (urlparse(raw).hostname or "").lower().replace("www.", "") or None
    except Exception:
        return None


def compute_match_confidence(
    *,
    business_name: str,
    city: str,
    state: str,
    input_phone: str,
    input_website: str,
    place: dict[str, Any],
) -> float:
    """Conservative 0..1 confidence that this Place row matches the input lead."""
    score = 0.0
    bn = _normalize_name_key(business_name)
    pn = _normalize_name_key(str(place.get("name") or ""))
    if bn and pn:
        if bn == pn:
            score += 0.48
        elif bn in pn or pn in bn:
            score += 0.38
        else:
            bt, pt = set(bn.split()), set(pn.split())
            if bt and pt:
                inter = len(bt & pt)
                denom = max(len(bt), len(pt))
                score += 0.28 * (inter / denom)
    addr = str(place.get("address") or "").lower()
    c = _trim(city).lower()
    st = _trim(state).lower()
    if c and len(c) > 2 and c in addr:
        score += 0.14
    if st and len(st) == 2 and st in addr:
        score += 0.1
    ip = _digits(input_phone)
    pp = _digits(str(place.get("phone") or ""))
    if ip and pp and (ip in pp or pp in ip or ip[-10:] == pp[-10:]):
        score += 0.22
    ih = _website_host(input_website)
    ph = _website_host(str(place.get("website") or ""))
    if ih and ph and ih == ph:
        score += 0.18
    return max(0.0, min(1.0, score))


def compute_source_confidence(
    source_type: str,
    *,
    has_source_url: bool,
    has_facebook_url: bool,
    match_confidence: float,
    places_hit: bool,
) -> float:
    base = 0.35
    st = (source_type or "unknown").lower()
    if st in ("extension", "facebook") and (has_source_url or has_facebook_url):
        base = 0.52
    elif st == "manual":
        base = 0.45
    elif st == "google":
        base = 0.55
    if places_hit:
        base += 0.18
    base += 0.15 * match_confidence
    return max(0.0, min(1.0, base))


def score_web_design_lead(
    *,
    has_real_website: bool,
    facebook_only: bool,
    no_website: bool,
    weak_website: bool,
    polished: bool,
    local_service: bool,
    has_phone: bool,
    has_email: bool,
    match_confidence: float,
) -> int:
    """0–100 score: how strong a *web design sales* lead this is."""
    s = 42
    if no_website:
        s += 22
    if facebook_only:
        s += 14
    if weak_website and has_real_website:
        s += 12
    if local_service:
        s += 10
    if has_phone:
        s += 7
    if has_email:
        s += 9
    if polished:
        s -= 28
    if match_confidence < 0.32:
        s -= 14
    elif match_confidence < 0.5:
        s -= 6
    return max(0, min(100, int(round(s))))


def _why_string(
    *,
    facebook_only: bool,
    no_website: bool,
    weak_website: bool,
    polished: bool,
    local_service: bool,
    has_email: bool,
    has_phone: bool,
) -> str:
    if no_website and facebook_only:
        return "Facebook-only business. Strong web design target."
    if no_website and local_service:
        return "No website. Local service — very strong opportunity."
    if no_website:
        return "No website. Strong opportunity."
    if weak_website and local_service:
        return "Weak website with local service focus."
    if weak_website:
        return "Website could use a stronger, clearer presence."
    if polished:
        return "Polished site — lighter web-design angle unless they want a refresh."
    if has_email and has_phone:
        return "Has contact info; pitch a clearer web presence and ownership."
    if has_phone:
        return "Phone available — good for a quick call about their web presence."
    return "Review match quality and reach out with a simple site offer."


def _best_contact_method(
    email: str | None, phone: str | None, facebook: str | None, contact_page: str | None
) -> str:
    if _trim(email):
        return "email"
    if _trim(phone):
        return "phone"
    if _trim(facebook):
        return "facebook"
    if _trim(contact_page):
        return "contact_page"
    return "none"


def _best_next_move(method: str) -> str:
    return {
        "email": "send short website offer",
        "phone": "call now",
        "facebook": "message on Facebook",
        "contact_page": "use their contact form",
        "none": "research later",
    }.get(method, "research later")


def _pitch_angle(
    *,
    facebook_only: bool,
    no_website: bool,
    weak_website: bool,
    local_service: bool,
) -> str:
    if facebook_only:
        return "They rely on Facebook only — help them own a simple site that converts better than a social feed."
    if no_website and local_service:
        return "No site yet — offer a fast local-service site built to drive calls and trust."
    if no_website:
        return "No website — offer a simple professional site so customers can find them outside social."
    if weak_website:
        return "Site feels dated or weak on mobile — pitch clarity, speed, and stronger calls-to-action."
    if local_service:
        return "Local service business — emphasize maps, calls, and trust on mobile."
    return "Lead with how a cleaner site can turn more visitors into calls and bookings."


def _append_enrichment_log(scout_dir: Path, payload: dict[str, Any]) -> None:
    path = scout_dir / "data" / "enrich_log.jsonl"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"at": datetime.now(timezone.utc).isoformat(), **payload}, default=str)
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def run_lead_enrichment(
    req: EnrichLeadRequest,
    *,
    scout_dir: Path | None = None,
    log: LogFn = None,
) -> EnrichedLead:
    """
    Main entry: partial lead → enriched normalized record.
    Does not invent emails/phones; only extracts public signals.
    """
    scout_root = scout_dir or Path(__file__).resolve().parent
    raw_signals: dict[str, Any] = {"steps": []}

    business_name = _trim(req.business_name) or "Unknown business"
    city = _trim(req.city) or None
    state = _trim(req.state) or None
    source_url = _trim(req.source_url) or None
    facebook_in = _trim(req.facebook_url) or None
    if not facebook_in and source_url and is_facebook_url(source_url):
        facebook_in = source_url

    # Website from explicit source_url when it's a normal site
    website_guess: str | None = None
    if source_url and is_standalone_website(source_url):
        website_guess = source_url if source_url.startswith("http") else f"https://{source_url}"

    from . import places_client

    lat: float | None = None
    lng: float | None = None
    if city:
        geo = places_client.geocode(f"{city} {state or ''}".strip(), log=log)
        if geo:
            lat, lng = float(geo[0]), float(geo[1])
            raw_signals["steps"].append("geocoded_city")

    query = " ".join(x for x in [business_name, city or "", state or ""] if x).strip()
    places_results: list[dict[str, Any]] = []
    places_hit = False
    best_place: dict[str, Any] | None = None
    match_conf = 0.0

    if query and places_client.PLACES_ENABLED:
        try:
            places_results = places_client.text_search_new(
                query, lat, lng, radius_meters=min(50000.0, 1609.34 * 25), max_results=6, log=log
            )
            places_hit = bool(places_results)
            raw_signals["steps"].append("places_text_search")
        except Exception as e:
            _log(log, f"places search failed: {e}")
            raw_signals["places_error"] = str(e)

    if places_results:
        scored: list[tuple[float, dict[str, Any]]] = []
        for p in places_results:
            mc = compute_match_confidence(
                business_name=business_name,
                city=city or "",
                state=state or "",
                input_phone="",
                input_website=website_guess or "",
                place=p,
            )
            scored.append((mc, p))
        scored.sort(key=lambda x: x[0], reverse=True)
        match_conf, best_place = scored[0]
        raw_signals["place_candidates"] = len(places_results)
        raw_signals["best_match_confidence"] = match_conf

    phone: str | None = None
    email = None
    email_source = None
    website: str | None = website_guess
    contact_page: str | None = None
    category: str | None = None
    place_id: str | None = None

    if best_place:
        place_id = str(best_place.get("place_id") or best_place.get("id") or "") or None
        if not website:
            w = _trim(str(best_place.get("website") or ""))
            if w and is_standalone_website(w):
                website = w if w.startswith("http") else f"https://{w}"
        ph = _trim(str(best_place.get("phone") or ""))
        if ph:
            phone = ph
        category = _trim(str(best_place.get("category") or "")) or category
        addr = str(best_place.get("address") or "")
        if addr and (not city or not state):
            # light parse: last two parts often city, ST zip — skip heavy parse
            parts = [x.strip() for x in addr.split(",") if x.strip()]
            if len(parts) >= 2 and not city:
                city = city or parts[-2]
            if len(parts) >= 1 and not state:
                m = re.search(r"\b([A-Z]{2})\b", parts[-1])
                if m:
                    state = state or m.group(1)

    investigation: dict[str, Any] | None = None
    crawl = os.environ.get("SCOUT_ENRICH_CRAWL_INTERNAL", "").strip().lower() in {"1", "true", "yes"}
    if website and is_standalone_website(website):
        try:
            from .investigator import investigate

            investigation = investigate(
                website,
                crawl_internal=crawl,
                timeout=int(os.environ.get("SCOUT_ENRICH_TIMEOUT", "12") or "12"),
                screenshot_dir=None,
            )
            raw_signals["steps"].append("investigate_website")
            inv_email = (investigation.get("emails") or [])[:1]
            if inv_email:
                email = _trim(inv_email[0])
                email_source = _trim(str(investigation.get("email_source") or "site"))
            inv_phones = investigation.get("phones") or []
            if inv_phones:
                p0 = _trim(str(inv_phones[0]))
                if p0 and not phone:
                    phone = p0
            cp = investigation.get("contact_page") or (investigation.get("contact_matrix") or {}).get(
                "contact_page"
            )
            if cp:
                contact_page = _trim(str(cp))
            soc = investigation.get("social") or {}
            fb = _trim(str(soc.get("facebook") or ""))
            if fb and not facebook_in:
                facebook_in = fb if fb.startswith("http") else f"https://{fb}"
            if not category and best_place:
                pass
        except Exception as e:
            _log(log, f"investigate failed: {e}")
            raw_signals["investigate_error"] = str(e)

    has_real_website = bool(website and is_standalone_website(website))
    fb_url = facebook_in if _trim(facebook_in or "") else None
    has_facebook = bool(fb_url)

    ws = None
    if investigation:
        ws = investigation.get("website_score")
        if isinstance(ws, str) and ws.isdigit():
            ws = int(ws)
        elif isinstance(ws, float):
            ws = int(ws)

    weak = classify_weak_website(investigation, ws if isinstance(ws, int) else None)
    polished = classify_polished_site(ws if isinstance(ws, int) else None, investigation)

    local_svc = classify_local_service(business_name, category)
    facebook_only = has_facebook and not has_real_website
    no_website = not has_real_website

    source_conf = compute_source_confidence(
        req.source_type,
        has_source_url=bool(source_url),
        has_facebook_url=has_facebook,
        match_confidence=match_conf if best_place else 0.0,
        places_hit=places_hit,
    )

    strong_target = (facebook_only or no_website) and (local_svc or match_conf >= 0.45)

    tags = build_web_design_tags(
        has_facebook=has_facebook,
        has_real_website=has_real_website,
        has_phone=bool(_trim(phone)),
        has_email=bool(_trim(email)),
        weak_website=weak,
        polished=polished,
        local_service=local_svc,
        strong_target=strong_target,
    )

    score = score_web_design_lead(
        has_real_website=has_real_website,
        facebook_only=facebook_only,
        no_website=no_website,
        weak_website=weak,
        polished=polished,
        local_service=local_svc,
        has_phone=bool(_trim(phone)),
        has_email=bool(_trim(email)),
        match_confidence=match_conf if best_place else 0.25,
    )

    why = _why_string(
        facebook_only=facebook_only,
        no_website=no_website,
        weak_website=weak,
        polished=polished,
        local_service=local_svc,
        has_email=bool(email),
        has_phone=bool(phone),
    )

    bcm = _best_contact_method(email, phone, fb_url, contact_page)
    next_move = _best_next_move(bcm)
    pitch = _pitch_angle(
        facebook_only=facebook_only,
        no_website=no_website,
        weak_website=weak,
        local_service=local_svc,
    )

    norm_web = normalized_website_key(website)

    lead = EnrichedLead(
        business_name=business_name,
        source_type=req.source_type,
        source_url=source_url,
        facebook_url=fb_url,
        website=website,
        normalized_website=norm_web,
        phone=phone,
        email=email,
        email_source=email_source,
        contact_page=contact_page,
        city=city,
        state=state,
        category=category,
        tags=tags,
        score=score,
        why_this_lead_is_here=why,
        best_contact_method=bcm,  # type: ignore[arg-type]
        best_next_move=next_move,
        pitch_angle=pitch,
        source_confidence=round(source_conf, 3),
        match_confidence=round(match_conf if best_place else 0.0, 3),
        raw_signals={
            **raw_signals,
            "has_places_match": bool(best_place),
            "website_score": ws,
            "crawl_internal": crawl,
        },
        place_id=place_id,
    )

    # Optional persistence (JSONL, does not touch case files)
    h = hashlib.sha256(
        json.dumps(
            {"n": business_name, "c": city, "s": state, "u": source_url},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    _append_enrichment_log(
        scout_root,
        {
            "fingerprint": h,
            "score": score,
            "tags": tags,
            "place_id": place_id,
            "match_confidence": lead.match_confidence,
        },
    )

    return lead
