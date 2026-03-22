"""
Web-design sales classification — explicit tags for MixedMakerShop-style outreach.
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

SERVICE_KEYWORDS = (
    "roof",
    "roofing",
    "hvac",
    "plumb",
    "plumbing",
    "lawn",
    "landscaping",
    "electric",
    "cleaning",
    "pressure wash",
    "contractor",
    "salon",
    "barber",
    "auto repair",
    "mechanic",
    "pest",
    "locksmith",
    "moving",
    "tree",
    "fence",
    "pool",
    "restaurant",
    "cafe",
    "coffee",
    "diner",
    "church",
    "gym",
    "fitness",
    "dental",
    "law ",
    " attorney",
)


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def is_facebook_url(url: str | None) -> bool:
    u = _norm(url)
    return "facebook." in u or "fb.com" in u


def is_standalone_website(url: str | None) -> bool:
    raw = str(url or "").strip()
    if not raw:
        return False
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        h = urlparse(raw).hostname or ""
        hl = h.lower()
        if "facebook." in hl or hl.endswith("fb.com"):
            return False
        return bool(hl and "." in hl)
    except Exception:
        return False


def classify_local_service(business_name: str, category: str | None) -> bool:
    hay = f"{_norm(business_name)} {_norm(category)}"
    return any(k in hay for k in SERVICE_KEYWORDS)


def classify_weak_website(investigation: dict[str, Any] | None, website_score: int | None) -> bool:
    if website_score is not None and website_score < 55:
        return True
    if not investigation:
        return False
    issues = investigation.get("audit_issues") or investigation.get("strongest_problems") or []
    if isinstance(issues, list) and len(issues) >= 3:
        return True
    wa = investigation.get("website_audit") or {}
    checks = wa.get("checks") if isinstance(wa, dict) else {}
    if isinstance(checks, dict):
        if checks.get("missing_viewport_meta") is True:
            return True
        if checks.get("missing_meta_description") is True:
            return True
    if investigation.get("viewport_ok") is False:
        return True
    if investigation.get("outdated_design_clues"):
        return True
    return False


def classify_polished_site(website_score: int | None, investigation: dict[str, Any] | None) -> bool:
    if website_score is not None and website_score >= 78:
        return True
    inv = investigation or {}
    if inv.get("fetch_ok") is False:
        return False
    ws = inv.get("website_score")
    if isinstance(ws, (int, float)) and float(ws) >= 78:
        return True
    return False


def build_web_design_tags(
    *,
    has_facebook: bool,
    has_real_website: bool,
    has_phone: bool,
    has_email: bool,
    weak_website: bool,
    polished: bool,
    local_service: bool,
    strong_target: bool,
) -> list[str]:
    tags: list[str] = []
    if has_facebook and not has_real_website:
        tags.append("facebook_only")
    if not has_real_website:
        tags.append("no_website_opportunity")
    if has_real_website and weak_website and not polished:
        tags.append("weak_website")
    if has_phone:
        tags.append("has_phone")
    if has_email:
        tags.append("has_email")
    if local_service:
        tags.append("local_service_business")
    if strong_target:
        tags.append("strong_web_design_target")
    return list(dict.fromkeys(tags))
