"""
Deep website investigator — crawls homepage + internal pages, extracts contact
data, platform clues, owner names, and builds a contact matrix.

Used by Morning Runner to build detailed research per opportunity.
"""
import re
import time
from pathlib import Path
import urllib.request
import urllib.error
from urllib.parse import urljoin, urlparse
from typing import Any

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) MassiveBrainInvestigator/2.0"

# Paths to probe for contact, menu, staff, etc.
CRAWL_PATHS = [
    "/contact", "/contact-us", "/contactus", "/get-in-touch",
    "/about", "/about-us", "/aboutus",
    "/menu", "/our-menu", "/food", "/food-menu", "/menus",
    "/location", "/locations", "/find-us",
    "/order", "/order-online", "/orderonline", "/shop",
    "/reservations", "/book", "/book-a-table", "/reserve",
    "/events", "/calendar", "/upcoming-events",
    "/donate", "/donations", "/give",
    "/staff", "/team", "/our-team", "/meet-the-team", "/leadership",
]

INTERNAL_SCREENSHOT_PRIORITY = ["menu", "services", "about", "contact"]


def _fetch(url: str, timeout: int = 10) -> tuple[str, int, float | None]:
    """Fetch URL, return (html, status, elapsed_seconds)."""
    if not url.startswith("http"):
        url = "https://" + url
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(400000)
            elapsed = time.perf_counter() - started
            return body.decode("utf-8", errors="ignore"), resp.status, elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.perf_counter() - started
        return "", e.code, elapsed
    except Exception:
        elapsed = time.perf_counter() - started
        return "", 0, elapsed


def _extract_emails(html: str) -> list[str]:
    seen = set()
    out = []
    for m in re.finditer(r"mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", html, re.I):
        e = m.group(1).strip().lower()
        if e and e not in seen and "example" not in e and "sentry" not in e and "wix" not in e:
            seen.add(e)
            out.append(e)
    for m in re.finditer(r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b", html):
        e = m.group(1).strip().lower()
        if e and e not in seen and "example" not in e and "sentry" not in e and "wix" not in e:
            seen.add(e)
            out.append(e)
    return out[:10]


def _extract_footer_mailto_links(html: str) -> list[str]:
    footer_emails: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r"<footer[^>]*>(.*?)</footer>", html or "", flags=re.I | re.S):
        footer_html = m.group(1) or ""
        for em in _extract_emails(footer_html):
            val = str(em or "").strip().lower()
            if val and val not in seen:
                seen.add(val)
                footer_emails.append(val)
    return footer_emails[:5]


def _extract_phones(html: str) -> list[str]:
    seen = set()
    out = []
    for m in re.finditer(r"tel:([\d\s\-\(\)\+\.]+)", html):
        p = re.sub(r"\D", "", m.group(1))
        if len(p) >= 10 and p not in seen:
            seen.add(p)
            out.append(m.group(1).strip())
    for m in re.finditer(r"\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}", html):
        p = re.sub(r"\D", "", m.group(0))
        if len(p) == 10 and p not in seen:
            seen.add(p)
            out.append(m.group(0).strip())
    return out[:5]


def _extract_social(html: str) -> dict[str, str]:
    lower = html.lower()
    social = {}
    patterns = [
        (r"facebook\.com/[a-zA-Z0-9._\-/]+", "facebook"),
        (r"instagram\.com/[a-zA-Z0-9._\-/]+", "instagram"),
        (r"twitter\.com/[a-zA-Z0-9_]+", "twitter"),
        (r"linkedin\.com/[a-zA-Z0-9/\-]+", "linkedin"),
        (r"youtube\.com/[a-zA-Z0-9_\-\/]+", "youtube"),
    ]
    for pat, key in patterns:
        for m in re.finditer(pat, lower, re.I):
            raw = m.group(0)
            url = raw if raw.startswith("http") else "https://" + raw
            url = url.split("?")[0].rstrip("/")
            if key not in social:
                social[key] = url
    return social


def _extract_reservation_order_links(html: str, base: str) -> dict[str, str]:
    found = {}
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
        href = m.group(1).strip()
        if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full = urljoin(base, href)
        path = urlparse(full).path.lower()
        if not found.get("reservations") and any(x in path for x in ["reservation", "book", "reserve", "opentable", "tock"]):
            found["reservations"] = full
        if not found.get("order") and any(x in path for x in ["order", "shop", "buy", "cart", "checkout"]):
            found["order"] = full
    return found


def _extract_contact_form_url(html: str, base: str) -> str | None:
    for m in re.finditer(r"<form\b[^>]*\baction=[\"']([^\"']+)[\"'][^>]*>", html or "", re.I):
        action = (m.group(1) or "").strip()
        if not action:
            continue
        if action.startswith("javascript:") or action.startswith("#"):
            continue
        return urljoin(base, action)
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html or "", re.I):
        href = (m.group(1) or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        full = urljoin(base, href)
        lower = full.lower()
        if any(k in lower for k in ["/contact", "contact-us", "get-in-touch", "/support"]):
            return full
    return None


def _normalize_title(raw: str) -> str:
    title = (raw or "").strip().lower()
    mapping = {
        "co founder": "co-founder",
        "cofounder": "co-founder",
        "ceo": "ceo",
        "owner": "owner",
        "founder": "founder",
        "co-founder": "co-founder",
        "director": "director",
        "manager": "manager",
        "pastor": "pastor",
    }
    return mapping.get(title, title)


def _extract_owner_candidates(html: str) -> list[dict[str, str]]:
    """
    Extract likely owner/decision-maker mentions from text snippets.
    Returns list of {name, title}.
    """
    titles = ["owner", "founder", "co-founder", "co founder", "ceo", "director", "manager", "pastor"]
    seen: set[tuple[str, str]] = set()
    candidates: list[dict[str, str]] = []

    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.I | re.S)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.I | re.S)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    name_pat = r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})"

    patterns: list[tuple[str, str]] = []
    for title in titles:
        t = re.escape(title)
        patterns.extend(
            [
                (title, rf"\b{t}\b\s*[:\-]\s*{name_pat}\b"),
                (title, rf"\b{name_pat}\b\s*[—\-:,]\s*{t}\b"),
                (title, rf"\b{t}\b\s+(?:is|is\s+our|at)\s+{name_pat}\b"),
            ]
        )
    patterns.extend(
        [
            ("founder", rf"\bfounded\s+by\s+{name_pat}\b"),
            ("founder", rf"\bour\s+founder\s+{name_pat}\b"),
            ("manager", rf"\bmanaged\s+by\s+{name_pat}\b"),
        ]
    )

    for raw_title, pat in patterns:
        for m in re.finditer(pat, cleaned, re.I):
            groups = [g for g in m.groups() if g]
            if not groups:
                continue
            # Name is always the last capturing group in our patterns.
            name = groups[-1].strip()
            if len(name) < 5:
                continue
            title = _normalize_title(raw_title)
            key = (name.lower(), title)
            if key in seen:
                continue
            seen.add(key)
            candidates.append({"name": name, "title": title})
    return candidates[:10]


def _extract_owner_names(html: str) -> list[str]:
    return [c["name"] for c in _extract_owner_candidates(html)[:5]]


def _extract_internal_links(html: str, base: str) -> dict[str, str]:
    found = {}
    path_hints = {
        "menu": ["menu", "our-menu", "food", "food-menu"],
        "about": ["about", "about-us"],
        "contact": ["contact", "contact-us", "get-in-touch"],
        "order": ["order", "order-online", "shop"],
        "reservations": ["reservations", "book", "reserve"],
        "events": ["events", "calendar"],
        "donate": ["donate", "donations", "give"],
        "location": ["location", "locations", "find-us", "directions"],
        "staff": ["staff", "team", "our-team", "meet"],
    }
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
        href = m.group(1).strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        full = urljoin(base, href)
        path = urlparse(full).path.rstrip("/").lower() or "/"
        for key, hints in path_hints.items():
            if key in found:
                continue
            for h in hints:
                if h in path or path.endswith(h) or path.endswith(h + "/"):
                    found[key] = full
                    break
    return found


def _extract_nav_items(html: str) -> list[str]:
    items = []
    for tag in ["nav", "header"]:
        for nav_match in re.finditer(rf"<{tag}[^>]*>(.*?)</{tag}>", html, re.I | re.S):
            nav_html = nav_match.group(1)
            for m in re.finditer(r"<a[^>]*>([^<]{2,50})</a>", nav_html):
                t = re.sub(r"\s+", " ", m.group(1)).strip()
                if t and t not in items:
                    items.append(t)
    return items[:20]


def _detect_platform(html: str) -> str | None:
    lower = html.lower()
    if "wp-content" in lower or "wordpress" in lower or "/wp-includes/" in lower:
        return "WordPress"
    if "wix.com" in lower or "wixstatic" in lower or "parastorage" in lower:
        return "Wix"
    if "weebly" in lower or "editmysite" in lower:
        return "Weebly"
    if "squarespace" in lower:
        return "Squarespace"
    if "shopify" in lower or "cdn.shopify" in lower:
        return "Shopify"
    return None


def _analyze_page(html: str, url: str) -> dict[str, Any]:
    """Analyze one page HTML and return extraction results."""
    lower = html.lower()
    return {
        "emails": _extract_emails(html),
        "footer_emails": _extract_footer_mailto_links(html),
        "phones": _extract_phones(html),
        "social": _extract_social(html),
        "internal_links": _extract_internal_links(html, url),
        "reservation_order": _extract_reservation_order_links(html, url),
        "contact_form_url": _extract_contact_form_url(html, url),
        "owner_names": _extract_owner_names(html),
        "owner_candidates": _extract_owner_candidates(html),
        "nav_items": _extract_nav_items(html),
        "platform": _detect_platform(html),
        "viewport_ok": "viewport" in lower and 'name="viewport"' in lower,
        "tap_to_call_present": "tel:" in lower,
        "contact_form_present": ("contact" in lower or "form" in lower) and ("submit" in lower or "mailto:" in lower or "action=" in lower),
        "menu_visibility": sum(1 for w in ["menu", "breakfast", "lunch", "dinner", "special"] if w in lower) >= 2,
        "hours_visibility": sum(1 for w in ["hours", "open", "closed", "monday", "tuesday"] if w in lower) >= 2,
        "directions_visibility": any(w in lower for w in ["map", "directions", "location", "find us"]),
        "title": None,
        "meta_description": None,
    }


def _fetch_profile_contact_hints(profile_url: str, timeout: int = 8) -> dict[str, Any]:
    html, status, _ = _fetch(profile_url, timeout=timeout)
    if not html or status == 0:
        return {"emails": [], "phones": [], "social": {}, "contact_form_url": None}
    page = _analyze_page(html, profile_url)
    emails = list(dict.fromkeys((page.get("emails") or []) + (page.get("footer_emails") or [])))
    return {
        "emails": emails[:10],
        "phones": page.get("phones") or [],
        "social": page.get("social") or {},
        "contact_form_url": page.get("contact_form_url"),
    }


def _get_title_meta(html: str) -> tuple[str | None, str | None]:
    title = None
    meta = None
    m = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
    if m:
        title = re.sub(r"\s+", " ", m.group(1)).strip()[:250]
    m2 = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html, re.I
    )
    if m2:
        meta = m2.group(1).strip()[:400]
    return title, meta


def _estimate_text_content_length(html: str) -> int:
    cleaned = re.sub(r"<script[^>]*>.*?</script>", " ", html or "", flags=re.I | re.S)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", " ", cleaned, flags=re.I | re.S)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return len(cleaned)


def _count_images(html: str) -> int:
    return len(re.findall(r"<img\b", html or "", flags=re.I))


def _extract_image_urls(html: str, base_url: str, max_images: int = 10) -> list[str]:
    urls: list[str] = []
    for m in re.finditer(r"<img\b[^>]*\bsrc=[\"']([^\"']+)[\"'][^>]*>", html or "", flags=re.I):
        src = (m.group(1) or "").strip()
        if not src or src.startswith("data:") or src.startswith("blob:"):
            continue
        full = urljoin(base_url, src)
        if full not in urls:
            urls.append(full)
        if len(urls) >= max_images:
            break
    return urls


def _count_large_images_over_300kb(html: str, base_url: str, timeout: int = 6, max_checks: int = 8) -> int:
    candidates = _extract_image_urls(html, base_url, max_images=max_checks)
    large_count = 0
    for image_url in candidates:
        size_bytes: int | None = None
        try:
            req = urllib.request.Request(
                image_url,
                headers={"User-Agent": USER_AGENT},
                method="HEAD",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_len = resp.headers.get("Content-Length")
                if raw_len is not None:
                    size_bytes = int(raw_len)
        except Exception:
            try:
                req = urllib.request.Request(
                    image_url,
                    headers={"User-Agent": USER_AGENT},
                    method="GET",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw_len = resp.headers.get("Content-Length")
                    if raw_len is not None:
                        size_bytes = int(raw_len)
            except Exception:
                continue
        if size_bytes is not None and size_bytes > (300 * 1024):
            large_count += 1
    return large_count


def _count_broken_links_from_html(html: str, base_url: str, timeout: int = 6, max_checks: int = 6) -> int:
    candidates: list[str] = []
    for m in re.finditer(r'href=["\']([^"\']+)["\']', html or "", re.I):
        href = (m.group(1) or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        full = urljoin(base_url, href)
        if full not in candidates:
            candidates.append(full)
        if len(candidates) >= max_checks:
            break
    broken = 0
    for link in candidates:
        _, status, _ = _fetch(link, timeout=timeout)
        if status == 0 or status >= 400:
            broken += 1
    return broken


def _clamp_score(v: float) -> int:
    return int(max(0, min(100, round(v))))


def auditWebsite(html: str, metadata: dict[str, Any], screenshots: dict[str, str | None]) -> dict[str, Any]:
    """
    Lightweight AI-style website audit (heuristic scoring).
    Returns category scores (0-100), overall website_score, and audit issues.
    """
    lower = (html or "").lower()
    debug_log = metadata.get("debug_log")
    if isinstance(debug_log, list):
        debug_log.append("website audit started")
    issues: list[str] = []
    structured_issues: list[dict[str, str]] = []

    def add_issue(label: str, category: str | None = None) -> None:
        text = str(label or "").strip()
        if not text:
            return
        if text not in issues:
            issues.append(text)
        cat = str(category or "").strip()
        if cat:
            key = (cat.lower(), text.lower())
            existing_keys = {
                (str(i.get("category") or "").strip().lower(), str(i.get("issue") or "").strip().lower())
                for i in structured_issues
            }
            if key not in existing_keys:
                structured_issues.append({"category": cat, "issue": text})

    viewport_ok = metadata.get("viewport_ok") is True
    tap_to_call = metadata.get("tap_to_call_present") is True
    menu_visibility = metadata.get("menu_visibility") is True
    contact_form_present = metadata.get("contact_form_present") is True
    outdated_design = metadata.get("outdated_design_clues") is True
    text_heavy = metadata.get("text_heavy_clues") is True
    has_email = bool(metadata.get("emails"))
    has_phone = bool(metadata.get("phones"))
    has_contact_page = bool(metadata.get("contact_page"))
    has_ordering_or_booking = bool(metadata.get("order_link")) or bool(metadata.get("reservation_link"))
    nav_items = metadata.get("navigation_items") or metadata.get("page_navigation_items") or []
    cta_patterns = [
        r"\bbook now\b",
        r"\bcall now\b",
        r"\bget quote\b",
        r"\brequest (?:a )?quote\b",
        r"\bschedule (?:now|today)\b",
        r"\bcontact us\b",
        r"\bstart now\b",
        r"\bget started\b",
    ]
    cta_present = bool(any(re.search(pat, lower) for pat in cta_patterns))
    homepage_phone_present = bool(metadata.get("homepage_phone_present"))
    homepage_load_seconds = metadata.get("homepage_load_seconds")
    missing_meta_title = bool(metadata.get("missing_meta_title"))
    missing_meta_description = bool(metadata.get("missing_meta_description"))
    ssl_ok = metadata.get("ssl_ok")
    image_count = metadata.get("image_count")
    broken_links_count = int(metadata.get("broken_links_count") or 0)
    large_images_over_300kb = int(metadata.get("large_images_over_300kb") or 0)
    contact_link_depth = int(metadata.get("contact_link_depth") or (1 if has_contact_page else 3))
    h1_count = len(re.findall(r"<h1\b", html or "", flags=re.I))
    img_without_alt_count = len(
        re.findall(r"<img\b(?![^>]*\balt=)[^>]*>", html or "", flags=re.I)
    )
    inline_style_count = len(re.findall(r"\sstyle\s*=", html or "", flags=re.I))
    small_text_detected = (
        bool(metadata.get("text_heavy_clues"))
        or bool(re.search(r"font-size\\s*:\\s*(?:[0-9]|1[0-1])px", lower))
    )
    duplicate_title_signals = bool(re.search(r"<title>\s*([^<]+?)\s*[|\-]\s*\1\s*</title>", html or "", flags=re.I))
    render_blocking_scripts = len(
        re.findall(r"<script\b(?![^>]*\b(?:defer|async)\b)[^>]*>", html or "", flags=re.I)
    )
    image_optimization_issues = (
        img_without_alt_count > 0
        or bool(re.search(r"<img\\b(?![^>]*\\bloading=)[^>]*>", html or "", flags=re.I))
        or large_images_over_300kb > 0
    )
    mobile_pagespeed_score = max(
        0,
        min(
            100,
            int(
                round(
                    100
                    - (12 if homepage_load_seconds and float(homepage_load_seconds) > 2.5 else 0)
                    - (20 if homepage_load_seconds and float(homepage_load_seconds) > 4.0 else 0)
                    - (16 if render_blocking_scripts >= 4 else 0)
                    - (12 if image_optimization_issues else 0)
                    - (18 if not viewport_ok else 0)
                )
            ),
        ),
    )

    desktop_shot = bool(screenshots.get("desktop_homepage_path"))
    mobile_shot = bool(screenshots.get("mobile_homepage_path"))

    mobile_score = 85.0
    if not viewport_ok:
        mobile_score -= 35
        add_issue("Missing viewport meta", "Mobile UX")
    if not tap_to_call:
        mobile_score -= 10
        add_issue("Missing mobile call button", "Conversion")
    if not mobile_shot:
        mobile_score -= 5
    if small_text_detected:
        mobile_score -= 10
        add_issue("Small text detected", "Mobile UX")

    design_score = 80.0
    if outdated_design:
        design_score -= 25
        add_issue("Outdated layout signals detected", "Site Structure")
    if text_heavy:
        design_score -= 20
        add_issue("Text heavy homepage", "Mobile UX")
    if not desktop_shot:
        design_score -= 5
    if inline_style_count > 25:
        design_score -= 10
        add_issue("Excessive inline styles detected", "Site Structure")

    navigation_score = 80.0
    if not menu_visibility:
        navigation_score -= 20
        add_issue("Difficult navigation", "Site Structure")
    if len(nav_items) < 3:
        navigation_score -= 15
    if not has_contact_page:
        navigation_score -= 10
        add_issue("Contact link depth is too high", "Conversion")

    conversion_score = 82.0
    if not cta_present:
        conversion_score -= 20
        add_issue("Missing call to action above the fold", "Conversion")
    if not homepage_phone_present:
        conversion_score -= 8
        add_issue("Homepage phone number not clearly visible", "Conversion")
    if not contact_form_present and not has_email and not has_phone:
        conversion_score -= 20
        add_issue("Contact information hard to find", "Conversion")
    if not has_ordering_or_booking:
        conversion_score -= 15
        add_issue("No booking or ordering system", "Conversion")
    if not tap_to_call:
        conversion_score -= 8
    if len(re.findall(r"(button|btn|cta)", lower)) >= 6 and re.search(r"(button|btn|cta).{0,24}(button|btn|cta)", lower):
        add_issue("Buttons too close for mobile taps", "Mobile UX")

    try:
        if homepage_load_seconds is not None and float(homepage_load_seconds) > 3.0:
            add_issue("Page load slow", "Mobile Performance")
    except Exception:
        pass
    if missing_meta_title:
        add_issue("Duplicate or weak page title signals", "SEO")
    if missing_meta_description:
        add_issue("Missing meta description", "SEO")
    if h1_count == 0:
        add_issue("Missing H1", "SEO")
    if img_without_alt_count > 0:
        add_issue("Missing alt tags on images", "SEO")
    if duplicate_title_signals:
        add_issue("Duplicate titles detected", "SEO")
    if ssl_ok is False:
        add_issue("Broken SSL / HTTP site", "Site Structure")
    if large_images_over_300kb > 0:
        add_issue("Images not optimized", "Mobile Performance")
    try:
        if image_count is not None and int(image_count) < 3:
            add_issue("Image optimization issues detected", "Mobile Performance")
    except Exception:
        pass
    if render_blocking_scripts >= 4:
        add_issue("Render-blocking scripts detected", "Mobile Performance")
    if broken_links_count > 0:
        add_issue("Broken links detected", "Site Structure")
    if mobile_pagespeed_score < 50:
        add_issue("Mobile PageSpeed score below 50", "Mobile Performance")

    mobile_score_i = _clamp_score(mobile_score)
    design_score_i = _clamp_score(design_score)
    navigation_score_i = _clamp_score(navigation_score)
    conversion_score_i = _clamp_score(conversion_score)
    website_score = _clamp_score(
        (mobile_score_i + design_score_i + navigation_score_i + conversion_score_i) / 4.0
    )

    deduped_issues = list(dict.fromkeys(issues))
    website_audit = {
        "mobile_score": mobile_pagespeed_score,
        "load_time": homepage_load_seconds,
        "issues": deduped_issues[:12],
        "checks": {
            "mobile_pagespeed_score": mobile_pagespeed_score,
            "load_time_seconds": homepage_load_seconds,
            "large_images_detected": bool(large_images_over_300kb > 0),
            "large_images_over_300kb": large_images_over_300kb,
            "render_blocking_scripts_detected": bool(render_blocking_scripts >= 4),
            "missing_meta_description": bool(missing_meta_description),
            "missing_h1": bool(h1_count == 0),
            "missing_alt_tags": bool(img_without_alt_count > 0),
            "duplicate_title_detected": bool(duplicate_title_signals),
            "missing_call_to_action": bool(not cta_present),
            "cta_present": bool(cta_present),
            "homepage_phone_visible": bool(homepage_phone_present),
            "missing_viewport_meta": bool(not viewport_ok),
            "small_text_detected": bool(small_text_detected),
            "buttons_too_close_detected": bool(
                any(
                    str(item.get("issue") or "").strip().lower() == "buttons too close for mobile taps"
                    for item in structured_issues
                )
            ),
            "layout_not_responsive": bool(not viewport_ok or small_text_detected),
            "booking_or_ordering_missing": bool(not has_ordering_or_booking),
            "contact_page_present": bool(has_contact_page),
            "contact_click_depth": int(contact_link_depth),
            "broken_links_count": int(broken_links_count),
            "outdated_layout_signals": bool(outdated_design or inline_style_count > 25),
            "excessive_inline_styles": bool(inline_style_count > 25),
            # Backward-compatible aliases
            "homepage_phone_present": bool(homepage_phone_present),
            "small_text_or_crowded_buttons": bool(
                small_text_detected
                or any(
                    str(item.get("issue") or "").strip().lower() == "buttons too close for mobile taps"
                    for item in structured_issues
                )
            ),
            "broken_internal_links_count": int(broken_links_count),
        },
    }
    if isinstance(debug_log, list):
        debug_log.append("website audit completed")
    return {
        "website_score": website_score,
        "mobile_score": mobile_score_i,
        "design_score": design_score_i,
        "navigation_score": navigation_score_i,
        "conversion_score": conversion_score_i,
        "mobile_pagespeed_score": mobile_pagespeed_score,
        "load_time": homepage_load_seconds,
        "image_optimization_issues": bool(image_optimization_issues),
        "large_images_over_300kb": int(large_images_over_300kb),
        "render_blocking_scripts": int(render_blocking_scripts),
        "missing_call_to_action": bool(not cta_present),
        "homepage_phone_present": bool(homepage_phone_present),
        "contact_link_depth": int(contact_link_depth),
        "missing_mobile_call_button": bool(not tap_to_call),
        "missing_viewport_meta": bool(not viewport_ok),
        "small_text_detected": bool(small_text_detected),
        "buttons_too_close": bool(
            any(
                str(item.get("issue") or "").strip().lower() == "buttons too close for mobile taps"
                for item in structured_issues
            )
        ),
        "missing_h1": bool(h1_count == 0),
        "missing_alt_tags": bool(img_without_alt_count > 0),
        "duplicate_titles": bool(duplicate_title_signals),
        "outdated_layout_signals": bool(outdated_design or inline_style_count > 25),
        "broken_links": bool(broken_links_count > 0),
        "excessive_inline_styles": bool(inline_style_count > 25),
        "website_issues": structured_issues,
        "audit_issues": deduped_issues,
        "detected_issues": deduped_issues,
        "website_audit": website_audit,
    }


def _capture_website_screenshots(
    homepage_url: str,
    contact_url: str | None,
    output_dir: Path,
    timeout: int,
    debug_log: list[str],
) -> dict[str, str | None]:
    screenshots = {
        "desktop_homepage_path": None,
        "mobile_homepage_path": None,
        "contact_page_path": None,
        # Backward-compatible alias used in older callsites.
        "internal_page_path": None,
    }
    debug_log.append("screenshot capture started")
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        debug_log.append(f"screenshot capture failed: playwright unavailable ({e})")
        return screenshots

    output_dir.mkdir(parents=True, exist_ok=True)
    desktop_path = output_dir / "desktop.png"
    mobile_path = output_dir / "mobile.png"
    internal_path = output_dir / "internal.png"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={"width": 1280, "height": 800})
            page = context.new_page()
            page.goto(homepage_url, wait_until="networkidle", timeout=timeout * 1000)
            page.screenshot(path=str(desktop_path), full_page=True)
            screenshots["desktop_homepage_path"] = str(desktop_path)
            debug_log.append("desktop screenshot saved")
            context.close()

            mobile_context = browser.new_context(
                viewport={"width": 390, "height": 844},
                user_agent=(
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
                ),
                is_mobile=True,
            )
            mobile_page = mobile_context.new_page()
            mobile_page.goto(homepage_url, wait_until="networkidle", timeout=timeout * 1000)
            mobile_page.screenshot(path=str(mobile_path), full_page=True)
            screenshots["mobile_homepage_path"] = str(mobile_path)
            debug_log.append("mobile screenshot saved")
            mobile_context.close()

            if contact_url:
                internal_context = browser.new_context(viewport={"width": 1280, "height": 800})
                internal_page = internal_context.new_page()
                internal_page.goto(contact_url, wait_until="networkidle", timeout=timeout * 1000)
                internal_page.screenshot(path=str(internal_path), full_page=True)
                screenshots["contact_page_path"] = str(internal_path)
                screenshots["internal_page_path"] = str(internal_path)
                debug_log.append("contact screenshot saved")
                internal_context.close()

            browser.close()
        debug_log.append("screenshot capture completed")
    except Exception as e:
        debug_log.append(f"screenshot capture failed: {e}")

    return screenshots


def investigate(
    url: str,
    crawl_internal: bool = True,
    timeout: int = 12,
    screenshot_dir: str | None = None,
    google_profile_url: str | None = None,
) -> dict[str, Any]:
    """
    Deep-investigate a website: homepage + internal pages.
    Extracts emails, phones, social, owner names, platform, contact matrix.
    Returns result with debug_log.
    """
    if not url.startswith("http"):
        url = "https://" + url
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    ssl_ok = parsed.scheme == "https"
    debug_log: list[str] = []

    home_html, status, homepage_load_seconds = _fetch(url, timeout)
    if not home_html:
        debug_log.append("website: fetch failed")
        return {
            "url": url,
            "ssl_ok": ssl_ok,
            "fetch_ok": False,
            "debug_log": debug_log,
            "emails": [], "phones": [], "social": {},
            "contact_page": None,
            "internal_links_found": {},
            "discovered_pages": [],
            "owner_names": [],
            "owner_name": None,
            "owner_title": None,
            "owner_source_page": None,
            "reservation_link": None, "order_link": None,
            "contact_matrix": {},
            "platform_used": None,
            "homepage_title": None,
            "meta_description": None,
            "viewport_ok": None,
            "tap_to_call_present": None,
            "contact_form_present": None,
            "menu_visibility": None,
            "hours_visibility": None,
            "directions_visibility": None,
            "text_heavy_clues": None,
            "website_score": None,
            "mobile_score": None,
            "design_score": None,
            "navigation_score": None,
            "conversion_score": None,
            "website_issues": [],
            "audit_issues": [],
            "website_audit": {
                "mobile_score": None,
                "load_time": homepage_load_seconds,
                "issues": ["Website could not be fetched"],
                "checks": {
                    "mobile_pagespeed_score": None,
                    "load_time_seconds": homepage_load_seconds,
                    "large_images_over_300kb": 0,
                    "missing_meta_description": None,
                    "missing_h1": None,
                    "cta_present": None,
                    "homepage_phone_present": None,
                    "missing_viewport_meta": None,
                    "small_text_or_crowded_buttons": None,
                    "contact_page_present": None,
                    "contact_click_depth": None,
                    "broken_internal_links_count": None,
                },
            },
            "homepage_http_status": status,
            "homepage_load_seconds": homepage_load_seconds,
            "missing_meta_title": None,
            "missing_meta_description": None,
            "text_content_length": None,
            "image_count": None,
            "broken_links_count": None,
            "problems": ["Website could not be fetched"],
            "pitch": ["review manually"],
            "desktop_homepage_path": None,
            "mobile_homepage_path": None,
            "internal_page_path": None,
        }

    debug_log.append("website: fetched")

    all_emails: set[str] = set()
    all_phones: set[str] = set()
    all_social: dict[str, str] = {}
    all_internal: dict[str, str] = {}
    all_owner_names: set[str] = set()
    all_owner_candidates: list[dict[str, str | None]] = []
    reservation_link: str | None = None
    order_link: str | None = None
    contact_page_url: str | None = None
    contact_form_url: str | None = None
    discovered_pages: list[str] = []
    platform = None
    viewport_ok = False
    tap_to_call = False
    contact_form = False
    menu_vis = False
    hours_vis = False
    directions_vis = False
    page_nav_items: list[str] = []
    combined = home_html

    home_result = _analyze_page(home_html, url)
    title, meta = _get_title_meta(home_html)
    missing_meta_title = not bool((title or "").strip())
    missing_meta_description = not bool((meta or "").strip())
    text_content_length = _estimate_text_content_length(home_html)
    image_count = _count_images(home_html)
    large_images_over_300kb = _count_large_images_over_300kb(home_html, url, timeout=min(timeout, 8), max_checks=8)
    broken_links_count = _count_broken_links_from_html(home_html, url, timeout=min(timeout, 8))
    platform = home_result["platform"]
    viewport_ok = home_result["viewport_ok"]
    tap_to_call = home_result["tap_to_call_present"]
    contact_form = home_result["contact_form_present"]
    menu_vis = home_result["menu_visibility"]
    hours_vis = home_result["hours_visibility"]
    directions_vis = home_result["directions_visibility"]
    page_nav_items = home_result["nav_items"]

    for e in home_result["emails"]:
        all_emails.add(e)
    for e in home_result.get("footer_emails") or []:
        all_emails.add(e)
    for p in home_result["phones"]:
        all_phones.add(p)
    all_social.update(home_result["social"])
    all_internal.update(home_result["internal_links"])
    for n in home_result["owner_names"]:
        all_owner_names.add(n)
    for c in home_result.get("owner_candidates") or []:
        all_owner_candidates.append(
            {
                "name": c.get("name"),
                "title": c.get("title"),
                "source_page": url,
            }
        )
    if home_result["reservation_order"].get("reservations"):
        reservation_link = home_result["reservation_order"]["reservations"]
    if home_result["reservation_order"].get("order"):
        order_link = home_result["reservation_order"]["order"]
    if "contact" in all_internal:
        contact_page_url = all_internal["contact"]
    contact_form_url = home_result.get("contact_form_url") or None
    homepage_contact_link_present = bool(
        re.search(r'href=["\'][^"\']*(contact|contact-us|contactus|get-in-touch)[^"\']*["\']', home_html, flags=re.I)
    )
    homepage_phone_present = bool(home_result["phones"])

    # Crawl internal pages
    if crawl_internal:
        for path in CRAWL_PATHS:
            candidate = urljoin(base, path)
            if candidate.rstrip("/") == url.rstrip("/"):
                continue
            html, _, _ = _fetch(candidate, timeout=8)
            if html:
                discovered_pages.append(candidate)
                combined += "\n" + html
                pr = _analyze_page(html, candidate)
                for e in pr["emails"]:
                    all_emails.add(e)
                for e in pr.get("footer_emails") or []:
                    all_emails.add(e)
                for p in pr["phones"]:
                    all_phones.add(p)
                all_social.update(pr["social"])
                all_internal.update(pr["internal_links"])
                for n in pr["owner_names"]:
                    all_owner_names.add(n)
                for c in pr.get("owner_candidates") or []:
                    all_owner_candidates.append(
                        {
                            "name": c.get("name"),
                            "title": c.get("title"),
                            "source_page": candidate,
                        }
                    )
                if not contact_page_url and path.startswith("/contact"):
                    contact_page_url = candidate
                if not contact_form_url and pr.get("contact_form_url"):
                    contact_form_url = pr.get("contact_form_url")
                if not reservation_link and pr["reservation_order"].get("reservations"):
                    reservation_link = pr["reservation_order"]["reservations"]
                if not order_link and pr["reservation_order"].get("order"):
                    order_link = pr["reservation_order"]["order"]
                if not platform and pr["platform"]:
                    platform = pr["platform"]
                viewport_ok = viewport_ok or pr["viewport_ok"]
                tap_to_call = tap_to_call or pr["tap_to_call_present"]
                contact_form = contact_form or pr["contact_form_present"]
                menu_vis = menu_vis or pr["menu_visibility"]
                hours_vis = hours_vis or pr["hours_visibility"]
                directions_vis = directions_vis or pr["directions_visibility"]

    # Google Business profile link scan (best effort) for additional contact hints.
    if google_profile_url:
        try:
            debug_log.append("google profile scan started")
            gp = _fetch_profile_contact_hints(google_profile_url, timeout=min(timeout, 8))
            for e in gp.get("emails") or []:
                all_emails.add(str(e))
            for p in gp.get("phones") or []:
                all_phones.add(str(p))
            for key, value in (gp.get("social") or {}).items():
                if key not in all_social and value:
                    all_social[key] = value
            if not contact_form_url and gp.get("contact_form_url"):
                contact_form_url = str(gp.get("contact_form_url"))
            debug_log.append("google profile scan completed")
        except Exception as e:
            debug_log.append(f"google profile scan failed: {e}")

    # Facebook page scan (best effort) for public contact details.
    facebook_url = str(all_social.get("facebook") or "").strip()
    if facebook_url:
        try:
            debug_log.append("facebook page scan started")
            fb = _fetch_profile_contact_hints(facebook_url, timeout=min(timeout, 8))
            for e in fb.get("emails") or []:
                all_emails.add(str(e))
            for p in fb.get("phones") or []:
                all_phones.add(str(p))
            if not contact_form_url and fb.get("contact_form_url"):
                contact_form_url = str(fb.get("contact_form_url"))
            debug_log.append("facebook page scan completed")
        except Exception as e:
            debug_log.append(f"facebook page scan failed: {e}")

    debug_log.append(f"contact_page: {'found' if contact_page_url else 'not found'}")
    debug_log.append(f"emails: {'found (' + str(len(all_emails)) + ')' if all_emails else 'not found'}")
    debug_log.append(f"social_links: {'found' if all_social else 'not found'}")
    debug_log.append(f"owner_names: {'found (' + str(len(all_owner_names)) + ')' if all_owner_names else 'not found'}")

    h2_count = len(re.findall(r"<h2\b", combined.lower()))
    br_count = combined.lower().count("<br>")
    text_heavy = h2_count > 8 or br_count > 25

    problems = []
    pitch = []
    if not viewport_ok:
        problems.append("Mobile layout may not be optimized")
        pitch.append("improve mobile experience")
    if not tap_to_call:
        problems.append("No tap-to-call link found")
        pitch.append("add tap-to-call for mobile visitors")
    if not contact_form and not all_emails:
        problems.append("Contact method not obvious")
        pitch.append("make contact easier to find")
    if text_heavy:
        problems.append("Page may be text-heavy and harder to scan")
        pitch.append("improve visual hierarchy")
    if platform and "weebly" in (platform or "").lower():
        problems.append("Older platform/template detected")
        pitch.append("modernize layout")
    if not problems:
        problems = ["Site could be clearer and more conversion-focused"]
        pitch = ["create a cleaner mobile-friendly layout"]

    owner_priority = {
        "owner": 0,
        "founder": 1,
        "co-founder": 2,
        "ceo": 3,
        "director": 4,
        "manager": 5,
        "pastor": 6,
    }
    deduped_owner_hits: list[dict[str, str | None]] = []
    owner_seen = set()
    for hit in all_owner_candidates:
        nm = (hit.get("name") or "").strip()
        tt = (hit.get("title") or "").strip().lower()
        key = (nm.lower(), tt)
        if not nm or key in owner_seen:
            continue
        owner_seen.add(key)
        deduped_owner_hits.append(hit)
    deduped_owner_hits.sort(key=lambda h: owner_priority.get(str(h.get("title") or "").lower(), 99))
    selected_owner = deduped_owner_hits[0] if deduped_owner_hits else None

    contact_screenshot_url = contact_page_url

    shot_paths = {
        "desktop_homepage_path": None,
        "mobile_homepage_path": None,
        "contact_page_path": None,
        "internal_page_path": None,
    }
    if screenshot_dir:
        shot_paths = _capture_website_screenshots(
            homepage_url=url,
            contact_url=contact_screenshot_url,
            output_dir=Path(screenshot_dir),
            timeout=timeout,
            debug_log=debug_log,
        )

    audit = auditWebsite(
        combined,
        {
            "viewport_ok": viewport_ok,
            "tap_to_call_present": tap_to_call,
            "menu_visibility": menu_vis,
            "contact_form_present": contact_form,
            "outdated_design_clues": bool(platform and "weebly" in (platform or "").lower()),
            "text_heavy_clues": text_heavy,
            "emails": list(all_emails),
            "phones": list(all_phones),
            "contact_page": contact_page_url,
            "order_link": order_link,
            "reservation_link": reservation_link,
            "navigation_items": page_nav_items,
            "homepage_load_seconds": homepage_load_seconds,
            "missing_meta_title": missing_meta_title,
            "missing_meta_description": missing_meta_description,
            "ssl_ok": ssl_ok,
            "image_count": image_count,
            "broken_links_count": broken_links_count,
            "large_images_over_300kb": large_images_over_300kb,
            "homepage_phone_present": homepage_phone_present,
            "contact_link_depth": 1 if homepage_contact_link_present else 2 if contact_page_url else 3,
            "debug_log": debug_log,
        },
        shot_paths,
    )
    issues_for_pitch = list(dict.fromkeys((audit.get("audit_issues") or []) + problems))

    best_contact = (
        "email"
        if all_emails
        else "contact_page"
        if (contact_page_url or contact_form_url)
        else "phone"
        if all_phones
        else "facebook"
        if all_social.get("facebook")
        else "facebook"
        if all_social
        else "contact_page"
    )
    backup_contact = (
        "contact_page"
        if all_emails and (contact_page_url or contact_form_url)
        else "phone"
        if all_emails and all_phones
        else "phone"
        if (contact_page_url or contact_form_url) and all_phones
        else "facebook"
        if (contact_page_url or contact_form_url) and all_social.get("facebook")
        else None
    )

    contact_matrix = {
        "best_contact": best_contact,
        "best_contact_method": best_contact,
        "backup_contact": backup_contact,
        "backup_contact_method": backup_contact,
        "email": (list(all_emails)[:1] or [None])[0],
        "phone": (list(all_phones)[:1] or [None])[0],
        "contact_page": contact_page_url,
        "contact_form_url": contact_form_url,
        "facebook": all_social.get("facebook"),
        "instagram": all_social.get("instagram"),
        "linkedin": all_social.get("linkedin"),
        "owner_name": selected_owner.get("name") if selected_owner else (list(all_owner_names)[:1] or [None])[0],
        "owner_title": selected_owner.get("title") if selected_owner else None,
        "owner_source_page": selected_owner.get("source_page") if selected_owner else None,
        "phone_available": bool(all_phones),
        "contact_form_available": contact_form,
        "social_available": bool(all_social),
        "email_available": bool(all_emails),
    }

    return {
        "url": url,
        "ssl_ok": ssl_ok,
        "fetch_ok": True,
        "debug_log": debug_log,
        "homepage_title": title,
        "meta_description": meta,
        "platform_used": platform or "Custom",
        "viewport_ok": viewport_ok,
        "tap_to_call_present": tap_to_call,
        "contact_form_present": contact_form,
        "menu_visibility": menu_vis,
        "hours_visibility": hours_vis,
        "directions_visibility": directions_vis,
        "emails": list(all_emails)[:10],
        "phones": list(all_phones)[:5],
        "social": all_social,
        "contact_page": contact_page_url,
        "contact_form_url": contact_form_url,
        "internal_links_found": all_internal,
        "important_internal_links": all_internal,
        "discovered_pages": discovered_pages[:15],
        "owner_names": list(all_owner_names)[:5],
        "owner_name": selected_owner.get("name") if selected_owner else None,
        "owner_title": selected_owner.get("title") if selected_owner else None,
        "owner_source_page": selected_owner.get("source_page") if selected_owner else None,
        "owner_candidates": deduped_owner_hits[:10],
        "reservation_link": reservation_link,
        "order_link": order_link,
        "contact_matrix": contact_matrix,
        "page_navigation_items": page_nav_items,
        "navigation_items": page_nav_items,
        "text_heavy_clues": text_heavy,
        "outdated_design_clues": bool(platform and "weebly" in (platform or "").lower()),
        "problems": issues_for_pitch[:8],
        "strongest_problems": issues_for_pitch[:8],
        "pitch": pitch[:6],
        "website_score": audit.get("website_score"),
        "mobile_score": audit.get("mobile_score"),
        "design_score": audit.get("design_score"),
        "navigation_score": audit.get("navigation_score"),
        "conversion_score": audit.get("conversion_score"),
        "website_issues": audit.get("website_issues") or [],
        "audit_issues": audit.get("audit_issues") or [],
        "detected_issues": audit.get("detected_issues") or [],
        "website_audit": audit.get("website_audit") or {},
        "homepage_http_status": status,
        "homepage_load_seconds": homepage_load_seconds,
        "missing_meta_title": missing_meta_title,
        "missing_meta_description": missing_meta_description,
        "text_content_length": text_content_length,
        "image_count": image_count,
        "large_images_over_300kb": large_images_over_300kb,
        "broken_links_count": broken_links_count,
        "desktop_homepage_path": shot_paths.get("desktop_homepage_path"),
        "mobile_homepage_path": shot_paths.get("mobile_homepage_path"),
        "contact_page_path": shot_paths.get("contact_page_path"),
        "internal_page_path": shot_paths.get("internal_page_path"),
    }
