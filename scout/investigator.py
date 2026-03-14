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
        "phones": _extract_phones(html),
        "social": _extract_social(html),
        "internal_links": _extract_internal_links(html, url),
        "reservation_order": _extract_reservation_order_links(html, url),
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
    issues: list[str] = []

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
    cta_present = ("book" in lower or "order" in lower or "call now" in lower or "get quote" in lower)

    desktop_shot = bool(screenshots.get("desktop_homepage_path"))
    mobile_shot = bool(screenshots.get("mobile_homepage_path"))

    mobile_score = 85.0
    if not viewport_ok:
        mobile_score -= 35
        issues.append("poor mobile layout")
    if not tap_to_call:
        mobile_score -= 10
    if not mobile_shot:
        mobile_score -= 5

    design_score = 80.0
    if outdated_design:
        design_score -= 25
        issues.append("outdated design")
    if text_heavy:
        design_score -= 20
        issues.append("text-heavy homepage")
    if not desktop_shot:
        design_score -= 5

    navigation_score = 80.0
    if not menu_visibility:
        navigation_score -= 20
        issues.append("menu difficult to find")
    if len(nav_items) < 3:
        navigation_score -= 15
    if not has_contact_page:
        navigation_score -= 10

    conversion_score = 82.0
    if not cta_present:
        conversion_score -= 20
        issues.append("missing call-to-action")
    if not contact_form_present and not has_email and not has_phone:
        conversion_score -= 20
        issues.append("missing contact information")
    if not has_ordering_or_booking:
        conversion_score -= 15
        issues.append("no online ordering/booking")

    mobile_score_i = _clamp_score(mobile_score)
    design_score_i = _clamp_score(design_score)
    navigation_score_i = _clamp_score(navigation_score)
    conversion_score_i = _clamp_score(conversion_score)
    website_score = _clamp_score(
        (mobile_score_i + design_score_i + navigation_score_i + conversion_score_i) / 4.0
    )

    deduped_issues = list(dict.fromkeys(issues))
    return {
        "website_score": website_score,
        "mobile_score": mobile_score_i,
        "design_score": design_score_i,
        "navigation_score": navigation_score_i,
        "conversion_score": conversion_score_i,
        "audit_issues": deduped_issues,
        "detected_issues": deduped_issues,
    }


def _capture_website_screenshots(
    homepage_url: str,
    internal_url: str | None,
    output_dir: Path,
    timeout: int,
    debug_log: list[str],
) -> dict[str, str | None]:
    screenshots = {
        "desktop_homepage_path": None,
        "mobile_homepage_path": None,
        "internal_page_path": None,
    }
    debug_log.append("screenshot capture started")
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        debug_log.append(f"screenshot failed: playwright unavailable ({e})")
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
            debug_log.append("screenshot captured: desktop_homepage")
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
            debug_log.append("screenshot captured: mobile_homepage")
            mobile_context.close()

            if internal_url:
                internal_context = browser.new_context(viewport={"width": 1280, "height": 800})
                internal_page = internal_context.new_page()
                internal_page.goto(internal_url, wait_until="networkidle", timeout=timeout * 1000)
                internal_page.screenshot(path=str(internal_path), full_page=True)
                screenshots["internal_page_path"] = str(internal_path)
                debug_log.append("screenshot captured: key_internal_page")
                internal_context.close()

            browser.close()
    except Exception as e:
        debug_log.append(f"screenshot failed: {e}")

    return screenshots


def investigate(
    url: str,
    crawl_internal: bool = True,
    timeout: int = 12,
    screenshot_dir: str | None = None,
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
            "audit_issues": [],
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

    internal_screenshot_url = None
    for key in INTERNAL_SCREENSHOT_PRIORITY:
        if all_internal.get(key):
            internal_screenshot_url = all_internal.get(key)
            break
    if not internal_screenshot_url and discovered_pages:
        internal_screenshot_url = discovered_pages[0]

    shot_paths = {
        "desktop_homepage_path": None,
        "mobile_homepage_path": None,
        "internal_page_path": None,
    }
    if screenshot_dir:
        shot_paths = _capture_website_screenshots(
            homepage_url=url,
            internal_url=internal_screenshot_url,
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
        },
        shot_paths,
    )
    issues_for_pitch = list(dict.fromkeys((audit.get("audit_issues") or []) + problems))

    best_contact = "email" if all_emails else "phone" if all_phones else "contact_form" if contact_form else "social" if all_social else "unknown"
    backup_contact = "phone" if all_emails and all_phones else "email" if all_phones and not all_emails else "contact_form" if all_social else None

    contact_matrix = {
        "best_contact": best_contact,
        "best_contact_method": best_contact,
        "backup_contact": backup_contact,
        "backup_contact_method": backup_contact,
        "email": (list(all_emails)[:1] or [None])[0],
        "phone": (list(all_phones)[:1] or [None])[0],
        "contact_page": contact_page_url,
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
        "audit_issues": audit.get("audit_issues") or [],
        "detected_issues": audit.get("detected_issues") or [],
        "homepage_http_status": status,
        "homepage_load_seconds": homepage_load_seconds,
        "missing_meta_title": missing_meta_title,
        "missing_meta_description": missing_meta_description,
        "text_content_length": text_content_length,
        "image_count": image_count,
        "broken_links_count": broken_links_count,
        "desktop_homepage_path": shot_paths.get("desktop_homepage_path"),
        "mobile_homepage_path": shot_paths.get("mobile_homepage_path"),
        "internal_page_path": shot_paths.get("internal_page_path"),
    }
