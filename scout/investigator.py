"""
Deep website investigator — crawls homepage + internal pages, extracts contact
data, platform clues, owner names, and builds a contact matrix.

Used by Morning Runner to build detailed research per opportunity.
"""
import re
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


def _fetch(url: str, timeout: int = 10) -> tuple[str, int]:
    """Fetch URL, return (html, status)."""
    if not url.startswith("http"):
        url = "https://" + url
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(400000)
            return body.decode("utf-8", errors="ignore"), resp.status
    except urllib.error.HTTPError as e:
        return "", e.code
    except Exception:
        return "", 0


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


def _extract_owner_names(html: str) -> list[str]:
    """Look for Owner, Founder, Pastor, Chef, Manager, Director and extract nearby names."""
    titles = ["owner", "founder", "pastor", "chef", "manager", "director", "proprietor", "lead"]
    names = []
    # Strip script/style to reduce noise
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.I | re.S)
    html = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.I | re.S)
    for title in titles:
        # Pattern: "Name — Owner" or "Owner: Name" or "Name, Owner" or <strong>Name</strong> ... Owner
        for m in re.finditer(
            rf"(?:^|[>\s])([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*[—\-:,]\s*{re.escape(title)}\b",
            html,
            re.I,
        ):
            name = m.group(1).strip()
            if len(name) > 3 and name not in names:
                names.append(name)
        for m in re.finditer(
            rf"\b{re.escape(title)}\s*[:\s]+\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            html,
            re.I,
        ):
            name = m.group(1).strip()
            if len(name) > 3 and name not in names:
                names.append(name)
        # Near heading: <h3>John Smith</h3> ... Owner
        for m in re.finditer(
            r"<h[2-4][^>]*>([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)</h[2-4]>[\s\S]{0,80}?" + re.escape(title),
            html,
            re.I,
        ):
            name = m.group(1).strip()
            if len(name) > 3 and name not in names:
                names.append(name)
    return names[:5]


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


def investigate(url: str, crawl_internal: bool = True, timeout: int = 12) -> dict[str, Any]:
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

    home_html, status = _fetch(url, timeout)
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
            "problems": ["Website could not be fetched"],
            "pitch": ["review manually"],
        }

    debug_log.append("website: fetched")

    all_emails: set[str] = set()
    all_phones: set[str] = set()
    all_social: dict[str, str] = {}
    all_internal: dict[str, str] = {}
    all_owner_names: set[str] = set()
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
            html, _ = _fetch(candidate, timeout=8)
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
        "owner_name": (list(all_owner_names)[:1] or [None])[0],
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
        "reservation_link": reservation_link,
        "order_link": order_link,
        "contact_matrix": contact_matrix,
        "page_navigation_items": page_nav_items,
        "navigation_items": page_nav_items,
        "text_heavy_clues": text_heavy,
        "outdated_design_clues": bool(platform and "weebly" in (platform or "").lower()),
        "problems": problems[:6],
        "strongest_problems": problems[:6],
        "pitch": pitch[:6],
    }
