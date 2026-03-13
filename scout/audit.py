"""
Shared website audit logic — used by the single app and by Morning Runner
to fill case file fields (platform, viewport, tap-to-call, problems, pitch).
"""
import re
import urllib.request
import urllib.error
from typing import Any


def analyze_html(url: str, html: str) -> dict[str, Any]:
    """
    Analyze HTML and return facts, problems, pitch, plus structured booleans
    for case file fields (viewport_ok, tap_to_call_present, etc.).
    """
    lower = html.lower()

    problems = []
    pitch = []
    facts = []
    platform_used = None
    viewport_ok = True
    tap_to_call_present = False
    menu_visibility = False
    hours_visibility = False
    directions_visibility = False
    contact_form_present = False
    text_heavy_clues = False
    meta_description = None
    homepage_title = None

    # Platform
    if "weebly" in lower or "editmysite" in lower:
        platform_used = "Weebly/EditMySite"
        facts.append("Detected older Weebly/EditMySite platform")
        problems.append("Site appears to use an older website platform/template")
        pitch.append("move to a cleaner custom layout with a more modern first impression")
    if "wix" in lower:
        platform_used = platform_used or "Wix"
        facts.append("Detected Wix-related markup")
        problems.append("Site may rely on a generic builder layout")
        pitch.append("simplify the layout and tighten the mobile experience")
    if "squarespace" in lower:
        platform_used = platform_used or "Squarespace"
        facts.append("Detected Squarespace-related markup")

    # Viewport
    if 'name="viewport"' not in lower:
        viewport_ok = False
        facts.append("No viewport meta tag found")
        problems.append("Mobile layout may not be properly optimized")

    # Title
    title_match = re.search(r"<title>(.*?)</title>", html, re.I | re.S)
    if title_match:
        homepage_title = re.sub(r"\s+", " ", title_match.group(1)).strip()[:200]
        facts.append(f"Title tag: {homepage_title[:120]}")
    else:
        problems.append("No clear title tag found")

    # Meta description
    meta_desc = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', html, re.I
    )
    if meta_desc:
        meta_description = meta_desc.group(1).strip()[:300]
        facts.append("Meta description found")
    else:
        problems.append("No obvious meta description found")
        pitch.append("improve search appearance with stronger SEO basics")

    # Menu / hours / directions / contact
    menu_words = ["menu", "special", "breakfast", "lunch", "dinner", "carry out", "carryout"]
    menu_hits = sum(1 for w in menu_words if w in lower)
    if menu_hits >= 2:
        facts.append("Restaurant/menu content detected")
        menu_visibility = True

    hours_words = ["hours", "open", "closed", "monday", "tuesday", "sunday"]
    if sum(1 for w in hours_words if w in lower) >= 2:
        hours_visibility = True

    if "google maps" in lower or "maps.google" in lower or "map" in lower or "directions" in lower:
        facts.append("Map/location references detected")
        directions_visibility = True
    else:
        pitch.append("make directions and location easier to find")

    if "tel:" in lower:
        tap_to_call_present = True
        facts.append("Tap-to-call link found")
    else:
        problems.append("No obvious tap-to-call phone link found")
        pitch.append("make phone contact easier for mobile visitors")

    if "contact" in lower and ("form" in lower or "mailto:" in lower or "submit" in lower):
        contact_form_present = True

    # Heuristic clutter
    h2_count = len(re.findall(r"<h2\b", lower))
    br_count = lower.count("<br")
    if h2_count > 8 or br_count > 25:
        text_heavy_clues = True
        problems.append("Page may be text-heavy and harder to scan quickly on phones")
        pitch.append("reduce reading load and improve visual hierarchy")

    if menu_hits >= 2:
        pitch.append("make menu, hours, specials, and location easier to scan on mobile")

    if not problems:
        problems = [
            "Site could likely be made clearer and more conversion-focused",
            "Important actions may not stand out enough on phones",
        ]
    if not pitch:
        pitch = [
            "create a cleaner mobile-friendly layout",
            "make the main actions easier for visitors",
            "improve the overall first impression",
        ]

    return {
        "url": url,
        "facts": facts[:8],
        "problems": problems[:6],
        "pitch": pitch[:6],
        "platform_used": platform_used,
        "homepage_title": homepage_title,
        "meta_description": meta_description,
        "viewport_ok": viewport_ok,
        "tap_to_call_present": tap_to_call_present,
        "menu_visibility": menu_visibility,
        "hours_visibility": hours_visibility,
        "directions_visibility": directions_visibility,
        "contact_form_present": contact_form_present,
        "text_heavy_clues": text_heavy_clues,
        "outdated_design_clues": bool(platform_used and "weebly" in (platform_used or "").lower()),
    }


def fetch_and_audit(url: str, timeout: int = 12) -> dict[str, Any]:
    """Fetch URL and run analyze_html. Raises on network error."""
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 MassiveBrainAuditor/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read(400000)
        html = body.decode("utf-8", errors="ignore")
    return analyze_html(url, html)
