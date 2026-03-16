"""
Outreach pack generator for Scout-Brain leads.

Generates practical first-touch outreach messages using only known dossier data.
"""

from __future__ import annotations

from typing import Any, Callable


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    return [text] if text else []


def _first(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _log(logger: Callable[[str], None] | None, message: str) -> None:
    if logger:
        logger(message)
    else:
        print(message)


def _normalize_issue_label(issue: str) -> str:
    raw = str(issue or "").strip()
    if not raw:
        return ""
    lower = raw.lower()
    if "mobile" in lower and ("not optimized" in lower or "layout" in lower):
        return "site not mobile friendly"
    if "slow" in lower or "load" in lower:
        return "page load slow"
    if "call-to-action" in lower or "cta" in lower:
        return "missing CTA above the fold"
    if "text-heavy" in lower or "text heavy" in lower or "low text" in lower:
        return "text heavy homepage"
    if "booking" in lower or "ordering" in lower:
        return "no booking or ordering system"
    if "seo" in lower or "meta" in lower:
        return "missing SEO title/description"
    if "outdated" in lower and "wordpress" not in lower:
        return "outdated visual design"
    if "navigation" in lower or "menu" in lower or "layout" in lower:
        return "difficult navigation"
    if "contact" in lower and ("missing" in lower or "hard" in lower):
        return "contact information hard to find"
    if "ssl" in lower or "https" in lower or "http site" in lower:
        return "broken SSL / http site"
    if "image" in lower:
        return "images not optimized"
    if "website has visible quality issues" in lower or "web presence can be improved" in lower:
        return ""
    return raw


def generate_outreach_pack(
    case: dict[str, Any],
    *,
    city_hint: str | None = None,
    logger: Callable[[str], None] | None = None,
) -> dict[str, str]:
    """
    Build outreach messages from existing case intelligence.
    Never invents names or facts. Handles missing fields safely.
    """
    _log(logger, "generating outreach pack")

    business_name = _first(case.get("business_name"), case.get("name"), "your business")
    lane = (case.get("lane") or "").strip().lower()
    if not lane:
        lane = "no_website" if case.get("no_website") else "weak_website"

    category = _first(case.get("category"))
    owner_name = _first(
        case.get("owner_name"),
        case.get("owner_manager_name"),
        (_as_list(case.get("owner_names")) or [None])[0],
    )
    recommended_contact = _first(case.get("recommended_contact_method"), case.get("recommended_contact"))
    strongest_pitch = _first(case.get("strongest_pitch_angle"), case.get("pitch_angle"))
    best_service = _first(case.get("best_service_to_offer"))
    best_demo = _first(case.get("best_demo_to_show"), case.get("demo_to_show"), case.get("demo_url"))
    demo_url = _first(case.get("demo_url"))
    location = _first(city_hint, case.get("address"))
    distance = case.get("distance_miles")
    review_rating = case.get("rating")
    review_count = case.get("review_count")
    website_score = case.get("website_score")
    website_status = _first(case.get("website_status"))
    review_themes = _as_list(case.get("review_themes"))
    strongest_problems = _as_list(case.get("strongest_problems")) or _as_list(
        (case.get("website_analysis") or {}).get("issues")
    )
    audit_issues = _as_list(case.get("audit_issues"))
    website_audit = case.get("website_audit") if isinstance(case.get("website_audit"), dict) else {}
    audit_structured_issues = _as_list((website_audit or {}).get("issues"))
    audit_checks = (website_audit or {}).get("checks") if isinstance((website_audit or {}).get("checks"), dict) else {}
    opportunity_reason = _first(case.get("opportunity_reason"), case.get("main_issue_observed"))
    issue_pool_raw = list(
        dict.fromkeys([*([opportunity_reason] if opportunity_reason else []), *audit_structured_issues, *strongest_problems, *audit_issues])
    )
    issue_pool: list[str] = []
    for raw_issue in issue_pool_raw:
        normalized = _normalize_issue_label(raw_issue)
        if normalized and normalized not in issue_pool:
            issue_pool.append(normalized)

    issue_fallback = None
    if lane == "no_website":
        issue_fallback = "No website found for the business"
    elif website_status == "unreachable":
        issue_fallback = "Website appears unreachable"
    elif website_status == "weak":
        issue_fallback = "Website has visible quality issues"
    main_issue = _first((issue_pool or [None])[0], issue_fallback, "Web presence can be improved")

    why_this_lead = []
    if lane == "no_website":
        why_this_lead.append("No website creates a clear first-build opportunity.")
    if website_status == "weak":
        why_this_lead.append("Current site quality leaves room for conversion improvements.")
    if website_status == "unreachable":
        why_this_lead.append("Unreachable website is hurting trust and discoverability.")
    try:
        if float(review_rating or 0) >= 4.2 and int(review_count or 0) >= 20:
            why_this_lead.append("Strong reviews suggest demand, but web presence is underperforming.")
    except Exception:
        pass
    if not why_this_lead:
        why_this_lead.append("The business has demand signals and a clear web improvement angle.")
    why_this_lead_text = " ".join(why_this_lead)

    greeting = f"Hi {owner_name}," if owner_name else "Hi there,"
    category_fragment = f" ({category})" if category else ""

    if lane == "no_website":
        primary_observation = (
            f"I noticed {business_name}{category_fragment} does not seem to have a website yet."
        )
        value_angle = strongest_pitch or (
            "A simple, professional site can help local customers find your hours, services, and best contact info."
        )
    else:
        weak_site_observation = main_issue
        primary_observation = weak_site_observation or (
            f"I noticed a few easy website improvements for {business_name}{category_fragment}."
        )
        value_angle = strongest_pitch or (
            "A cleaner mobile experience and clearer calls to action can make outreach and bookings easier."
        )

    proof_lines: list[str] = []
    if issue_pool:
        proof_lines.extend(issue_pool[:2])
    if review_themes:
        proof_lines.append(f"Customers often mention: {', '.join(review_themes[:2])}.")
    if distance is not None:
        proof_lines.append(f"You're about {distance} miles from my focus area.")
    if not proof_lines and location:
        proof_lines.append(f"I was reviewing businesses around {location}.")

    offer_line = best_service or "I can share a focused plan and a quick before/after concept."
    demo_line = f"I can also show a quick demo: {best_demo}." if best_demo else ""
    contact_line = (
        f"Best way to reach you seems to be {recommended_contact}."
        if recommended_contact
        else "Happy to use whatever contact method works best for your team."
    )

    issue_hint = _first(main_issue, (issue_pool or [None])[0], "something that might be affecting conversions")
    screenshot_count = int(
        sum(
            1
            for key in ["desktop_screenshot_url", "mobile_screenshot_url", "contact_page_screenshot_url", "internal_screenshot_url", "screenshot_url"]
            if _first(case.get(key))
        )
    )
    screenshot_note = (
        "I captured screenshots while reviewing it."
        if screenshot_count >= 2
        else "I grabbed a quick screenshot while reviewing it."
    )
    issue_line = (
        f"I was looking at your website and noticed: {issue_hint}."
        if issue_hint
        else "I was looking at your website and noticed something that might be affecting conversions."
    )
    if audit_checks.get("booking_or_ordering_missing") is True and "booking" not in issue_line.lower():
        issue_line = f"{issue_line} It also looks like online booking/ordering is missing."
    short_email = (
        "Hi,\n\n"
        f"{issue_line}\n\n"
        f"{screenshot_note}\n\n"
        "Would you like me to send it over?\n\n"
        "– Topher"
    )

    long_sections = [
        greeting,
        "",
        f"I took a quick look at {business_name}{category_fragment}.",
        primary_observation,
        value_angle,
        "",
    ]
    if proof_lines:
        long_sections.append("What stood out:")
        long_sections.extend([f"- {line}" for line in proof_lines])
        long_sections.append("")
    long_sections.extend(
        [
            f"Offer: {offer_line}",
            demo_line if demo_line else "",
            contact_line,
            "",
            "If you're open to it, I can send a short walkthrough and keep it low-pressure.",
            "",
            "Thanks,",
            "Topher",
            "topher@mixedmakershop.com",
        ]
    )
    longer_email = "\n".join([line for line in long_sections if line is not None])

    contact_form_version = (
        f"{issue_line} "
        "I grabbed a quick screenshot showing it. "
        "Would you like me to send it over? "
        "– Topher"
    ).strip()

    social_dm_version = (
        f"Hey — quick note about {business_name}. {primary_observation} "
        f"{value_angle} Happy to send one simple idea if useful."
    ).strip()

    follow_up_1 = (
        f"Quick follow-up on my note about {business_name}. "
        "If you'd like, I can send that one-page idea and keep it brief."
    )
    follow_up_2 = (
        f"Final quick follow-up for {business_name}: happy to share a simple before/after plan "
        f"focused on {main_issue.lower()}."
    )

    missing_fields: list[str] = []
    if not owner_name:
        missing_fields.append("owner_manager_name")
    if not strongest_problems:
        missing_fields.append("strongest_problems")
    if not strongest_pitch:
        missing_fields.append("strongest_pitch_angle")
    if not best_service:
        missing_fields.append("best_service_to_offer")
    if missing_fields:
        _log(logger, f"missing dossier fields handled: {', '.join(missing_fields)}")

    _log(logger, "outreach pack generated")
    return {
        "short_email": short_email,
        "longer_email": longer_email,
        "contact_form_version": contact_form_version,
        "social_dm_version": social_dm_version,
        "follow_up_note": follow_up_1,
        "follow_up_1": follow_up_1,
        "follow_up_2": follow_up_2,
        "follow_up_line": follow_up_1,
        "why_this_lead": why_this_lead_text,
        "main_issue_observed": main_issue,
        "best_opening_angle": value_angle,
        "best_offer_to_make": offer_line,
        "demo_url": demo_url or best_demo,
        "review_rating": review_rating,
        "review_count": review_count,
        "website_score": website_score,
    }
