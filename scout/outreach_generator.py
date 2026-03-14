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
    owner_name = _first(case.get("owner_manager_name"), (_as_list(case.get("owner_names")) or [None])[0])
    recommended_contact = _first(case.get("recommended_contact_method"), case.get("recommended_contact"))
    strongest_pitch = _first(case.get("strongest_pitch_angle"), case.get("pitch_angle"))
    best_service = _first(case.get("best_service_to_offer"))
    best_demo = _first(case.get("best_demo_to_show"), case.get("demo_to_show"))
    location = _first(city_hint, case.get("address"))
    distance = case.get("distance_miles")
    review_themes = _as_list(case.get("review_themes"))
    strongest_problems = _as_list(case.get("strongest_problems")) or _as_list(
        (case.get("website_analysis") or {}).get("issues")
    )

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
        weak_site_observation = strongest_problems[0] if strongest_problems else None
        primary_observation = weak_site_observation or (
            f"I noticed a few easy website improvements for {business_name}{category_fragment}."
        )
        value_angle = strongest_pitch or (
            "A cleaner mobile experience and clearer calls to action can make outreach and bookings easier."
        )

    proof_lines: list[str] = []
    if strongest_problems:
        proof_lines.extend(strongest_problems[:2])
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

    short_email = (
        f"{greeting}\n\n"
        f"{primary_observation}\n"
        f"{value_angle}\n\n"
        "If helpful, I can send one quick idea you can review in a few minutes.\n\n"
        "Thanks,\n"
        "Topher\n"
        "topher@mixedmakershop.com"
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
        f"{primary_observation} {value_angle} "
        f"Offer: {offer_line}. "
        "If helpful, I can send one quick idea you can review today. "
        "Topher (MixedMakerShop) — topher@mixedmakershop.com"
    ).strip()

    social_dm_version = (
        f"Hey — quick note about {business_name}. {primary_observation} "
        f"{value_angle} Happy to send one simple idea if useful."
    ).strip()

    follow_up_note = (
        f"Quick follow-up on my note about {business_name}. "
        "If you'd like, I can send that one-page idea and keep it brief."
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
        "follow_up_note": follow_up_note,
        "follow_up_line": follow_up_note,
    }
