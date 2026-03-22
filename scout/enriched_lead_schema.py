"""
Normalized enriched-lead contract for Scout Brain → CRM handoff.

Use these Pydantic models for consistent JSON over HTTP and internal pipelines.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

SourceType = Literal["extension", "facebook", "google", "manual", "unknown", "mixed"]
BestContactMethod = Literal["email", "phone", "facebook", "contact_page", "none"]


class EnrichLeadRequest(BaseModel):
    business_name: str = ""
    city: str = ""
    state: str = ""
    source_url: str = ""
    facebook_url: str = ""
    source_type: SourceType = "unknown"


class EnrichedLead(BaseModel):
    business_name: str = ""
    source_type: SourceType = "unknown"
    source_url: str | None = None
    facebook_url: str | None = None
    website: str | None = None
    normalized_website: str | None = None
    phone: str | None = None
    email: str | None = None
    email_source: str | None = None
    contact_page: str | None = None
    city: str | None = None
    state: str | None = None
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    score: int = Field(default=0, ge=0, le=100)
    why_this_lead_is_here: str = ""
    best_contact_method: BestContactMethod = "none"
    best_next_move: str = ""
    pitch_angle: str = ""
    source_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    match_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    raw_signals: dict[str, Any] = Field(default_factory=dict)
    place_id: str | None = None


class EnrichLeadResponse(BaseModel):
    ok: bool = True
    enriched_lead: EnrichedLead
    message: str | None = None
