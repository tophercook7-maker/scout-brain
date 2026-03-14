#!/usr/bin/env python3
"""
Massive Brain backend (FastAPI).

- API routes: /scout-data, /run-scout, /audit, /case/*
- Reads/writes scout/config.json, scout/history.json, scout/opportunities.json, scout/today.json
- Loads GOOGLE_MAPS_API_KEY and Supabase secrets from env
- Optional frontend serving can be enabled with SERVE_FRONTEND=1
"""
from pathlib import Path
import json
import os
import sys
import webbrowser
import threading
import time
from datetime import datetime, timezone
from urllib import request as urllib_request
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

APP_DIR = Path(__file__).resolve().parent
UI_DIR = APP_DIR / "ui"
DIST_DIR = APP_DIR / "dist"
DIST_ASSETS_DIR = DIST_DIR / "assets"
DIST_INDEX_PATH = DIST_DIR / "index.html"
SCOUT_DIR = APP_DIR / "scout"
CASES_DIR = SCOUT_DIR / "cases"
CONFIG_PATH = SCOUT_DIR / "config.json"
HISTORY_PATH = SCOUT_DIR / "history.json"
OPPORTUNITIES_PATH = SCOUT_DIR / "opportunities.json"
TODAY_PATH = SCOUT_DIR / "today.json"
ENV_PATH = APP_DIR / ".env"

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

try:
    from scout.errors import ScoutRunError
except ImportError:
    ScoutRunError = None  # will not match in except

# Load .env
_env_loaded = False
_maps_key = None
_supabase_url = None
_supabase_service_key = None
_supabase_jwt_secret = None
_resend_api_key = None
_resend_from_email = None
_dashboard_url = None
try:
    from dotenv import load_dotenv
    load_dotenv(SCOUT_DIR / ".env")
    load_dotenv(ENV_PATH)
    _maps_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    _supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    _supabase_service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    _supabase_jwt_secret = os.environ.get("SUPABASE_JWT_SECRET", "").strip()
    _resend_api_key = os.environ.get("RESEND_API_KEY", "").strip()
    _resend_from_email = os.environ.get("RESEND_FROM_EMAIL", "").strip()
    _dashboard_url = os.environ.get("SCOUT_APP_URL", "").strip() or os.environ.get("VITE_APP_URL", "").strip()
    _env_loaded = True
except ImportError:
    pass


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Railway backend-only mode should not require npm or frontend assets.
# Enable only when intentionally serving frontend from this process.
SERVE_FRONTEND = _env_flag("SERVE_FRONTEND", default=False)
ENABLE_SCHEDULED_SCOUT = _env_flag("ENABLE_SCHEDULED_SCOUT", default=True)
SCHEDULED_SCOUT_HOUR = int(os.environ.get("SCHEDULED_SCOUT_HOUR", "6"))

app = FastAPI(title="Massive Brain", version="2.0")

_scheduler = None

# CORS for Vercel frontend calling Railway backend.
# Configure explicit origins with ALLOWED_ORIGINS="https://your-app.vercel.app,https://other-domain.com"
# and optionally customize ALLOWED_ORIGIN_REGEX.
def _parse_allowed_origins() -> list[str]:
    raw = os.environ.get("ALLOWED_ORIGINS", "").strip()
    if raw:
        origins = [o.strip().rstrip("/") for o in raw.split(",") if o.strip()]
        # Keep ordering stable while removing duplicates.
        return list(dict.fromkeys(origins))
    return [
        "http://localhost:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
    ]


_allowed_origins = _parse_allowed_origins()
_allowed_origin_regex_raw = os.environ.get("ALLOWED_ORIGIN_REGEX")
if _allowed_origin_regex_raw is None:
    _allowed_origin_regex = r"^https://.*\.vercel\.app$"
else:
    _allowed_origin_regex = _allowed_origin_regex_raw.strip() or None

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_origin_regex=_allowed_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _run_morning_runner(current_lat: float | None = None, current_lng: float | None = None):
    from scout.morning_runner import run
    run(current_lat=current_lat, current_lng=current_lng)


def _load_scout_data():
    today = {
        "generated_at": None,
        "summary": "No scout run yet.",
        "top_opportunities": [],
        "case_slugs": [],
    }
    if TODAY_PATH.exists():
        with open(TODAY_PATH, encoding="utf-8") as f:
            today = json.load(f)

    opportunities = []
    case_slugs = today.get("case_slugs") or today.get("top_opportunities") or []
    if case_slugs and CASES_DIR.is_dir():
        from scout.case_schema import case_to_ui
        for slug in case_slugs:
            if isinstance(slug, dict):
                opportunities.append(slug)
                continue
            path = CASES_DIR / f"{slug}.json"
            if path.exists():
                try:
                    with open(path, encoding="utf-8") as f:
                        opportunities.append(case_to_ui(json.load(f)))
                except Exception:
                    pass
    if not opportunities and OPPORTUNITIES_PATH.exists():
        with open(OPPORTUNITIES_PATH, encoding="utf-8") as f:
            opportunities = json.load(f)
    if not opportunities and today.get("top_opportunities"):
        first = today["top_opportunities"][0] if today["top_opportunities"] else None
        if isinstance(first, dict):
            opportunities = today["top_opportunities"]

    today = dict(today)
    today["top_opportunities"] = opportunities
    return {"today": today, "opportunities": opportunities}


def _get_user_id_from_request(request: Request) -> str | None:
    """Verify Bearer JWT and return user_id (uuid). Returns None if no/invalid auth."""
    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if not token or not _supabase_jwt_secret:
        return None
    try:
        import jwt
        payload = jwt.decode(token, _supabase_jwt_secret, algorithms=["HS256"])
        return payload.get("sub")
    except Exception:
        return None


def _get_workspace_id_from_request(request: Request) -> str | None:
    """Optional workspace hint for multi-tenant routing."""
    ws = (request.headers.get("X-Workspace-Id") or "").strip()
    return ws or None


def _is_missing_workspace_schema_error(err: Exception) -> bool:
    msg = str(err).lower()
    return (
        "workspace_id" in msg
        or "workspace_users" in msg
        or "workspace_memberships" in msg
        or "workspaces" in msg
    )


def _resolve_workspace_id_for_user(sb, user_id: str, requested_workspace_id: str | None = None) -> str | None:
    """
    Determine workspace from authenticated user (preferred), with optional requested workspace hint.
    """
    if not user_id:
        return None
    try:
        # Prefer explicitly requested workspace if the user belongs to it.
        if requested_workspace_id:
            candidate = (
                sb.table("workspace_users")
                .select("workspace_id")
                .eq("user_id", user_id)
                .eq("workspace_id", requested_workspace_id)
                .limit(1)
                .execute()
            )
            if candidate.data:
                return requested_workspace_id

        # Default workspace from workspace_users membership.
        rows = (
            sb.table("workspace_users")
            .select("workspace_id")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
        if rows.data:
            return rows.data[0].get("workspace_id")
    except Exception as e:
        # Legacy fallback path if schema still on workspace_memberships or no workspace tables.
        if not _is_missing_workspace_schema_error(e):
            raise

    try:
        legacy = (
            sb.table("workspace_memberships")
            .select("workspace_id")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
        if legacy.data:
            return legacy.data[0].get("workspace_id")
    except Exception:
        pass

    return requested_workspace_id


def _insert_with_workspace_fallback(sb, table_name: str, row: dict):
    """
    Insert row and gracefully retry without workspace_id for legacy schemas.
    """
    try:
        return sb.table(table_name).insert(row).execute()
    except Exception as e:
        if "workspace_id" in str(e).lower() and "workspace_id" in row:
            legacy = dict(row)
            legacy.pop("workspace_id", None)
            return sb.table(table_name).insert(legacy).execute()
        raise


def _sync_scout_to_supabase(user_id: str, workspace_id: str | None = None) -> None:
    """Load scout results from local files and upsert into Supabase (opportunities + case_files)."""
    if not _supabase_url or not _supabase_service_key:
        return
    try:
        from supabase import create_client
        data = _load_scout_data()
        opportunities_ui = data.get("opportunities") or []
        if not opportunities_ui:
            return
        sb = create_client(_supabase_url, _supabase_service_key)
        effective_workspace_id = _resolve_workspace_id_for_user(sb, user_id, requested_workspace_id=workspace_id)
        for opp_ui in opportunities_ui:
            slug = opp_ui.get("slug") or opp_ui.get("id")
            name = (opp_ui.get("name") or opp_ui.get("business_name") or "").strip()
            if not name:
                continue
            opp_row = {
                "user_id": user_id,
                "workspace_id": effective_workspace_id,
                "business_name": name,
                "category": opp_ui.get("category"),
                "lane": opp_ui.get("lane"),
                "distance_miles": opp_ui.get("distance_miles"),
                "address": opp_ui.get("address"),
                "phone": opp_ui.get("phone"),
                "website": opp_ui.get("website"),
                "maps_link": opp_ui.get("maps_url") or opp_ui.get("maps_link"),
                "rating": opp_ui.get("rating"),
                "review_count": opp_ui.get("review_count"),
                "hours": opp_ui.get("hours"),
                "no_website": bool(opp_ui.get("no_website")),
                "recommended_contact_method": opp_ui.get("recommended_contact") or opp_ui.get("recommended_contact_method"),
                "backup_contact_method": opp_ui.get("backup_contact_method"),
                "strongest_pitch_angle": opp_ui.get("pitch_angle") or opp_ui.get("strongest_pitch_angle"),
                "best_service_to_offer": opp_ui.get("best_service_to_offer"),
                "demo_to_show": opp_ui.get("demo_to_show"),
                "internal_score": opp_ui.get("score") or opp_ui.get("internal_score"),
                "priority": opp_ui.get("priority"),
                "status": opp_ui.get("status") or "New",
            }
            ins = _insert_with_workspace_fallback(sb, "opportunities", opp_row)
            if not ins.data or len(ins.data) == 0:
                continue
            opp_id = ins.data[0]["id"]
            case_path = CASES_DIR / f"{slug}.json"
            if case_path.exists():
                with open(case_path, encoding="utf-8") as f:
                    case = json.load(f)
                cf_row = {
                    "opportunity_id": opp_id,
                    "workspace_id": effective_workspace_id,
                    "email": case.get("email"),
                    "contact_page": case.get("contact_page"),
                    "phone_from_site": case.get("phone_from_site"),
                    "facebook": case.get("facebook"),
                    "instagram": case.get("instagram"),
                    "owner_manager_name": case.get("owner_manager_name"),
                    "platform_used": case.get("platform_used"),
                    "homepage_title": case.get("homepage_title"),
                    "meta_description": case.get("meta_description"),
                    "viewport_ok": case.get("viewport_ok"),
                    "tap_to_call_present": case.get("tap_to_call_present"),
                    "menu_visibility": str(case.get("menu_visibility")) if case.get("menu_visibility") is not None else None,
                    "hours_visibility": str(case.get("hours_visibility")) if case.get("hours_visibility") is not None else None,
                    "directions_visibility": str(case.get("directions_visibility")) if case.get("directions_visibility") is not None else None,
                    "contact_form_present": case.get("contact_form_present"),
                    "strongest_problems": case.get("strongest_problems"),
                    "short_email": case.get("short_email"),
                    "longer_email": case.get("longer_email"),
                    "contact_form_version": case.get("contact_form_version"),
                    "follow_up_note": case.get("follow_up_note"),
                    "outreach_notes": case.get("outreach_notes"),
                    "follow_up_due": case.get("follow_up_due"),
                    "outcome": case.get("outcome"),
                    "status": case.get("status") or "New",
                }
                _insert_with_workspace_fallback(sb, "case_files", cf_row)
    except Exception as e:
        print(f"  [Scout] Supabase sync error: {e}", file=sys.stderr)
        raise


def _is_ignored_lead_status(status: str | None) -> bool:
    raw = (status or "").strip().lower()
    ignored = {
        "closed",
        "contacted",
        "not interested",
        "do not contact",
        "skip",
    }
    return raw in ignored


def _lead_rank(row: dict) -> float:
    lane = (row.get("lane") or "").strip().lower()
    score = float(row.get("internal_score") or 0)
    review_count = int(row.get("review_count") or 0)
    rating = float(row.get("rating") or 0) if row.get("rating") is not None else 0.0
    distance = row.get("distance_miles")
    try:
        distance_val = float(distance) if distance is not None else 9999.0
    except Exception:
        distance_val = 9999.0
    has_contact = bool(
        (row.get("recommended_contact_method") or "").strip()
        or (row.get("phone") or "").strip()
        or (row.get("website") or "").strip()
    )
    contacted = bool((row.get("first_contacted_at") or "").strip() or (row.get("last_contacted_at") or "").strip())
    problems = row.get("strongest_problems")
    has_issues = bool(problems) if isinstance(problems, list) else bool(str(problems or "").strip())

    rank = 0.0
    if lane == "no_website" or bool(row.get("no_website")):
        rank += 600.0
    elif lane == "weak_website":
        rank += 420.0
    if has_issues:
        rank += 140.0
    if has_contact:
        rank += 120.0
    if not contacted:
        rank += 110.0
    rank += min(max(score, 0.0), 1000.0) * 10.0
    rank += min(review_count, 200) * 0.4
    rank += rating * 3.0
    rank -= min(distance_val, 200.0) * 1.5
    return rank


def getTopOpportunities(workspace_id: str | None):
    """
    Return top 5 leads for outreach from workspace-filtered opportunities.
    Uses Supabase when configured; falls back to local case files.
    """
    leads: list[dict] = []

    if _supabase_url and _supabase_service_key:
        try:
            from supabase import create_client
            sb = create_client(_supabase_url, _supabase_service_key)
            query = sb.table("opportunities").select("*")
            if workspace_id:
                query = query.eq("workspace_id", workspace_id)
            res = query.execute()
            rows = res.data or []

            filtered = [r for r in rows if not _is_ignored_lead_status(r.get("status"))]
            ranked = sorted(filtered, key=_lead_rank, reverse=True)[:5]
            leads = [
                {
                    "business_name": r.get("business_name"),
                    "category": r.get("category"),
                    "distance": r.get("distance_miles"),
                    "score": r.get("internal_score"),
                    "lane": "no_website" if r.get("no_website") else (r.get("lane") or "weak_website"),
                    "best_contact_method": r.get("recommended_contact_method") or r.get("backup_contact_method"),
                    "slug": r.get("id"),
                }
                for r in ranked
            ]
            return leads
        except Exception as e:
            print(f"  [Scout] top opportunities supabase fallback: {e}", file=sys.stderr)

    # Local file fallback.
    if CASES_DIR.is_dir():
        rows = []
        for path in CASES_DIR.glob("*.json"):
            try:
                with open(path, encoding="utf-8") as f:
                    row = json.load(f)
                row["id"] = row.get("slug") or path.stem
                rows.append(row)
            except Exception:
                continue
        filtered = [r for r in rows if not _is_ignored_lead_status(r.get("status"))]
        ranked = sorted(filtered, key=_lead_rank, reverse=True)[:5]
        leads = [
            {
                "business_name": r.get("business_name"),
                "category": r.get("category"),
                "distance": r.get("distance_miles"),
                "score": r.get("internal_score"),
                "lane": "no_website" if r.get("no_website") else (r.get("lane") or "weak_website"),
                "best_contact_method": r.get("recommended_contact_method") or r.get("backup_contact_method"),
                "slug": r.get("id"),
            }
            for r in ranked
        ]
    return leads


def _default_user_settings() -> dict:
    return {
        "email_notifications_enabled": True,
        "email_frequency": "daily",
        "include_new_leads": True,
        "include_followups": True,
        "include_top_opportunities": True,
    }


def _normalize_user_settings(payload: dict | None) -> dict:
    defaults = _default_user_settings()
    src = payload or {}
    normalized = {
        "email_notifications_enabled": bool(src.get("email_notifications_enabled", defaults["email_notifications_enabled"])),
        "email_frequency": str(src.get("email_frequency", defaults["email_frequency"]) or "daily").strip().lower(),
        "include_new_leads": bool(src.get("include_new_leads", defaults["include_new_leads"])),
        "include_followups": bool(src.get("include_followups", defaults["include_followups"])),
        "include_top_opportunities": bool(src.get("include_top_opportunities", defaults["include_top_opportunities"])),
    }
    if normalized["email_frequency"] not in {"daily", "weekly", "off"}:
        normalized["email_frequency"] = "daily"
    if normalized["email_frequency"] == "off":
        normalized["email_notifications_enabled"] = False
    return normalized


def _ensure_user_settings(sb, user_id: str, workspace_id: str | None) -> dict:
    defaults = _default_user_settings()
    if not workspace_id:
        return defaults
    row = {"user_id": user_id, "workspace_id": workspace_id, **defaults}
    try:
        existing = (
            sb.table("user_settings")
            .select("*")
            .eq("user_id", user_id)
            .eq("workspace_id", workspace_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            return _normalize_user_settings(existing.data[0])
        created = (
            sb.table("user_settings")
            .insert(row)
            .execute()
        )
        if created.data:
            return _normalize_user_settings(created.data[0])
    except Exception:
        pass
    return defaults


def _load_user_settings(sb, user_id: str, workspace_id: str | None) -> dict:
    if not workspace_id:
        return _default_user_settings()
    try:
        res = (
            sb.table("user_settings")
            .select("*")
            .eq("user_id", user_id)
            .eq("workspace_id", workspace_id)
            .limit(1)
            .execute()
        )
        if res.data:
            return _normalize_user_settings(res.data[0])
    except Exception:
        pass
    return _ensure_user_settings(sb, user_id, workspace_id)


def _save_user_settings(sb, user_id: str, workspace_id: str | None, updates: dict) -> dict:
    if not workspace_id:
        return _default_user_settings()
    normalized = _normalize_user_settings(updates)
    row = {"user_id": user_id, "workspace_id": workspace_id, **normalized}
    try:
        res = (
            sb.table("user_settings")
            .upsert(row, on_conflict="user_id,workspace_id")
            .execute()
        )
        if res.data:
            return _normalize_user_settings(res.data[0])
    except Exception:
        pass
    return normalized


def _count_followups_due(sb, workspace_id: str | None) -> int:
    if not workspace_id:
        return 0
    try:
        rows = (
            sb.table("case_files")
            .select("id,status,follow_up_due")
            .eq("workspace_id", workspace_id)
            .execute()
        )
        now = datetime.now(timezone.utc).date()
        due = 0
        for row in rows.data or []:
            status = (row.get("status") or "").strip().lower()
            follow_up_due = (row.get("follow_up_due") or "").strip()
            if status == "follow up":
                due += 1
                continue
            if follow_up_due:
                try:
                    date_str = follow_up_due.split("T")[0]
                    if datetime.fromisoformat(date_str).date() <= now:
                        due += 1
                except Exception:
                    continue
        return due
    except Exception:
        return 0


def _count_new_leads_today(sb, workspace_id: str | None) -> int:
    if not workspace_id:
        return 0
    try:
        rows = (
            sb.table("opportunities")
            .select("created_at")
            .eq("workspace_id", workspace_id)
            .execute()
        )
        today = datetime.now(timezone.utc).date()
        count = 0
        for row in rows.data or []:
            created = (row.get("created_at") or "").strip()
            if not created:
                continue
            try:
                iso = created.replace("Z", "+00:00")
                if datetime.fromisoformat(iso).astimezone(timezone.utc).date() == today:
                    count += 1
            except Exception:
                continue
        return count
    except Exception:
        return 0


def _build_lead_briefing_summary(sb, workspace_id: str | None, workspace_name: str | None = None) -> dict:
    top = getTopOpportunities(workspace_id)
    return {
        "workspace_name": workspace_name or "Workspace",
        "new_leads": _count_new_leads_today(sb, workspace_id),
        "top_opportunities": top,
        "followups_due": _count_followups_due(sb, workspace_id),
        "dashboard_url": _dashboard_url or "",
    }


def sendLeadBriefingEmail(user: dict, summary: dict):
    if not _resend_api_key or not _resend_from_email:
        print("  email alerts disabled")
        return False
    to_email = (user.get("email") or "").strip()
    if not to_email:
        print("  email alerts disabled")
        return False

    top = summary.get("top_opportunities") or []
    top_lines = []
    for lead in top[:5]:
        lane = "No Website" if str(lead.get("lane") or "") == "no_website" else "Weak Website"
        top_lines.append(
            f"- {lead.get('business_name') or 'Lead'} | {lane} | Score {lead.get('score') or 0} | "
            f"{lead.get('best_contact_method') or 'Contact unknown'}"
        )

    subject = (
        f"Scout-Brain Daily Lead Briefing — {summary.get('new_leads', 0)} New Opportunities"
    )
    body = "\n".join(
        [
            f"Hi {(user.get('display_name') or user.get('email') or 'there')},",
            "",
            "Here is your Scout-Brain lead briefing.",
            "",
            "Summary",
            f"- New leads discovered: {summary.get('new_leads', 0)}",
            f"- Follow-ups due: {summary.get('followups_due', 0)}",
            "",
            "Top Opportunities",
            *(top_lines or ["- No top opportunities right now."]),
            "",
            "Open Scout-Brain Dashboard",
            summary.get("dashboard_url") or "Open your Scout-Brain dashboard",
            "",
            "— Scout-Brain",
        ]
    )

    print("  email alert sending")
    payload = {
        "from": _resend_from_email,
        "to": [to_email],
        "subject": subject,
        "text": body,
    }
    req = urllib_request.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_resend_api_key}",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=20) as resp:
            code = getattr(resp, "status", 200)
            if 200 <= code < 300:
                print("  email alert sent")
                return True
    except Exception as e:
        print(f"  [Scout] email alert error: {e}", file=sys.stderr)
    return False


def _frequency_allows_send(settings: dict) -> bool:
    if not settings.get("email_notifications_enabled"):
        return False
    freq = (settings.get("email_frequency") or "daily").strip().lower()
    if freq == "off":
        return False
    if freq == "weekly":
        return datetime.now(timezone.utc).weekday() == 0
    return True


def _send_workspace_briefing_if_enabled(sb, owner_user: dict, workspace: dict) -> None:
    workspace_id = workspace.get("id")
    user_id = owner_user.get("id")
    if not workspace_id or not user_id:
        return
    settings = _load_user_settings(sb, user_id, workspace_id)
    if not _frequency_allows_send(settings):
        print("  email alerts disabled")
        return

    summary = _build_lead_briefing_summary(sb, workspace_id, workspace.get("name"))
    if not settings.get("include_new_leads"):
        summary["new_leads"] = 0
    if not settings.get("include_followups"):
        summary["followups_due"] = 0
    if not settings.get("include_top_opportunities"):
        summary["top_opportunities"] = []
    sendLeadBriefingEmail(owner_user, summary)


def _run_scheduled_scout_job():
    if not _supabase_url or not _supabase_service_key:
        return
    print("  scheduled scout starting")
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        _run_morning_runner()

        workspaces = sb.table("workspaces").select("id,name,owner_user_id").execute().data or []
        for ws in workspaces:
            owner_id = ws.get("owner_user_id")
            if not owner_id:
                continue
            _sync_scout_to_supabase(owner_id, workspace_id=ws.get("id"))
            user_rows = (
                sb.table("profiles")
                .select("id,email,display_name")
                .eq("id", owner_id)
                .limit(1)
                .execute()
                .data
                or []
            )
            if user_rows:
                _send_workspace_briefing_if_enabled(sb, user_rows[0], ws)

        print("  scheduled scout finished")
    except Exception as e:
        print(f"  [Scout] scheduled scout error: {e}", file=sys.stderr)


def _bootstrap_workspace_for_user(sb, user_id: str) -> dict:
    membership = (
        sb.table("workspace_users")
        .select("workspace_id, role, workspaces:workspace_id(id,name)")
        .eq("user_id", user_id)
        .order("created_at", desc=False)
        .limit(1)
        .execute()
    )
    if membership.data:
        row = membership.data[0]
        ws = row.get("workspaces") or {}
        return {
            "workspace_id": row.get("workspace_id"),
            "workspace_name": ws.get("name") or "Workspace",
            "role": row.get("role") or "member",
            "created": False,
        }

    profile = (
        sb.table("profiles")
        .select("display_name,email")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    p = (profile.data or [{}])[0]
    display_name = (p.get("display_name") or "").strip()
    email = (p.get("email") or "").strip()
    base_name = display_name or (email.split("@")[0].strip() if "@" in email else "")
    workspace_name = f"{base_name}'s Workspace" if base_name else "Personal Workspace"

    created_ws = (
        sb.table("workspaces")
        .insert({"name": workspace_name, "owner_user_id": user_id, "plan": None})
        .execute()
    )
    if not created_ws.data:
        raise RuntimeError("workspace_create_failed")
    workspace_id = created_ws.data[0]["id"]

    sb.table("workspace_users").insert(
        {"workspace_id": workspace_id, "user_id": user_id, "role": "owner"}
    ).execute()

    try:
        sb.table("user_settings").upsert(
            {
                "user_id": user_id,
                "workspace_id": workspace_id,
                **_default_user_settings(),
            },
            on_conflict="user_id,workspace_id",
        ).execute()
    except Exception:
        pass

    return {
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "role": "owner",
        "created": True,
    }


def _append_history(count: int, summary: str):
    history = []
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []
    if not isinstance(history, list):
        history = []
    from datetime import datetime, timezone
    history.append({
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": count,
        "summary": summary,
    })
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history[-100:], f, indent=2)  # keep last 100 runs


def _audit_url(url: str):
    from scout.audit import fetch_and_audit
    return fetch_and_audit(url)


def _serve_frontend_index():
    """Serve built Vite frontend; fall back to ui/ for local pre-build runs."""
    if DIST_INDEX_PATH.is_file():
        return FileResponse(DIST_INDEX_PATH, media_type="text/html")

    index_file = UI_DIR / "index.html"
    if index_file.is_file():
        return FileResponse(index_file, media_type="text/html")

    raise HTTPException(status_code=404, detail="Frontend not found")


# --- Routes -----------------------------------------------------------------

@app.get("/")
def serve_root():
    if SERVE_FRONTEND:
        return _serve_frontend_index()
    return {"ok": True, "service": "scout-brain-backend", "mode": "api-only"}


if SERVE_FRONTEND:
    # Serve compiled Vite assets only when frontend serving is explicitly enabled.
    app.mount("/assets", StaticFiles(directory=str(DIST_ASSETS_DIR), check_dir=False), name="assets")
    app.mount("/ui", StaticFiles(directory=str(UI_DIR), html=True), name="ui")


@app.get("/scout-data")
def get_scout_data():
    try:
        return _load_scout_data()
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "today": None, "opportunities": []},
        )


@app.get("/top-opportunities")
def get_top_opportunities(request: Request):
    workspace_id = _get_workspace_id_from_request(request)
    leads = getTopOpportunities(workspace_id)
    return {"leads": leads}


def _scout_error_response(error_type: str, error_message: str, user_friendly_message: str):
    payload = {
        "success": False,
        "ok": False,
        "error_type": error_type,
        "error_message": error_message,
        "user_friendly_message": user_friendly_message,
        "today": None,
        "opportunities": [],
    }
    print(f"  [Scout failed] {error_type} | {error_message}", file=sys.stderr)
    return JSONResponse(status_code=200, content=payload)


class RunScoutBody(BaseModel):
    current_lat: float | None = None
    current_lng: float | None = None


class UserSettingsBody(BaseModel):
    email_notifications_enabled: bool = True
    email_frequency: str = "daily"
    include_new_leads: bool = True
    include_followups: bool = True
    include_top_opportunities: bool = True


@app.post("/run-scout")
def post_run_scout(request: Request, body: RunScoutBody | None = None):
    try:
        current_lat = body.current_lat if body else None
        current_lng = body.current_lng if body else None
        if current_lat is not None and current_lng is not None:
            print(f"  [Scout] run scout using current location: {current_lat}, {current_lng}")
        else:
            print("  [Scout] run scout using saved config location")
        _run_morning_runner(current_lat=current_lat, current_lng=current_lng)
        data = _load_scout_data()
        today = data["today"]
        opportunities = data["opportunities"]
        _append_history(len(opportunities), today.get("summary", ""))
        user_id = _get_user_id_from_request(request)
        workspace_id = _get_workspace_id_from_request(request)
        if user_id and _supabase_url and _supabase_service_key:
            _sync_scout_to_supabase(user_id, workspace_id=workspace_id)
        return {"ok": True, "success": True, "stdout": "", "stderr": "", "today": today, "opportunities": opportunities}
    except Exception as e:
        err_str = str(e).upper()
        err_lower = str(e).lower()

        if ScoutRunError and isinstance(e, ScoutRunError):
            return _scout_error_response(
                e.error_type,
                e.error_message,
                e.user_friendly_message,
            )
        if "REQUEST_DENIED" in err_str or "LEGACY" in err_str:
            return _scout_error_response(
                "REQUEST_DENIED",
                str(e),
                "Scout failed: API returned REQUEST_DENIED. Enable 'Places API (New)' and "
                "'Geocoding API' in your Google Cloud project. You may be calling a legacy API that is not enabled.",
            )
        if "api" in err_lower and "key" in err_lower:
            return _scout_error_response(
                "api_key_missing",
                str(e),
                "Scout failed: Google Maps API key not configured. Set GOOGLE_MAPS_API_KEY in backend environment variables.",
            )
        if "certificate" in err_lower or "ssl" in err_lower:
            return _scout_error_response(
                "ssl_verify_failed",
                str(e),
                "Scout failed: Python SSL certificate verification failed while calling Google APIs.",
            )
        if "geocode" in err_lower or "resolve" in err_lower:
            return _scout_error_response(
                "geocode_failed",
                str(e),
                "Scout could not resolve the configured city. Check SSL certificates or API access.",
            )
        if "supabase" in err_lower or "insert" in err_lower and "fail" in err_lower:
            return _scout_error_response(
                "supabase_insert_failure",
                str(e),
                f"Scout failed: database insert failed. {str(e)}",
            )
        if "valid" in err_lower or "missing" in err_lower:
            return _scout_error_response(
                "validation_failure",
                str(e),
                f"Scout failed: validation error. {str(e)}",
            )
        return _scout_error_response(
            "scout_error",
            str(e),
            f"Scout failed: {str(e)}",
        )


@app.get("/user-settings")
def get_user_settings(request: Request):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not _supabase_url or not _supabase_service_key:
        return _default_user_settings()
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        requested_workspace = _get_workspace_id_from_request(request)
        workspace_id = _resolve_workspace_id_for_user(sb, user_id, requested_workspace)
        settings = _load_user_settings(sb, user_id, workspace_id)
        return settings
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load settings: {e}")


@app.post("/user-settings")
def post_user_settings(request: Request, body: UserSettingsBody):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not _supabase_url or not _supabase_service_key:
        return _normalize_user_settings(body.model_dump())
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        requested_workspace = _get_workspace_id_from_request(request)
        workspace_id = _resolve_workspace_id_for_user(sb, user_id, requested_workspace)
        settings = _save_user_settings(sb, user_id, workspace_id, body.model_dump())
        return settings
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save settings: {e}")


@app.post("/workspace/bootstrap")
def post_workspace_bootstrap(request: Request):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not _supabase_url or not _supabase_service_key:
        raise HTTPException(status_code=500, detail="Supabase is not configured")
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        result = _bootstrap_workspace_for_user(sb, user_id)
        return {"ok": True, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not bootstrap workspace: {e}")


class AuditBody(BaseModel):
    url: str


class CaseUpdateBody(BaseModel):
    status: str | None = None
    first_contacted_at: str | None = None
    last_contacted_at: str | None = None
    follow_up_due: str | None = None
    outcome: str | None = None
    outreach_notes: str | None = None
    short_email: str | None = None
    longer_email: str | None = None
    contact_form_version: str | None = None
    social_dm_version: str | None = None
    follow_up_note: str | None = None


@app.get("/case/{slug}")
def get_case_raw(slug: str):
    """Return raw case JSON for a lead. Used for debug / View raw case JSON."""
    path = CASES_DIR / f"{slug}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Case {slug} not found")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@app.post("/case/{slug}/update")
def post_case_update(slug: str, body: CaseUpdateBody):
    """Update outreach queue fields for a case. Persists to scout/cases/{slug}.json."""
    path = CASES_DIR / f"{slug}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Case {slug} not found")
    try:
        with open(path, encoding="utf-8") as f:
            case = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read case: {e}")

    now_iso = None
    if body.status is not None:
        case["status"] = body.status
        if body.status == "Contacted":
            from datetime import datetime, timezone
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if not case.get("first_contacted_at"):
                case["first_contacted_at"] = now_iso
            case["last_contacted_at"] = now_iso
    if body.first_contacted_at is not None:
        case["first_contacted_at"] = body.first_contacted_at
    if body.last_contacted_at is not None:
        case["last_contacted_at"] = body.last_contacted_at
    if body.follow_up_due is not None:
        case["follow_up_due"] = body.follow_up_due
    if body.outcome is not None:
        case["outcome"] = body.outcome
    if body.outreach_notes is not None:
        case["outreach_notes"] = body.outreach_notes
    if body.short_email is not None:
        case["short_email"] = body.short_email
    if body.longer_email is not None:
        case["longer_email"] = body.longer_email
    if body.contact_form_version is not None:
        case["contact_form_version"] = body.contact_form_version
    if body.social_dm_version is not None:
        case["social_dm_version"] = body.social_dm_version
    if body.follow_up_note is not None:
        case["follow_up_note"] = body.follow_up_note
        case["follow_up_line"] = body.follow_up_note

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(case, f, indent=2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save case: {e}")

    from scout.case_schema import case_to_ui
    return {"ok": True, "case": case_to_ui(case)}


@app.post("/case/{slug}/regenerate-outreach")
def post_case_regenerate_outreach(slug: str):
    """Regenerate outreach pack for one case using current dossier data."""
    path = CASES_DIR / f"{slug}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Case {slug} not found")
    try:
        with open(path, encoding="utf-8") as f:
            case = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read case: {e}")

    try:
        from scout.outreach_generator import generate_outreach_pack
        city_hint = None
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)
                city_hint = cfg.get("home_city")
        pack = generate_outreach_pack(case, city_hint=city_hint)
        case["short_email"] = pack.get("short_email")
        case["longer_email"] = pack.get("longer_email")
        case["contact_form_version"] = pack.get("contact_form_version")
        case["social_dm_version"] = pack.get("social_dm_version")
        case["follow_up_note"] = pack.get("follow_up_note")
        case["follow_up_line"] = pack.get("follow_up_line")
        print(f"  [Scout] outreach pack regenerated: {slug}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not regenerate outreach: {e}")

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(case, f, indent=2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save case: {e}")

    from scout.case_schema import case_to_ui
    return {"ok": True, "case": case_to_ui(case)}


@app.post("/audit")
def post_audit(body: AuditBody):
    url = (body.url or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="missing url")
    try:
        return _audit_url(url)
    except Exception as e:
        return {
            "url": url,
            "facts": [f"Error: {str(e)}"],
            "problems": ["Website could not be fully fetched"],
            "pitch": ["review manually and use a simpler outreach angle"],
        }


@app.get("/scout/config.json")
def get_scout_config():
    if not CONFIG_PATH.exists():
        raise HTTPException(status_code=404, detail="scout/config.json not found")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


if SERVE_FRONTEND:
    @app.get("/{full_path:path}", response_class=HTMLResponse)
    def spa_fallback(full_path: str):
        """
        SPA fallback for frontend routes.
        Keep API/static prefixes protected so backend routes are not shadowed.
        """
        protected_prefixes = ("run-scout", "scout-data", "audit", "case", "scout", "assets", "ui")
        for prefix in protected_prefixes:
            if full_path == prefix or full_path.startswith(f"{prefix}/"):
                raise HTTPException(status_code=404, detail="Not found")
        return _serve_frontend_index()


def _start_scheduler():
    global _scheduler
    if _scheduler is not None or not ENABLE_SCHEDULED_SCOUT:
        return
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        _scheduler = BackgroundScheduler(timezone="UTC")
        _scheduler.add_job(
            _run_scheduled_scout_job,
            CronTrigger(hour=SCHEDULED_SCOUT_HOUR, minute=0),
            id="scheduled-scout-daily",
            replace_existing=True,
        )
        _scheduler.start()
        print(f"  Scheduled scout enabled at {SCHEDULED_SCOUT_HOUR:02d}:00 UTC")
    except Exception as e:
        print(f"  [Scout] scheduler start failed: {e}", file=sys.stderr)
        _scheduler = None


def _stop_scheduler():
    global _scheduler
    if _scheduler is None:
        return
    try:
        _scheduler.shutdown(wait=False)
    except Exception:
        pass
    _scheduler = None


@app.on_event("startup")
def _on_startup():
    _start_scheduler()


@app.on_event("shutdown")
def _on_shutdown():
    _stop_scheduler()


def _open_browser():
    time.sleep(1.2)
    webbrowser.open("http://localhost:8760")


def main():
    import uvicorn
    port = int(os.environ.get("PORT", 8760))
    print()
    print("  Massive Brain - backend")
    print("  -------------------------")
    print(f"  App running at:  http://0.0.0.0:{port}")
    print(f"  FRONTEND_SERVING: {'enabled' if SERVE_FRONTEND else 'disabled (api-only)'}")
    print(f"  SCHEDULED_SCOUT: {'enabled' if ENABLE_SCHEDULED_SCOUT else 'disabled'}")
    print(f"  SCHEDULED_SCOUT_HOUR: {SCHEDULED_SCOUT_HOUR:02d}:00 UTC")
    print()
    if _env_loaded:
        if _maps_key:
            print("  GOOGLE_MAPS_API_KEY: set")
        else:
            print("  GOOGLE_MAPS_API_KEY: not set (required for Run Scout)")
        if _supabase_url and _supabase_service_key:
            print("  SUPABASE: configured (scout results will sync)")
        else:
            print("  SUPABASE: not set (scout results local only)")
        if _resend_api_key and _resend_from_email:
            print("  EMAIL_ALERTS: configured (Resend)")
        else:
            print("  EMAIL_ALERTS: not set (daily briefing emails disabled)")
    else:
        print("  .env: python-dotenv not installed or .env missing")
    print(f"  ALLOWED_ORIGINS: {', '.join(_allowed_origins)}")
    if _allowed_origin_regex:
        print(f"  ALLOWED_ORIGIN_REGEX: {_allowed_origin_regex}")
    else:
        print("  ALLOWED_ORIGIN_REGEX: disabled")
    print()
    if port == 8760:
        threading.Thread(target=_open_browser, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
