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
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from urllib import request as urllib_request
from urllib import error as urllib_error
from urllib.parse import urlparse
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
CASE_FILES_DIR = SCOUT_DIR / "case_files"
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
_last_email_provider_diag: dict = {}
_supabase_jwks_cache: dict | None = None
_supabase_jwks_cache_expires_at: float = 0.0
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


def _email_sender_config() -> dict:
    outreach_from_email = (os.environ.get("OUTREACH_FROM_EMAIL") or "").strip()
    resend_from_email = (_resend_from_email or "").strip()
    from_name = (os.environ.get("OUTREACH_FROM_NAME") or "Scout-Brain").strip()
    if outreach_from_email:
        sender_source = "OUTREACH_FROM_EMAIL"
        from_email = outreach_from_email
    elif resend_from_email:
        sender_source = "RESEND_FROM_EMAIL"
        from_email = resend_from_email
    else:
        sender_source = "missing"
        from_email = ""
    return {
        "from_email": from_email,
        "from_name": from_name,
        "sender_source": sender_source,
        "has_resend_api_key": bool(_resend_api_key),
        "has_outreach_from_email": bool(outreach_from_email),
        "has_resend_from_email": bool(resend_from_email),
    }


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Railway backend-only mode should not require npm or frontend assets.
# Enable only when intentionally serving frontend from this process.
SERVE_FRONTEND = _env_flag("SERVE_FRONTEND", default=False)
ENABLE_SCHEDULED_SCOUT = _env_flag("ENABLE_SCHEDULED_SCOUT", default=True)
SCHEDULED_SCOUT_HOUR = int(os.environ.get("SCHEDULED_SCOUT_HOUR", "2"))
SCHEDULED_SCOUT_TIMEZONE = (os.environ.get("SCHEDULED_SCOUT_TIMEZONE", "local") or "local").strip()
SCHEDULED_SCOUT_JOB_NAME = (os.environ.get("SCHEDULED_SCOUT_JOB_NAME", "nightly_scout_run") or "nightly_scout_run").strip()
SCHEDULED_SCOUT_SCOPE = (os.environ.get("SCHEDULED_SCOUT_SCOPE", "internal") or "internal").strip().lower()
SCHEDULED_SCOUT_WORKSPACE_NAME = (os.environ.get("SCHEDULED_SCOUT_WORKSPACE_NAME", "MixedMakerShop") or "MixedMakerShop").strip()
SCHEDULED_SCOUT_REGIONS = ["northwest", "central", "river_valley", "delta", "south", "ouachita"]
CRM_AUTO_INTAKE_ENABLED = _env_flag("CRM_AUTO_INTAKE_ENABLED", default=True)
CRM_INTAKE_MIN_SCORE = float(os.environ.get("CRM_INTAKE_MIN_SCORE", "80"))
CRM_INTAKE_MAX_CANDIDATES = int(os.environ.get("CRM_INTAKE_MAX_CANDIDATES", "250"))
AUTO_SEQUENCE_SEND_STEP1 = _env_flag("AUTO_SEQUENCE_SEND_STEP1", default=False)
SEQUENCE_STEP_2_DELAY_DAYS = int(os.environ.get("SEQUENCE_STEP_2_DELAY_DAYS", "3"))
SEQUENCE_STEP_3_DELAY_DAYS = int(os.environ.get("SEQUENCE_STEP_3_DELAY_DAYS", "7"))
CRM_SUPABASE_URL = (os.environ.get("CRM_SUPABASE_URL", "") or "").strip() or _supabase_url
CRM_SUPABASE_SERVICE_ROLE_KEY = (os.environ.get("CRM_SUPABASE_SERVICE_ROLE_KEY", "") or "").strip() or _supabase_service_key
CRM_LEADS_TABLE = (os.environ.get("CRM_LEADS_TABLE", "leads") or "leads").strip()
INBOUND_EMAIL_WEBHOOK_SECRET = (os.environ.get("INBOUND_EMAIL_WEBHOOK_SECRET", "") or "").strip()
SCOUT_WORKSPACE_ID = (os.environ.get("SCOUT_WORKSPACE_ID", "") or "").strip()
SCHEDULED_SCOUT_WORKSPACE = (os.environ.get("SCHEDULED_SCOUT_WORKSPACE", "") or "").strip()
SCOUT_VERBOSE_LOGS = _env_flag("SCOUT_VERBOSE_LOGS", default=False)
SCOUT_AUTH_DEBUG = _env_flag("SCOUT_AUTH_DEBUG", default=False)

app = FastAPI(title="Massive Brain", version="2.0")

_scheduler = None
_scout_jobs: dict[str, dict] = {}
_scout_jobs_lock = threading.Lock()
_daily_scout_lock = threading.Lock()

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


@app.get("/healthz")
def get_healthz():
    snapshot = _runtime_config_snapshot()
    return {
        "ok": True,
        "service": "scout-brain",
        "scheduler_enabled": bool(ENABLE_SCHEDULED_SCOUT),
        "runtime": {
            # Canonical keys requested by admin diagnostics.
            "has_supabase_url": bool(snapshot.get("supabase_url_present")),
            "has_service_role_key": bool(snapshot.get("supabase_service_key_present")),
            "backend_supabase_host": snapshot.get("supabase_url_host"),
            "crm_supabase_host": snapshot.get("crm_supabase_url_host"),
            "workspace_fallback_set": bool(
                snapshot.get("scout_workspace_id_env_set")
                or snapshot.get("scheduled_scout_workspace_env_set")
            ),
            # Backward-compatible fields used by existing debug panels.
            "supabase_url_present": bool(snapshot.get("supabase_url_present")),
            "supabase_service_key_present": bool(snapshot.get("supabase_service_key_present")),
            "supabase_url_host": snapshot.get("supabase_url_host"),
            "crm_supabase_url_present": bool(snapshot.get("crm_supabase_url_present")),
            "crm_supabase_service_key_present": bool(snapshot.get("crm_supabase_service_key_present")),
            "crm_supabase_url_host": snapshot.get("crm_supabase_url_host"),
            "scout_workspace_id_env_set": bool(snapshot.get("scout_workspace_id_env_set")),
            "scheduled_scout_workspace_env_set": bool(snapshot.get("scheduled_scout_workspace_env_set")),
        },
    }


def _run_morning_runner(
    current_lat: float | None = None,
    current_lng: float | None = None,
    scan_settings: dict | None = None,
    progress_callback=None,
    cancel_callback=None,
):
    from scout.morning_runner import run
    run(
        current_lat=current_lat,
        current_lng=current_lng,
        scan_settings=scan_settings or {},
        progress_callback=progress_callback,
        cancel_callback=cancel_callback,
    )


def _nightly_region_for_today() -> str:
    day_of_year = max(1, int(datetime.now().timetuple().tm_yday))
    index = (day_of_year - 1) % len(SCHEDULED_SCOUT_REGIONS)
    return SCHEDULED_SCOUT_REGIONS[index]


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


def _job_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _job_store(job: dict) -> None:
    with _scout_jobs_lock:
        _scout_jobs[job["id"]] = dict(job)


def _job_get(job_id: str) -> dict | None:
    with _scout_jobs_lock:
        job = _scout_jobs.get(job_id)
        return dict(job) if job else None


def _job_update(job_id: str, **updates) -> dict | None:
    with _scout_jobs_lock:
        job = _scout_jobs.get(job_id)
        if not job:
            return None
        job.update(updates)
        _scout_jobs[job_id] = job
        return dict(job)


def _job_is_cancelled(job_id: str) -> bool:
    job = _job_get(job_id)
    if not job:
        job = _load_job_from_supabase(job_id)
    if not job:
        return False
    return str(job.get("status") or "").strip().lower() == "cancelled"


def _fetch_supabase_jwks(force_refresh: bool = False) -> dict | None:
    global _supabase_jwks_cache, _supabase_jwks_cache_expires_at
    if not _supabase_url:
        return None
    now_ts = time.time()
    if (
        not force_refresh
        and _supabase_jwks_cache is not None
        and now_ts < _supabase_jwks_cache_expires_at
    ):
        return _supabase_jwks_cache
    jwks_url = f"{_supabase_url.rstrip('/')}/auth/v1/.well-known/jwks.json"
    req = urllib_request.Request(
        jwks_url,
        headers={
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib_request.urlopen(req, timeout=8) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        payload = json.loads(raw) if raw else {}
    keys = payload.get("keys") if isinstance(payload, dict) else None
    if not isinstance(keys, list):
        raise RuntimeError("jwks_payload_missing_keys")
    _supabase_jwks_cache = payload
    _supabase_jwks_cache_expires_at = now_ts + 3600
    return payload


def _decode_rs_token_via_jwks(token: str, alg: str, kid: str | None) -> dict:
    import jwt

    def _attempt_decode(jwks_payload: dict) -> dict:
        keys = jwks_payload.get("keys") if isinstance(jwks_payload, dict) else []
        if not isinstance(keys, list) or not keys:
            raise RuntimeError("jwks_keys_empty")
        candidates = keys
        if kid:
            keyed = [k for k in keys if str(k.get("kid") or "").strip() == kid]
            if keyed:
                candidates = keyed
        last_error: Exception | None = None
        for jwk in candidates:
            try:
                public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
                return jwt.decode(
                    token,
                    public_key,
                    algorithms=[alg],
                    options={"verify_aud": False},
                )
            except Exception as decode_error:
                last_error = decode_error
                continue
        if last_error:
            raise last_error
        raise RuntimeError("jwks_key_decode_failed")

    jwks_payload = _fetch_supabase_jwks(force_refresh=False)
    if jwks_payload:
        try:
            return _attempt_decode(jwks_payload)
        except Exception:
            pass
    jwks_payload = _fetch_supabase_jwks(force_refresh=True)
    if not jwks_payload:
        raise RuntimeError("jwks_unavailable")
    return _attempt_decode(jwks_payload)


def _get_user_id_from_request(request: Request) -> str | None:
    """Verify Bearer JWT and return user_id (uuid). Returns None if no/invalid auth."""
    auth = request.headers.get("Authorization")
    if not auth or not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if not token:
        return None

    token_alg = None
    token_kid = None
    try:
        import jwt
        header = jwt.get_unverified_header(token)
        token_alg = str(header.get("alg") or "").strip() or None
        token_kid = str(header.get("kid") or "").strip() or None
    except Exception:
        token_alg = None
        token_kid = None

    supabase_verify_error_type = None
    if _supabase_url and _supabase_service_key:
        try:
            req = urllib_request.Request(
                f"{_supabase_url.rstrip('/')}/auth/v1/user",
                headers={
                    "Authorization": f"Bearer {token}",
                    "apikey": _supabase_service_key,
                },
                method="GET",
            )
            with urllib_request.urlopen(req, timeout=10) as resp:
                code = getattr(resp, "status", 200)
                raw = resp.read().decode("utf-8", errors="ignore")
                payload = json.loads(raw) if raw else {}
                if 200 <= int(code) < 300:
                    user_id = str(payload.get("id") or payload.get("sub") or "").strip()
                    if user_id:
                        return user_id
                supabase_verify_error_type = f"upstream_{code}"
        except Exception as e:
            supabase_verify_error_type = type(e).__name__
            if SCOUT_AUTH_DEBUG:
                print(f"  [Auth] supabase-auth-user verify failed: {supabase_verify_error_type}", file=sys.stderr)

    if token_alg and token_alg.upper().startswith("RS"):
        try:
            payload = _decode_rs_token_via_jwks(token, token_alg, token_kid)
            user_id = str(payload.get("sub") or payload.get("id") or "").strip()
            if user_id:
                return user_id
        except Exception:
            if SCOUT_AUTH_DEBUG:
                print("  [Auth] local-jwks verify failed", file=sys.stderr)

    if not _supabase_jwt_secret:
        return None

    try:
        import jwt
        allowed_algs = ["HS256", "HS384", "HS512"]
        if token_alg and token_alg.upper().startswith("HS"):
            allowed_algs = [token_alg.upper()]
        payload = jwt.decode(
            token,
            _supabase_jwt_secret,
            algorithms=allowed_algs,
            options={"verify_aud": False},
        )
        user_id = str(payload.get("sub") or "").strip()
        if user_id:
            return user_id
        return None
    except HTTPException:
        raise
    except Exception as e:
        if SCOUT_AUTH_DEBUG:
            reason = type(e).__name__
            print(f"  [Auth] local-hs verify failed: {reason}", file=sys.stderr)
            if supabase_verify_error_type:
                print(f"  [Auth] prior supabase verify failure: {supabase_verify_error_type}", file=sys.stderr)
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


def _safe_error_payload(error: Exception) -> dict:
    payload = {
        "type": type(error).__name__,
        "message": str(error),
        "args": [str(a) for a in getattr(error, "args", [])],
    }
    for key in ("code", "details", "hint", "status_code"):
        value = getattr(error, key, None)
        if value is not None:
            payload[key] = value
    response = getattr(error, "response", None)
    if response is not None:
        payload["response"] = str(response)
    return payload


def _log_write_stage(stage: str, event: str, meta: dict | None = None) -> None:
    if not SCOUT_VERBOSE_LOGS:
        important_events = {
            "job_created",
            "started",
            "finished",
            "failed",
            "workspace_unresolved",
            "workspace_resolution_failed",
            "workspace_fallback_applied",
        }
        if event not in important_events:
            return
    details = meta or {}
    try:
        printable = json.dumps(details, default=str)
    except Exception:
        printable = str(details)
    print(f"  [Persist:{stage}] {event} {printable}")


def _normalize_plan(plan: str | None) -> str:
    raw = (plan or "").strip().lower()
    if raw in {"internal", "free", "pro", "agency"}:
        return raw
    return "free"


def _workspace_env_fallback_id() -> str | None:
    """
    Optional workspace fallback for manual runs when membership lookup fails.
    """
    candidate = SCOUT_WORKSPACE_ID or SCHEDULED_SCOUT_WORKSPACE
    candidate = str(candidate or "").strip()
    return candidate or None


def _runtime_config_snapshot() -> dict:
    backend_host = urlparse(_supabase_url or "").netloc or None
    crm_host = urlparse(CRM_SUPABASE_URL or "").netloc or None
    return {
        "supabase_url_present": bool(_supabase_url),
        "supabase_service_key_present": bool(_supabase_service_key),
        "supabase_url_host": backend_host,
        "crm_supabase_url_present": bool(CRM_SUPABASE_URL),
        "crm_supabase_service_key_present": bool(CRM_SUPABASE_SERVICE_ROLE_KEY),
        "crm_supabase_url_host": crm_host,
        "scout_workspace_id_env_set": bool(SCOUT_WORKSPACE_ID),
        "scheduled_scout_workspace_env_set": bool(SCHEDULED_SCOUT_WORKSPACE),
    }


def _log_runtime_config(context: str) -> None:
    if context != "startup" and not SCOUT_VERBOSE_LOGS:
        return
    snapshot = _runtime_config_snapshot()
    print(
        f"  [Config:{context}] "
        f"SUPABASE_URL set={snapshot['supabase_url_present']} host={snapshot['supabase_url_host'] or 'missing'} | "
        f"SUPABASE_SERVICE_ROLE_KEY set={snapshot['supabase_service_key_present']} | "
        f"CRM_SUPABASE_URL set={snapshot['crm_supabase_url_present']} host={snapshot['crm_supabase_url_host'] or 'missing'} | "
        f"CRM_SUPABASE_SERVICE_ROLE_KEY set={snapshot['crm_supabase_service_key_present']} | "
        f"SCOUT_WORKSPACE_ID set={snapshot['scout_workspace_id_env_set']} | "
        f"SCHEDULED_SCOUT_WORKSPACE set={snapshot['scheduled_scout_workspace_env_set']}"
    )


def _get_workspace_for_user(sb, user_id: str, requested_workspace_id: str | None = None) -> dict:
    workspace_id = _resolve_workspace_id_for_user(sb, user_id, requested_workspace_id=requested_workspace_id)
    if workspace_id:
        try:
            ws = (
                sb.table("workspaces")
                .select("id,name,plan")
                .eq("id", workspace_id)
                .limit(1)
                .execute()
            )
            if ws.data:
                row = ws.data[0]
                return {
                    "id": row.get("id"),
                    "name": row.get("name") or "Workspace",
                    "plan": _normalize_plan(row.get("plan")),
                }
        except Exception:
            pass
    return {"id": workspace_id, "name": "Workspace", "plan": "free"}


def _get_workspace_usage(sb, workspace_id: str | None) -> dict:
    if not workspace_id:
        return {"monthly_scout_runs": 0, "saved_leads": 0}
    month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    runs_count = 0
    leads_count = 0
    try:
        runs = (
            sb.table("scout_runs")
            .select("id,created_at")
            .eq("workspace_id", workspace_id)
            .gte("created_at", month_start)
            .execute()
        )
        runs_count = len(runs.data or [])
    except Exception:
        runs_count = 0
    try:
        leads = (
            sb.table("opportunities")
            .select("id")
            .eq("workspace_id", workspace_id)
            .execute()
        )
        leads_count = len(leads.data or [])
    except Exception:
        leads_count = 0
    return {"monthly_scout_runs": runs_count, "saved_leads": leads_count}


def _plan_limits(plan: str) -> dict:
    normalized = _normalize_plan(plan)
    if normalized == "free":
        return {
            "max_scout_runs_per_month": 5,
            "max_saved_leads": 10,
            "full_outreach": False,
            "top_opportunities_dashboard": False,
            "daily_email_briefing": False,
        }
    return {
        "max_scout_runs_per_month": None,
        "max_saved_leads": None,
        "full_outreach": True,
        "top_opportunities_dashboard": True,
        "daily_email_briefing": True,
    }


def _plan_limit_message() -> str:
    return "Free plan limit reached. Upgrade to Pro for unlimited scouting."


def _check_plan_limits_for_run(plan: str, usage: dict) -> tuple[bool, str | None]:
    limits = _plan_limits(plan)
    max_runs = limits.get("max_scout_runs_per_month")
    max_leads = limits.get("max_saved_leads")
    if max_runs is not None and int(usage.get("monthly_scout_runs") or 0) >= int(max_runs):
        print("  plan limit reached")
        return False, _plan_limit_message()
    if max_leads is not None and int(usage.get("saved_leads") or 0) >= int(max_leads):
        print("  plan limit reached")
        return False, _plan_limit_message()
    return True, None


def _apply_outreach_plan_limits(case_row: dict, plan: str) -> dict:
    if _plan_limits(plan).get("full_outreach", True):
        return case_row
    limited = dict(case_row)
    limited["longer_email"] = None
    limited["contact_form_version"] = None
    limited["social_dm_version"] = None
    limited["follow_up_note"] = None
    limited["follow_up_line"] = None
    return limited


def _upsert_case_file_row(sb, cf_row: dict) -> str:
    """
    Write case_files by opportunity_id with duplicate-safe skip behavior.
    Returns one of: inserted, duplicate_skipped.
    """
    opportunity_id = cf_row.get("opportunity_id")
    if not opportunity_id:
        return "duplicate_skipped"

    # Requested behavior: if a case file already exists, skip insert.
    try:
        existing = (
            sb.table("case_files")
            .select("id")
            .eq("opportunity_id", opportunity_id)
            .limit(1)
            .execute()
        )
        if existing.data:
            print("  case file duplicate skipped")
            print("  scout run continuing after case file conflict")
            return "duplicate_skipped"
    except Exception:
        # Continue to insert attempt; duplicate guard below still protects the run.
        pass

    try:
        _insert_with_workspace_fallback(sb, "case_files", cf_row)
        print("  case file inserted")
        return "inserted"
    except Exception as e:
        message = str(e).lower()
        if "duplicate key value violates unique constraint" in message and "case_files_opportunity_id_key" in message:
            print("  case file duplicate skipped")
            print("  scout run continuing after case file conflict")
            return "duplicate_skipped"
        raise


def _sync_scout_to_supabase(
    user_id: str,
    workspace_id: str | None = None,
    workspace_plan: str | None = None,
) -> dict:
    """Load scout results from local files and upsert into Supabase (opportunities + case_files)."""
    stats = {
        "inserted": 0,
        "updated": 0,
        "duplicate_skipped": 0,
        "opportunities_attempted": 0,
        "opportunities_inserted": 0,
        "opportunities_updated": 0,
        "opportunities_failed": 0,
        "opportunity_errors": [],
        "case_files_attempted": 0,
        "case_files_failed": 0,
        "case_file_errors": [],
        "resolved_workspace_id": None,
    }
    if not _supabase_url or not _supabase_service_key:
        return stats
    try:
        from supabase import create_client
        data = _load_scout_data()
        opportunities_ui = data.get("opportunities") or []
        if not opportunities_ui:
            return stats
        sb = create_client(_supabase_url, _supabase_service_key)
        effective_workspace_id = _resolve_workspace_id_for_user(sb, user_id, requested_workspace_id=workspace_id)
        stats["resolved_workspace_id"] = effective_workspace_id
        effective_workspace_plan = _normalize_plan(workspace_plan)
        _log_write_stage(
            "sync",
            "started",
            {
                "user_id": user_id,
                "requested_workspace_id": workspace_id,
                "resolved_workspace_id": effective_workspace_id,
                "supabase_url": _supabase_url,
                "opportunities_loaded": len(opportunities_ui),
            },
        )
        existing_by_place_id: dict[str, str] = {}
        existing_by_website: dict[str, str] = {}
        existing_by_phone: dict[str, str] = {}
        existing_by_name_city: dict[str, str] = {}
        try:
            if effective_workspace_id:
                existing_rows = (
                    sb.table("opportunities")
                    .select("id,place_id,website,phone,business_name,city")
                    .eq("workspace_id", effective_workspace_id)
                    .limit(5000)
                    .execute()
                    .data
                    or []
                )
                for row in existing_rows:
                    row_id = str(row.get("id") or "").strip()
                    if not row_id:
                        continue
                    place_id_key = _normalize_text(row.get("place_id"))
                    website_key = _normalize_website(row.get("website"))
                    phone_key = _normalize_phone(row.get("phone"))
                    name_city_key = _build_name_city_key(row.get("business_name"), row.get("city"))
                    if place_id_key and place_id_key not in existing_by_place_id:
                        existing_by_place_id[place_id_key] = row_id
                    if website_key and website_key not in existing_by_website:
                        existing_by_website[website_key] = row_id
                    if phone_key and phone_key not in existing_by_phone:
                        existing_by_phone[phone_key] = row_id
                    if name_city_key and name_city_key not in existing_by_name_city:
                        existing_by_name_city[name_city_key] = row_id
        except Exception:
            pass
        def _reason_to_text(value) -> str | None:
            if isinstance(value, list):
                normalized = [str(v).strip() for v in value if str(v).strip()]
                if not normalized:
                    return None
                return " | ".join(normalized[:4])
            text = str(value or "").strip()
            return text or None

        for opp_ui in opportunities_ui:
            slug = opp_ui.get("slug") or opp_ui.get("id")
            name = (opp_ui.get("name") or opp_ui.get("business_name") or "").strip()
            if not name:
                continue
            stats["opportunities_attempted"] += 1
            opp_row = {
                "user_id": user_id,
                "workspace_id": effective_workspace_id,
                "business_name": name,
                "place_id": opp_ui.get("place_id"),
                "city": opp_ui.get("city"),
                "state": opp_ui.get("state"),
                "industry": opp_ui.get("industry") or opp_ui.get("category"),
                "category": opp_ui.get("category"),
                "lane": opp_ui.get("lane"),
                "distance_miles": opp_ui.get("distance_miles"),
                "address": opp_ui.get("address"),
                "phone": opp_ui.get("phone"),
                "website": opp_ui.get("website"),
                "maps_link": opp_ui.get("maps_url") or opp_ui.get("maps_link"),
                "rating": opp_ui.get("rating"),
                "google_rating": opp_ui.get("rating"),
                "website_score": opp_ui.get("website_score"),
                "website_status": opp_ui.get("website_status"),
                "website_speed": opp_ui.get("website_speed"),
                "mobile_ready": opp_ui.get("mobile_ready"),
                "seo_score": opp_ui.get("seo_score"),
                "website_quality_score": opp_ui.get("website_quality_score"),
                "review_count": opp_ui.get("review_count"),
                "hours": opp_ui.get("hours"),
                "no_website": bool(opp_ui.get("no_website")),
                "recommended_contact_method": opp_ui.get("recommended_contact") or opp_ui.get("recommended_contact_method"),
                "backup_contact_method": opp_ui.get("backup_contact_method"),
                "strongest_pitch_angle": opp_ui.get("pitch_angle") or opp_ui.get("strongest_pitch_angle"),
                "best_service_to_offer": opp_ui.get("best_service_to_offer"),
                "demo_to_show": opp_ui.get("demo_to_show"),
                "opportunity_score": opp_ui.get("opportunity_score") if opp_ui.get("opportunity_score") is not None else opp_ui.get("score") or opp_ui.get("internal_score"),
                "internal_score": opp_ui.get("score") or opp_ui.get("internal_score"),
                "lead_tier": opp_ui.get("lead_tier"),
                "tier": opp_ui.get("tier") or opp_ui.get("lead_tier"),
                "opportunity_signals": opp_ui.get("opportunity_signals") or [],
                "opportunity_reason": _reason_to_text(opp_ui.get("opportunity_reason")) or _reason_to_text(opp_ui.get("what_stood_out")),
                "lead_bucket": opp_ui.get("lead_bucket"),
                "close_probability": opp_ui.get("close_probability"),
                "lead_type": opp_ui.get("lead_type"),
                "best_contact_method": opp_ui.get("best_contact_method"),
                "best_pitch_angle": opp_ui.get("best_pitch_angle"),
                "recommended_next_action": opp_ui.get("recommended_next_action"),
                "priority": opp_ui.get("priority"),
                "status": opp_ui.get("status") or "New",
            }
            try:
                existing_id = None
                place_id_key = _normalize_text(opp_row.get("place_id"))
                website_key = _normalize_website(opp_row.get("website"))
                phone_key = _normalize_phone(opp_row.get("phone"))
                name_city_key = _build_name_city_key(opp_row.get("business_name"), opp_row.get("city"))
                if place_id_key and place_id_key in existing_by_place_id:
                    existing_id = existing_by_place_id.get(place_id_key)
                elif website_key and website_key in existing_by_website:
                    existing_id = existing_by_website.get(website_key)
                elif phone_key and phone_key in existing_by_phone:
                    existing_id = existing_by_phone.get(phone_key)
                elif name_city_key and name_city_key in existing_by_name_city:
                    existing_id = existing_by_name_city.get(name_city_key)
                if existing_id:
                    ins = sb.table("opportunities").update(opp_row).eq("id", existing_id).execute()
                    if not ins.data:
                        ins = {"data": [{"id": existing_id}]}
                else:
                    ins = _insert_with_workspace_fallback(sb, "opportunities", opp_row)
            except Exception as e:
                msg = str(e).lower()
                if any(
                    k in msg
                    for k in [
                        "opportunity_score",
                        "lead_tier",
                        "tier",
                        "opportunity_signals",
                        "opportunity_reason",
                        "lead_bucket",
                        "close_probability",
                        "lead_type",
                        "best_contact_method",
                        "best_pitch_angle",
                        "recommended_next_action",
                        "place_id",
                        "city",
                        "state",
                        "industry",
                        "website_score",
                        "google_rating",
                        "website_status",
                        "website_speed",
                        "mobile_ready",
                        "seo_score",
                        "website_quality_score",
                    ]
                ):
                    legacy_opp = dict(opp_row)
                    legacy_opp.pop("opportunity_score", None)
                    legacy_opp.pop("lead_tier", None)
                    legacy_opp.pop("tier", None)
                    legacy_opp.pop("opportunity_signals", None)
                    legacy_opp.pop("opportunity_reason", None)
                    legacy_opp.pop("lead_bucket", None)
                    legacy_opp.pop("close_probability", None)
                    legacy_opp.pop("lead_type", None)
                    legacy_opp.pop("best_contact_method", None)
                    legacy_opp.pop("best_pitch_angle", None)
                    legacy_opp.pop("recommended_next_action", None)
                    legacy_opp.pop("place_id", None)
                    legacy_opp.pop("city", None)
                    legacy_opp.pop("state", None)
                    legacy_opp.pop("industry", None)
                    legacy_opp.pop("website_score", None)
                    legacy_opp.pop("google_rating", None)
                    legacy_opp.pop("website_status", None)
                    legacy_opp.pop("website_speed", None)
                    legacy_opp.pop("mobile_ready", None)
                    legacy_opp.pop("seo_score", None)
                    legacy_opp.pop("website_quality_score", None)
                    if existing_id:
                        ins = sb.table("opportunities").update(legacy_opp).eq("id", existing_id).execute()
                        if not ins.data:
                            ins = {"data": [{"id": existing_id}]}
                    else:
                        ins = _insert_with_workspace_fallback(sb, "opportunities", legacy_opp)
                else:
                    stats["opportunities_failed"] += 1
                    payload = _safe_error_payload(e)
                    if len(stats["opportunity_errors"]) < 8:
                        stats["opportunity_errors"].append(payload)
                    _log_write_stage(
                        "opportunities",
                        "insert_failed",
                        {"business_name": name, "workspace_id": effective_workspace_id, "error": payload},
                    )
                    continue
            ins_data = ins.data if hasattr(ins, "data") else ins.get("data")
            if not ins_data or len(ins_data) == 0:
                stats["opportunities_failed"] += 1
                _log_write_stage(
                    "opportunities",
                    "insert_failed",
                    {
                        "business_name": name,
                        "workspace_id": effective_workspace_id,
                        "error": {"message": "Supabase returned no inserted/updated rows"},
                    },
                )
                continue
            opp_id = ins_data[0]["id"]
            if existing_id:
                stats["opportunities_updated"] += 1
            else:
                stats["opportunities_inserted"] += 1
            if place_id_key:
                existing_by_place_id[place_id_key] = opp_id
            if website_key:
                existing_by_website[website_key] = opp_id
            if phone_key:
                existing_by_phone[phone_key] = opp_id
            if name_city_key:
                existing_by_name_city[name_city_key] = opp_id
            case_path = CASES_DIR / f"{slug}.json"
            if case_path.exists():
                stats["case_files_attempted"] += 1
                with open(case_path, encoding="utf-8") as f:
                    case = json.load(f)
                case = _apply_outreach_plan_limits(case, effective_workspace_plan)
                cf_row = {
                    "opportunity_id": opp_id,
                    "workspace_id": effective_workspace_id,
                    "email": case.get("email"),
                    "contact_page": case.get("contact_page"),
                    "contact_form_url": case.get("contact_form_url"),
                    "phone_from_site": case.get("phone_from_site"),
                    "facebook": case.get("facebook"),
                    "facebook_url": case.get("facebook_url") or case.get("facebook"),
                    "instagram": case.get("instagram"),
                    "instagram_url": case.get("instagram_url") or case.get("instagram"),
                    "owner_manager_name": case.get("owner_manager_name"),
                    "owner_name": case.get("owner_name"),
                    "owner_title": case.get("owner_title"),
                    "owner_source_page": case.get("owner_source_page"),
                    "platform_used": case.get("platform_used"),
                    "homepage_title": case.get("homepage_title"),
                    "meta_description": case.get("meta_description"),
                    "viewport_ok": case.get("viewport_ok"),
                    "tap_to_call_present": case.get("tap_to_call_present"),
                    "menu_visibility": str(case.get("menu_visibility")) if case.get("menu_visibility") is not None else None,
                    "hours_visibility": str(case.get("hours_visibility")) if case.get("hours_visibility") is not None else None,
                    "directions_visibility": str(case.get("directions_visibility")) if case.get("directions_visibility") is not None else None,
                    "contact_form_present": case.get("contact_form_present"),
                    "website_score": case.get("website_score"),
                    "mobile_score": case.get("mobile_score"),
                    "design_score": case.get("design_score"),
                    "navigation_score": case.get("navigation_score"),
                    "conversion_score": case.get("conversion_score"),
                    "activity_summary": case.get("activity_summary") or [],
                    "website_audit": case.get("website_audit") or {},
                    "website_issues": case.get("website_issues") or [],
                    "audit_issues": case.get("audit_issues") or [],
                    "high_opportunity": bool(case.get("high_opportunity")),
                    "strongest_problems": case.get("strongest_problems"),
                    "short_email": case.get("short_email"),
                    "longer_email": case.get("longer_email"),
                    "contact_form_version": case.get("contact_form_version"),
                    "follow_up_note": case.get("follow_up_note"),
                    "desktop_screenshot_url": case.get("desktop_screenshot_url"),
                    "mobile_screenshot_url": case.get("mobile_screenshot_url"),
                    "contact_page_screenshot_url": case.get("contact_page_screenshot_url"),
                    "internal_screenshot_url": case.get("internal_screenshot_url"),
                    "outreach_notes": case.get("outreach_notes"),
                    "follow_up_due": case.get("follow_up_due"),
                    "outcome": case.get("outcome"),
                    "status": case.get("status") or "New",
                }
                try:
                    action = _upsert_case_file_row(sb, cf_row)
                    if action == "updated":
                        stats["updated"] += 1
                    elif action == "inserted":
                        stats["inserted"] += 1
                    else:
                        stats["duplicate_skipped"] += 1
                except Exception as e:
                    msg = str(e).lower()
                    if any(
                        k in msg
                        for k in [
                            "desktop_screenshot_url",
                            "mobile_screenshot_url",
                            "contact_page_screenshot_url",
                            "internal_screenshot_url",
                            "owner_name",
                            "owner_title",
                            "owner_source_page",
                            "website_score",
                            "mobile_score",
                            "design_score",
                            "navigation_score",
                            "conversion_score",
                            "activity_summary",
                            "website_audit",
                            "website_issues",
                            "audit_issues",
                            "high_opportunity",
                            "contact_form_url",
                            "facebook_url",
                            "instagram_url",
                        ]
                    ):
                        legacy_cf = dict(cf_row)
                        legacy_cf.pop("desktop_screenshot_url", None)
                        legacy_cf.pop("mobile_screenshot_url", None)
                        legacy_cf.pop("contact_page_screenshot_url", None)
                        legacy_cf.pop("internal_screenshot_url", None)
                        legacy_cf.pop("owner_name", None)
                        legacy_cf.pop("owner_title", None)
                        legacy_cf.pop("owner_source_page", None)
                        legacy_cf.pop("website_score", None)
                        legacy_cf.pop("mobile_score", None)
                        legacy_cf.pop("design_score", None)
                        legacy_cf.pop("navigation_score", None)
                        legacy_cf.pop("conversion_score", None)
                        legacy_cf.pop("activity_summary", None)
                        legacy_cf.pop("website_audit", None)
                        legacy_cf.pop("website_issues", None)
                        legacy_cf.pop("audit_issues", None)
                        legacy_cf.pop("high_opportunity", None)
                        legacy_cf.pop("contact_form_url", None)
                        legacy_cf.pop("facebook_url", None)
                        legacy_cf.pop("instagram_url", None)
                        try:
                            action = _upsert_case_file_row(sb, legacy_cf)
                            if action == "updated":
                                stats["updated"] += 1
                            elif action == "inserted":
                                stats["inserted"] += 1
                            else:
                                stats["duplicate_skipped"] += 1
                        except Exception as legacy_err:
                            legacy_msg = str(legacy_err).lower()
                            if (
                                "duplicate key value violates unique constraint" in legacy_msg
                                and "case_files_opportunity_id_key" in legacy_msg
                            ):
                                print("  case file duplicate skipped")
                                print("  scout run continuing after case file conflict")
                                stats["duplicate_skipped"] += 1
                                continue
                            stats["case_files_failed"] += 1
                            payload = _safe_error_payload(legacy_err)
                            if len(stats["case_file_errors"]) < 8:
                                stats["case_file_errors"].append(payload)
                            _log_write_stage(
                                "case_files",
                                "insert_failed",
                                {"opportunity_id": opp_id, "workspace_id": effective_workspace_id, "error": payload},
                            )
                            continue
                    else:
                        if (
                            "duplicate key value violates unique constraint" in msg
                            and "case_files_opportunity_id_key" in msg
                        ):
                            print("  case file duplicate skipped")
                            print("  scout run continuing after case file conflict")
                            stats["duplicate_skipped"] += 1
                            continue
                        stats["case_files_failed"] += 1
                        payload = _safe_error_payload(e)
                        if len(stats["case_file_errors"]) < 8:
                            stats["case_file_errors"].append(payload)
                        _log_write_stage(
                            "case_files",
                            "insert_failed",
                            {"opportunity_id": opp_id, "workspace_id": effective_workspace_id, "error": payload},
                        )
                        continue
        print(
            "  case file sync summary: "
            f"inserted {stats['inserted']} case files, "
            f"updated {stats['updated']} existing case files, "
            f"skipped {stats['duplicate_skipped']} duplicates"
        )
        _log_write_stage(
            "sync",
            "finished",
            {
                "workspace_id": effective_workspace_id,
                "opportunities_attempted": stats["opportunities_attempted"],
                "opportunities_inserted": stats["opportunities_inserted"],
                "opportunities_updated": stats["opportunities_updated"],
                "opportunities_failed": stats["opportunities_failed"],
                "case_files_attempted": stats["case_files_attempted"],
                "case_files_inserted": stats["inserted"],
                "case_files_updated": stats["updated"],
                "case_files_duplicates": stats["duplicate_skipped"],
                "case_files_failed": stats["case_files_failed"],
            },
        )
        return stats
    except Exception as e:
        _log_write_stage("sync", "failed", {"error": _safe_error_payload(e)})
        print(f"  [Scout] Supabase sync error: {e}", file=sys.stderr)
        raise


def _record_scout_run_supabase(
    user_id: str,
    workspace_id: str | None,
    today: dict | None,
    opportunities: list | None,
    nightly_report: dict | None = None,
) -> None:
    if not _supabase_url or not _supabase_service_key:
        return
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        opps = opportunities or []
        summary = (today or {}).get("summary") or ""
        processed_count = int((today or {}).get("processed_count") or len(opps))
        saved_count = int((today or {}).get("saved_count") or len(opps))
        skipped_count = int((today or {}).get("skipped_count") or max(0, processed_count - saved_count))
        no_website_count = int((today or {}).get("no_website") or len([o for o in opps if o.get("no_website") or o.get("lane") == "no_website"]))
        weak_websites_count = int((today or {}).get("weak_websites") or max(0, len(opps) - no_website_count))
        strong_opportunities = int(
            len(
                [
                    o
                    for o in opps
                    if float(o.get("opportunity_score") or o.get("score") or o.get("internal_score") or 0) >= 70
                ]
            )
        )
        businesses_discovered = int((today or {}).get("businesses_discovered") or len(opps))
        analyzed_total = int((today or {}).get("processed_count") or len(opps))
        high_opportunity_total = int(strong_opportunities)
        leads_found = int((today or {}).get("leads_found") or len(opps))
        cities_scanned = int((nightly_report or {}).get("cities_scanned") or (today or {}).get("cities_scanned") or 0)
        industries_scanned = int((nightly_report or {}).get("industries_scanned") or (today or {}).get("industries_scanned") or 0)
        businesses_found = int((nightly_report or {}).get("businesses_found") or (today or {}).get("businesses_found") or businesses_discovered)
        opportunities_scored = int((nightly_report or {}).get("opportunities_scored") or len(opps))
        leads_created = int((nightly_report or {}).get("leads_created") or 0)
        email_drafts_generated = int((nightly_report or {}).get("email_drafts_generated") or 0)
        run_date = datetime.now().date().isoformat()
        row = {
            "user_id": user_id,
            "workspace_id": workspace_id,
            "job_name": SCHEDULED_SCOUT_JOB_NAME,
            "run_date": run_date,
            "run_time": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
            "processed_count": processed_count,
            "saved_count": saved_count,
            "skipped_count": skipped_count,
            "businesses_discovered": businesses_discovered,
            "analyzed_total": analyzed_total,
            "high_opportunity_total": high_opportunity_total,
            "leads_found": leads_found,
            "strong_opportunities": strong_opportunities,
            "weak_websites": weak_websites_count,
            "no_website": no_website_count,
            "cities_scanned": cities_scanned,
            "industries_scanned": industries_scanned,
            "businesses_found": businesses_found,
            "opportunities_scored": opportunities_scored,
            "leads_created": leads_created,
            "email_drafts_generated": email_drafts_generated,
            "nightly_report": {
                "cities_scanned": cities_scanned,
                "industries_scanned": industries_scanned,
                "businesses_found": businesses_found,
                "opportunities_scored": opportunities_scored,
                "leads_created": leads_created,
                "email_drafts_generated": email_drafts_generated,
            },
        }
        try:
            _insert_with_workspace_fallback(sb, "scout_runs", row)
        except Exception as e:
            msg = str(e).lower()
            if any(
                k in msg
                for k in [
                    "run_date",
                    "run_time",
                    "businesses_discovered",
                    "analyzed_total",
                    "high_opportunity_total",
                    "leads_found",
                    "strong_opportunities",
                    "weak_websites",
                    "no_website",
                    "job_name",
                    "cities_scanned",
                    "industries_scanned",
                    "businesses_found",
                    "opportunities_scored",
                    "leads_created",
                    "email_drafts_generated",
                    "nightly_report",
                ]
            ):
                legacy = dict(row)
                legacy.pop("run_date", None)
                legacy.pop("run_time", None)
                legacy.pop("businesses_discovered", None)
                legacy.pop("analyzed_total", None)
                legacy.pop("high_opportunity_total", None)
                legacy.pop("leads_found", None)
                legacy.pop("strong_opportunities", None)
                legacy.pop("weak_websites", None)
                legacy.pop("no_website", None)
                legacy.pop("job_name", None)
                legacy.pop("cities_scanned", None)
                legacy.pop("industries_scanned", None)
                legacy.pop("businesses_found", None)
                legacy.pop("opportunities_scored", None)
                legacy.pop("leads_created", None)
                legacy.pop("email_drafts_generated", None)
                legacy.pop("nightly_report", None)
                _insert_with_workspace_fallback(sb, "scout_runs", legacy)
            else:
                raise
        print(f"  leads discovered: {leads_found}")
        print("  scout run stored")
    except Exception as e:
        print(f"  [Scout] scout_runs insert error: {e}", file=sys.stderr)


_scout_run_row_by_job_id: dict[str, str] = {}
_scout_run_optional_columns = {
    "job_id",
    "job_name",
    "status",
    "progress",
    "started_at",
    "completed_at",
    "discovered_count",
    "case_files_count",
    "stage",
    "message",
    "cities_scanned",
    "industries_scanned",
    "businesses_found",
    "opportunities_scored",
    "leads_created",
    "email_drafts_generated",
    "nightly_report",
}


def _safe_write_scout_run(sb, row: dict, existing_run_id: str | None = None):
    payload = dict(row)
    _log_write_stage(
        "scout_runs",
        "insert_attempted" if not existing_run_id else "update_attempted",
        {
            "workspace_id": payload.get("workspace_id"),
            "job_id": payload.get("job_id"),
            "status": payload.get("status"),
            "progress": payload.get("progress"),
        },
    )
    while True:
        try:
            if existing_run_id:
                result = (
                    sb.table("scout_runs")
                    .update(payload)
                    .eq("id", existing_run_id)
                    .execute()
                )
                _log_write_stage(
                    "scout_runs",
                    "update_succeeded",
                    {"workspace_id": payload.get("workspace_id"), "job_id": payload.get("job_id")},
                )
                return result
            result = _insert_with_workspace_fallback(sb, "scout_runs", payload)
            _log_write_stage(
                "scout_runs",
                "insert_succeeded",
                {"workspace_id": payload.get("workspace_id"), "job_id": payload.get("job_id")},
            )
            return result
        except Exception as e:
            msg = str(e).lower()
            removable = [k for k in list(payload.keys()) if k in _scout_run_optional_columns and k in msg]
            if not removable:
                _log_write_stage(
                    "scout_runs",
                    "write_failed",
                    {"workspace_id": payload.get("workspace_id"), "job_id": payload.get("job_id"), "error": _safe_error_payload(e)},
                )
                raise
            for key in removable:
                payload.pop(key, None)


def _checkpoint_scout_run_supabase(
    user_id: str,
    workspace_id: str | None,
    job: dict | None,
    today: dict | None = None,
    opportunities: list | None = None,
    sync_stats: dict | None = None,
) -> bool:
    if not _supabase_url or not _supabase_service_key or not user_id:
        return False
    try:
        from supabase import create_client

        sb = create_client(_supabase_url, _supabase_service_key)
        opps = opportunities or []
        processed_count = int((today or {}).get("processed_count") or len(opps))
        saved_count = int((today or {}).get("saved_count") or len(opps))
        skipped_count = int((today or {}).get("skipped_count") or max(0, processed_count - saved_count))
        strong_opportunities = int(
            len(
                [
                    o
                    for o in opps
                    if float(o.get("opportunity_score") or o.get("score") or o.get("internal_score") or 0) >= 70
                ]
            )
        )
        no_website_count = int(
            (today or {}).get("no_website")
            or len([o for o in opps if o.get("no_website") or o.get("lane") == "no_website"])
        )
        weak_websites_count = int((today or {}).get("weak_websites") or max(0, len(opps) - no_website_count))
        case_files_count = int((sync_stats or {}).get("inserted") or 0) + int((sync_stats or {}).get("updated") or 0)

        job_id = str((job or {}).get("id") or "").strip()
        summary = str((today or {}).get("summary") or (job or {}).get("result_summary") or "").strip()
        row = {
            "user_id": user_id,
            "workspace_id": workspace_id,
            "job_name": "manual_scout_run",
            "run_date": datetime.now().date().isoformat(),
            "run_time": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
            "processed_count": processed_count,
            "saved_count": saved_count,
            "skipped_count": skipped_count,
            "businesses_discovered": int((today or {}).get("businesses_discovered") or len(opps)),
            "analyzed_total": int((today or {}).get("processed_count") or len(opps)),
            "high_opportunity_total": int(strong_opportunities),
            "leads_found": int((today or {}).get("leads_found") or len(opps)),
            "strong_opportunities": strong_opportunities,
            "weak_websites": weak_websites_count,
            "no_website": no_website_count,
            "job_id": job_id or None,
            "status": (job or {}).get("status"),
            "progress": int((job or {}).get("progress") or 0),
            "started_at": (job or {}).get("started_at"),
            "completed_at": (job or {}).get("finished_at"),
            "discovered_count": int((today or {}).get("businesses_discovered") or len(opps)),
            "case_files_count": case_files_count,
            "stage": (job or {}).get("stage"),
            "message": (job or {}).get("message") or (job or {}).get("result_summary"),
            "cities_scanned": int((today or {}).get("cities_scanned") or 0),
            "industries_scanned": int((today or {}).get("industries_scanned") or 0),
            "businesses_found": int((today or {}).get("businesses_found") or len(opps)),
            "opportunities_scored": len(opps),
            "leads_created": 0,
            "email_drafts_generated": 0,
        }
        existing_run_id = _scout_run_row_by_job_id.get(job_id) if job_id else None
        result = _safe_write_scout_run(sb, row, existing_run_id=existing_run_id)
        if job_id and not existing_run_id:
            inserted = getattr(result, "data", None) or []
            inserted_id = str((inserted[0] or {}).get("id") or "").strip() if inserted else ""
            if inserted_id:
                _scout_run_row_by_job_id[job_id] = inserted_id
        return True
    except Exception as e:
        print(f"  [Scout] scout_runs checkpoint error: {e}", file=sys.stderr)
        return False


def _upsert_job_supabase(job: dict) -> None:
    if not _supabase_url or not _supabase_service_key:
        return
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        row = {
            "id": job.get("id"),
            "workspace_id": job.get("workspace_id"),
            "type": job.get("type"),
            "job_type": job.get("job_type") or job.get("type"),
            "status": job.get("status"),
            "progress": int(job.get("progress") or 0),
            "payload": job.get("payload") or {},
            "result_summary": job.get("result_summary"),
            "message": job.get("message") or job.get("result_summary"),
            "error": job.get("error"),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
        }
        try:
            sb.table("jobs").upsert(row, on_conflict="id").execute()
        except Exception as e:
            # Backward compatibility for schemas without job_type/message columns.
            msg = str(e).lower()
            if "job_type" in msg or "message" in msg:
                legacy = dict(row)
                legacy.pop("job_type", None)
                legacy.pop("message", None)
                sb.table("jobs").upsert(legacy, on_conflict="id").execute()
            else:
                raise
    except Exception:
        # Keep async jobs running even when jobs table is not migrated yet.
        return


def _load_job_from_supabase(job_id: str, workspace_id: str | None = None) -> dict | None:
    if not _supabase_url or not _supabase_service_key:
        return None
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        query = sb.table("jobs").select("*").eq("id", job_id).limit(1)
        if workspace_id:
            query = query.eq("workspace_id", workspace_id)
        res = query.execute()
        if not res.data:
            return None
        row = res.data[0]
        summary = row.get("message")
        if summary is None:
            summary = row.get("result_summary")
        job_type = row.get("job_type")
        if not job_type:
            job_type = row.get("type")
        return {
            "id": row.get("id"),
            "workspace_id": row.get("workspace_id"),
            "type": row.get("type") or job_type,
            "job_type": job_type,
            "status": row.get("status"),
            "progress": int(row.get("progress") or 0),
            "payload": row.get("payload") or {},
            "message": summary,
            "result_summary": summary,
            "error": row.get("error"),
            "created_at": row.get("created_at"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
        }
    except Exception:
        return None


def _job_progress(
    job_id: str,
    progress: int,
    status: str | None = None,
    result_summary: str | None = None,
    stage: str | None = None,
) -> None:
    updates: dict = {"progress": max(0, min(100, int(progress)))}
    if status:
        updates["status"] = status
    if result_summary is not None:
        updates["result_summary"] = result_summary
        updates["message"] = result_summary
    if stage:
        updates["stage"] = stage
    job = _job_update(job_id, **updates)
    if not job:
        restored = _load_job_from_supabase(job_id)
        if restored:
            _job_store(restored)
            job = _job_update(job_id, **updates)
    if job:
        if SCOUT_VERBOSE_LOGS:
            print(
                f"  job progress updated: {updates.get('progress')}% "
                f"status={updates.get('status') or job.get('status')} "
                f"stage={updates.get('stage') or job.get('stage') or 'n/a'}"
            )
        _upsert_job_supabase(job)


def _load_active_job_from_supabase(workspace_id: str | None) -> dict | None:
    if not _supabase_url or not _supabase_service_key or not workspace_id:
        return None
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        rows = (
            sb.table("jobs")
            .select("*")
            .eq("workspace_id", workspace_id)
            .in_("status", ["queued", "running"])
            .order("created_at", desc=True)
            .limit(20)
            .execute()
            .data
            or []
        )
        for row in rows:
            row_type = str(row.get("job_type") or row.get("type") or "").strip().lower()
            if row_type not in {"scout", "run_morning_scout"}:
                continue
            return _load_job_from_supabase(str(row.get("id")), workspace_id=workspace_id)
        return None
    except Exception:
        return None


def _workspace_ids_for_user(sb, user_id: str) -> list[str]:
    workspace_ids: list[str] = []
    try:
        rows = (
            sb.table("workspace_users")
            .select("workspace_id")
            .eq("user_id", user_id)
            .limit(200)
            .execute()
            .data
            or []
        )
        for row in rows:
            ws = str(row.get("workspace_id") or "").strip()
            if ws and ws not in workspace_ids:
                workspace_ids.append(ws)
    except Exception:
        pass
    if workspace_ids:
        return workspace_ids
    try:
        legacy_rows = (
            sb.table("workspace_memberships")
            .select("workspace_id")
            .eq("user_id", user_id)
            .limit(200)
            .execute()
            .data
            or []
        )
        for row in legacy_rows:
            ws = str(row.get("workspace_id") or "").strip()
            if ws and ws not in workspace_ids:
                workspace_ids.append(ws)
    except Exception:
        pass
    return workspace_ids


def _execute_scout_job(
    job_id: str,
    user_id: str | None,
    workspace_id: str | None,
    workspace_plan: str,
    current_lat: float | None,
    current_lng: float | None,
    scan_settings: dict | None = None,
) -> None:
    current_stage: str | None = None
    persistence_debug = {
        "scout_run_saved": False,
        "opportunities_created": 0,
        "opportunities_updated": 0,
        "case_files_created": 0,
        "case_files_updated": 0,
        "leads_created": 0,
        "duplicates_skipped": 0,
        "workspace_id": workspace_id,
        "backend_supabase_url": _supabase_url,
        "backend_admin_supabase_url": CRM_SUPABASE_URL,
        "admin_supabase_url": CRM_SUPABASE_URL,
        "backend_supabase_url_host": urlparse(_supabase_url or "").netloc or None,
        "backend_admin_supabase_url_host": urlparse(CRM_SUPABASE_URL or "").netloc or None,
        "admin_supabase_url_host": urlparse(CRM_SUPABASE_URL or "").netloc or None,
        "supabase_url_present": bool(_supabase_url),
        "supabase_service_key_present": bool(_supabase_service_key),
        "errors": [],
    }

    def _runner_progress_update(payload: dict) -> None:
        nonlocal current_stage
        if _job_is_cancelled(job_id):
            return
        stage = str(payload.get("stage") or "").strip() or None
        progress = int(payload.get("progress") or 0)
        message = str(payload.get("message") or "").strip() or "Scout running..."
        if stage and stage != current_stage:
            current_stage = stage
            print(f"  job stage changed: {stage}")
        _job_progress(
            job_id,
            progress,
            status="running",
            result_summary=message,
            stage=stage,
        )

    job = _job_update(
        job_id,
        status="running",
        started_at=_job_now_iso(),
        progress=10,
        message="Scout job queued",
        result_summary="Scout job queued",
        stage="queued",
    )
    if job:
        print("  scout job started")
        _upsert_job_supabase(job)
        if user_id:
            _checkpoint_scout_run_supabase(user_id, workspace_id, job)
    try:
        if not _supabase_url:
            persistence_debug["errors"].append(
                {
                    "stage": "runtime_config",
                    "error": "backend Supabase URL missing (SUPABASE_URL)",
                    "required_env": "SUPABASE_URL",
                }
            )
        if not _supabase_service_key:
            persistence_debug["errors"].append(
                {
                    "stage": "runtime_config",
                    "error": "backend service role key missing (SUPABASE_SERVICE_ROLE_KEY)",
                    "required_env": "SUPABASE_SERVICE_ROLE_KEY",
                }
            )
        if not workspace_id:
            persistence_debug["errors"].append(
                {
                    "stage": "workspace_resolution",
                    "error": "workspace_id unresolved before scout execution",
                    "fallback_workspace_env_present": bool(_workspace_env_fallback_id()),
                }
            )
        if _job_is_cancelled(job_id):
            cancelled = _job_update(
                job_id,
                status="cancelled",
                progress=100,
                message="Scout cancelled",
                result_summary="Scout cancelled",
                stage="cancelled",
                finished_at=_job_now_iso(),
                error=None,
            )
            if cancelled:
                _upsert_job_supabase(cancelled)
            return
        _job_progress(
            job_id,
            10,
            status="running",
            result_summary="Discovery started",
            stage="discovering_businesses",
        )
        if user_id:
            _checkpoint_scout_run_supabase(user_id, workspace_id, _job_get(job_id))
        _run_morning_runner(
            current_lat=current_lat,
            current_lng=current_lng,
            scan_settings=scan_settings or {},
            progress_callback=_runner_progress_update,
            cancel_callback=lambda: _job_is_cancelled(job_id),
        )
        if _job_is_cancelled(job_id):
            cancelled = _job_update(
                job_id,
                status="cancelled",
                progress=100,
                message="Scout cancelled",
                result_summary="Scout cancelled",
                stage="cancelled",
                finished_at=_job_now_iso(),
                error=None,
            )
            if cancelled:
                print("  scout job cancelled")
                _upsert_job_supabase(cancelled)
            return
        _job_progress(
            job_id,
            90,
            status="running",
            result_summary="Generating dossiers",
            stage="generating_dossiers",
        )
        if user_id:
            _checkpoint_scout_run_supabase(user_id, workspace_id, _job_get(job_id))

        data = _load_scout_data()
        today = data["today"]
        opportunities = data["opportunities"]
        _append_history(len(opportunities), today.get("summary", ""))
        _job_progress(
            job_id,
            96,
            status="running",
            result_summary="Saving results",
            stage="saving_results",
        )
        if user_id:
            _checkpoint_scout_run_supabase(user_id, workspace_id, _job_get(job_id))

        sync_stats = {"inserted": 0, "updated": 0, "duplicate_skipped": 0}
        intake_stats = None
        if user_id and _supabase_url and _supabase_service_key:
            sync_stats = _sync_scout_to_supabase(
                user_id,
                workspace_id=workspace_id,
                workspace_plan=workspace_plan,
            ) or sync_stats
            intake_workspace_id = (
                str(sync_stats.get("resolved_workspace_id") or "").strip()
                or str(workspace_id or "").strip()
            )
            if not intake_workspace_id:
                persistence_debug["errors"].append(
                    {
                        "stage": "workspace_resolution",
                        "error": "No workspace_id resolved for CRM intake",
                        "requested_workspace_id": workspace_id,
                        "sync_resolved_workspace_id": sync_stats.get("resolved_workspace_id"),
                    }
                )
            scout_run_checkpoint_ok = _checkpoint_scout_run_supabase(
                user_id,
                workspace_id,
                _job_get(job_id),
                today=today,
                opportunities=opportunities,
                sync_stats=sync_stats,
            )
            persistence_debug["scout_run_saved"] = bool(scout_run_checkpoint_ok)
            persistence_debug["opportunities_created"] = int(sync_stats.get("opportunities_inserted") or 0)
            persistence_debug["opportunities_updated"] = int(sync_stats.get("opportunities_updated") or 0)
            persistence_debug["case_files_created"] = int(sync_stats.get("inserted") or 0)
            persistence_debug["case_files_updated"] = int(sync_stats.get("updated") or 0)
            persistence_debug["duplicates_skipped"] = int(sync_stats.get("duplicate_skipped") or 0)
            persistence_debug["workspace_id"] = intake_workspace_id or workspace_id
            _log_write_stage(
                "opportunities",
                "insert_succeeded",
                {
                    "workspace_id": intake_workspace_id or workspace_id,
                    "attempted": int(sync_stats.get("opportunities_attempted") or 0),
                    "inserted": int(sync_stats.get("opportunities_inserted") or 0),
                    "updated": int(sync_stats.get("opportunities_updated") or 0),
                    "failed": int(sync_stats.get("opportunities_failed") or 0),
                },
            )
            _log_write_stage(
                "case_files",
                "insert_succeeded",
                {
                    "workspace_id": intake_workspace_id or workspace_id,
                    "attempted": int(sync_stats.get("case_files_attempted") or 0),
                    "inserted": int(sync_stats.get("inserted") or 0),
                    "updated": int(sync_stats.get("updated") or 0),
                    "duplicates": int(sync_stats.get("duplicate_skipped") or 0),
                    "failed": int(sync_stats.get("case_files_failed") or 0),
                },
            )
            if sync_stats.get("opportunities_failed"):
                persistence_debug["errors"].append(
                    {
                        "stage": "opportunities",
                        "failed": int(sync_stats.get("opportunities_failed") or 0),
                        "samples": sync_stats.get("opportunity_errors") or [],
                    }
                )
            if sync_stats.get("case_files_failed"):
                persistence_debug["errors"].append(
                    {
                        "stage": "case_files",
                        "failed": int(sync_stats.get("case_files_failed") or 0),
                        "samples": sync_stats.get("case_file_errors") or [],
                    }
                )
            try:
                from supabase import create_client

                sb = create_client(_supabase_url, _supabase_service_key)
                intake_stats = _run_workspace_crm_intake(
                    sb,
                    {"id": intake_workspace_id},
                    user_id,
                    debug_mode=True,
                )
                persistence_debug["leads_created"] = int((intake_stats or {}).get("created") or 0)
                persistence_debug["intake"] = {
                    "workspace_id": intake_workspace_id,
                    "opportunities_found": int((intake_stats or {}).get("opportunities_found") or 0),
                    "opportunities_loaded": int((intake_stats or {}).get("opportunities_loaded") or 0),
                    "opportunities_evaluated": int((intake_stats or {}).get("opportunities_evaluated") or 0),
                    "evaluated": int((intake_stats or {}).get("evaluated") or 0),
                    "eligible_for_lead_creation": int((intake_stats or {}).get("eligible_for_lead_creation") or 0),
                    "eligible": int((intake_stats or {}).get("eligible") or 0),
                    "leads_created": int((intake_stats or {}).get("leads_created") or 0),
                    "created": int((intake_stats or {}).get("created") or 0),
                    "duplicates_skipped": int((intake_stats or {}).get("duplicates_skipped") or 0),
                    "duplicate_skipped": int((intake_stats or {}).get("duplicate_skipped") or 0),
                    "insert_failed": int((intake_stats or {}).get("insert_failed") or 0),
                    "filtered_out": int((intake_stats or {}).get("filtered_out") or 0),
                    "filtered_low_score": int((intake_stats or {}).get("filtered_low_score") or 0),
                    "filtered_missing_contact_path": int((intake_stats or {}).get("filtered_missing_contact_path") or 0),
                    "filtered_missing_email": int((intake_stats or {}).get("filtered_missing_email") or 0),
                    "filtered_missing_opportunity_reason": int((intake_stats or {}).get("filtered_missing_opportunity_reason") or 0),
                    "filtered_closed_or_dnc": int((intake_stats or {}).get("filtered_closed_or_dnc") or 0),
                    "filtered_missing_business_name": int((intake_stats or {}).get("filtered_missing_business_name") or 0),
                    "filtered_missing_workspace": int((intake_stats or {}).get("filtered_missing_workspace") or 0),
                    "filtered_existing_linked_opportunity": int((intake_stats or {}).get("filtered_existing_linked_opportunity") or 0),
                    "duplicate_by_website": int((intake_stats or {}).get("duplicate_by_website") or 0),
                    "duplicate_by_phone": int((intake_stats or {}).get("duplicate_by_phone") or 0),
                    "duplicate_by_business_name_city": int((intake_stats or {}).get("duplicate_by_business_name_city") or 0),
                    "reason_counts": (intake_stats or {}).get("reason_counts") or {},
                    "leads_with_email": int((intake_stats or {}).get("leads_with_email") or 0),
                    "leads_with_phone": int((intake_stats or {}).get("leads_with_phone") or 0),
                    "leads_with_contact_page": int((intake_stats or {}).get("leads_with_contact_page") or 0),
                    "leads_with_facebook": int((intake_stats or {}).get("leads_with_facebook") or 0),
                    "leads_with_no_contact_path": int((intake_stats or {}).get("leads_with_no_contact_path") or 0),
                    "actionable_email_leads_created": int((intake_stats or {}).get("actionable_email_leads_created") or 0),
                    "leads_skipped_due_no_email": int((intake_stats or {}).get("leads_skipped_due_no_email") or 0),
                    "leads_created_with_low_score": int((intake_stats or {}).get("leads_created_with_low_score") or 0),
                    "leads_created_high_score": int((intake_stats or {}).get("leads_created_high_score") or 0),
                    "query_error": (intake_stats or {}).get("query_error"),
                    "intake_threshold_used": (intake_stats or {}).get("intake_threshold_used"),
                }
                _log_write_stage(
                    "leads_intake",
                    "insert_succeeded",
                    {
                        "workspace_id": intake_workspace_id or workspace_id,
                        "created": int((intake_stats or {}).get("created") or 0),
                        "eligible": int((intake_stats or {}).get("eligible") or 0),
                        "duplicates": int((intake_stats or {}).get("duplicate_skipped") or 0),
                        "failed": int((intake_stats or {}).get("insert_failed") or 0),
                    },
                )
                _process_workspace_outreach_sequences(workspace_id or "", user_id)
            except Exception as intake_error:
                print(f"  [Scout] crm intake after run failed: {intake_error}", file=sys.stderr)
                persistence_debug["errors"].append(
                    {"stage": "leads_intake", "error": _safe_error_payload(intake_error)}
                )
            current_job = _job_get(job_id) or {}
            _job_update(
                job_id,
                payload={**(current_job.get("payload") or {}), "persistence_debug": persistence_debug},
            )
        else:
            persistence_debug["errors"].append(
                {
                    "stage": "persistence",
                    "error": "persistence skipped because user_id or Supabase backend env is missing",
                    "user_id_present": bool(user_id),
                    "supabase_url_present": bool(_supabase_url),
                    "supabase_service_key_present": bool(_supabase_service_key),
                }
            )

        processed = int((today or {}).get("processed_count") or len(opportunities))
        saved = int((today or {}).get("saved_count") or len(opportunities))
        skipped = int((today or {}).get("skipped_count") or max(0, processed - saved))
        scout_summary = {
            "cities_scanned": int((today or {}).get("cities_scanned") or 0),
            "industries_scanned": int((today or {}).get("industries_scanned") or 0),
            "businesses_found": int((today or {}).get("businesses_found") or len(opportunities)),
            "high_score_opportunities": int((today or {}).get("high_score_opportunities") or 0),
            "leads_created": int((intake_stats or {}).get("created") or 0),
        }
        summary = (
            f"Scout complete — {len(opportunities)} leads discovered. "
            f"Processed {processed}, saved {saved}, skipped {skipped}. "
            f"Cities {scout_summary['cities_scanned']}, industries {scout_summary['industries_scanned']}, "
            f"high score opportunities {scout_summary['high_score_opportunities']}, "
            f"CRM leads created {scout_summary['leads_created']}. "
            f"Case files: inserted {int(sync_stats.get('inserted') or 0)}, "
            f"updated {int(sync_stats.get('updated') or 0)}, "
            f"skipped duplicates {int(sync_stats.get('duplicate_skipped') or 0)}."
        )
        finished = _job_update(
            job_id,
            status="completed",
            progress=100,
            message=summary,
            result_summary=summary,
            finished_at=_job_now_iso(),
            error=None,
            stage="finished",
        )
        if finished:
            print("  scout job finished")
            finished["payload"] = {
                **(finished.get("payload") or {}),
                "persistence_debug": persistence_debug,
                "scout_summary": scout_summary,
            }
            _upsert_job_supabase(finished)
            if user_id:
                _checkpoint_scout_run_supabase(
                    user_id,
                    workspace_id,
                    finished,
                    today=today,
                    opportunities=opportunities,
                    sync_stats=sync_stats,
                )
    except Exception as e:
        if ScoutRunError and isinstance(e, ScoutRunError) and e.error_type == "cancelled":
            cancelled = _job_update(
                job_id,
                status="cancelled",
                progress=100,
                message="Scout cancelled",
                result_summary="Scout cancelled",
                finished_at=_job_now_iso(),
                error=None,
                stage="cancelled",
            )
            if cancelled:
                print("  scout job cancelled")
                _upsert_job_supabase(cancelled)
                if user_id:
                    _checkpoint_scout_run_supabase(user_id, workspace_id, cancelled)
            return
        failed = _job_update(
            job_id,
            status="failed",
            progress=100,
            error=str(e),
            finished_at=_job_now_iso(),
            message="Scout job failed",
            result_summary="Scout job failed",
            stage="failed",
        )
        print("  scout job failed")
        if failed:
            persistence_debug["errors"].append({"stage": "scout_job", "error": _safe_error_payload(e)})
            failed["payload"] = {**(failed.get("payload") or {}), "persistence_debug": persistence_debug}
            _upsert_job_supabase(failed)
            if user_id:
                _checkpoint_scout_run_supabase(user_id, workspace_id, failed)


def _is_ignored_lead_status(status: str | None) -> bool:
    raw = (status or "").strip().lower()
    ignored = {
        "contacted",
        "follow_up_due",
        "replied",
        "closed",
        "closed_won",
        "closed_lost",
        "do not contact",
        "do_not_contact",
    }
    return raw in ignored


def _lead_rank(row: dict) -> float:
    lane = (row.get("lane") or "").strip().lower()
    score = float(row.get("opportunity_score") or row.get("internal_score") or 0)
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
            suppression_sets = {
                "linked_ids": set(),
                "place_ids": set(),
                "websites": set(),
                "phones": set(),
                "name_addr": set(),
            }
            crm_sb = _create_crm_client()
            if crm_sb is not None and workspace_id:
                suppression_sets = _crm_suppression_sets_for_workspace(crm_sb, workspace_id)

            filtered = [
                r
                for r in rows
                if not _is_ignored_lead_status(r.get("status"))
                and str(r.get("id") or "") not in suppression_sets["linked_ids"]
                and _normalize_text(r.get("place_id")) not in suppression_sets["place_ids"]
                and _normalize_website(r.get("website")) not in suppression_sets["websites"]
                and _normalize_phone(r.get("phone")) not in suppression_sets["phones"]
                and _build_name_address_key(r.get("business_name"), r.get("address")) not in suppression_sets["name_addr"]
            ]
            ranked = sorted(
                filtered,
                key=lambda r: float(r.get("opportunity_score") or r.get("internal_score") or 0),
                reverse=True,
            )[:5]
            leads = [
                {
                    "id": r.get("id"),
                    "business_name": r.get("business_name"),
                    "category": r.get("category"),
                    "website": r.get("website"),
                    "distance": r.get("distance_miles"),
                    "opportunity_score": r.get("opportunity_score") if r.get("opportunity_score") is not None else r.get("internal_score"),
                    "score": r.get("opportunity_score") if r.get("opportunity_score") is not None else r.get("internal_score"),
                    "lead_bucket": (
                        r.get("lead_bucket")
                        or ("Easy Win" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 90 else None)
                        or ("High Value" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 75 else None)
                        or ("Good Prospect" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 60 else None)
                        or ("Needs Review" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 40 else None)
                        or "Low Priority"
                    ),
                    "rating": r.get("rating"),
                    "review_count": r.get("review_count"),
                    "city": r.get("city") or r.get("address"),
                    "lane": "no_website" if r.get("no_website") else (r.get("lane") or "weak_website"),
                    "best_contact_method": (
                        r.get("best_contact_method")
                        or r.get("recommended_contact_method")
                        or r.get("backup_contact_method")
                    ),
                    "best_pitch_angle": r.get("best_pitch_angle"),
                    "recommended_next_action": r.get("recommended_next_action"),
                    "opportunity_signals": r.get("opportunity_signals") or [],
                    "opportunity_reason": r.get("opportunity_reason"),
                    "close_probability": r.get("close_probability"),
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
        ranked = sorted(
            filtered,
            key=lambda r: float(r.get("opportunity_score") or r.get("internal_score") or 0),
            reverse=True,
        )[:5]
        leads = [
            {
                "id": r.get("id"),
                "business_name": r.get("business_name"),
                "category": r.get("category"),
                "website": r.get("website"),
                "distance": r.get("distance_miles"),
                "opportunity_score": r.get("opportunity_score") if r.get("opportunity_score") is not None else r.get("internal_score"),
                "score": r.get("opportunity_score") if r.get("opportunity_score") is not None else r.get("internal_score"),
                "lead_bucket": (
                    r.get("lead_bucket")
                    or ("Easy Win" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 90 else None)
                    or ("High Value" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 75 else None)
                    or ("Good Prospect" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 60 else None)
                    or ("Needs Review" if float(r.get("opportunity_score") or r.get("internal_score") or 0) >= 40 else None)
                    or "Low Priority"
                ),
                "rating": r.get("rating"),
                "review_count": r.get("review_count"),
                "city": r.get("city") or r.get("address"),
                "lane": "no_website" if r.get("no_website") else (r.get("lane") or "weak_website"),
                "best_contact_method": (
                    r.get("best_contact_method")
                    or r.get("recommended_contact_method")
                    or r.get("backup_contact_method")
                ),
                "best_pitch_angle": r.get("best_pitch_angle"),
                "recommended_next_action": r.get("recommended_next_action"),
                "website_status": r.get("website_status"),
                "website_speed": r.get("website_speed"),
                "mobile_ready": r.get("mobile_ready"),
                "seo_score": r.get("seo_score"),
                "website_quality_score": r.get("website_quality_score"),
                "opportunity_signals": r.get("opportunity_signals") or [],
                "opportunity_reason": r.get("opportunity_reason"),
                "close_probability": r.get("close_probability"),
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
        city = lead.get("city") or "Unknown city"
        top_lines.append(
            f"- {lead.get('business_name') or 'Lead'} | {city} | {lane} | Score {lead.get('score') or 0} | "
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
            "User-Agent": "scout-brain/1.0",
        },
        method="POST",
    )
    try:
        print("  [Email] sending resend request with user-agent")
        with urllib_request.urlopen(req, timeout=20) as resp:
            code = getattr(resp, "status", 200)
            if 200 <= code < 300:
                print("  email alert sent")
                return True
    except urllib_error.HTTPError as e:
        status_code = int(getattr(e, "code", 0) or 0)
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="ignore")
        except Exception:
            raw = str(e)
        print(f"  [Email] provider response status: {status_code}", file=sys.stderr)
        print(f"  [Email] provider response body: {raw}", file=sys.stderr)
        print(f"  [Scout] email alert error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"  [Scout] email alert error: {e}", file=sys.stderr)
    return False


def _send_resend_email(to_email: str, subject: str, body: str):
    global _last_email_provider_diag
    sender_cfg = _email_sender_config()
    has_resend_key = bool(sender_cfg.get("has_resend_api_key"))
    if not has_resend_key:
        print("  [Email] RESEND_API_KEY exists: False", file=sys.stderr)
        raise RuntimeError("RESEND_API_KEY is not configured")
    from_email = str(sender_cfg.get("from_email") or "").strip()
    from_name = str(sender_cfg.get("from_name") or "Scout-Brain").strip()
    sender_source = str(sender_cfg.get("sender_source") or "missing")
    print("  [Email] RESEND_API_KEY exists: True")
    print(f"  [Email] OUTREACH_FROM_EMAIL in use: {from_email or '(missing)'}")
    print(f"  [Email] OUTREACH_FROM_NAME in use: {from_name}")
    print(f"  [Email] sender source in use: {sender_source}")
    if not from_email:
        raise RuntimeError("OUTREACH_FROM_EMAIL is not configured")
    html_body = "<br/>".join((body or "").splitlines())
    payload = {
        "from": f"{from_name} <{from_email}>",
        "to": [to_email],
        "subject": subject,
        "html": f"<div>{html_body}</div>",
        "text": body,
    }
    req = urllib_request.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {_resend_api_key}",
            "Content-Type": "application/json",
            "User-Agent": "scout-brain/1.0",
        },
        method="POST",
    )
    try:
        print("  [Email] sending resend request with user-agent")
        print("  [Email] resend request started")
        with urllib_request.urlopen(req, timeout=35) as resp:
            status_code = getattr(resp, "status", 200)
            raw = resp.read().decode("utf-8", errors="ignore")
            parsed = {}
            try:
                parsed = json.loads(raw) if raw else {}
            except Exception:
                parsed = {}
            print(f"  [Email] provider response status: {status_code}")
            if not (200 <= status_code < 300):
                provider_message = parsed.get("message") or parsed.get("error") or raw or "Unknown provider error"
                print(f"  [Email] provider error message: {provider_message}", file=sys.stderr)
                print(f"  [Email] provider response body: {raw}", file=sys.stderr)
                _last_email_provider_diag = {
                    "status_code": int(status_code),
                    "provider_message": str(provider_message),
                    "provider_body": raw,
                    "from_email": from_email,
                    "from_name": from_name,
                    "sender_source": sender_source,
                    "has_resend_api_key": True,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                if int(status_code) == 403:
                    raise RuntimeError(
                        f"email_provider_forbidden: {provider_message}"
                    )
                raise RuntimeError(f"email_send_failed_http_{status_code}: {provider_message}")
            print("  [Email] resend request completed")
            provider_thread_id = (
                parsed.get("thread_id")
                or parsed.get("threadId")
                or parsed.get("conversation_id")
                or parsed.get("conversationId")
            )
            _last_email_provider_diag = {
                "status_code": int(status_code),
                "provider_message": "ok",
                "provider_body": raw,
                "provider_message_id": parsed.get("id"),
                "from_email": from_email,
                "from_name": from_name,
                "sender_source": sender_source,
                "has_resend_api_key": True,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            return {
                "provider_message_id": parsed.get("id"),
                "provider_thread_id": provider_thread_id,
                "provider_raw": parsed,
            }
    except urllib_error.HTTPError as e:
        status_code = int(getattr(e, "code", 0) or 0)
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="ignore")
        except Exception:
            raw = str(e)
        parsed = {}
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception:
            parsed = {}
        provider_message = parsed.get("message") or parsed.get("error") or raw or str(e)
        print(f"  [Email] provider response status: {status_code}", file=sys.stderr)
        print(f"  [Email] provider error message: {provider_message}", file=sys.stderr)
        print(f"  [Email] provider response body: {raw}", file=sys.stderr)
        _last_email_provider_diag = {
            "status_code": int(status_code),
            "provider_message": str(provider_message),
            "provider_body": raw,
            "from_email": from_email,
            "from_name": from_name,
            "sender_source": sender_source,
            "has_resend_api_key": True,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if status_code == 403:
            raise RuntimeError(
                f"email_provider_forbidden: {provider_message}"
            )
        raise RuntimeError(f"email_send_failed_http_{status_code}: {provider_message}")


def _insert_email_event(crm_sb, row: dict):
    try:
        crm_sb.table("email_events").insert(row).execute()
    except Exception as e:
        # Keep sends resilient if table is not migrated yet.
        print(f"  [Scout] email_events insert skipped: {e}", file=sys.stderr)


def _provider_reason_from_error(err: Exception | str) -> str:
    msg = str(err or "").strip()
    prefix = "email_provider_forbidden:"
    if msg.lower().startswith(prefix):
        return msg[len(prefix):].strip() or "Provider rejected the send."
    return msg or "Provider rejected the send."


def _normalize_message_id(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("<") and raw.endswith(">") and len(raw) > 2:
        raw = raw[1:-1].strip()
    return raw


def _normalize_email_subject(subject: str | None) -> str:
    normalized = str(subject or "").strip().lower()
    while normalized.startswith("re:") or normalized.startswith("fw:") or normalized.startswith("fwd:"):
        if normalized.startswith("re:"):
            normalized = normalized[3:].strip()
        elif normalized.startswith("fw:"):
            normalized = normalized[3:].strip()
        elif normalized.startswith("fwd:"):
            normalized = normalized[4:].strip()
    return " ".join(normalized.split())


def _upsert_email_thread(
    crm_sb,
    *,
    workspace_id: str | None,
    lead_id: str,
    contact_email: str,
    subject: str | None,
    provider_thread_id: str | None,
    owner_id: str,
):
    contact = str(contact_email or "").strip().lower()
    normalized_subject = _normalize_email_subject(subject)
    thread = None

    if provider_thread_id:
        try:
            q = (
                crm_sb.table("email_threads")
                .select("*")
                .eq("provider_thread_id", provider_thread_id)
                .limit(1)
            )
            if workspace_id:
                q = q.eq("workspace_id", workspace_id)
            res = q.execute()
            thread = (res.data or [None])[0]
        except Exception:
            thread = None

    if not thread:
        try:
            q = (
                crm_sb.table("email_threads")
                .select("*")
                .eq("lead_id", lead_id)
                .eq("contact_email", contact)
                .eq("owner_id", owner_id)
                .order("last_message_at", desc=True)
                .limit(10)
            )
            if workspace_id:
                q = q.eq("workspace_id", workspace_id)
            rows = q.execute().data or []
            for candidate in rows:
                subj = _normalize_email_subject(candidate.get("subject"))
                if not normalized_subject or subj == normalized_subject:
                    thread = candidate
                    break
            if not thread and rows:
                thread = rows[0]
        except Exception:
            thread = None

    now_iso = datetime.now(timezone.utc).isoformat()
    if thread:
        updates = {
            "last_message_at": now_iso,
            "status": "open",
        }
        if provider_thread_id and not thread.get("provider_thread_id"):
            updates["provider_thread_id"] = provider_thread_id
        if subject and not thread.get("subject"):
            updates["subject"] = subject
        try:
            crm_sb.table("email_threads").update(updates).eq("id", thread.get("id")).execute()
        except Exception:
            pass
        thread.update(updates)
        return thread

    row = {
        "workspace_id": workspace_id or None,
        "lead_id": lead_id,
        "contact_email": contact,
        "subject": subject,
        "provider_thread_id": provider_thread_id,
        "status": "open",
        "last_message_at": now_iso,
        "owner_id": owner_id,
    }
    try:
        res = crm_sb.table("email_threads").insert(row).execute()
        data = res.data or []
        return data[0] if data else None
    except Exception as e:
        print(f"  [Scout] email thread upsert skipped: {e}", file=sys.stderr)
        return None


def _insert_email_message(crm_sb, row: dict):
    payload = dict(row or {})
    try:
        res = crm_sb.table("email_messages").insert(payload).execute()
        data = res.data or []
        return data[0] if data else None
    except Exception as e:
        msg = str(e).lower()
        if "column" in msg and ("status" in msg or "generated_by" in msg):
            payload.pop("status", None)
            payload.pop("generated_by", None)
            try:
                res = crm_sb.table("email_messages").insert(payload).execute()
                data = res.data or []
                return data[0] if data else None
            except Exception as retry_e:
                print(f"  [Scout] email_messages insert skipped: {retry_e}", file=sys.stderr)
                return None
        print(f"  [Scout] email_messages insert skipped: {e}", file=sys.stderr)
    return None


def _resolve_thread_from_references(
    crm_sb,
    references: list[str],
    workspace_id: str | None = None,
):
    refs_raw = [str(r or "").strip() for r in references if str(r or "").strip()]
    refs_normalized = [_normalize_message_id(r) for r in refs_raw]
    refs = list({r for r in (refs_raw + refs_normalized) if r})
    if not refs:
        return None
    try:
        msg_q = (
            crm_sb.table("email_messages")
            .select("thread_id,provider_message_id,created_at")
            .in_("provider_message_id", refs)
            .order("created_at", desc=True)
            .limit(1)
        )
        msg_rows = msg_q.execute().data or []
        if not msg_rows:
            return None
        thread_id = msg_rows[0].get("thread_id")
        if not thread_id:
            return None
        thread_q = crm_sb.table("email_threads").select("*").eq("id", thread_id).limit(1)
        if workspace_id:
            thread_q = thread_q.eq("workspace_id", workspace_id)
        thread_rows = thread_q.execute().data or []
        return thread_rows[0] if thread_rows else None
    except Exception:
        return None


def _match_inbound_thread(
    crm_sb,
    *,
    workspace_id: str | None,
    from_email: str,
    subject: str | None,
    provider_thread_id: str | None,
    references: list[str],
):
    contact = str(from_email or "").strip().lower()
    normalized_subject = _normalize_email_subject(subject)

    if provider_thread_id:
        try:
            q = (
                crm_sb.table("email_threads")
                .select("*")
                .eq("provider_thread_id", provider_thread_id)
                .limit(1)
            )
            if workspace_id:
                q = q.eq("workspace_id", workspace_id)
            rows = q.execute().data or []
            if rows:
                return rows[0]
        except Exception:
            pass

    from_refs = _resolve_thread_from_references(crm_sb, references, workspace_id=workspace_id)
    if from_refs:
        return from_refs

    try:
        q = (
            crm_sb.table("email_threads")
            .select("*")
            .eq("contact_email", contact)
            .order("last_message_at", desc=True)
            .limit(20)
        )
        if workspace_id:
            q = q.eq("workspace_id", workspace_id)
        rows = q.execute().data or []
        for row in rows:
            if _normalize_email_subject(row.get("subject")) == normalized_subject:
                return row
        if rows:
            return rows[0]
    except Exception:
        pass
    return None


def _mark_lead_replied_after_inbound(crm_sb, lead_id: str):
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        lead_rows = (
            crm_sb.table(CRM_LEADS_TABLE)
            .select("id,status")
            .eq("id", lead_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        if not lead_rows:
            return
        current_status = str(lead_rows[0].get("status") or "").strip().lower()
        allowed = {"new", "contacted", "follow_up_due", "replied"}
        if current_status not in allowed:
            return
        updates = {
            "last_contacted_at": now_iso,
            "next_follow_up_at": None,
            "sequence_active": False,
        }
        if current_status in {"new", "contacted", "follow_up_due"}:
            updates["status"] = "replied"
        crm_sb.table(CRM_LEADS_TABLE).update(updates).eq("id", lead_id).execute()
        print("  lead marked replied")
    except Exception as e:
        print(f"  [Scout] lead reply update skipped: {e}", file=sys.stderr)


def _crm_fetch_lead(crm_sb, lead_id: str, user_id: str | None):
    q = crm_sb.table(CRM_LEADS_TABLE).select("*").eq("id", lead_id).limit(1)
    if user_id:
        q = q.eq("owner_id", user_id)
    res = q.execute()
    rows = res.data or []
    return rows[0] if rows else None


def _crm_fetch_lead_by_linked_opportunity(
    crm_sb,
    *,
    owner_id: str,
    workspace_id: str | None,
    linked_opportunity_id: str,
):
    if not linked_opportunity_id:
        return None
    try:
        q = (
            crm_sb.table(CRM_LEADS_TABLE)
            .select("*")
            .eq("owner_id", owner_id)
            .eq("linked_opportunity_id", linked_opportunity_id)
            .order("created_at", desc=True)
            .limit(1)
        )
        if workspace_id:
            q = q.eq("workspace_id", workspace_id)
        rows = q.execute().data or []
        return rows[0] if rows else None
    except Exception:
        return None


def _apply_lead_updates_safe(crm_sb, lead_id: str, updates: dict) -> None:
    if not str(lead_id or "").strip():
        return
    payload = dict(updates or {})
    try:
        crm_sb.table(CRM_LEADS_TABLE).update(payload).eq("id", lead_id).execute()
    except Exception as e:
        msg = str(e).lower()
        if "column" in msg and (
            "sequence_step" in msg
            or "sequence_active" in msg
        ):
            payload.pop("sequence_step", None)
            payload.pop("sequence_active", None)
            crm_sb.table(CRM_LEADS_TABLE).update(payload).eq("id", lead_id).execute()
            return
        raise


def _mark_lead_contacted_after_email(
    crm_sb,
    lead: dict,
    message_type: str,
    *,
    sequence_step_sent: int | None = None,
):
    now = datetime.now(timezone.utc)
    is_follow_up = str(message_type or "").strip().lower() in {"follow_up", "follow-up", "followup"}
    current_status = str(lead.get("status") or "").strip().lower()
    updates = {"last_contacted_at": now.isoformat()}
    if sequence_step_sent is not None:
        if int(sequence_step_sent) <= 1:
            updates["sequence_step"] = 2
            updates["sequence_active"] = True
            updates["next_follow_up_at"] = (now + timedelta(days=SEQUENCE_STEP_2_DELAY_DAYS)).isoformat()
        elif int(sequence_step_sent) == 2:
            updates["sequence_step"] = 3
            updates["sequence_active"] = True
            updates["next_follow_up_at"] = (now + timedelta(days=SEQUENCE_STEP_3_DELAY_DAYS)).isoformat()
        else:
            updates["sequence_step"] = 4
            updates["sequence_active"] = False
            updates["next_follow_up_at"] = None
    else:
        next_days = 7 if is_follow_up else 4
        next_follow_up = now + timedelta(days=next_days)
        updates["next_follow_up_at"] = next_follow_up.isoformat()
    if current_status == "new":
        updates["status"] = "contacted"
        print("  lead marked contacted")
    elif current_status == "follow_up_due":
        updates["status"] = "contacted"
    if is_follow_up:
        updates["follow_up_count"] = int(lead.get("follow_up_count") or 0) + 1
    _apply_lead_updates_safe(crm_sb, str(lead.get("id") or ""), updates)
    return updates


def _sequence_step_from_lead(lead: dict) -> int:
    try:
        step = int(lead.get("sequence_step") or 1)
        return max(1, step)
    except Exception:
        return 1


def _sequence_stop_status(status: str | None) -> bool:
    s = str(status or "").strip().lower()
    return s in {"replied", "closed_won", "do_not_contact"}


def _sequence_template_for_step(lead: dict, step: int) -> tuple[str, str, str]:
    business_name = str(lead.get("business_name") or "your business").strip() or "your business"
    linked_opp_id = str(lead.get("linked_opportunity_id") or "").strip()
    workspace_id = str(lead.get("workspace_id") or "").strip() or None
    template = _load_outreach_template_for_opportunity(linked_opp_id, workspace_id=workspace_id) if linked_opp_id else {}

    short_email = str(template.get("short_email") or "").strip()
    longer_email = str(template.get("longer_email") or "").strip()
    follow_up_1 = str(template.get("follow_up_1") or template.get("follow_up_note") or "").strip()
    follow_up_2 = str(template.get("follow_up_2") or "").strip()

    if step <= 1:
        subject = "quick question about your website"
        body = short_email or longer_email
        message_type = "short_email"
    elif step == 2:
        subject = f"Quick follow-up for {business_name}"
        body = follow_up_1 or short_email or longer_email
        message_type = "follow_up"
    else:
        subject = f"Final follow-up for {business_name}"
        body = follow_up_2 or follow_up_1 or longer_email or short_email
        message_type = "follow_up"

    if not body:
        fallback = generate_outreach_email(
            {
                "business_name": business_name,
                "category": lead.get("industry"),
                "website": lead.get("website"),
                "address": lead.get("address"),
            }
        )
        body = str(fallback.get("body") or "").strip()
        if not subject:
            subject = str(fallback.get("subject") or "").strip()

    return subject, body, message_type


def _send_outreach_email_for_lead(
    crm_sb,
    *,
    lead: dict,
    owner_id: str,
    workspace_id: str | None,
    recipient: str,
    subject: str,
    content: str,
    message_type: str,
    case_id: str | None = None,
    sequence_step_sent: int | None = None,
) -> dict:
    lead_id = str(lead.get("id") or "").strip()
    if not lead_id:
        return {"ok": False, "error": "lead id is missing"}
    if not recipient:
        return {"ok": False, "error": "recipient email is missing"}
    if not subject:
        return {"ok": False, "error": "email subject is required"}
    if not content:
        return {"ok": False, "error": "message body is empty"}

    lead_status = str(lead.get("status") or "").strip().lower()
    if lead_status == "do_not_contact":
        return {"ok": False, "error": "lead is marked do_not_contact"}
    if lead_status == "closed_lost":
        return {"ok": False, "error": "lead is marked closed_lost"}

    provider_message_id = None
    provider_thread_id = None
    thread = _upsert_email_thread(
        crm_sb,
        workspace_id=workspace_id or None,
        lead_id=lead_id,
        contact_email=recipient,
        subject=subject,
        provider_thread_id=None,
        owner_id=owner_id,
    )
    thread_id = thread.get("id") if thread else None
    try:
        send_result = _send_resend_email(recipient, subject, content)
        provider_message_id = _normalize_message_id(send_result.get("provider_message_id"))
        provider_thread_id = send_result.get("provider_thread_id")
        if provider_thread_id and thread_id:
            try:
                crm_sb.table("email_threads").update(
                    {"provider_thread_id": provider_thread_id}
                ).eq("id", thread_id).execute()
            except Exception:
                pass
        updates = _mark_lead_contacted_after_email(
            crm_sb,
            lead,
            message_type,
            sequence_step_sent=sequence_step_sent,
        )
        _insert_email_message(
            crm_sb,
            {
                "thread_id": thread_id,
                "lead_id": lead_id,
                "direction": "outbound",
                "provider_message_id": provider_message_id,
                "subject": subject,
                "body": content,
                "delivery_status": "sent",
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "received_at": None,
                "owner_id": owner_id,
            },
        )
        _insert_email_event(
            crm_sb,
            {
                "workspace_id": workspace_id or None,
                "lead_id": lead_id,
                "case_id": case_id,
                "recipient_email": recipient,
                "subject": subject,
                "body": content,
                "message_type": message_type,
                "send_status": "sent",
                "provider_message_id": provider_message_id,
                "sent_at": datetime.now(timezone.utc).isoformat(),
                "owner_id": owner_id,
            },
        )
        return {
            "ok": True,
            "provider_message_id": provider_message_id,
            "email_thread_id": thread_id,
            "lead_id": lead_id,
            "lead_updates": updates,
        }
    except Exception as e:
        _insert_email_message(
            crm_sb,
            {
                "thread_id": thread_id,
                "lead_id": lead_id,
                "direction": "outbound",
                "provider_message_id": provider_message_id,
                "subject": subject,
                "body": content,
                "delivery_status": "failed",
                "sent_at": None,
                "received_at": None,
                "owner_id": owner_id,
            },
        )
        _insert_email_event(
            crm_sb,
            {
                "workspace_id": workspace_id or None,
                "lead_id": lead_id,
                "case_id": case_id,
                "recipient_email": recipient,
                "subject": subject,
                "body": content,
                "message_type": message_type,
                "send_status": "failed",
                "provider_message_id": provider_message_id,
                "sent_at": None,
                "owner_id": owner_id,
            },
        )
        return {"ok": False, "error": str(e)}


def _load_outreach_template_for_opportunity(
    linked_opportunity_id: str,
    workspace_id: str | None = None,
    regenerate: bool = False,
) -> dict:
    if not linked_opportunity_id:
        return {}
    if not _supabase_url or not _supabase_service_key:
        return {}
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)

        case_row = None
        case_queries = [
            lambda: (
                sb.table("case_files")
                .select(
                    "opportunity_id,short_email,longer_email,contact_form_version,social_dm_version,follow_up_note,"
                    "owner_name,owner_manager_name,website_score,audit_issues,strongest_problems,best_service_to_offer,"
                    "best_demo_to_show,demo_to_show,demo_url,website_status,rating,review_count,website_audit,"
                    "desktop_screenshot_url,mobile_screenshot_url,contact_page_screenshot_url,internal_screenshot_url,screenshot_url"
                )
                .eq("opportunity_id", linked_opportunity_id)
                .eq("workspace_id", workspace_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("case_files")
                .select(
                    "opportunity_id,short_email,longer_email,contact_form_version,social_dm_version,follow_up_note,"
                    "owner_name,owner_manager_name,website_score,audit_issues,strongest_problems,best_service_to_offer,"
                    "best_demo_to_show,demo_to_show,demo_url,website_status,rating,review_count,website_audit,"
                    "desktop_screenshot_url,mobile_screenshot_url,contact_page_screenshot_url,internal_screenshot_url,screenshot_url"
                )
                .eq("opportunity_id", linked_opportunity_id)
                .limit(1)
                .execute()
            ),
        ]
        for q in case_queries:
            try:
                res = q()
                rows = res.data or []
                if rows:
                    case_row = rows[0]
                    break
            except Exception:
                continue

        opp_row = None
        opp_queries = [
            lambda: (
                sb.table("opportunities")
                .select(
                    "id,business_name,category,lane,opportunity_score,strongest_pitch_angle,best_service_to_offer,"
                    "demo_to_show,website_score,rating,review_count,address,website,lead_bucket,close_probability"
                )
                .eq("id", linked_opportunity_id)
                .eq("workspace_id", workspace_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("opportunities")
                .select(
                    "id,business_name,category,lane,opportunity_score,strongest_pitch_angle,best_service_to_offer,"
                    "demo_to_show,website_score,rating,review_count,address,website,lead_bucket,close_probability"
                )
                .eq("id", linked_opportunity_id)
                .limit(1)
                .execute()
            ),
        ]
        for q in opp_queries:
            try:
                res = q()
                rows = res.data or []
                if rows:
                    opp_row = rows[0]
                    break
            except Exception:
                continue

        owner_name = ""
        if case_row:
            owner_name = str(
                case_row.get("owner_name") or case_row.get("owner_manager_name") or ""
            ).strip()

        template_payload = {
            "linked_opportunity_id": linked_opportunity_id,
            "short_email": (case_row or {}).get("short_email"),
            "longer_email": (case_row or {}).get("longer_email"),
            "contact_form_version": (case_row or {}).get("contact_form_version"),
            "social_dm_version": (case_row or {}).get("social_dm_version"),
            "follow_up_note": (case_row or {}).get("follow_up_note"),
            "follow_up_1": (case_row or {}).get("follow_up_note"),
            "follow_up_2": None,
            "why_this_lead": None,
            "main_issue_observed": None,
            "best_opening_angle": None,
            "best_offer_to_make": None,
            "demo_url": (case_row or {}).get("demo_url") or (opp_row or {}).get("demo_to_show"),
            "metadata": {
                "business_name": (opp_row or {}).get("business_name"),
                "owner_name": owner_name or None,
                "category": (opp_row or {}).get("category"),
                "lane": (opp_row or {}).get("lane"),
                "score": (opp_row or {}).get("opportunity_score"),
                "close_probability": (opp_row or {}).get("close_probability"),
                "website_score": (case_row or {}).get("website_score") or (opp_row or {}).get("website_score"),
                "audit_issues": (case_row or {}).get("audit_issues"),
                "review_rating": (case_row or {}).get("rating") or (opp_row or {}).get("rating"),
                "review_count": (case_row or {}).get("review_count") or (opp_row or {}).get("review_count"),
                "strongest_pitch_angle": (opp_row or {}).get("strongest_pitch_angle"),
                "best_service_to_offer": (case_row or {}).get("best_service_to_offer")
                or (opp_row or {}).get("best_service_to_offer"),
                "demo_url": (case_row or {}).get("demo_url") or (opp_row or {}).get("demo_to_show"),
            },
        }
        should_generate = regenerate or not bool(
            str(template_payload.get("short_email") or "").strip()
            and str(template_payload.get("longer_email") or "").strip()
            and str(template_payload.get("follow_up_note") or "").strip()
        )
        if should_generate:
            try:
                from scout.outreach_generator import generate_outreach_pack

                case_for_generation = {}
                if opp_row:
                    case_for_generation.update(opp_row)
                if case_row:
                    case_for_generation.update(case_row)
                pack = generate_outreach_pack(case_for_generation, city_hint=(opp_row or {}).get("address"))
                template_payload["short_email"] = pack.get("short_email")
                template_payload["longer_email"] = pack.get("longer_email")
                template_payload["contact_form_version"] = pack.get("contact_form_version")
                template_payload["social_dm_version"] = pack.get("social_dm_version")
                template_payload["follow_up_note"] = pack.get("follow_up_note")
                template_payload["follow_up_1"] = pack.get("follow_up_1") or pack.get("follow_up_note")
                template_payload["follow_up_2"] = pack.get("follow_up_2")
                template_payload["why_this_lead"] = pack.get("why_this_lead")
                template_payload["main_issue_observed"] = pack.get("main_issue_observed")
                template_payload["best_opening_angle"] = pack.get("best_opening_angle")
                template_payload["best_offer_to_make"] = pack.get("best_offer_to_make")
                template_payload["demo_url"] = pack.get("demo_url") or template_payload.get("demo_url")
            except Exception as e:
                print(f"  [Scout] outreach regeneration failed: {e}", file=sys.stderr)
        return template_payload
    except Exception as e:
        print(f"  [Scout] outreach template load failed: {e}", file=sys.stderr)
        return {}


def generate_outreach_email(lead: dict) -> dict:
    opportunity = lead.get("opportunity") if isinstance(lead.get("opportunity"), dict) else lead
    case_file = lead.get("case_file") if isinstance(lead.get("case_file"), dict) else lead
    business_name = str(opportunity.get("business_name") or "there").strip() or "there"
    contact_name = str(lead.get("contact_name") or "").strip()
    greeting_name = contact_name or business_name
    category = str(opportunity.get("category") or opportunity.get("industry") or "local business").strip() or "local business"
    city = str(opportunity.get("city") or opportunity.get("address") or "your area").strip() or "your area"
    screenshot_url = (
        case_file.get("desktop_screenshot_url")
        or case_file.get("mobile_screenshot_url")
        or case_file.get("contact_page_screenshot_url")
        or case_file.get("internal_screenshot_url")
        or case_file.get("screenshot_url")
    )
    screenshot_url = str(screenshot_url).strip() if screenshot_url else None
    screenshot_count = int(
        sum(
            1
            for val in [
                case_file.get("desktop_screenshot_url"),
                case_file.get("mobile_screenshot_url"),
                case_file.get("contact_page_screenshot_url"),
            ]
            if str(val or "").strip()
        )
    )

    issue_candidates: list[str] = []

    def normalize_issue_label(issue: str) -> str:
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
        if "navigation" in lower or "menu" in lower:
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
    mobile_score = case_file.get("mobile_score")
    seo_score = case_file.get("seo_score")
    performance_score = case_file.get("performance_score")
    website_speed = case_file.get("website_speed") or case_file.get("homepage_load_seconds")
    missing_ssl = case_file.get("missing_ssl")
    ssl_ok = case_file.get("ssl_ok")
    slow_load = case_file.get("slow_load")
    mobile_layout_issue = case_file.get("mobile_layout_issue")
    outdated_design = bool(case_file.get("outdated_design_clues"))
    missing_meta_title = bool(case_file.get("missing_meta_title"))
    missing_meta_description = bool(case_file.get("missing_meta_description"))
    opportunity_reason = str(
        opportunity.get("opportunity_reason")
        or case_file.get("opportunity_reason")
        or ""
    ).strip()

    try:
        if mobile_score is not None and float(mobile_score) < 70:
            issue_candidates.append("The site layout shifts on mobile devices")
    except Exception:
        pass
    if mobile_layout_issue is True or case_file.get("mobile_ready") is False:
        issue_candidates.append("The site layout shifts on mobile devices")

    try:
        if performance_score is not None and float(performance_score) < 70:
            issue_candidates.append("Page load speed is slower than average")
    except Exception:
        pass
    try:
        if website_speed is not None and float(website_speed) > 3:
            issue_candidates.append("Page load speed is slower than average")
    except Exception:
        pass
    if slow_load is True:
        issue_candidates.append("Page load speed is slower than average")

    try:
        if seo_score is not None and float(seo_score) < 70:
            issue_candidates.append("The site may not be optimized for Google search visibility")
    except Exception:
        pass
    if missing_meta_title or missing_meta_description:
        issue_candidates.append("The site may not be optimized for Google search visibility")

    if missing_ssl is True or ssl_ok is False:
        issue_candidates.append("The website does not appear to be fully secured with HTTPS")

    if outdated_design:
        issue_candidates.append("The design feels outdated compared to nearby competitors")

    if case_file.get("contact_form_present") is False and not str(case_file.get("phone_from_site") or "").strip():
        issue_candidates.append("Customers may have trouble finding a clear contact path")

    audit_issues = case_file.get("audit_issues")
    if isinstance(audit_issues, list):
        for item in audit_issues:
            text = str(item or "").strip()
            if text:
                issue_candidates.append(text)
    audit_results = case_file.get("audit_results")
    if isinstance(audit_results, dict):
        for key in ["issues", "problems", "findings"]:
            values = audit_results.get(key)
            if isinstance(values, list):
                for item in values:
                    text = str(item or "").strip()
                    if text:
                        issue_candidates.append(text)

    issues: list[str] = []
    if opportunity_reason:
        normalized_reason = normalize_issue_label(opportunity_reason)
        if normalized_reason:
            issues.append(normalized_reason)
    for candidate in issue_candidates:
        normalized = normalize_issue_label(candidate)
        if not normalized:
            continue
        if normalized not in issues:
            issues.append(normalized)
        if len(issues) >= 3:
            break
    if not issues:
        issues = [
            "The mobile experience could be improved for local customers",
            "Page speed appears slower than ideal on first load",
            "The site likely has opportunities to improve local search visibility",
        ]

    lead_issue = issues[0] if issues else "something that might be affecting conversions"
    if screenshot_url and screenshot_count >= 2:
        screenshot_line = (
            f"I captured fresh website screenshots (desktop/mobile/contact where available):\n{screenshot_url}\n\n"
        )
    elif screenshot_url:
        screenshot_line = f"I grabbed a quick screenshot showing it:\n{screenshot_url}\n\n"
    else:
        screenshot_line = "I grabbed a quick screenshot showing it.\n\n"
    subject = "quick question about your website"
    body = (
        "Hi,\n\n"
        f"I was looking at your website and noticed: {lead_issue}.\n\n"
        f"{screenshot_line}"
        "Would you like me to send it over?\n\n"
        "– Topher"
    )
    return {
        "subject": subject,
        "body": body,
        "issues": issues[:3],
        "screenshot_url": screenshot_url,
    }


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
    plan = _normalize_plan(workspace.get("plan"))
    if not _plan_limits(plan).get("daily_email_briefing", True):
        print("  email alerts disabled")
        return
    if not _frequency_allows_send(settings):
        print("  email alerts disabled")
        return

    summary = _build_lead_briefing_summary(sb, workspace_id, workspace.get("name"))
    if int(len(summary.get("top_opportunities") or [])) < 1:
        print("  email alerts disabled")
        return
    if not settings.get("include_new_leads"):
        summary["new_leads"] = 0
    if not settings.get("include_followups"):
        summary["followups_due"] = 0
    if not settings.get("include_top_opportunities"):
        summary["top_opportunities"] = []
    sendLeadBriefingEmail(owner_user, summary)


def _normalize_text(value) -> str:
    return str(value or "").strip().lower()


def _normalize_website(value) -> str:
    site = _normalize_text(value)
    if site.startswith("https://"):
        site = site[8:]
    elif site.startswith("http://"):
        site = site[7:]
    return site.rstrip("/")


def _normalize_phone(value) -> str:
    raw = str(value or "")
    digits = [c for c in raw if c.isdigit()]
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    return "".join(digits)


def _is_closed_or_dnc(status_text: str) -> bool:
    s = _normalize_text(status_text)
    if not s:
        return False
    blocked_tokens = {
        "closed",
        "do_not_contact",
        "do not contact",
        "dnc",
        "unqualified",
        "lost",
    }
    return any(token in s for token in blocked_tokens)


def _lane_priority(lane: str) -> int:
    key = _normalize_text(lane)
    if key == "no_website":
        return 0
    if key == "weak_website":
        return 1
    return 2


def _build_name_address_key(business_name: str, address: str) -> str:
    return f"{_normalize_text(business_name)}|{_normalize_text(address)}"


def _build_name_city_key(business_name: str, city_or_address: str) -> str:
    return f"{_normalize_text(business_name)}|{_normalize_text(city_or_address)}"


def _crm_worked_statuses() -> set[str]:
    return {
        "contacted",
        "follow_up_due",
        "replied",
        "closed_won",
        "closed_lost",
        "do_not_contact",
    }


def _crm_suppression_sets_for_workspace(crm_sb, workspace_id: str) -> dict[str, set[str]]:
    if not workspace_id:
        return {
            "linked_ids": set(),
            "place_ids": set(),
            "websites": set(),
            "phones": set(),
            "name_addr": set(),
        }
    worked_statuses = _crm_worked_statuses()
    select_sets = [
        "linked_opportunity_id,place_id,website,phone,business_name,address,status,last_contacted_at,next_follow_up_at",
        "linked_opportunity_id,place_id,website,phone,business_name,address,status,last_contacted_at",
        "linked_opportunity_id,place_id,website,phone,business_name,address,status",
    ]
    for cols in select_sets:
        try:
            rows = (
                crm_sb.table(CRM_LEADS_TABLE)
                .select(cols)
                .eq("workspace_id", workspace_id)
                .limit(5000)
                .execute()
            )
            linked_ids: set[str] = set()
            place_ids: set[str] = set()
            websites: set[str] = set()
            phones: set[str] = set()
            name_addr: set[str] = set()
            for row in rows.data or []:
                status = _normalize_text(row.get("status"))
                has_contacted = bool(str(row.get("last_contacted_at") or "").strip())
                has_followup = bool(str(row.get("next_follow_up_at") or "").strip())
                worked = status in worked_statuses or has_contacted or has_followup
                if not worked:
                    continue
                linked_id = str(row.get("linked_opportunity_id") or "").strip()
                if linked_id:
                    linked_ids.add(linked_id)
                place_id = _normalize_text(row.get("place_id"))
                if place_id:
                    place_ids.add(place_id)
                website = _normalize_website(row.get("website"))
                if website:
                    websites.add(website)
                phone = _normalize_phone(row.get("phone"))
                if phone:
                    phones.add(phone)
                key = _build_name_address_key(row.get("business_name"), row.get("address"))
                if key != "|":
                    name_addr.add(key)
            return {
                "linked_ids": linked_ids,
                "place_ids": place_ids,
                "websites": websites,
                "phones": phones,
                "name_addr": name_addr,
            }
        except Exception:
            continue
    return {
        "linked_ids": set(),
        "place_ids": set(),
        "websites": set(),
        "phones": set(),
        "name_addr": set(),
    }


def _create_crm_client():
    if not CRM_SUPABASE_URL or not CRM_SUPABASE_SERVICE_ROLE_KEY:
        return None
    try:
        from supabase import create_client
        return create_client(CRM_SUPABASE_URL, CRM_SUPABASE_SERVICE_ROLE_KEY)
    except Exception:
        return None


def _load_existing_crm_leads_for_owner(crm_sb, owner_id: str) -> list[dict]:
    select_sets = [
        "id,business_name,city,address,phone,website,place_id,status,linked_opportunity_id",
        "id,business_name,phone,website,status,linked_opportunity_id",
    ]
    for cols in select_sets:
        try:
            rows = (
                crm_sb.table(CRM_LEADS_TABLE)
                .select(cols)
                .eq("owner_id", owner_id)
                .limit(5000)
                .execute()
            )
            return rows.data or []
        except Exception:
            continue
    return []


def _crm_linked_opportunity_ids_for_workspace(crm_sb, workspace_id: str) -> set[str]:
    return _crm_suppression_sets_for_workspace(crm_sb, workspace_id).get("linked_ids", set())


def _case_map_for_workspace(sb, workspace_id: str, opp_ids: list[str]) -> dict:
    if not opp_ids:
        return {}
    queries = [
        lambda: (
            sb.table("case_files")
            .select(
                "opportunity_id,email,email_source,contact_page,contact_form_url,phone_from_site,facebook,facebook_url,instagram,status,outcome"
            )
            .eq("workspace_id", workspace_id)
            .in_("opportunity_id", opp_ids)
            .execute()
        ),
        lambda: (
            sb.table("case_files")
            .select(
                "opportunity_id,email,email_source,contact_page,contact_form_url,phone_from_site,facebook,facebook_url,instagram,status,outcome"
            )
            .in_("opportunity_id", opp_ids)
            .execute()
        ),
        lambda: (
            sb.table("case_files")
            .select(
                "opportunity_id,email,contact_page,contact_form_url,phone_from_site,facebook,facebook_url,instagram,status,outcome"
            )
            .in_("opportunity_id", opp_ids)
            .execute()
        ),
    ]
    data = []
    for q in queries:
        try:
            res = q()
            data = res.data or []
            break
        except Exception:
            continue
    return {str(row.get("opportunity_id")): row for row in data if row.get("opportunity_id")}


def _opportunity_has_contact_path(opp: dict, case: dict | None) -> bool:
    case = case or {}
    checks = [
        opp.get("phone"),
        opp.get("website"),
        opp.get("recommended_contact_method"),
        opp.get("backup_contact_method"),
        case.get("email"),
        case.get("contact_page"),
        case.get("contact_form_url"),
        case.get("phone_from_site"),
        case.get("facebook"),
        case.get("facebook_url"),
        case.get("instagram"),
    ]
    return any(str(v or "").strip() for v in checks)


def _insert_crm_lead(crm_sb, row: dict) -> bool:
    try:
        res = crm_sb.table(CRM_LEADS_TABLE).insert(row).execute()
        data = res.data or []
        return {"ok": True, "mode": "full", "error": None, "data": (data[0] if data else None)}
    except Exception as e:
        # Schema fallback for older CRM deployments that have fewer intake columns.
        if "column" not in str(e).lower():
            return {"ok": False, "mode": "full", "error": str(e), "data": None}
        minimal = {
            "owner_id": row.get("owner_id"),
            "business_name": row.get("business_name"),
            "contact_name": row.get("contact_name"),
            "email": row.get("email"),
            "phone": row.get("phone"),
            "website": row.get("website"),
            "industry": row.get("industry"),
            "lead_source": row.get("lead_source"),
            "status": row.get("status"),
            "notes": row.get("notes"),
        }
        try:
            res = crm_sb.table(CRM_LEADS_TABLE).insert(minimal).execute()
            data = res.data or []
            return {"ok": True, "mode": "minimal", "error": None, "data": (data[0] if data else None)}
        except Exception as retry_e:
            return {"ok": False, "mode": "minimal", "error": str(retry_e), "data": None}


def _refresh_workspace_followups(crm_sb, workspace_id: str, owner_id: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        crm_sb.table(CRM_LEADS_TABLE).update({"status": "follow_up_due"}).eq(
            "owner_id", owner_id
        ).eq("workspace_id", workspace_id).in_(
            "status", ["contacted", "replied"]
        ).lte("next_follow_up_at", now_iso).execute()
    except Exception:
        try:
            crm_sb.table(CRM_LEADS_TABLE).update({"status": "follow_up_due"}).eq(
                "owner_id", owner_id
            ).in_("status", ["contacted", "replied"]).lte("next_follow_up_at", now_iso).execute()
        except Exception:
            pass


def _process_workspace_outreach_sequences(workspace_id: str, owner_id: str) -> dict:
    stats = {
        "checked": 0,
        "sent": 0,
        "failed": 0,
        "stopped": 0,
    }
    crm_sb = _create_crm_client()
    if crm_sb is None or not workspace_id or not owner_id:
        return stats
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        due_rows = (
            crm_sb.table(CRM_LEADS_TABLE)
            .select(
                "id,owner_id,workspace_id,business_name,email,status,linked_opportunity_id,"
                "industry,address,website,sequence_step,sequence_active,next_follow_up_at"
            )
            .eq("owner_id", owner_id)
            .eq("workspace_id", workspace_id)
            .eq("sequence_active", True)
            .lte("next_follow_up_at", now_iso)
            .order("next_follow_up_at", desc=False)
            .limit(200)
            .execute()
            .data
            or []
        )
    except Exception as e:
        print(f"  [Scout] outreach sequence scan skipped: {e}", file=sys.stderr)
        return stats

    for lead in due_rows:
        stats["checked"] += 1
        lead_id = str(lead.get("id") or "").strip()
        if not lead_id:
            continue
        status = str(lead.get("status") or "").strip().lower()
        if _sequence_stop_status(status):
            _apply_lead_updates_safe(
                crm_sb,
                lead_id,
                {"sequence_active": False, "next_follow_up_at": None},
            )
            stats["stopped"] += 1
            continue
        recipient = str(lead.get("email") or "").strip()
        if not recipient:
            _apply_lead_updates_safe(
                crm_sb,
                lead_id,
                {"sequence_active": False, "next_follow_up_at": None},
            )
            stats["stopped"] += 1
            print(f"  [Scout] sequence stopped (missing recipient) lead_id={lead_id}")
            continue

        step = _sequence_step_from_lead(lead)
        if step == 1 and not AUTO_SEQUENCE_SEND_STEP1:
            _apply_lead_updates_safe(
                crm_sb,
                lead_id,
                {"next_follow_up_at": None},
            )
            continue
        if step > 3:
            _apply_lead_updates_safe(
                crm_sb,
                lead_id,
                {"sequence_active": False, "next_follow_up_at": None},
            )
            stats["stopped"] += 1
            continue

        subject, body, message_type = _sequence_template_for_step(lead, step)
        result = _send_outreach_email_for_lead(
            crm_sb,
            lead=lead,
            owner_id=owner_id,
            workspace_id=workspace_id,
            recipient=recipient,
            subject=subject,
            content=body,
            message_type=message_type,
            sequence_step_sent=step,
        )
        if result.get("ok"):
            stats["sent"] += 1
            print(f"  [Scout] sequence email sent lead_id={lead_id} step={step}")
        else:
            stats["failed"] += 1
            retry_at = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
            _apply_lead_updates_safe(
                crm_sb,
                lead_id,
                {"next_follow_up_at": retry_at},
            )
            print(
                f"  [Scout] sequence email failed lead_id={lead_id} step={step} "
                f"error={result.get('error')}",
                file=sys.stderr,
            )
    return stats

def _compute_lead_conversion_score(
    *,
    email: str,
    contact_page: str,
    phone: str,
    facebook_url: str,
    website_status: str,
    review_count: int,
    category: str,
    business_name: str,
    website: str,
    opportunity_reason: str,
) -> tuple[int, dict]:
    score = 0
    breakdown: dict[str, int | str | list[str]] = {
        "positive": [],
        "negative": [],
        "raw_score": 0,
    }
    status = str(website_status or "").strip().lower()
    cat = str(category or "").strip().lower()
    name = str(business_name or "").strip().lower()
    reason = str(opportunity_reason or "").strip().lower()
    has_email = bool(str(email or "").strip())
    has_contact_page = bool(str(contact_page or "").strip())
    has_phone = bool(str(phone or "").strip())
    has_facebook = bool(str(facebook_url or "").strip())
    has_contact_path = has_email or has_contact_page or has_phone or has_facebook
    reviews = int(review_count or 0)
    website_text = str(website or "").strip().lower()
    clear_info = bool(name and cat and (website_text or reason))
    service_fit = any(
        token in cat
        for token in ["plumber", "roofer", "hvac", "electrician", "landscaping", "cleaning", "auto repair", "contractor"]
    )
    church_fit = "church" in cat
    shop_fit = any(token in cat for token in ["shop", "store", "retail", "cafe", "restaurant", "salon"])
    chain_or_franchise = any(token in name for token in ["franchise", "chain", "walmart", "target", "mcdonald", "starbucks"])

    if has_email:
        score += 40
        breakdown["positive"].append("email_exists:+40")
    if has_contact_page:
        score += 15
        breakdown["positive"].append("contact_page_exists:+15")
    if has_phone and not has_email and not has_contact_page:
        score += 5
        breakdown["positive"].append("phone_only:+5")

    if status == "no_website":
        score += 30
        breakdown["positive"].append("no_website:+30")
    elif status == "broken_website":
        score += 25
        breakdown["positive"].append("broken_website:+25")
    elif status == "outdated_website":
        score += 20
        breakdown["positive"].append("outdated_website:+20")
    elif status in {"missing_contact_page", "missing_contact_info"}:
        score += 15
        breakdown["positive"].append("missing_contact_info:+15")

    if reviews >= 10:
        score += 15
        breakdown["positive"].append("reviews_10_plus:+15")
    elif reviews >= 3:
        score += 10
        breakdown["positive"].append("reviews_3_10:+10")
    elif reviews >= 1:
        score += 5
        breakdown["positive"].append("reviews_1_2:+5")

    if service_fit:
        score += 15
        breakdown["positive"].append("service_business_fit:+15")
    elif church_fit:
        score += 10
        breakdown["positive"].append("church_fit:+10")
    elif shop_fit:
        score += 10
        breakdown["positive"].append("local_shop_fit:+10")

    if not has_contact_path:
        score -= 40
        breakdown["negative"].append("no_contact_path:-40")
    if reviews <= 0:
        score -= 20
        breakdown["negative"].append("no_reviews:-20")
    if chain_or_franchise:
        score -= 25
        breakdown["negative"].append("chain_or_franchise:-25")
    if status in {"healthy_website", "strong_website", "modern_website"}:
        score -= 20
        breakdown["negative"].append("strong_existing_website:-20")
    if not clear_info:
        score -= 15
        breakdown["negative"].append("unclear_business_info:-15")

    bounded = max(0, min(100, int(score)))
    breakdown["raw_score"] = int(score)
    breakdown["final_score"] = bounded
    return bounded, breakdown


def _run_workspace_crm_intake(sb, workspace: dict, owner_id: str, debug_mode: bool = False) -> dict:
    stats = {
        "scanned_count": 0,
        "enriched_count": 0,
        "opportunities_found": 0,
        "opportunities_evaluated": 0,
        "eligible_for_lead_creation": 0,
        "leads_created": 0,
        "duplicates_skipped": 0,
        "filtered_out": 0,
        "evaluated": 0,
        "eligible": 0,
        "created": 0,
        "duplicate_skipped": 0,
        "filtered_existing_linked_opportunity": 0,
        "duplicate_by_place_id": 0,
        "duplicate_by_website": 0,
        "duplicate_by_phone": 0,
        "duplicate_by_business_name_city": 0,
        "filtered_low_score": 0,
        "filtered_missing_workspace": 0,
        "filtered_closed_or_dnc": 0,
        "filtered_missing_contact_path": 0,
        "filtered_missing_email": 0,
        "filtered_missing_opportunity_reason": 0,
        "filtered_missing_business_name": 0,
        "filtered_other": 0,
        "insert_attempted": 0,
        "insert_succeeded": 0,
        "insert_failed": 0,
        "leads_with_email": 0,
        "leads_with_phone": 0,
        "leads_with_contact_page": 0,
        "leads_with_facebook": 0,
        "leads_with_no_contact_path": 0,
        "actionable_email_leads_created": 0,
        "research_later_leads_created": 0,
        "door_to_door_candidates_created": 0,
        "archived_low_priority_created": 0,
        "leads_skipped_due_no_email": 0,
        "leads_created_with_low_score": 0,
        "leads_created_high_score": 0,
        "sequence_started": 0,
        "sequence_send_failed": 0,
        "sequence_stopped": 0,
        "top_contacts": [],
        "insert_errors": 0,
        "insert_error_samples": [],
        "query_error": None,
        "workspace_id_used": None,
        "opportunities_loaded": 0,
        "owner_profile_exists": None,
        "debug_mode": bool(debug_mode),
        "intake_threshold_used": None,
        "contact_rule_used": "strict",
        "debug_decisions": [],
        "exclusion_samples": [],
        "leads_skipped_reason": {},
    }
    debug_decision_limit = 40
    exclusion_sample_limit = 25

    def _append_decision(
        opp_id: str,
        business_name: str,
        score: float,
        decision: str,
        reason: str,
    ) -> None:
        if len(stats["debug_decisions"]) >= debug_decision_limit:
            return
        stats["debug_decisions"].append(
            {
                "opportunity_id": opp_id or None,
                "business_name": business_name or None,
                "score": round(float(score), 2),
                "workspace_id": workspace_id or None,
                "decision": decision,
                "reason": reason,
            }
        )

    def _append_exclusion(
        business_name: str,
        score: float,
        exclusion_reason: str,
    ) -> None:
        if len(stats["exclusion_samples"]) >= exclusion_sample_limit:
            return
        stats["exclusion_samples"].append(
            {
                "business_name": business_name or None,
                "score": round(float(score), 2),
                "exclusion_reason": exclusion_reason,
            }
        )

    def _count_skip_reason(reason: str) -> None:
        key = str(reason or "other").strip() or "other"
        current = int((stats["leads_skipped_reason"] or {}).get(key) or 0)
        stats["leads_skipped_reason"][key] = current + 1

    workspace_id = str(workspace.get("id") or "").strip()
    stats["workspace_id_used"] = workspace_id or None
    if not CRM_AUTO_INTAKE_ENABLED or not workspace_id or not owner_id:
        if not workspace_id:
            stats["filtered_missing_workspace"] = 1
            print("  [Intake] filtered: missing workspace_id")
        return stats

    crm_sb = _create_crm_client()
    if crm_sb is None:
        print("  [Scout] crm intake skipped: CRM Supabase not configured")
        return stats

    try:
        owner_profile_rows = (
            crm_sb.table("profiles")
            .select("id")
            .eq("id", owner_id)
            .limit(1)
            .execute()
            .data
            or []
        )
        stats["owner_profile_exists"] = bool(owner_profile_rows)
    except Exception:
        stats["owner_profile_exists"] = None

    _refresh_workspace_followups(crm_sb, workspace_id, owner_id)
    print("  evaluating opportunities for CRM intake")
    _log_write_stage(
        "leads_intake",
        "attempted",
        {"owner_id": owner_id, "workspace_id": workspace_id, "debug_mode": bool(debug_mode)},
    )
    intake_min_score = 60.0 if debug_mode else max(float(CRM_INTAKE_MIN_SCORE), 80.0)
    stats["intake_threshold_used"] = intake_min_score
    stats["contact_rule_used"] = "email_required_score_not_blocking"
    opportunities = []
    intake_select_variants = [
        (
            "id,workspace_id,business_name,category,city,lane,address,phone,website,place_id,"
            "recommended_contact_method,backup_contact_method,opportunity_score,"
            "opportunity_signals,opportunity_reason,lead_bucket,close_probability,website_status,status"
        ),
        (
            "id,workspace_id,business_name,category,city,lane,address,phone,website,place_id,"
            "recommended_contact_method,backup_contact_method,opportunity_score,"
            "opportunity_reason,lead_bucket,close_probability,website_status,status"
        ),
        (
            "id,workspace_id,business_name,category,city,lane,address,phone,website,place_id,"
            "recommended_contact_method,backup_contact_method,opportunity_score,website_status,status"
        ),
        (
            "id,workspace_id,business_name,category,city,lane,address,phone,website,place_id,"
            "recommended_contact_method,backup_contact_method,opportunity_score,"
            "opportunity_signals,opportunity_reason,lead_bucket,close_probability,status"
        ),
        (
            "id,workspace_id,business_name,category,city,lane,address,phone,website,place_id,"
            "recommended_contact_method,backup_contact_method,opportunity_score,"
            "opportunity_reason,lead_bucket,close_probability,status"
        ),
        (
            "id,workspace_id,business_name,category,city,lane,address,phone,website,place_id,"
            "recommended_contact_method,backup_contact_method,opportunity_score,status"
        ),
    ]
    intake_queries = []
    for select_clause in intake_select_variants:
        intake_queries.append(
            lambda select_clause=select_clause: (
                sb.table("opportunities")
                .select(select_clause)
                .eq("workspace_id", workspace_id)
                .order("opportunity_score", desc=True)
                .limit(CRM_INTAKE_MAX_CANDIDATES)
                .execute()
                .data
                or []
            )
        )
        intake_queries.append(
            lambda select_clause=select_clause: (
                sb.table("opportunities")
                .select(select_clause)
                .eq("workspace_id", workspace_id)
                .order("created_at", desc=True)
                .limit(CRM_INTAKE_MAX_CANDIDATES)
                .execute()
                .data
                or []
            )
        )
    query_error = None
    for q in intake_queries:
        try:
            rows = q()
            if rows:
                opportunities = rows
                break
        except Exception as e:
            query_error = e
            continue
    if not opportunities and query_error is not None:
        print(f"  [Scout] crm intake query failed: {query_error}", file=sys.stderr)
        stats["query_error"] = str(query_error)
        _log_write_stage(
            "leads_intake",
            "failed",
            {"workspace_id": workspace_id, "error": _safe_error_payload(query_error)},
        )
        return stats

    stats["opportunities_loaded"] = len(opportunities)
    stats["opportunities_found"] = len(opportunities)
    if not opportunities:
        print("  crm intake complete (no qualifying opportunities)")
        return stats

    opp_ids = [str(opp.get("id")) for opp in opportunities if opp.get("id")]
    case_by_opp = _case_map_for_workspace(sb, workspace_id, opp_ids)
    existing = _load_existing_crm_leads_for_owner(crm_sb, owner_id)

    existing_place_ids = {
        _normalize_text(row.get("place_id")) for row in existing if _normalize_text(row.get("place_id"))
    }
    existing_websites = {
        _normalize_website(row.get("website")) for row in existing if _normalize_website(row.get("website"))
    }
    existing_phones = {
        _normalize_phone(row.get("phone")) for row in existing if _normalize_phone(row.get("phone"))
    }
    existing_name_city = {
        _build_name_city_key(row.get("business_name"), row.get("city") or row.get("address"))
        for row in existing
        if _normalize_text(row.get("business_name"))
    }
    existing_linked_opps = {
        str(row.get("linked_opportunity_id") or "").strip()
        for row in existing
        if str(row.get("linked_opportunity_id") or "").strip()
    }

    candidates: list[dict] = []
    for opp in opportunities:
        stats["scanned_count"] += 1
        stats["evaluated"] += 1
        stats["opportunities_evaluated"] += 1
        case = case_by_opp.get(str(opp.get("id"))) or {}
        score = float(opp.get("opportunity_score") or 0)
        opp_id = str(opp.get("id") or "").strip()
        opp_workspace_id = str(opp.get("workspace_id") or "").strip()
        business_name = str(opp.get("business_name") or "").strip()
        display_name = business_name or "(missing)"
        print(
            f"  [Intake] candidate opp_id={opp_id or '(missing)'} "
            f"name={display_name} score={score:.2f} workspace_id={workspace_id}"
        )
        if not opp_workspace_id:
            stats["filtered_missing_workspace"] += 1
            _count_skip_reason("missing_workspace_id")
            print(f"  [Intake] filtered: missing_workspace_id opp_id={opp_id or '(missing)'}")
            _append_exclusion(
                business_name,
                score,
                "missing workspace_id",
            )
            _append_decision(
                opp_id,
                business_name,
                score,
                "filtered",
                "missing workspace_id",
            )
            continue
        if not business_name:
            stats["filtered_missing_business_name"] += 1
            _count_skip_reason("missing_business_name")
            print(f"  [Intake] filtered: missing_business_name opp_id={opp_id or '(missing)'}")
            _append_exclusion(
                business_name,
                score,
                "missing business_name",
            )
            _append_decision(
                opp_id,
                business_name,
                score,
                "filtered",
                "missing business_name",
            )
            continue

        status_tokens = " ".join(
            [
                str(opp.get("status") or ""),
                str(case.get("status") or ""),
                str(case.get("outcome") or ""),
            ]
        )
        if _is_closed_or_dnc(status_tokens):
            stats["filtered_closed_or_dnc"] += 1
            _count_skip_reason("closed_or_dnc")
            print(f"  [Intake] filtered: closed_or_dnc opp_id={opp_id or '(missing)'}")
            _append_exclusion(
                business_name,
                score,
                "opportunity or case is closed/do_not_contact",
            )
            _append_decision(
                opp_id,
                business_name,
                score,
                "filtered",
                "opportunity or case is closed/do_not_contact",
            )
            continue

        # Enrichment fail-safe: do not create CRM leads until enrichment is present.
        has_case_enrichment = bool(case)
        has_website_analysis = bool(str(opp.get("website_status") or "").strip())
        has_opportunity_reason = bool(str(opp.get("opportunity_reason") or "").strip())
        if not has_case_enrichment:
            stats["filtered_other"] += 1
            _count_skip_reason("missing_case_enrichment")
            _append_exclusion(business_name, score, "missing enrichment case_file")
            _append_decision(
                opp_id,
                business_name,
                score,
                "filtered",
                "missing enrichment case_file",
            )
            continue
        if not has_website_analysis:
            stats["filtered_other"] += 1
            _count_skip_reason("missing_website_analysis")
            _append_exclusion(business_name, score, "missing website_status after enrichment")
            _append_decision(
                opp_id,
                business_name,
                score,
                "filtered",
                "missing website_status after enrichment",
            )
            continue
        if not has_opportunity_reason:
            stats["filtered_missing_opportunity_reason"] += 1
            _count_skip_reason("missing_opportunity_reason")
            _append_exclusion(business_name, score, "missing opportunity_reason after enrichment")
            _append_decision(
                opp_id,
                business_name,
                score,
                "filtered",
                "missing opportunity_reason after enrichment",
            )
            continue

        lead_email_candidate = str(case.get("email") or "").strip()
        lead_phone_candidate = str(opp.get("phone") or case.get("phone_from_site") or "").strip()
        has_contact_available = bool(
            lead_email_candidate
            or lead_phone_candidate
            or str(case.get("contact_page") or case.get("contact_form_url") or "").strip()
            or str(case.get("facebook_url") or case.get("facebook") or "").strip()
        )

        candidates.append(
            {
                "opp": opp,
                "case": case,
                "lead_tier": (
                    "actionable_email"
                    if lead_email_candidate
                    else "contact_available"
                    if has_contact_available
                    else "door_to_door_candidate"
                ),
            }
        )
        stats["enriched_count"] += 1

    candidates.sort(
        key=lambda item: (
            _lane_priority(item["opp"].get("lane")),
            -float(item["opp"].get("opportunity_score") or item["opp"].get("internal_score") or 0),
        )
    )

    for item in candidates:
        opp = item["opp"]
        case = item["case"] or {}
        lead_tier = str(item.get("lead_tier") or "").strip() or "contact_available"
        opp_id = str(opp.get("id") or "").strip()
        score = float(opp.get("opportunity_score") or 0)
        business_name = str(opp.get("business_name") or "").strip() or "(missing)"
        stats["eligible"] += 1
        stats["eligible_for_lead_creation"] += 1

        duplicate_reason = None
        if opp_id and opp_id in existing_linked_opps:
            duplicate_reason = "linked_opportunity_id"
            stats["filtered_existing_linked_opportunity"] += 1
            stats["duplicate_skipped"] += 1
            stats["duplicates_skipped"] += 1
            _count_skip_reason("duplicate_by_linked_opportunity_id")
            print(f"  [Intake] duplicate crm lead skipped opp_id={opp_id} reason={duplicate_reason}")
            _append_exclusion(
                business_name,
                score,
                "already linked to existing CRM lead",
            )
            _append_decision(
                opp_id,
                business_name,
                score,
                "duplicate_skipped",
                "already linked to existing CRM lead",
            )
            continue

        place_key = _normalize_text(opp.get("place_id"))
        site_key = _normalize_website(opp.get("website"))
        phone_key = _normalize_phone(opp.get("phone") or case.get("phone_from_site"))
        name_city_key = _build_name_city_key(opp.get("business_name"), opp.get("city") or opp.get("address"))

        if place_key and place_key in existing_place_ids:
            duplicate_reason = "place_id"
            stats["duplicate_by_place_id"] += 1
        elif site_key and site_key in existing_websites:
            duplicate_reason = "website"
            stats["duplicate_by_website"] += 1
        elif phone_key and phone_key in existing_phones:
            duplicate_reason = "phone"
            stats["duplicate_by_phone"] += 1
        elif name_city_key and name_city_key in existing_name_city:
            duplicate_reason = "business_name+city"
            stats["duplicate_by_business_name_city"] += 1
        if duplicate_reason:
            stats["duplicate_skipped"] += 1
            stats["duplicates_skipped"] += 1
            _count_skip_reason(f"duplicate_by_{duplicate_reason}")
            print(
                f"  [Intake] duplicate crm lead skipped opp_id={opp_id or '(missing)'} "
                f"reason={duplicate_reason}"
            )
            _append_exclusion(
                business_name,
                score,
                f"duplicate by {duplicate_reason}",
            )
            _append_decision(
                opp_id,
                business_name,
                score,
                "duplicate_skipped",
                f"duplicate by {duplicate_reason}",
            )
            continue

        lead_email = str(case.get("email") or "").strip()
        lead_email_source = str(case.get("email_source") or "not_found").strip().lower() or "not_found"
        lead_phone = str(opp.get("phone") or case.get("phone_from_site") or "").strip()
        lead_contact_page = str(case.get("contact_page") or case.get("contact_form_url") or "").strip()
        lead_facebook = str(case.get("facebook_url") or case.get("facebook") or "").strip()
        if lead_email:
            best_contact = "email"
        elif lead_contact_page:
            best_contact = "contact_page"
        elif lead_phone:
            best_contact = "phone"
        elif lead_facebook:
            best_contact = "facebook"
        else:
            best_contact = "none"
        lead_bucket = (
            "Easy Win" if score >= 90 else
            "High Value" if score >= 75 else
            "Good Prospect" if score >= 60 else
            "Needs Review" if score >= 40 else
            "Low Priority"
        )
        opportunity_reason = str(opp.get("opportunity_reason") or "").strip()
        has_contact_path = bool(lead_email or lead_contact_page or lead_phone or lead_facebook)
        actionable_email_ready = bool(business_name and workspace_id and lead_email)
        if not has_contact_path:
            lead_bucket = "Door-to-Door Candidates"
        elif not actionable_email_ready:
            lead_bucket = "Needs Review"
        issue_list = opp.get("opportunity_signals") if isinstance(opp.get("opportunity_signals"), list) else []
        if not issue_list:
            issue_list = case.get("strongest_problems") if isinstance(case.get("strongest_problems"), list) else []
        issues_summary = ", ".join([str(i).strip() for i in issue_list if str(i).strip()][:3]) or "Contact info is hard to find"
        review_count = int(opp.get("review_count") or case.get("google_review_count") or 0)
        conversion_score, score_breakdown = _compute_lead_conversion_score(
            email=lead_email,
            contact_page=lead_contact_page,
            phone=lead_phone,
            facebook_url=lead_facebook,
            website_status=str(opp.get("website_status") or "").strip(),
            review_count=review_count,
            category=str(opp.get("category") or "").strip(),
            business_name=business_name,
            website=str(opp.get("website") or "").strip(),
            opportunity_reason=opportunity_reason or issues_summary,
        )
        row = {
            "owner_id": owner_id,
            "workspace_id": workspace_id,
            "linked_opportunity_id": opp.get("id"),
            "business_name": business_name,
            "contact_name": None,
            "email": lead_email or None,
            "phone": lead_phone or None,
            "website": opp.get("website"),
            "contact_page": lead_contact_page or None,
            "category": opp.get("category"),
            "industry": opp.get("category"),
            "lead_source": "scout-brain",
            "address": opp.get("address"),
            "city": opp.get("city"),
            "place_id": opp.get("place_id"),
            "best_contact_method": best_contact,
            "has_contact_path": has_contact_path,
            "opportunity_score": score,
            "conversion_score": conversion_score,
            "score_breakdown": score_breakdown,
            "opportunity_reason": opportunity_reason or issues_summary,
            "lead_bucket": lead_bucket,
            "website_status": str(opp.get("website_status") or "").strip() or None,
            "google_rating": opp.get("google_rating") or opp.get("rating"),
            "google_review_count": review_count,
            "recommended_next_action": (
                "Send First Touch"
                if lead_email
                else "Open Contact Path"
                if (lead_contact_page or lead_facebook)
                else "Text Outreach"
                if lead_phone
                else "Save for Door-to-Door"
            ),
            "why_it_matters": (
                "Customers may have trouble contacting this business online."
                if not has_contact_path
                else "Business appears active, but website/contact flow can be improved to capture more customers."
            ),
            "best_pitch_angle": (
                f"I noticed {opportunity_reason or issues_summary.lower()} and can help you get more customer inquiries."
            ),
            "auto_intake": True,
            "status": "new" if actionable_email_ready else "research_later",
            "sequence_active": bool(actionable_email_ready),
            "sequence_step": 1 if actionable_email_ready else None,
            "next_follow_up_at": datetime.now(timezone.utc).isoformat() if (actionable_email_ready and AUTO_SEQUENCE_SEND_STEP1) else None,
            "notes": (
                f"Auto-added from Scout-Brain (lane: {opp.get('lane') or 'unknown'}, lead_bucket: {lead_bucket}, close_probability: {opp.get('close_probability') or 'medium'}). "
                f"Issues: {issues_summary}. "
                f"Reason: {opportunity_reason or issues_summary}. "
                f"email_source: {lead_email_source}. "
                f"lead_tier: {lead_tier}. "
                f"actionable_email_ready: {'yes' if actionable_email_ready else 'no'}."
            ),
        }
        stats["insert_attempted"] += 1
        print(
            f"  [Intake] insert_attempted opp_id={opp_id or '(missing)'} "
            f"name={business_name} score={score:.2f} workspace_id={workspace_id} "
            f"payload_keys={','.join(sorted(row.keys()))}"
        )
        insert_result = _insert_crm_lead(crm_sb, row)
        if bool(insert_result.get("ok")):
            stats["insert_succeeded"] += 1
            stats["created"] += 1
            stats["leads_created"] += 1
            has_email = bool(lead_email)
            has_phone = bool(lead_phone)
            has_contact_page = bool(lead_contact_page)
            has_facebook = bool(lead_facebook)
            if has_email:
                stats["leads_with_email"] += 1
                stats["actionable_email_leads_created"] += 1
            elif has_contact_page or has_facebook:
                # Tier 2 leads are intentionally kept for contact-page/social workflows.
                stats["research_later_leads_created"] += 1
            if has_phone:
                stats["leads_with_phone"] += 1
            if has_contact_page:
                stats["leads_with_contact_page"] += 1
            if has_facebook:
                stats["leads_with_facebook"] += 1
            if str(lead_bucket).strip().lower() == "door-to-door candidates":
                stats["door_to_door_candidates_created"] += 1
            if str(lead_bucket).strip().lower() in {"low priority", "low_priority"}:
                stats["archived_low_priority_created"] += 1
            if not (has_email or has_phone or has_contact_page or has_facebook):
                stats["leads_with_no_contact_path"] += 1
                _count_skip_reason("created_without_contact_path")
                stats["research_later_leads_created"] += 1
            if score < intake_min_score:
                stats["leads_created_with_low_score"] += 1
            else:
                stats["leads_created_high_score"] += 1
            print(
                f"  crm lead created from scout opportunity "
                f"(opp_id={opp_id or '(missing)'}, mode={insert_result.get('mode')})"
            )
            if len(stats["top_contacts"]) < 5:
                stats["top_contacts"].append(
                    {
                        "business_name": business_name,
                        "score": score,
                        "best_contact_method": best_contact,
                    }
                )
            if place_key:
                existing_place_ids.add(place_key)
            if site_key:
                existing_websites.add(site_key)
            if phone_key:
                existing_phones.add(phone_key)
            if name_city_key:
                existing_name_city.add(name_city_key)
            if opp_id:
                existing_linked_opps.add(opp_id)
            inserted_lead = None
            inserted_data = insert_result.get("data") if isinstance(insert_result, dict) else None
            inserted_id = str((inserted_data or {}).get("id") or "").strip() if isinstance(inserted_data, dict) else ""
            if inserted_id:
                inserted_lead = _crm_fetch_lead(crm_sb, inserted_id, owner_id)
            if inserted_lead is None and opp_id:
                inserted_lead = _crm_fetch_lead_by_linked_opportunity(
                    crm_sb,
                    owner_id=owner_id,
                    workspace_id=workspace_id,
                    linked_opportunity_id=opp_id,
                )
            if inserted_lead and AUTO_SEQUENCE_SEND_STEP1 and actionable_email_ready:
                recipient = str(inserted_lead.get("email") or "").strip()
                if recipient:
                    subject, email_body, message_type = _sequence_template_for_step(inserted_lead, 1)
                    seq_result = _send_outreach_email_for_lead(
                        crm_sb,
                        lead=inserted_lead,
                        owner_id=owner_id,
                        workspace_id=workspace_id,
                        recipient=recipient,
                        subject=subject,
                        content=email_body,
                        message_type=message_type,
                        sequence_step_sent=1,
                    )
                    if seq_result.get("ok"):
                        stats["sequence_started"] += 1
                    else:
                        stats["sequence_send_failed"] += 1
                        print(
                            f"  [Scout] sequence start failed opp_id={opp_id or '(missing)'} "
                            f"lead_id={inserted_lead.get('id')} error={seq_result.get('error')}",
                            file=sys.stderr,
                        )
                else:
                    stats["sequence_stopped"] += 1
                    _apply_lead_updates_safe(
                        crm_sb,
                        str(inserted_lead.get("id") or ""),
                        {"sequence_active": False, "next_follow_up_at": None},
                    )
                    print(
                        f"  [Scout] sequence stopped (missing email) "
                        f"lead_id={inserted_lead.get('id')}",
                        file=sys.stderr,
                    )
            _append_decision(
                opp_id,
                business_name,
                score,
                "inserted",
                f"lead inserted successfully (mode={insert_result.get('mode')})",
            )
        else:
            stats["insert_failed"] += 1
            stats["insert_errors"] += 1
            _count_skip_reason("insert_error")
            insert_error = str(insert_result.get("error") or "unknown insert error")
            print(
                f"  [Scout] crm intake insert failed opp_id={opp_id or '(missing)'} "
                f"name={business_name} reason={insert_error}",
                file=sys.stderr,
            )
            if len(stats["insert_error_samples"]) < 5:
                stats["insert_error_samples"].append(insert_error)
            _append_exclusion(
                business_name,
                score,
                f"insert_failed: {insert_error}",
            )
            _append_decision(
                opp_id,
                business_name,
                score,
                "insert_failed",
                insert_error,
            )

    stats["filtered_out"] = int(
        stats["filtered_low_score"]
        + stats["filtered_missing_workspace"]
        + stats["filtered_closed_or_dnc"]
        + stats["filtered_missing_contact_path"]
        + stats["filtered_missing_email"]
        + stats["filtered_missing_opportunity_reason"]
        + stats["filtered_missing_business_name"]
        + stats["filtered_other"]
    )

    print(
        "  crm intake complete "
        f"(scanned_count={stats['scanned_count']}, enriched_count={stats['enriched_count']}, "
        f"(evaluated={stats['evaluated']}, eligible={stats['eligible']}, created={stats['created']}, "
        f"duplicate_skipped={stats['duplicate_skipped']}, insert_attempted={stats['insert_attempted']}, "
        f"insert_succeeded={stats['insert_succeeded']}, insert_failed={stats['insert_failed']}, "
        f"sequence_started={stats['sequence_started']}, sequence_send_failed={stats['sequence_send_failed']}, "
        f"sequence_stopped={stats['sequence_stopped']}, "
        f"filtered_low_score={stats['filtered_low_score']}, filtered_missing_contact_path={stats['filtered_missing_contact_path']}, "
        f"filtered_missing_email={stats['filtered_missing_email']}, filtered_missing_opportunity_reason={stats['filtered_missing_opportunity_reason']}, "
        f"leads_with_email={stats['leads_with_email']}, leads_with_phone={stats['leads_with_phone']}, "
        f"leads_with_contact_page={stats['leads_with_contact_page']}, leads_with_facebook={stats['leads_with_facebook']}, "
        f"leads_with_no_contact_path={stats['leads_with_no_contact_path']}, actionable_email_leads_created={stats['actionable_email_leads_created']}, "
        f"research_later_leads_created={stats['research_later_leads_created']}, door_to_door_candidates_created={stats['door_to_door_candidates_created']}, "
        f"archived_low_priority_created={stats['archived_low_priority_created']}, "
        f"leads_skipped_due_no_email={stats['leads_skipped_due_no_email']}, "
        f"leads_created_with_low_score={stats['leads_created_with_low_score']}, leads_created_high_score={stats['leads_created_high_score']}, "
        f"filtered_closed_or_dnc={stats['filtered_closed_or_dnc']}, leads_skipped_reason={stats['leads_skipped_reason']})"
    )
    _log_write_stage(
        "leads_intake",
        "succeeded",
        {
            "workspace_id": workspace_id,
            "created": stats["created"],
            "duplicate_skipped": stats["duplicate_skipped"],
            "filtered_low_score": stats["filtered_low_score"],
            "insert_failed": stats["insert_failed"],
            "query_error": stats["query_error"],
            "scanned_count": stats["scanned_count"],
            "enriched_count": stats["enriched_count"],
            "leads_skipped_reason": stats["leads_skipped_reason"],
        },
    )
    stats["reason_counts"] = {
        "missing_business_name": int(stats["filtered_missing_business_name"]),
        "missing_workspace_id": int(stats["filtered_missing_workspace"]),
        "missing_case_enrichment": int((stats["leads_skipped_reason"] or {}).get("missing_case_enrichment") or 0),
        "missing_website_analysis": int((stats["leads_skipped_reason"] or {}).get("missing_website_analysis") or 0),
        "missing_contact_path": int(stats["filtered_missing_contact_path"]),
        "missing_email": int(stats["filtered_missing_email"]),
        "missing_opportunity_reason": int(stats["filtered_missing_opportunity_reason"]),
        "score_below_threshold": int(stats["filtered_low_score"]),
        "duplicate_by_linked_opportunity_id": int(stats["filtered_existing_linked_opportunity"]),
        "duplicate_by_website": int(stats["duplicate_by_website"]),
        "duplicate_by_phone": int(stats["duplicate_by_phone"]),
        "duplicate_by_business_name_city": int(stats["duplicate_by_business_name_city"]),
        "insert_error": int(stats["insert_failed"]),
    }
    return stats


def _generate_outreach_drafts_for_workspace(
    sb,
    workspace_id: str,
    owner_id: str,
    *,
    min_score: float = 80.0,
    max_candidates: int = 120,
) -> int:
    draft_count = 0
    if not workspace_id or not owner_id:
        return draft_count
    try:
        leads_rows = (
            sb.table(CRM_LEADS_TABLE)
            .select("id,workspace_id,linked_opportunity_id,email,contact_name,opportunity_score,status")
            .eq("owner_id", owner_id)
            .eq("workspace_id", workspace_id)
            .gte("opportunity_score", min_score)
            .in_("status", ["new", "contacted", "follow_up_due"])
            .order("opportunity_score", desc=True)
            .limit(max_candidates)
            .execute()
            .data
            or []
        )
    except Exception as e:
        print(f"  [Scout] nightly draft query failed: {e}", file=sys.stderr)
        return draft_count

    for lead in leads_rows:
        lead_id = str(lead.get("id") or "").strip()
        linked_opportunity_id = str(lead.get("linked_opportunity_id") or "").strip()
        contact_email = str(lead.get("email") or "").strip().lower()
        if not lead_id or not linked_opportunity_id or not contact_email:
            continue
        try:
            existing_draft = (
                sb.table("email_messages")
                .select("id")
                .eq("lead_id", lead_id)
                .eq("status", "draft")
                .limit(1)
                .execute()
                .data
                or []
            )
            if existing_draft:
                continue
        except Exception:
            pass

        template = _load_outreach_template_for_opportunity(
            linked_opportunity_id,
            workspace_id=workspace_id,
        )
        if not template:
            continue

        subject = str(template.get("subject") or "quick question about your website").strip()
        body = (
            str(template.get("short_email") or "").strip()
            or str(template.get("longer_email") or "").strip()
            or str(template.get("follow_up_note") or "").strip()
        )
        if not body:
            continue

        thread = _upsert_email_thread(
            sb,
            workspace_id=workspace_id,
            lead_id=lead_id,
            contact_email=contact_email,
            subject=subject,
            provider_thread_id=None,
            owner_id=owner_id,
        )
        thread_id = str((thread or {}).get("id") or "").strip() or None
        inserted = _insert_email_message(
            sb,
            {
                "thread_id": thread_id,
                "lead_id": lead_id,
                "direction": "outbound",
                "provider_message_id": None,
                "subject": subject,
                "body": body,
                "delivery_status": "queued",
                "status": "draft",
                "generated_by": "scout-brain",
                "sent_at": None,
                "received_at": None,
                "owner_id": owner_id,
            },
        )
        if inserted:
            draft_count += 1
    return draft_count


def _build_nightly_report(
    today: dict | None,
    opportunities: list | None,
    intake_stats: dict | None,
    email_drafts_generated: int,
) -> dict:
    opps = opportunities or []
    return {
        "cities_scanned": int((today or {}).get("cities_scanned") or 0),
        "industries_scanned": int((today or {}).get("industries_scanned") or 0),
        "businesses_found": int((today or {}).get("businesses_found") or len(opps)),
        "opportunities_scored": int(len(opps)),
        "leads_created": int((intake_stats or {}).get("created") or 0),
        "email_drafts_generated": int(email_drafts_generated or 0),
    }


def daily_scout_job():
    if not _daily_scout_lock.acquire(blocking=False):
        print("  [Scout] daily scout already running, skipping duplicate trigger")
        return
    try:
        print("  nightly scout started")
        if not _supabase_url or not _supabase_service_key:
            print("  [Scout] scheduled scout skipped: Supabase env not configured", file=sys.stderr)
            return
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        region = _nightly_region_for_today()
        print(f"  nightly region selected: {region}")
        previous_region = os.environ.get("SCOUT_REGION_OVERRIDE")
        os.environ["SCOUT_REGION_OVERRIDE"] = region
        try:
            _run_morning_runner()
        finally:
            if previous_region is None:
                os.environ.pop("SCOUT_REGION_OVERRIDE", None)
            else:
                os.environ["SCOUT_REGION_OVERRIDE"] = previous_region
        print("  discovery completed")
        print("  analysis completed")
        data = _load_scout_data()
        today = data.get("today") or {}
        opportunities = data.get("opportunities") or []

        workspaces = sb.table("workspaces").select("id,name,owner_user_id,plan").execute().data or []
        if SCHEDULED_SCOUT_SCOPE == "internal":
            scoped = [
                w for w in workspaces
                if str((w.get("name") or "")).strip().lower() == SCHEDULED_SCOUT_WORKSPACE_NAME.lower()
            ]
            workspaces = scoped

        for ws in workspaces:
            owner_id = ws.get("owner_user_id")
            if not owner_id:
                continue
            ws_id = str(ws.get("id") or "").strip()
            _sync_scout_to_supabase(owner_id, workspace_id=ws_id)
            intake_stats = _run_workspace_crm_intake(sb, ws, owner_id)
            email_drafts_generated = _generate_outreach_drafts_for_workspace(
                sb,
                ws_id,
                owner_id,
                min_score=80.0,
            )
            nightly_report = _build_nightly_report(
                today,
                opportunities,
                intake_stats,
                email_drafts_generated,
            )
            _record_scout_run_supabase(
                owner_id,
                ws_id,
                today,
                opportunities,
                nightly_report=nightly_report,
            )
            _process_workspace_outreach_sequences(str(ws.get("id") or ""), owner_id)
            print(
                f"  nightly report ({ws_id}): "
                f"cities={nightly_report['cities_scanned']}, "
                f"industries={nightly_report['industries_scanned']}, "
                f"found={nightly_report['businesses_found']}, "
                f"scored={nightly_report['opportunities_scored']}, "
                f"leads={nightly_report['leads_created']}, "
                f"drafts={nightly_report['email_drafts_generated']}"
            )
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

        print("  nightly intake complete")
        print("  scheduled scout finished")
    except Exception as e:
        print(f"  [Scout] daily scout error: {e}", file=sys.stderr)
    finally:
        try:
            _daily_scout_lock.release()
        except Exception:
            pass


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
        .insert({"name": workspace_name, "owner_user_id": user_id, "plan": "free"})
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
    user_id = _get_user_id_from_request(request)
    if user_id and _supabase_url and _supabase_service_key:
        try:
            from supabase import create_client
            sb = create_client(_supabase_url, _supabase_service_key)
            workspace = _get_workspace_for_user(sb, user_id, workspace_id)
            plan = workspace.get("plan") or "free"
            if not _plan_limits(plan).get("top_opportunities_dashboard", True):
                print("  upgrade prompt shown")
                return {"leads": [], "plan_notice": _plan_limit_message(), "plan": plan}
            workspace_id = workspace.get("id")
        except Exception:
            pass
    leads = getTopOpportunities(workspace_id)
    return {"leads": leads}


@app.get("/opportunities/search")
def search_opportunities(
    request: Request,
    city: str | None = None,
    state: str | None = None,
    industry: str | None = None,
    website_score: float | None = None,
    rating: float | None = None,
    limit: int = 100,
):
    workspace_id = _get_workspace_id_from_request(request)
    user_id = _get_user_id_from_request(request)

    def _row_matches(row: dict) -> bool:
        if city and city.lower() not in str(row.get("city") or "").lower():
            return False
        if state and state.lower() not in str(row.get("state") or "").lower():
            return False
        if industry and industry.lower() not in str(row.get("industry") or row.get("category") or "").lower():
            return False
        if website_score is not None:
            try:
                ws = float(row.get("website_score"))
                if ws > float(website_score):
                    return False
            except Exception:
                return False
        if rating is not None:
            try:
                rv = float(row.get("rating"))
                if rv < float(rating):
                    return False
            except Exception:
                return False
        return True

    rows: list[dict] = []
    suppression_sets = {
        "linked_ids": set(),
        "place_ids": set(),
        "websites": set(),
        "phones": set(),
        "name_addr": set(),
    }
    if _supabase_url and _supabase_service_key:
        try:
            from supabase import create_client
            sb = create_client(_supabase_url, _supabase_service_key)
            if user_id:
                try:
                    ws = _get_workspace_for_user(sb, user_id, workspace_id)
                    workspace_id = ws.get("id")
                except Exception:
                    pass
            query = sb.table("opportunities").select("*")
            if workspace_id:
                query = query.eq("workspace_id", workspace_id)
            if city:
                query = query.ilike("city", f"%{city}%")
            if state:
                query = query.ilike("state", f"%{state}%")
            if industry:
                query = query.or_(f"industry.ilike.%{industry}%,category.ilike.%{industry}%")
            if website_score is not None:
                query = query.lte("website_score", float(website_score))
            if rating is not None:
                query = query.gte("rating", float(rating))
            res = query.limit(max(1, min(500, int(limit)))).execute()
            rows = res.data or []
            crm_sb = _create_crm_client()
            if crm_sb is not None and workspace_id:
                suppression_sets = _crm_suppression_sets_for_workspace(crm_sb, workspace_id)
        except Exception:
            rows = []

    if not rows and CASES_DIR.is_dir():
        for path in CASES_DIR.glob("*.json"):
            try:
                with open(path, encoding="utf-8") as f:
                    row = json.load(f)
                if _row_matches(row):
                    rows.append(row)
            except Exception:
                continue

    visible_rows = [
        r
        for r in rows
        if not _is_ignored_lead_status(r.get("status"))
        and str(r.get("id") or "") not in suppression_sets["linked_ids"]
        and _normalize_text(r.get("place_id")) not in suppression_sets["place_ids"]
        and _normalize_website(r.get("website")) not in suppression_sets["websites"]
        and _normalize_phone(r.get("phone")) not in suppression_sets["phones"]
        and _build_name_address_key(r.get("business_name"), r.get("address")) not in suppression_sets["name_addr"]
    ]

    ranked = sorted(
        visible_rows,
        key=lambda r: float(r.get("opportunity_score") or r.get("internal_score") or 0),
        reverse=True,
    )[: max(1, min(500, int(limit)))]
    return {"count": len(ranked), "opportunities": ranked}


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
    scan_settings: dict | None = None


class UserSettingsBody(BaseModel):
    email_notifications_enabled: bool = True
    email_frequency: str = "daily"
    include_new_leads: bool = True
    include_followups: bool = True
    include_top_opportunities: bool = True


class OutreachSendBody(BaseModel):
    workspace_id: str | None = None
    lead_id: str
    case_id: str | None = None
    to: str | None = None
    subject: str
    body: str
    message_type: str = "short_email"


class OutreachTemplateBody(BaseModel):
    workspace_id: str | None = None
    lead_id: str | None = None
    linked_opportunity_id: str | None = None


class OutreachRegenerateBody(BaseModel):
    workspace_id: str | None = None
    lead_id: str | None = None
    linked_opportunity_id: str | None = None


class OutreachGenerateEmailBody(BaseModel):
    workspace_id: str | None = None
    lead_id: str | None = None
    linked_opportunity_id: str | None = None


class OutreachTestBody(BaseModel):
    to: str
    subject: str | None = None
    body: str | None = None


class InboundEmailBody(BaseModel):
    from_email: str | None = None
    subject: str | None = None
    body: str | None = None
    provider_message_id: str | None = None
    provider_thread_id: str | None = None
    in_reply_to: str | None = None
    references: list[str] | None = None
    workspace_id: str | None = None
    received_at: str | None = None


class IntakeBackfillBody(BaseModel):
    workspace_id: str | None = None
    debug_mode: bool | None = None


@app.get("/job/{job_id}")
def get_job_status(request: Request, job_id: str):
    workspace_id = _get_workspace_id_from_request(request)
    job = _job_get(job_id)
    if not job:
        job = _load_job_from_supabase(job_id, workspace_id=workspace_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if workspace_id and job.get("workspace_id") and job.get("workspace_id") != workspace_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return {
        "id": job.get("id"),
        "status": job.get("status"),
        "progress": int(job.get("progress") or 0),
        "message": job.get("message") or job.get("result_summary"),
        "stage": job.get("stage"),
        "summary": job.get("message") or job.get("result_summary"),
        "error": job.get("error"),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "persistence_debug": (job.get("payload") or {}).get("persistence_debug"),
        "scout_summary": (job.get("payload") or {}).get("scout_summary"),
        "scan_settings": (job.get("payload") or {}).get("scan_settings"),
    }


@app.post("/job/{job_id}/cancel")
def cancel_job(request: Request, job_id: str):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    workspace_id = _get_workspace_id_from_request(request)
    job = _job_get(job_id)
    if not job:
        job = _load_job_from_supabase(job_id, workspace_id=workspace_id)
        if job:
            _job_store(job)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if workspace_id and job.get("workspace_id") and job.get("workspace_id") != workspace_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    status = str(job.get("status") or "").strip().lower()
    if status in {"finished", "completed", "failed", "cancelled"}:
        return {
            "ok": True,
            "job_id": job_id,
            "status": status,
            "message": job.get("message") or "Job already finished",
        }
    updated = _job_update(
        job_id,
        status="cancelled",
        message="Stopping scout...",
        result_summary="Stopping scout...",
        stage="cancelled",
        finished_at=_job_now_iso(),
        error=None,
    )
    if updated:
        _upsert_job_supabase(updated)
    return {
        "ok": True,
        "job_id": job_id,
        "status": "cancelled",
        "message": "Stopping scout...",
    }


@app.get("/jobs/active")
def get_active_job(request: Request):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    workspace_id = _get_workspace_id_from_request(request)
    if _supabase_url and _supabase_service_key:
        try:
            from supabase import create_client
            sb = create_client(_supabase_url, _supabase_service_key)
            workspace_id = _resolve_workspace_id_for_user(sb, user_id, workspace_id) or workspace_id
            if not workspace_id:
                # Cross-device fallback: check all user workspaces for active scout jobs.
                user_workspaces = _workspace_ids_for_user(sb, user_id)
                for ws_id in user_workspaces:
                    active_for_ws = _load_active_job_from_supabase(ws_id)
                    if active_for_ws:
                        print(f"  restoring active scout job from jobs table: {active_for_ws.get('id')}")
                        return {
                            "active_job": {
                                "id": active_for_ws.get("id"),
                                "status": active_for_ws.get("status"),
                                "progress": int(active_for_ws.get("progress") or 0),
                                "message": active_for_ws.get("message") or active_for_ws.get("result_summary"),
                                "stage": active_for_ws.get("stage"),
                                "summary": active_for_ws.get("message") or active_for_ws.get("result_summary"),
                                "error": active_for_ws.get("error"),
                                "created_at": active_for_ws.get("created_at"),
                                "started_at": active_for_ws.get("started_at"),
                                "finished_at": active_for_ws.get("finished_at"),
                                "persistence_debug": (active_for_ws.get("payload") or {}).get("persistence_debug"),
                                "scout_summary": (active_for_ws.get("payload") or {}).get("scout_summary"),
                                "scan_settings": (active_for_ws.get("payload") or {}).get("scan_settings"),
                            }
                        }
        except Exception:
            pass
    active = _load_active_job_from_supabase(workspace_id)
    if not active:
        print("  restoring active scout job from jobs table: none")
        return {"active_job": None}
    print(f"  restoring active scout job from jobs table: {active.get('id')}")
    return {
        "active_job": {
            "id": active.get("id"),
            "status": active.get("status"),
            "progress": int(active.get("progress") or 0),
            "message": active.get("message") or active.get("result_summary"),
            "stage": active.get("stage"),
            "summary": active.get("message") or active.get("result_summary"),
            "error": active.get("error"),
            "created_at": active.get("created_at"),
            "started_at": active.get("started_at"),
            "finished_at": active.get("finished_at"),
            "persistence_debug": (active.get("payload") or {}).get("persistence_debug"),
            "scout_summary": (active.get("payload") or {}).get("scout_summary"),
            "scan_settings": (active.get("payload") or {}).get("scan_settings"),
        }
    }


@app.post("/scheduled/scout")
def post_scheduled_scout():
    if _daily_scout_lock.locked():
        return {
            "ok": True,
            "started": False,
            "job_name": SCHEDULED_SCOUT_JOB_NAME,
            "message": "Nightly scout is already running",
        }
    worker = threading.Thread(target=daily_scout_job, daemon=True)
    worker.start()
    return {
        "ok": True,
        "started": True,
        "job_name": SCHEDULED_SCOUT_JOB_NAME,
        "message": "Nightly scout started",
    }


@app.post("/scheduled/outreach-sequences")
def post_scheduled_outreach_sequences():
    if not _supabase_url or not _supabase_service_key:
        raise HTTPException(status_code=500, detail="Supabase backend is not configured")
    try:
        from supabase import create_client

        sb = create_client(_supabase_url, _supabase_service_key)
        workspaces = sb.table("workspaces").select("id,owner_user_id,name").execute().data or []
        total_checked = 0
        total_sent = 0
        total_failed = 0
        total_stopped = 0
        for ws in workspaces:
            owner_id = str(ws.get("owner_user_id") or "").strip()
            workspace_id = str(ws.get("id") or "").strip()
            if not owner_id or not workspace_id:
                continue
            stats = _process_workspace_outreach_sequences(workspace_id, owner_id)
            total_checked += int(stats.get("checked") or 0)
            total_sent += int(stats.get("sent") or 0)
            total_failed += int(stats.get("failed") or 0)
            total_stopped += int(stats.get("stopped") or 0)
        return {
            "ok": True,
            "checked": total_checked,
            "sent": total_sent,
            "failed": total_failed,
            "stopped": total_stopped,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"scheduled_outreach_sequences_failed: {e}")


@app.post("/crm/intake/backfill")
def post_crm_intake_backfill(request: Request, body: IntakeBackfillBody | None = None):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not _supabase_url or not _supabase_service_key:
        raise HTTPException(status_code=500, detail="Supabase backend is not configured")

    try:
        from supabase import create_client

        sb = create_client(_supabase_url, _supabase_service_key)
        requested_workspace_id = (body.workspace_id or "").strip() if body else ""
        requested_workspace_id = requested_workspace_id or _get_workspace_id_from_request(request) or ""
        workspace = _get_workspace_for_user(sb, user_id, requested_workspace_id)
        workspace_id = str(workspace.get("id") or "").strip()
        if not workspace_id:
            raise HTTPException(status_code=400, detail="workspace could not be resolved")

        debug_mode = True if body is None or body.debug_mode is None else bool(body.debug_mode)
        print(f"  evaluating opportunities for CRM intake (debug_mode={debug_mode})")
        stats = _run_workspace_crm_intake(sb, workspace, user_id, debug_mode=debug_mode)
        print("  crm intake complete")
        crm_host = urlparse(CRM_SUPABASE_URL or "").netloc or None
        scout_host = urlparse(_supabase_url or "").netloc or None
        return {
            "ok": True,
            "workspace_id": workspace_id,
            "stats": stats,
            "crm_supabase_host": crm_host,
            "scout_supabase_host": scout_host,
            "message": (
                f"Backfill complete: opportunities_found={int(stats.get('opportunities_found') or 0)}, "
                f"opportunities_evaluated={int(stats.get('opportunities_evaluated') or 0)}, "
                f"eligible_for_lead_creation={int(stats.get('eligible_for_lead_creation') or 0)}, "
                f"leads_created={int(stats.get('leads_created') or 0)}, "
                f"duplicates_skipped={int(stats.get('duplicates_skipped') or 0)}, "
                f"insert_failed={int(stats.get('insert_failed') or 0)}, "
                f"filtered_out={int(stats.get('filtered_out') or 0)}, "
                f"leads_with_email={int(stats.get('leads_with_email') or 0)}, "
                f"leads_with_phone={int(stats.get('leads_with_phone') or 0)}, "
                f"leads_with_contact_page={int(stats.get('leads_with_contact_page') or 0)}, "
                f"leads_with_facebook={int(stats.get('leads_with_facebook') or 0)}, "
                f"leads_with_no_contact_path={int(stats.get('leads_with_no_contact_path') or 0)}, "
                f"actionable_email_leads_created={int(stats.get('actionable_email_leads_created') or 0)}, "
                f"leads_skipped_due_no_email={int(stats.get('leads_skipped_due_no_email') or 0)}, "
                f"leads_created_with_low_score={int(stats.get('leads_created_with_low_score') or 0)}, "
                f"leads_created_high_score={int(stats.get('leads_created_high_score') or 0)}."
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"crm_intake_backfill_failed: {e}")


@app.post("/run-scout")
def post_run_scout(request: Request, body: RunScoutBody | None = None):
    try:
        user_id = _get_user_id_from_request(request)
        if not user_id:
            _log_write_stage(
                "run_scout",
                "request_rejected",
                {"reason": "user_id_not_resolved"},
            )
            raise HTTPException(status_code=401, detail="Unauthorized")
        requested_workspace_id = _get_workspace_id_from_request(request)
        workspace_plan = "free"
        workspace_id = requested_workspace_id
        workspace_resolution = {
            "requested_workspace_id": requested_workspace_id,
            "resolved_workspace_id": None,
            "workspace_plan": workspace_plan,
            "resolution_source": "request_header",
            "resolution_error": None,
        }
        _log_write_stage(
            "run_scout",
            "request_received",
            {
                "resolved_user_id": user_id,
                "requested_workspace_id": requested_workspace_id,
                "backend_supabase_url": _supabase_url,
                "crm_supabase_url": CRM_SUPABASE_URL,
                "runtime_config": _runtime_config_snapshot(),
            },
        )
        _log_runtime_config("run_scout")
        if user_id and _supabase_url and _supabase_service_key:
            try:
                from supabase import create_client
                sb = create_client(_supabase_url, _supabase_service_key)
                ws = _get_workspace_for_user(sb, user_id, requested_workspace_id)
                workspace_id = str(ws.get("id") or "").strip() or None
                workspace_plan = _normalize_plan(ws.get("plan"))
                workspace_resolution["resolved_workspace_id"] = workspace_id
                workspace_resolution["workspace_plan"] = workspace_plan
                workspace_resolution["resolution_source"] = "workspace_lookup"
                _log_write_stage(
                    "run_scout",
                    "workspace_resolved",
                    {
                        "resolved_user_id": user_id,
                        "resolved_workspace_id": workspace_id,
                        "workspace_plan": workspace_plan,
                        "resolution_source": workspace_resolution["resolution_source"],
                    },
                )
                if not workspace_id:
                    bootstrap = _bootstrap_workspace_for_user(sb, user_id)
                    workspace_id = str(bootstrap.get("workspace_id") or "").strip() or None
                    workspace_resolution["resolved_workspace_id"] = workspace_id
                    workspace_resolution["resolution_source"] = "workspace_bootstrap"
                    _log_write_stage(
                        "run_scout",
                        "workspace_bootstrapped",
                        {
                            "resolved_user_id": user_id,
                            "resolved_workspace_id": workspace_id,
                            "workspace_created": bool(bootstrap.get("created")),
                        },
                    )
                allowed, limit_msg = _check_plan_limits_for_run(workspace_plan, _get_workspace_usage(sb, workspace_id))
                if not allowed:
                    print("  upgrade prompt shown")
                    return _scout_error_response(
                        "plan_limit_reached",
                        limit_msg or _plan_limit_message(),
                        limit_msg or _plan_limit_message(),
                    )
            except Exception as workspace_error:
                # Never block scout run if plan lookup fails unexpectedly.
                workspace_plan = "free"
                workspace_resolution["resolution_error"] = _safe_error_payload(workspace_error)
                _log_write_stage(
                    "run_scout",
                    "workspace_resolution_failed",
                    {
                        "resolved_user_id": user_id,
                        "requested_workspace_id": requested_workspace_id,
                        "error": _safe_error_payload(workspace_error),
                    },
                )
        else:
            missing = []
            if not _supabase_url:
                missing.append("SUPABASE_URL")
            if not _supabase_service_key:
                missing.append("SUPABASE_SERVICE_ROLE_KEY")
            workspace_resolution["resolution_error"] = {
                "type": "RuntimeConfigMissing",
                "message": "Cannot resolve workspace via membership lookup because backend Supabase config is missing.",
                "missing_env": missing,
            }
            _log_write_stage(
                "run_scout",
                "workspace_resolution_skipped",
                {
                    "resolved_user_id": user_id,
                    "missing_env": missing,
                },
            )

        if not workspace_id:
            fallback_workspace_id = _workspace_env_fallback_id()
            if fallback_workspace_id:
                workspace_id = fallback_workspace_id
                workspace_resolution["resolved_workspace_id"] = workspace_id
                workspace_resolution["resolution_source"] = "env_fallback"
                _log_write_stage(
                    "run_scout",
                    "workspace_fallback_applied",
                    {
                        "resolved_user_id": user_id,
                        "resolved_workspace_id": workspace_id,
                        "fallback_source": "SCOUT_WORKSPACE_ID|SCHEDULED_SCOUT_WORKSPACE",
                    },
                )
            else:
                workspace_resolution["resolution_error"] = workspace_resolution["resolution_error"] or {
                    "type": "WorkspaceResolutionFailed",
                    "message": "No workspace_id resolved from header, membership lookup, bootstrap, or env fallback.",
                }
                _log_write_stage(
                    "run_scout",
                    "workspace_unresolved",
                    {
                        "resolved_user_id": user_id,
                        "requested_workspace_id": requested_workspace_id,
                        "resolution_error": workspace_resolution["resolution_error"],
                    },
                )

        current_lat = body.current_lat if body else None
        current_lng = body.current_lng if body else None
        scan_settings = body.scan_settings if body and isinstance(body.scan_settings, dict) else {}
        payload = {
            "current_lat": current_lat,
            "current_lng": current_lng,
            "location_mode": "current" if current_lat is not None and current_lng is not None else "saved",
            "scan_settings": scan_settings,
            "workspace_resolution": workspace_resolution,
        }
        job_id = str(uuid4())
        job = {
            "id": job_id,
            "workspace_id": workspace_id,
            "type": "scout",
            "job_type": "scout",
            "status": "queued",
            "progress": 10,
            "payload": payload,
            "message": "Scout job queued",
            "result_summary": "Scout job queued",
            "stage": "queued",
            "error": None,
            "created_at": _job_now_iso(),
            "started_at": None,
            "finished_at": None,
        }
        _job_store(job)
        _upsert_job_supabase(job)
        print("  scout job created")
        _log_write_stage(
            "run_scout",
            "job_created",
            {"job_id": job_id, "workspace_id": workspace_id, "status": "queued"},
        )

        worker = threading.Thread(
            target=_execute_scout_job,
            args=(job_id, user_id, workspace_id, workspace_plan, current_lat, current_lng, scan_settings),
            daemon=True,
        )
        worker.start()
        return {
            "ok": True,
            "success": True,
            "job_id": job_id,
            "status": "queued",
            "progress": 10,
            "message": "Scout job queued",
            "poll_url": f"/job/{job_id}",
        }
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


@app.post("/outreach/send")
def post_outreach_send(request: Request, body: OutreachSendBody):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    recipient = (body.to or "").strip()
    subject = (body.subject or "").strip()
    content = (body.body or "").strip()
    message_type = (body.message_type or "short_email").strip().lower()
    if not recipient:
        raise HTTPException(status_code=400, detail="recipient email is missing")
    if not content:
        raise HTTPException(status_code=400, detail="message body is empty")

    crm_sb = _create_crm_client()
    if crm_sb is None:
        raise HTTPException(status_code=500, detail="CRM client is not configured")

    lead = _crm_fetch_lead(crm_sb, body.lead_id, user_id)
    if not lead:
        raise HTTPException(status_code=404, detail="lead not found")

    lead_status = str(lead.get("status") or "").strip().lower()
    if lead_status == "do_not_contact":
        raise HTTPException(status_code=400, detail="lead is marked do_not_contact")
    if lead_status == "closed_lost":
        raise HTTPException(status_code=400, detail="lead is marked closed_lost")

    if not subject:
        raise HTTPException(status_code=400, detail="email subject is required")

    workspace_id = (
        (body.workspace_id or "").strip()
        or str(lead.get("workspace_id") or "").strip()
        or _get_workspace_id_from_request(request)
    )
    if not workspace_id and _supabase_url and _supabase_service_key:
        try:
            from supabase import create_client
            sb = create_client(_supabase_url, _supabase_service_key)
            workspace_id = _resolve_workspace_id_for_user(sb, user_id, None) or ""
        except Exception:
            workspace_id = ""

    print("  sending outreach email")
    sequence_step_sent = None
    if bool(lead.get("sequence_active")):
        current_step = _sequence_step_from_lead(lead)
        if 1 <= current_step <= 3:
            sequence_step_sent = current_step
    result = _send_outreach_email_for_lead(
        crm_sb,
        lead=lead,
        owner_id=user_id,
        workspace_id=workspace_id or None,
        recipient=recipient,
        subject=subject,
        content=content,
        message_type=message_type,
        case_id=body.case_id,
        sequence_step_sent=sequence_step_sent,
    )
    if result.get("ok"):
        print("  outreach email sent")
        print("  outbound email logged")
        return result
    error_text = str(result.get("error") or "unknown send error")
    print(f"  outreach email failed: {error_text}", file=sys.stderr)
    print("  outbound email logged")
    if "email_provider_forbidden" in error_text:
        provider_reason = _provider_reason_from_error(Exception(error_text))
        raise HTTPException(
            status_code=403,
            detail=f"Email provider rejected the send: {provider_reason}",
        )
    raise HTTPException(status_code=500, detail=f"outreach_email_failed: {error_text}")


@app.get("/outreach/thread/{lead_id}")
def get_outreach_thread(request: Request, lead_id: str):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    crm_sb = _create_crm_client()
    if crm_sb is None:
        raise HTTPException(status_code=500, detail="CRM client is not configured")

    lead = _crm_fetch_lead(crm_sb, lead_id, user_id)
    if not lead:
        raise HTTPException(status_code=404, detail="lead not found")

    workspace_id = (
        str(lead.get("workspace_id") or "").strip()
        or _get_workspace_id_from_request(request)
        or None
    )
    try:
        q_threads = (
            crm_sb.table("email_threads")
            .select("*")
            .eq("lead_id", lead_id)
            .eq("owner_id", user_id)
            .order("last_message_at", desc=True)
            .limit(50)
        )
        if workspace_id:
            q_threads = q_threads.eq("workspace_id", workspace_id)
        threads = q_threads.execute().data or []
    except Exception:
        threads = []

    try:
        q_messages = (
            crm_sb.table("email_messages")
            .select("*")
            .eq("lead_id", lead_id)
            .eq("owner_id", user_id)
            .order("created_at", desc=False)
            .limit(500)
        )
        messages = q_messages.execute().data or []
    except Exception:
        messages = []

    return {
        "lead_id": lead_id,
        "threads": threads,
        "messages": messages,
    }


@app.post("/outreach/inbound")
def post_outreach_inbound(request: Request, body: InboundEmailBody):
    secret_header = (request.headers.get("x-inbound-email-secret") or "").strip()
    if INBOUND_EMAIL_WEBHOOK_SECRET and secret_header != INBOUND_EMAIL_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    crm_sb = _create_crm_client()
    if crm_sb is None:
        raise HTTPException(status_code=500, detail="CRM client is not configured")

    from_email = str(body.from_email or "").strip().lower()
    subject = (body.subject or "").strip()
    content = (body.body or "").strip()
    provider_message_id = _normalize_message_id(body.provider_message_id) or None
    provider_thread_id = str(body.provider_thread_id or "").strip() or None
    workspace_id = str(body.workspace_id or "").strip() or None
    received_at = (body.received_at or "").strip() or datetime.now(timezone.utc).isoformat()
    references = [str(r or "").strip() for r in (body.references or []) if str(r or "").strip()]
    in_reply_to = _normalize_message_id(body.in_reply_to)
    if in_reply_to:
        references.append(in_reply_to)
    normalized_refs = []
    for ref in references:
        normalized = _normalize_message_id(ref)
        if normalized:
            normalized_refs.append(normalized)
    references = list({r for r in normalized_refs if r})

    print("  inbound reply received")
    if not from_email or not content:
        raise HTTPException(status_code=400, detail="from_email and body are required")

    thread = _match_inbound_thread(
        crm_sb,
        workspace_id=workspace_id,
        from_email=from_email,
        subject=subject,
        provider_thread_id=provider_thread_id,
        references=references,
    )
    if not thread:
        print("  reply could not be matched")
        return {"ok": False, "matched": False, "reason": "reply could not be matched"}

    lead_id = str(thread.get("lead_id") or "").strip() or None
    owner_id = str(thread.get("owner_id") or "").strip() or None
    thread_id = str(thread.get("id") or "").strip() or None
    if not lead_id or not owner_id or not thread_id:
        print("  reply could not be matched")
        return {"ok": False, "matched": False, "reason": "reply could not be matched"}

    if provider_message_id:
        try:
            existing = (
                crm_sb.table("email_messages")
                .select("id")
                .eq("provider_message_id", provider_message_id)
                .limit(1)
                .execute()
            )
            if existing.data:
                print("  inbound reply duplicate ignored")
                _mark_lead_replied_after_inbound(crm_sb, lead_id)
                return {"ok": True, "matched": True, "duplicate_ignored": True, "lead_id": lead_id, "thread_id": thread_id}
        except Exception:
            pass

    _insert_email_message(
        crm_sb,
        {
            "thread_id": thread_id,
            "lead_id": lead_id,
            "direction": "inbound",
            "provider_message_id": provider_message_id,
            "subject": subject or thread.get("subject"),
            "body": content,
            "delivery_status": "received",
            "sent_at": None,
            "received_at": received_at,
            "owner_id": owner_id,
        },
    )
    try:
        crm_sb.table("email_threads").update(
            {
                "status": "active",
                "last_message_at": received_at,
                "provider_thread_id": provider_thread_id or thread.get("provider_thread_id"),
            }
        ).eq("id", thread_id).execute()
    except Exception:
        pass
    _mark_lead_replied_after_inbound(crm_sb, lead_id)
    print("  reply matched to lead")
    return {"ok": True, "matched": True, "lead_id": lead_id, "thread_id": thread_id}


@app.post("/outreach/generate-email")
def post_outreach_generate_email(request: Request, body: OutreachGenerateEmailBody):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    print("  loading dossier outreach template")
    linked_opportunity_id = (body.linked_opportunity_id or "").strip()
    workspace_id = (body.workspace_id or "").strip() or _get_workspace_id_from_request(request)

    crm_sb = _create_crm_client()
    if body.lead_id and not linked_opportunity_id and crm_sb is not None:
        lead = _crm_fetch_lead(crm_sb, body.lead_id, user_id)
        if lead:
            linked_opportunity_id = str(lead.get("linked_opportunity_id") or "").strip()
            workspace_id = workspace_id or str(lead.get("workspace_id") or "").strip()

    if not linked_opportunity_id:
        raise HTTPException(status_code=400, detail="linked_opportunity_id is required")
    if not _supabase_url or not _supabase_service_key:
        raise HTTPException(status_code=500, detail="Supabase backend is not configured")

    try:
        from supabase import create_client

        sb = create_client(_supabase_url, _supabase_service_key)
        opportunity = {}
        case_file = {}

        opp_queries = [
            lambda: (
                sb.table("opportunities")
                .select(
                    "id,business_name,category,city,address,website,opportunity_score,website_status,"
                    "website_speed,mobile_ready,seo_score,website_quality_score,opportunity_reason,lead_bucket,close_probability"
                )
                .eq("id", linked_opportunity_id)
                .eq("workspace_id", workspace_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("opportunities")
                .select(
                    "id,business_name,category,city,address,website,opportunity_score,website_status,"
                    "website_speed,mobile_ready,seo_score,website_quality_score,opportunity_reason,lead_bucket,close_probability"
                )
                .eq("id", linked_opportunity_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("opportunities")
                .select(
                    "id,business_name,category,city,address,website,opportunity_score,website_status,"
                    "website_speed,mobile_ready,seo_score,website_quality_score,lead_bucket,close_probability"
                )
                .eq("id", linked_opportunity_id)
                .eq("workspace_id", workspace_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("opportunities")
                .select(
                    "id,business_name,category,city,address,website,opportunity_score,website_status,"
                    "website_speed,mobile_ready,seo_score,website_quality_score,lead_bucket,close_probability"
                )
                .eq("id", linked_opportunity_id)
                .limit(1)
                .execute()
            ),
        ]
        for q in opp_queries:
            try:
                res = q()
                rows = res.data or []
                if rows:
                    opportunity = rows[0]
                    break
            except Exception:
                continue

        case_queries = [
            lambda: (
                sb.table("case_files")
                .select(
                    "opportunity_id,mobile_score,performance_score,seo_score,missing_ssl,slow_load,mobile_layout_issue,"
                    "website_speed,homepage_load_seconds,mobile_ready,ssl_ok,missing_meta_title,missing_meta_description,"
                    "audit_issues,audit_results,contact_form_present,phone_from_site,outdated_design_clues,desktop_screenshot_url,mobile_screenshot_url,contact_page_screenshot_url,internal_screenshot_url,"
                    "screenshot_url"
                )
                .eq("opportunity_id", linked_opportunity_id)
                .eq("workspace_id", workspace_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("case_files")
                .select(
                    "opportunity_id,mobile_score,performance_score,seo_score,missing_ssl,slow_load,mobile_layout_issue,"
                    "website_speed,homepage_load_seconds,mobile_ready,ssl_ok,missing_meta_title,missing_meta_description,"
                    "audit_issues,audit_results,contact_form_present,phone_from_site,outdated_design_clues,desktop_screenshot_url,mobile_screenshot_url,contact_page_screenshot_url,internal_screenshot_url,"
                    "screenshot_url"
                )
                .eq("opportunity_id", linked_opportunity_id)
                .limit(1)
                .execute()
            ),
            lambda: (
                sb.table("case_files")
                .select(
                    "opportunity_id,mobile_score,performance_score,seo_score,missing_ssl,slow_load,mobile_layout_issue,"
                    "website_speed,homepage_load_seconds,mobile_ready,ssl_ok,missing_meta_title,missing_meta_description,"
                    "audit_issues,outdated_design_clues,desktop_screenshot_url,mobile_screenshot_url,contact_page_screenshot_url,internal_screenshot_url,"
                    "screenshot_url"
                )
                .eq("opportunity_id", linked_opportunity_id)
                .limit(1)
                .execute()
            ),
        ]
        for q in case_queries:
            try:
                res = q()
                rows = res.data or []
                if rows:
                    case_file = rows[0]
                    break
            except Exception:
                continue

        lead_row = None
        if body.lead_id and crm_sb is not None:
            lead_row = _crm_fetch_lead(crm_sb, body.lead_id, user_id)
        generated = generate_outreach_email(
            {
                "opportunity": opportunity or {},
                "case_file": case_file or {},
                "contact_name": (lead_row or {}).get("contact_name"),
            }
        )
        draft_message_id = None
        draft_thread_id = None
        if body.lead_id and crm_sb is not None and lead_row:
            contact_email = str(lead_row.get("email") or "").strip().lower()
            if contact_email:
                thread = _upsert_email_thread(
                    crm_sb,
                    workspace_id=workspace_id or str(lead_row.get("workspace_id") or "").strip() or None,
                    lead_id=body.lead_id,
                    contact_email=contact_email,
                    subject=generated.get("subject"),
                    provider_thread_id=None,
                    owner_id=user_id,
                )
                draft_thread_id = str(thread.get("id") or "").strip() if thread else ""
                if not draft_thread_id:
                    draft_thread_id = None
            draft_row = _insert_email_message(
                crm_sb,
                {
                    "thread_id": draft_thread_id,
                    "lead_id": body.lead_id,
                    "direction": "outbound",
                    "provider_message_id": None,
                    "subject": generated.get("subject"),
                    "body": generated.get("body"),
                    "delivery_status": "queued",
                    "status": "draft",
                    "generated_by": "scout-brain",
                    "sent_at": None,
                    "received_at": None,
                    "owner_id": user_id,
                },
            )
            if draft_row:
                draft_message_id = draft_row.get("id")
        print("  outreach template loaded")
        return {
            "linked_opportunity_id": linked_opportunity_id,
            "subject": generated.get("subject"),
            "body": generated.get("body"),
            "issues": generated.get("issues") or [],
            "screenshot_url": generated.get("screenshot_url"),
            "draft_message_id": draft_message_id,
            "draft_thread_id": draft_thread_id,
            "metadata": {
                "business_name": (opportunity or {}).get("business_name"),
                "category": (opportunity or {}).get("category"),
                "city": (opportunity or {}).get("city") or (opportunity or {}).get("address"),
                "website": (opportunity or {}).get("website"),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"outreach_generate_failed: {e}")


@app.post("/outreach/template")
def post_outreach_template(request: Request, body: OutreachTemplateBody):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    linked_opportunity_id = (body.linked_opportunity_id or "").strip()
    workspace_id = (body.workspace_id or "").strip() or _get_workspace_id_from_request(request)

    crm_sb = _create_crm_client()
    if body.lead_id and not linked_opportunity_id and crm_sb is not None:
        lead = _crm_fetch_lead(crm_sb, body.lead_id, user_id)
        if lead:
            linked_opportunity_id = str(lead.get("linked_opportunity_id") or "").strip()
            workspace_id = workspace_id or str(lead.get("workspace_id") or "").strip()

    if not linked_opportunity_id:
        raise HTTPException(
            status_code=400,
            detail="linked_opportunity_id (or lead with linked_opportunity_id) is required",
        )

    print("  loading dossier outreach template")
    template = _load_outreach_template_for_opportunity(
        linked_opportunity_id, workspace_id=workspace_id or None
    )
    if not template:
        raise HTTPException(status_code=404, detail="Outreach template not found")

    print("  outreach template loaded")
    return template


@app.post("/outreach/regenerate")
def post_outreach_regenerate(request: Request, body: OutreachRegenerateBody):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    linked_opportunity_id = (body.linked_opportunity_id or "").strip()
    workspace_id = (body.workspace_id or "").strip() or _get_workspace_id_from_request(request)

    crm_sb = _create_crm_client()
    if body.lead_id and not linked_opportunity_id and crm_sb is not None:
        lead = _crm_fetch_lead(crm_sb, body.lead_id, user_id)
        if lead:
            linked_opportunity_id = str(lead.get("linked_opportunity_id") or "").strip()
            workspace_id = workspace_id or str(lead.get("workspace_id") or "").strip()

    if not linked_opportunity_id:
        raise HTTPException(
            status_code=400,
            detail="linked_opportunity_id (or lead with linked_opportunity_id) is required",
        )

    print("  regenerating personalized outreach pack")
    template = _load_outreach_template_for_opportunity(
        linked_opportunity_id,
        workspace_id=workspace_id or None,
        regenerate=True,
    )
    if not template:
        raise HTTPException(status_code=404, detail="Outreach template not found")
    print("  personalized outreach pack regenerated")
    return template


@app.get("/outreach/email-diagnostics")
def get_outreach_email_diagnostics(request: Request):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    sender_cfg = _email_sender_config()
    return {
        "ok": True,
        "sender_email": sender_cfg.get("from_email") or None,
        "sender_name": sender_cfg.get("from_name") or None,
        "sender_source": sender_cfg.get("sender_source"),
        "has_resend_api_key": bool(sender_cfg.get("has_resend_api_key")),
        "has_outreach_from_email": bool(sender_cfg.get("has_outreach_from_email")),
        "has_resend_from_email": bool(sender_cfg.get("has_resend_from_email")),
        "priority_rule": "OUTREACH_FROM_EMAIL overrides RESEND_FROM_EMAIL when both are set",
        "routes_using_sender": [
            "/outreach/send",
            "/outreach/test",
        ],
        "shared_sender_helper": "_send_resend_email",
        "last_provider_diagnostic": _last_email_provider_diag or None,
    }


@app.post("/outreach/test")
def post_outreach_test(request: Request, body: OutreachTestBody):
    print("  backend /outreach/test entered")
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    recipient = (body.to or "").strip()
    if not recipient:
        raise HTTPException(status_code=400, detail="recipient email is missing")

    subject = (body.subject or "").strip() or "Scout-Brain Email Test"
    content = (
        (body.body or "").strip()
        or "This is a Scout-Brain email smoke test from MixedMakerShop admin."
    )

    print("  sending test email")
    try:
        send_result = _send_resend_email(recipient, subject, content)
        print("  test email sent")
        return {
            "ok": True,
            "provider_message_id": send_result.get("provider_message_id"),
        }
    except Exception as e:
        print(f"  test email failed: {e}", file=sys.stderr)
        msg = str(e)
        if "email_provider_forbidden" in msg:
            provider_reason = _provider_reason_from_error(e)
            raise HTTPException(
                status_code=403,
                detail=f"Email provider rejected the send: {provider_reason}",
            )
        raise HTTPException(status_code=500, detail=f"test_email_failed: {e}")


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


@app.get("/workspace/plan")
def get_workspace_plan(request: Request):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not _supabase_url or not _supabase_service_key:
        return {
            "plan": "free",
            "limits": _plan_limits("free"),
            "usage": {"monthly_scout_runs": 0, "saved_leads": 0},
        }
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        requested_workspace = _get_workspace_id_from_request(request)
        workspace = _get_workspace_for_user(sb, user_id, requested_workspace)
        plan = _normalize_plan(workspace.get("plan"))
        usage = _get_workspace_usage(sb, workspace.get("id"))
        limits = _plan_limits(plan)
        return {
            "workspace_id": workspace.get("id"),
            "workspace_name": workspace.get("name"),
            "plan": plan,
            "limits": limits,
            "usage": usage,
            "can_run_scout": _check_plan_limits_for_run(plan, usage)[0],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load workspace plan: {e}")


@app.get("/scout-summary")
def get_scout_summary(request: Request):
    user_id = _get_user_id_from_request(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not _supabase_url or not _supabase_service_key:
        return {
            "last_run_time": None,
            "leads_found_today": 0,
            "top_opportunities_count": 0,
            "followups_due": 0,
            "today_businesses_discovered": 0,
            "today_analyzed_total": 0,
            "today_high_opportunity_total": 0,
            "total_businesses_scanned": 0,
            "businesses_without_websites": 0,
            "weak_websites_detected": 0,
            "top_opportunities": [],
            "dashboard_businesses_discovered": 0,
            "dashboard_websites_audited": 0,
            "dashboard_high_opportunities": 0,
            "dashboard_outreach_sent": 0,
        }
    try:
        from supabase import create_client
        sb = create_client(_supabase_url, _supabase_service_key)
        requested_workspace = _get_workspace_id_from_request(request)
        workspace = _get_workspace_for_user(sb, user_id, requested_workspace)
        workspace_id = workspace.get("id")

        last_run_time = None
        if workspace_id:
            try:
                runs = (
                    sb.table("scout_runs")
                    .select("run_time,created_at")
                    .eq("workspace_id", workspace_id)
                    .order("run_time", desc=True)
                    .limit(1)
                    .execute()
                )
            except Exception:
                runs = (
                    sb.table("scout_runs")
                    .select("created_at")
                    .eq("workspace_id", workspace_id)
                    .order("created_at", desc=True)
                    .limit(1)
                    .execute()
                )
            if runs.data:
                last_run_time = runs.data[0].get("run_time") or runs.data[0].get("created_at")

        leads_found_today = _count_new_leads_today(sb, workspace_id)
        top_opps = getTopOpportunities(workspace_id)
        top_opportunities_count = len(top_opps)
        followups_due = _count_followups_due(sb, workspace_id)
        today_businesses_discovered = 0
        today_analyzed_total = 0
        today_high_opportunity_total = 0
        today_weak_websites = 0
        today_no_website = 0
        if workspace_id:
            today_key = datetime.now().date().isoformat()
            try:
                day_runs = (
                    sb.table("scout_runs")
                    .select("run_date,businesses_discovered,analyzed_total,high_opportunity_total,weak_websites,no_website,created_at")
                    .eq("workspace_id", workspace_id)
                    .order("created_at", desc=True)
                    .limit(20)
                    .execute()
                )
                for row in day_runs.data or []:
                    row_date = (row.get("run_date") or "").strip()
                    if not row_date:
                        created_at = (row.get("created_at") or "").strip()
                        if created_at:
                            try:
                                row_date = datetime.fromisoformat(created_at.replace("Z", "+00:00")).date().isoformat()
                            except Exception:
                                row_date = ""
                    if row_date != today_key:
                        continue
                    today_businesses_discovered += int(row.get("businesses_discovered") or 0)
                    today_analyzed_total += int(row.get("analyzed_total") or 0)
                    today_high_opportunity_total += int(row.get("high_opportunity_total") or 0)
                    today_weak_websites += int(row.get("weak_websites") or 0)
                    today_no_website += int(row.get("no_website") or 0)
            except Exception:
                pass

        dashboard_businesses_discovered = 0
        dashboard_websites_audited = 0
        dashboard_high_opportunities = 0
        dashboard_outreach_sent = 0
        if workspace_id:
            try:
                from datetime import timedelta
                since_iso = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
                recent_runs = (
                    sb.table("scout_runs")
                    .select("businesses_discovered,created_at,leads_found")
                    .eq("workspace_id", workspace_id)
                    .gte("created_at", since_iso)
                    .execute()
                )
                for row in recent_runs.data or []:
                    dashboard_businesses_discovered += int(row.get("businesses_discovered") or row.get("leads_found") or 0)
            except Exception:
                pass
            try:
                audited_rows = (
                    sb.table("case_files")
                    .select("id,website_score,audit_issues,status,outreach_notes")
                    .eq("workspace_id", workspace_id)
                    .execute()
                )
                for row in audited_rows.data or []:
                    has_audit = row.get("website_score") is not None or bool(row.get("audit_issues"))
                    if has_audit:
                        dashboard_websites_audited += 1
                    status = str(row.get("status") or "").strip().lower()
                    if status in {"ready to contact", "contacted", "follow up", "queued"} or bool(row.get("outreach_notes")):
                        dashboard_outreach_sent += 1
            except Exception:
                pass
            try:
                opp_rows = (
                    sb.table("opportunities")
                    .select("opportunity_score,internal_score")
                    .eq("workspace_id", workspace_id)
                    .execute()
                )
                for row in opp_rows.data or []:
                    score = row.get("opportunity_score")
                    if score is None:
                        score = row.get("internal_score")
                    try:
                        if float(score or 0) >= 70:
                            dashboard_high_opportunities += 1
                    except Exception:
                        continue
            except Exception:
                pass

        return {
            "last_run_time": last_run_time,
            "leads_found_today": leads_found_today,
            "top_opportunities_count": top_opportunities_count,
            "followups_due": followups_due,
            "today_businesses_discovered": today_businesses_discovered,
            "today_analyzed_total": today_analyzed_total,
            "today_high_opportunity_total": today_high_opportunity_total,
            "total_businesses_scanned": int(today_businesses_discovered),
            "businesses_without_websites": int(today_no_website),
            "weak_websites_detected": int(today_weak_websites),
            "top_opportunities": top_opps[:10],
            "dashboard_businesses_discovered": dashboard_businesses_discovered,
            "dashboard_websites_audited": dashboard_websites_audited,
            "dashboard_high_opportunities": dashboard_high_opportunities,
            "dashboard_outreach_sent": dashboard_outreach_sent,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load scout summary: {e}")


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


def _screenshot_file_for_case(slug: str, kind: str) -> Path | None:
    slug_safe = slug.strip()
    if not slug_safe:
        return None
    folder = CASE_FILES_DIR / slug_safe
    mapping = {
        "desktop_homepage": "desktop.png",
        "mobile_homepage": "mobile.png",
        "contact_page": "internal.png",
        "key_internal_page": "internal.png",
    }
    filename = mapping.get(kind)
    if not filename:
        return None
    path = folder / filename
    return path if path.exists() else None


@app.get("/case/{slug}")
def get_case_raw(slug: str):
    """Return raw case JSON for a lead. Used for debug / View raw case JSON."""
    path = CASES_DIR / f"{slug}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Case {slug} not found")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@app.get("/case/{slug}/screenshot/{kind}")
def get_case_screenshot(slug: str, kind: str):
    path = _screenshot_file_for_case(slug, kind)
    if not path:
        raise HTTPException(status_code=404, detail="Screenshot not found")
    return FileResponse(path)


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
def post_case_regenerate_outreach(request: Request, slug: str):
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
        workspace_plan = "free"
        user_id = _get_user_id_from_request(request)
        requested_workspace_id = _get_workspace_id_from_request(request)
        if user_id and _supabase_url and _supabase_service_key:
            try:
                from supabase import create_client
                sb = create_client(_supabase_url, _supabase_service_key)
                ws = _get_workspace_for_user(sb, user_id, requested_workspace_id)
                workspace_plan = _normalize_plan(ws.get("plan"))
            except Exception:
                workspace_plan = "free"
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)
                city_hint = cfg.get("home_city")
        pack = generate_outreach_pack(case, city_hint=city_hint)
        pack = _apply_outreach_plan_limits(pack, workspace_plan)
        if not _plan_limits(workspace_plan).get("full_outreach", True):
            print("  plan limit reached")
            print("  upgrade prompt shown")
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
        if SCHEDULED_SCOUT_TIMEZONE.lower() == "local":
            _scheduler = BackgroundScheduler()
            trigger = CronTrigger(hour=SCHEDULED_SCOUT_HOUR, minute=0)
            tz_label = "local"
        else:
            _scheduler = BackgroundScheduler(timezone=SCHEDULED_SCOUT_TIMEZONE)
            trigger = CronTrigger(hour=SCHEDULED_SCOUT_HOUR, minute=0, timezone=SCHEDULED_SCOUT_TIMEZONE)
            tz_label = SCHEDULED_SCOUT_TIMEZONE
        _scheduler.add_job(
            daily_scout_job,
            trigger,
            id=SCHEDULED_SCOUT_JOB_NAME,
            replace_existing=True,
        )
        _scheduler.start()
        print(
            f"  Scheduled scout '{SCHEDULED_SCOUT_JOB_NAME}' enabled at "
            f"{SCHEDULED_SCOUT_HOUR:02d}:00 {tz_label}"
        )
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
    _log_runtime_config("startup")
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
    print(f"  SCHEDULED_SCOUT_JOB_NAME: {SCHEDULED_SCOUT_JOB_NAME}")
    print(f"  SCHEDULED_SCOUT_HOUR: {SCHEDULED_SCOUT_HOUR:02d}:00")
    print(f"  SCHEDULED_SCOUT_TIMEZONE: {SCHEDULED_SCOUT_TIMEZONE}")
    print(f"  SCHEDULED_SCOUT_SCOPE: {SCHEDULED_SCOUT_SCOPE}")
    if SCHEDULED_SCOUT_SCOPE == "internal":
        print(f"  SCHEDULED_SCOUT_WORKSPACE: {SCHEDULED_SCOUT_WORKSPACE_NAME}")
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
