#!/usr/bin/env python3
"""
Massive Brain - single local app (FastAPI).

One command: python3 app.py
Opens http://localhost:8760 in the browser.

- Serves UI at / from ui/
- GET /scout-data, POST /run-scout, POST /audit
- Reads/writes scout/config.json, scout/history.json, scout/opportunities.json, scout/today.json
- Loads GOOGLE_MAPS_API_KEY from .env (backend only)
"""
from pathlib import Path
import json
import os
import sys
import webbrowser
import threading
import time
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
try:
    from dotenv import load_dotenv
    load_dotenv(SCOUT_DIR / ".env")
    load_dotenv(ENV_PATH)
    _maps_key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    _supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    _supabase_service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    _supabase_jwt_secret = os.environ.get("SUPABASE_JWT_SECRET", "").strip()
    _env_loaded = True
except ImportError:
    pass

app = FastAPI(title="Massive Brain", version="2.0")

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


def _sync_scout_to_supabase(user_id: str) -> None:
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
        for opp_ui in opportunities_ui:
            slug = opp_ui.get("slug") or opp_ui.get("id")
            name = (opp_ui.get("name") or opp_ui.get("business_name") or "").strip()
            if not name:
                continue
            opp_row = {
                "user_id": user_id,
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
            ins = sb.table("opportunities").insert(opp_row).execute()
            if not ins.data or len(ins.data) == 0:
                continue
            opp_id = ins.data[0]["id"]
            case_path = CASES_DIR / f"{slug}.json"
            if case_path.exists():
                with open(case_path, encoding="utf-8") as f:
                    case = json.load(f)
                cf_row = {
                    "opportunity_id": opp_id,
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
                sb.table("case_files").insert(cf_row).execute()
    except Exception as e:
        print(f"  [Scout] Supabase sync error: {e}", file=sys.stderr)
        raise


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

@app.get("/", response_class=HTMLResponse)
def serve_app():
    return _serve_frontend_index()


# Serve compiled Vite assets in production.
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
        if user_id and _supabase_url and _supabase_service_key:
            _sync_scout_to_supabase(user_id)
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


class AuditBody(BaseModel):
    url: str


class CaseUpdateBody(BaseModel):
    status: str | None = None
    first_contacted_at: str | None = None
    last_contacted_at: str | None = None
    follow_up_due: str | None = None
    outcome: str | None = None
    outreach_notes: str | None = None


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
