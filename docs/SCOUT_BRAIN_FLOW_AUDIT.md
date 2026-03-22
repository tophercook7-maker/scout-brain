# Scout Brain — flow audit & enrichment insertion point

## Current roles (summary)

| File | Role |
|------|------|
| **`scout/places_client.py`** | Google **Places API (New)** Text Search, Place Details, optional Geocoding. Produces normalized place dicts (`name`, `address`, `phone`, `website`, `place_id`, reviews metadata). **Primary discovery/matching** for city/category scout runs. |
| **`scout/investigator.py`** | **Deep website research**: fetch homepage, optional internal paths (`CRAWL_PATHS`), extract emails (with source ranking), phones, socials, contact page, owner hints, then **`auditWebsite()`** for scores/issues. Used heavily by **Morning Runner** after a URL is known. |
| **`scout/audit.py`** | **Lightweight HTML audit** (`analyze_html`, `fetch_and_audit`) — viewport, platform clues, CTA/contact heuristics. Used by app `/audit` and feeds investigator’s scoring path. |
| **`scout/outreach_generator.py`** | Builds **outreach packs** (email/DM text) from an **existing case dict**; does not discover businesses. |
| **`scout/control_server.py`** | Small **local** HTTP server (`8766`): `/status`, `/scout-data`, `/run-scout` → shell runner + JSON files. Not the main Railway API. |
| **`app.py`** | **FastAPI** main app: `/run-scout`, `/scout-data`, `/audit`, `/case/*`, Supabase sync, jobs, outreach, **`/api/enrich-lead`** (new). **Reduced-mode enrichment** (`_run_reduced_mode_enrichment`) fills gaps on existing opportunity rows when Places is off. |

## Where things happen today

- **Discovery:** `places_client.search_places` / `text_search_new` from **Morning Runner** (`scout/morning_runner.py`) and `_execute_scout_job` in `app.py`.
- **Google / Places matching:** `places_client.py` only.
- **Opportunity scoring:** Case pipeline combines investigator + audit signals into `website_score`, `opportunity_score`, tiers — see morning runner + case assembly (not a single small function).
- **Cases / opportunities written:** `scout/cases/{slug}.json`, `scout/opportunities.json`, `scout/today.json`, plus **Supabase** sync in `app.py` (`_sync_scout_to_supabase`, `_upsert_case_file_row`, etc.).
- **Prior “enrichment”:** Investigator on a known URL; reduced-mode row patches in app when Places unavailable — **no** dedicated partial-lead → normalized CRM payload until **`lead_enrichment_pipeline.py`**.

## Best insertion point for the new pipeline

**`scout/lead_enrichment_pipeline.py`** — orchestrates:

1. Places text search + match confidence (reuse `places_client`).
2. Website pass via **`investigate()`** when a standalone URL exists (reuse investigator; optional deep crawl via `SCOUT_ENRICH_CRAWL_INTERNAL`).
3. Tags + web-design score + `why` / `best_next_move` / `pitch_angle` (reuse `web_design_classify.py`).

**API surface:** `app.py` → `POST /api/enrich-lead` calls `run_lead_enrichment()` so CRM and extensions can hit one stable contract without touching case slug lifecycle.

## Isolation from `next-app`

This project keeps **its own** `supabase/migrations/`. Do not run `next-app` Supabase CLI against this tree. Enrichment logs go to `scout/data/enrich_log.jsonl` only unless you explicitly extend Supabase writes later.
