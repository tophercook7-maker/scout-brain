# Scout Brain — `POST /api/enrich-lead`

Lead normalization, Google Places matching (when configured), website investigation, web-design scoring, and next-step hints. Intended for **future** MixedMaker CRM (`next-app`) calls — this repo stays the source of truth for intelligence.

## Endpoint

- **Method:** `POST`
- **Path:** `/api/enrich-lead`
- **Content-Type:** `application/json`

## Auth (optional)

`source_type` may be `extension`, `facebook`, `google`, `manual`, `unknown`, or `mixed` (e.g. CRM `scout_mixed`).

If `SCOUT_ENRICH_API_KEY` is set in the environment, every request must send:

```http
X-Scout-Enrich-Key: <same value as SCOUT_ENRICH_API_KEY>
```

If the env var is **unset**, the endpoint is open (use only behind a private network or reverse-proxy auth).

## Request body

| Field | Type | Required | Notes |
|--------|------|----------|--------|
| `business_name` | string | recommended | |
| `city` | string | optional | Improves Places bias via geocode |
| `state` | string | optional | e.g. `AR` |
| `source_url` | string | optional | Capture URL; Facebook or normal site |
| `facebook_url` | string | optional | Explicit Facebook page |
| `source_type` | string | optional | `extension` \| `facebook` \| `google` \| `manual` \| `unknown` (default `unknown`) |

Example:

```json
{
  "business_name": "Main Street Coffee",
  "city": "Hot Springs",
  "state": "AR",
  "source_url": "https://www.facebook.com/...",
  "facebook_url": "",
  "source_type": "facebook"
}
```

## Success response

```json
{
  "ok": true,
  "enriched_lead": {
    "business_name": "...",
    "source_type": "facebook",
    "source_url": "...",
    "facebook_url": "...",
    "website": "...",
    "normalized_website": "...",
    "phone": "...",
    "email": "...",
    "email_source": "contact_page",
    "contact_page": "...",
    "city": "...",
    "state": "...",
    "category": "...",
    "tags": ["facebook_only", "no_website_opportunity", "..."],
    "score": 85,
    "why_this_lead_is_here": "...",
    "best_contact_method": "facebook",
    "best_next_move": "message on Facebook",
    "pitch_angle": "...",
    "source_confidence": 0.62,
    "match_confidence": 0.71,
    "raw_signals": { "steps": ["geocoded_city", "places_text_search", "investigate_website"] },
    "place_id": "places/ChIJ..."
  }
}
```

## Error response

HTTP 500 with JSON:

```json
{
  "ok": false,
  "error": "message",
  "enriched_lead": null
}
```

## Environment

| Variable | Effect |
|----------|--------|
| `GOOGLE_MAPS_API_KEY` | Places Text Search + optional geocode (`SCOUT_ENABLE_GEOCODING`, `SCOUT_ENABLE_PLACES`) |
| `SCOUT_ENRICH_CRAWL_INTERNAL` | `true` — deeper `investigate()` crawl (slower, richer email/contact) |
| `SCOUT_ENRICH_TIMEOUT` | Investigator fetch timeout seconds (default `12`) |
| `SCOUT_VERBOSE_LOGS` | Log enrichment steps to stdout |

## Persistence (Brain-only)

Append-only log: `scout/data/enrich_log.jsonl` (created automatically). Does **not** modify `scout/cases/*.json` or opportunities flow.

## CRM integration (next steps)

1. From `next-app`, `POST` to the deployed Scout Brain base URL + `/api/enrich-lead`.
2. Map `enriched_lead` fields into `leads` / `crm-lead-schema` whitelists (already partially aligned in next-app).
3. Optionally set `SCOUT_ENRICH_API_KEY` and store it as a server secret in the Next.js environment.
