# Scout & Morning Runner

**Single config:** `config.json` in this folder drives both the in-app **Scout Console** and the **Morning Runner** script. Edit the JSON to change city, radius, categories, or chain filtering — no code changes required.

## config.json

- **home_city** — Base city to search from (e.g. `"Hot Springs, Arkansas"`).
- **search_radius_miles** — How far out to search (used by Morning Runner; future real search will respect this).
- **categories** — Business types to look for (e.g. `["coffee shop", "diner", "church"]`).
- **max_results_per_category** — Max leads per category.
- **ignore_chains** — When `true`, skip likely chain businesses.

## Scout Console (in app)

1. Open **Scout Console** in the app.
2. Config is loaded from `scout/config.json` automatically (served by your HTTP server).
3. City is pre-filled from `home_city`. Optionally type a single category in the second field to override and search only that type.
4. Click **Scan For Opportunities** — results use all categories from config (or the override), limited by `max_results_per_category`, with chains filtered when `ignore_chains` is true.

## Morning Runner (CLI)

From the **scout-brain** folder:

```bash
cd scout && python3 morning_runner.py
```

Or from repo root:

```bash
python3 scout-brain/scout/morning_runner.py
```

Reads `scout/config.json` at runtime and prints a run summary plus JSON (config + leads) for future integration (e.g. real Maps API, import into Brain).
