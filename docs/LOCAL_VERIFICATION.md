# Local verification checklist

Each step lists the command (or PyCharm run configuration from `.run/`) and what you should see.
Numbers are from the reference run in `reports/`; timings vary by machine.

> **Order matters.** `run --method legacy` and `run --method improved` write the same warehouse
> tables. Run **improved last**, because the publisher and dashboard serve whatever ran last.
> Each run also overwrites `reports/run_<method>.json`; use `git restore reports` to go back to
> the committed reference numbers.

| # | Command / run configuration | Expected |
|---|---|---|
| 1 | `pip install -r requirements-dev.txt && pip install -e . --no-deps` | `nem-pipeline --help` lists `run, extract, publish, benchmark, dashboard` |
| 2 | **0 All tests** — `pytest --cov` | `124 passed`, `TOTAL … 97%` |
| 3 | `ruff check src tests && ruff format --check src tests` | `All checks passed!` and `… files already formatted` |
| 4 | Copy `.env.example` to `.env`, set `APP_OPENELECTRICITY_API_KEY` | — |
| 5 | **1 Run pipeline (legacy method)** — `nem-pipeline run --method legacy` | First run: ~22 API requests and geocoding at 1 req/s (several minutes). Summary JSON with `"facilities": 486`, `"facility_metric_rows": 568237`, `"map_coverage": 0.3168…` |
| 6 | **2 Run pipeline (improved method)** — `nem-pipeline run --method improved` | `"map_coverage": 0.9958…`; from cache the whole run takes about 4 s |
| 7 | Open `reports/run_improved.json` | `matching.legacy_fuzzy.precision ≈ 0.173`, `matching.improved_fuzzy.precision ≈ 0.754`, `diagnostics.unit_overwrite.wrong_rate ≈ 0.197`, `diagnostics.positional_pairing.misaligned_rows = 0` |
| 8 | `docker compose up -d broker` | `docker compose ps` shows the broker `(healthy)` |
| 9 | **3 Publish to MQTT** — `nem-pipeline publish --cycles 0` | `Publishing to localhost:1883 (nem/facilities/metrics, nem/market/metrics)`; stop with Ctrl+C → `Published N messages …` |
| 10 | **4 Dashboard** (while 9 is running) — open http://localhost:8501 | Headers change from "waiting for data" to a 2025-10-0x timestamp within a few seconds; about 480 coloured points on the map; market table with 5 regions |
| 11 | `docker compose exec broker mosquitto_pub -t nem/facilities/metrics -m 'not json'` | Sidebar "Rejected messages" goes up by 1; the dashboard keeps updating |
| 12 | **5 Benchmark publish** — `nem-pipeline benchmark --intervals 2016 --qos 1` | `"lost": 0`, p50/p95 latency in single-digit milliseconds |
| 13 | `docker compose down && docker compose up --build` | `pipeline` exits 0, then `publisher` and `dashboard` start; `docker compose exec dashboard whoami` → `app` |
| 14 | `git status` | If step 13 or 5–6 rewrote `reports/`, run `git restore reports` |
