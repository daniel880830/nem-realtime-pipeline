# Original scripts (archived)

The two scripts the project started from, kept unchanged except that the two hard-coded API
keys were replaced by `<REDACTED ...>` placeholders.

| File | What it did |
|---|---|
| `original_pipeline.py` | Scraped and geocoded CER stations into DuckDB (Google Maps), pulled one week of OpenElectricity data, cleaned it into CSVs, fuzzy-joined facilities to stations and published both streams to a public MQTT broker. Everything ran at import time. |
| `original_dashboard.py` | Streamlit + pydeck dashboard subscribed to the facility topic; market data was read from the CSV. |

They are **not** used by the package. Their logic is reproduced faithfully (and tested against
a verbatim copy where it matters) by the `*_legacy` functions in `src/nem_pipeline`, so that
`nem-pipeline run --method legacy` can be compared with `--method improved` on the same data.
The original used Google Maps for geocoding; the reproduction uses OpenStreetMap Nominatim with
the identical query string.
