"""Geocoding with OpenStreetMap Nominatim (rate-limited and cached on disk)."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import requests

from nem_pipeline.config import GeocodeConfig

logger = logging.getLogger(__name__)

Coordinates = tuple[float, float]


class NominatimGeocoder:
    """Free-text geocoder honouring the Nominatim usage policy.

    The policy requires an identifying User-Agent, at most one request per second and
    caching of results. Misses are cached too, so a re-run sends no repeated queries.
    """

    def __init__(
        self,
        cfg: GeocodeConfig,
        user_agent: str,
        cache_path: Path | None = None,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._cfg = cfg
        self._session = session or requests.Session()
        self._session.headers["User-Agent"] = user_agent
        self._cache_path = cache_path
        self._cache: dict[str, Coordinates | None] = self._load_cache()
        self._sleep = sleep
        self._clock = clock
        self._last_request: float | None = None
        self.requests_sent = 0

    def _load_cache(self) -> dict[str, Coordinates | None]:
        if self._cache_path is None or not self._cache_path.exists():
            return {}
        try:
            raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Ignoring corrupt geocode cache %s", self._cache_path)
            return {}
        return {k: (tuple(v) if v else None) for k, v in raw.items()}

    def save_cache(self) -> None:
        """Persist the cache to disk."""
        if self._cache_path is None:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_text(
            json.dumps(self._cache, indent=1, sort_keys=True), encoding="utf-8"
        )

    def _throttle(self) -> None:
        if self._last_request is not None:
            wait = self._cfg.min_interval_s - (self._clock() - self._last_request)
            if wait > 0:
                self._sleep(wait)
        self._last_request = self._clock()

    def geocode(self, query: str) -> Coordinates | None:
        """Return ``(latitude, longitude)`` for ``query`` within Australia, or ``None``."""
        query = " ".join(query.split())
        if not query:
            return None
        if query in self._cache:
            return self._cache[query]
        self._throttle()
        self.requests_sent += 1
        try:
            response = self._session.get(
                self._cfg.endpoint,
                params={"q": query, "format": "jsonv2", "limit": 1, "countrycodes": "au"},
                timeout=self._cfg.timeout_s,
            )
            response.raise_for_status()
            hits = response.json()
        except (requests.RequestException, ValueError) as exc:
            # Transient failures are not cached so that a re-run retries them.
            logger.warning("Geocoding %r failed: %s", query, exc)
            return None
        result = (float(hits[0]["lat"]), float(hits[0]["lon"])) if hits else None
        self._cache[query] = result
        return result


def legacy_queries(row: pd.Series) -> list[tuple[str, str]]:
    """The original single query: ``"<name>, <state>, Australia"``."""
    return [("name", f"{row['project_name']}, {row['state']}, Australia")]


def improved_queries(row: pd.Series) -> list[tuple[str, str]]:
    """Fallback chain: full name, name without generic suffix, then postcode centroid."""
    name, state = row["project_name"], row["state"]
    queries = [("name", f"{name}, {state}, Australia")]
    short = (
        pd.Series([name])
        .str.replace(
            r"\b(solar|wind|energy|power|renewable|hybrid|battery)?\s*"
            r"(farm|park|station|project|hub|plant|facility)\b.*$",
            "",
            case=False,
            regex=True,
        )[0]
        .strip()
    )
    if short and short.lower() != str(name).lower():
        queries.append(("short_name", f"{short}, {state}, Australia"))
    postcode = row.get("postcode")
    if isinstance(postcode, str) and postcode.strip().isdigit():
        queries.append(("postcode", f"{postcode.strip()}, {state}, Australia"))
    return queries


def geocode_stations(
    df: pd.DataFrame,
    geocoder: NominatimGeocoder,
    query_builder: Callable[[pd.Series], list[tuple[str, str]]],
) -> pd.DataFrame:
    """Add ``latitude``, ``longitude`` and ``geocode_method`` columns to the CER frame."""
    lats, lons, methods = [], [], []
    for _, row in df.iterrows():
        lat = lon = method = None
        for label, query in query_builder(row):
            hit = geocoder.geocode(query)
            if hit is not None:
                (lat, lon), method = hit, label
                break
        lats.append(lat)
        lons.append(lon)
        methods.append(method)
    geocoder.save_cache()
    out = df.copy()
    out["latitude"] = pd.Series(lats, index=df.index, dtype="float64")
    out["longitude"] = pd.Series(lons, index=df.index, dtype="float64")
    out["geocode_method"] = pd.Series(methods, index=df.index, dtype="object")
    return out
