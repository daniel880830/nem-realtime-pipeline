"""Extraction from the OpenElectricity v4 REST API.

Raw JSON responses are written to ``data/raw/oe`` and reused on later runs, because the free
COMMUNITY plan only grants a few hundred API credits per month.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from nem_pipeline.config import ExtractConfig
from nem_pipeline.schema import (
    FACILITY_COLUMNS,
    RAW_FACILITY_COLUMNS,
    RAW_MARKET_COLUMNS,
    REGION_TO_STATE,
)

logger = logging.getLogger(__name__)

API_URL = "https://api.openelectricity.org.au/v4"
NEM_UTC_OFFSET = "+10:00"
# Facilities whose units are in one of these states are extracted (same filter as the original).
SELECTED_STATUSES = frozenset({"operating", "committed"})
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class OpenElectricityError(RuntimeError):
    """Raised when the OpenElectricity API returns an error or an unusable payload."""


class OpenElectricityClient:
    """Minimal synchronous client for the endpoints this pipeline uses."""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = API_URL,
        session: requests.Session | None = None,
        timeout_s: float = 120,
        max_retries: int = 3,
        backoff_s: float = 2.0,
    ) -> None:
        if not api_key:
            raise OpenElectricityError("An OpenElectricity API key is required")
        self._base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        self._session.headers["Authorization"] = f"Bearer {api_key}"
        self._timeout_s = timeout_s
        self._max_retries = max_retries
        self._backoff_s = backoff_s

    def _get(self, path: str, params: list[tuple[str, str]]) -> dict[str, Any]:
        url = f"{self._base_url}/{path.lstrip('/')}"
        last_error: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._session.get(url, params=params, timeout=self._timeout_s)
            except requests.RequestException as exc:
                last_error = exc
            else:
                if response.status_code not in _RETRYABLE_STATUS:
                    if not response.ok:
                        raise OpenElectricityError(
                            f"GET {path} failed with {response.status_code}: {response.text[:300]}"
                        )
                    payload = response.json()
                    if not payload.get("success", False):
                        raise OpenElectricityError(
                            f"GET {path} unsuccessful: {payload.get('error')}"
                        )
                    return payload
                last_error = OpenElectricityError(f"GET {path} returned {response.status_code}")
            logger.warning(
                "OpenElectricity attempt %d/%d failed: %s", attempt, self._max_retries, last_error
            )
            if attempt < self._max_retries:
                time.sleep(self._backoff_s * attempt)
        raise OpenElectricityError(
            f"GET {path} failed after {self._max_retries} attempts"
        ) from last_error

    def facilities(self, network: str = "NEM") -> dict[str, Any]:
        """Return every facility (with units and location) on ``network``."""
        return self._get("facilities/", [("network_id", network)])

    def facility_data(
        self,
        network: str,
        facility_codes: Sequence[str],
        metrics: Sequence[str],
        interval: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, Any]:
        """Return unit-level time series for ``facility_codes`` in ``[start, end)``."""
        if not facility_codes:
            raise OpenElectricityError("facility_codes must not be empty")
        params = [("facility_code", c) for c in facility_codes]
        params += [("metrics", m) for m in metrics]
        params += [("interval", interval), *_window_params(start, end)]
        return self._get(f"data/facilities/{network}", params)

    def market(
        self, network: str, metrics: Sequence[str], interval: str, start: datetime, end: datetime
    ) -> dict[str, Any]:
        """Return price and demand per network region in ``[start, end)``."""
        params = [("metrics", m) for m in metrics]
        params += [("interval", interval), ("primary_grouping", "network_region")]
        params += _window_params(start, end)
        return self._get(f"market/network/{network}", params)


def _window_params(start: datetime, end: datetime) -> list[tuple[str, str]]:
    return [("date_start", start.isoformat()), ("date_end", end.isoformat())]


def date_windows(start: datetime, end: datetime, max_days: int) -> list[tuple[datetime, datetime]]:
    """Split ``[start, end)`` into consecutive windows of at most ``max_days`` days.

    Raises:
        ValueError: If the window is empty or ``max_days`` is not positive.
    """
    if start >= end:
        raise ValueError("start must be earlier than end")
    if max_days < 1:
        raise ValueError("max_days must be at least 1")
    windows = []
    cursor = start
    while cursor < end:
        upper = min(cursor + timedelta(days=max_days), end)
        windows.append((cursor, upper))
        cursor = upper
    return windows


def batched(items: Sequence[str], size: int) -> Iterator[list[str]]:
    """Yield consecutive chunks of ``items`` with at most ``size`` elements."""
    if size < 1:
        raise ValueError("size must be at least 1")
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


@dataclass(frozen=True)
class RawSnapshot:
    """Locations of the cached raw responses."""

    facilities: Path
    facility_data: list[Path]
    market: list[Path]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OpenElectricityError(f"Corrupt raw file {path}: {exc}") from exc


def download_raw(
    client: OpenElectricityClient, cfg: ExtractConfig, raw_dir: Path, *, refresh: bool = False
) -> RawSnapshot:
    """Download every response needed by the pipeline, skipping files already cached.

    Args:
        client: API client.
        cfg: Extraction window and batching.
        raw_dir: Directory for the JSON files (``data/raw/oe``).
        refresh: Re-download even when a cached file exists.

    Returns:
        Paths of the facility list, facility-data batches and market windows.
    """
    tag = f"{cfg.start:%Y%m%dT%H%M}_{cfg.end:%Y%m%dT%H%M}_{cfg.interval}"
    fac_path = raw_dir / "facilities.json"
    if refresh or not fac_path.exists():
        logger.info("Downloading facility list")
        _write_json(fac_path, client.facilities(cfg.network))
    facilities, _ = parse_facilities(_read_json(fac_path))
    codes = sorted(facilities["facility_code"])

    windows = date_windows(cfg.start, cfg.end, cfg.max_days_per_request)
    data_paths: list[Path] = []
    for w, (start, end) in enumerate(windows):
        for b, batch in enumerate(batched(codes, cfg.facility_batch_size)):
            path = raw_dir / f"facility_data_{tag}_w{w:02d}_b{b:03d}.json"
            if refresh or not path.exists():
                logger.info("Downloading facility batch w%d b%d (%d facilities)", w, b, len(batch))
                payload = client.facility_data(
                    cfg.network, batch, ("power", "emissions"), cfg.interval, start, end
                )
                _write_json(path, payload)
            data_paths.append(path)

    market_paths: list[Path] = []
    for w, (start, end) in enumerate(windows):
        path = raw_dir / f"market_{tag}_w{w:02d}.json"
        if refresh or not path.exists():
            logger.info("Downloading market window w%d", w)
            _write_json(
                path, client.market(cfg.network, ("price", "demand"), cfg.interval, start, end)
            )
        market_paths.append(path)
    return RawSnapshot(fac_path, data_paths, market_paths)


def _fuel_label(fueltech: str | None) -> str:
    return (fueltech or "unknown").replace("_", " ").title()


def parse_facilities(payload: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the facility list into a facility dimension and a unit table.

    Only facilities with at least one operating or committed unit are kept. A facility's
    ``fuel_type`` is the fuel of its largest unit by registered capacity.

    Returns:
        ``(facilities, units)``; units has ``unit_code, facility_code, fuel_type, status``.
    """
    fac_rows, unit_rows = [], []
    for fac in payload.get("data") or []:
        units = fac.get("units") or []
        if not any(u.get("status_id") in SELECTED_STATUSES for u in units):
            continue
        largest = max(units, key=lambda u: u.get("capacity_registered") or 0)
        location = fac.get("location") or {}
        region = fac.get("network_region")
        fac_rows.append(
            {
                "facility_code": fac["code"],
                "facility_name": (fac.get("name") or fac["code"]).strip(),
                "region": region,
                "state": REGION_TO_STATE.get(region or ""),
                "fuel_type": _fuel_label(largest.get("fueltech_id")),
                "latitude": location.get("lat"),
                "longitude": location.get("lng"),
            }
        )
        unit_rows.extend(
            {
                "unit_code": u["code"],
                "facility_code": fac["code"],
                "fuel_type": _fuel_label(u.get("fueltech_id")),
                "status": u.get("status_id"),
            }
            for u in units
        )
    facilities = pd.DataFrame(fac_rows, columns=FACILITY_COLUMNS)
    facilities[["latitude", "longitude"]] = facilities[["latitude", "longitude"]].astype("float64")
    units = pd.DataFrame(unit_rows, columns=["unit_code", "facility_code", "fuel_type", "status"])
    return facilities, units


def to_market_time(values: pd.Series) -> pd.Series:
    """Convert ISO timestamps with a UTC offset to naive NEM market time (UTC+10)."""
    ts = pd.to_datetime(values, utc=True)
    return (ts + pd.Timedelta(hours=10)).dt.tz_localize(None)


def parse_facility_data(payload: dict[str, Any], units: pd.DataFrame) -> pd.DataFrame:
    """Flatten a facility-data response into long format (one row per unit, time, metric).

    Args:
        payload: Raw ``data/facilities`` response.
        units: Unit table from :func:`parse_facilities`, used to attach ``facility_code``.
    """
    unit_to_facility = dict(zip(units["unit_code"], units["facility_code"], strict=True))
    rows = []
    for series in payload.get("data") or []:
        metric = series["metric"]
        for result in series.get("results") or []:
            unit_code = (result.get("columns") or {}).get("unit_code")
            facility_code = unit_to_facility.get(unit_code)
            if facility_code is None:
                continue
            rows.extend(
                (facility_code, unit_code, ts, metric, value) for ts, value in result["data"]
            )
    df = pd.DataFrame(rows, columns=RAW_FACILITY_COLUMNS)
    df["timestamp"] = to_market_time(df["timestamp"])
    df["value"] = df["value"].astype("float64")
    return df


def parse_market(payload: dict[str, Any]) -> pd.DataFrame:
    """Flatten a market response into long format (one row per region, time, metric)."""
    rows = []
    for series in payload.get("data") or []:
        metric = series["metric"]
        for result in series.get("results") or []:
            region = (result.get("columns") or {}).get("network_region") or result["name"].split(
                "_"
            )[-1]
            rows.extend((ts, region, metric, value) for ts, value in result["data"])
    df = pd.DataFrame(rows, columns=RAW_MARKET_COLUMNS)
    df["timestamp"] = to_market_time(df["timestamp"])
    df["value"] = df["value"].astype("float64")
    return df


def load_raw(
    snapshot: RawSnapshot,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse every cached response.

    Returns:
        ``(facilities, units, facility_long, market_long)``.
    """
    facilities, units = parse_facilities(_read_json(snapshot.facilities))
    fac_frames = [parse_facility_data(_read_json(p), units) for p in snapshot.facility_data]
    market_frames = [parse_market(_read_json(p)) for p in snapshot.market]
    facility_long = (
        pd.concat(fac_frames, ignore_index=True)
        if fac_frames
        else pd.DataFrame(columns=RAW_FACILITY_COLUMNS)
    )
    market_long = (
        pd.concat(market_frames, ignore_index=True)
        if market_frames
        else pd.DataFrame(columns=RAW_MARKET_COLUMNS)
    )
    return facilities, units, facility_long, market_long
