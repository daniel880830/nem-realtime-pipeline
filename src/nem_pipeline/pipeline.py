"""End-to-end orchestration of one pipeline run, with stage timings and a JSON report."""

from __future__ import annotations

import json
import logging
import platform
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from nem_pipeline import __version__, cer, extract, geocode, matching, transform, warehouse
from nem_pipeline.config import Settings

logger = logging.getLogger(__name__)

Method = Literal["legacy", "improved"]
CerSource = Literal["snapshot", "live"]
SNAPSHOT_DIR = "cer_snapshot_2025"


class StageTimer:
    """Collects wall-clock durations of named stages."""

    def __init__(self) -> None:
        self.durations: dict[str, float] = {}

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        """Time the enclosed block."""
        started = time.perf_counter()
        logger.info("Stage %s ...", name)
        try:
            yield
        finally:
            self.durations[name] = round(time.perf_counter() - started, 3)
            logger.info("Stage %s took %.2fs", name, self.durations[name])


def unit_overwrite_error(metrics: pd.DataFrame) -> dict[str, float | int]:
    """Quantify the original dashboard bug that showed only the last unit of a facility.

    For every (timestamp, facility) the original buffer kept the last unit's power. This
    compares that value with the facility total.
    """
    if metrics.empty:
        return {
            "facility_intervals": 0,
            "wrong_intervals": 0,
            "wrong_rate": 0.0,
            "abs_error_mw_mean_when_wrong": 0.0,
        }
    ordered = metrics.sort_values(["timestamp", "facility_code", "unit_code"], kind="stable")
    grouped = ordered.groupby(["timestamp", "facility_code"])["power_mw"]
    shown = grouped.last()
    total = grouped.sum()
    error = (total - shown).abs()
    wrong = error > 0.5
    return {
        "facility_intervals": len(total),
        "wrong_intervals": int(wrong.sum()),
        "wrong_rate": float(wrong.mean()),
        "abs_error_mw_mean_when_wrong": float(error[wrong].mean()) if wrong.any() else 0.0,
    }


def _cer_paths(settings: Settings, source: CerSource) -> dict[str, Path]:
    cfg = settings.pipeline.cer
    if source == "snapshot":
        base = settings.raw_dir / SNAPSHOT_DIR
        return {name: base / f"cer_{name}.csv" for name in cfg.datasets}
    return cer.download_datasets(cfg, settings.raw_dir / "cer_live")


def run(
    settings: Settings,
    method: Method = "improved",
    *,
    cer_source: CerSource = "snapshot",
    refresh: bool = False,
    client_factory: Callable[[str], extract.OpenElectricityClient] = extract.OpenElectricityClient,
    geocoder: geocode.NominatimGeocoder | None = None,
) -> dict[str, Any]:
    """Run CER → geocode → extract → transform → match → load, and write a report.

    Args:
        settings: Runtime settings.
        method: ``"legacy"`` reproduces the original logic, ``"improved"`` uses the fixes.
        cer_source: ``"snapshot"`` (2025 Wayback copies in ``data/raw``) or ``"live"``.
        refresh: Re-download OpenElectricity data even if cached.
        client_factory: Builds the API client from a key (overridden in tests).
        geocoder: Pre-built geocoder (overridden in tests).

    Returns:
        The report that is also written to ``reports/run_<method>.json``.
    """
    if method not in ("legacy", "improved"):
        raise ValueError(f"Unknown method: {method!r}")
    cfg = settings.pipeline
    timer = StageTimer()
    report: dict[str, Any] = {"method": method, "cer_source": cer_source}

    with timer.stage("cer_scrape_clean"):
        stations = cer.integrate(cer.read_datasets(_cer_paths(settings, cer_source)))
        report["cer_rows_integrated"] = len(stations)
        stations = (
            cer.clean_legacy(stations) if method == "legacy" else cer.clean_improved(stations)
        )
        report["cer_stations_clean"] = len(stations)

    with timer.stage("geocode"):
        geocoder = geocoder or geocode.NominatimGeocoder(
            cfg.geocode,
            user_agent=f"nem-realtime-pipeline/{__version__} ({settings.nominatim_contact})",
            cache_path=settings.geocode_cache_path,
        )
        builder = geocode.legacy_queries if method == "legacy" else geocode.improved_queries
        stations = geocode.geocode_stations(stations, geocoder, builder)
        report["geocoded"] = int(stations["latitude"].notna().sum())
        report["geocode_rate"] = report["geocoded"] / len(stations) if len(stations) else 0.0
        report["geocode_methods"] = stations["geocode_method"].value_counts().to_dict()
        report["nominatim_requests"] = geocoder.requests_sent

    with timer.stage("extract"):
        client = client_factory(settings.require_openelectricity_key())
        snapshot = extract.download_raw(
            client, cfg.extract, settings.raw_dir / "oe", refresh=refresh
        )
        facilities, _units, fac_long, market_long = extract.load_raw(snapshot)
        report["facilities"] = len(facilities)
        report["facilities_with_official_location"] = int(facilities["latitude"].notna().sum())
        report["raw_facility_rows"] = len(fac_long)
        report["raw_market_rows"] = len(market_long)

    with timer.stage("transform"):
        correct = transform.facility_metrics(fac_long)
        market_correct = transform.market_metrics(market_long)
        diagnostics: dict[str, Any] = {"unit_overwrite": unit_overwrite_error(correct)}
        try:
            legacy_metrics = transform.facility_metrics_legacy(fac_long)
            diagnostics["positional_pairing"] = transform.misalignment_rate(legacy_metrics, correct)
        except ValueError as exc:
            legacy_metrics = None
            diagnostics["positional_pairing"] = {"crashed": str(exc)}
        try:
            legacy_market = transform.market_metrics_legacy(market_long)
        except ValueError as exc:
            legacy_market = None
            diagnostics["market_positional_pairing"] = {"crashed": str(exc)}
        if method == "legacy":
            if legacy_metrics is None or legacy_market is None:
                raise RuntimeError("The original positional pairing crashes on this extract")
            metrics, market = legacy_metrics, legacy_market
        else:
            metrics, market = correct, market_correct
        report["facility_metric_rows"] = len(metrics)
        report["market_rows"] = len(market)
        report["diagnostics"] = diagnostics

    with timer.stage("match"):
        legacy_matches = matching.match_legacy(facilities, stations, cfg.matching)
        improved_matches = matching.match_improved(facilities, stations, cfg.matching)
        report["matching"] = {
            "legacy_fuzzy": matching.evaluate_matches(
                legacy_matches, facilities, cfg.matching.correct_within_km
            ),
            "improved_fuzzy": matching.evaluate_matches(
                improved_matches, facilities, cfg.matching.correct_within_km
            ),
        }
        if method == "legacy":
            locations = legacy_matches[["facility_code", "latitude", "longitude"]].assign(
                source="cer_fuzzy"
            )
        else:
            locations = facilities[["facility_code", "latitude", "longitude"]].assign(
                source="openelectricity"
            )
        locations = locations.dropna(subset=["latitude", "longitude"])
        report["facilities_on_map"] = len(locations)
        report["map_coverage"] = len(locations) / len(facilities) if len(facilities) else 0.0

    with timer.stage("load"):
        with warehouse.connect(settings.warehouse_path) as conn:
            warehouse.load_stations(conn, stations)
            warehouse.replace_table(conn, "cer_facility_match", improved_matches)
            warehouse.write_serving_tables(conn, facilities, metrics, market, locations)
        report["warehouse_mb"] = round(settings.warehouse_path.stat().st_size / 1e6, 2)

    report["stage_seconds"] = timer.durations
    report["total_seconds"] = round(sum(timer.durations.values()), 3)
    report["environment"] = environment_info()
    write_report(settings.reports_dir / f"run_{method}.json", report)
    return report


def environment_info() -> dict[str, str]:
    """Versions and platform used for a measurement."""
    import duckdb
    import rapidfuzz

    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "python": platform.python_version(),
        "platform": platform.platform(terse=True),
        "processor": platform.processor() or platform.machine(),
        "pandas": pd.__version__,
        "duckdb": duckdb.__version__,
        "rapidfuzz": rapidfuzz.__version__,
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    """Write ``report`` as pretty JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
