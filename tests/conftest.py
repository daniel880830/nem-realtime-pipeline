"""Synthetic fixtures: small OpenElectricity/CER payloads and offline fakes for every network call."""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from nem_pipeline.config import GeocodeConfig, Settings, load_pipeline_config

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "pipeline.toml"


def oe_facility(
    code: str, name: str, region: str, units: list[tuple[str, str, str, float]], loc=None
):
    """Build one facility in OpenElectricity format. ``units`` = (code, fueltech, status, capacity)."""
    return {
        "code": code,
        "name": name,
        "network_id": "NEM",
        "network_region": region,
        "location": {"lat": loc[0], "lng": loc[1]} if loc else None,
        "units": [
            {"code": u, "fueltech_id": f, "status_id": s, "capacity_registered": c}
            for u, f, s, c in units
        ],
    }


FACILITIES_PAYLOAD: dict[str, Any] = {
    "success": True,
    "data": [
        oe_facility("BAYSW", "Bayswater", "NSW1", [("BW01", "coal_black", "operating", 660.0),
                                                   ("BW02", "coal_black", "operating", 660.0)],
                    (-32.395, 150.949)),
        oe_facility("MANSF", "Mansfield Solar Farm", "VIC1", [("MSF1", "solar_utility", "operating", 50.0)],
                    (-37.05, 146.09)),
        oe_facility("HORNS", "Hornsdale Wind Farm", "SA1", [("HDWF1", "wind", "operating", 100.0),
                                                            ("HPR1", "battery_discharging", "operating", 150.0)],
                    (-33.06, 138.54)),
        oe_facility("NOLOC", "Nowhere Battery", "QLD1", [("NOL1", "battery", "committed", 20.0)]),
        oe_facility("OLD", "Retired Plant", "NSW1", [("OLD1", "gas_ocgt", "retired", 100.0)], (-33.0, 151.0)),
    ],
}  # fmt: skip

TIMES = ["2025-10-01T00:00:00+10:00", "2025-10-01T00:05:00+10:00", "2025-10-01T00:10:00+10:00"]


def series(metric: str, unit: str, values: dict[str, list[float | None]]) -> dict[str, Any]:
    """One metric series with a result per unit code."""
    return {
        "metric": metric,
        "unit": unit,
        "results": [
            {
                "name": f"{metric}_{u}",
                "columns": {"unit_code": u},
                "data": [[t, v] for t, v in zip(TIMES, vals, strict=True)],
            }
            for u, vals in values.items()
        ],
    }


POWER = {"BW01": [500.0, 510.0, 520.0], "BW02": [400.0, 0.0, 410.0], "MSF1": [0.0, 0.0, 5.0],
         "HDWF1": [80.0, 81.0, 82.0], "HPR1": [10.0, 20.0, 30.0]}  # fmt: skip
EMISSIONS = {"BW01": [40.0, 41.0, 42.0], "BW02": [32.0, 0.0, 33.0], "MSF1": [0.0, 0.0, 0.0],
             "HDWF1": [0.0, 0.0, 0.0], "HPR1": [0.0, 0.0, 0.0]}  # fmt: skip

FACILITY_DATA_PAYLOAD = {
    "success": True,
    "data": [series("power", "MW", POWER), series("emissions", "t", EMISSIONS)],
}

MARKET_PAYLOAD = {
    "success": True,
    "data": [
        {
            "metric": m,
            "unit": u,
            "results": [
                {"name": f"{m}_{r}", "columns": {"network_region": r}, "data": [[t, base + i] for i, t in enumerate(TIMES)]}
                for r, base in (("NSW1", 100.0), ("VIC1", 50.0))
            ],
        }
        for m, u in (("price", "AUD/MWh"), ("demand", "MW"))
    ],
}  # fmt: skip


class FakeOEClient:
    """Stands in for :class:`OpenElectricityClient`; counts calls."""

    def __init__(self, *_: Any, **__: Any) -> None:
        self.calls: list[str] = []

    def facilities(self, network: str = "NEM") -> dict[str, Any]:
        self.calls.append("facilities")
        return FACILITIES_PAYLOAD

    def facility_data(self, network, codes, metrics, interval, start, end) -> dict[str, Any]:
        self.calls.append(f"data:{','.join(codes)}")
        return FACILITY_DATA_PAYLOAD

    def market(self, network, metrics, interval, start, end) -> dict[str, Any]:
        self.calls.append("market")
        return MARKET_PAYLOAD


class FakeResponse:
    def __init__(self, status: int = 200, payload: Any = None, text: str = "") -> None:
        self.status_code = status
        self._payload = payload
        self.text = text or (json.dumps(payload) if payload is not None else "")
        self.ok = 200 <= status < 400

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("no json")
        return self._payload

    def raise_for_status(self) -> None:
        import requests

        if not self.ok:
            raise requests.HTTPError(f"{self.status_code}")


class FakeSession:
    """``requests.Session`` replacement driven by a responder callable."""

    def __init__(self, responder: Callable[[str, dict], FakeResponse | Exception]) -> None:
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, Any]] = []
        self._responder = responder

    def get(self, url: str, params: Any = None, timeout: float | None = None) -> FakeResponse:
        self.calls.append((url, params))
        result = self._responder(url, params)
        if isinstance(result, Exception):
            raise result
        return result


# Station coordinates known to the fake geocoder (lat, lon).
KNOWN_PLACES = {
    "Bayswater": (-32.40, 150.95),
    "Mansfield Solar Farm": (-37.06, 146.10),
    "Hornsdale Wind Farm": (-33.07, 138.55),
    "3722": (-37.05, 146.08),
}


def fake_nominatim(url: str, params: dict) -> FakeResponse:
    query = params["q"]
    for key, (lat, lon) in KNOWN_PLACES.items():
        if query.startswith(f"{key},"):
            return FakeResponse(payload=[{"lat": str(lat), "lon": str(lon)}])
    return FakeResponse(payload=[])


class FakeMqtt:
    """Records published messages."""

    class _Result:
        def __init__(self, rc: int) -> None:
            self.rc = rc

    def __init__(self, rc: int = 0) -> None:
        self.messages: list[tuple[str, str, int]] = []
        self._rc = rc

    def publish(self, topic: str, payload: str, qos: int = 0) -> _Result:
        self.messages.append((topic, payload, qos))
        return self._Result(self._rc)


CER_CSVS = {
    "accredited": (
        "Accreditation code,Power station name,State,Postcode,Installed capacity (MW),Fuel Source (s)\n"
        "A1,Mansfield Solar Farm - Solar - VIC,VIC,3722,50.0,Solar\n"
        "A2,Smith Farm - Solar w SGU - NSW,NSW,2000,0.2,Solar\n"
        "A3,Smith Farm - Shed 2 - Solar - NSW,NSW,2000,0.4,Solar\n"
    ),
    "committed": 'Project Name,State ,MW Capacity,Fuel Source\nBayswater,NSW,"1,320.0",Coal\n',
    "probable": "Project Name,State,MW Capacity,Fuel Source\nHornsdale Wind Farm,SA,100,Wind\nBad Row,XX,1,Wind\n",
}


@pytest.fixture
def pipeline_config():
    return load_pipeline_config(CONFIG)


@pytest.fixture
def geocode_config() -> GeocodeConfig:
    return GeocodeConfig(min_interval_s=1.0, timeout_s=1)


@pytest.fixture
def cer_frames() -> dict[str, pd.DataFrame]:
    import io

    return {k: pd.read_csv(io.StringIO(v), dtype=str) for k, v in CER_CSVS.items()}


@pytest.fixture
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temporary project root with config, CER snapshot and APP_ variables set."""
    (tmp_path / "config").mkdir()
    shutil.copy(CONFIG, tmp_path / "config" / "pipeline.toml")
    snap = tmp_path / "data" / "raw" / "cer_snapshot_2025"
    snap.mkdir(parents=True)
    for name, text in CER_CSVS.items():
        (snap / f"cer_{name}.csv").write_text(text, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    for key in list(__import__("os").environ):
        if key.startswith("APP_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("APP_OPENELECTRICITY_API_KEY", "test-key")
    monkeypatch.setenv("APP_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("APP_REPORTS_DIR", str(tmp_path / "reports"))
    monkeypatch.setenv("APP_CONFIG_FILE", str(tmp_path / "config" / "pipeline.toml"))
    return tmp_path


@pytest.fixture
def settings(workspace: Path) -> Settings:
    return Settings(_env_file=None)
