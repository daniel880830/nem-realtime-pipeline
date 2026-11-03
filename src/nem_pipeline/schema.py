"""Column contracts shared by every pipeline stage, and the MQTT payload models."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

# NEM network regions map 1:1 to states (the ACT sits inside NSW1).
REGION_TO_STATE: dict[str, str] = {
    "NSW1": "NSW",
    "QLD1": "QLD",
    "VIC1": "VIC",
    "SA1": "SA",
    "TAS1": "TAS",
}

# Raw long-format facility metrics, one row per (unit, timestamp, metric).
RAW_FACILITY_COLUMNS = ["facility_code", "unit_code", "timestamp", "metric", "value"]

# OpenElectricity facility dimension.
FACILITY_COLUMNS = [
    "facility_code",
    "facility_name",
    "region",
    "state",
    "fuel_type",
    "latitude",
    "longitude",
]

# Tidy facility metrics, one row per (unit, timestamp).
FACILITY_METRIC_COLUMNS = ["facility_code", "unit_code", "timestamp", "power_mw", "emissions_t"]

RAW_MARKET_COLUMNS = ["timestamp", "region", "metric", "value"]
MARKET_COLUMNS = ["timestamp", "region", "price_aud_mwh", "demand_mw"]

# Clean Energy Regulator station list after integration.
CER_COLUMNS = ["project_name", "state", "mw_capacity", "fuel_source", "status", "postcode"]


class SchemaError(ValueError):
    """Raised when a DataFrame does not have the columns a stage needs."""


def require_columns(df: pd.DataFrame, columns: Iterable[str], name: str) -> None:
    """Fail fast when ``df`` lacks any of ``columns``.

    Args:
        df: Frame to check.
        columns: Required column names.
        name: Human-readable name of the frame, used in the error message.

    Raises:
        SchemaError: If at least one column is missing.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise SchemaError(f"{name} is missing columns: {missing}")


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FacilityReading(_Payload):
    """One facility's aggregated reading inside a facility message."""

    facility_code: str
    facility_name: str
    state: str | None
    fuel_type: str
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    power_mw: float
    emissions_t: float | None


class MarketReading(_Payload):
    """One region's price and demand inside a market message."""

    region: str
    price_aud_mwh: float | None
    demand_mw: float | None


class FacilityMessage(_Payload):
    """MQTT payload on the facility topic: every facility for one 5-minute interval."""

    timestamp: str
    data: list[FacilityReading]


class MarketMessage(_Payload):
    """MQTT payload on the market topic: every region for one 5-minute interval."""

    timestamp: str
    data: list[MarketReading]
