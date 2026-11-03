"""State handling for the live dashboard, kept free of Streamlit so it can be unit-tested."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field

import pandas as pd
from pydantic import ValidationError

from nem_pipeline.schema import FacilityMessage, MarketMessage

logger = logging.getLogger(__name__)

FUEL_COLORS: dict[str, str] = {
    "Battery": "#d62728",
    "Coal": "#000000",
    "Distillate": "#e377c2",
    "Solar": "#ff9f1c",
    "Wind": "#17becf",
    "Hydro": "#1f77b4",
    "Bioenergy": "#2ca02c",
    "Gas": "#7f7f7f",
    "Pumps": "#9467bd",
    "Unknown": "#aaaaaa",
}


def fuel_group(fuel_type: str | None) -> str:
    """Map a detailed fuel label (e.g. ``"Gas Ocgt"``) to a colour group (``"Gas"``)."""
    text = (fuel_type or "").lower()
    for group in FUEL_COLORS:
        if group.lower() in text:
            return group
    if "battery" in text:
        return "Battery"
    if "bio" in text:
        return "Bioenergy"
    return "Unknown"


def hex_to_rgb(color: str) -> list[int]:
    """Convert ``"#rrggbb"`` to ``[r, g, b]``.

    Raises:
        ValueError: If ``color`` is not a 6-digit hex colour.
    """
    value = color.lstrip("#")
    if len(value) != 6:
        raise ValueError(f"Not a hex colour: {color!r}")
    return [int(value[i : i + 2], 16) for i in (0, 2, 4)]


@dataclass
class LiveState:
    """Latest reading per facility and latest market snapshot, fed by the MQTT thread."""

    facilities: dict[str, dict] = field(default_factory=dict)
    market: list[dict] = field(default_factory=list)
    market_timestamp: str | None = None
    facility_timestamp: str | None = None
    rejected: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def ingest(
        self, topic: str, raw: bytes | str, *, facility_topic: str, market_topic: str
    ) -> bool:
        """Validate and apply one MQTT message.

        Returns:
            ``True`` if the message was applied, ``False`` if it was rejected.
        """
        try:
            payload = json.loads(raw)
            if topic == facility_topic:
                msg = FacilityMessage.model_validate(payload)
                with self._lock:
                    for reading in msg.data:
                        self.facilities[reading.facility_code] = reading.model_dump()
                    self.facility_timestamp = msg.timestamp
            elif topic == market_topic:
                market = MarketMessage.model_validate(payload)
                with self._lock:
                    self.market = [r.model_dump() for r in market.data]
                    self.market_timestamp = market.timestamp
            else:
                return False
        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as exc:
            logger.warning("Rejected message on %s: %s", topic, exc)
            with self._lock:
                self.rejected += 1
            return False
        return True

    def facility_frame(self, base: pd.DataFrame) -> pd.DataFrame:
        """Join the latest readings onto the static facility table."""
        with self._lock:
            live = pd.DataFrame(list(self.facilities.values()))
        out = base.copy()
        if live.empty:
            out["power_mw"] = float("nan")
            out["emissions_t"] = float("nan")
        else:
            out = out.merge(
                live[["facility_code", "power_mw", "emissions_t"]], on="facility_code", how="left"
            )
        out["fuel_group"] = out["fuel_type"].map(fuel_group)
        out["color"] = out["fuel_group"].map(lambda g: hex_to_rgb(FUEL_COLORS[g]))
        return out

    def market_frame(self) -> pd.DataFrame:
        """Latest market snapshot sorted by region."""
        with self._lock:
            df = pd.DataFrame(self.market, columns=["region", "price_aud_mwh", "demand_mw"])
        return df.sort_values("region").reset_index(drop=True)


def filter_facilities(df: pd.DataFrame, states: list[str], fuel_groups: list[str]) -> pd.DataFrame:
    """Keep facilities in the selected states and fuel groups (empty selection → empty frame)."""
    return df[df["state"].isin(states) & df["fuel_group"].isin(fuel_groups)]


def emission_radius(
    emissions: pd.Series, *, min_radius_m: float = 2500, max_radius_m: float = 60000
) -> pd.Series:
    """Map emissions to a circle radius in metres, so circle *area* is proportional to emissions.

    The largest emitter gets ``max_radius_m``; zero, negative or missing emissions get
    ``min_radius_m``.
    """
    values = emissions.astype("float64").fillna(0).clip(lower=0)
    peak = values.max()
    if not peak or peak <= 0:
        return pd.Series(min_radius_m, index=emissions.index, dtype="float64")
    return min_radius_m + (max_radius_m - min_radius_m) * (values / peak) ** 0.5


def prepare_emission_snapshot(snapshot: pd.DataFrame) -> pd.DataFrame:
    """Add colour, radius and display labels to a :func:`warehouse.facility_snapshot` frame.

    Emitting facilities are drawn opaque in their fuel colour; zero-emission facilities are
    drawn faint so the emitters stand out.
    """
    out = snapshot.copy()
    out["fuel_group"] = out["fuel_type"].map(fuel_group)
    emitting = out["emissions_t"].fillna(0) > 0
    out["color"] = [
        [*hex_to_rgb(FUEL_COLORS[g]), 220 if e else 60]
        for g, e in zip(out["fuel_group"], emitting, strict=True)
    ]
    out["radius_m"] = emission_radius(out["emissions_t"])
    out["emissions_label"] = out["emissions_t"].map(lambda v: "n/a" if pd.isna(v) else f"{v:,.2f}")
    out["power_label"] = out["power_mw"].map(lambda v: "n/a" if pd.isna(v) else f"{v:,.1f}")
    return out
