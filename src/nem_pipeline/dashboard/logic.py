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
