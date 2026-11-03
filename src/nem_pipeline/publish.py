"""Replay the warehouse over MQTT, one message per topic and 5-minute interval."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd

logger = logging.getLogger(__name__)

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M"


class MqttLike(Protocol):
    """The subset of ``paho.mqtt.client.Client`` the publisher needs."""

    def publish(self, topic: str, payload: str, qos: int = 0) -> Any:
        """Publish ``payload`` on ``topic``."""


def _clean(value: Any) -> Any:
    """Make a value JSON-safe (NaN becomes null, numpy scalars become Python numbers)."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    return value.item() if hasattr(value, "item") else value


def facility_frames(metrics: pd.DataFrame, facilities: pd.DataFrame) -> pd.DataFrame:
    """Aggregate unit readings to facility level and attach display attributes.

    The original dashboard kept only the *last* unit received for a facility name, so a
    multi-unit power station showed one unit's output instead of the station total.
    """
    agg = (
        metrics.groupby(["timestamp", "facility_code"], as_index=False)
        .agg(
            power_mw=("power_mw", "sum"), emissions_t=("emissions_t", lambda s: s.sum(min_count=1))
        )
        .merge(facilities, on="facility_code", how="inner")
    )
    return agg.sort_values(["timestamp", "facility_code"], kind="stable").reset_index(drop=True)


FACILITY_FIELDS = ["facility_code", "facility_name", "state", "fuel_type", "latitude", "longitude",
                   "power_mw", "emissions_t"]  # fmt: skip
MARKET_FIELDS = ["region", "price_aud_mwh", "demand_mw"]


def build_payload(ts: pd.Timestamp, group: pd.DataFrame, fields: list[str]) -> dict[str, Any]:
    """Build the JSON-ready payload for one interval."""
    data = [
        {k: _clean(v) for k, v in zip(fields, row, strict=True)}
        for row in group[fields].itertuples(index=False)
    ]
    return {"timestamp": ts.strftime(TIMESTAMP_FORMAT), "data": data}


def facility_messages(frame: pd.DataFrame) -> Iterator[tuple[pd.Timestamp, dict[str, Any]]]:
    """Yield ``(timestamp, payload)`` for each interval of :func:`facility_frames` output."""
    for ts, group in frame.groupby("timestamp", sort=True):
        yield ts, build_payload(ts, group, FACILITY_FIELDS)


def market_messages(market: pd.DataFrame) -> Iterator[tuple[pd.Timestamp, dict[str, Any]]]:
    """Yield ``(timestamp, payload)`` for each interval of the market table."""
    for ts, group in market.groupby("timestamp", sort=True):
        yield ts, build_payload(ts, group, MARKET_FIELDS)


@dataclass
class ReplayStats:
    """Counters collected while replaying."""

    messages: int = 0
    bytes: int = 0
    intervals: int = 0
    cycles: int = 0
    elapsed_s: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def messages_per_s(self) -> float:
        """Average publish rate."""
        return self.messages / self.elapsed_s if self.elapsed_s > 0 else 0.0


def replay(
    client: MqttLike,
    facility_frame: pd.DataFrame,
    market: pd.DataFrame,
    *,
    facility_topic: str,
    market_topic: str,
    qos: int = 0,
    interval_delay_s: float = 0.1,
    cycles: int | None = 1,
    stop_event: threading.Event | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayStats:
    """Publish every interval in timestamp order, interleaving facility and market messages.

    Args:
        client: Connected MQTT client.
        facility_frame: Output of :func:`facility_frames`.
        market: Market table.
        facility_topic: Topic for facility messages.
        market_topic: Topic for market messages.
        qos: MQTT quality of service.
        interval_delay_s: Pause after each interval.
        cycles: Number of passes over the data; ``None`` loops until ``stop_event`` is set.
        stop_event: Set from another thread to stop after the current message.
        sleep: Injectable sleep function (tests pass a no-op).

    Returns:
        Publishing statistics.
    """
    if cycles is not None and cycles < 1:
        raise ValueError("cycles must be at least 1 or None")
    stop_event = stop_event or threading.Event()
    stats = ReplayStats()
    fac_groups = dict(list(facility_frame.groupby("timestamp", sort=True)))
    market_groups = dict(list(market.groupby("timestamp", sort=True)))
    timestamps = sorted(set(fac_groups) | set(market_groups))
    started = time.perf_counter()
    while not stop_event.is_set() and (cycles is None or stats.cycles < cycles):
        for ts in timestamps:
            if stop_event.is_set():
                break
            for topic, groups, fields in (
                (facility_topic, fac_groups, FACILITY_FIELDS),
                (market_topic, market_groups, MARKET_FIELDS),
            ):
                if ts not in groups:
                    continue
                payload = build_payload(ts, groups[ts], fields)
                body = json.dumps(payload, separators=(",", ":"))
                result = client.publish(topic, body, qos=qos)
                rc = getattr(result, "rc", 0)
                if rc != 0:
                    stats.errors.append(f"{topic}@{payload['timestamp']}: rc={rc}")
                stats.messages += 1
                stats.bytes += len(body.encode("utf-8"))
            stats.intervals += 1
            if interval_delay_s > 0:
                sleep(interval_delay_s)
        stats.cycles += 1
    stats.elapsed_s = time.perf_counter() - started
    return stats


def connect_mqtt(host: str, port: int, client_id: str = "nem-publisher") -> Any:
    """Create a paho client, connect it and start its network loop.

    Raises:
        ConnectionError: If the broker cannot be reached.
    """
    import paho.mqtt.client as mqtt

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.max_queued_messages_set(0)
    try:
        client.connect(host, port, keepalive=60)
    except OSError as exc:
        raise ConnectionError(f"Cannot reach MQTT broker {host}:{port}: {exc}") from exc
    client.loop_start()
    return client


def connect_subscriber(
    host: str,
    port: int,
    topics: list[str],
    on_message: Callable[[str, bytes], None],
    *,
    timeout_s: float = 10.0,
) -> Any:
    """Connect a subscriber and block until its subscription is acknowledged.

    Raises:
        ConnectionError: If the broker is unreachable or does not acknowledge in time.
    """
    import paho.mqtt.client as mqtt

    subscribed = threading.Event()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="nem-benchmark-sub")
    client.on_connect = lambda c, *_: c.subscribe([(t, 1) for t in topics])
    client.on_subscribe = lambda *_: subscribed.set()
    client.on_message = lambda _c, _u, msg: on_message(msg.topic, msg.payload)
    try:
        client.connect(host, port, keepalive=60)
    except OSError as exc:
        raise ConnectionError(f"Cannot reach MQTT broker {host}:{port}: {exc}") from exc
    client.loop_start()
    if not subscribed.wait(timeout_s):
        client.loop_stop()
        raise ConnectionError(f"Broker {host}:{port} did not acknowledge the subscription")
    return client
