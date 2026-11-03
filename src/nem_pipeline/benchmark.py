"""End-to-end MQTT benchmark: publish part of the warehouse and time its arrival at a subscriber."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd

from nem_pipeline import publish

# Payloads start with {"timestamp":"YYYY-MM-DD HH:MM", so the interval key sits at a fixed offset.
_TS_SLICE = slice(len('{"timestamp":"'), len('{"timestamp":"') + 16)


class TimedPublisher:
    """Wraps an MQTT client and records when each (topic, interval) message was sent."""

    def __init__(
        self, client: publish.MqttLike, clock: Callable[[], float] = time.perf_counter
    ) -> None:
        self._client = client
        self._clock = clock
        self.sent: dict[tuple[str, str], float] = {}

    def publish(self, topic: str, payload: str, qos: int = 0) -> Any:
        """Record the send time, then publish."""
        self.sent[(topic, payload[_TS_SLICE])] = self._clock()
        return self._client.publish(topic, payload, qos=qos)


class ArrivalRecorder:
    """Subscriber-side callback that records arrival times."""

    def __init__(self, expected: int, clock: Callable[[], float] = time.perf_counter) -> None:
        self.received: dict[tuple[str, str], float] = {}
        self._expected = expected
        self._clock = clock
        self.done = threading.Event()

    def on_message(self, topic: str, payload: bytes) -> None:
        """Store the arrival time of one message."""
        self.received[(topic, payload[_TS_SLICE].decode())] = self._clock()
        if len(self.received) >= self._expected:
            self.done.set()


def summarize(
    sent: dict[tuple[str, str], float], received: dict[tuple[str, str], float]
) -> dict[str, Any]:
    """Latency percentiles, loss and delivered throughput."""
    keys = [k for k in sent if k in received]
    latencies_ms = np.array([(received[k] - sent[k]) * 1000 for k in keys])
    span = (max(received[k] for k in keys) - min(sent.values())) if keys else 0.0
    return {
        "sent": len(sent),
        "received": len(keys),
        "lost": len(sent) - len(keys),
        "latency_ms_p50": round(float(np.percentile(latencies_ms, 50)), 2) if keys else None,
        "latency_ms_p95": round(float(np.percentile(latencies_ms, 95)), 2) if keys else None,
        "latency_ms_max": round(float(latencies_ms.max()), 2) if keys else None,
        "delivered_msgs_per_s": round(len(keys) / span, 1) if span > 0 else None,
    }


def run_benchmark(
    publisher_client: publish.MqttLike,
    recorder: ArrivalRecorder,
    facility_frame: pd.DataFrame,
    market: pd.DataFrame,
    *,
    facility_topic: str,
    market_topic: str,
    qos: int,
    timeout_s: float = 60.0,
) -> dict[str, Any]:
    """Publish everything with no pacing and wait for the subscriber to receive it.

    Args:
        publisher_client: Connected MQTT client used to publish.
        recorder: Arrival recorder already attached to a subscribed client.
        facility_frame: Facility-level frame to publish.
        market: Market table to publish.
        facility_topic: Facility topic.
        market_topic: Market topic.
        qos: MQTT QoS.
        timeout_s: Maximum wait for outstanding messages after publishing.
    """
    timed = TimedPublisher(publisher_client)
    stats = publish.replay(
        timed,
        facility_frame,
        market,
        facility_topic=facility_topic,
        market_topic=market_topic,
        qos=qos,
        interval_delay_s=0,
        cycles=1,
    )
    recorder.done.wait(timeout_s)
    result = summarize(timed.sent, recorder.received)
    result.update(
        {
            "intervals": stats.intervals,
            "qos": qos,
            "payload_mb": round(stats.bytes / 1e6, 2),
            "avg_payload_kb": round(stats.bytes / max(stats.messages, 1) / 1e3, 1),
            "publish_loop_s": round(stats.elapsed_s, 3),
            "publish_errors": len(stats.errors),
        }
    )
    return result
