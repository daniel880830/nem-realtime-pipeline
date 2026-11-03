import json
import threading

import numpy as np
import pandas as pd
import pytest

from nem_pipeline import extract, publish, transform
from nem_pipeline.schema import FacilityMessage, MarketMessage
from tests.conftest import FACILITIES_PAYLOAD, FACILITY_DATA_PAYLOAD, MARKET_PAYLOAD, FakeMqtt


@pytest.fixture
def frames():
    fac, units = extract.parse_facilities(FACILITIES_PAYLOAD)
    metrics = transform.facility_metrics(extract.parse_facility_data(FACILITY_DATA_PAYLOAD, units))
    market = transform.market_metrics(extract.parse_market(MARKET_PAYLOAD))
    display = fac.dropna(subset=["latitude"])[
        ["facility_code", "facility_name", "state", "fuel_type", "latitude", "longitude"]
    ]
    return publish.facility_frames(metrics, display), market


def test_facility_frames_sum_units(frames):
    fac_frame, _ = frames
    first = fac_frame[(fac_frame["facility_code"] == "BAYSW")].iloc[0]
    assert first["power_mw"] == 900.0  # 500 + 400, not just the last unit
    assert first["emissions_t"] == 72.0


def test_facility_frames_all_nan_emissions_stay_nan():
    metrics = pd.DataFrame(
        {"facility_code": ["F", "F"], "unit_code": ["U1", "U2"], "timestamp": pd.to_datetime(["2025-01-01"] * 2),
         "power_mw": [1.0, 2.0], "emissions_t": [np.nan, np.nan]}
    )  # fmt: skip
    disp = pd.DataFrame({"facility_code": ["F"], "facility_name": ["f"], "state": ["NSW"], "fuel_type": ["Wind"],
                         "latitude": [-33.0], "longitude": [151.0]})  # fmt: skip
    out = publish.facility_frames(metrics, disp)
    assert out["power_mw"].item() == 3.0 and np.isnan(out["emissions_t"].item())


def test_clean_values():
    assert publish._clean(float("nan")) is None
    assert publish._clean(np.float64(1.5)) == 1.5 and isinstance(publish._clean(np.int64(2)), int)
    assert publish._clean(None) is None and publish._clean("x") == "x"


def test_replay_publishes_valid_messages_in_order(frames):
    fac_frame, market = frames
    client = FakeMqtt()
    sleeps = []
    stats = publish.replay(client, fac_frame, market, facility_topic="f", market_topic="m",
                           interval_delay_s=0.5, sleep=sleeps.append)  # fmt: skip
    assert stats.intervals == 3 and stats.messages == 6 and stats.errors == []
    assert sleeps == [0.5] * 3
    timestamps = []
    for topic, body, _ in client.messages:
        payload = json.loads(body)
        model = FacilityMessage if topic == "f" else MarketMessage
        model.model_validate(payload)  # the consumer contract holds
        timestamps.append(payload["timestamp"])
    assert timestamps == sorted(timestamps)
    assert stats.bytes == sum(len(b.encode()) for _, b, _ in client.messages)


def test_replay_multiple_cycles_and_rc_errors(frames):
    fac_frame, market = frames
    client = FakeMqtt(rc=4)
    stats = publish.replay(client, fac_frame, market, facility_topic="f", market_topic="m",
                           interval_delay_s=0, cycles=2)  # fmt: skip
    assert stats.cycles == 2 and stats.messages == 12
    assert len(stats.errors) == 12 and "rc=4" in stats.errors[0]
    assert stats.messages_per_s >= 0


def test_replay_stops_on_event(frames):
    fac_frame, market = frames
    stop = threading.Event()
    client = FakeMqtt()

    def sleep(_):
        stop.set()

    stats = publish.replay(client, fac_frame, market, facility_topic="f", market_topic="m", cycles=None,
                           stop_event=stop, sleep=sleep)  # fmt: skip
    assert stats.intervals == 1


def test_replay_rejects_bad_cycles(frames):
    with pytest.raises(ValueError):
        publish.replay(FakeMqtt(), *frames, facility_topic="f", market_topic="m", cycles=0)


def test_replay_empty_data():
    stats = publish.replay(
        FakeMqtt(), pd.DataFrame(columns=["timestamp"]), pd.DataFrame(columns=["timestamp"]),
        facility_topic="f", market_topic="m",
    )  # fmt: skip
    assert stats.messages == 0 and stats.messages_per_s == 0.0


def test_connect_mqtt_unreachable():
    with pytest.raises(ConnectionError):
        publish.connect_mqtt("127.0.0.1", 1)
