import pandas as pd

from nem_pipeline import benchmark
from tests.conftest import FakeMqtt


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        self.t += 0.001
        return self.t


def _frames():
    ts = pd.to_datetime(["2025-10-01 00:00", "2025-10-01 00:05"])
    fac = pd.DataFrame(
        {"timestamp": ts, "facility_code": ["F", "F"], "facility_name": ["f", "f"], "state": ["NSW"] * 2,
         "fuel_type": ["Wind"] * 2, "latitude": [-33.0] * 2, "longitude": [151.0] * 2,
         "power_mw": [1.0, 2.0], "emissions_t": [0.0, 0.0]}
    )  # fmt: skip
    market = pd.DataFrame(
        {"timestamp": ts[:1], "region": ["NSW1"], "price_aud_mwh": [1.0], "demand_mw": [2.0]}
    )
    return fac, market


class Loopback(FakeMqtt):
    def __init__(self, recorder, drop_market=False):
        super().__init__()
        self.recorder = recorder
        self.drop_market = drop_market

    def publish(self, topic, payload, qos=0):
        if not (self.drop_market and topic == "m"):
            self.recorder.on_message(topic, payload.encode())
        return super().publish(topic, payload, qos)


def test_benchmark_measures_latency_and_loss():
    fac, market = _frames()
    recorder = benchmark.ArrivalRecorder(expected=3)
    result = benchmark.run_benchmark(
        Loopback(recorder),
        recorder,
        fac,
        market,
        facility_topic="f",
        market_topic="m",
        qos=1,
        timeout_s=1,
    )
    assert result["sent"] == 3 and result["received"] == 3 and result["lost"] == 0
    assert result["latency_ms_p50"] >= 0 and result["intervals"] == 2
    assert recorder.done.is_set()


def test_benchmark_reports_lost_messages():
    fac, market = _frames()
    recorder = benchmark.ArrivalRecorder(expected=3)
    result = benchmark.run_benchmark(
        Loopback(recorder, drop_market=True), recorder, fac, market,
        facility_topic="f", market_topic="m", qos=0, timeout_s=0.01,
    )  # fmt: skip
    assert result["lost"] == 1 and result["received"] == 2


def test_summarize_with_injected_clock_and_empty():
    clock = Clock()
    timed = benchmark.TimedPublisher(FakeMqtt(), clock=clock)
    timed.publish("f", '{"timestamp":"2025-10-01 00:00","data":[]}')
    received = {("f", "2025-10-01 00:00"): clock() + 0.010}
    out = benchmark.summarize(timed.sent, received)
    assert out["latency_ms_p50"] == 11.0 and out["delivered_msgs_per_s"] > 0
    empty = benchmark.summarize({}, {})
    assert empty["latency_ms_p50"] is None and empty["delivered_msgs_per_s"] is None
