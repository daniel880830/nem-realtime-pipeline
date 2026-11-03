"""End-to-end runs on synthetic data: CER snapshot → geocode → extract → transform → match → DuckDB → MQTT."""

import json

import pytest
from typer.testing import CliRunner

from nem_pipeline import cli, geocode, pipeline, publish, warehouse
from tests.conftest import (
    FACILITY_DATA_PAYLOAD,
    FakeMqtt,
    FakeOEClient,
    FakeSession,
    fake_nominatim,
)

pytestmark = pytest.mark.integration


def _geocoder(settings):
    return geocode.NominatimGeocoder(
        settings.pipeline.geocode, "test", settings.geocode_cache_path,
        session=FakeSession(fake_nominatim), sleep=lambda s: None,
    )  # fmt: skip


def _run(settings, method, client=None):
    client = client or FakeOEClient()
    return pipeline.run(
        settings, method, client_factory=lambda key: client, geocoder=_geocoder(settings)
    )


@pytest.mark.parametrize("method", ["legacy", "improved"])
def test_run_writes_report_and_serving_tables(settings, method):
    report = _run(settings, method)
    assert report["facilities"] == 4
    assert report["facility_metric_rows"] > 0 and report["market_rows"] == 6
    assert set(report["stage_seconds"]) == {
        "cer_scrape_clean",
        "geocode",
        "extract",
        "transform",
        "match",
        "load",
    }
    saved = json.loads((settings.reports_dir / f"run_{method}.json").read_text(encoding="utf-8"))
    assert saved["method"] == method
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        display = warehouse.facilities_for_display(conn)
        stations = warehouse.read_stations(conn)
    assert len(display) == report["facilities_on_map"]
    assert stations["latitude"].notna().sum() == report["geocoded"]


def _station_names(settings):
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        return set(warehouse.read_stations(conn)["project_name"])


def test_improved_vs_legacy_differences(settings):
    legacy = _run(settings, "legacy")
    legacy_names = _station_names(settings)
    improved = _run(settings, "improved")
    improved_names = _station_names(settings)
    # Legacy merges the two "Smith Farm" sites and keeps an invalid state; improved does the opposite.
    assert "Smith Farm Shed 2" in improved_names and "Smith Farm Shed 2" not in legacy_names
    assert "Bad Row" in legacy_names and "Bad Row" not in improved_names
    # Improved uses official coordinates for every facility that has them.
    assert improved["facilities_on_map"] == 3
    assert improved["map_coverage"] >= legacy["map_coverage"]
    assert improved["diagnostics"]["unit_overwrite"]["wrong_intervals"] > 0
    m = improved["matching"]
    assert m["improved_fuzzy"]["cross_state_matches"] == 0


def test_rerun_uses_caches(settings):
    client = FakeOEClient()
    _run(settings, "improved", client)
    calls = len(client.calls)
    report = _run(settings, "improved", client)
    assert len(client.calls) == calls
    assert report["nominatim_requests"] == 0


def test_legacy_fails_where_original_crashed(settings):
    class GappyClient(FakeOEClient):
        def facility_data(self, *a, **kw):
            payload = json.loads(json.dumps(FACILITY_DATA_PAYLOAD))
            payload["data"][1]["results"][0]["data"].pop()
            return payload

    with pytest.raises(RuntimeError, match="crashes"):
        _run(settings, "legacy", GappyClient())
    report = _run(settings, "improved", GappyClient())
    assert "crashed" in report["diagnostics"]["positional_pairing"]


def test_invalid_method(settings):
    with pytest.raises(ValueError):
        pipeline.run(settings, "magic")  # type: ignore[arg-type]


def test_warehouse_feeds_publisher(settings):
    _run(settings, "improved")
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        frame = publish.facility_frames(
            warehouse.read_table(conn, "facility_metrics"), warehouse.facilities_for_display(conn)
        )
        market = warehouse.read_table(conn, "market_metrics")
    client = FakeMqtt()
    stats = publish.replay(
        client, frame, market, facility_topic="f", market_topic="m", interval_delay_s=0
    )
    assert stats.messages == 6


class _FakePaho(FakeMqtt):
    def loop_stop(self):
        pass

    def disconnect(self):
        pass


def test_cli_end_to_end_offline(settings, monkeypatch):
    runner = CliRunner()
    # Fill every cache with the fakes so the CLI runs without network access.
    _run(settings, "improved")
    result = runner.invoke(cli.app, ["run", "--method", "improved"])
    assert result.exit_code == 0, result.output
    assert '"method": "improved"' in result.output

    result = runner.invoke(cli.app, ["extract"])
    assert result.exit_code == 0 and "1 market windows cached" in result.output

    fake = _FakePaho()
    monkeypatch.setattr(publish, "connect_mqtt", lambda *a, **k: fake)
    result = runner.invoke(cli.app, ["publish", "--cycles", "1", "--delay", "0"])
    assert result.exit_code == 0, result.output
    assert "Published 6 messages" in result.output

    class Loopback(_FakePaho):
        on_message = None

        def publish(self, topic, payload, qos=0):
            Loopback.on_message(topic, payload.encode())
            return super().publish(topic, payload, qos)

    def fake_subscriber(host, port, topics, on_message):
        Loopback.on_message = on_message
        return _FakePaho()

    monkeypatch.setattr(publish, "connect_subscriber", fake_subscriber)
    monkeypatch.setattr(publish, "connect_mqtt", lambda *a, **k: Loopback())
    result = runner.invoke(cli.app, ["benchmark", "--intervals", "2", "--qos", "0"])
    assert result.exit_code == 0, result.output
    bench = json.loads(
        (settings.reports_dir / "benchmark_publish.json").read_text(encoding="utf-8")
    )
    assert bench["intervals"] == 2 and bench["sent"] == 4 and bench["lost"] == 0
    assert bench["latency_ms_p95"] >= bench["latency_ms_p50"] >= 0


def _unreachable(*_a, **_k):
    raise ConnectionError("no broker")


def test_cli_errors(settings, monkeypatch):
    runner = CliRunner()
    assert runner.invoke(cli.app, ["run", "--method", "magic"]).exit_code == 2
    assert runner.invoke(cli.app, ["publish"]).exit_code == 1  # no warehouse yet
    _run(settings, "improved")
    monkeypatch.setattr(publish, "connect_subscriber", _unreachable)
    assert runner.invoke(cli.app, ["benchmark"]).exit_code == 1
    monkeypatch.setenv("APP_CONFIG_FILE", "missing.toml")
    result = runner.invoke(cli.app, ["extract"])
    assert result.exit_code == 2 and "not found" in result.output
