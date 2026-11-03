"""Command-line entry point: ``nem-pipeline run | extract | publish | dashboard | benchmark``."""

from __future__ import annotations

import json
import logging
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Annotated

import typer

from nem_pipeline import benchmark as benchmark_mod
from nem_pipeline import extract, pipeline, publish, warehouse
from nem_pipeline.config import ConfigError, Settings

app = typer.Typer(help="NEM real-time ETL and MQTT streaming pipeline.", no_args_is_help=True)


def _settings() -> Settings:
    try:
        settings = Settings()
        _ = settings.pipeline  # validate the TOML early
    except ConfigError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    return settings


@app.callback()
def main(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Debug logging.")] = False,
) -> None:
    """Configure logging for every command."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


@app.command()
def run(
    method: Annotated[str, typer.Option(help="legacy (original logic) or improved.")] = "improved",
    cer_source: Annotated[
        str, typer.Option(help="snapshot (2025 copy) or live (current CER site).")
    ] = "snapshot",
    refresh: Annotated[bool, typer.Option(help="Re-download OpenElectricity data.")] = False,
) -> None:
    """Run the whole ETL and load the warehouse used by the publisher and dashboard."""
    if method not in ("legacy", "improved") or cer_source not in ("snapshot", "live"):
        typer.secho(
            "--method must be legacy|improved and --cer-source snapshot|live", fg="red", err=True
        )
        raise typer.Exit(2)
    settings = _settings()
    try:
        report = pipeline.run(settings, method, cer_source=cer_source, refresh=refresh)  # type: ignore[arg-type]
    except (ConfigError, extract.OpenElectricityError, RuntimeError) as exc:
        typer.secho(f"Pipeline failed: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    summary = {
        k: report[k]
        for k in ("method", "facilities", "facility_metric_rows", "map_coverage", "total_seconds")
    }
    typer.echo(json.dumps(summary, indent=2))


@app.command("extract")
def extract_cmd(
    refresh: Annotated[bool, typer.Option(help="Ignore cached responses.")] = False,
) -> None:
    """Only download (and cache) the OpenElectricity responses."""
    settings = _settings()
    try:
        client = extract.OpenElectricityClient(settings.require_openelectricity_key())
        snap = extract.download_raw(
            client, settings.pipeline.extract, settings.raw_dir / "oe", refresh=refresh
        )
    except (ConfigError, extract.OpenElectricityError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    typer.echo(
        f"{len(snap.facility_data)} facility batches, {len(snap.market)} market windows cached"
    )


def _load_serving(settings: Settings):
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        display = warehouse.facilities_for_display(conn)
        metrics = warehouse.read_table(conn, "facility_metrics")
        market = warehouse.read_table(conn, "market_metrics")
    return publish.facility_frames(metrics, display), market


@app.command("publish")
def publish_cmd(
    cycles: Annotated[int, typer.Option(help="Passes over the data; 0 loops until Ctrl+C.")] = 1,
    delay: Annotated[
        float | None, typer.Option(help="Seconds between intervals (default from config).")
    ] = None,
) -> None:
    """Replay the warehouse to the MQTT broker."""
    settings = _settings()
    cfg = settings.pipeline.publish
    try:
        facility_frame, market = _load_serving(settings)
        client = publish.connect_mqtt(settings.mqtt_host, settings.mqtt_port)
    except (FileNotFoundError, ConnectionError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    typer.echo(
        f"Publishing to {settings.mqtt_host}:{settings.mqtt_port} "
        f"({cfg.facility_topic}, {cfg.market_topic})"
    )
    try:
        stats = publish.replay(
            client,
            facility_frame,
            market,
            facility_topic=cfg.facility_topic,
            market_topic=cfg.market_topic,
            qos=cfg.qos,
            interval_delay_s=cfg.interval_delay_s if delay is None else delay,
            cycles=cycles or None,
            stop_event=stop,
        )
    finally:
        client.loop_stop()
        client.disconnect()
    typer.echo(
        f"Published {stats.messages} messages ({stats.bytes / 1e6:.1f} MB) "
        f"in {stats.elapsed_s:.1f}s"
    )


@app.command()
def benchmark(
    intervals: Annotated[int, typer.Option(help="Number of 5-minute intervals to publish.")] = 288,
    qos: Annotated[int, typer.Option(help="MQTT QoS for the benchmark.")] = 1,
) -> None:
    """Measure end-to-end MQTT latency and throughput (publisher → broker → subscriber)."""
    settings = _settings()
    cfg = settings.pipeline.publish
    facility_frame, market = _load_serving(settings)
    keep = sorted(facility_frame["timestamp"].unique())[:intervals]
    facility_frame = facility_frame[facility_frame["timestamp"].isin(keep)]
    market = market[market["timestamp"].isin(keep)]
    expected = facility_frame["timestamp"].nunique() + market["timestamp"].nunique()
    recorder = benchmark_mod.ArrivalRecorder(expected)
    try:
        subscriber = publish.connect_subscriber(
            settings.mqtt_host,
            settings.mqtt_port,
            [cfg.facility_topic, cfg.market_topic],
            recorder.on_message,
        )
        client = publish.connect_mqtt(
            settings.mqtt_host, settings.mqtt_port, client_id="nem-benchmark"
        )
    except ConnectionError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc
    try:
        result = benchmark_mod.run_benchmark(
            client, recorder, facility_frame, market,
            facility_topic=cfg.facility_topic, market_topic=cfg.market_topic, qos=qos,
        )  # fmt: skip
    finally:
        for c in (client, subscriber):
            c.loop_stop()
            c.disconnect()
    result["broker"] = f"{settings.mqtt_host}:{settings.mqtt_port}"
    result["environment"] = pipeline.environment_info()
    pipeline.write_report(settings.reports_dir / "benchmark_publish.json", result)
    typer.echo(json.dumps(result, indent=2))


@app.command()
def dashboard(port: Annotated[int, typer.Option(help="Port for Streamlit.")] = 8501) -> None:
    """Start the Streamlit dashboard."""
    app_path = Path(__file__).parent / "dashboard" / "app.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path), "--server.port", str(port),
           "--server.address", "0.0.0.0", "--server.headless", "true"]  # fmt: skip
    raise typer.Exit(subprocess.call(cmd))


if __name__ == "__main__":
    app()
