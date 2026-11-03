import pandas as pd
import pytest

from nem_pipeline import warehouse


def stations():
    return pd.DataFrame(
        {"project_name": ["A", "B"], "state": ["NSW", "VIC"], "mw_capacity": [1.0, None],
         "fuel_source": ["Solar", "Wind"], "status": ["accredited", "probable"], "postcode": ["2000", None],
         "latitude": [-33.0, None], "longitude": [151.0, None], "geocode_method": ["name", None]}
    )  # fmt: skip


def test_star_schema_round_trip_and_reload(tmp_path):
    path = tmp_path / "w.duckdb"
    with warehouse.connect(path) as conn:
        assert warehouse.load_stations(conn, stations()) == 2
        assert warehouse.load_stations(conn, stations().iloc[:1]) == 1  # reload replaces tables
        out = warehouse.read_stations(conn)
    assert out["project_name"].tolist() == ["A"]
    assert out["latitude"].item() == -33.0


def test_duplicate_station_violates_unique(tmp_path):
    with (
        warehouse.connect(tmp_path / "w.duckdb") as conn,
        pytest.raises(Exception, match=r"(?i)constraint"),
    ):
        warehouse.load_stations(conn, pd.concat([stations(), stations()]))


def test_serving_tables(tmp_path):
    fac = pd.DataFrame({"facility_code": ["F1", "F2"], "facility_name": ["One", "Two"], "region": ["NSW1", "SA1"],
                        "state": ["NSW", "SA"], "fuel_type": ["Wind", "Solar"], "latitude": [1.0, 2.0],
                        "longitude": [3.0, 4.0]})  # fmt: skip
    metrics = pd.DataFrame({"facility_code": ["F1"], "unit_code": ["U1"], "timestamp": pd.to_datetime(["2025-01-01"]),
                            "power_mw": [1.0], "emissions_t": [0.0]})  # fmt: skip
    market = pd.DataFrame({"timestamp": pd.to_datetime(["2025-01-01"]), "region": ["NSW1"],
                           "price_aud_mwh": [10.0], "demand_mw": [100.0]})  # fmt: skip
    loc = pd.DataFrame({"facility_code": ["F1", "F2"], "latitude": [1.0, None], "longitude": [3.0, None],
                        "source": ["openelectricity"] * 2})  # fmt: skip
    path = tmp_path / "w.duckdb"
    with warehouse.connect(path) as conn:
        warehouse.write_serving_tables(conn, fac, metrics, market, loc)
    with warehouse.connect(path, read_only=True) as conn:
        assert warehouse.facilities_for_display(conn)["facility_code"].tolist() == ["F1"]
        assert len(warehouse.read_table(conn, "facility_metrics")) == 1
        with pytest.raises(ValueError):
            warehouse.read_table(conn, "power_station_dim; DROP TABLE x")


def test_invalid_table_name_and_missing_file(tmp_path):
    with warehouse.connect(tmp_path / "w.duckdb") as conn, pytest.raises(ValueError):
        warehouse.replace_table(conn, "bad name", pd.DataFrame({"a": [1]}))
    with pytest.raises(FileNotFoundError, match="nem-pipeline run"):
        warehouse.connect(tmp_path / "missing.duckdb", read_only=True).__enter__()


def _history_warehouse(path):
    fac = pd.DataFrame({"facility_code": ["COAL", "WIND", "HIDDEN"], "facility_name": ["Coal", "Wind", "Hidden"],
                        "region": ["NSW1", "SA1", "VIC1"], "state": ["NSW", "SA", "VIC"],
                        "fuel_type": ["Coal Black", "Wind", "Gas Ocgt"], "latitude": [1.0, 2.0, None],
                        "longitude": [3.0, 4.0, None]})  # fmt: skip
    ts = pd.to_datetime(
        [
            "2025-10-01 00:00",
            "2025-10-01 00:00",
            "2025-10-01 00:05",
            "2025-10-01 00:00",
            "2025-10-01 00:00",
        ]
    )
    metrics = pd.DataFrame({"facility_code": ["COAL", "COAL", "COAL", "WIND", "HIDDEN"],
                            "unit_code": ["C1", "C2", "C1", "W1", "H1"], "timestamp": ts,
                            "power_mw": [500.0, 400.0, 510.0, 80.0, 50.0],
                            "emissions_t": [40.0, 32.0, 41.0, None, 3.0]})  # fmt: skip
    market = pd.DataFrame(
        {"timestamp": ts[:1], "region": ["NSW1"], "price_aud_mwh": [1.0], "demand_mw": [1.0]}
    )
    loc = fac[["facility_code", "latitude", "longitude"]].assign(source="openelectricity")
    with warehouse.connect(path) as conn:
        warehouse.write_serving_tables(conn, fac, metrics, market, loc)


def test_emission_timeline_sums_per_state_and_interval(tmp_path):
    path = tmp_path / "w.duckdb"
    _history_warehouse(path)
    with warehouse.connect(path, read_only=True) as conn:
        tl = warehouse.emission_timeline(conn)
    first = tl[tl["timestamp"] == pd.Timestamp("2025-10-01 00:00")].set_index("state")
    assert first.loc["NSW", "emissions_t"] == 72.0 and first.loc["NSW", "power_mw"] == 900.0
    assert first.loc["SA", "emissions_t"] == 0.0  # all-null emissions become 0
    assert (
        first.loc["VIC", "emissions_t"] == 3.0
    )  # timeline includes facilities without coordinates
    assert tl["timestamp"].is_monotonic_increasing


def test_facility_snapshot_at_timestamp(tmp_path):
    path = tmp_path / "w.duckdb"
    _history_warehouse(path)
    with warehouse.connect(path, read_only=True) as conn:
        snap = warehouse.facility_snapshot(conn, pd.Timestamp("2025-10-01 00:00"))
        later = warehouse.facility_snapshot(conn, pd.Timestamp("2025-10-01 00:05"))
        none = warehouse.facility_snapshot(conn, pd.Timestamp("2030-01-01"))
    assert snap["facility_code"].tolist() == [
        "COAL",
        "WIND",
    ]  # sorted by emissions, unmapped excluded
    assert snap.iloc[0]["emissions_t"] == 72.0 and snap.iloc[0]["power_mw"] == 900.0
    assert pd.isna(snap.iloc[1]["emissions_t"])
    assert later["power_mw"].tolist() == [510.0]
    assert none.empty
