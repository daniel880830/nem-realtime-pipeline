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
