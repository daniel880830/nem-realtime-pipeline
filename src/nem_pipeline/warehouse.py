"""DuckDB warehouse: CER station star schema plus NEM facility and market tables."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd

from nem_pipeline.schema import FACILITY_COLUMNS, FACILITY_METRIC_COLUMNS, MARKET_COLUMNS

_DDL = """
DROP TABLE IF EXISTS power_station_fact;
DROP TABLE IF EXISTS power_station_dim;
CREATE OR REPLACE SEQUENCE station_id_seq START 1;

CREATE TABLE power_station_dim (
    station_id   INTEGER PRIMARY KEY DEFAULT nextval('station_id_seq'),
    project_name VARCHAR NOT NULL,
    state        VARCHAR NOT NULL,
    fuel_source  VARCHAR,
    status       VARCHAR NOT NULL,
    postcode     VARCHAR,
    UNIQUE (project_name, state)
);

CREATE TABLE power_station_fact (
    station_id     INTEGER PRIMARY KEY REFERENCES power_station_dim (station_id),
    mw_capacity    DOUBLE,
    latitude       DOUBLE,
    longitude      DOUBLE,
    geocode_method VARCHAR
);
"""

# Tables the publisher and dashboard read; written by the last `run`.
SERVING_TABLES = ("facility_dim", "facility_metrics", "market_metrics", "facility_location")


def _run(conn: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    """Execute ``sql`` and consume the result (DuckDB 2 requires it before the next call)."""
    return conn.execute(sql).fetchall()


@contextmanager
def connect(path: Path, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the warehouse and always close it.

    Raises:
        FileNotFoundError: If ``read_only`` and the warehouse does not exist yet.
    """
    if read_only and not path.exists():
        raise FileNotFoundError(f"Warehouse not found: {path}. Run `nem-pipeline run` first.")
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path), read_only=read_only)
    try:
        yield conn
    finally:
        conn.close()


def load_stations(conn: duckdb.DuckDBPyConnection, stations: pd.DataFrame) -> int:
    """(Re)create the CER star schema and load the geocoded stations.

    Returns:
        Number of stations loaded.
    """
    _run(conn, _DDL)
    dim = stations[["project_name", "state", "fuel_source", "status", "postcode"]]
    conn.register("dim_df", dim)
    _run(
        conn,
        "INSERT INTO power_station_dim (project_name, state, fuel_source, status, postcode) "
        "SELECT * FROM dim_df",
    )
    conn.unregister("dim_df")
    fact = stations[
        ["project_name", "state", "mw_capacity", "latitude", "longitude", "geocode_method"]
    ]
    conn.register("fact_df", fact)
    _run(
        conn,
        """
        INSERT INTO power_station_fact
        SELECT d.station_id, f.mw_capacity, f.latitude, f.longitude, f.geocode_method
        FROM fact_df f JOIN power_station_dim d USING (project_name, state)
        """,
    )
    conn.unregister("fact_df")
    return _run(conn, "SELECT count(*) FROM power_station_dim")[0][0]


def read_stations(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return the geocoded CER stations (denormalised)."""
    return conn.execute(
        """
        SELECT d.project_name, d.state, f.mw_capacity, d.fuel_source, d.status, d.postcode,
               f.latitude, f.longitude, f.geocode_method
        FROM power_station_dim d JOIN power_station_fact f USING (station_id)
        ORDER BY d.station_id
        """
    ).fetchdf()


def replace_table(conn: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame) -> None:
    """Create or replace table ``name`` with the contents of ``df``."""
    if not name.isidentifier():
        raise ValueError(f"Invalid table name: {name!r}")
    conn.register("_df", df)
    _run(conn, f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")
    conn.unregister("_df")


def write_serving_tables(
    conn: duckdb.DuckDBPyConnection,
    facilities: pd.DataFrame,
    metrics: pd.DataFrame,
    market: pd.DataFrame,
    locations: pd.DataFrame,
) -> None:
    """Write the four tables consumed by the publisher and dashboard."""
    replace_table(conn, "facility_dim", facilities[FACILITY_COLUMNS])
    replace_table(conn, "facility_metrics", metrics[FACILITY_METRIC_COLUMNS])
    replace_table(conn, "market_metrics", market[MARKET_COLUMNS])
    replace_table(
        conn, "facility_location", locations[["facility_code", "latitude", "longitude", "source"]]
    )


def read_table(conn: duckdb.DuckDBPyConnection, name: str) -> pd.DataFrame:
    """Return a whole serving table."""
    if name not in SERVING_TABLES:
        raise ValueError(f"Unknown table: {name!r}")
    return conn.execute(f"SELECT * FROM {name}").fetchdf()


def facilities_for_display(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Facilities joined to the coordinates chosen by the last run."""
    return conn.execute(
        """
        SELECT d.facility_code, d.facility_name, d.state, d.fuel_type, l.latitude, l.longitude
        FROM facility_dim d JOIN facility_location l USING (facility_code)
        WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
        ORDER BY d.facility_code
        """
    ).fetchdf()
