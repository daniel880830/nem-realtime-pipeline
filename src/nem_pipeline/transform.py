"""Reshape long-format API data into tidy metric tables."""

from __future__ import annotations

import pandas as pd

from nem_pipeline.schema import (
    FACILITY_METRIC_COLUMNS,
    MARKET_COLUMNS,
    RAW_FACILITY_COLUMNS,
    RAW_MARKET_COLUMNS,
    require_columns,
)


def _drop_idle(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows without power, and rows where power and emissions are both zero."""
    df = df.dropna(subset=["power_mw"])
    idle = (df["power_mw"] == 0) & (df["emissions_t"].fillna(0) == 0)
    return df[~idle]


def facility_metrics_legacy(long: pd.DataFrame) -> pd.DataFrame:
    """Reproduce the original positional pairing of power and emissions.

    The original took every power row and every emissions row of the whole extract and
    paired them by position (``.values``). That is only correct when every unit has exactly
    the same timestamps for both metrics, in the same order; one missing interval shifts
    every later row onto the wrong unit or time. It also used ``dropna()`` on all columns.

    Raises:
        ValueError: When the two metrics have a different number of rows (the original
            crashed in the same way).
    """
    require_columns(long, RAW_FACILITY_COLUMNS, "facility long data")
    power = long[long["metric"] == "power"].reset_index(drop=True)
    emissions = long[long["metric"] == "emissions"].reset_index(drop=True)
    if len(power) != len(emissions):
        raise ValueError(
            f"Positional pairing impossible: {len(power)} power rows "
            f"vs {len(emissions)} emissions rows"
        )
    out = power[["facility_code", "unit_code", "timestamp"]].copy()
    out["power_mw"] = power["value"].to_numpy()
    out["emissions_t"] = emissions["value"].to_numpy()
    out = out.dropna()
    return _drop_idle(out).reset_index(drop=True)[FACILITY_METRIC_COLUMNS]


def facility_metrics(long: pd.DataFrame) -> pd.DataFrame:
    """Pivot power and emissions on ``(facility, unit, timestamp)`` keys.

    Pairing on keys instead of position keeps every value on its own unit and interval.
    Missing emissions stay NaN instead of discarding the power reading.
    """
    require_columns(long, RAW_FACILITY_COLUMNS, "facility long data")
    keys = ["facility_code", "unit_code", "timestamp"]
    if long.duplicated([*keys, "metric"]).any():
        long = long.drop_duplicates([*keys, "metric"], keep="last")
    wide = long.pivot(index=keys, columns="metric", values="value").reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={"power": "power_mw", "emissions": "emissions_t"})
    for col in ("power_mw", "emissions_t"):
        if col not in wide.columns:
            wide[col] = float("nan")
    wide = wide.sort_values(keys, kind="stable")
    return _drop_idle(wide).reset_index(drop=True)[FACILITY_METRIC_COLUMNS]


def market_metrics_legacy(long: pd.DataFrame) -> pd.DataFrame:
    """Reproduce the original positional pairing of price and demand rows.

    Raises:
        ValueError: When price and demand have a different number of rows.
    """
    require_columns(long, RAW_MARKET_COLUMNS, "market long data")
    price = long[long["metric"] == "price"].reset_index(drop=True)
    demand = long[long["metric"] == "demand"].reset_index(drop=True)
    if len(price) != len(demand):
        raise ValueError(
            f"Positional pairing impossible: {len(price)} price vs {len(demand)} demand rows"
        )
    out = price[["timestamp", "region"]].copy()
    out["price_aud_mwh"] = price["value"].to_numpy()
    out["demand_mw"] = demand["value"].to_numpy()
    return out[MARKET_COLUMNS]


def market_metrics(long: pd.DataFrame) -> pd.DataFrame:
    """Pivot price and demand on ``(timestamp, region)`` keys."""
    require_columns(long, RAW_MARKET_COLUMNS, "market long data")
    keys = ["timestamp", "region"]
    long = long.drop_duplicates([*keys, "metric"], keep="last")
    wide = long.pivot(index=keys, columns="metric", values="value").reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={"price": "price_aud_mwh", "demand": "demand_mw"})
    for col in ("price_aud_mwh", "demand_mw"):
        if col not in wide.columns:
            wide[col] = float("nan")
    return wide.sort_values(keys, kind="stable").reset_index(drop=True)[MARKET_COLUMNS]


def misalignment_rate(legacy: pd.DataFrame, correct: pd.DataFrame) -> dict[str, float | int]:
    """Measure how many legacy rows carry an emissions value from another unit or interval.

    Both frames are joined on ``(unit_code, timestamp)``; a row is wrong when its emissions
    differ from the key-based value by more than 1e-9.
    """
    keys = ["unit_code", "timestamp"]
    joined = legacy.merge(correct, on=keys, how="left", suffixes=("_legacy", "_correct"))
    wrong = (joined["emissions_t_legacy"] - joined["emissions_t_correct"]).abs() > 1e-9
    wrong |= joined["emissions_t_correct"].isna()
    rows = len(joined)
    return {
        "legacy_rows": rows,
        "correct_rows": len(correct),
        "misaligned_rows": int(wrong.sum()),
        "misaligned_rate": float(wrong.mean()) if rows else 0.0,
        "rows_lost_vs_correct": len(correct) - rows,
    }
