import numpy as np
import pandas as pd
import pytest

from nem_pipeline import extract, transform
from nem_pipeline.schema import SchemaError
from tests.conftest import FACILITIES_PAYLOAD, FACILITY_DATA_PAYLOAD, MARKET_PAYLOAD


@pytest.fixture
def long_df():
    _, units = extract.parse_facilities(FACILITIES_PAYLOAD)
    return extract.parse_facility_data(FACILITY_DATA_PAYLOAD, units)


def test_keyed_and_positional_agree_on_aligned_data(long_df):
    correct = transform.facility_metrics(long_df)
    legacy = transform.facility_metrics_legacy(long_df)
    pd.testing.assert_frame_equal(
        correct.sort_values(["unit_code", "timestamp"]).reset_index(drop=True),
        legacy.sort_values(["unit_code", "timestamp"]).reset_index(drop=True),
    )
    assert transform.misalignment_rate(legacy, correct)["misaligned_rows"] == 0


def test_idle_rows_dropped(long_df):
    out = transform.facility_metrics(long_df)
    msf = out[out["unit_code"] == "MSF1"]
    assert len(msf) == 1 and msf["power_mw"].item() == 5.0
    bw02 = out[out["unit_code"] == "BW02"]
    assert len(bw02) == 2  # the 0/0 interval is removed


def test_positional_pairing_shifts_after_a_gap(long_df):
    """One missing power and one missing emissions reading in different units shifts every row between them."""
    gap_power = (
        (long_df["unit_code"] == "BW01")
        & (long_df["metric"] == "power")
        & (long_df["timestamp"] == long_df["timestamp"].min())
    )
    gap_emis = (
        (long_df["unit_code"] == "HPR1")
        & (long_df["metric"] == "emissions")
        & (long_df["timestamp"] == long_df["timestamp"].max())
    )
    broken = long_df[~(gap_power | gap_emis)]
    correct = transform.facility_metrics(broken)
    legacy = transform.facility_metrics_legacy(broken)
    stats = transform.misalignment_rate(legacy, correct)
    assert stats["misaligned_rows"] > 0
    assert stats["misaligned_rate"] > 0.3


def test_positional_pairing_crashes_on_unequal_counts(long_df):
    drop = long_df.index[(long_df["metric"] == "emissions")][:1]
    with pytest.raises(ValueError, match="Positional pairing impossible"):
        transform.facility_metrics_legacy(long_df.drop(drop))


def test_keyed_pairing_keeps_power_when_emissions_missing(long_df):
    long_df = long_df[~((long_df["unit_code"] == "HDWF1") & (long_df["metric"] == "emissions"))]
    out = transform.facility_metrics(long_df)
    hd = out[out["unit_code"] == "HDWF1"]
    assert len(hd) == 3 and hd["emissions_t"].isna().all()


def test_duplicates_and_missing_metric_column():
    df = pd.DataFrame(
        {"facility_code": ["F"] * 3, "unit_code": ["U"] * 3,
         "timestamp": pd.to_datetime(["2025-01-01"] * 3), "metric": ["power"] * 3, "value": [1.0, 2.0, 3.0]}
    )  # fmt: skip
    out = transform.facility_metrics(df)
    assert out["power_mw"].tolist() == [3.0]
    assert np.isnan(out["emissions_t"].item())


def test_missing_columns_raise():
    with pytest.raises(SchemaError):
        transform.facility_metrics(pd.DataFrame({"unit_code": []}))
    with pytest.raises(SchemaError):
        transform.market_metrics(pd.DataFrame({"region": []}))


def test_market_pivot_and_legacy_agree():
    long = extract.parse_market(MARKET_PAYLOAD)
    keyed = transform.market_metrics(long)
    legacy = transform.market_metrics_legacy(long)
    assert len(keyed) == 6
    pd.testing.assert_frame_equal(
        keyed.sort_values(["timestamp", "region"]).reset_index(drop=True),
        legacy.sort_values(["timestamp", "region"]).reset_index(drop=True),
    )
    nsw = keyed[keyed["region"] == "NSW1"].iloc[0]
    assert nsw["price_aud_mwh"] == 100.0 and nsw["demand_mw"] == 100.0


def test_market_legacy_crash_and_missing_metric():
    long = extract.parse_market(MARKET_PAYLOAD)
    with pytest.raises(ValueError):
        transform.market_metrics_legacy(long.iloc[1:])
    only_price = transform.market_metrics(long[long["metric"] == "price"])
    assert only_price["demand_mw"].isna().all()


def test_empty_long_frame():
    empty = pd.DataFrame(columns=["facility_code", "unit_code", "timestamp", "metric", "value"])
    assert transform.facility_metrics(empty).empty
