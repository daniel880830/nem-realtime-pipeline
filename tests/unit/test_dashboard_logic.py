import json

import pandas as pd
import pytest

from nem_pipeline.dashboard import logic

TOPICS = {"facility_topic": "f", "market_topic": "m"}
READING = {"facility_code": "BAYSW", "facility_name": "Bayswater", "state": "NSW", "fuel_type": "Coal Black",
           "latitude": -32.4, "longitude": 150.9, "power_mw": 900.0, "emissions_t": 72.0}  # fmt: skip


def base_frame():
    return pd.DataFrame(
        {"facility_code": ["BAYSW", "HORNS"], "facility_name": ["Bayswater", "Hornsdale"], "state": ["NSW", "SA"],
         "fuel_type": ["Coal Black", "Wind"], "latitude": [-32.4, -33.0], "longitude": [150.9, 138.5]}
    )  # fmt: skip


def test_ingest_facility_and_market():
    state = logic.LiveState()
    assert state.ingest(
        "f", json.dumps({"timestamp": "2025-10-01 00:00", "data": [READING]}), **TOPICS
    )
    market = {"timestamp": "2025-10-01 00:00", "data": [{"region": "VIC1", "price_aud_mwh": 50, "demand_mw": None},
                                                        {"region": "NSW1", "price_aud_mwh": 100, "demand_mw": 7000}]}  # fmt: skip
    assert state.ingest("m", json.dumps(market).encode(), **TOPICS)
    assert state.facility_timestamp == "2025-10-01 00:00"
    assert state.market_frame()["region"].tolist() == ["NSW1", "VIC1"]


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        b"\xff\xfe",
        json.dumps({"timestamp": "t", "data": [{**READING, "latitude": 999}]}),  # out of range
        json.dumps({"timestamp": "t", "data": [{**READING, "extra": 1}]}),  # extra field
        json.dumps(
            {"timestamp": "t", "data": [{k: v for k, v in READING.items() if k != "power_mw"}]}
        ),
        json.dumps({"timestamp": "t", "data": [{**READING, "power_mw": "lots"}]}),  # wrong type
    ],
)
def test_ingest_rejects_invalid(raw):
    state = logic.LiveState()
    assert not state.ingest("f", raw, **TOPICS)
    assert state.rejected == 1 and state.facilities == {}


def test_ingest_unknown_topic_ignored():
    state = logic.LiveState()
    assert not state.ingest("other", "{}", **TOPICS)
    assert state.rejected == 0


def test_facility_frame_before_and_after_data():
    state = logic.LiveState()
    empty = state.facility_frame(base_frame())
    assert empty["power_mw"].isna().all() and empty["fuel_group"].tolist() == ["Coal", "Wind"]
    state.ingest("f", json.dumps({"timestamp": "t", "data": [READING]}), **TOPICS)
    out = state.facility_frame(base_frame()).set_index("facility_code")
    assert out.loc["BAYSW", "power_mw"] == 900.0 and pd.isna(out.loc["HORNS", "power_mw"])
    assert out.loc["BAYSW", "color"] == [0, 0, 0]


@pytest.mark.parametrize(
    ("fuel", "group"),
    [("Gas Ocgt", "Gas"), ("Battery Discharging", "Battery"), ("Bioenergy Biogas", "Bioenergy"),
     ("Solar Utility", "Solar"), (None, "Unknown"), ("Nuclear", "Unknown")],
)  # fmt: skip
def test_fuel_group(fuel, group):
    assert logic.fuel_group(fuel) == group


def test_hex_to_rgb():
    assert logic.hex_to_rgb("#ff9f1c") == [255, 159, 28]
    with pytest.raises(ValueError):
        logic.hex_to_rgb("#fff")


def test_filter_facilities():
    df = logic.LiveState().facility_frame(base_frame())
    assert len(logic.filter_facilities(df, ["NSW"], ["Coal", "Wind"])) == 1
    assert logic.filter_facilities(df, [], ["Coal"]).empty
