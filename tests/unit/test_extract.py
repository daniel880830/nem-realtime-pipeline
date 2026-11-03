import json
from datetime import datetime

import pandas as pd
import pytest
import requests

from nem_pipeline import extract
from tests.conftest import (
    FACILITIES_PAYLOAD,
    FACILITY_DATA_PAYLOAD,
    MARKET_PAYLOAD,
    FakeOEClient,
    FakeResponse,
    FakeSession,
)


def test_date_windows_split_and_cover():
    w = extract.date_windows(datetime(2025, 10, 1), datetime(2025, 10, 16), 7)
    assert w[0] == (datetime(2025, 10, 1), datetime(2025, 10, 8))
    assert w[-1] == (datetime(2025, 10, 15), datetime(2025, 10, 16))
    assert len(w) == 3


@pytest.mark.parametrize(("start", "end", "days"), [(2, 1, 7), (1, 1, 7), (1, 2, 0)])
def test_date_windows_invalid(start, end, days):
    with pytest.raises(ValueError):
        extract.date_windows(datetime(2025, 1, start), datetime(2025, 1, end), days)


def test_batched():
    assert list(extract.batched(list("abcde"), 2)) == [["a", "b"], ["c", "d"], ["e"]]
    assert list(extract.batched([], 3)) == []
    with pytest.raises(ValueError):
        list(extract.batched(["a"], 0))


def test_parse_facilities_filters_status_and_maps_region():
    fac, units = extract.parse_facilities(FACILITIES_PAYLOAD)
    assert set(fac["facility_code"]) == {"BAYSW", "MANSF", "HORNS", "NOLOC"}  # retired excluded
    row = fac.set_index("facility_code").loc["HORNS"]
    assert row["state"] == "SA"
    assert row["fuel_type"] == "Battery Discharging"  # largest unit wins
    assert pd.isna(fac.set_index("facility_code").loc["NOLOC", "latitude"])
    assert set(units["unit_code"]) >= {"BW01", "BW02", "HPR1"}


def test_parse_facilities_empty():
    fac, units = extract.parse_facilities({"data": []})
    assert fac.empty and units.empty
    assert fac.columns[0] == "facility_code"


def test_parse_facility_data_converts_time_and_skips_unknown_units():
    _, units = extract.parse_facilities(FACILITIES_PAYLOAD)
    payload = json.loads(json.dumps(FACILITY_DATA_PAYLOAD))
    payload["data"][0]["results"].append(
        {
            "name": "power_X",
            "columns": {"unit_code": "UNKNOWN"},
            "data": [["2025-10-01T00:00:00+10:00", 1]],
        }
    )
    df = extract.parse_facility_data(payload, units)
    assert "UNKNOWN" not in set(df["unit_code"])
    assert df["timestamp"].min() == pd.Timestamp("2025-10-01 00:00:00")
    assert df["timestamp"].dt.tz is None
    assert len(df) == 2 * 5 * 3


def test_to_market_time_from_utc():
    out = extract.to_market_time(pd.Series(["2025-09-30T14:00:00Z"]))
    assert out.iloc[0] == pd.Timestamp("2025-10-01 00:00:00")


def test_parse_market_region_and_fallback_name():
    payload = json.loads(json.dumps(MARKET_PAYLOAD))
    del payload["data"][0]["results"][0]["columns"]["network_region"]
    df = extract.parse_market(payload)
    assert set(df["region"]) == {"NSW1", "VIC1"}
    assert len(df) == 2 * 2 * 3


def _client(responder, **kw):
    session = FakeSession(responder)
    return extract.OpenElectricityClient("k", session=session, backoff_s=0, **kw), session


def test_client_sends_bearer_and_repeated_params():
    client, session = _client(lambda u, p: FakeResponse(payload={"success": True, "data": []}))
    client.facility_data(
        "NEM", ["A", "B"], ["power", "emissions"], "5m", datetime(2025, 1, 1), datetime(2025, 1, 2)
    )
    assert session.headers["Authorization"] == "Bearer k"
    url, params = session.calls[0]
    assert url.endswith("/data/facilities/NEM")
    assert ("facility_code", "A") in params and ("facility_code", "B") in params
    assert ("date_start", "2025-01-01T00:00:00") in params


def test_client_retries_then_succeeds():
    answers = iter(
        [
            requests.ConnectionError("boom"),
            FakeResponse(503),
            FakeResponse(payload={"success": True}),
        ]
    )
    client, session = _client(lambda u, p: next(answers))
    assert client.facilities()["success"] is True
    assert len(session.calls) == 3


def test_client_gives_up_after_max_retries():
    client, session = _client(lambda u, p: FakeResponse(429), max_retries=2)
    with pytest.raises(extract.OpenElectricityError, match="after 2 attempts"):
        client.facilities()
    assert len(session.calls) == 2


def test_client_fails_fast_on_auth_error():
    client, session = _client(lambda u, p: FakeResponse(401, text="unauthorised"))
    with pytest.raises(extract.OpenElectricityError, match="401"):
        client.facilities()
    assert len(session.calls) == 1


def test_client_unsuccessful_payload():
    client, _ = _client(lambda u, p: FakeResponse(payload={"success": False, "error": "bad range"}))
    with pytest.raises(extract.OpenElectricityError, match="bad range"):
        client.market("NEM", ["price"], "5m", datetime(2025, 1, 1), datetime(2025, 1, 2))


def test_client_requires_key_and_codes():
    with pytest.raises(extract.OpenElectricityError):
        extract.OpenElectricityClient("")
    client, _ = _client(lambda u, p: FakeResponse(payload={"success": True}))
    with pytest.raises(extract.OpenElectricityError, match="empty"):
        client.facility_data("NEM", [], ["power"], "5m", datetime(2025, 1, 1), datetime(2025, 1, 2))


def test_download_raw_caches(tmp_path, pipeline_config):
    cfg = pipeline_config.extract.model_copy(update={"facility_batch_size": 3})
    client = FakeOEClient()
    snap = extract.download_raw(client, cfg, tmp_path)
    assert len(snap.facility_data) == 2  # 4 facilities in batches of 3
    assert client.calls.count("market") == 1
    n_calls = len(client.calls)
    extract.download_raw(client, cfg, tmp_path)
    assert len(client.calls) == n_calls  # everything served from cache
    extract.download_raw(client, cfg, tmp_path, refresh=True)
    assert len(client.calls) == 2 * n_calls


def test_load_raw_and_corrupt_file(tmp_path, pipeline_config):
    snap = extract.download_raw(FakeOEClient(), pipeline_config.extract, tmp_path)
    fac, _units, fl, ml = extract.load_raw(snap)
    assert len(fac) == 4 and not fl.empty and not ml.empty
    snap.market[0].write_text("{not json", encoding="utf-8")
    with pytest.raises(extract.OpenElectricityError, match="Corrupt"):
        extract.load_raw(snap)


def test_load_raw_without_batches(tmp_path):
    path = tmp_path / "facilities.json"
    path.write_text(json.dumps(FACILITIES_PAYLOAD), encoding="utf-8")
    _, _, fl, ml = extract.load_raw(extract.RawSnapshot(path, [], []))
    assert fl.empty and ml.empty
