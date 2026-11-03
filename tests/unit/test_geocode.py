import json

import pandas as pd
import pytest
import requests

from nem_pipeline import geocode
from tests.conftest import FakeResponse, FakeSession, fake_nominatim


class Clock:
    def __init__(self):
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self):
        return self.now

    def sleep(self, s):
        self.slept.append(s)
        self.now += s


def _geocoder(geocode_config, responder=fake_nominatim, cache=None):
    clock = Clock()
    g = geocode.NominatimGeocoder(
        geocode_config,
        "test-agent",
        cache,
        session=FakeSession(responder),
        sleep=clock.sleep,
        clock=clock,
    )
    return g, clock


def test_geocode_hit_sets_user_agent_and_params(geocode_config):
    g, _ = _geocoder(geocode_config)
    assert g.geocode("Bayswater, NSW, Australia") == (-32.40, 150.95)
    _, params = g._session.calls[0]
    assert params["countrycodes"] == "au" and params["limit"] == 1
    assert g._session.headers["User-Agent"] == "test-agent"


def test_rate_limit_one_request_per_second(geocode_config):
    g, clock = _geocoder(geocode_config)
    g.geocode("Bayswater, NSW, Australia")
    g.geocode("Nowhere, NSW, Australia")
    assert clock.slept == [pytest.approx(1.0)]


def test_cache_hits_and_misses_are_not_requested_twice(geocode_config, tmp_path):
    cache = tmp_path / "c.json"
    g, _ = _geocoder(geocode_config, cache=cache)
    assert g.geocode("Nowhere, NSW, Australia") is None
    assert g.geocode("Nowhere,   NSW, Australia") is None  # whitespace-normalised
    assert g.requests_sent == 1
    g.save_cache()
    g2, _ = _geocoder(geocode_config, cache=cache)
    g2.geocode("Nowhere, NSW, Australia")
    assert g2.requests_sent == 0


def test_transient_failure_not_cached(geocode_config):
    answers = iter([requests.Timeout("slow"), FakeResponse(payload=[{"lat": "1", "lon": "2"}])])
    g, _ = _geocoder(geocode_config, responder=lambda u, p: next(answers))
    assert g.geocode("X, NSW, Australia") is None
    assert g.geocode("X, NSW, Australia") == (1.0, 2.0)


def test_bad_json_and_empty_query(geocode_config):
    g, _ = _geocoder(geocode_config, responder=lambda u, p: FakeResponse(text="<html>"))
    assert g.geocode("X") is None
    assert g.geocode("   ") is None


def test_corrupt_cache_ignored(geocode_config, tmp_path):
    cache = tmp_path / "c.json"
    cache.write_text("{oops", encoding="utf-8")
    g, _ = _geocoder(geocode_config, cache=cache)
    assert g.geocode("Bayswater, NSW, Australia") is not None
    g.save_cache()
    assert "Bayswater, NSW, Australia" in json.loads(cache.read_text(encoding="utf-8"))


def test_query_builders():
    row = pd.Series({"project_name": "Mansfield Solar Farm", "state": "VIC", "postcode": "3722"})
    assert geocode.legacy_queries(row) == [("name", "Mansfield Solar Farm, VIC, Australia")]
    labels = [label for label, _ in geocode.improved_queries(row)]
    assert labels == ["name", "short_name", "postcode"]
    plain = pd.Series({"project_name": "Bayswater", "state": "NSW", "postcode": None})
    assert [label for label, _ in geocode.improved_queries(plain)] == ["name"]


def test_geocode_stations_uses_fallback_chain(geocode_config):
    g, _ = _geocoder(geocode_config)
    df = pd.DataFrame(
        {"project_name": ["Bayswater", "Unknown Solar Farm", "Ghost"], "state": ["NSW", "VIC", "SA"],
         "postcode": [None, "3722", None]}
    )  # fmt: skip
    out = geocode.geocode_stations(df, g, geocode.improved_queries)
    assert out["geocode_method"].tolist() == ["name", "postcode", None]
    assert out["latitude"].notna().tolist() == [True, True, False]
