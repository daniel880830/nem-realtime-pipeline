import pandas as pd
import pytest
import requests

from nem_pipeline import cer
from nem_pipeline.schema import CER_COLUMNS, SchemaError
from tests.conftest import CER_CSVS, FakeResponse, FakeSession

PAGE = """
<div class="vue-datavis-wrapper"><div class="cer-accordion__body">
  <a href="/document/power-stations-and-projects-status">status</a>
  <a href="/document/power-stations-and-projects-accredited">csv</a>
</div></div>
<a href="https://cer.gov.au/document/power-stations-and-projects-committed">c</a>
<a href="/document/power-stations-and-projects-probable">p</a>
<a href="/document/power-stations-and-projects-probable-old">dup</a>
"""


def test_find_dataset_urls():
    urls = cer.find_dataset_urls(
        PAGE, "https://cer.gov.au/", ("accredited", "committed", "probable")
    )
    assert (
        urls["accredited"] == "https://cer.gov.au/document/power-stations-and-projects-accredited"
    )
    assert urls["committed"].startswith("https://cer.gov.au/document/")
    assert urls["probable"].endswith("-probable")


def test_find_dataset_urls_missing():
    with pytest.raises(cer.CerError, match="probable"):
        cer.find_dataset_urls("<a href='/x-accredited'>a</a>", "https://x", ("probable",))


def test_parse_capacity_handles_thousands_separator():
    out = cer.parse_capacity(pd.Series(["1,400.000", " 5 ", "n/a", None]))
    assert out.iloc[0] == 1400.0 and out.iloc[1] == 5.0
    assert out.iloc[2:].isna().all()


def test_integrate_renames_and_types(cer_frames):
    df = cer.integrate(cer_frames)
    assert list(df.columns) == CER_COLUMNS
    assert df.loc[df["project_name"] == "Bayswater", "mw_capacity"].item() == 1320.0
    assert set(df["status"]) == {"accredited", "committed", "probable"}
    assert df.loc[df["status"] == "committed", "postcode"].isna().all()


def test_integrate_missing_column_and_empty(cer_frames):
    cer_frames["committed"] = cer_frames["committed"].drop(columns=["Fuel Source"])
    with pytest.raises(SchemaError, match="committed"):
        cer.integrate(cer_frames)
    with pytest.raises(SchemaError):
        cer.integrate({})


def test_clean_legacy_merges_distinct_stations(cer_frames):
    df = cer.clean_legacy(cer.integrate(cer_frames))
    smith = df[df["project_name"] == "Smith Farm"]
    assert len(smith) == 1  # two different sites collapsed into one
    assert smith["mw_capacity"].item() == pytest.approx(0.3)  # averaged


def test_clean_improved_keeps_stations_and_drops_bad_states(cer_frames):
    df = cer.clean_improved(cer.integrate(cer_frames))
    assert {"Smith Farm", "Smith Farm Shed 2", "Mansfield Solar Farm"} <= set(df["project_name"])
    assert "Bad Row" not in set(df["project_name"])


def test_clean_improved_prefers_most_advanced_status():
    df = pd.DataFrame(
        {"project_name": ["A Farm", "A Farm"], "state": ["nsw", "NSW"], "mw_capacity": [1.0, 2.0],
         "fuel_source": ["Solar", "Solar"], "status": ["probable", "accredited"], "postcode": [None, None]}
    )  # fmt: skip
    out = cer.clean_improved(df)
    assert len(out) == 1 and out["status"].item() == "accredited"


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("DONS FORT - SOLAR W SGU - QLD", "DONS FORT"),
        ("Ventora - Acacia Ridge - Solar - QLD", "Ventora Acacia Ridge"),
        ("Coca-Cola Amatil", "Coca-Cola Amatil"),
        ("Solar - NSW", "Solar"),
    ],
)
def test_base_name(name, expected):
    assert cer.base_name(name) == expected


def test_fetch_text_retries_and_fails(monkeypatch):
    monkeypatch.setattr(cer.time, "sleep", lambda s: None)
    session = FakeSession(lambda u, p: requests.Timeout("slow"))
    with pytest.raises(cer.CerError):
        cer.fetch_text("https://x", timeout_s=1, max_retries=2, session=session)
    assert len(session.calls) == 2
    ok = FakeSession(lambda u, p: FakeResponse(text="hello"))
    assert cer.fetch_text("https://x", timeout_s=1, max_retries=1, session=ok) == "hello"


def test_download_and_read_datasets(tmp_path, pipeline_config):
    def responder(url, params):
        if url == pipeline_config.cer.page_url:
            return FakeResponse(text=PAGE)
        return FakeResponse(text=CER_CSVS[url.rsplit("-", 1)[-1]])

    session = FakeSession(responder)
    paths = cer.download_datasets(pipeline_config.cer, tmp_path, session=session)
    frames = cer.read_datasets(paths)
    assert set(frames) == {"accredited", "committed", "probable"}
    calls = len(session.calls)
    cer.download_datasets(pipeline_config.cer, tmp_path, session=session)
    assert len(session.calls) == calls  # cached


def test_read_datasets_missing_and_empty(tmp_path):
    with pytest.raises(cer.CerError, match="not found"):
        cer.read_datasets({"a": tmp_path / "a.csv"})
    (tmp_path / "e.csv").write_text("", encoding="utf-8")
    with pytest.raises(cer.CerError, match="empty"):
        cer.read_datasets({"e": tmp_path / "e.csv"})
