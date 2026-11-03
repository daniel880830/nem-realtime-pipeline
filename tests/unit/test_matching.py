import re

import numpy as np
import pandas as pd
import pytest
from rapidfuzz import fuzz, process

from nem_pipeline import matching


def stations_df():
    return pd.DataFrame(
        {
            "project_name": [
                "Mansfield Solar Farm",
                "Bayswater",
                "Smith Farm",
                "Hornsdale Wind Farm",
                "Ps",
            ],
            "state": ["VIC", "NSW", "NSW", "SA", "QLD"],
            "latitude": [-37.06, -32.40, -33.87, -33.07, -27.0],
            "longitude": [146.10, 150.95, 151.21, 138.55, 153.0],
        }
    )


def facilities_df():
    return pd.DataFrame(
        {
            "facility_code": ["MANSF", "BAYSW", "HORNS", "SMITHVIC", "NOLOC"],
            "facility_name": [
                "Mansfield Solar Farm",
                "Bayswater",
                "Hornsdale Wind Farm",
                "Smith Farm",
                "Nowhere",
            ],
            "state": ["VIC", "NSW", "SA", "VIC", "QLD"],
            "latitude": [-37.05, -32.395, -33.06, -37.5, np.nan],
            "longitude": [146.09, 150.949, 138.54, 144.0, np.nan],
        }
    )


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Mansfield Solar Farm", "manield"),
        ("Capsicum Wind Farm", "caicum"),
        (None, ""),
        (float("nan"), ""),
    ],
)
def test_normalize_legacy_reproduces_substring_bug(name, expected):
    assert matching.normalize_legacy(name) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [("Mansfield Solar Farm", "mansfield"), ("Capsicum Wind Farm", "capsicum"), ("Bango 973 WF", "bango 973"),
     (None, "")],
)  # fmt: skip
def test_normalize_improved_word_boundaries(name, expected):
    assert matching.normalize_improved(name) == expected


def original_row_wise(df_db: pd.DataFrame, df_power: pd.DataFrame) -> pd.DataFrame:
    """Verbatim logic of the original ``merge_with_duckdb_location_fuzzy`` (minus file I/O)."""
    df_db = df_db.copy()
    df_power = df_power.copy()
    df_db["clean_name"] = df_db["projectName"].apply(matching.normalize_legacy)
    df_power["clean_name"] = df_power["Facility Name"].apply(matching.normalize_legacy)
    coord_names = (
        df_db.drop_duplicates(subset=["clean_name"])
        .set_index("clean_name")[["state", "latitude", "longitude"]]
        .to_dict(orient="index")
    )
    db_names = list(coord_names)
    mv = process.cdist(
        df_power["clean_name"], db_names, scorer=fuzz.partial_ratio, workers=-1, score_cutoff=70
    )
    fb = mv.max(axis=1) < 70
    names = [df_power["clean_name"].iloc[i] for i in range(len(df_power)) if fb[i]]
    if names:
        mv[fb] = process.cdist(
            names, db_names, scorer=fuzz.token_sort_ratio, workers=-1, score_cutoff=65
        )
    idx, sc = mv.argmax(axis=1), mv.max(axis=1)
    best = [db_names[i] if s > 0 else None for i, s in zip(idx, sc, strict=True)]
    df_power["latitude"] = [coord_names.get(n, {}).get("latitude") for n in best]
    df_power["longitude"] = [coord_names.get(n, {}).get("longitude") for n in best]
    return df_power.dropna()


def test_match_legacy_equals_original_row_wise(pipeline_config):
    fac, st = facilities_df(), stations_df()
    ours = matching.match_legacy(fac, st, pipeline_config.matching).set_index("facility_code")
    rows = fac.loc[fac.index.repeat(3), ["facility_code", "facility_name"]].rename(
        columns={"facility_name": "Facility Name"}
    )
    orig = original_row_wise(
        st.rename(columns={"project_name": "projectName"}), rows
    ).drop_duplicates("facility_code")
    orig = orig.set_index("facility_code")
    assert set(ours.index) == set(orig.index)
    for code in ours.index:
        assert ours.loc[code, "latitude"] == pytest.approx(orig.loc[code, "latitude"])


def test_match_legacy_crosses_states(pipeline_config):
    out = matching.match_legacy(facilities_df(), stations_df(), pipeline_config.matching).set_index(
        "facility_code"
    )
    assert out.loc["SMITHVIC", "cer_state"] == "NSW"  # VIC facility matched to a NSW station


def test_match_improved_blocks_by_state_and_skips_short_names(pipeline_config):
    out = matching.match_improved(facilities_df(), stations_df(), pipeline_config.matching)
    assert set(out["facility_code"]) == {"MANSF", "BAYSW", "HORNS"}
    assert (
        out["cer_state"]
        == facilities_df().set_index("facility_code").loc[out["facility_code"], "state"].values
    ).all()


def test_match_empty_inputs(pipeline_config):
    empty = stations_df().iloc[0:0]
    for fn in (matching.match_legacy, matching.match_improved):
        assert fn(facilities_df(), empty, pipeline_config.matching).empty
        assert fn(facilities_df().iloc[0:0], stations_df(), pipeline_config.matching).empty
    no_state = stations_df().assign(state="WA")
    assert matching.match_improved(facilities_df(), no_state, pipeline_config.matching).empty


def test_haversine_known_distance():
    # Sydney to Melbourne is about 714 km.
    d = matching.haversine_km(
        np.array([-33.8688]), np.array([151.2093]), np.array([-37.8136]), np.array([144.9631])
    )
    assert d[0] == pytest.approx(714, abs=5)


def test_evaluate_matches(pipeline_config):
    fac = facilities_df()
    legacy = matching.match_legacy(fac, stations_df(), pipeline_config.matching)
    metrics = matching.evaluate_matches(legacy, fac, 25.0)
    assert metrics["facilities"] == 5 and metrics["facilities_with_truth"] == 4
    assert metrics["cross_state_matches"] >= 1
    assert metrics["correct"] == 3
    assert metrics["precision"] == pytest.approx(3 / metrics["evaluated"])
    empty = matching.evaluate_matches(legacy.iloc[0:0], fac, 25.0)
    assert empty["precision"] is None and empty["median_error_km"] is None


def test_improved_normaliser_never_returns_generic_words():
    for word in ["solar farm", "wind farm", "power station", "battery energy storage"]:
        assert not re.search(
            r"\b(solar|farm|wind|power|station|battery)\b", matching.normalize_improved(word)
        )
