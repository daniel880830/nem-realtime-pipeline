"""Fuzzy matching of OpenElectricity facilities to geocoded CER stations, and its evaluation.

OpenElectricity publishes each facility's real coordinates, which makes it possible to
measure how often the name-based join places a facility at the wrong site.
"""

from __future__ import annotations

import math
import re

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from nem_pipeline.config import MatchingConfig

MATCH_COLUMNS = ["facility_code", "cer_project_name", "cer_state", "score", "latitude", "longitude"]

_LEGACY_TERMS = [
    "solar farm", "wind farm", "power station", "power plant", "energy",
    "battery", "ps", "wf", "sf", "farm", "plant", "station",
]  # fmt: skip


def normalize_legacy(name: object) -> str:
    """Reproduce the original normaliser.

    It removes the terms with ``str.replace``, i.e. also *inside* words: ``"Mansfield"``
    becomes ``"manield"`` and ``"Capsicum"`` becomes ``"caicum"``.
    """
    if name is None or (isinstance(name, float) and math.isnan(name)):
        return ""
    name = re.sub(r"[^A-Za-z0-9 ]+", "", str(name)).lower()
    for term in _LEGACY_TERMS:
        name = name.replace(term, "")
    return re.sub(r"\s+", " ", name).strip()


_GENERIC_WORDS = re.compile(
    r"\b(solar|wind|farm|park|power|station|plant|energy|renewable|battery|bess|"
    r"storage|hybrid|project|hub|facility|ps|wf|sf|pty|ltd|stage|the)\b"
)


def normalize_improved(name: object) -> str:
    """Lower-case, strip punctuation and drop generic words on word boundaries only."""
    if name is None or (isinstance(name, float) and math.isnan(name)):
        return ""
    text = re.sub(r"[^a-z0-9 ]+", " ", str(name).lower())
    text = _GENERIC_WORDS.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _empty_matches() -> pd.DataFrame:
    return pd.DataFrame(columns=MATCH_COLUMNS)


def match_legacy(
    facilities: pd.DataFrame, stations: pd.DataFrame, cfg: MatchingConfig
) -> pd.DataFrame:
    """Reproduce the original join: ``partial_ratio`` >= 70, else ``token_sort_ratio`` >= 65.

    Matching ignores the state, and duplicate normalised CER names keep only the first row.
    The original scored every metric row; scoring each unique facility name gives the same
    result because the scorers are deterministic.
    """
    stations = stations.dropna(subset=["latitude", "longitude"]).copy()
    if facilities.empty or stations.empty:
        return _empty_matches()
    stations["clean"] = stations["project_name"].map(normalize_legacy)
    lookup = stations.drop_duplicates("clean").set_index("clean")
    choices = list(lookup.index)
    queries = facilities["facility_name"].map(normalize_legacy).tolist()

    scores = process.cdist(
        queries,
        choices,
        scorer=fuzz.partial_ratio,
        workers=-1,
        score_cutoff=cfg.legacy_partial_cutoff,
    )
    fallback = scores.max(axis=1) < cfg.legacy_partial_cutoff
    if fallback.any():
        scores[fallback] = process.cdist(
            [q for q, f in zip(queries, fallback, strict=True) if f],
            choices,
            scorer=fuzz.token_sort_ratio,
            workers=-1,
            score_cutoff=cfg.legacy_token_sort_cutoff,
        )
    best = scores.argmax(axis=1)
    best_score = scores.max(axis=1)
    rows = []
    for code, idx, score in zip(facilities["facility_code"], best, best_score, strict=True):
        if score <= 0:
            continue
        hit = lookup.iloc[idx]
        rows.append(
            (
                code,
                hit["project_name"],
                hit["state"],
                float(score),
                hit["latitude"],
                hit["longitude"],
            )
        )
    return pd.DataFrame(rows, columns=MATCH_COLUMNS)


def match_improved(
    facilities: pd.DataFrame, stations: pd.DataFrame, cfg: MatchingConfig
) -> pd.DataFrame:
    """Match within the same state using ``token_set_ratio`` on word-boundary normalised names.

    Names that are empty after normalisation are never matched, which stops very short
    strings from matching everything.
    """
    stations = stations.dropna(subset=["latitude", "longitude"]).copy()
    if facilities.empty or stations.empty:
        return _empty_matches()
    stations["clean"] = stations["project_name"].map(normalize_improved)
    stations = stations[stations["clean"].str.len() >= 3]
    rows = []
    for state, group in facilities.groupby("state", dropna=True):
        candidates = stations[stations["state"] == state]
        if candidates.empty:
            continue
        choices = candidates["clean"].tolist()
        for code, name in zip(group["facility_code"], group["facility_name"], strict=True):
            query = normalize_improved(name)
            if len(query) < 3:
                continue
            hit = process.extractOne(
                query, choices, scorer=fuzz.token_set_ratio, score_cutoff=cfg.improved_cutoff
            )
            if hit is None:
                continue
            station = candidates.iloc[hit[2]]
            rows.append(
                (
                    code,
                    station["project_name"],
                    state,
                    float(hit[1]),
                    station["latitude"],
                    station["longitude"],
                )
            )
    return pd.DataFrame(rows, columns=MATCH_COLUMNS) if rows else _empty_matches()


def haversine_km(
    lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray
) -> np.ndarray:
    """Great-circle distance in kilometres."""
    lat1, lon1, lat2, lon2 = (
        np.radians(np.asarray(a, dtype="float64")) for a in (lat1, lon1, lat2, lon2)
    )
    a = (
        np.sin((lat2 - lat1) / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    )
    return 6371.0088 * 2 * np.arcsin(np.sqrt(a))


def evaluate_matches(
    matches: pd.DataFrame, facilities: pd.DataFrame, correct_within_km: float
) -> dict[str, float | int | None]:
    """Score a match table against OpenElectricity's own facility coordinates.

    A match is *correct* when the matched station lies within ``correct_within_km`` of the
    facility's official location.
    """
    truth = facilities.dropna(subset=["latitude", "longitude"])
    joined = matches.merge(
        truth[["facility_code", "state", "latitude", "longitude"]],
        on="facility_code",
        how="inner",
        suffixes=("_match", "_truth"),
    )
    n_fac = len(facilities)
    n_match = len(matches)
    if joined.empty:
        errors = np.array([])
    else:
        errors = haversine_km(
            joined["latitude_match"], joined["longitude_match"],
            joined["latitude_truth"], joined["longitude_truth"],
        )  # fmt: skip
    correct = int((errors <= correct_within_km).sum())
    cross_state = int((joined["cer_state"] != joined["state"]).sum()) if not joined.empty else 0
    return {
        "facilities": n_fac,
        "facilities_with_truth": len(truth),
        "matched": n_match,
        "match_rate": n_match / n_fac if n_fac else 0.0,
        "evaluated": len(joined),
        "correct": correct,
        "precision": correct / len(joined) if len(joined) else None,
        "recall": correct / len(truth) if len(truth) else None,
        "cross_state_matches": cross_state,
        "median_error_km": float(np.median(errors)) if errors.size else None,
        "p90_error_km": float(np.percentile(errors, 90)) if errors.size else None,
        "correct_within_km": correct_within_km,
    }
