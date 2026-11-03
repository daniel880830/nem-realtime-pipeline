"""Clean Energy Regulator (CER) large-scale renewable station lists: scrape, integrate, clean."""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from nem_pipeline.config import CerConfig
from nem_pipeline.schema import CER_COLUMNS, SchemaError, require_columns

logger = logging.getLogger(__name__)

# The three CSVs use different headers for the same fields.
RENAME_MAP = {
    "Power station name": "project_name",
    "Project Name": "project_name",
    "State": "state",
    "State ": "state",
    "Installed capacity (MW)": "mw_capacity",
    "MW Capacity": "mw_capacity",
    "Fuel Source (s)": "fuel_source",
    "Fuel Source": "fuel_source",
    "Postcode": "postcode",
}
STATUS_PRIORITY = {"accredited": 0, "committed": 1, "probable": 2}
AUSTRALIAN_STATES = frozenset({"NSW", "QLD", "VIC", "SA", "TAS", "WA", "NT", "ACT"})


class CerError(RuntimeError):
    """Raised when the CER page or datasets cannot be retrieved or parsed."""


def fetch_text(url: str, *, timeout_s: float, max_retries: int, session: requests.Session) -> str:
    """GET ``url`` with a timeout and bounded retries (the original retried forever).

    Raises:
        CerError: When every attempt fails.
    """
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout_s)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("GET %s attempt %d/%d failed: %s", url, attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(attempt)
    raise CerError(f"Could not download {url}") from last_error


def find_dataset_urls(html: str, base_url: str, datasets: tuple[str, ...]) -> dict[str, str]:
    """Find the CSV link of each requested dataset on the CER data page.

    Args:
        html: Page HTML.
        base_url: Prefix for relative links.
        datasets: Dataset keywords, e.g. ``("accredited", "committed", "probable")``.

    Returns:
        Mapping of dataset keyword to absolute URL.

    Raises:
        CerError: If any requested dataset has no link.
    """
    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        for name in datasets:
            if name not in found and re.search(rf"power-stations-and-projects-{name}\b", href):
                found[name] = href if href.startswith("http") else base_url.rstrip("/") + href
    missing = [d for d in datasets if d not in found]
    if missing:
        raise CerError(f"CER page has no link for datasets: {missing}")
    return found


def download_datasets(
    cfg: CerConfig, raw_dir: Path, *, refresh: bool = False, session: requests.Session | None = None
) -> dict[str, Path]:
    """Download the CER CSVs into ``raw_dir`` (cached unless ``refresh``)."""
    session = session or requests.Session()
    paths = {name: raw_dir / f"cer_{name}.csv" for name in cfg.datasets}
    if not refresh and all(p.exists() for p in paths.values()):
        return paths
    html = fetch_text(
        cfg.page_url, timeout_s=cfg.request_timeout_s, max_retries=cfg.max_retries, session=session
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    for name, url in find_dataset_urls(html, cfg.base_url, cfg.datasets).items():
        text = fetch_text(
            url, timeout_s=cfg.request_timeout_s, max_retries=cfg.max_retries, session=session
        )
        paths[name].write_text(text, encoding="utf-8")
        logger.info("Saved CER %s dataset to %s", name, paths[name])
    return paths


def read_datasets(paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    """Read the cached CER CSVs.

    Raises:
        CerError: If a file is missing or empty.
    """
    frames = {}
    for name, path in paths.items():
        if not path.exists():
            raise CerError(f"CER dataset not found: {path}")
        try:
            frames[name] = pd.read_csv(path, dtype=str)
        except pd.errors.EmptyDataError as exc:
            raise CerError(f"CER dataset is empty: {path}") from exc
    return frames


def parse_capacity(values: pd.Series) -> pd.Series:
    """Parse capacities such as ``"1,400.000"`` to floats (invalid values become NaN)."""
    cleaned = values.astype("string").str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(cleaned, errors="coerce")


def integrate(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine the CER datasets into one frame with :data:`CER_COLUMNS`.

    Raises:
        SchemaError: If a dataset lacks a required field.
    """
    parts = []
    for status, df in frames.items():
        df = df.rename(columns=RENAME_MAP)
        require_columns(
            df, ["project_name", "state", "mw_capacity", "fuel_source"], f"CER {status}"
        )
        if "postcode" not in df.columns:
            df["postcode"] = pd.NA
        df = df.assign(status=status)
        parts.append(df[CER_COLUMNS])
    if not parts:
        raise SchemaError("No CER datasets to integrate")
    out = pd.concat(parts, ignore_index=True)
    for col in ["project_name", "state", "fuel_source", "postcode"]:
        out[col] = out[col].astype("string").str.strip()
    out["mw_capacity"] = parse_capacity(out["mw_capacity"])
    return out


def clean_legacy(df: pd.DataFrame) -> pd.DataFrame:
    """Reproduce the original cleaning step.

    The original cut every name at the first ``-`` and then averaged rows sharing the
    truncated name and state, which merges distinct stations (e.g. two sites of one owner).
    """
    df = df.copy()
    df["project_name"] = df["project_name"].str.split("-").str[0].str.strip()
    keys = ["project_name", "state"]
    rules = {c: "first" for c in df.columns if c not in keys}
    rules["mw_capacity"] = "mean"
    return df.groupby(keys, as_index=False, sort=True).agg(rules)[CER_COLUMNS]


_DESCRIPTOR = re.compile(
    r"\b(w\s*sgu|sgu|solar|wind|hydro|biomass|landfill gas|battery|"
    r"nsw|qld|vic|sa|tas|wa|nt|act)\b",
    re.IGNORECASE,
)


def base_name(name: str) -> str:
    """Strip descriptor segments such as ``" - Solar w SGU - QLD"`` from an accredited name.

    Segments made only of fuel/state descriptors are dropped; real name parts are kept.
    """
    segments = [s.strip() for s in re.split(r"\s+-\s+|\s*-\s*$", name) if s and s.strip()]
    kept = [s for s in segments if _DESCRIPTOR.sub("", s).strip(" -")]
    return " ".join(kept or segments[:1])


def clean_improved(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the integrated CER frame without merging distinct stations.

    * Whitespace is normalised and descriptor suffixes are removed, but the name is not
      truncated at the first hyphen.
    * Rows with an unknown state or missing name are dropped.
    * Exact duplicates of ``(project_name, state)`` keep the most advanced status
      (accredited > committed > probable).
    """
    df = df.copy()
    df["project_name"] = (
        df["project_name"]
        .fillna("")
        .map(base_name)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    df["state"] = df["state"].str.upper()
    df = df[(df["project_name"] != "") & df["state"].isin(AUSTRALIAN_STATES)]
    df = df.assign(_rank=df["status"].map(STATUS_PRIORITY).fillna(9))
    df = df.sort_values(["project_name", "state", "_rank"], kind="stable")
    df = df.drop_duplicates(["project_name", "state"], keep="first")
    return df.drop(columns="_rank").reset_index(drop=True)[CER_COLUMNS]
