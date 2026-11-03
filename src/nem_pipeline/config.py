"""Runtime settings (.env / environment) and pipeline parameters (TOML)."""

from __future__ import annotations

import tomllib
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigError(ValueError):
    """Raised when the pipeline configuration is missing or invalid."""


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExtractConfig(_Section):
    """OpenElectricity extraction window and batching."""

    network: Literal["NEM"] = "NEM"
    interval: Literal["5m", "1h", "1d"] = "5m"
    start: datetime
    end: datetime
    facility_batch_size: int = Field(25, ge=1, le=500)
    max_days_per_request: int = Field(7, ge=1, le=31)

    @model_validator(mode="after")
    def _check_window(self) -> ExtractConfig:
        if self.start.tzinfo is not None or self.end.tzinfo is not None:
            raise ValueError("extract.start/end must be naive NEM market time (AEST)")
        if self.start >= self.end:
            raise ValueError("extract.start must be earlier than extract.end")
        return self


class CerConfig(_Section):
    """Clean Energy Regulator (CER) scraping parameters."""

    page_url: str
    base_url: str
    datasets: tuple[str, ...] = ("accredited", "committed", "probable")
    request_timeout_s: float = Field(30, gt=0)
    max_retries: int = Field(3, ge=1, le=10)


class GeocodeConfig(_Section):
    """Nominatim (OpenStreetMap) geocoder parameters."""

    endpoint: str = "https://nominatim.openstreetmap.org/search"
    min_interval_s: float = Field(1.1, ge=1.0)
    timeout_s: float = Field(20, gt=0)


class MatchingConfig(_Section):
    """Fuzzy-matching thresholds."""

    legacy_partial_cutoff: int = Field(70, ge=0, le=100)
    legacy_token_sort_cutoff: int = Field(65, ge=0, le=100)
    improved_cutoff: int = Field(85, ge=0, le=100)
    correct_within_km: float = Field(25.0, gt=0)


class PublishConfig(_Section):
    """MQTT replay parameters."""

    facility_topic: str = Field("nem/facilities/metrics", min_length=1)
    market_topic: str = Field("nem/market/metrics", min_length=1)
    interval_delay_s: float = Field(0.1, ge=0)
    qos: Literal[0, 1, 2] = 0


class PipelineConfig(_Section):
    """All pipeline parameters read from the TOML file."""

    extract: ExtractConfig
    cer: CerConfig
    geocode: GeocodeConfig = GeocodeConfig()
    matching: MatchingConfig = MatchingConfig()
    publish: PublishConfig = PublishConfig()


def load_pipeline_config(path: Path) -> PipelineConfig:
    """Load and validate the TOML pipeline configuration.

    Args:
        path: Path to the TOML file.

    Returns:
        The validated configuration.

    Raises:
        ConfigError: If the file is missing, is not valid TOML or fails validation.
    """
    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        return PipelineConfig.model_validate(raw)
    except (tomllib.TOMLDecodeError, ValueError) as exc:
        raise ConfigError(f"Invalid config file {path}: {exc}") from exc


class Settings(BaseSettings):
    """Deployment settings read from environment variables or ``.env`` (prefix ``APP_``)."""

    model_config = SettingsConfigDict(
        env_prefix="APP_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    openelectricity_api_key: SecretStr | None = None
    nominatim_contact: str = "unknown"
    mqtt_host: str = "localhost"
    mqtt_port: int = Field(1883, ge=1, le=65535)
    data_dir: Path = Path("data")
    reports_dir: Path = Path("reports")
    config_file: Path = Path("config/pipeline.toml")

    @cached_property
    def pipeline(self) -> PipelineConfig:
        """Pipeline parameters loaded from ``config_file``."""
        return load_pipeline_config(self.config_file)

    @property
    def warehouse_path(self) -> Path:
        """DuckDB warehouse file."""
        return self.data_dir / "warehouse" / "nem.duckdb"

    @property
    def raw_dir(self) -> Path:
        """Directory for raw snapshots of downloaded data."""
        return self.data_dir / "raw"

    @property
    def geocode_cache_path(self) -> Path:
        """JSON cache of geocoding results."""
        return self.data_dir / "cache" / "geocode.json"

    def require_openelectricity_key(self) -> str:
        """Return the OpenElectricity API key.

        Raises:
            ConfigError: If the key is not configured.
        """
        if (
            self.openelectricity_api_key is None
            or not self.openelectricity_api_key.get_secret_value()
        ):
            raise ConfigError(
                "APP_OPENELECTRICITY_API_KEY is not set. Add it to .env (see .env.example)."
            )
        return self.openelectricity_api_key.get_secret_value()
