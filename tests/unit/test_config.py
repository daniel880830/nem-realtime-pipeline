from pathlib import Path

import pytest

from nem_pipeline.config import ConfigError, Settings, load_pipeline_config
from tests.conftest import CONFIG


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "p.toml"
    path.write_text(text, encoding="utf-8")
    return path


BASE = CONFIG.read_text(encoding="utf-8")


def test_default_config_is_valid(pipeline_config):
    assert pipeline_config.extract.start < pipeline_config.extract.end
    assert pipeline_config.publish.facility_topic


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_pipeline_config(tmp_path / "nope.toml")


def test_invalid_toml_raises(tmp_path):
    with pytest.raises(ConfigError, match="Invalid config"):
        load_pipeline_config(_write(tmp_path, "[extract\nstart ="))


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ('end = "2025-10-08T00:00:00"', 'end = "2025-09-01T00:00:00"'),  # end before start
        ('start = "2025-10-01T00:00:00"', 'start = "2025-10-01T00:00:00+10:00"'),  # tz-aware
        ("facility_batch_size = 25", "facility_batch_size = 0"),
        ("min_interval_s = 1.1", "min_interval_s = 0.2"),  # violates Nominatim policy
        ("qos = 0", "qos = 3"),
        ('interval = "5m"', 'interval = "7m"'),
    ],
)
def test_invalid_values_raise(tmp_path, old, new):
    assert old in BASE
    with pytest.raises(ConfigError):
        load_pipeline_config(_write(tmp_path, BASE.replace(old, new)))


def test_unknown_key_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_pipeline_config(_write(tmp_path, BASE.replace("[publish]", "[publish]\nbogus = 1")))


def test_missing_required_section_rejected(tmp_path):
    with pytest.raises(ConfigError):
        load_pipeline_config(_write(tmp_path, "[cer]\npage_url='x'\nbase_url='y'\n"))


def test_settings_read_prefixed_env(workspace, monkeypatch):
    monkeypatch.setenv("APP_MQTT_PORT", "1999")
    s = Settings(_env_file=None)
    assert s.mqtt_port == 1999
    assert s.require_openelectricity_key() == "test-key"
    assert s.warehouse_path.name == "nem.duckdb"
    assert s.pipeline.extract.network == "NEM"


def test_settings_reject_invalid_port(workspace, monkeypatch):
    monkeypatch.setenv("APP_MQTT_PORT", "70000")
    with pytest.raises(ValueError):
        Settings(_env_file=None)


def test_missing_api_key_raises(workspace, monkeypatch):
    monkeypatch.delenv("APP_OPENELECTRICITY_API_KEY")
    with pytest.raises(ConfigError, match="APP_OPENELECTRICITY_API_KEY"):
        Settings(_env_file=None).require_openelectricity_key()


def test_api_key_not_leaked_in_repr(workspace):
    assert "test-key" not in repr(Settings(_env_file=None))
