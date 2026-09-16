# SPDX-License-Identifier: Apache-2.0
import pytest

from fleetless_bridge.config import BridgeConfig


def test_config_reads_token_and_url_from_env(monkeypatch):
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_abc")
    monkeypatch.setenv("FLEETLESS_CLOUD_URL", "ws://localhost:8080/bridge")
    cfg = BridgeConfig.from_env()
    assert cfg.token == "frt_abc"
    assert cfg.cloud_url == "ws://localhost:8080/bridge"


def test_cloud_url_has_a_production_default(monkeypatch):
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_abc")
    monkeypatch.delenv("FLEETLESS_CLOUD_URL", raising=False)
    cfg = BridgeConfig.from_env()
    assert cfg.cloud_url == "wss://api.fleetless.dev/bridge"


def test_missing_token_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("FLEETLESS_TOKEN", raising=False)
    with pytest.raises(ValueError, match="FLEETLESS_TOKEN"):
        BridgeConfig.from_env()


def test_the_token_never_shows_up_in_a_printed_config():
    config = BridgeConfig(token="frt_supersecret", cloud_url="ws://localhost:8080/bridge")
    for rendered in (repr(config), str(config), "{}".format(config)):
        assert "frt_supersecret" not in rendered
        assert "ws://localhost:8080/bridge" in rendered
