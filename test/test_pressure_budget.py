# SPDX-License-Identifier: Apache-2.0
import pytest

from fleetless_bridge.config import BridgeConfig
from fleetless_bridge.pressure import UplinkBudget


def test_unbudgeted_admits_everything():
    b = UplinkBudget(None)
    assert b.video_budget_kbps() is None
    assert b.admits(50_000, 50_000)


def test_reserve_is_percentage_with_an_absolute_floor():
    # 2000 kbps: pct reserve 400 > floor 128 -> video 1600
    assert UplinkBudget(2000).video_budget_kbps() == 1600
    # 500 kbps: pct reserve 100 < floor 128 -> video 372
    assert UplinkBudget(500).video_budget_kbps() == 372


def test_admission_is_against_the_video_budget():
    b = UplinkBudget(2000)
    assert b.admits(1600, 0)
    assert not b.admits(1601, 0)
    assert not b.admits(800, 900)


def test_live_override_wins_and_zero_means_no_video():
    b = UplinkBudget(2000)
    b.set_live_override(1000)   # reserve max(200,128)=200 -> video 800
    assert b.video_budget_kbps() == 800
    b.set_live_override(0)
    assert b.video_budget_kbps() == 0
    assert not b.admits(1, 0)


def test_override_applies_even_without_an_env_value():
    b = UplinkBudget(None)
    b.set_live_override(1000)
    assert b.video_budget_kbps() == 800


def test_snapshot_returns_dict_with_budget_info():
    b = UplinkBudget(2000)
    b.set_live_override(1000)
    snapshot = b.snapshot()
    assert snapshot == {
        "uplink_kbps": 2000,
        "override_kbps": 1000,
        "video_budget_kbps": 800,
        "reserve_kbps": 200,
    }


def test_config_reads_uplink_fields_from_env(monkeypatch):
    monkeypatch.setenv("FLEETLESS_UPLINK_KBPS", "2000")
    monkeypatch.setenv("FLEETLESS_UPLINK_RESERVE_PCT", "25")
    monkeypatch.setenv("FLEETLESS_UPLINK_RESERVE_MIN_KBPS", "256")
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_test")
    cfg = BridgeConfig.from_env()
    assert cfg.uplink_kbps == 2000
    assert cfg.uplink_reserve_pct == 25
    assert cfg.uplink_reserve_min_kbps == 256


def test_uplink_fields_have_defaults_when_absent(monkeypatch):
    monkeypatch.delenv("FLEETLESS_UPLINK_KBPS", raising=False)
    monkeypatch.delenv("FLEETLESS_UPLINK_RESERVE_PCT", raising=False)
    monkeypatch.delenv("FLEETLESS_UPLINK_RESERVE_MIN_KBPS", raising=False)
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_test")
    cfg = BridgeConfig.from_env()
    assert cfg.uplink_kbps is None
    assert cfg.uplink_reserve_pct == 20
    assert cfg.uplink_reserve_min_kbps == 128


def test_uplink_kbps_non_integer_raises_error(monkeypatch):
    monkeypatch.setenv("FLEETLESS_UPLINK_KBPS", "not_a_number")
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_test")
    with pytest.raises(ValueError, match="FLEETLESS_UPLINK_KBPS"):
        BridgeConfig.from_env()


def test_uplink_reserve_pct_non_integer_raises_error(monkeypatch):
    monkeypatch.setenv("FLEETLESS_UPLINK_RESERVE_PCT", "not_a_number")
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_test")
    with pytest.raises(ValueError, match="FLEETLESS_UPLINK_RESERVE_PCT"):
        BridgeConfig.from_env()


def test_uplink_reserve_min_kbps_non_integer_raises_error(monkeypatch):
    monkeypatch.setenv("FLEETLESS_UPLINK_RESERVE_MIN_KBPS", "not_a_number")
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_test")
    with pytest.raises(ValueError, match="FLEETLESS_UPLINK_RESERVE_MIN_KBPS"):
        BridgeConfig.from_env()


def test_floating_point_precision_in_reserve_calculation():
    # Regression: pct=7 -> 7.0/100*100 floats to 7.000000000001; ceil gives 8,
    # int math gives correct 7. floor=1 avoids masking it.
    b = UplinkBudget(100, reserve_pct=7, reserve_min_kbps=1)
    # Reserve = max(ceil(7*100/100), 1) = 7 -> video 93
    assert b.video_budget_kbps() == 93


def test_clearing_override_reverts_to_env_value():
    b = UplinkBudget(2000)
    assert b.video_budget_kbps() == 1600
    b.set_live_override(1000)
    assert b.video_budget_kbps() == 800
    b.set_live_override(None)
    assert b.video_budget_kbps() == 1600
