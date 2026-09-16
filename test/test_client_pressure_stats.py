# SPDX-License-Identifier: Apache-2.0
"""`BridgeClient.pressure_stats()` — the pressure counters, collected now.

One dict from the session's `PrioritizedWriter.counters()`,
`RosRuntime.video_stats()` and `pressure.UplinkBudget.snapshot()`. No
console consumer should ever need a null check per field, so it must be
well-formed and zeroed with nothing behind it: before the first
connection, and for a ROS-less client. Borrows `test_client_writer_
wiring.py`'s pattern — `_build_writer` directly, `TimedWs` for a link
that is actually slow.
"""
import asyncio
import time

from helpers import TimedWs, by_slug

import fleetless_bridge.client as client_module
from fleetless_bridge.camera import SNAPSHOT_MAX_BYTES
from fleetless_bridge.client import BridgeClient
from fleetless_bridge.config import BridgeConfig
from fleetless_bridge.pressure import UplinkBudget
from test_ros_runtime import (
    _camera_cfg,
    _drain_camera_states,
    _fake_live_factory,
    run,
)

_EXPECTED_TIERS = (0, 1, 2, 3, 4, 5)
_ZERO_TIER = {"sent": 0, "bytes": 0, "drops": 0, "high_water": 0}


def _offline_client(ros=None) -> BridgeClient:
    """A client that never connects — same helper `test_client_writer_
    wiring.py` uses to drive a session's writer directly, not a whole
    session."""
    return BridgeClient(
        BridgeConfig(token="frt_test_token", cloud_url="ws://example.invalid/bridge"),
        ros=ros,
    )


async def _run_briefly(writer, seconds: float) -> None:
    task = asyncio.ensure_future(writer.run())
    await asyncio.sleep(seconds)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_pressure_stats_is_well_formed_and_zeroed_for_a_ros_less_client():
    """No connection attempted (`self._last_writer is None`) and
    `ros=None` — the two conditions requiring a well-formed dict, not a
    null. Every key present, every tier present, nothing `None` where a
    number belongs."""
    client = _offline_client(ros=None)
    before_ms = int(time.time() * 1000)
    stats = client.pressure_stats()
    after_ms = int(time.time() * 1000)

    assert set(stats.keys()) == {
        "timestamp_ms", "tiers", "rate_bps", "snapshot_max_bytes", "video",
    }
    assert before_ms <= stats["timestamp_ms"] <= after_ms

    assert set(stats["tiers"].keys()) == set(_EXPECTED_TIERS)
    for tier in _EXPECTED_TIERS:
        assert stats["tiers"][tier] == _ZERO_TIER, tier

    assert stats["rate_bps"] is None
    assert stats["snapshot_max_bytes"] == SNAPSHOT_MAX_BYTES

    assert stats["video"] == {
        "active_streams": 0,
        "bitrate_sum_kbps": 0,
        "uplink_kbps": None,
        "override_kbps": None,
        "video_budget_kbps": None,
        "reserve_kbps": 0,
    }


def test_pressure_stats_reflects_tier_traffic_and_the_measured_rate():
    """`TimedWs`: every send takes a known time, so the rate estimate is a
    real, moving number, not whatever a loopback socket did. Pushed onto
    tier 2 via `enqueue`, same as `test_client_writer_wiring.py` drives a
    pong onto tier 0 with no real source behind it. `tiers[2]["sent"]`
    and `rate_bps` must both have moved."""
    client = _offline_client(ros=None)
    before_ms = int(time.time() * 1000)

    async def scenario():
        ws = TimedWs(0.02)
        writer = client._build_writer(ws)
        for i in range(5):
            # Over `RATE_SAMPLE_MIN_BYTES`: only a send that outlives the
            # write buffer measures the link, so only this size moves
            # `rate_bps` off `None` at all.
            writer.enqueue(client_module._TIER_TELEMETRY, b"x" * 100_000)
        await _run_briefly(writer, 0.3)

    asyncio.run(scenario())
    after_ms = int(time.time() * 1000)
    stats = client.pressure_stats()

    assert stats["tiers"][client_module._TIER_TELEMETRY]["sent"] >= 1
    assert stats["tiers"][client_module._TIER_TELEMETRY]["bytes"] >= 100_000
    assert stats["rate_bps"] is not None
    assert stats["rate_bps"] > 0
    # Every other tier is still well-formed, untouched.
    assert stats["tiers"][0] == _ZERO_TIER
    assert before_ms <= stats["timestamp_ms"] <= after_ms


def test_pressure_stats_reports_a_real_runtimes_video_and_budget():
    """The production branch: `self._ros` set, so `video_stats()` and
    `uplink_budget.snapshot()` are the runtime's own numbers, not the
    zeroed fallbacks above. Both other tests pass `ros=None`; a
    `pressure_stats` that just returned the zero shape unconditionally
    would have been green here from day one — nothing else covers this
    branch.

    A real, spinning `RosRuntime` (test_ros_runtime.py's `run` harness, a
    fake LiveKit publisher standing in for `live.LivePublisher` — same
    pattern as test_ros_runtime_uplink.py) with one 500 kbps camera live
    against a 2000 kbps budget. Reserve = max(20% of 2000, 128) = 400, so
    video may use 1600; the stream is committed, so `bitrate_sum_kbps` is
    500.

    `==` on the whole `video` dict, not key-by-key: `pressure_stats`
    merges `video_stats()` then `uplink_budget.snapshot()` over it, so a
    key lost or shadowed by the wrong side is exactly what a per-key
    assertion would miss."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=500)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-1")
        await _drain_camera_states(rt)
        assert "front" in rt._live_publishers, "the fake publisher never committed"
        # Built here, not around `run`: `video_stats()` reads loop-side
        # state and must run on the loop thread, where `body` runs.
        return _offline_client(ros=rt).pressure_stats()

    stats = run(
        body,
        uplink_budget=UplinkBudget(2000),
        live_publisher_factory=_fake_live_factory(),
    )
    assert stats["video"] == {
        "active_streams": 1,
        "bitrate_sum_kbps": 500,
        "uplink_kbps": 2000,
        "override_kbps": None,
        "video_budget_kbps": 1600,
        "reserve_kbps": 400,
    }
