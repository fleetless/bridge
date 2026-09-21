# SPDX-License-Identifier: Apache-2.0
"""Low-bandwidth mode where it touches ROS: the eight parameters on a real
node, the datapoint cap over a real subscription, and the camera lever.

The decision itself lives in `test_link_mode.py` and the session wiring in
`test_client_link_mode.py`. What is left here is what only a real node can
answer — that `ros2 param set` reaches the validator, that a refused value
stays unset, and that a capped sample lands in the backlog rather than
nowhere.
"""
import asyncio

import pytest
import rclpy
from rclpy.parameter import Parameter
from sensor_msgs.msg import BatteryState
from test_ros_runtime import _battery, _camera_cfg, _drain_camera_states, run

from helpers import by_slug

from fleetless_bridge.link_mode import LowBandwidthSettings
from fleetless_bridge.protocol import (
    LOW_BANDWIDTH_DEFAULTS,
    DatapointConfig,
    DatapointNumeric,
    RetentionConfig,
)


def _dp(slug, *, rate_throttle_hz=None, keep=False, retention=None):
    return DatapointConfig(
        slug=slug,
        topic="/battery",
        type="sensor_msgs/msg/BatteryState",
        field="percentage",
        rate_throttle_hz=rate_throttle_hz,
        numeric=DatapointNumeric(),
        retention=retention or RetentionConfig(enabled=False),
        low_bandwidth_keep=keep,
    )


def _settings(**overrides):
    values = dict(LOW_BANDWIDTH_DEFAULTS)
    values.update(overrides)
    return LowBandwidthSettings.resolve(values, {})


def _publish(count):
    """`count` battery messages on `/battery` from a throwaway node, as fast
    as they go out — well inside one second, so a 1 Hz cap admits exactly the
    first of them."""
    node = rclpy.create_node("test_lb_publisher")
    try:
        pub = node.create_publisher(BatteryState, "/battery", 10)
        for i in range(count):
            pub.publish(_battery(0.1 * i))
    finally:
        node.destroy_node()


class _LivePublisher:
    """A stand-in for `live.LivePublisher` with the one method this file is
    about. Local rather than `test_ros_runtime.py`'s own fake: the camera
    lever is the only caller of `set_bitrate_kbps`, and a shared fake would
    put a method on every camera test that has no use for it."""

    instances = []

    def __init__(self, *, holder, width, height, fps, bitrate_kbps, on_lost=None):
        self.bitrate_kbps = bitrate_kbps
        self.on_lost = on_lost
        self.start_calls = []
        self.stop_calls = 0
        self.bitrate_calls = []
        _LivePublisher.instances.append(self)

    async def start(self, url, room, token):
        self.start_calls.append((url, room, token))

    async def stop(self):
        self.stop_calls += 1

    def set_bitrate_kbps(self, kbps):
        self.bitrate_calls.append(kbps)
        self.bitrate_kbps = kbps


def _live_factory():
    def factory(**kwargs):
        return _LivePublisher(**kwargs)

    return factory


async def _drain_samples(rt):
    await asyncio.sleep(0.3)
    out = []
    while True:
        sample = rt.samples.try_get()
        if sample is None:
            return out
        out.append(sample)


@pytest.fixture(autouse=True)
def _reset_publisher_instances():
    _LivePublisher.instances.clear()
    yield
    _LivePublisher.instances.clear()


# --- the parameters ---------------------------------------------------------


def test_parameters_are_declared_with_the_defaults():
    async def body(rt):
        return {
            key: rt._node.get_parameter("low_bandwidth." + key).value
            for key in LOW_BANDWIDTH_DEFAULTS
        }

    values = run(body)
    assert values["mode"] == "auto"
    assert values["enter_lag_ms"] == 2000
    assert values["camera"] == "reduce"
    # Declared as a double, not an integer: the vendored default is `1` and
    # ROS would take that literally, refusing `1.5` for the rest of the run.
    assert isinstance(values["datapoint_max_hz"], float)
    assert values["datapoint_max_hz"] == 1.0


def test_low_bandwidth_params_reads_all_eight_back():
    async def body(rt):
        return rt.low_bandwidth_params()

    values = run(body)
    assert set(values) == set(LOW_BANDWIDTH_DEFAULTS)
    assert LowBandwidthSettings.resolve(values, {}).mode == "auto"


def test_a_parameter_set_reaches_the_callback_with_the_whole_section():
    seen = []

    async def body(rt):
        rt.on_low_bandwidth_params(seen.append)
        result = rt._node.set_parameters(
            [Parameter("low_bandwidth.mode", Parameter.Type.STRING, "on")]
        )
        await asyncio.sleep(0.1)
        return result

    result = run(body)
    assert result[0].successful is True
    # The whole section, not only the key that moved: the client resolves
    # all eight against the YAML on top of it.
    assert len(seen) == 1
    assert seen[0]["mode"] == "on"
    assert seen[0]["enter_lag_ms"] == 2000


def test_a_value_the_settings_refuse_is_refused_with_the_same_sentence():
    seen = []

    async def body(rt):
        rt.on_low_bandwidth_params(seen.append)
        result = rt._node.set_parameters(
            [Parameter("low_bandwidth.enter_lag_ms", Parameter.Type.INTEGER, 50)]
        )
        await asyncio.sleep(0.1)
        return result, rt.low_bandwidth_params()["enter_lag_ms"]

    result, stored = run(body)
    assert result[0].successful is False
    assert result[0].reason == "low_bandwidth.enter_lag_ms must be an integer >= 100"
    assert stored == 2000
    assert seen == []


def test_a_parameter_of_someone_elses_never_reaches_the_callback():
    """`use_sim_time` is declared by rclpy itself and set by ordinary ROS
    tooling. A notification per unrelated parameter would re-resolve and
    re-apply the levers for a change that is none of this mode's business."""
    seen = []

    async def body(rt):
        rt.on_low_bandwidth_params(seen.append)
        rt._node.set_parameters([Parameter("use_sim_time", Parameter.Type.BOOL, False)])
        await asyncio.sleep(0.1)

    run(body)
    assert seen == []


# --- the datapoint cap ------------------------------------------------------


def test_the_cap_applies_unless_the_datapoint_says_keep():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped"), _dp("kept", keep=True)]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        _publish(6)
        return await _drain_samples(rt)

    samples = run(body)
    per_slug = {}
    for sample in samples:
        per_slug.setdefault(sample.slug, []).append(sample)
    # Six publishes inside a second: the 1 Hz cap lets the first through and
    # nothing after it.
    assert len(per_slug.get("capped", [])) == 1
    assert len(per_slug.get("kept", [])) == 6


def test_a_datapoint_already_slower_than_the_cap_is_left_alone():
    """The cap is a ceiling, not a rate. A datapoint configured at 0.5 Hz
    must not be re-policed by a 1 Hz cap — that would reset its own timer on
    every transition and could only ever send it less often, never more."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("slow", rate_throttle_hz=0.5)]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        return rt._subscriptions["slow"].cap

    assert run(body) is None


def test_leaving_the_mode_clears_every_cap():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped")]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        in_mode = rt._subscriptions["capped"].cap
        await rt.set_low_bandwidth(False, _settings(datapoint_max_hz=1))
        return in_mode, rt._subscriptions["capped"].cap

    in_mode, after = run(body)
    assert in_mode is not None
    assert after is None


def test_a_datapoint_configured_while_the_mode_is_on_is_capped_too():
    async def body(rt):
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        await rt.apply_config(by_slug([_dp("late")]))
        await asyncio.sleep(0.3)
        _publish(6)
        return await _drain_samples(rt)

    samples = run(body)
    assert len([s for s in samples if s.slug == "late"]) == 1


def test_capped_samples_go_to_the_backlog_when_retention_is_enabled():
    """The cap spares the link, not the history: a buffered datapoint keeps
    every sample the cap held back, and the cloud fills the gap in once the
    link recovers."""

    async def body(rt):
        await rt.apply_config(by_slug([
            _dp("buffered", retention=RetentionConfig(enabled=True, max_buffer_values=50)),
        ]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        _publish(6)
        live = await _drain_samples(rt)
        held = []
        while True:
            sample = rt.backlog.pop_any()
            if sample is None:
                break
            held.append(sample)
        return live, held

    live, held = run(body)
    assert len(live) == 1
    assert len(held) == 5


def test_an_unbuffered_capped_sample_is_dropped_and_not_queued_anywhere():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("plain")]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        _publish(6)
        live = await _drain_samples(rt)
        return live, rt.backlog.has_pending()

    live, pending = run(body)
    assert len(live) == 1
    assert pending is False


# --- the camera lever -------------------------------------------------------


def test_reduce_lowers_a_running_stream_and_restores_it_on_exit():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.set_low_bandwidth(True, _settings(camera="reduce", camera_bitrate_kbps=300))
        await rt.set_low_bandwidth(False, _settings(camera="reduce", camera_bitrate_kbps=300))
        return _LivePublisher.instances[0].bitrate_calls

    assert run(body, live_publisher_factory=_live_factory()) == [300, 800]


def test_stop_ends_running_streams_and_says_why():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        await rt.set_low_bandwidth(True, _settings(camera="stop"))
        states = await _drain_camera_states(rt)
        return states, _LivePublisher.instances[0].stop_calls

    states, stop_calls = run(body, live_publisher_factory=_live_factory())
    assert stop_calls == 1
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error == (
        "low_bandwidth", "Live video stopped: the bridge entered low-bandwidth mode."
    )
    assert states[0].cause == "live_lost"


def test_start_live_is_refused_while_the_mode_holds():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.set_low_bandwidth(True, _settings(camera="reduce"))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-9")
        return await _drain_camera_states(rt), list(rt._live_publishers)

    states, publishers = run(body, live_publisher_factory=_live_factory())
    assert publishers == []
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].request_id == "req-9"
    assert states[0].cause == "command"
    assert states[0].error == (
        "low_bandwidth",
        "The bridge is in low-bandwidth mode; live video waits until the link recovers.",
    )


def test_a_slug_with_no_camera_is_still_answered_as_unavailable_not_as_the_mode():
    """The mode is not the reason a slug nobody configured cannot go live.
    Answering `low_bandwidth` there would send a developer chasing the link
    instead of the document."""

    async def body(rt):
        await rt.set_low_bandwidth(True, _settings())
        await rt.start_live("ghost", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_live_factory())
    assert states[0].error[0] == "live_unavailable"


def test_leaving_the_mode_lets_a_stream_start_again():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.set_low_bandwidth(True, _settings())
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        await rt.set_low_bandwidth(False, _settings())
        await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "req-2")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_live_factory())
    assert len(states) == 1
    assert states[0].publishing is True
    assert states[0].request_id == "req-2"


def test_the_lever_is_idempotent():
    """client.py pulls it on every settings change as well as on every
    transition, so the second pull with the same numbers has to be free."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        for _ in range(3):
            await rt.set_low_bandwidth(True, _settings(camera="stop"))
        return await _drain_camera_states(rt), _LivePublisher.instances[0].stop_calls

    states, stop_calls = run(body, live_publisher_factory=_live_factory())
    assert stop_calls == 1
    assert len(states) == 1


def test_setting_the_mode_before_start_is_not_an_error():
    """`main.py` constructs the runtime before it starts the node. Nothing
    reads the parameters that early, but a client that applied a forced
    section from a cached config would reach this with no node at all."""
    from fleetless_bridge.ros_runtime import RosRuntime

    rt = RosRuntime(node_name="test_lb_unstarted")
    assert rt.low_bandwidth_params() == {}
    asyncio.run(rt.set_low_bandwidth(True, _settings()))
    rt.stop()  # must not raise
