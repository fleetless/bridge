# SPDX-License-Identifier: Apache-2.0
"""The `live.*` parameters where they touch ROS: on a real node.

The layers are `test_live_settings.py`'s and the session wiring is
`test_client_live_settings.py`'s. What is left here is what only a real node
answers: that each parameter is declared, that `ros2 param set` reaches the
validator, and that a refused value stays unset. `live.max_packet_bytes`
first, then `live.encoder` and what a live start does with it.
"""
import asyncio

import pytest
from rclpy.parameter import Parameter
from test_ros_runtime import run

from fleetless_bridge.live_settings import LiveSettings
from fleetless_bridge.protocol import LIVE_DEFAULTS

_NAME = "live.max_packet_bytes"
_RULE = "live.max_packet_bytes must be an integer in [576, 1350]"


def test_the_parameter_is_declared_with_the_vendored_default():
    async def body(rt):
        return rt._node.get_parameter(_NAME).value, rt._node.describe_parameter(_NAME)

    value, descriptor = run(body)
    assert value == LIVE_DEFAULTS["max_packet_bytes"] == 1200
    assert type(value) is int
    # `ros2 param describe` is where an operator on the robot reads the rule.
    assert "576" in descriptor.description and "1350" in descriptor.description


def test_live_params_reads_the_layer_back():
    async def body(rt):
        return rt.live_params()

    values = run(body)
    assert values == {"max_packet_bytes": 1200, "encoder": "software"}
    assert LiveSettings.resolve(values, {}).max_packet_bytes == 1200


def test_live_params_is_empty_before_the_node_exists():
    """`main.py` builds the client before it starts the runtime; an empty
    layer resolves to the default, which is the right answer for it."""
    from fleetless_bridge.ros_runtime import RosRuntime

    rt = RosRuntime(node_name="test_live_unstarted")
    assert rt.live_params() == {}
    rt.stop()  # must not raise


def test_set_live_settings_caps_the_packetizers_and_needs_no_node():
    """The lever client.py pulls. It is aiortc's module constant, so it works
    before the node exists and holds for every stream."""
    from aiortc.codecs import h264, vpx

    from fleetless_bridge import live
    from fleetless_bridge.ros_runtime import RosRuntime

    rt = RosRuntime(node_name="test_live_lever")
    try:
        assert rt.set_live_settings(LiveSettings(max_packet_bytes=1000)) == 1000
        assert vpx.PACKET_MAX == h264.PACKET_MAX == 1000 - live.RTP_OVERHEAD_BYTES
    finally:
        live.set_max_packet_bytes(LIVE_DEFAULTS["max_packet_bytes"])
        rt.stop()


def test_a_parameter_set_reaches_the_callback_with_the_whole_layer():
    seen = []

    async def body(rt):
        rt.on_live_params(seen.append)
        result = rt._node.set_parameters([Parameter(_NAME, Parameter.Type.INTEGER, 1000)])
        await asyncio.sleep(0.1)
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is True
    assert stored == {"max_packet_bytes": 1000, "encoder": "software"}
    assert seen == [{"max_packet_bytes": 1000, "encoder": "software"}]


@pytest.mark.parametrize("value", [576, 1350])
def test_the_bounds_themselves_are_accepted(value):
    async def body(rt):
        result = rt._node.set_parameters([Parameter(_NAME, Parameter.Type.INTEGER, value)])
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is True
    assert stored == {"max_packet_bytes": value, "encoder": "software"}


@pytest.mark.parametrize(
    "kind, value",
    [
        (Parameter.Type.INTEGER, 575),
        (Parameter.Type.INTEGER, 1351),
        (Parameter.Type.INTEGER, 1400),
        (Parameter.Type.DOUBLE, 1200.5),
        (Parameter.Type.DOUBLE, 1200.0),
        (Parameter.Type.STRING, "1200"),
        (Parameter.Type.BOOL, True),
    ],
)
def test_a_value_outside_the_rule_is_refused_with_the_sentence(kind, value):
    """Out of bounds or not an integer: the same sentence either way, the
    parameter keeps what it had, and nobody is told."""
    seen = []

    async def body(rt):
        rt.on_live_params(seen.append)
        result = rt._node.set_parameters([Parameter(_NAME, kind, value)])
        await asyncio.sleep(0.1)
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is False
    assert result[0].reason == _RULE
    assert stored == {"max_packet_bytes": 1200, "encoder": "software"}
    assert seen == []


def test_unsetting_the_parameter_is_refused_too():
    """A parameter that takes any type so it can refuse a fraction in its own
    words would also take "no value", which in rclpy deletes it. The bridge
    reads it at every greeting, so it stays declared."""
    async def body(rt):
        result = rt._node.set_parameters([Parameter(_NAME, Parameter.Type.NOT_SET, None)])
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is False
    assert result[0].reason == _RULE
    assert stored == {"max_packet_bytes": 1200, "encoder": "software"}


def test_each_group_hears_only_its_own_parameters():
    """A `low_bandwidth.*` set re-resolves the mode and not the packet size,
    and the reverse; `use_sim_time` is neither's business."""
    live, mode = [], []

    async def body(rt):
        rt.on_live_params(live.append)
        rt.on_low_bandwidth_params(mode.append)
        rt._node.set_parameters([Parameter("use_sim_time", Parameter.Type.BOOL, False)])
        await asyncio.sleep(0.1)
        after_foreign = (list(live), list(mode))
        rt._node.set_parameters([Parameter("low_bandwidth.mode", Parameter.Type.STRING, "on")])
        await asyncio.sleep(0.1)
        after_mode = (list(live), len(mode))
        rt._node.set_parameters([Parameter(_NAME, Parameter.Type.INTEGER, 900)])
        await asyncio.sleep(0.1)
        return after_foreign, after_mode

    after_foreign, after_mode = run(body)
    assert after_foreign == ([], [])
    assert after_mode == ([], 1)
    assert live == [{"max_packet_bytes": 900, "encoder": "software"}]
    assert len(mode) == 1


def test_one_atomic_set_across_both_groups_is_refused_whole_if_either_is_bad():
    """`set_parameters_atomically` stores all or nothing, so a good half must
    not be announced when the other half is refused."""
    live, mode = [], []

    async def body(rt):
        rt.on_live_params(live.append)
        rt.on_low_bandwidth_params(mode.append)
        bad_live = rt._node.set_parameters_atomically([
            Parameter("low_bandwidth.mode", Parameter.Type.STRING, "on"),
            Parameter(_NAME, Parameter.Type.INTEGER, 1401),
        ])
        bad_mode = rt._node.set_parameters_atomically([
            Parameter("low_bandwidth.enter_lag_ms", Parameter.Type.INTEGER, 50),
            Parameter(_NAME, Parameter.Type.INTEGER, 900),
        ])
        await asyncio.sleep(0.1)
        refused = (list(live), list(mode), rt.live_params(), rt.low_bandwidth_params()["mode"])
        good = rt._node.set_parameters_atomically([
            Parameter("low_bandwidth.mode", Parameter.Type.STRING, "on"),
            Parameter(_NAME, Parameter.Type.INTEGER, 900),
        ])
        await asyncio.sleep(0.1)
        return bad_live, bad_mode, refused, good

    bad_live, bad_mode, refused, good = run(body)
    assert bad_live.successful is False
    assert bad_live.reason == _RULE
    assert bad_mode.successful is False
    assert bad_mode.reason == "low_bandwidth.enter_lag_ms must be an integer >= 100"
    assert refused == ([], [], {"max_packet_bytes": 1200, "encoder": "software"}, "auto")
    assert good.successful is True
    assert live == [{"max_packet_bytes": 900, "encoder": "software"}]
    assert len(mode) == 1 and mode[0]["mode"] == "on"


# --- live.encoder ----------------------------------------------------------------

_ENCODER = "live.encoder"
_ENCODER_RULE = "live.encoder must be one of software, auto, nvenc, vaapi, v4l2"


def test_the_encoder_parameter_is_declared_as_software():
    async def body(rt):
        return rt._node.get_parameter(_ENCODER).value, rt._node.describe_parameter(_ENCODER)

    value, descriptor = run(body)
    assert value == LIVE_DEFAULTS["encoder"] == "software"
    for name in ("software", "auto", "nvenc", "vaapi", "v4l2"):
        assert name in descriptor.description


@pytest.mark.parametrize("value", ["software", "auto", "nvenc", "vaapi", "v4l2"])
def test_each_encoder_name_is_accepted_and_reaches_the_callback(value):
    seen = []

    async def body(rt):
        rt.on_live_params(seen.append)
        result = rt._node.set_parameters([Parameter(_ENCODER, Parameter.Type.STRING, value)])
        await asyncio.sleep(0.1)
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is True
    assert stored == {"max_packet_bytes": 1200, "encoder": value}
    assert seen == [{"max_packet_bytes": 1200, "encoder": value}]


@pytest.mark.parametrize(
    "kind, value",
    [
        (Parameter.Type.STRING, "hardware"),
        (Parameter.Type.STRING, "NVENC"),
        (Parameter.Type.STRING, "nvenc "),
        (Parameter.Type.STRING, ""),
        (Parameter.Type.STRING, "x264"),
        (Parameter.Type.INTEGER, 1),
        (Parameter.Type.BOOL, True),
        (Parameter.Type.NOT_SET, None),
    ],
)
def test_a_sixth_name_is_refused_with_the_list_and_nobody_is_told(kind, value):
    seen = []

    async def body(rt):
        rt.on_live_params(seen.append)
        result = rt._node.set_parameters([Parameter(_ENCODER, kind, value)])
        await asyncio.sleep(0.1)
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is False
    assert result[0].reason == _ENCODER_RULE
    assert stored == {"max_packet_bytes": 1200, "encoder": "software"}
    assert seen == []


def test_both_live_parameters_in_one_set_are_judged_each_by_its_own_rule():
    seen = []

    async def body(rt):
        rt.on_live_params(seen.append)
        bad = rt._node.set_parameters_atomically([
            Parameter(_NAME, Parameter.Type.INTEGER, 900),
            Parameter(_ENCODER, Parameter.Type.STRING, "h264"),
        ])
        good = rt._node.set_parameters_atomically([
            Parameter(_NAME, Parameter.Type.INTEGER, 900),
            Parameter(_ENCODER, Parameter.Type.STRING, "auto"),
        ])
        await asyncio.sleep(0.1)
        return bad, good, rt.live_params()

    bad, good, stored = run(body)
    assert bad.successful is False and bad.reason == _ENCODER_RULE
    assert good.successful is True
    assert stored == {"max_packet_bytes": 900, "encoder": "auto"}
    assert seen == [{"max_packet_bytes": 900, "encoder": "auto"}]


# --- what a live start does with it ------------------------------------------------


class _Publisher:
    """What `start_live` builds, recording what it was handed."""

    built = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.stopped = 0
        _Publisher.built.append(self)

    async def start(self, url, room, token):
        pass

    async def stop(self):
        self.stopped += 1

    def set_bitrate_kbps(self, kbps):
        pass


@pytest.fixture
def publishers():
    from fleetless_bridge import live_encoder

    _Publisher.built = []
    live_encoder.forget_chains()
    yield _Publisher.built
    for publisher in _Publisher.built:
        encoder = publisher.kwargs.get("encoder")
        if encoder is not None:
            encoder.close()
    live_encoder.forget_chains()


def _camera(fps=7):
    from helpers import by_slug
    from test_ros_runtime import _camera_cfg

    return by_slug([_camera_cfg("front", fps=fps), _camera_cfg("rear", topic="/rear", fps=fps)])


def test_with_software_the_publisher_is_handed_no_encoder(publishers):
    """`software` builds no pipeline and sets no codec preference: the
    publisher gets `None`, and `None` is the path that touches nothing."""

    async def body(rt):
        await rt.apply_cameras(_camera())
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")

    run(body, live_publisher_factory=_Publisher)
    assert len(publishers) == 1
    assert "encoder" in publishers[0].kwargs and publishers[0].kwargs["encoder"] is None


@pytest.mark.parametrize("setting", ["auto", "nvenc", "vaapi", "v4l2"])
def test_with_a_hardware_setting_each_live_start_gets_an_encoder_of_its_own(publishers, setting):
    from fleetless_bridge.live_encoder import HardwareH264Encoder, chains_for

    async def body(rt):
        await rt.apply_cameras(_camera(fps=7))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder=setting))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        await rt.start_live("rear", "ws://livekit.invalid", "room", "token", "req-2")

    run(body, live_publisher_factory=_Publisher)
    first, second = (publisher.kwargs["encoder"] for publisher in publishers)
    assert type(first) is HardwareH264Encoder and type(second) is HardwareH264Encoder
    assert first is not second  # one per session, closed with it
    assert first._chains == chains_for(setting) and first._backend == setting
    assert first._fps == 7  # the camera's own rate, for the rate control
    # Nothing is built or probed until a frame is encoded.
    assert first.chain is None and not first.software


def test_a_changed_setting_applies_at_the_next_start_and_a_running_stream_keeps_its_encoder(
    publishers,
):
    from fleetless_bridge.live_encoder import HardwareH264Encoder

    async def body(rt):
        await rt.apply_cameras(_camera())
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        still_running = ("front" in rt._live_publishers, publishers[0].stopped)
        await rt.start_live("rear", "ws://livekit.invalid", "room", "token", "req-2")
        return still_running

    still_running = run(body, live_publisher_factory=_Publisher)
    assert still_running == (True, 0)
    assert publishers[0].kwargs["encoder"] is None
    assert type(publishers[1].kwargs["encoder"]) is HardwareH264Encoder


def test_a_change_of_the_encoder_forgets_what_was_learned_about_the_chains(publishers):
    from fleetless_bridge import live_encoder
    from fleetless_bridge.live_encoder import CHAINS
    from fleetless_bridge.ros_runtime import RosRuntime

    jetson = CHAINS["nvenc"][0]
    rt = RosRuntime(node_name="test_live_encoder_memory")
    try:
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        live_encoder.MEMORY.cannot_be_built(jetson, "GStreamer has no element 'nvvidconv'")
        live_encoder.MEMORY.did_not_encode(CHAINS["nvenc"][1], (1024, 768), "no access unit")
        # The same setting again, and a change of the other key: nothing is forgotten.
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1000, encoder="nvenc"))
        kept = live_encoder.MEMORY.candidates(CHAINS["nvenc"], (1024, 768))[1]
        # Another encoder: the next start probes again.
        rt.set_live_settings(LiveSettings(max_packet_bytes=1000, encoder="auto"))
        cleared = live_encoder.MEMORY.candidates(CHAINS["nvenc"], (1024, 768))[1]
    finally:
        from fleetless_bridge import live

        live.set_max_packet_bytes(LIVE_DEFAULTS["max_packet_bytes"])
        rt.stop()
    assert kept == {
        "nvenc/jetson": "nvenc/jetson cannot be built: GStreamer has no element 'nvvidconv'",
        "nvenc/desktop": "nvenc/desktop did not encode a 1024x768 picture: no access unit",
    }
    # Both kinds of finding go: what could not be built and what was refused.
    assert cleared == {}
