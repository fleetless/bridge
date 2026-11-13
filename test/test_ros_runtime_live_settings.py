# SPDX-License-Identifier: Apache-2.0
"""`live.max_packet_bytes` where it touches ROS: the parameter on a real node.

The layers are `test_live_settings.py`'s and the session wiring is
`test_client_live_settings.py`'s. What is left here is what only a real node
answers: that the parameter is declared, that `ros2 param set` reaches the
validator, and that a refused value stays unset.
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
    assert values == {"max_packet_bytes": 1200}
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
    assert stored == {"max_packet_bytes": 1000}
    assert seen == [{"max_packet_bytes": 1000}]


@pytest.mark.parametrize("value", [576, 1350])
def test_the_bounds_themselves_are_accepted(value):
    async def body(rt):
        result = rt._node.set_parameters([Parameter(_NAME, Parameter.Type.INTEGER, value)])
        return result, rt.live_params()

    result, stored = run(body)
    assert result[0].successful is True
    assert stored == {"max_packet_bytes": value}


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
    assert stored == {"max_packet_bytes": 1200}
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
    assert stored == {"max_packet_bytes": 1200}


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
    assert live == [{"max_packet_bytes": 900}]
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
    assert refused == ([], [], {"max_packet_bytes": 1200}, "auto")
    assert good.successful is True
    assert live == [{"max_packet_bytes": 900}]
    assert len(mode) == 1 and mode[0]["mode"] == "on"
