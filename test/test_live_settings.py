# SPDX-License-Identifier: Apache-2.0
"""`LiveSettings.resolve`: default, then ROS parameter, then `fleetless.yaml`.

No rclpy and no aiortc here: the three layers and the bounds are arithmetic,
and they are tested as arithmetic.
"""
import json

import pytest
from schemas import SCHEMA_DIR

from fleetless_bridge.live_settings import (
    MAX_PACKET_BYTES_MAX,
    MAX_PACKET_BYTES_MIN,
    LiveSettings,
)
from fleetless_bridge.protocol import LIVE_BOUNDS, LIVE_DEFAULTS

_RULE = "live.max_packet_bytes must be an integer in [576, 1350]"


def test_with_neither_layer_the_vendored_default_holds():
    assert LiveSettings.resolve({}, {}).max_packet_bytes == LIVE_DEFAULTS["max_packet_bytes"]
    assert LiveSettings.resolve({}, {}).max_packet_bytes == 1200


def test_a_parameter_overrides_the_default():
    assert LiveSettings.resolve({"max_packet_bytes": 1000}, {}).max_packet_bytes == 1000


def test_a_yaml_key_overrides_the_parameter():
    resolved = LiveSettings.resolve({"max_packet_bytes": 1000}, {"max_packet_bytes": 900})
    assert resolved.max_packet_bytes == 900


def test_an_absent_yaml_key_leaves_the_parameter_alone():
    assert LiveSettings.resolve({"max_packet_bytes": 1000}, {}).max_packet_bytes == 1000


def test_none_in_a_layer_falls_through():
    """An undeclared parameter reads as `None`; it must not erase the layer
    below it."""
    assert LiveSettings.resolve({"max_packet_bytes": None}, {}).max_packet_bytes == 1200
    resolved = LiveSettings.resolve({"max_packet_bytes": 1000}, {"max_packet_bytes": None})
    assert resolved.max_packet_bytes == 1000


def test_unknown_keys_are_ignored_in_both_layers():
    """A console that writes a newer key must not take this bridge off the
    air; the section is read whole and only what is known is used."""
    resolved = LiveSettings.resolve({"encoder": "h264"}, {"encoder": "vp8", "max_packet_bytes": 800})
    assert resolved == LiveSettings(max_packet_bytes=800)


@pytest.mark.parametrize("value", [576, 577, 1200, 1349, 1350])
def test_the_bounds_are_inclusive(value):
    assert LiveSettings.resolve({}, {"max_packet_bytes": value}).max_packet_bytes == value
    assert LiveSettings.resolve({"max_packet_bytes": value}, {}).max_packet_bytes == value


@pytest.mark.parametrize(
    "value", [575, 1351, 1356, 1400, 0, -1200, 1200.5, 1200.0, "1200", True, [1200]]
)
@pytest.mark.parametrize("layer", ["params", "yaml"])
def test_a_bad_value_is_refused_with_the_key_and_the_rule(value, layer):
    """One sentence for the parameter callback and for the config apply.
    `1200.0` is refused too: the parameter is an integer, and a float that
    happens to be whole is still a float the next `ros2 param set` would
    have to match. `True` is an `int` in Python and would otherwise pass as
    1, which no check below 576 would catch only by luck."""
    layers = {"params": {}, "yaml": {}}
    layers[layer] = {"max_packet_bytes": value}
    with pytest.raises(ValueError) as refused:
        LiveSettings.resolve(layers["params"], layers["yaml"])
    assert str(refused.value) == _RULE


def test_a_bad_parameter_under_a_good_yaml_key_is_not_reached():
    """Only the winning layer is in force, so only it is judged. The
    parameter callback judges a parameter on its own, with no YAML layer."""
    resolved = LiveSettings.resolve({"max_packet_bytes": 99999}, {"max_packet_bytes": 1000})
    assert resolved.max_packet_bytes == 1000


def test_the_bounds_are_read_from_the_vendored_constants():
    """Read, not typed: the cloud refuses a published document by these two
    numbers, and a second pair in this package would one day refuse, or
    accept, something else. The literals are here once, so a re-vendor that
    moves a bound is a decision somebody reads rather than a number that
    changed under a green suite."""
    assert LIVE_BOUNDS == {"max_packet_bytes": {"min": 576, "max": 1350}}
    assert (MAX_PACKET_BYTES_MIN, MAX_PACKET_BYTES_MAX) == (576, 1350)
    assert MAX_PACKET_BYTES_MIN <= LIVE_DEFAULTS["max_packet_bytes"] <= MAX_PACKET_BYTES_MAX


def test_the_bounds_are_the_ones_the_config_frame_schema_carries():
    """The same pair a third time, in the schema of the frame the bridge
    receives. All three come out of one contracts release; this is where a
    hand-picked re-vendor of one file without the other shows."""
    schema = json.loads((SCHEMA_DIR / "cloud-config.schema.json").read_text(encoding="utf-8"))
    key = schema["properties"]["doc"]["properties"]["live"]["properties"]["max_packet_bytes"]
    assert (key["minimum"], key["maximum"]) == (MAX_PACKET_BYTES_MIN, MAX_PACKET_BYTES_MAX)


def test_the_settings_are_frozen_and_comparable():
    """`client.py` applies a change only when `!=` says the value moved."""
    assert LiveSettings.resolve({}, {}) == LiveSettings(max_packet_bytes=1200)
    with pytest.raises(Exception):
        LiveSettings.resolve({}, {}).max_packet_bytes = 1  # type: ignore[misc]
