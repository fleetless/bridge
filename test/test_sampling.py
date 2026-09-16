# SPDX-License-Identifier: Apache-2.0
"""Field extraction, message -> JSON rules, scale/offset and rate policies —
real ROS message types throughout, not synthetic stand-ins, so the IDL
quirks (nested messages in short form, `float`/`double` not
`float32`/`float64`, `sequence<uint8>` for byte arrays) are real."""
import base64

import pytest
from geometry_msgs.msg import PoseStamped
from sensor_msgs.msg import BatteryState, CompressedImage, JointState

from fleetless_bridge.sampling import (
    FieldPathError,
    MaxHzPolicy,
    SendEveryPolicy,
    apply_scale_offset,
    extract_value,
    message_to_json,
    rate_policy,
    resolve_field,
    value_to_json,
)


# --- resolve_field ------------------------------------------------------------


def test_a_null_field_resolves_to_the_whole_message_type():
    assert resolve_field(BatteryState, None) == "sensor_msgs/msg/BatteryState"


def test_a_top_level_scalar_field_resolves_to_its_idl_type():
    assert resolve_field(BatteryState, "percentage") == "float"


def test_an_unknown_field_name_is_a_field_path_error():
    with pytest.raises(FieldPathError):
        resolve_field(BatteryState, "not_a_real_field")


def test_a_dotted_path_walks_through_a_nested_message():
    assert resolve_field(PoseStamped, "pose.position.x") == "double"


def test_indexing_into_a_sequence_field_resolves_to_the_item_type():
    assert resolve_field(JointState, "position[0]") == "double"


def test_indexing_into_a_non_array_field_is_a_field_path_error():
    with pytest.raises(FieldPathError):
        resolve_field(BatteryState, "percentage[0]")


def test_nested_arrays_are_rejected_ros_has_none():
    with pytest.raises(FieldPathError):
        resolve_field(JointState, "position[0][0]")


# --- extract_value + value_to_json --------------------------------------------


def test_extracting_a_scalar_field():
    msg = BatteryState()
    msg.percentage = 0.755
    assert extract_value(msg, "percentage") == pytest.approx(0.755)


def test_extracting_through_a_nested_message():
    msg = PoseStamped()
    msg.pose.position.x = 1.5
    assert extract_value(msg, "pose.position.x") == 1.5


def test_extracting_an_indexed_sequence_element():
    msg = JointState()
    msg.position = [1.0, 2.0, 3.0]
    assert extract_value(msg, "position[1]") == 2.0


def test_a_byte_sequence_becomes_base64():
    msg = CompressedImage()
    msg.data = [1, 2, 3, 255]
    value = extract_value(msg, "data")
    encoded = value_to_json(value, resolve_field(CompressedImage, "data"))
    assert encoded == base64.b64encode(bytes([1, 2, 3, 255])).decode("ascii")


def test_a_numeric_sequence_becomes_a_plain_list():
    msg = JointState()
    msg.position = [1.0, 2.0]
    value = extract_value(msg, "position")
    assert value_to_json(value, resolve_field(JointState, "position")) == [1.0, 2.0]


def test_whole_message_conversion_nests_messages_as_objects():
    msg = PoseStamped()
    msg.header.frame_id = "map"
    msg.header.stamp.sec = 7
    msg.header.stamp.nanosec = 500
    msg.pose.position.x = 1.0
    msg.pose.position.y = 2.0
    msg.pose.position.z = 3.0
    msg.pose.orientation.w = 1.0

    result = message_to_json(msg)

    # builtin_interfaces/Time has just sec/nanosec ints, so {sec, nanosec}
    # falls straight out of the generic nested-message rule.
    assert result["header"]["stamp"] == {"sec": 7, "nanosec": 500}
    assert result["header"]["frame_id"] == "map"
    assert result["pose"]["position"] == {"x": 1.0, "y": 2.0, "z": 3.0}
    assert result["pose"]["orientation"] == {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}


def test_a_field_pointing_at_a_nested_message_converts_just_that_subtree():
    msg = PoseStamped()
    msg.pose.position.x = 9.0
    value = extract_value(msg, "pose.position")
    assert value_to_json(value, resolve_field(PoseStamped, "pose.position")) == {
        "x": 9.0,
        "y": 0.0,
        "z": 0.0,
    }


# --- scale/offset ---------------------------------------------------------------


def test_scale_and_offset_apply_to_a_numeric_value():
    assert apply_scale_offset(0.5, scale=100, offset=0) == pytest.approx(50.0)
    assert apply_scale_offset(10, scale=2, offset=3) == 23


def test_scale_and_offset_are_skipped_when_neither_is_set():
    assert apply_scale_offset(0.5, scale=None, offset=None) == 0.5


def test_scale_and_offset_never_touch_a_non_numeric_value():
    assert apply_scale_offset({"x": 1}, scale=100, offset=0) == {"x": 1}
    assert apply_scale_offset([1, 2], scale=100, offset=0) == [1, 2]


def test_scale_and_offset_never_touch_a_bool_even_though_its_an_int_in_python():
    assert apply_scale_offset(True, scale=100, offset=0) is True


# --- rate policies ------------------------------------------------------------


def test_max_hz_sends_the_first_sample_then_drops_until_the_interval_passes():
    policy = MaxHzPolicy(hz=2.0)  # one sample every 0.5s
    assert policy.should_send(1, now=0.0) is True
    assert policy.should_send(2, now=0.2) is False
    assert policy.should_send(3, now=0.6) is True
    assert policy.should_send(4, now=0.65) is False


def test_max_hz_does_not_collapse_toward_half_rate_when_capped_near_the_source_rate():
    """A cap at the source's own rate lost about a third of samples in
    practice (10 Hz source, 10 Hz cap -> 6.58 Hz measured): a strict `>=`
    against the last *accepted* time makes one barely-early sample cost the
    next one too, and the baseline never advances on a rejection — so a
    source only fractionally faster (real jitter, not a faster source) keeps
    missing by a shrinking-then-resetting margin.

    Reproduced deterministically with a synthetic source 0.1% faster than
    the cap — not real jitter, same mechanism. Without tolerance this
    collapses toward half rate; the shape is what matters here, not the
    exact number."""
    policy = MaxHzPolicy(hz=10.0)  # one sample every 0.1s
    ticks = 200
    accepted = 0
    now = 0.0
    for i in range(ticks):
        now = i * 0.0999  # a source only trivially faster than the 10 Hz cap
        if policy.should_send(i, now=now):
            accepted += 1
    effective_hz = accepted / now
    # Only trivially faster than the 10 Hz cap: the rate should stay close
    # to 10 Hz, not collapse toward half — the unfixed policy measures
    # ~5.0 Hz on this exact sequence.
    assert effective_hz > 9.0


def test_no_ceiling_sends_a_repeated_value_rather_than_deduplicating_it():
    # Replaces a deduplicating policy. A battery steady at 87 % is the
    # ordinary case — most datapoints carry no rate_throttle_hz at all — and
    # dropping repeats made one publish once, then go silent for hours: an
    # empty history the customer is billed for, with retention on, and
    # nothing flagging a fault.
    policy = SendEveryPolicy()
    assert policy.should_send(87, now=0.0) is True
    assert policy.should_send(87, now=1.0) is True
    assert policy.should_send(87, now=2.0) is True


def test_no_ceiling_sends_a_repeated_whole_message_too():
    policy = SendEveryPolicy()
    assert policy.should_send({"x": 1}, now=0.0) is True
    assert policy.should_send({"x": 1}, now=1.0) is True


def test_rate_policy_builds_the_right_policy_from_the_wire_config():
    assert isinstance(rate_policy(5), MaxHzPolicy)


def test_no_ceiling_and_a_zero_ceiling_are_the_same_answer():
    """`0` is in range on the wire (`rate_throttle_hz` bounds `0..20`) and
    means the same as an omitted field: no throttling. Both must become "no
    policy" here — `MaxHzPolicy` divides by `hz`."""
    assert isinstance(rate_policy(None), SendEveryPolicy)
    assert isinstance(rate_policy(0), SendEveryPolicy)
    assert isinstance(rate_policy(0.0), SendEveryPolicy)
