# SPDX-License-Identifier: Apache-2.0
"""Builds a ROS message from a template plus caller values, and covers the
two structural checks config-apply runs.

Before 3.0, `params` was a flat dict keyed by a dotted field path,
unflattened into the message tree. Now a parameter is a free-standing name;
where its value lands is decided by where `${name}` sits in the `message`
template.
"""
import pytest
from geometry_msgs.msg import Twist
from rosidl_runtime_py.utilities import get_message

from fleetless_bridge.params import (
    build_from_template,
    build_message,
    resolve_failsafe_body,
    validate_template,
)
from fleetless_bridge.protocol import ParameterSpec
from fleetless_bridge.templates import TemplateError

BatteryState = get_message("sensor_msgs/msg/BatteryState")

_DRIVE = {"linear": {"x": "${speed}"}, "angular": {"z": "${turn}"}}
_DRIVE_PARAMS = {"speed": ParameterSpec(type="float64"), "turn": ParameterSpec(type="float64")}


# --- build_message ---------------------------------------------------------


def test_build_message_fills_from_an_already_nested_body():
    msg = build_message(Twist, {"linear": {"x": 0.0}, "angular": {"z": 0.0}})
    assert msg.linear.x == 0.0
    assert msg.angular.z == 0.0


# --- build_from_template ---------------------------------------------------


def test_a_callers_values_land_where_the_placeholders_are():
    msg = build_from_template(Twist, _DRIVE, _DRIVE_PARAMS, {"speed": 1.5, "turn": -0.3}, shared={})
    assert msg.linear.x == pytest.approx(1.5)
    assert msg.angular.z == pytest.approx(-0.3)
    # A field the developer did not mark is untouched and unreachable.
    assert msg.linear.y == 0.0


def test_a_literal_in_the_template_is_sent_as_written():
    template = {"linear": {"x": "${speed}"}, "angular": {"z": 0.0}}
    msg = build_from_template(
        Twist, template, {"speed": ParameterSpec(type="float64")}, {"speed": 1.0}, shared={}
    )
    assert msg.angular.z == 0.0


def test_a_shared_message_reference_is_followed():
    shared = {"drive": _DRIVE}
    msg = build_from_template(
        Twist, "${drive}", _DRIVE_PARAMS, {"speed": 0.5, "turn": 0.0}, shared=shared
    )
    assert msg.linear.x == pytest.approx(0.5)


def test_a_value_is_coerced_to_its_declared_type():
    # JSON cannot tell 1 from 1.0; the declared type can.
    msg = build_from_template(
        Twist, {"linear": {"x": "${speed}"}}, {"speed": ParameterSpec(type="float64")},
        {"speed": 1}, shared={},
    )
    assert isinstance(msg.linear.x, float)


def test_a_value_of_the_wrong_type_is_refused():
    with pytest.raises(TemplateError):
        build_from_template(
            Twist, {"linear": {"x": "${speed}"}}, {"speed": ParameterSpec(type="float64")},
            {"speed": "fast"}, shared={},
        )


def test_an_undeclared_name_is_refused_not_ignored():
    """Nothing declared it, so nothing here can type it — and silently
    dropping it would let a stale cloud send a value that never arrives."""
    with pytest.raises(TemplateError, match="warp_factor"):
        build_from_template(
            Twist, _DRIVE, _DRIVE_PARAMS, {"speed": 1.0, "turn": 0.0, "warp_factor": 9}, shared={},
        )


def test_a_missing_required_value_fails_closed_rather_than_sending_a_placeholder():
    with pytest.raises(TemplateError, match="turn"):
        build_from_template(Twist, _DRIVE, _DRIVE_PARAMS, {"speed": 1.0}, shared={})


def test_an_omitted_optional_parameter_falls_back_to_its_declared_default():
    """The cloud validates values and deliberately does not substitute
    defaults — an omitted optional parameter reaches the bridge omitted, so
    this is the only place its declared value can be applied."""
    parameters = {
        "speed": ParameterSpec(type="float64"),
        "turn": ParameterSpec(type="float64", default=0.25),
    }
    msg = build_from_template(Twist, _DRIVE, parameters, {"speed": 1.0}, shared={})
    assert msg.angular.z == pytest.approx(0.25)


def test_an_explicit_null_is_treated_as_omitted_not_as_a_value():
    """A browser client that clears an input sends `null` rather than
    dropping the key — the same reading the cloud's own validator gives it."""
    parameters = {
        "speed": ParameterSpec(type="float64"),
        "turn": ParameterSpec(type="float64", default=0.25),
    }
    msg = build_from_template(Twist, _DRIVE, parameters, {"speed": 1.0, "turn": None}, shared={})
    assert msg.angular.z == pytest.approx(0.25)


def test_an_empty_template_builds_an_empty_message():
    """An absent `message` on an action or a service normalises to `{}`, and
    that is the ordinary case — `std_srvs/srv/Trigger` has no fields at
    all."""
    trigger = get_message("std_msgs/msg/Empty")
    build_from_template(trigger, {}, {}, {}, shared={})


# --- validate_template ------------------------------------------------------


def test_a_template_that_can_build_the_message_type_passes():
    validate_template(Twist, _DRIVE, _DRIVE_PARAMS, shared={})


def test_a_template_naming_a_field_the_message_does_not_have_is_refused():
    with pytest.raises(Exception):
        validate_template(Twist, {"warp_factor": 9}, {}, shared={})


def test_a_placeholder_naming_nothing_declared_is_refused_at_apply_time():
    with pytest.raises(TemplateError, match="turn"):
        validate_template(Twist, _DRIVE, {"speed": ParameterSpec(type="float64")}, shared={})


def test_a_reference_to_a_message_that_does_not_exist_is_refused_at_apply_time():
    with pytest.raises(TemplateError, match="drive"):
        validate_template(Twist, "${drive}", _DRIVE_PARAMS, shared={})


def test_an_undeclarable_parameter_type_is_refused_at_apply_time():
    with pytest.raises(TemplateError, match="not_a_ros_type"):
        validate_template(
            Twist, {"linear": {"x": "${speed}"}},
            {"speed": ParameterSpec(type="not_a_ros_type")}, shared={},
        )


def test_the_probe_value_type_is_what_decides_not_its_value():
    """A `string` parameter cannot fill a `float64` field, and apply time is
    where that is found out — not the first invoke."""
    with pytest.raises(Exception):
        validate_template(
            Twist, {"linear": {"x": "${speed}"}},
            {"speed": ParameterSpec(type="string")}, shared={},
        )


def test_no_parameters_and_an_empty_template_is_trivially_valid():
    validate_template(Twist, {}, {}, shared={})


def test_the_same_checks_work_against_any_message_type():
    validate_template(
        BatteryState, {"percentage": "${level}"},
        {"level": ParameterSpec(type="float32")}, shared={},
    )


# --- resolve_failsafe_body ---------------------------------------------------


def test_a_literal_failsafe_body_resolves_to_itself():
    body = {"linear": {"x": 0.0}, "angular": {"z": 0.0}}
    assert resolve_failsafe_body(Twist, body, shared={}) == body


def test_a_failsafe_may_reference_a_shared_message():
    shared = {"stop": {"linear": {"x": 0.0}, "angular": {"z": 0.0}}}
    assert resolve_failsafe_body(Twist, "${stop}", shared=shared) == shared["stop"]


def test_a_placeholder_in_an_inline_failsafe_is_refused():
    with pytest.raises(TemplateError, match="speed"):
        resolve_failsafe_body(Twist, {"linear": {"x": "${speed}"}}, shared={})


def test_a_placeholder_behind_a_shared_message_reference_is_refused():
    """What no schema catches: contracts validates one publisher at a time
    and never sees `messages:`, so `message: ${stop}` where `stop` holds
    `${speed}` passes every schema there is. Nothing else stops a robot
    arming a failsafe it could never send."""
    shared = {"stop": {"linear": {"x": "${speed}"}, "angular": {"z": 0.0}}}
    with pytest.raises(TemplateError, match="speed"):
        resolve_failsafe_body(Twist, "${stop}", shared=shared)


def test_an_unbuildable_failsafe_body_is_refused():
    with pytest.raises(Exception):
        resolve_failsafe_body(Twist, {"not_a_field": 1}, shared={})
