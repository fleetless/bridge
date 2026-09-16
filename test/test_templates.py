# SPDX-License-Identifier: Apache-2.0
import pytest

from fleetless_bridge.templates import (
    TemplateError,
    coerce,
    placeholder_names,
    resolve_template,
)


def test_placeholder_names_finds_every_depth():
    template = {"linear": {"x": "${speed}"}, "angular": {"z": "${turn}"}, "frame": "map"}
    assert placeholder_names(template) == {"speed", "turn"}


def test_placeholder_names_survives_a_cycle():
    # A YAML anchor can make a template point to itself; an explicit stack
    # with a seen-set returns, recursion would not.
    node = {"a": 1}
    node["self"] = node
    assert placeholder_names(node) == set()


def test_a_literal_that_merely_looks_like_a_name_is_a_literal():
    assert placeholder_names({"mode": "speed"}) == set()
    assert placeholder_names({"mode": "$speed"}) == set()
    assert placeholder_names({"mode": "${speed} and more"}) == set()


def test_resolve_substitutes_at_every_depth():
    template = {"linear": {"x": "${speed}"}, "angular": {"z": 0.0}}
    assert resolve_template(template, {"speed": 0.5}, shared={}) == {
        "linear": {"x": 0.5},
        "angular": {"z": 0.0},
    }


def test_resolve_follows_a_shared_message_reference():
    shared = {"stop_twist": {"linear": {"x": 0.0}, "angular": {"z": 0.0}}}
    assert resolve_template("${stop_twist}", {}, shared=shared) == {
        "linear": {"x": 0.0},
        "angular": {"z": 0.0},
    }


def test_a_shared_message_may_carry_placeholders_the_caller_fills():
    shared = {"drive": {"linear": {"x": "${speed}"}}}
    assert resolve_template("${drive}", {"speed": 1.5}, shared=shared) == {"linear": {"x": 1.5}}


def test_a_reference_to_an_unknown_message_fails_closed():
    with pytest.raises(TemplateError, match="stop_twist"):
        resolve_template("${stop_twist}", {}, shared={})


def test_a_shared_message_may_not_reference_another():
    shared = {"a": "${b}", "b": {"x": 0.0}}
    with pytest.raises(TemplateError, match="nested"):
        resolve_template("${a}", {}, shared=shared)


def test_a_missing_value_fails_closed_rather_than_sending_a_placeholder():
    with pytest.raises(TemplateError, match="speed"):
        resolve_template({"linear": {"x": "${speed}"}}, {}, shared={})


def test_a_placeholder_inside_a_list_is_substituted():
    assert resolve_template({"ranges": ["${near}", 2.0]}, {"near": 1.0}, shared={}) == {
        "ranges": [1.0, 2.0]
    }


@pytest.mark.parametrize(
    "value,ros_type,expected",
    [
        (1, "int32", 1),
        (1.0, "int32", 1),
        (1, "float64", 1.0),
        (0.5, "float64", 0.5),
        (True, "bool", True),
        ("go", "string", "go"),
    ],
)
def test_coerce_accepts_what_the_type_admits(value, ros_type, expected):
    result = coerce(value, ros_type)
    assert result == expected
    assert type(result) is type(expected)


@pytest.mark.parametrize(
    "value,ros_type",
    [
        (1.5, "int32"),
        ("1", "int32"),
        (1, "bool"),
        (True, "int32"),
        (1, "string"),
    ],
)
def test_coerce_fails_closed_on_a_type_mismatch(value, ros_type):
    with pytest.raises(TemplateError):
        coerce(value, ros_type)
