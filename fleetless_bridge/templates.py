# SPDX-License-Identifier: Apache-2.0
"""Resolve a message template against caller-supplied parameter values.

A template is the literal Goal, Request or published message as the developer
wrote it, with ``${name}`` at the positions a caller may fill. Everything else
is fixed -- a field written ``0.0`` in the configuration stays ``0.0`` on the
wire -- and that is the whole safety story: a client may set only what the
developer marked.

Two things this module deliberately does NOT do:

* **It does not check bounds.** ``min_value``, ``max_value``, ``enum`` and
  ``regex`` are enforced in the cloud before anything reaches the robot. A
  second check here would be a second policy for one decision, and the one
  that arrives later is always the weaker -- it sees less. Type agreement is
  different: it is a structural precondition for building the ROS message at
  all, so it is checked here and fails closed.
* **It does not resolve field paths.** A parameter's name is a free-standing
  identifier, not a path into the message. Where its value lands is decided by
  where the developer wrote the placeholder.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Set

PLACEHOLDER_RE = re.compile(r"^\$\{([a-z][a-z0-9]*(?:_[a-z0-9]+)*)\}$")

INTEGER_TYPES = frozenset(
    {"byte", "char", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"}
)
FLOAT_TYPES = frozenset({"float32", "float64"})
STRING_TYPES = frozenset({"string", "wstring"})


class TemplateError(Exception):
    """A template could not be resolved. Always fails closed, never partially."""


def _placeholder(node: Any) -> str | None:
    if not isinstance(node, str):
        return None
    match = PLACEHOLDER_RE.match(node)
    return match.group(1) if match else None


def placeholder_names(node: Any) -> Set[str]:
    """Every placeholder name in a template.

    Walked with an explicit stack and a seen-set, not recursion. A template
    reaches the bridge as parsed YAML, and YAML anchors can produce both very
    deep and genuinely cyclic structures. Recursion would raise
    ``RecursionError`` on the first and never return on the second; both would
    surface as the bridge dying rather than as a rejected configuration.
    """
    found: Set[str] = set()
    seen: Set[int] = set()
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, (dict, list)):
            if id(current) in seen:
                continue
            seen.add(id(current))
            stack.extend(current.values() if isinstance(current, dict) else current)
            continue
        name = _placeholder(current)
        if name is not None:
            found.add(name)
    return found


def coerce(value: Any, ros_type: str) -> Any:
    """One caller value, converted to its declared ROS type, or an error.

    ``bool`` is checked before the integer types on purpose: in Python ``True``
    is an ``int``, so an unguarded integer check would silently accept a
    boolean for an ``int32`` and send ``1`` where the caller wrote ``true``.
    """
    if ros_type == "bool":
        if isinstance(value, bool):
            return value
        raise TemplateError(f"expected a bool for type '{ros_type}', got {type(value).__name__}")
    if ros_type in STRING_TYPES:
        if isinstance(value, str):
            return value
        raise TemplateError(f"expected a string for type '{ros_type}', got {type(value).__name__}")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TemplateError(f"expected a number for type '{ros_type}', got {type(value).__name__}")
    if ros_type in INTEGER_TYPES:
        # 1.0 and 1 are the same number and JSON cannot tell them apart, so an
        # integral float is accepted. 1.5 is a different number and is not.
        if isinstance(value, float) and not value.is_integer():
            raise TemplateError(f"expected an integer for type '{ros_type}', got {value}")
        return int(value)
    if ros_type in FLOAT_TYPES:
        return float(value)
    raise TemplateError(f"unknown parameter type '{ros_type}'")


def _substitute(node: Any, values: Mapping[str, Any]) -> Any:
    if isinstance(node, dict):
        return {key: _substitute(item, values) for key, item in node.items()}
    if isinstance(node, list):
        return [_substitute(item, values) for item in node]
    name = _placeholder(node)
    if name is None:
        return node
    if name not in values:
        raise TemplateError(f"no value for parameter '{name}'")
    return values[name]


def dereference(template: Any, *, shared: Mapping[str, Any]) -> Any:
    """The body a ``message:`` position names, with nothing substituted yet.

    Position decides what a ``${name}`` means -- the one rule to hold: at
    ``message:`` it names a shared template, elsewhere in a body it names a
    parameter. Checking instead whether the name happens to be declared would
    let a parameter added later silently reinterpret a literal somewhere else.

    Split out from ``resolve_template`` because a failsafe needs exactly this
    half and not the other: it has to ask whether any placeholder survived
    following the reference, and ``resolve_template`` cannot be asked that --
    it refuses a placeholder it has no value for, so a question put after it
    would never be reached. See ``params.resolve_failsafe_body``.
    """
    reference = _placeholder(template)
    if reference is None:
        return template
    if reference not in shared:
        raise TemplateError(f"no shared message named '{reference}'")
    body = shared[reference]
    if _placeholder(body) is not None:
        raise TemplateError(
            f"shared message '{reference}' is a nested reference; nesting is not allowed"
        )
    return body


def resolve_template(
    template: Any,
    values: Mapping[str, Any],
    *,
    shared: Mapping[str, Any],
) -> Any:
    """A template plus caller values, as a plain tree ready for ``set_message_fields``."""
    return _substitute(dereference(template, shared=shared), values)
