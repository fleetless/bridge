# SPDX-License-Identifier: Apache-2.0
"""Building ROS goal/request/message instances from a configured message
template plus caller-supplied values.

Since 3.0 there is exactly one story here, and it starts in the
configuration document rather than on the invoke frame. The developer writes
the Goal, Request or published message out in full, with `${name}` at the
positions a caller may fill; `templates.py` substitutes the caller's values
into a copy of that tree; `build_message` hands the result to rosidl's own
`set_message_fields` — the same function `ros2 topic pub`/`ros2 action
send_goal` use — which instantiates nested messages, builds arrays and
coerces scalars. Reusing it means the bridge accepts exactly what those tools
would, and does not maintain a second, subtly different notion of "how does a
dict become a message".

**There are no dotted field paths any more, and no `unflatten`.** A
parameter's name is a free-standing identifier, not a path into the message;
where its value lands is decided by where the developer wrote the
placeholder. The machinery that turned `"pose.position.x"` into a nested tree
had no remaining input and is gone rather than left standing.

Structural validation lives here too, and it is narrower than it used to be:
`validate_template` answers "can this template ever produce this message
type", by resolving it with a stand-in value of each declared parameter's
type and building it. The whole-document questions — every placeholder names
a declared parameter, every declared parameter is used — need the joined
index across `messages:` and the entry's own `parameters:`, which is the
cloud's to hold (contracts' `undeclared_parameter`/`unused_parameter`).

`resolve_failsafe_body` is the one check here that is not a narrower version
of a cloud check — see its own docstring.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping

from rosidl_runtime_py.set_message import set_message_fields

from fleetless_bridge import templates


def build_message(message_class, nested: dict):
    """Instantiate `message_class` and fill it from `nested` — already the
    right shape (a plain dict/list/scalar tree: a resolved template, or a
    publisher's resolved failsafe body)."""
    msg = message_class()
    set_message_fields(msg, nested)
    return msg


def _probe_value(ros_type: str) -> Any:
    """A stand-in of the declared type, for the apply-time build check.

    Its *value* is never sent anywhere and is deliberately uninteresting; its
    *type* is the whole point — it decides whether `set_message_fields` can
    place it. An unknown declared type raises here rather than defaulting to
    something plausible: a parameter the bridge cannot type is one it cannot
    build a message from."""
    if ros_type == "bool":
        return False
    if ros_type in templates.STRING_TYPES:
        return ""
    if ros_type in templates.INTEGER_TYPES:
        return 0
    if ros_type in templates.FLOAT_TYPES:
        return 0.0
    raise templates.TemplateError("unknown parameter type {!r}".format(ros_type))


def build_from_template(
    message_class,
    template: Any,
    parameters: Mapping[str, Any],
    values: Mapping[str, Any],
    *,
    shared: Mapping[str, Any],
):
    """One caller's `invoke.params`/`publish.message` to a filled message.

    Every supplied value is coerced against its parameter's declared `type`
    first, and a name that was never declared is refused rather than passed
    through: an undeclared name cannot be typed, so nothing here could say
    what it means, and the version of this that quietly ignored it would let
    a stale cloud send a value that silently never arrives.

    **An omitted parameter falls back to its declared `default`, and this is
    the only place that happens.** The cloud validates a caller's values and
    deliberately does not fill defaults — the cloud says so on the branch
    that decides required-ness: "an omitted optional parameter reaches the bridge
    omitted... filling the default belongs with the message-template walk
    that puts a parameter's value at its placeholder's position". This is
    that walk. A `None` is treated as omitted for the same reason the cloud
    does: a browser client that clears an input sends `null` rather than
    dropping the key.

    Bounds (`min_value`, `max_value`, `enum`, `regex`) are **not** re-checked
    — they are enforced in the cloud before anything reaches the robot, and a
    second enforcement point here would be the weaker of two policies for one
    decision. Type agreement is different: it is a structural precondition
    for building the message at all."""
    for name in values:
        if name not in parameters:
            raise templates.TemplateError("no parameter named {!r} is declared".format(name))
    coerced: Dict[str, Any] = {}
    for name, spec in parameters.items():
        value = values.get(name)
        if value is None:
            value = spec.default
        if value is None:
            continue  # required and unsupplied: resolve_template names it, if it is used
        coerced[name] = templates.coerce(value, spec.type)
    return build_message(message_class, templates.resolve_template(template, coerced, shared=shared))


def validate_template(
    message_class,
    template: Any,
    parameters: Mapping[str, Any],
    *,
    shared: Mapping[str, Any],
) -> None:
    """Confirms `template` can build a `message_class` at all — raises if not.

    Called at config-apply time, so a template that could never produce this
    message type is a `config_applied` error immediately rather than a
    surprise the first time somebody invokes the slug. It resolves the
    template with a stand-in value of each declared parameter's type, which
    catches a reference to a shared message that does not exist, a nested
    reference, a placeholder naming nothing that is declared, an undeclared
    parameter type, and a field the message type does not have.

    What it deliberately does not catch is a value-shaped problem — the
    caller's actual number is not here yet, and its bounds are the cloud's."""
    values = {name: _probe_value(spec.type) for name, spec in parameters.items()}
    build_message(message_class, templates.resolve_template(template, values, shared=shared))


def resolve_failsafe_body(message_class, message: Any, *, shared: Mapping[str, Any]) -> Any:
    """A publisher's failsafe as a finished tree, or an error — no caller,
    ever, so no placeholder may survive.

    **This is not a narrower copy of a cloud check.** Contracts refuses a
    placeholder in an *inline* failsafe body and says plainly that it cannot
    answer the other half: a schema over one publisher cannot see `messages:`,
    so a failsafe written `message: ${stop}` where `stop` holds a `${speed}`
    passes every schema there is. That case is caught here and nowhere else
    before the robot arms a failsafe it could never send — and the moment it
    is needed is an emergency, which is the worst possible time to find out.

    Resolved once, at config-apply time, and stored: `_fire_failsafe` builds
    from the tree this returns rather than re-following the reference under a
    watchdog tick."""
    body = templates.dereference(message, shared=shared)
    unfilled = templates.placeholder_names(body)
    if unfilled:
        raise templates.TemplateError(
            "a failsafe message must contain no placeholder — nothing will ever fill "
            "{}".format(", ".join("'{}'".format(name) for name in sorted(unfilled)))
        )
    # Built once here for the same reason the placeholder check runs here: a
    # failsafe that cannot be built is caught now, not the first time the
    # watchdog needs to fire it.
    build_message(message_class, body)
    return body
