# SPDX-License-Identifier: Apache-2.0
"""Turning a ROS message into a datapoint sample.

Three independent concerns live here:
- `resolve_field` / `extract_value`: the `field` path — `null` means the
  whole message, a dot path with optional `[idx]` picks one value or one
  nested message out of it.
- `value_to_json`: the fixed message -> JSON rules (nested message -> object,
  sequence -> array, `uint8[]` -> base64, everything else passthrough — this
  also covers `builtin_interfaces/Time` and `Duration`, whose only fields are
  `sec`/`nanosec` ints, so `{sec, nanosec}` falls out of the generic nested
  message rule with no special case).
- `RatePolicy`: a `rate_throttle_hz` ceiling (drop samples faster than
  `1/hz`), or, when there is none, drop samples equal to the last one sent —
  the bridge is the only place that rate-limits, so every subscriber of a
  slug sees the same rate.
"""
from __future__ import annotations

import base64
import re
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from fleetless_bridge.ros_types import classify, full_type_name, is_message_ref

_SEGMENT_RE = re.compile(r"^([a-z_][a-z0-9_]*)((?:\[\d+\])*)$")
_INDEX_RE = re.compile(r"\[(\d+)\]")


class FieldPathError(ValueError):
    """`field` does not resolve against the message type it names."""


def split_field_path(field_path: str) -> List[Tuple[str, List[int]]]:
    segments = []
    for part in field_path.split("."):
        match = _SEGMENT_RE.match(part)
        if not match:
            raise FieldPathError("unusable field path segment: {!r}".format(part))
        name = match.group(1)
        indices = [int(i) for i in _INDEX_RE.findall(match.group(2))]
        segments.append((name, indices))
    return segments


def resolve_field(message_class, field_path: Optional[str]) -> str:
    """The IDL type string of `field_path` against `message_class` — a
    structural check, no message instance needed. Raises `FieldPathError`
    for an unresolvable path (unknown field name, or indexing something
    that is not an array). Used at config-apply time, so a bad field path
    becomes a `config_applied` error immediately, not a crash the first
    time a message arrives (the format: the bridge validates structurally
    too)."""
    if field_path is None:
        return full_type_name_of(message_class)

    current_class = message_class
    type_str = None
    for name, indices in split_field_path(field_path):
        if len(indices) > 1:
            # ROS2 IDL has no nested/multi-dimensional arrays: a path
            # indexes one level per segment.
            raise FieldPathError("{!r}: ROS has no nested arrays".format(name))
        fields = current_class.get_fields_and_field_types()
        if name not in fields:
            raise FieldPathError(
                "{!r} has no field {!r}".format(full_type_name_of(current_class), name)
            )
        type_str = fields[name]
        item_type, is_array = classify(type_str)
        if indices:
            if not is_array:
                raise FieldPathError("{!r} is not an array".format(name))
            type_str = item_type
        if is_message_ref(item_type):
            current_class = _import_nested(item_type)
    return type_str


def full_type_name_of(message_class) -> str:
    module = message_class.__module__.split(".")[0]
    return "{}/msg/{}".format(module, message_class.__name__)


def _import_nested(item_type: str):
    from rosidl_runtime_py.utilities import get_message

    return get_message(full_type_name(item_type))


def extract_value(msg: Any, field_path: Optional[str]) -> Any:
    """The raw value (not yet JSON-converted) `field_path` picks out of `msg`
    — a message instance, a primitive, or a list, matching what `resolve_field`
    validated. `field_path=None` returns `msg` itself."""
    if field_path is None:
        return msg
    current = msg
    for name, indices in split_field_path(field_path):
        current = getattr(current, name)
        for idx in indices:
            current = current[idx]
    return current


def value_to_json(value: Any, type_str: str) -> Any:
    """The fixed value -> JSON rules for one field's raw value, given its IDL
    type string (from `resolve_field`, or a message class's own
    `get_fields_and_field_types()` while recursing)."""
    item_type, is_array = classify(type_str)
    if item_type == "uint8" and is_array:
        return base64.b64encode(bytes(value)).decode("ascii")
    if is_message_ref(item_type):
        if is_array:
            return [_message_fields_to_json(item) for item in value]
        return _message_fields_to_json(value)
    if is_array:
        return list(value)
    return value


def _message_fields_to_json(msg: Any) -> dict:
    return {
        name: value_to_json(getattr(msg, name), type_str)
        for name, type_str in type(msg).get_fields_and_field_types().items()
    }


def message_to_json(msg: Any) -> dict:
    """The whole-message conversion (`field: null`)."""
    return _message_fields_to_json(msg)


def apply_scale_offset(value: Any, scale: Optional[float], offset: Optional[float]) -> Any:
    """`value*scale + offset` — only when the value is numeric and the
    config sets scale/offset (applied by the bridge; unit and range are
    metadata, passed through untouched)."""
    if scale is None and offset is None:
        return value
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return value
    return value * (1.0 if scale is None else scale) + (0.0 if offset is None else offset)


class RatePolicy:
    """Decides, for one slug, whether the next sample should be sent."""

    def should_send(self, value: Any, now: float) -> bool:  # noqa: D102 - see subclasses
        raise NotImplementedError


class MaxHzPolicy(RatePolicy):
    """At most `hz` sends per second; the first sample always goes.

    `_TOLERANCE`: a cap set at (or near) the
    source's own rate lost about a third of samples in practice — 10 Hz
    source, 10 Hz cap measured 6.58 Hz — because comparing the raw interval
    against `_min_interval` with no slack makes one barely-early arrival
    (ordinary publish jitter, not a genuinely faster source) get rejected,
    and rejection does not advance `_last_sent`, so the *next* arrival is
    now compared against a baseline that is further behind than the source
    actually is. A small relative tolerance absorbs realistic jitter
    without loosening the cap for a source that really is faster: it only
    ever pulls the threshold down by up to 2%, so a source publishing at
    twice the cap or more is throttled exactly as before."""

    _TOLERANCE = 0.02  # 2% of the interval

    def __init__(self, hz: float) -> None:
        self._min_interval = 1.0 / hz
        self._threshold = self._min_interval * (1.0 - self._TOLERANCE)
        self._last_sent: Optional[float] = None

    def should_send(self, value: Any, now: float) -> bool:
        if self._last_sent is None or now - self._last_sent >= self._threshold:
            # Baselined on the actual arrival time, not the ideal schedule
            # (`_last_sent + _min_interval`) — an ideal-schedule baseline
            # would let a run of early arrivals silently drift the
            # effective rate upward over time; anchoring on `now` instead
            # means the tolerance only ever forgives one arrival's worth of
            # jitter at a time, never accumulates it.
            self._last_sent = now
            return True
        return False


class AverageHzPolicy(RatePolicy):
    """`hz` sends per second on average, whatever grid the arrivals land on.

    **Why not `MaxHzPolicy`.** A minimum gap is exactly right for a ceiling
    applied to a raw topic and exactly wrong for one applied to a stream some
    other policy has already thinned. Low-bandwidth mode is the second case:
    the configured `rate_throttle_hz` decides what is recorded, and the mode
    then picks which of those go out now. Ask a 5 Hz minimum interval about a
    5.56 Hz grid and it is never quite due — 0.18 s against a 0.196 s
    threshold — so every second arrival is skipped and the result is 2.8 Hz,
    not 5. The error is invisible to any check of the form "at most the
    ceiling".

    So this spends a credit rather than measuring a gap: one per send,
    refilled at `hz` per second. Over any arrival pattern at or above `hz`
    the long-run rate is `hz`; below it, every arrival passes.

    `_CAPACITY` bounds what a silence can bank. It has to be above one, or the
    credit left over from a grid slightly faster than `hz` is clamped away on
    the very next arrival and the policy collapses to the half rate it exists
    to avoid — which is the same defect in a different costume. Two is the
    smallest value that cannot: an arrival can never accrue more than one
    credit while the source is faster than `hz`, and when it is slower every
    arrival passes anyway. A datapoint quiet for an hour therefore comes back
    with two samples, not an hour of them.
    """

    #: Credits, not seconds. See the class docstring for why it is not one.
    _CAPACITY = 2.0

    def __init__(self, hz: float) -> None:
        self._hz = hz
        self._credit = 1.0
        self._last: Optional[float] = None

    def should_send(self, value: Any, now: float) -> bool:
        if self._last is not None:
            self._credit = min(self._CAPACITY, self._credit + (now - self._last) * self._hz)
        self._last = now
        if self._credit >= 1.0:
            self._credit -= 1.0
            return True
        return False


class SendEveryPolicy(RatePolicy):
    """Every sample goes. What an absent or zero `rate_throttle_hz` means.

    3.0 removed the `rate: {mode, hz}` object, and with it the
    `on_change` mode that dropped a sample equal to the last one sent.
    Removing a mode from the format makes it **unavailable**, not the
    default — "no throttling" means no filtering of any kind.

    The distinction is not academic, and getting it backwards is expensive
    in the quiet direction. A battery holding a steady 87 % is the ordinary
    case, and most datapoints will carry no `rate_throttle_hz` at all. Under
    deduplication such a datapoint publishes once and then nothing for
    hours: the live value looks stale, and with `retention.enabled` the
    cloud has nothing to write, so the history it is being billed for is
    empty. Nothing errors. Nothing is logged. It reads as a robot that has
    stopped reporting."""

    def should_send(self, value: Any, now: float) -> bool:
        return True


def rate_policy(hz: Optional[float]) -> RatePolicy:
    """The policy a datapoint's `rate_throttle_hz` asks for.

    `None` and `0` are the same answer — no ceiling — and both have to be
    turned into a policy here rather than passed on, because `MaxHzPolicy`
    divides by `hz`. `0` is in range on the wire (contracts bounds it
    `0..20`), so this is an ordinary value to receive, not a malformed one
    to reject."""
    if hz:
        return MaxHzPolicy(hz)
    return SendEveryPolicy()


@dataclass
class Sample:
    slug: str
    value: Any
    timestamp_ms: int


def capture_timestamp_ms() -> int:
    """The bridge capture instant — read this first, before any
    extraction/conversion work, wherever a subscription callback starts."""
    return int(time.time() * 1000)
