# SPDX-License-Identifier: Apache-2.0
"""Reading ROS2 IDL type strings.

`SomeMessage.get_fields_and_field_types()` (a classmethod every generated
message provides, no instance needed) reports each field's IDL type as a
string: a primitive (`'float'`, `'uint8'`, `'string'`, ...), an array of one
(`'sequence<T>'`, `'sequence<T, N>'` bounded, or `'T[N]'` fixed-size), or a
nested message in *short* two-part form (`'std_msgs/Header'` — never the
three-part `'pkg/msg/Type'` used everywhere else in the contracts). This
module is the one place that reads those strings, so introspection.py and
sampling.py agree on what they mean.
"""
from __future__ import annotations

import re
from typing import Tuple

_SEQUENCE_RE = re.compile(r"^sequence<(?P<item>.+?)(?:,\s*\d+)?>$")
_FIXED_ARRAY_RE = re.compile(r"^(?P<item>.+)\[(?:<=)?\d+\]$")


def classify(type_str: str) -> Tuple[str, bool]:
    """Split an IDL type string into `(item_type, is_array)`. A plain scalar
    type is returned unchanged with `is_array=False`."""
    match = _SEQUENCE_RE.match(type_str)
    if match:
        return match.group("item"), True
    match = _FIXED_ARRAY_RE.match(type_str)
    if match:
        return match.group("item"), True
    return type_str, False


def is_message_ref(item_type: str) -> bool:
    """A field's item type is a nested message iff its IDL name carries a
    package — primitives (`'float'`, `'uint8'`, ...) never do."""
    return "/" in item_type


def full_type_name(short_ref: str) -> str:
    """`'std_msgs/Header'` -> `'std_msgs/msg/Header'`. Nested message fields
    are always reported in the short two-part form; the rest of the codebase
    (and the contracts) use the three-part form everywhere, so this is the
    one place that inserts the missing `/msg/`."""
    package, _, class_name = short_ref.partition("/")
    return "{}/msg/{}".format(package, class_name)
