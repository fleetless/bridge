# SPDX-License-Identifier: Apache-2.0
"""Validation against the vendored contracts artifacts (see SOURCE.md).

Validates both directions: frames the bridge sends, and frames the fake
cloud sends to it. The second half keeps the fake honest — it's only useful
if it behaves like the cloud the contracts describe.

Two vendored copies: `schema/` is input-mode, matching the cloud's lenient
`zod.parse()` — `.default()`ed fields read optional. Right for describing the
wire contract, wrong for the one other thing this module uses it for:
outgoing messages in `protocol.py` are hand-typed dict literals
(`{"type": "hello", ...}`), so a typo in a key name gets no static
protection — a dataclass attribute access would raise, this wouldn't.
`schema-outgoing/` is output-mode, for exactly the thirteen frames
`protocol.py` builds (`OUTGOING_FRAME_NAMES` below): `.default()`ed fields
are required and `additionalProperties: false` is back, so a missing or
misspelled field fails loudly here instead of being silently dropped by the
cloud's own parser. Frames the bridge only ever *receives* (`cloud-*`) stay
on the input-mode copy — this module need not be stricter than the real
cloud about what it tolerates from itself.
"""
import json
import pathlib
from typing import Optional

# Apt's own jsonschema carries no Draft202012Validator on Humble (3.2.0) —
# Jazzy's 4.10.3 and Lyrical's 4.19.2 both have it; the class has shipped
# since jsonschema 4.0.0, and Humble alone predates that — even though every
# vendored schema declares the Draft 2020-12 $schema URI. Draft7Validator is
# the fallback rather than a shim written here, because it IS correct for
# these schemas: none uses a keyword 2020-12 added over Draft 7. That is not
# a one-off claim taken on faith — `test/test_contracts_sync.py` walks every
# vendored schema for exactly those keywords on every run, with a fixture
# proving the walk can actually find one, because Draft7Validator does not
# error on an unknown keyword, it silently ignores it, and a scan that always
# returned empty would "prove" the same thing a scan that works does. A newer
# jsonschema, where it exists, is still preferred over the fallback.
try:
    from jsonschema import Draft202012Validator as _Validator
except ImportError:
    from jsonschema import Draft7Validator as _Validator

SCHEMA_DIR = pathlib.Path(__file__).parent / "contracts" / "schema"
SCHEMA_OUTGOING_DIR = pathlib.Path(__file__).parent / "contracts" / "schema-outgoing"

# The thirteen frames protocol.py builds and sends — see hello_message,
# pong_message, link_mode_message, config_applied_message,
# introspect_message, type_definitions_message, datapoint_message,
# job_update_message, job_lost_message, snapshot_frame,
# bridge_camera_state_message, bridge_assets_available_message,
# bridge_asset_progress_message.
# Exhaustive list, not a naming pattern — datapoint-frame and
# snapshot-header carry no "bridge-" prefix.
OUTGOING_FRAME_NAMES = frozenset(
    {
        "bridge-hello",
        "bridge-pong",
        "bridge-link-mode",
        "bridge-config-applied",
        "bridge-introspect",
        "bridge-type-definitions",
        "datapoint-frame",
        "bridge-job-update",
        "bridge-job-lost",
        "snapshot-header",
        "bridge-camera-state",
        "bridge-assets-available",
        "bridge-asset-progress",
    }
)

_validators = {}


def _validator(name: str, *, strict: bool) -> _Validator:
    key = (name, strict)
    if key not in _validators:
        directory = SCHEMA_OUTGOING_DIR if strict else SCHEMA_DIR
        with (directory / "{}.schema.json".format(name)).open() as handle:
            _validators[key] = _Validator(json.load(handle))
    return _validators[key]


def validate_frame(name: str, payload: dict, *, strict: Optional[bool] = None) -> dict:
    """Raise `ValidationError` unless `payload` is a valid `name` frame — the
    strict output-mode copy for a bridge-sent frame (`OUTGOING_FRAME_NAMES`,
    the default when `strict` is left `None`), the input-mode copy for one
    it only receives.

    `strict=False` forces the lenient input-mode copy even for a name in
    `OUTGOING_FRAME_NAMES`: some tests build a frame by hand to prove what
    the *wire contract* tolerates (omitting a `.default()`ed field), not
    what `protocol.py` actually produces — `hello_message()` always sends
    `active_jobs`, so that claim would be false. Conflating the two would
    fail those tests for the wrong reason, or quietly weaken this module's
    guarantee for real callers. The caller states which question it is
    asking."""
    is_strict = name in OUTGOING_FRAME_NAMES if strict is None else strict
    _validator(name, strict=is_strict).validate(payload)
    return payload
