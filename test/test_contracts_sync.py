# SPDX-License-Identifier: Apache-2.0
"""Guards the vendored schemas and vendored contracts constants against
drifting from the real contracts.

`test/contracts/schema/` and `fleetless_bridge/contracts_constants.json` are
vendored so the suite runs from a bare clone, no npm, no network. Compared
against `@fleetless/contracts` when a copy is available; skipped when not.

**Why the copy is named, not inferred.** `$FLEETLESS_CONTRACTS_DIR` names the
directory holding `artifacts/` — a contracts checkout or an unpacked
`npm pack @fleetless/contracts@<version>` tarball. It used to be inferred as
"a directory called `contracts` two levels up". That broke both ways: outside
that layout every comparison skipped silently and drift landed; beside any
same-named directory it compared against a stranger's tree and reported a
pass. A path test cannot tell the source of truth from a same-named
directory, so it is named. `run-tests.sh` passes the variable through and
says when it is unset, so a skip is read, not missed.
"""
import json
import os
import pathlib
import re

import pytest
from schemas import OUTGOING_FRAME_NAMES, SCHEMA_DIR, SCHEMA_OUTGOING_DIR

from fleetless_bridge.protocol import PROTOCOL_VERSION

_ENV_VAR = "FLEETLESS_CONTRACTS_DIR"
_NO_CONTRACTS = (
    "set {}=<a @fleetless/contracts checkout or unpacked npm tarball> "
    "to compare the vendored copies against it".format(_ENV_VAR)
)

_configured = os.environ.get(_ENV_VAR, "").strip()
CONTRACTS_ROOT_DIR = pathlib.Path(_configured).resolve() if _configured else None


def _under_contracts(*parts):
    """A path inside the configured contracts package, or None when there is
    none. Returning None rather than a path under a bogus root keeps
    `is_file()`/`is_dir()` from being asked about a path that means nothing."""
    if CONTRACTS_ROOT_DIR is None:
        return None
    return CONTRACTS_ROOT_DIR.joinpath(*parts)


def _no_dir(path):
    return path is None or not path.is_dir()


def _no_file(path):
    return path is None or not path.is_file()


CONTRACTS_ARTIFACTS_DIR = _under_contracts("artifacts")
CONTRACTS_DIR = _under_contracts("artifacts", "schema")
CONTRACTS_OUTGOING_DIR = _under_contracts("artifacts", "schema-outgoing")
CONTRACTS_CONSTANTS_FILE = _under_contracts("artifacts", "constants.json")
#: `PROTOCOL_VERSION` is declared in the TypeScript source and survives into
#: the compiled `dist/` the npm package ships, in the same `export const`
#: shape. Both are accepted so that this guard is reachable from an unpacked
#: tarball, which carries `dist/` and no `src/`, and not only from a checkout.
CONTRACTS_PROTOCOL_SOURCE_CANDIDATES = [
    p
    for p in (_under_contracts("src", "protocol.ts"), _under_contracts("dist", "protocol.js"))
    if p is not None
]
VENDORED_CONSTANTS_FILE = (
    pathlib.Path(__file__).resolve().parents[1] / "fleetless_bridge" / "contracts_constants.json"
)


def _protocol_source_file():
    for candidate in CONTRACTS_PROTOCOL_SOURCE_CANDIDATES:
        if candidate.is_file():
            return candidate
    return None


@pytest.mark.skipif(_no_dir(CONTRACTS_DIR), reason=_NO_CONTRACTS)
def test_the_vendored_schemas_still_match_the_contracts():
    stale = []
    for vendored in sorted(SCHEMA_DIR.glob("*.schema.json")):
        source = CONTRACTS_DIR / vendored.name
        assert source.is_file(), "{} no longer exists in contracts".format(vendored.name)
        if source.read_bytes() != vendored.read_bytes():
            stale.append(vendored.name)
    assert not stale, (
        "vendored schemas are out of date: {}. Copy them again from "
        "contracts/artifacts/schema and update test/contracts/SOURCE.md.".format(
            ", ".join(stale)
        )
    )


@pytest.mark.skipif(_no_dir(CONTRACTS_OUTGOING_DIR), reason=_NO_CONTRACTS)
def test_the_vendored_outgoing_schemas_still_match_the_contracts():
    """`schema-outgoing/` is the strict, output-mode copy for the twelve
    frames `OUTGOING_FRAME_NAMES` says the bridge itself sends — same
    staleness guard as the input-mode copy above, kept separate rather than
    globbed together so a name present in only one directory fails loudly
    (`source.is_file()` / `vendored.name not in {the other set}`) instead of
    being silently skipped."""
    on_disk = {p.name[: -len(".schema.json")] for p in SCHEMA_OUTGOING_DIR.glob("*.schema.json")}
    assert on_disk == OUTGOING_FRAME_NAMES, (
        "schema-outgoing/ does not match schemas.py's OUTGOING_FRAME_NAMES: "
        "on disk only {}, in the list only {}.".format(
            sorted(on_disk - OUTGOING_FRAME_NAMES), sorted(OUTGOING_FRAME_NAMES - on_disk)
        )
    )
    stale = []
    for vendored in sorted(SCHEMA_OUTGOING_DIR.glob("*.schema.json")):
        source = CONTRACTS_OUTGOING_DIR / vendored.name
        assert source.is_file(), "{} no longer exists in contracts' schema-outgoing".format(
            vendored.name
        )
        if source.read_bytes() != vendored.read_bytes():
            stale.append(vendored.name)
    assert not stale, (
        "vendored outgoing schemas are out of date: {}. Copy them again from "
        "contracts/artifacts/schema-outgoing and update test/contracts/SOURCE.md.".format(
            ", ".join(stale)
        )
    )


def test_every_vendored_outgoing_schema_actually_rejects_unknown_properties():
    """The property this second copy exists for, asserted directly — not
    assumed from "we vendored the output-mode export". A schema could match
    a fresh export byte for byte and still lack `additionalProperties: false`
    if the export itself regressed; the two byte-comparison guards above
    can't see that. Needs no contracts package — reads only the vendored
    copies, so a standalone clone gets this guarantee too."""
    unguarded = []
    for path in sorted(SCHEMA_OUTGOING_DIR.glob("*.schema.json")):
        schema = json.loads(path.read_text())
        if schema.get("additionalProperties") is not False:
            unguarded.append(path.name)
    assert not unguarded, (
        "these vendored outgoing schemas do not reject unknown properties, "
        "which defeats the entire point of a second, stricter copy: {}".format(
            ", ".join(unguarded)
        )
    )


#: Every keyword Draft 2020-12 added over Draft 7. `Draft7Validator` does not
#: error on an unknown keyword -- it silently ignores it -- so this is the
#: one thing standing between "validated" and "the validator did not know
#: the keyword" for the fallback `schemas.py` takes on Humble (apt's
#: jsonschema there is 3.2.0, with no `Draft202012Validator`; see that
#: module's own comment).
_POST_DRAFT7_KEYWORDS = frozenset({
    "unevaluatedProperties", "unevaluatedItems", "dependentRequired",
    "dependentSchemas", "prefixItems", "minContains", "maxContains",
    "$dynamicRef", "$dynamicAnchor", "$recursiveRef", "$recursiveAnchor",
})


def _post_draft7_keywords(node):
    """Every name in `_POST_DRAFT7_KEYWORDS` found anywhere in a JSON Schema
    document, walked recursively -- not only at the top level, since any of
    these can appear nested inside `properties`, `definitions`, `$defs`,
    `items`, `anyOf`, and so on."""
    found = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key in _POST_DRAFT7_KEYWORDS:
                found.add(key)
            found |= _post_draft7_keywords(value)
    elif isinstance(node, list):
        for item in node:
            found |= _post_draft7_keywords(item)
    return found


def test_the_post_draft7_keyword_scan_actually_flags_one():
    """The guard below needs its own break fixture, checked in rather than
    generated at test time: a scan that always returned empty would "prove"
    the guard the same way a working one does, and only a fixture that must
    be flagged tells the two apart."""
    assert _post_draft7_keywords(
        {"type": "object", "properties": {"a": {"prefixItems": [{"type": "string"}]}}}
    ) == {"prefixItems"}
    assert _post_draft7_keywords({"type": "string", "enum": ["a", "b"]}) == set()


def test_no_vendored_schema_uses_a_keyword_draft7validator_cannot_read():
    """`schemas.py`'s Draft7Validator fallback is correct only if no vendored
    schema uses a keyword Draft 2020-12 added over Draft 7 — otherwise
    Humble's fallback silently stops checking it while every other
    distribution keeps enforcing it. A one-off manual sweep proved this once,
    for the schema set that existed then; this walks the current vendored set
    on every run instead, so a contracts change adding one of these keywords
    is caught here rather than discovered as a bridge that validated a frame
    the cloud's zod parser then rejected. Needs no contracts package — reads
    only the vendored copies, so a standalone clone gets this guarantee
    too."""
    offending = {}
    for directory in (SCHEMA_DIR, SCHEMA_OUTGOING_DIR):
        for path in sorted(directory.glob("*.schema.json")):
            found = _post_draft7_keywords(json.loads(path.read_text()))
            if found:
                offending[str(path.relative_to(directory.parent))] = sorted(found)
    assert not offending, (
        "these vendored schemas use a keyword Draft7Validator ignores rather "
        "than enforces, which the Humble fallback would then silently not "
        "check: {}".format(offending)
    )


@pytest.mark.skipif(_no_file(CONTRACTS_CONSTANTS_FILE), reason=_NO_CONTRACTS)
def test_the_vendored_constants_still_match_the_contracts():
    """`URDF_ASSET_NAME` and `ASSET_UPLOAD_HEADERS` used to be
    Python literals hand-typed under a comment naming the TypeScript
    constant they came from — exactly the drift those constants exist to
    prevent, surviving in the one place that could not import them. This is
    what makes that impossible again: a diff, not a promise."""
    assert CONTRACTS_CONSTANTS_FILE.read_bytes() == VENDORED_CONSTANTS_FILE.read_bytes(), (
        "fleetless_bridge/contracts_constants.json is out of date. Copy it "
        "again from the contracts package and update "
        "test/contracts/SOURCE.md."
    )


@pytest.mark.skipif(_protocol_source_file() is None, reason=_NO_CONTRACTS)
def test_the_bridge_protocol_version_still_matches_the_contracts():
    """`PROTOCOL_VERSION` is a hand-kept literal on both sides of the wire —
    `fleetless_bridge/protocol.py` here, the contracts package there —
    and nothing else checks that they agree. `test_protocol.py`'s own
    `PROTOCOL_VERSION == 2` catches an edit to *this* file, which is real
    value, but not the two sides drifting apart, since it never opens the
    other one. A diff, not a promise, same shape as the schema and constants
    guards above."""
    source_file = _protocol_source_file()
    match = re.search(
        r"export const PROTOCOL_VERSION = (\d+)",
        source_file.read_text(),
    )
    assert match is not None, "could not find PROTOCOL_VERSION in the contracts source"
    assert PROTOCOL_VERSION == int(match.group(1)), (
        "bridge PROTOCOL_VERSION ({}) does not match the contracts source's "
        "PROTOCOL_VERSION ({})".format(PROTOCOL_VERSION, match.group(1))
    )


@pytest.mark.skipif(_no_file(_under_contracts("package.json")), reason=_NO_CONTRACTS)
def test_the_contracts_being_compared_against_are_the_version_source_md_pins():
    """The comparisons above answer "do these bytes match that tree?", not
    "is that tree the version this package vendored" — different questions:
    a contracts checkout at 1.0.6 with artifacts built compares clean or
    dirty by accident of what moved between releases, and either answer
    would be about a version `test/contracts/SOURCE.md` doesn't name. So the
    pin is read from SOURCE.md — the record — and checked against the tree
    being compared against."""
    package_json = json.loads(_under_contracts("package.json").read_text())
    assert package_json.get("name") == "@fleetless/contracts", (
        "{} points at {}, which is not the @fleetless/contracts package".format(
            _ENV_VAR, CONTRACTS_ROOT_DIR
        )
    )
    source_md = (pathlib.Path(__file__).resolve().parent / "contracts" / "SOURCE.md").read_text()
    pinned = re.search(r"@fleetless/contracts@(\S+)", source_md)
    assert pinned is not None, "test/contracts/SOURCE.md names no @fleetless/contracts version"
    assert package_json.get("version") == pinned.group(1), (
        "the vendored copies are recorded as coming from @fleetless/contracts@{}, "
        "but {} points at {}. Re-vendor and update SOURCE.md, or point the "
        "variable at the pinned version.".format(pinned.group(1), _ENV_VAR, package_json.get("version"))
    )
