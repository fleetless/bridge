# Vendored contract schemas

`schema/` holds copies of the JSON Schema artifacts for the twenty-five wire
messages the bridge speaks: the handshake (hello, hello_ok, hello_error,
ping, pong), configuration and introspection (config, config_applied,
introspect_request, introspect, type_request, type_definitions), telemetry
(datapoint, pressure), jobs (invoke, cancel, publish, job_update, job_lost),
cameras (snapshot [binary frame header], camera_start, camera_stop,
camera_state) and assets (assets_available, asset_request, asset_progress).

The count and the list are both here on purpose, and they have to agree: the
list said twenty-four and named twenty-four while twenty-five files sat in the
directory, which is exactly the state somebody re-vendoring reads as "one of
these is stray".

**`../../fleetless_bridge/contracts_constants.json` is a second,
differently-shaped vendored artifact, not a schema** — a copy of the contracts
package's own `constants.json`, loaded by `ros_runtime.py` at import time
rather than only checked against by the test suite. It lives inside the Python
package, not here, because it has to ship with an installed robot (`setup.py`'s
`package_data`), while `schema/` only ever needs to exist for this package's
own suite. `test_the_vendored_constants_still_match_the_contracts` is its
staleness guard, same shape as the schema one. Before this, the values it
carries (`URDF_ASSET_NAME`, `ASSET_UPLOAD_HEADERS`) were Python literals
hand-typed under a comment naming the TypeScript constant they came from — the
exact drift these constants exist to prevent, surviving in the one place that
could not import them.

**The three asset frames describe a conversation, not a payload.** The bytes never
travel on this socket — a frame is capped at 2 MiB and a mesh exceeds that
routinely — so these say what exists, transfer this, and here is how far I got,
while the bytes go over HTTP with the robot's own credential. A schema here is
not evidence that the transfer works; that needs an end-to-end check. They are
vendored so that this package builds and tests with nothing but this
repository and a `ros:humble` image — no npm, no network, no build step. Every one of these schemas is
self-contained (no cross-file `$ref`), which is why the REST-only shapes
(`robot-config-doc`, `action-config`, `service-config`, `publisher-config`,
`parameter-spec`, `datapoint-config`, `ros-graph`, `type-definition`,
`camera-list-response`, `live-session-response`, `snapshot-meta-response`,
... — used by the cloud's HTTP API, not the bridge protocol) are not
vendored here even though they describe the same data: `cloud-config`
embeds the whole `robot-config-doc` tree (and everything nested under it,
`cameras` included) inline, the same way `bridge-introspect` and
`bridge-type-definitions` embed the graph/field-tree shapes.

**`schema-outgoing/` is a second copy of exactly
twelve of the twenty-five — the ones `protocol.py` sends, never receives —
in zod's *output* mode instead of `schema/`'s input mode.** Input mode is
the right description of the wire contract (a `.default()`ed field reads
optional, matching the real cloud's own lenient `zod.parse()`), but every
message in `protocol.py` is a hand-typed dict literal with no static
protection against a key typo — unlike an attribute access on a dataclass,
which would raise. Output mode reads `.default()`ed fields as required and
keeps `additionalProperties: false`, so `schemas.py`'s `validate_frame`
checks an outgoing frame (`OUTGOING_FRAME_NAMES`) against this stricter
copy by default, catching a missing or misspelled field here instead of
letting it reach the cloud's own parser, which would just silently drop
it. `schema/`'s copies of the same twelve stay in service too, for the
rarer test that deliberately proves what the *contract* tolerates rather
than what this bridge's own serializer produces (`strict=False`).

## Which version these came from

    Source: @fleetless/contracts@1.0.5
            artifacts/schema/, artifacts/schema-outgoing/, artifacts/constants.json

An **exact npm version**, not a git revision. The contracts package is
published; a revision of the repository that produced it is not something a
reader of this file can resolve, and two people reading "the current pin" a
month apart have to end up with the same bytes.

`test/test_contracts_sync.py` compares these copies against a real
`@fleetless/contracts` package when `FLEETLESS_CONTRACTS_DIR` names one — a
checkout, or the `package/` directory an
`npm pack @fleetless/contracts@<version>` unpacks to — and skips when the
variable is unset, saying so. It also checks that the package it was pointed at
carries the version this section names, so a comparison against some other
release cannot be reported as agreement with this line. The comparison is the
guard; this line is the record.

## Refreshing

1. Get the artifacts for the version you want:

       npm pack @fleetless/contracts@<version>
       tar xf fleetless-contracts-<version>.tgz

   `package/artifacts/` is what you need, and `package/` is what
   `FLEETLESS_CONTRACTS_DIR` should point at when you run the suite below.
   (From a contracts checkout, `pnpm artifacts` in it produces the same tree;
   a test there fails if the committed artifacts are stale.)

2. Copy `artifacts/schema/`, `artifacts/schema-outgoing/` and
   `artifacts/constants.json` over the copies here — **all of them, not the
   ones you think changed.** Hand-picking files is how a "we re-vendored" claim
   quietly stops being true.

3. Update the version above.

4. `FLEETLESS_CONTRACTS_DIR=<that directory> ./run-tests.sh`. Without the
   variable the four comparisons skip; `run-tests.sh` prints that it is
   skipping them, and it is those four that are the whole point of this step.

**"We re-vendored" and "nothing else changed" are different claims**, and only
the second one needs evidence: diff every vendored file against the new
version's artifacts rather than assuming a version bump moved nothing. An
overwrite that changes nothing and a copy that never happened look identical in
a directory listing, which is why the copy above is unconditional and the test
run after it is not optional.

Do not edit the files here — the contracts package is the source.

**The per-version history this file used to carry is gone.** It recorded, for
every re-vendor, which artifacts moved and which did not, by internal revision.
Those revisions name a repository this one no longer travels with, so the
entries could not be read by anybody using this file. What they were for — an
answer to "did this bump change a frame the bridge sends?" — is answered
instead by diffing the copies against the new version's artifacts, at the
moment somebody actually needs it.
