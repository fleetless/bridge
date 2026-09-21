# SPDX-License-Identifier: Apache-2.0
"""The bridge half of the bridge ↔ cloud wire protocol.

The message shapes mirror the contracts artifacts (`bridge-hello`,
`bridge-pong`, `cloud-ping`, `cloud-hello-ok`, `cloud-hello-error`,
`cloud-config`, `bridge-config-applied`, `cloud-introspect-request`,
`bridge-introspect`, `cloud-type-request`, `bridge-type-definitions`,
`datapoint-frame`, and the `cloud-invoke`, `cloud-cancel`, `cloud-publish`,
`bridge-job-update`, `bridge-job-lost` — plus `bridge-hello` growing
`active_jobs`, `{job_id, slug, state}` per entry (renamed from
`active_job_ids`, the `cloud-invoke`/`cloud-cancel` gaining
`patience_ms`/`job_id`, and the `cloud-camera-start`/`-stop` gaining
`request_id`); copies of those schemas live in `test/contracts/schema/` and
the suite validates every frame against them in both directions.

Parsing is deliberately forgiving. A message this version does not understand
is reported as `Unknown` rather than raising, so that a newer cloud speaking a
richer protocol cannot take an older bridge off the air. The same leniency
applies to a `config` frame that is present but malformed in some field: the
whole frame becomes `Unknown` rather than raising, and rather than trying to
salvage individual datapoint entries — per-entry tolerance belongs to
*applying* a well-formed config (config.py), not to parsing the wire frame.

In 3.0, `doc` is the `fleetless.yaml` document, and every section in it
is a **mapping keyed by slug** rather than a list of entries carrying their
own slug. A parameter is likewise keyed by a short name, and that name is a
free-standing identifier — not a path into the message. What a caller's value
fills is decided by where the developer wrote `${name}` in the entry's
`message` template (`templates.py`), which is a wholesale replacement for the
dotted-field-path scheme below and does not migrate: no 2.x document stays
parseable.

Also new in 3.0: a camera's credentials live **inside its own source**, and
the wire-only `credentials` map beside `doc` is gone with the `credentials_ref`
that named into it. One place for a camera's password, so there is no second
place for it to disagree with.

Cameras: `cameraConfig.topic`/`.type` are replaced by `source`, a discriminated
union on `kind` (`ros | rtsp | mjpeg | v4l2`) — a breaking change,
deliberately not migrated: no 2.x document stays parseable.

Jobs: actions, services and publishers are jobs uniformly — a service call mints a job exactly like an action invoke, and
only the REST/realtime layer above the bridge tells them apart, so `Invoke`
covers both. Parameters are **flat on the wire, keyed by the parameter's own
short name**, for invoke, call *and* publish alike — `Publish`'s `message`
field is despite its name the same flat, caller-supplied shape as
`Invoke.params`, not a nested ROS message body. The nested whole messages are
a publisher's `message` and its `failsafe.message`, authored once by the
developer against the type — they live in `Config`, not in any per-command
frame.

The bridge → cloud direction is built rather than parsed. `graph` and
`definitions`/`unresolved` are accepted here as plain dicts/lists already
shaped like the contracts artifacts (`ros graph` and `type field tree`) — they
are assembled by `introspection.py`, which is where those shapes are defined
and kept in step with the schema; this module only frames them onto the wire.
"""
from __future__ import annotations

import json
import pathlib
import re
import struct
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Sequence, Tuple, Union

# Read from the vendored contracts constants, not typed here: until 2026-09
# the `2` was a literal on both sides of the wire and one test stood between
# them and drift. `ros_runtime.py` loads the same file — two readers of one
# file is fine, two literals is not.
_CONSTANTS_PATH = pathlib.Path(__file__).with_name("contracts_constants.json")
with _CONSTANTS_PATH.open("r", encoding="utf-8") as _f:
    _CONSTANTS = json.load(_f)

#: The protocol version this bridge speaks, sent in every hello.
PROTOCOL_VERSION: int = _CONSTANTS["PROTOCOL_VERSION"]
#: The window the cloud serves: one entry per protocol version, saying which
#: bridge release first spoke it and when it was deprecated (`None` while it
#: is current). The cloud decides; this copy is what the package can answer
#: with offline.
PROTOCOL_VERSIONS: List[Dict[str, Any]] = _CONSTANTS["PROTOCOL_VERSIONS"]
#: The newest bridge release the contracts knew about when they were built —
#: one release behind after every bridge release, by construction.
LATEST_BRIDGE_VERSION: str = _CONSTANTS["LATEST_BRIDGE_VERSION"]

# The `fleetless.yaml` format version — `doc.fleetless`. Still a hand-kept
# literal on both sides of the wire, which `PROTOCOL_VERSION` above stopped
# being: contracts' `FLEETLESS_FORMAT_VERSION` exports no artifact constant
# to vendor, so there is nothing here to read it from.
# Deliberately not called a version anywhere it could be read as the
# published-configuration counter `Config.version` holds: those are two
# different numbers.
FLEETLESS_FORMAT_VERSION = 1

# A hello refused for one of these reasons will be refused again for as long as
# the robot's configuration stays as it is: retrying only hammers the cloud
# with a token it has already rejected.
#
# `protocol_mismatch` left this set with the version window: a cloud that
# refuses a version today may serve it after its own next deploy is rolled
# back, and a robot that exits leaves its supervisor respawning it every five
# seconds. The client waits out a long backoff before that exit instead.
TERMINAL_HELLO_ERROR_CODES = frozenset({"invalid_token"})

# Why a camera_state frame was sent (`bridgeCameraState.cause` in the wire
# contracts). Required on every frame, not just the unsolicited ones:
# {publishing: false, error: None} used to mean three different things — an
# answer to camera_stop, a stream stopped by a config change, and a source
# that recovered — distinguishable only by the cloud remembering what it saw
# before. Every sender already knows its own reason, so this says it rather
# than making the cloud infer it.
CAMERA_STATE_CAUSE_COMMAND = "command"  # answers a camera_start/camera_stop
CAMERA_STATE_CAUSE_SOURCE = "source"  # unsolicited: the source's own health changed
CAMERA_STATE_CAUSE_CONFIG_CHANGE = "config_change"  # a config change stopped this stream
CAMERA_STATE_CAUSE_LIVE_LOST = "live_lost"  # publishing ended unexpectedly after it started

# Why one `bridgeAssetProgress.failed` entry did not make it into the store
# (`assetFailure` in the wire contracts). Six producers wrote three
# different facts into a flat `string[]` indistinguishably —
# reconciliation could not tell "no longer referenced" from "referenced and
# not delivered", and the console printed all of them, including the
# literal `robot_description`, under "these meshes could not be resolved".
# `unresolvable` is the ONLY kind a reconciliation may treat as gone — see
# ros_runtime.py's own call sites for which producer emits which kind and
# why.
ASSET_FAILURE_KIND_UNRESOLVABLE = "unresolvable"  # names nothing this bridge can find, or escapes its package
ASSET_FAILURE_KIND_UPLOAD_FAILED = "upload_failed"  # the bytes exist; the transfer did not succeed
ASSET_FAILURE_KIND_REFUSED = "refused"  # never attempted — a producer-side ceiling, or an aborted sync
# A fourth kind, and deliberately not a `refused` variant: `refused`
# already carries the array's own collective-overflow sentinel, and a
# too-large file under that same key would repeat the `failed: string[]`
# defect one key over —
# two facts sharing one value, either capable of overwriting the other. Only
# `too_large` ever pairs with a non-null `details`: the two
# numbers a developer needs to decide whether to shrink the mesh or ask for
# the ceiling to move.
ASSET_FAILURE_KIND_TOO_LARGE = "too_large"  # never attempted — its size on disk already exceeds ASSET_UPLOAD_MAX_BYTES

# Which kind of exposure a `config_applied` error belongs to (contracts
# `applyErrorKind`) — a closed enum, unlike `ApplyError.code` below. Known at
# all five apply call sites (ros_runtime.py) and at `client.py`'s
# `_apply_or_report`, which used to receive only a human label for a log
# line ("the configuration", "the actions", ...) — not the kind, and "the
# configuration" was the datapoint pass.
APPLY_ERROR_KIND_DATAPOINT = "datapoint"
APPLY_ERROR_KIND_ACTION = "action"
APPLY_ERROR_KIND_SERVICE = "service"
APPLY_ERROR_KIND_PUBLISHER = "publisher"
APPLY_ERROR_KIND_CAMERA = "camera"

# `ApplyError.code` values this bridge actually produces (contracts:
# deliberately a bounded string, not an enum — a classification the bridge
# introduces does not need the cloud taught first). Exactly the three
# classifications the apply path really makes today; adding a fourth
# without first checking where it would actually be raised from is exactly
# the defect this design avoids (see ros_runtime.py's five apply loops and
# client.py's `_apply_or_report`).
APPLY_ERROR_CODE_FIELD_PATH_INVALID = "field_path_invalid"  # FieldPathError (sampling.py)
APPLY_ERROR_CODE_WHOLE_KIND_FAILED = "whole_kind_failed"  # _apply_or_report's catch, slug '*'
APPLY_ERROR_CODE_UNKNOWN = "unknown"  # everything else the per-slug broad catch sees

# Close codes the cloud uses (frozen with the cloud): 4000 supersede,
# 4002 hello timeout, 4003 pong timeout. Only supersede is terminal — it means
# another bridge now owns this robot, and reconnecting would kick that one off
# in turn, leaving two bridges trading the robot back and forth forever.
CLOSE_CODE_SUPERSEDED = 4000

# The cloud closes with this when the robot this connection was authenticated
# as no longer exists — a developer deleted it. Deliberately distinct
# from every other close code: a token that was valid a second ago is
# indistinguishable from one that was revoked unless the cloud says which,
# and without this code the bridge would reconnect forever against a robot
# that will never come back — a permanent load on the cloud, and a robot on
# someone's shelf whose logs say nothing more useful than "connection
# closed". Terminal, the same way CLOSE_CODE_SUPERSEDED is: nothing about
# this process starting over will change the answer.
CLOSE_CODE_ROBOT_DELETED = 4004


class ApplyError(NamedTuple):
    """One thing that did not apply during a config apply (`applyError` in
    the wire contracts). A `NamedTuple`, not a plain dataclass: every one of
    the five apply loops in ros_runtime.py already built a `(slug, message)`
    pair positionally
    (`errors.append((slug, str(exc)))`), and the existing suite reads a
    result back the same way (`errors[0][0]`) — widening the tuple in place
    keeps both call sites and that indexing working, while still giving
    every field a name for the entries that are new here (`kind`, `code`).

    `slug` is the exposure's slug, or `'*'` when `kind`'s whole apply pass
    raised before any individual slug was reached (`client.py`'s
    `_apply_or_report` catch) — a different claim from every other error:
    not "this slug is wrong" but "this kind was not applied at all and its
    slugs are in an unknown state".

    `code` is a bounded string, not an enum (contracts' own reasoning,
    matching `HelloError.code`): a string lets this bridge learn a new
    classification without the cloud being taught first. See the
    `APPLY_ERROR_CODE_*` constants above for the three this bridge produces
    today."""

    slug: str
    kind: str
    code: str
    message: str


@dataclass(frozen=True)
class HelloOk:
    """The cloud accepted the token and bound this connection to a robot.

    The window fields are optional on the wire (an older cloud sends none)
    and `None` here in that case."""

    robot_id: str
    protocol_status: Optional[str] = None
    sunset_at: Optional[str] = None
    latest_bridge_version: Optional[str] = None


@dataclass(frozen=True)
class HelloError:
    """The cloud refused the hello."""

    code: str
    message: str

    @property
    def terminal(self) -> bool:
        return self.code in TERMINAL_HELLO_ERROR_CODES


@dataclass(frozen=True)
class Ping:
    """The cloud's round-trip probe; `ts_ms` is the cloud's clock, not ours."""

    ts_ms: int


@dataclass(frozen=True)
class DatapointNumeric:
    """`numeric` — what to do with a numeric value before it is sent
    (`scale`, `offset`) and how to label it (`unit`). The group also carries
    `decimals`, which is display-only and deliberately not read here: the
    bridge sends the number, the console decides how many digits of it a
    person sees.

    Every member is optional, and an absent `numeric` is the same thing as an
    empty one — which is why `_parse_datapoint_config` produces this rather
    than `None` for a datapoint that has no `numeric` at all."""

    scale: Optional[float] = None
    offset: Optional[float] = None
    unit: Optional[str] = None


@dataclass(frozen=True)
class RetentionConfig:
    """`retention`, replacing the `buffer`.

    Only `max_buffer_values` is a robot-side concept — it is the depth of the
    disconnect buffer the bridge already keeps (`BacklogStore`). `enabled` and
    `interval_seconds` are the cloud's decision about writing to history:
    `interval_seconds` is parsed here so the shape is read whole, and is
    **never read anywhere**; its own contracts doc comment says it is "how
    often a value is written to history — not how often it is sent".

    `enabled` absent means off, which contracts states outright and gives the
    reason for: stored points are billed, so a default that turned history on
    would start charging for a value nobody asked to keep.

    `max_buffer_values` absent stays `None` rather than collapsing to `0`.
    Zero is a real depth ("buffer nothing") in `BacklogStore`, and absent is
    not that answer — it is no answer, and the caller that needs one picks it."""

    enabled: bool
    interval_seconds: Optional[int] = None
    max_buffer_values: Optional[int] = None


@dataclass(frozen=True)
class DatapointConfig:
    """One entry of a published config's `doc.datapoints`.

    `slug` is the mapping key `doc.datapoints` was read under, carried onto
    the entry so the five kinds keep one addressing story end to end.

    `rate_throttle_hz` is a plain ceiling, `None` meaning no throttling —
    which is also what a wire `0` means. The parser turns the one into the
    other, because `0` is in range on the wire and `MaxHzPolicy` divides by
    it."""

    slug: str
    topic: str
    type: str
    field: Optional[str]
    rate_throttle_hz: Optional[float]
    numeric: DatapointNumeric
    retention: RetentionConfig


@dataclass(frozen=True)
class ParameterSpec:
    """One declared parameter of an action, service or publisher.

    A parameter is addressed by the short name it is keyed under in
    `parameters`, and that name is a free-standing identifier — not a path
    into the message. Where its value lands is decided by where the developer
    wrote `${name}` in the entry's `message` template.

    **A parameter is required exactly when it has no `default`.** There is no
    `required` field, here or on the wire: two spellings of one fact are two
    things to keep in agreement.

    `min_value`, `max_value`, `enum` and `regex` are enforced in the **cloud**,
    before anything reaches the robot. They are carried so the bridge holds
    the whole declaration, not so it can re-check them — a second enforcement
    point would be a second policy for one decision, and the later one always
    sees less."""

    type: str
    default: Optional[Union[str, float, bool]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    enum: Optional[Tuple[Union[str, float], ...]] = None
    regex: Optional[str] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class ActionConfig:
    """One entry of a published config's `doc.actions`.

    `message` is the Goal, written out in full, with `${name}` at the
    positions a caller may fill. **An absent `message` is an empty Goal**, not
    an error and not "send nothing": a Goal with no fields is the ordinary
    case for a great many actions, and the format's own reference example is
    exactly that. The parser normalises it to `{}` so nothing downstream has
    to remember which of two spellings it is looking at."""

    slug: str
    ros_name: str
    type: str
    message: Any
    parameters: Dict[str, ParameterSpec]


@dataclass(frozen=True)
class ServiceConfig:
    """One entry of a published config's `doc.services`. Same
    shape as `ActionConfig`, `message` included — `std_srvs/srv/Trigger` has
    no request fields at all, so an absent `message` (an empty Request) is
    the common case here rather than an edge one."""

    slug: str
    ros_name: str
    type: str
    message: Any
    parameters: Dict[str, ParameterSpec]


@dataclass(frozen=True)
class FailsafeConfig:
    """A publisher's failsafe: what to send, and how long a silence
    it takes to send it. `timeout_ms` used to be a sibling of `failsafe` on
    the publisher; it is inside it now, where the two halves of one decision
    sit together.

    `message` must resolve with no placeholder left in it — there is no
    caller to fill one, by construction.

    **The bridge is the only place that can check this in full.** The
    contracts refuse a placeholder in an *inline* failsafe body and says
    plainly that it cannot answer the case where `message` is a `${name}`
    reference into the shared `messages:` map —
    a schema over one publisher cannot see that map. The cloud's own
    whole-document validation is the other half; this side re-derives it after
    dereferencing, which is the only point at which the question is fully
    answerable. See `params.resolve_failsafe_body`.

    Getting this backwards is the dangerous direction: a comment saying the
    cloud already handles it is an invitation to delete the check that
    actually does."""

    timeout_ms: int
    message: Any


@dataclass(frozen=True)
class PublisherConfig:
    """One entry of a published config's `doc.publishers`.

    `message` is required here, unlike on an action or a service: a publisher
    with nothing to send is not a publisher.

    `quiet_timeout_ms` stays a sibling of `failsafe` rather than moving inside
    it — it answers a different question (how long another publisher's silence
    means this one may take the topic over), and grouping it with the failsafe
    timeout would suggest the two are variants of one setting."""

    slug: str
    topic: str
    type: str
    message: Any
    parameters: Dict[str, ParameterSpec]
    failsafe: FailsafeConfig
    quiet_timeout_ms: int


@dataclass(frozen=True)
class RosSource:
    """`kind: 'ros'` — a ROS image topic. `type` is
    `sensor_msgs/msg/Image` or `sensor_msgs/msg/CompressedImage`; the bridge
    subscribes to `topic` and converts either shape to the same BGR array
    (camera.py)."""

    topic: str
    type: str


@dataclass(frozen=True)
class RtspSource:
    """`kind: 'rtsp'`. `transport` is `'tcp'` or `'udp'`,
    defaulted to `'tcp'` by the cloud (contracts) but parsed leniently here
    too — same reasoning `_parse_retention` already gives for a defaulted
    field.

    `credentials` is the username/password pair as the developer
    wrote it **inside this source**, replacing the `credentials_ref` into a
    frame-level map that no longer exists. `None` means none was written,
    which still permits userinfo in `url` itself
    (camera_sources.resolve_credentials)."""

    url: str
    transport: str
    credentials: Optional[Tuple[str, str]]


@dataclass(frozen=True)
class MjpegSource:
    """`kind: 'mjpeg'` — a `multipart/x-mixed-replace` HTTP
    stream, optionally behind HTTP Basic auth (`credentials`, same shape and
    precedence as `RtspSource`)."""

    url: str
    credentials: Optional[Tuple[str, str]]


@dataclass(frozen=True)
class V4l2Source:
    """`kind: 'v4l2'` — a local capture device (e.g.
    `/dev/video0`). No network, no credentials."""

    device: str


CameraSource = Union[RosSource, RtspSource, MjpegSource, V4l2Source]


@dataclass(frozen=True)
class CameraConfig:
    """One entry of a published config's `doc.cameras`.
    `source` replaced `topic`/`type` — a breaking change, deliberately
    not migrated: no 2.x document stays parseable.
    `width`/`height`/`fps`/`bitrate_kbps` are the developer's control over
    the robot's own bandwidth — the bridge carries them, it does not
    choose them.

    **`snapshot_interval_seconds` is seconds, and used to be milliseconds.**
    The wire field was renamed *and* re-scaled in 3.0 ("same range,
    expressed in the unit a person uses"), so the same number now means
    something a thousand times different. It is held here in the unit it
    arrives in; whatever converts to milliseconds does so where it needs
    them, in sight of the constant that says so."""

    slug: str
    source: CameraSource
    width: int
    height: int
    fps: int
    bitrate_kbps: int
    snapshot_interval_seconds: int


@dataclass(frozen=True)
class Config:
    """The published configuration to apply. `version: 0` with nothing in it
    means nothing has been published for this robot yet.

    Every section is a **mapping keyed by slug**, not a list of entries
    carrying their own slug: a duplicate name is then a YAML syntax
    error rather than a rule somebody has to write. Each section defaults to
    empty — a document that configures no cameras omits `cameras` entirely
    rather than writing an empty one.

    `messages` holds the shared message templates a `message: ${name}` refers
    to, as opaque trees. They are validated structurally when they are
    resolved against a real ROS type, not here; this parser only checks that
    each is keyed by a slug and is not `null`.

    There is **no `credentials` member**. The wire-only map beside `doc` is
    retired: a camera's username and password live inside its own source
    (`RtspSource`/`MjpegSource`), so there is no second place for them to
    disagree with."""

    version: int
    datapoints: Dict[str, DatapointConfig] = field(default_factory=dict)
    actions: Dict[str, ActionConfig] = field(default_factory=dict)
    services: Dict[str, ServiceConfig] = field(default_factory=dict)
    publishers: Dict[str, PublisherConfig] = field(default_factory=dict)
    cameras: Dict[str, CameraConfig] = field(default_factory=dict)
    messages: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IntrospectRequest:
    """Asks for a fresh ROS graph snapshot."""

    request_id: str


@dataclass(frozen=True)
class TypeRequest:
    """Asks for the field trees of the given message type names."""

    request_id: str
    type_names: Tuple[str, ...]


@dataclass(frozen=True)
class CloudInvoke:
    """Run an action or a service call. The cloud minted
    `job_id` before sending this, so a job exists — and can eventually be
    reported `lost` — even if this frame itself never reaches us. `params` is
    flat, keyed by each declared parameter's **short name** (`{"speed":
    0.5}`); the values are substituted into the entry's `message` template at
    the `${name}` positions the developer wrote, on the way into ROS (see
    `templates.py` and `params.py`).

    `patience_ms` is how long this one call is worth waiting for,
    already resolved by the cloud — required here, unlike at REST, because
    by the time the frame is on this socket somebody has decided (contracts'
    own doc comment). Used for goal acceptance in place of the constant
    `GOAL_ACCEPT_TIMEOUT_S` (ros_runtime.py) used to be — see
    `RosRuntime._invoke_action`."""

    job_id: str
    slug: str
    params: Dict[str, Any]
    patience_ms: int


@dataclass(frozen=True)
class CloudCancel:
    """Cancel — a real ROS goal cancel, not a local forget.

    `job_id` says *which* job: `None` means today's behaviour, cancel
    whatever is running on `slug` (a real request — an operator hitting
    stop). A non-`None` id that does not match what is currently running on
    `slug` must cancel **nothing** and must not fall back to the slug — a
    caller who named an id has ruled out "whatever is running" as the
    answer (see `RosRuntime._cancel_job`)."""

    slug: str
    job_id: Optional[str]


@dataclass(frozen=True)
class CloudPublish:
    """One publish to a configured publisher. `message` is flat and keyed by
    each declared parameter's short name, exactly like `CloudInvoke.params`
    — *not* the nested message body `PublisherConfig.message` carries. Its
    name is a wire name and predates the template format; what arrives here
    is a bag of parameter values, not a message."""

    slug: str
    message: Dict[str, Any]


@dataclass(frozen=True)
class CloudCameraStart:
    """Start publishing this camera live. The **cloud** mints
    `room`/`token`, for the same reason it mints a `job_id` before asking
    anything: the side that owns the refcount must own the identity
    of the stream, or the robot could end up publishing into a room nobody
    is watching.

    `request_id` names this attempt, and must be echoed on the
    `camera_state` that answers it — see `bridge_camera_state_message`."""

    slug: str
    url: str
    room: str
    token: str
    request_id: str


@dataclass(frozen=True)
class CloudCameraStop:
    """The last live viewer left; stop publishing (refcount).

    `request_id`, same meaning as `CloudCameraStart.request_id`."""

    slug: str
    request_id: str


@dataclass(frozen=True)
class CloudAssetRequest:
    """The explicit request that starts a sync. `bridgeAssetsAvailable` only
    ever reports what exists; nothing moves until this arrives.

    `upload_url`/`token` are minted per sync, not the robot's own hello
    credential — scoped to one robot's assets and one sync, so an expired
    or misused one fails a sync rather than the robot's whole connection
    (contracts' own reasoning).

    `meshes` names which `package://` URIs to send — not necessarily every
    one `bridgeAssetsAvailable` last reported (the cloud may already hold
    some, content-addressed). Empty means the URDF only."""

    sync_id: str
    upload_url: str
    token: str
    meshes: Tuple[str, ...]


@dataclass(frozen=True)
class Unknown:
    """Anything this version cannot act on, including malformed input."""

    raw: str
    reason: str


CloudMessage = Union[
    HelloOk,
    HelloError,
    Ping,
    Config,
    IntrospectRequest,
    TypeRequest,
    CloudInvoke,
    CloudCancel,
    CloudPublish,
    CloudCameraStart,
    CloudCameraStop,
    Unknown,
]


def hello_message(
    token: str, bridge_version: str, active_jobs: Sequence[Tuple[str, str, str]] = ()
) -> str:
    """The opening frame: it identifies the robot and the protocol we speak.

    `active_jobs` is every job this process still has in memory (the format,
), as `(job_id, slug, state)` triples (renamed from
    `active_job_ids`, which named only the id) — empty on a fresh process,
    which is exactly what tells the cloud a job it believes running here was
    actually lost. A reconnect of the *same* process lists its live jobs
    instead, so nothing is lost that is not really gone, and `state` is
    this process's own current answer for each — including a terminal one
    it has not yet managed to deliver, so the cloud writes down `succeeded`
    instead of guessing `lost`. See `jobs.py` for where the list comes
    from (`JobManager.active_jobs`)."""
    return json.dumps(
        {
            "type": "hello",
            "protocol_version": PROTOCOL_VERSION,
            "token": token,
            "bridge_version": bridge_version,
            "active_jobs": [
                {"job_id": job_id, "slug": slug, "state": state}
                for job_id, slug, state in active_jobs
            ],
        }
    )


def pong_message(ts_ms: int) -> str:
    """Echo a ping's timestamp untouched — the cloud derives latency from it."""
    return json.dumps({"type": "pong", "ts_ms": ts_ms})


def config_applied_message(
    version: int, ok: bool, errors: Sequence[ApplyError]
) -> str:
    """Ack a `config` frame. `errors` is a sequence of `ApplyError` — `slug`,
    `kind`, `code`, `message` (contracts `applyError`). A non-empty `errors`
    implies `ok=False`, but the caller states `ok` explicitly rather than
    have this function infer it, since an empty apply (version 0) is also
    `ok=True` with no errors."""
    return json.dumps(
        {
            "type": "config_applied",
            "version": version,
            "ok": ok,
            "errors": [
                {"slug": e.slug, "kind": e.kind, "code": e.code, "message": e.message}
                for e in errors
            ],
        }
    )


def introspect_message(request_id: str, graph: dict) -> str:
    """Answer an `introspect_request`. `graph` already matches the `ros graph`
    shape (topics/services/actions/captured_at_ms) — see introspection.py."""
    return json.dumps({"type": "introspect", "request_id": request_id, "graph": graph})


def type_definitions_message(
    request_id: str, definitions: Sequence[dict], unresolved: Sequence[str]
) -> str:
    """Answer a `type_request`. `definitions` already match the `type
    definition` shape (name/kind/fields) — see introspection.py. A type name
    that could not be resolved is listed in `unresolved` instead of raising."""
    return json.dumps(
        {
            "type": "type_definitions",
            "request_id": request_id,
            "definitions": list(definitions),
            "unresolved": list(unresolved),
        }
    )


def datapoint_message(slug: str, value: Any, timestamp_ms: int) -> str:
    """One sample. `timestamp_ms` is the bridge capture time."""
    return json.dumps(
        {"type": "datapoint", "slug": slug, "value": value, "timestamp_ms": timestamp_ms}
    )


def job_update_message(
    job_id: str,
    slug: str,
    state: str,
    *,
    timestamp_ms: int,
    feedback: Any = None,
    progress: Optional[float] = None,
    result: Any = None,
    error: Optional[Tuple[str, str]] = None,
    details: Any = None,
) -> str:
    """One update about a job. `timestamp_ms` is the bridge
    capture time, exactly like a datapoint — so feedback delivered late after
    a reconnect is visibly late rather than looking current. `error` is
    `(code, message)` or `None`; every field the schema requires is always
    present, `null` where there is nothing to say.

    `details` is the structured payload for an error code that has a documented
    one (`job_queue_full`'s `{limit, queued}`, today's only example) — nested
    inside `error`, not a
    sibling field, matching `bridgeJobUpdate.error.details`. Omitted from
    the object entirely when `None` (it is `.optional()` on the schema, not
    `.nullable()`: most job errors have nothing structured to add, and
    `"details": null` would claim there was something to say and it was
    nothing, which is not the same claim as never having the field).
    Ignored when `error` is `None` — details about no error is not a shape
    that means anything."""
    error_obj = None
    if error is not None:
        error_obj = {"code": error[0], "message": error[1]}
        if details is not None:
            error_obj["details"] = details
    return json.dumps(
        {
            "type": "job_update",
            "job_id": job_id,
            "slug": slug,
            "state": state,
            "feedback": feedback,
            "progress": progress,
            "result": result,
            "error": error_obj,
            "timestamp_ms": timestamp_ms,
        }
    )


def job_lost_message(job_ids: Sequence[str]) -> str:
    """Jobs a *connected* bridge can no longer account for — a
    restarted process says the same thing via `hello.active_job_ids` instead;
    see jobs.py for when this one actually gets used."""
    return json.dumps({"type": "job_lost", "job_ids": list(job_ids)})


def snapshot_frame(
    *, slug: str, mime: str, width: int, height: int, timestamp_ms: int, image_bytes: bytes
) -> bytes:
    """The one **binary** frame the bridge sends:

        [4-byte big-endian header length][UTF-8 JSON header][image bytes]

    Binary rather than base64 in a text frame, because base64 would cost a
    third of the robot's upstream for nothing. Self-contained — the header
    carries everything a reader needs, rather than depending on a preceding
    or following text frame for slug/mime/dimensions — because
    established, at some cost, that frame ordering across this socket is
    not something to lean on (see client.py's frame-ordering fix). Matches
    contracts' `snapshotHeader` field-for-field; `timestamp_ms` is the
    bridge's *capture* time, carried through from
    `camera.LatestFrame` untouched, never re-stamped at encode time."""
    header = json.dumps(
        {
            "type": "snapshot",
            "slug": slug,
            "mime": mime,
            "width": width,
            "height": height,
            "timestamp_ms": timestamp_ms,
        }
    ).encode("utf-8")
    return struct.pack(">I", len(header)) + header + image_bytes


def bridge_camera_state_message(
    slug: str,
    publishing: bool,
    error: Optional[Tuple[str, str]] = None,
    *,
    cause: str,
    observed_at_ms: int,
    request_id: Optional[str],
) -> str:
    """What the bridge made of a `camera_start`/`camera_stop` (the format,
), or an unsolicited report of a background change. `publishing:
    false` with an `error` is how a camera that cannot start says so — the
    cloud must not leave a viewer watching a black rectangle while believing
    the stream is live. `error` is `(code, message)` or `None`, the same
    shape `job_update_message`'s uses.

    `cause` is required, not defaulted (contracts' own reasoning, see
    protocol.py's `CAMERA_STATE_CAUSE_*` constants): every caller of this
    function already knows why it is sending a frame, and a default would
    just be a guess standing in for that.

    `observed_at_ms`: when the **robot** observed
    this state — bridge capture time, never send time, same discipline
    `timestamp_ms` already holds for datapoint samples. Exists
    because the cloud used to stamp its own `Date.now()` on receipt, and a
    *restatement* (`RosRuntime.report_current_camera_health`) is by
    definition an old state re-sent into a store that just lost it — so a
    failure from yesterday, restated after a reconnect, dated itself to the
    moment of the restart. Every caller must pass the time the state was
    *first* true, not the time this particular frame happens to be sent.

    `request_id`: which `camera_start`/`camera_stop` this frame
    answers, or `None` when it answers none. Required-and-nullable, not
    defaulted, for the same reason `cause` is not: a `cause: 'command'`
    frame must carry the id of the command it answers, and every other
    `cause` answers no request by definition and must say so explicitly
    with `None` rather than an absent field standing in for it. **The
    pairing rule itself (non-`None` iff `cause == 'command'`) is enforced
    by the cloud, not by this function or the JSON Schema** — see this
    module's own note by `CAMERA_STATE_CAUSE_COMMAND` and contracts'
    `bridgeCameraState.request_id` doc comment."""
    return json.dumps(
        {
            "type": "camera_state",
            "slug": slug,
            "publishing": publishing,
            "error": None if error is None else {"code": error[0], "message": error[1]},
            "cause": cause,
            "observed_at_ms": observed_at_ms,
            "request_id": request_id,
        }
    )


def bridge_assets_available_message(urdf: bool, meshes: Sequence[str]) -> str:
    """What the bridge found on `DEFAULT_URDF_TOPIC` (ros_runtime.py's
    `AssetsAvailable`) — availability only, never bytes.
    `meshes` is every `package://` URI the URDF references, verbatim and
    unresolved, including ones this bridge cannot find in its own
    workspace: reporting only the resolvable ones would make an incomplete
    workspace look like a complete robot."""
    return json.dumps({"type": "assets_available", "urdf": urdf, "meshes": list(meshes)})


def bridge_asset_progress_message(
    sync_id: str,
    done: int,
    total: int,
    failed: Sequence[Tuple[str, str, Optional[Dict[str, int]]]],
    state: str,
) -> str:
    """How far a sync got, and — required, not optional — what it could not
    do. `failed` carries `(reference, kind, details)` triples, not bare
    strings and not bare pairs: a sync that quietly drops three meshes and
    reports success moves the failure into somebody else's renderer, where it
    shows up as a robot with missing limbs and no cause — and *why* it wasn't
    delivered matters
    as much as *that* it wasn't, because only one of the four `kind`s
    (`ASSET_FAILURE_KIND_*` above) may ever be treated as "this reference is
    gone for good" by a reconciliation.

    `details`: `{"limit_bytes":..., "size_bytes":...}` on
    `too_large`, `None` on every other kind — and always sent as the literal
    JSON key with value `null` in that case, never omitted. The contract's
    `superRefine` (`assets.ts`) checks `details !== null` on the parsed
    value, not "was the key present" — a JSON `undefined` has no wire
    representation, so leaving the key out on a non-`too_large` entry is not
    the same claim as sending `null`, and only one of the two is guaranteed
    to satisfy the pairing rule on the receiving end.

    `state` is `'running' | 'finished' | 'refused_busy'` — three values
    because a plain `finished: bool` had nowhere to put a refusal. A second
    `asset_request` arriving while one is already in flight needs an
    honest answer; the alternative (reporting every requested URI in
    `failed`) would make that field mean both "did not resolve" and "was
    never attempted" at once. The cloud owns single-flight and refuses a
    concurrent sync first (contracts) — this bridge-side guard
    (`RosRuntime.sync_assets`) is the backstop, for the same request that
    slips past it anyway."""
    return json.dumps(
        {
            "type": "asset_progress",
            "sync_id": sync_id,
            "done": done,
            "total": total,
            "failed": [
                {"reference": reference, "kind": kind, "details": details}
                for reference, kind, details in failed
            ],
            "state": state,
        }
    )


_INVALID = object()

# One namespace, one grammar: every slug in the document — the five exposure
# kinds, a shared message, a parameter, an alert — is matched against this.
# 3.0 changed the separator from `-` to `_`, so a name that was legal in
# 2.x is not legal now; there is no migration.
_SLUG_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")
_SLUG_MIN_LENGTH = 2
_SLUG_MAX_LENGTH = 63


def _is_slug(value: object) -> bool:
    return (
        isinstance(value, str)
        and _SLUG_MIN_LENGTH <= len(value) <= _SLUG_MAX_LENGTH
        and _SLUG_RE.match(value) is not None
    )


def _is_number(value: object) -> bool:
    """A JSON number, which `True` is not — `isinstance(True, int)` is the
    trap every numeric check in this module has to step around."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_message_body(payload: dict, key: str) -> Any:
    """A `message:` position. Returns the template, `{}` when the key is
    absent, or the `_INVALID` sentinel — a template can be any JSON value at
    all, so there is no in-band value left to mean "malformed".

    An absent message is an **empty** Goal, Request or body — the right and
    common reading, not an error and not "send nothing": `std_srvs/srv/
    Trigger` has no request fields at all. Normalised to `{}` here so no
    caller downstream has to hold two spellings of one meaning.

    An explicit `null` is refused. Contracts refuses it at every depth of this
    position for exactly that reason — omission is the format's only spelling
    of "not set" — and a `message: null` reaching this parser is a cloud that
    has stopped agreeing with its own contract, not a document to salvage.
    (The refusal is a zod refinement and therefore invisible in the JSON
    Schema artifact, so validating a frame against the vendored copy does not
    catch it; this does.)"""
    if key not in payload:
        return {}
    body = payload[key]
    if body is None:
        return _INVALID
    return body


def _parse_numeric(payload: object) -> Optional[DatapointNumeric]:
    """Absent means an empty group, not a missing one — every member is
    optional, so `numeric: {}` and no `numeric` at all say the same thing.
    `decimals` is read by nobody here (display only) and so is not parsed."""
    if payload is None:
        return DatapointNumeric()
    if not isinstance(payload, dict):
        return None
    scale, offset = payload.get("scale"), payload.get("offset")
    for number in (scale, offset):
        if number is not None and not _is_number(number):
            return None
    unit = payload.get("unit")
    if unit is not None and not isinstance(unit, str):
        return None
    return DatapointNumeric(scale=scale, offset=offset, unit=unit)


def _parse_retention(payload: object) -> Optional[RetentionConfig]:
    """Missing or explicit `null` both mean *not retained and not buffered* —
    contracts says `enabled` absent means off and says why (stored points are
    billed), so a document that omits the group entirely gets the same answer
    as one that writes `enabled: false`."""
    if payload is None:
        return RetentionConfig(enabled=False)
    if not isinstance(payload, dict):
        return None
    enabled = payload.get("enabled")
    if enabled is None:
        enabled = False
    if not isinstance(enabled, bool):
        return None
    interval_seconds = payload.get("interval_seconds")
    if interval_seconds is not None and not (_is_int(interval_seconds) and interval_seconds > 0):
        return None
    max_buffer_values = payload.get("max_buffer_values")
    if max_buffer_values is not None and not (
        _is_int(max_buffer_values) and max_buffer_values > 0
    ):
        return None
    return RetentionConfig(
        enabled=enabled,
        interval_seconds=interval_seconds,
        max_buffer_values=max_buffer_values,
    )


def _parse_rate_throttle_hz(payload: object) -> Union[Optional[float], object]:
    """The throttle ceiling, or `None` for no throttling. Returns `_INVALID`
    for anything that is not a non-negative number.

    **`0` is a legal value and means no throttling**, so it is turned into
    `None` right here. The old `rate: {mode, hz}` union could not express a
    zero at all and got this outcome by accident; the new field can, and
    `MaxHzPolicy` divides by whatever it is handed."""
    if payload is None:
        return None
    if not _is_number(payload) or payload < 0:
        return _INVALID
    if payload == 0:
        return None
    return float(payload)


def _parse_datapoint_config(slug: str, payload: object) -> Optional[DatapointConfig]:
    if not isinstance(payload, dict):
        return None
    topic, type_name, field_path = (
        payload.get("topic"),
        payload.get("type"),
        payload.get("field"),
    )
    if not (isinstance(topic, str) and topic):
        return None
    if not (isinstance(type_name, str) and type_name):
        return None
    if field_path is not None and not isinstance(field_path, str):
        return None

    rate_throttle_hz = _parse_rate_throttle_hz(payload.get("rate_throttle_hz"))
    if rate_throttle_hz is _INVALID:
        return None

    numeric = _parse_numeric(payload.get("numeric"))
    if numeric is None:
        return None

    retention = _parse_retention(payload.get("retention"))
    if retention is None:
        return None

    return DatapointConfig(
        slug=slug,
        topic=topic,
        type=type_name,
        field=field_path,
        rate_throttle_hz=rate_throttle_hz,
        numeric=numeric,
        retention=retention,
    )


def _parse_parameter_spec(payload: object) -> Optional[ParameterSpec]:
    """The parameter's name is the key it was found under, not a field of
    this object, so it is not read here. Nothing in this function enforces a
    bound: `min_value`/`max_value`/`enum`/`regex` are the cloud's to enforce
    before a value ever reaches the robot (see `ParameterSpec`)."""
    if not isinstance(payload, dict):
        return None
    type_name = payload.get("type")
    if not (isinstance(type_name, str) and type_name):
        return None

    default = payload.get("default")
    if default is not None and not (isinstance(default, (str, bool)) or _is_number(default)):
        return None

    min_value, max_value = payload.get("min_value"), payload.get("max_value")
    for number in (min_value, max_value):
        if number is not None and not _is_number(number):
            return None

    enum = payload.get("enum")
    if enum is not None:
        if not (
            isinstance(enum, list)
            and enum
            and all(isinstance(item, str) or _is_number(item) for item in enum)
        ):
            return None
        enum = tuple(enum)

    regex = payload.get("regex")
    if regex is not None and not isinstance(regex, str):
        return None

    description = payload.get("description")
    if description is not None and not isinstance(description, str):
        return None

    return ParameterSpec(
        type=type_name,
        default=default,
        min_value=min_value,
        max_value=max_value,
        enum=enum,
        regex=regex,
        description=description,
    )


def _parse_parameters(payload: object) -> Optional[Dict[str, ParameterSpec]]:
    """`parameters` is a mapping keyed by the parameter's short name; absent
    means no parameters at all, which is not the same shape of statement as
    an empty mapping but is the same fact."""
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        return None
    parsed: Dict[str, ParameterSpec] = {}
    for name, entry in payload.items():
        if not _is_slug(name):
            return None
        spec = _parse_parameter_spec(entry)
        if spec is None:
            return None
        parsed[name] = spec
    return parsed


def _parse_callable_shape(
    payload: object, *, location_key: str
) -> Optional[Tuple[str, str, Any, Dict[str, ParameterSpec]]]:
    """The shape actions, services and publishers all share: a ROS-side
    location (`ros_name` for actions/services, `topic` for publishers),
    `type`, a `message` template and `parameters`. Returns
    `(location, type, message, parameters)` or `None`. The slug is not among
    them: it is the mapping key the entry was found under, and each caller
    already has it."""
    if not isinstance(payload, dict):
        return None
    location, type_name = payload.get(location_key), payload.get("type")
    if not (isinstance(location, str) and location):
        return None
    if not (isinstance(type_name, str) and type_name):
        return None
    message = _parse_message_body(payload, "message")
    if message is _INVALID:
        return None
    parameters = _parse_parameters(payload.get("parameters"))
    if parameters is None:
        return None
    return location, type_name, message, parameters


def _parse_action_config(slug: str, payload: object) -> Optional[ActionConfig]:
    shape = _parse_callable_shape(payload, location_key="ros_name")
    if shape is None:
        return None
    ros_name, type_name, message, parameters = shape
    return ActionConfig(
        slug=slug,
        ros_name=ros_name,
        type=type_name,
        message=message,
        parameters=parameters,
    )


def _parse_service_config(slug: str, payload: object) -> Optional[ServiceConfig]:
    shape = _parse_callable_shape(payload, location_key="ros_name")
    if shape is None:
        return None
    ros_name, type_name, message, parameters = shape
    return ServiceConfig(
        slug=slug,
        ros_name=ros_name,
        type=type_name,
        message=message,
        parameters=parameters,
    )


def _parse_failsafe(payload: object) -> Optional[FailsafeConfig]:
    """`{timeout_ms, message}` — both required. `timeout_ms` moved inside
    this group in 3.0; an old frame carrying it as a sibling of `failsafe`
    fails here, which is the intended outcome for a format change with no
    migration."""
    if not isinstance(payload, dict):
        return None
    timeout_ms = payload.get("timeout_ms")
    if not (_is_int(timeout_ms) and timeout_ms > 0):
        return None
    if "message" not in payload:
        return None
    message = _parse_message_body(payload, "message")
    if message is _INVALID:
        return None
    return FailsafeConfig(timeout_ms=timeout_ms, message=message)


def _parse_publisher_config(slug: str, payload: object) -> Optional[PublisherConfig]:
    shape = _parse_callable_shape(payload, location_key="topic")
    if shape is None:
        return None
    topic, type_name, message, parameters = shape
    assert isinstance(payload, dict)  # guaranteed by a successful _parse_callable_shape

    # Required on a publisher, unlike on an action or a service: a publisher
    # with nothing to send is not a publisher. `_parse_callable_shape` reads
    # an absent one as an empty body, so the presence check belongs here.
    if "message" not in payload:
        return None

    failsafe = _parse_failsafe(payload.get("failsafe"))
    if failsafe is None:
        return None

    quiet_timeout_ms = payload.get("quiet_timeout_ms")
    if not (_is_int(quiet_timeout_ms) and quiet_timeout_ms >= 0):
        return None

    return PublisherConfig(
        slug=slug,
        topic=topic,
        type=type_name,
        message=message,
        parameters=parameters,
        failsafe=failsafe,
        quiet_timeout_ms=quiet_timeout_ms,
    )


def _parse_positive_int(payload: object) -> Optional[int]:
    if _is_int(payload) and payload > 0:
        return payload
    return None


def _parse_camera_credentials(payload: object) -> Union[Optional[Tuple[str, str]], object]:
    """A camera's own username/password, inside its source.

    Both members are optional in the contract, so `{username: 'a'}` with no
    password is a legal thing to write; the missing half becomes `''` rather
    than a parse failure, because refusing it here would take the robot off
    the air over a credential the adapter is better placed to fail on. Absent
    or `null` means none was written, which still permits userinfo in the URL.

    Returns the pair, `None`, or `_INVALID` — `None` is a real answer here,
    so it cannot double as the failure signal."""
    if payload is None:
        return None
    if not isinstance(payload, dict):
        return _INVALID
    username, password = payload.get("username"), payload.get("password")
    for half in (username, password):
        if half is not None and not isinstance(half, str):
            return _INVALID
    if username is None and password is None:
        return None
    return (username or "", password or "")


def _parse_rtsp_transport(payload: object) -> Union[str, object]:
    """`transport` defaults to `'tcp'` in the contract (`.default('tcp')`)
    but published as `required` regardless, until `io: 'input'` was fixed
    across the board. A real cloud fills the default before sending anyway, so
    this only ever mattered for a hand-built or pre-fix frame; parsed leniently
    either way, same spirit
    as `_parse_retention`."""
    if payload is None:
        return "tcp"
    if payload in ("tcp", "udp"):
        return payload
    return _INVALID


def _parse_camera_source(payload: object) -> Optional[CameraSource]:
    """Structural only, like every other config parser here — the cloud
    enforces the actual bounds (URL length, device path length) before this
    ever reaches the bridge. The wire shape must make an impossible camera
    unrepresentable: each `kind` parses into its own
    dataclass, so nothing downstream can ever hold e.g. an RTSP source with
    a ROS topic."""
    if not isinstance(payload, dict):
        return None
    kind = payload.get("kind")

    if kind == "ros":
        topic, type_name = payload.get("topic"), payload.get("type")
        if not (isinstance(topic, str) and topic):
            return None
        if not (isinstance(type_name, str) and type_name):
            return None
        return RosSource(topic=topic, type=type_name)

    if kind == "rtsp":
        url = payload.get("url")
        if not (isinstance(url, str) and url):
            return None
        transport = _parse_rtsp_transport(payload.get("transport"))
        if transport is _INVALID:
            return None
        credentials = _parse_camera_credentials(payload.get("credentials"))
        if credentials is _INVALID:
            return None
        return RtspSource(url=url, transport=transport, credentials=credentials)

    if kind == "mjpeg":
        url = payload.get("url")
        if not (isinstance(url, str) and url):
            return None
        credentials = _parse_camera_credentials(payload.get("credentials"))
        if credentials is _INVALID:
            return None
        return MjpegSource(url=url, credentials=credentials)

    if kind == "v4l2":
        device = payload.get("device")
        if not (isinstance(device, str) and device):
            return None
        return V4l2Source(device=device)

    return None


def _parse_camera_config(slug: str, payload: object) -> Optional[CameraConfig]:
    """Structural only, like every other config parser here — the cloud
    enforces the actual bounds (width/height/fps/bitrate ranges, a
    one-second snapshot-interval floor) before this ever reaches the bridge;
    a well-formed but out-of-range value is not this function's problem to
    catch."""
    if not isinstance(payload, dict):
        return None

    source = _parse_camera_source(payload.get("source"))
    if source is None:
        return None

    width = _parse_positive_int(payload.get("width"))
    height = _parse_positive_int(payload.get("height"))
    fps = _parse_positive_int(payload.get("fps"))
    bitrate_kbps = _parse_positive_int(payload.get("bitrate_kbps"))
    snapshot_interval_seconds = _parse_positive_int(payload.get("snapshot_interval_seconds"))
    if None in (width, height, fps, bitrate_kbps, snapshot_interval_seconds):
        return None

    return CameraConfig(
        slug=slug,
        source=source,
        width=width,
        height=height,
        fps=fps,
        bitrate_kbps=bitrate_kbps,
        snapshot_interval_seconds=snapshot_interval_seconds,
    )


def _parse_messages(payload: object) -> Optional[Dict[str, Any]]:
    """`doc.messages` — shared templates, keyed by name. The bodies are kept
    as opaque trees: what a template means can only be decided against a real
    ROS type, which is `templates.resolve_template`'s job at invoke time, not
    this parser's. Only the two things visible from here are checked — the
    key is a slug, and the body is not `null` (see `_parse_message_body`)."""
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        return None
    parsed: Dict[str, Any] = {}
    for name, body in payload.items():
        if not _is_slug(name) or body is None:
            return None
        parsed[name] = body
    return parsed


def _parse_config_section(
    doc: dict, key: str, parser: Callable[[str, object], Optional[Any]]
) -> Optional[dict]:
    """`doc[key]` is a mapping from slug to entry, and defaults to `{}` when
    absent — a document that configures no cameras omits the section rather
    than writing an empty one (contracts: every section is `.optional()`).

    A key that is not a slug fails the whole frame, like any other malformed
    field. The key is not decoration here: it is the entry's name, the thing a
    role grant, an invoke and a datapoint frame all address it by, so a name
    the rest of the system cannot express is not a document to half-apply."""
    section = doc.get(key)
    if section is None:
        section = {}
    if not isinstance(section, dict):
        return None
    parsed = {}
    for slug, entry in section.items():
        if not _is_slug(slug):
            return None
        result = parser(slug, entry)
        if result is None:
            return None
        parsed[slug] = result
    return parsed


def parse_cloud_message(raw: object) -> CloudMessage:
    """Turn a received frame into a message object; never raises."""
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return Unknown(repr(raw), "frame is not UTF-8")
    if not isinstance(raw, str):
        return Unknown(repr(raw), "frame is not text")

    try:
        payload = json.loads(raw)
    except ValueError:
        return Unknown(raw, "frame is not valid JSON")
    if not isinstance(payload, dict):
        return Unknown(raw, "frame is not a JSON object")

    kind = payload.get("type")
    if kind == "hello_ok":
        robot_id = payload.get("robot_id")
        if not (isinstance(robot_id, str) and robot_id):
            return Unknown(raw, "hello_ok without a usable robot_id")
        # Each field is taken only if it is the type it should be: a window
        # the cloud garbled is worth less than no window, and never worth
        # losing the binding this frame carries.
        protocol = payload.get("protocol") if isinstance(payload.get("protocol"), dict) else {}
        bridge = payload.get("bridge") if isinstance(payload.get("bridge"), dict) else {}
        status = protocol.get("status")
        sunset = protocol.get("sunset_at")
        latest = bridge.get("latest_version")
        return HelloOk(
            robot_id=robot_id,
            protocol_status=status if isinstance(status, str) else None,
            sunset_at=sunset if isinstance(sunset, str) else None,
            latest_bridge_version=latest if isinstance(latest, str) else None,
        )
    if kind == "hello_error":
        code = payload.get("code")
        message = payload.get("message")
        if isinstance(code, str) and code and isinstance(message, str):
            return HelloError(code=code, message=message)
        return Unknown(raw, "hello_error without a usable code and message")
    if kind == "ping":
        ts_ms = payload.get("ts_ms")
        if isinstance(ts_ms, int) and not isinstance(ts_ms, bool) and ts_ms >= 0:
            return Ping(ts_ms=ts_ms)
        return Unknown(raw, "ping without a usable ts_ms")
    if kind == "config":
        version = payload.get("version")
        doc = payload.get("doc")
        if not (isinstance(version, int) and not isinstance(version, bool) and version >= 0):
            return Unknown(raw, "config without a usable version")
        if not isinstance(doc, dict):
            return Unknown(raw, "config without a usable doc")
        # `fleetless` is the format version — the one field the document
        # can't be read without, since every section below is now optional.
        # Unchecked, arbitrary JSON would parse as a valid empty config.
        if doc.get("fleetless") != FLEETLESS_FORMAT_VERSION:
            return Unknown(raw, "config whose doc is not a fleetless format document")

        messages = _parse_messages(doc.get("messages"))
        if messages is None:
            return Unknown(raw, "config with a malformed shared message")

        sections = {}
        for key, parser in (
            ("datapoints", _parse_datapoint_config),
            ("actions", _parse_action_config),
            ("services", _parse_service_config),
            ("publishers", _parse_publisher_config),
            ("cameras", _parse_camera_config),
        ):
            parsed = _parse_config_section(doc, key, parser)
            if parsed is None:
                # One malformed entry taints the whole frame: a cloud that
                # sends this has a bug, and there is no safe partial parse
                # at the wire level — that tolerance belongs to config.py,
                # once the frame is known to be well-formed.
                return Unknown(raw, "config with a malformed {} entry".format(key[:-1]))
            sections[key] = parsed

        return Config(version=version, messages=messages, **sections)
    if kind == "invoke":
        job_id = payload.get("job_id")
        slug = payload.get("slug")
        params = payload.get("params")
        patience_ms = payload.get("patience_ms")
        if not (isinstance(job_id, str) and job_id):
            return Unknown(raw, "invoke without a usable job_id")
        if not (isinstance(slug, str) and slug):
            return Unknown(raw, "invoke without a usable slug")
        if not isinstance(params, dict):
            return Unknown(raw, "invoke without usable params")
        # Required — structural check only (a positive int), same as
        # every other bounded numeric field this parser reads; the cloud
        # enforces the actual ceiling (MAX_PATIENCE_MS).
        if not (isinstance(patience_ms, int) and not isinstance(patience_ms, bool) and patience_ms > 0):
            return Unknown(raw, "invoke without a usable patience_ms")
        return CloudInvoke(job_id=job_id, slug=slug, params=params, patience_ms=patience_ms)
    if kind == "cancel":
        slug = payload.get("slug")
        if not (isinstance(slug, str) and slug):
            return Unknown(raw, "cancel without a usable slug")
        # `job_id` is required-and-nullable: the key must be present —
        # `null` is a real, distinct meaning ("whatever is running"), not
        # the same as the key being absent from an old-shaped frame.
        if "job_id" not in payload:
            return Unknown(raw, "cancel without a usable job_id")
        job_id = payload["job_id"]
        if job_id is not None and not (isinstance(job_id, str) and job_id):
            return Unknown(raw, "cancel without a usable job_id")
        return CloudCancel(slug=slug, job_id=job_id)
    if kind == "publish":
        slug = payload.get("slug")
        message = payload.get("message")
        if not (isinstance(slug, str) and slug):
            return Unknown(raw, "publish without a usable slug")
        if not isinstance(message, dict):
            return Unknown(raw, "publish without a usable message")
        return CloudPublish(slug=slug, message=message)
    if kind == "camera_start":
        slug = payload.get("slug")
        url = payload.get("url")
        room = payload.get("room")
        token = payload.get("token")
        request_id = payload.get("request_id")
        if not (isinstance(slug, str) and slug):
            return Unknown(raw, "camera_start without a usable slug")
        if not (isinstance(url, str) and url):
            return Unknown(raw, "camera_start without a usable url")
        if not (isinstance(room, str) and room):
            return Unknown(raw, "camera_start without a usable room")
        if not (isinstance(token, str) and token):
            return Unknown(raw, "camera_start without a usable token")
        if not (isinstance(request_id, str) and request_id):
            return Unknown(raw, "camera_start without a usable request_id")
        return CloudCameraStart(slug=slug, url=url, room=room, token=token, request_id=request_id)
    if kind == "camera_stop":
        slug = payload.get("slug")
        request_id = payload.get("request_id")
        if not (isinstance(slug, str) and slug):
            return Unknown(raw, "camera_stop without a usable slug")
        if not (isinstance(request_id, str) and request_id):
            return Unknown(raw, "camera_stop without a usable request_id")
        return CloudCameraStop(slug=slug, request_id=request_id)
    if kind == "asset_request":
        sync_id = payload.get("sync_id")
        upload_url = payload.get("upload_url")
        token = payload.get("token")
        meshes = payload.get("meshes")
        if not (isinstance(sync_id, str) and sync_id):
            return Unknown(raw, "asset_request without a usable sync_id")
        if not (isinstance(upload_url, str) and upload_url):
            return Unknown(raw, "asset_request without a usable upload_url")
        if not (isinstance(token, str) and token):
            return Unknown(raw, "asset_request without a usable token")
        if not (isinstance(meshes, list) and all(isinstance(m, str) and m for m in meshes)):
            return Unknown(raw, "asset_request without a usable meshes list")
        return CloudAssetRequest(
            sync_id=sync_id, upload_url=upload_url, token=token, meshes=tuple(meshes)
        )
    if kind == "introspect_request":
        request_id = payload.get("request_id")
        if isinstance(request_id, str) and request_id:
            return IntrospectRequest(request_id=request_id)
        return Unknown(raw, "introspect_request without a usable request_id")
    if kind == "type_request":
        request_id = payload.get("request_id")
        type_names = payload.get("type_names")
        if not (isinstance(request_id, str) and request_id):
            return Unknown(raw, "type_request without a usable request_id")
        if not (
            isinstance(type_names, list)
            and type_names
            and all(isinstance(name, str) and name for name in type_names)
        ):
            return Unknown(raw, "type_request without usable type_names")
        return TypeRequest(request_id=request_id, type_names=tuple(type_names))
    return Unknown(raw, "unsupported message type {!r}".format(kind))
