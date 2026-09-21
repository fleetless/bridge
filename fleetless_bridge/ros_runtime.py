# SPDX-License-Identifier: Apache-2.0
"""rclpy alongside the asyncio client.

One `Node`, spun by a `SingleThreadedExecutor` on a dedicated background
thread. Every rule below exists because of one fact about rclpy: entity
creation/destruction (`create_subscription`/`destroy_subscription`) mutates
plain lists the executor iterates while building its wait set, so touching
them from another thread is a real race, not merely bad style. That was
established against Humble's rclpy 3; whether the newer rclpy on Jazzy and
Lyrical still has it has NOT been measured, and the rule is kept on all
three rather than made conditional — it costs nothing where the race does
not exist, and a per-distribution threading rule would need its own
measurement for every distribution added. The
fix is one rule, applied uniformly: **all node interaction happens on the
executor thread.** A `GuardCondition` wakes the executor the instant work is
queued (no polling delay); the asyncio side submits a callable and blocks —
via `run_in_executor`, so the event loop itself is never blocked — on a
`concurrent.futures.Future` for the result. Graph reads (`get_topic_names_
and_types`) are technically fine off-thread, but go through the same queue
anyway: one rule is easier to keep correct than two.

Samples flow the other way. A subscription callback (already running on the
executor thread) reads the capture timestamp, converts and rate-limits the
sample, then hands it to the asyncio side via `loop.call_soon_threadsafe`
into a `SampleQueue` — bounded, dropping the oldest entry on overflow, a
safety net against a pump that has fallen behind while still connected.

Buffering across a disconnect is a second, deliberately
separate path: `set_connected(False)` (called by client.py when a session
ends) tells the callback to stop feeding `SampleQueue` — which is *live*
telemetry, by definition only meaningful while someone is actually connected
to receive it — and instead route each datapoint to `BacklogStore` if its
config says `buffer.enabled`, or drop it (an honest gap) if not. Both
`SampleQueue` and `BacklogStore` are still written from the executor thread
only, but now also *read* from the asyncio side outside of `_submit` (the
outgoing pump polls both to decide what to send next), so `BacklogStore`
takes the same `Lock` `JobManager` does — `SampleQueue` does not need one
because `asyncio.Queue` is already safe for exactly this producer/consumer
shape.

Actions follow the same executor-thread rule for
*starting* work — sending a goal, issuing a cancel — but their lifecycle
does not end when `_submit` returns. `ActionClient.send_goal_async` and a
goal handle's `get_result_async` return rclpy `Future`s whose callbacks the
executor itself invokes, later, as goal-response/feedback/result frames
arrive from the action server — so the "block on `_submit`" pattern only
covers kicking a goal off; everything after runs as ordinary executor-thread
callbacks, exactly like a subscription's. Those callbacks feed `self.jobs`
(jobs.py) via the same `loop.call_soon_threadsafe` handoff `SampleQueue`
uses, except `JobUpdateQueue` never drops — a job result has no successor
the way a sensor sample does.

The failsafe timer is the one piece of this file that
must keep working with no cloud, no asyncio loop, and no client connected at
all — it is the platform's safety primitive, independent of everything else
here by design. It is deliberately not one rclpy `Timer` per publisher,
reset on every publish: `Timer.reset()` does not reliably resume a
`cancel()`-ed timer across rclpy versions, and juggling per-publisher timer
lifecycles (create on config-apply, destroy on retarget/removal, reset on
publish, all from different call sites) is exactly the kind of thing "one
rule is easier to keep correct than two" argues against. Instead one shared
timer, ticking at `FAILSAFE_CHECK_INTERVAL_S`, scans every publisher and
compares `time.monotonic()` against `last_activity_at` — plain, testable
arithmetic, on the executor thread, entirely untouched by whether a
WebSocket happens to be open right now.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import mimetypes
import os
import pathlib
import posixpath
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    FrozenSet,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import defusedxml.ElementTree as ElementTree
from ament_index_python.packages import PackageNotFoundError, get_package_share_directory
from defusedxml import DefusedXmlException
import rclpy
from action_msgs.msg import GoalStatus
from rcl_interfaces.msg import SetParametersResult
from rclpy.action import ActionClient
from rclpy.duration import Duration
from rclpy.executors import SingleThreadedExecutor
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
from rosidl_runtime_py.utilities import get_action, get_message, get_service
from sensor_msgs.msg import CompressedImage, Image
from std_msgs.msg import String as StringMsg

from fleetless_bridge import camera, camera_sources, introspection, live, params, sampling
from fleetless_bridge.jobs import JobManager, JobUpdate
from fleetless_bridge.link_mode import LowBandwidthSettings
from fleetless_bridge.protocol import (
    LOW_BANDWIDTH_DEFAULTS,
    APPLY_ERROR_CODE_FIELD_PATH_INVALID,
    APPLY_ERROR_CODE_UNKNOWN,
    APPLY_ERROR_KIND_ACTION,
    APPLY_ERROR_KIND_CAMERA,
    APPLY_ERROR_KIND_DATAPOINT,
    APPLY_ERROR_KIND_PUBLISHER,
    APPLY_ERROR_KIND_SERVICE,
    ASSET_FAILURE_KIND_REFUSED,
    ASSET_FAILURE_KIND_UNRESOLVABLE,
    ASSET_FAILURE_KIND_UPLOAD_FAILED,
    CAMERA_STATE_CAUSE_COMMAND,
    CAMERA_STATE_CAUSE_CONFIG_CHANGE,
    CAMERA_STATE_CAUSE_LIVE_LOST,
    CAMERA_STATE_CAUSE_SOURCE,
    ActionConfig,
    ApplyError,
    CameraConfig,
    CameraSource,
    DatapointConfig,
    MjpegSource,
    PublisherConfig,
    RetentionConfig,
    RosSource,
    RtspSource,
    ServiceConfig,
    V4l2Source,
    snapshot_frame,
)

# `field_path_invalid` now has exactly one producer, and it is the datapoint
# apply pass: `sampling.resolve_field` against a datapoint's `field`. The 3.0
# configuration format took the other one away — a parameter's name used to
# be a dotted path into the Goal/Request and was resolved the same way, and
# it is a free-standing identifier now, so `_apply_actions`,
# `_apply_services` and `_apply_publishers` carry no `except
# sampling.FieldPathError` any more. A guard against an
# error nothing in reach can raise looks like a defence and is not one; the
# template failures those passes really do see are reported `unknown`, the
# code every other per-slug apply failure already uses.

log = logging.getLogger(__name__)

# How long the asyncio side waits for the executor thread to get around to a
# submitted unit of work. Generous: it only has to rule out a genuinely stuck
# executor, not compete with normal apply/introspect latency.
WORK_TIMEOUT_S = 10.0

DEFAULT_SAMPLE_QUEUE_SIZE = 1000

# How often the shared failsafe watchdog re-checks every publisher.
# Well below any realistic `timeout_ms` (a cmd_vel publisher's is
# typically in the low hundreds of ms) so a fire lands within roughly this
# margin of the deadline, never late by more than that.
FAILSAFE_CHECK_INTERVAL_S = 0.01

# How long a removed publisher's handle outlives its last failsafe. A
# reliable writer resends a sample its readers have not acknowledged; a
# destroyed one resends nothing, so a failsafe dropped on first send and
# destroyed a moment later never arrives and the robot keeps its last
# command. The handle stays until every reader has acknowledged or this
# runs out -- a reader that vanished without unmatching never will. Longer
# than one Fast DDS heartbeat period (3 s by default), which is how long a
# resend between processes can take.
PARTING_FAILSAFE_ACK_TIMEOUT_S = 4.0

# The one exception to not blocking for a retiring publisher: a new
# publisher of a different type on a topic a retiring handle still holds,
# which the middleware refuses within one node. The old handle gets this
# long to be acknowledged, on the executor thread, and then goes. Short,
# because every other publisher's watchdog waits with it; rare, because it
# takes a type change on a driven publisher.
TYPE_CHANGE_ACK_WAIT_S = 0.5

# How many jobs `JobManager` may hold at once — every job not yet
# *delivered* (jobs.py's own `finish`/`tracked_count`), whether still
# running or a terminal outcome the bridge is still holding for report.
# Without a bound the bridge accepted unlimited work while
# `JobUpdateQueue` grew without bound behind it.
#
# `JobUpdateQueue` itself stays unbounded and drop-nothing — the wire
# protocol promises a job's full outcome, and dropping one to make room
# would be exactly the lie this package keeps closing elsewhere. So the lever is admission, not
# eviction: refuse a *new* invoke once this many jobs are already owed a
# delivered report, rather than accepting work whose eventual outcome the
# process may never get to send.
#
# Sized against what actually drives concurrency here, not against the
# per-record cost (a `_JobRecord` is a handful of small fields — tens of
# bytes, negligible on its own): busy-per-slug already limits one *running*
# job per configured action/service slug, so the number of jobs genuinely
# in flight at once is bounded by how many such slugs a robot exposes —
# tens, not hundreds, for every configuration this package has built. The
# rest of this bound's headroom exists for the undelivered-terminal tail: a
# robot can go on being *invoked* for as long as its socket is open even
# while disconnected-and-reconnecting cycles keep it from ever delivering
# a result, so 200 is not "how many jobs run at once" but "how large a
# backlog of owed reports is worth refusing new work over" — generous
# enough that no real workload should ever see `job_queue_full`, and small
# enough that a caller flooding invokes for slugs it does not even own
# hits a wall long before the queue behind it could threaten the process.
MAX_TRACKED_JOBS = 200

# How long an invoked action may go unaccepted before the bridge gives up on
# its server ever answering — without this, a `send_goal_async()` whose server
# does not exist never resolves its response future: no `job_update` is ever
# emitted (not even `running`),
# `_active_goals` never gets an entry, and `cancel_job` becomes a silent
# no-op on top of it — the slug is wedged until the process restarts.
#
# superseded as the actual deadline by `cloudInvoke.patience_ms`, which
# is required on every invoke frame and travels with the call (see
# `_invoke_action`) — two independent 15 s constants (this one, and the
# cloud's own `commandTimeoutMs`) used to agree only by coincidence, with no
# way to tell whose deadline a caller had actually hit. Kept only as the
# number this used to be and the number `DEFAULT_PATIENCE_MS` in the wire
# contracts still is — not read anywhere in this file, and it stays that way:
# that reaches goal-acceptance timing with no `patience_ms` to use is a bug
# to fix, not a gap for this constant to quietly fill.
GOAL_ACCEPT_TIMEOUT_S = 15.0

# The topic the connected bridge detects a URDF on. Hardcoded: calling it a
# *default* implies a configurable alternative source, but no config surface
# for one exists in `config.py` or in the contracts' robot-config document.
# A known cut, not an oversight.
DEFAULT_URDF_TOPIC = "/robot_description"

# How often the active "does a publisher still exist" check runs — its own
# timer, separate from `FAILSAFE_CHECK_INTERVAL_S`'s 10ms
# cadence. `count_publishers` is a real graph query, not a dict lookup, and
# unlike a failsafe there is nothing safety-critical about noticing a lost
# URDF within milliseconds — a few seconds of latency is the honest cost of
# not running a graph query 100 times a second for the life of the node.
URDF_AVAILABILITY_CHECK_INTERVAL_S = 2.0

# `robot_state_publisher` publishes the URDF once, latched, so a late
# subscriber (the bridge almost always starts after the robot's own stack)
# still receives it — the whole point of TRANSIENT_LOCAL. A VOLATILE
# subscription here would silently see nothing from the common case (a
# robot that started before the bridge did) and read as "this robot has no
# URDF" rather than as a bug.
_URDF_SUBSCRIPTION_QOS = QoSProfile(
    depth=1,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    reliability=ReliabilityPolicy.RELIABLE,
)

# Vendored from the wire contracts' own `constants.json` — not
# hand-copied as Python literals under a comment naming the TypeScript
# constant, which is the drift those constants exist to prevent, surviving
# in the one repo that could not import them (`URDF_ASSET_NAME` and
# `ASSET_UPLOAD_HEADERS` used to be exactly that). `test_contracts_sync.py`
# compares this file byte-for-byte against the wire contracts' own copy
# whenever `$FLEETLESS_CONTRACTS_DIR` names one, the same guard already used
# for the vendored wire schemas. Nothing is inferred from a directory that
# happens to sit next to this checkout: with the variable unset the
# comparison skips, and `run-tests.sh` says out loud that it did.
with (pathlib.Path(__file__).parent / "contracts_constants.json").open() as _f:
    _CONTRACTS_CONSTANTS = json.load(_f)

# The `x-fleetless-asset-name` the URDF itself uploads under —
# pinned in the wire contracts as `URDF_ASSET_NAME`, not
# left as a conversational agreement between this package and the cloud's:
# that constant's own doc comment names the exact scar ("an agreement in
# conversation is exactly the thing that drifts") this package has been
# bitten by twice already. Matches `DEFAULT_URDF_TOPIC`'s basename.
URDF_ASSET_NAME = _CONTRACTS_CONSTANTS["URDF_ASSET_NAME"]

# The header *names* a `POST` to the cloud's per-sync upload URL carries,
# from contracts' `ASSET_UPLOAD_HEADERS` — used in `_upload_asset_bytes`
# below.
ASSET_UPLOAD_HEADERS = _CONTRACTS_CONSTANTS["ASSET_UPLOAD_HEADERS"]

# The robot's whole asset store, read only to be reported. There is no
# per-file ceiling any more: `ASSET_UPLOAD_MAX_BYTES` is gone from the
# contracts, and what a file costs is now charged against this store, which
# only the cloud can evaluate — it knows what the robot's other assets
# already occupy and this process does not. So nothing here compares a size
# against anything; the bridge attempts every file and passes back whatever
# the cloud answers. Vendored rather than typed for the usual reason: a
# limit one side cannot read is two limits again.
ROBOT_ASSET_STORE_BYTES = _CONTRACTS_CONSTANTS["ROBOT_ASSET_STORE_BYTES"]

# How much of a refusal's body is read looking for the store's three
# numbers. An error body is not a payload — the cloud's is a few hundred
# bytes — and an unbounded `read()` on whatever a proxy decides to return
# is the same shape as the buffering the per-file ceiling existed to
# prevent, one layer over.
ASSET_REFUSAL_BODY_MAX_BYTES = 64 * 1024

# The two statuses that mean "the cloud will not keep these bytes", as
# opposed to "the transfer did not succeed". `409` is the store's own
# refusal, which names its three numbers; `413` is the server's body
# limit, which names nothing — reached only by a file so large that the
# announced size never got weighed, and reported as a bare refusal rather
# than a transfer failure precisely because retrying it is futile.
ASSET_REFUSAL_STATUSES = frozenset({409, 413})

# How large a `.dae` may be before this bridge stops scanning it for
# internal texture references. A **local policy, not a wire constant**:
# nothing on the other side knows or cares about it, which is why it is a
# literal here rather than something vendored.
#
# The per-file upload ceiling is gone — the cloud's store decides what fits
# now — but `_extract_dae_texture_references` reads the whole file and
# builds an ElementTree over it, which is a multiple of the file size in
# memory and has nothing to do with what the cloud will accept. A real
# robot's 193 MB `base.dae` is the case this exists for. Above the cap the
# scan is skipped and the mesh still uploads, streamed: a `.dae` whose
# textures were never discovered renders untextured, which is worse than
# textured and much better than a bridge that died reading it.
DAE_SCAN_MAX_BYTES = 64 * 1024 * 1024

# Generous relative to a mesh file's realistic size (tens of MB is not
# unusual for a detailed collision mesh) — this only has to rule out a
# genuinely hung upload, not compete with normal transfer time on a slow
# link.
ASSET_UPLOAD_TIMEOUT_S = 60.0

# A single item's own retry ceiling for `429 rate_limited` — not
# a network-timeout retry, only this one status. Bounded because a sync
# that never ends is exactly what this ceiling exists to rule out; generous
# relative to what the production rate limiter ever actually demands.
# `retry_after_ms` is bounded by the bucket's own refill interval
# (`TokenBucketRateLimiter.check`'s own formula in the cloud:
# `Math.max(0, this.refillMs - (now - bucket.lastRefillAt))`), and this
# bridge uploads sequentially — one item drains, the next either finds a
# token waiting or refills within one more interval. 200 meshes measured
# draining in 85s wall time against the real bucket (capacity 30, refill
# 500ms); this ceiling is not expected to be reached in practice.
ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES = 10

# What this bridge waits if a `429` body cannot be read as
# `{"details": {"retry_after_ms": ...}}` at all — malformed JSON, a missing
# field, anything that would otherwise make this raise. A short, fixed
# fallback rather than giving up immediately: the refusal itself is real
# even when its details are not, and the cloud's own bucket will have
# moved on well before this elapses either way.
ASSET_UPLOAD_RATE_LIMIT_FALLBACK_WAIT_S = 0.5

# bounds on how many `.dae`-internal references one
# file, and one sync, will individually report — the reference count is
# nothing this bridge controls (it comes straight out of a `.dae`'s own
# text, graph-adjacent input the same way `/robot_description` itself is),
# while the terminal `asset_progress` frame's size is bounded by
# `MAX_WS_PAYLOAD_BYTES` on the *cloud's* socket. A 2,120,745-byte `.dae`
# with 17,331 unresolvable `<init_from>` refs produced a terminal frame 32
# bytes over the limit — not a dropped frame, the robot's own socket closed
# mid-sync by a file in
# its own workspace. The fix (report an unresolved reference instead of
# dropping it) turned a bounded-by-the-workspace list into one bounded only
# by a `.dae`'s own text; these two ceilings are what re-bound it.
#
# Per-file generous relative to any real model (a whole robot geometry has a
# handful of textures total) while keeping any one `.dae`'s contribution a
# small fraction of the per-sync ceiling, itself half of the contract's own
# `assetSyncStatus.failed` cap (1000 entries) — headroom for the URDF entry
# and ordinary mesh failures alongside whatever `.dae`s contribute.
DAE_MAX_INTERNAL_REFERENCES_PER_FILE = 100
DAE_MAX_INTERNAL_REFERENCES_PER_SYNC = 500

# The per-file cap above bounds the `.dae`-internal half
# of `failed` and left the mesh-level half — a `<mesh>`/`<texture>` URI
# straight off the URDF, uncapped in both `assets_available` and
# `cloud_asset_request` — uncapped too. A URDF declaring more than 1000
# unresolvable `package://` references (not exotic for a generated one)
# produced a terminal frame the contract's own `failed.max(1000)` then
# refused. The consequence measured worse than a wedge: `parseBridgeFrame`
# logged the rejected frame and returned `null`, the sync sat untouched,
# and the idle timer ended it 120s later reporting "no response from the
# robot" — a false cause, in the exact field added so that causes
# would stop being false. The robot answered; the cloud's own schema
# refused the answer.
#
# The arithmetic the two ceilings have to share, stated rather than
# assumed: contracts caps `failed` at 1000 entries. Worst case per sync:
# 1 (the URDF's own entry) + this ceiling + 1 (this ceiling's own
# overflow sentinel, added at most once) + `DAE_MAX_INTERNAL_REFERENCES_
# PER_SYNC` (500) + 1 (*its* own overflow sentinel, added at most once —
# see the follow-up comment above `sync_overflow_sentinel`: the
# per-file branch used to grow `sync_reference_count` without that
# ceiling ever being re-checked, so this "+1" did not hold until that was
# fixed) must stay comfortably under 1000. 1 + 400 + 1 + 500 + 1 = 903 —
# a bound that could be exceeded by adding the other bound to it is not a
# bound.
MESH_MAX_FAILED_PER_SYNC = 400


class SampleQueue:
    """The bounded, drop-oldest handoff from ROS callbacks to the client's
    send loop. `put_threadsafe` is the only method callable off the event
    loop thread; everything else assumes the caller already is on it.

    `on_put` is an optional zero-argument callback fired on the event loop
    thread whenever something lands here — `client.py` points it at its
    `PrioritizedWriter.wake` so the writer re-scans immediately instead of
    waiting out its idle tick. A settable attribute rather than a
    constructor-injected dependency because the writer does not exist yet
    when `RosRuntime` builds this queue, and deliberately a *callback*
    rather than an import: nothing in ros_runtime.py knows the writer
    exists, which is what keeps the ROS side testable without one."""

    def __init__(
        self,
        maxsize: int = DEFAULT_SAMPLE_QUEUE_SIZE,
        on_put: Optional[Callable[[], None]] = None,
    ) -> None:
        self._queue: "asyncio.Queue[sampling.Sample]" = asyncio.Queue(maxsize=maxsize)
        self._dropped = 0
        self.on_put = on_put

    def put_threadsafe(self, loop: asyncio.AbstractEventLoop, sample: sampling.Sample) -> None:
        loop.call_soon_threadsafe(self._put_dropping_oldest, sample)

    def _put_dropping_oldest(self, sample: sampling.Sample) -> None:
        if self._queue.full():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - full() just said otherwise
                pass
            else:
                self._dropped += 1
        self._queue.put_nowait(sample)
        if self.on_put is not None:
            self.on_put()

    async def get(self) -> sampling.Sample:
        return await self._queue.get()

    def try_get(self) -> Optional[sampling.Sample]:
        """Non-blocking: `None` if nothing is live right now. The outgoing
        pump uses this to check "is a live sample immediately ready" before
        ever considering a backfill item — live always wins the race, it is
        never made to wait its turn behind a backlog."""
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def drain_drop_count(self) -> int:
        """Read-and-reset — the client calls this once per reconnect, so a
        long-running drop streak is reported as one number, not a log line
        per sample."""
        count, self._dropped = self._dropped, 0
        return count


class CameraStateQueue:
    """Latest-per-slug handoff for outgoing `bridgeCameraState` updates —
    except for the ones that answer a command, which are never coalesced.

    Replaces the unbounded `asyncio.Queue` this used to be — the one queue
    in this package that was a defect rather than a deliberate trade, fixed
    here by going latest-per-slug.
    A camera whose source keeps failing and recovering produces a state
    transition per flap; with the writer busy on a weak link, an unbounded
    queue grows one entry per flap and then delivers a *replay* of states
    that stopped being true minutes ago.

    So an unsolicited state is a **level**, and only the latest one per
    slug is worth queueing: a superseded level has no value once a newer
    one exists, the same reasoning `camera.LatestFrameHolder` uses one
    level up the camera pipeline. What that costs, stated rather than
    implied: an intermediate transition is genuinely lost, not summarized.
    The cloud's copy converges on the truth as soon as the freshest entry
    lands, which is what makes that acceptable for a level.

    A state carrying a `request_id` is not a level — it is the **answer to
    a command** (the pairing rule: the cloud sends `camera_start` /
    `camera_stop` with a `request_id` and waits for exactly one state
    echoing it). Coalescing one away leaves an operator's command
    permanently unanswered, which is not a stale reading the next update
    corrects; it is a reply that never comes. Found by
    `test_stop_live_stops_the_publisher_and_reports_publishing_false`,
    where a start and an immediate stop queued `(front, True, req-1)` and
    `(front, False, None)` and a pure latest-per-slug rule delivered only
    the second. So a request-answering update takes its own position in
    the queue, coalesces nothing and is coalesced by nothing.

    That is exactly the split `JobUpdateQueue` (jobs.py) already makes —
    terminal updates drop-nothing, non-terminal ones latest-per-job — and
    this class borrows its structure too: `_order` holds either a
    request-answering `CameraStateUpdate` directly or a bare slug string
    standing for "the latest unsolicited update for this slug is in
    `_pending`". The bound the design asked for is unaffected: a flapping
    source reports `cause='source'` with `request_id=None` by
    construction, so the unbounded-growth case is precisely the coalesced
    one, and the uncoalesced remainder is bounded by how many commands the
    cloud has outstanding.

    Insertion order is preserved: a coalesced update replaces its
    predecessor **in place** rather than jumping the queue, so a slug that
    keeps flapping cannot starve another slug's pending state.

    **Threading.** `put` is event-loop-thread only; anything on the
    executor thread calls `put_threadsafe(loop, update)` instead, the same
    split `SampleQueue` and `JobUpdateQueue` already use. This is not
    stylistic: `put` and `try_get` are multi-statement read-modify-writes
    over `_order`/`_pending` with no lock, and the interleavings are not
    theoretical. A `try_get` that pops a slug marker between `put`'s
    `_order.append(slug)` and its `_pending[slug] = update` raises
    `KeyError` out of the writer's `try_next`, which the writer answers by
    closing the socket; two concurrent `put`s that both see "not pending"
    append two markers for one entry, and the second `_pending.pop` fails
    the same way. The `on_put` hook is the other half: it ends in
    `asyncio.Event.set()`, which does not wake the selector when called
    off the loop thread, so a wake fired from the executor could be lost
    outright. Marshalling through `call_soon_threadsafe` fixes both at
    once — every mutation, and every `on_put`, runs on the loop thread.

    `on_put` is the same optional wake hook `SampleQueue` documents.

    **Residual, stated rather than implied.** The bound is real for the
    case it was built for — a flapping source reports `cause='source'`
    with `request_id=None`, so its growth is exactly the coalesced case.
    It does **not** hold against a cloud that loops `camera_start` on a
    slug this bridge has no camera for: `start_live`'s unknown-slug branch
    answers every one of those with an uncoalescible `request_id`-bearing
    state, and nothing here caps that. Deliberate, twice over: suppressing
    the answer would break the pairing rule (the cloud is waiting for
    exactly that frame, and a dropped one never comes), and the defence
    that actually applies lives on the cloud, which enforces that a slug
    is a granted camera before ever minting a token — this branch
    is the fallback for a race it lost, not a steady state. Backpressure
    on inbound frames is deliberately out of scope here; if
    that changes, this is where it would be felt."""

    def __init__(self, on_put: Optional[Callable[[], None]] = None) -> None:
        self._order: Deque[Any] = deque()
        self._pending: Dict[str, "CameraStateUpdate"] = {}
        self.on_put = on_put

    def put_threadsafe(
        self, loop: asyncio.AbstractEventLoop, update: "CameraStateUpdate"
    ) -> None:
        """The only method callable off the event loop thread — see the
        class docstring for the two races this closes."""
        loop.call_soon_threadsafe(self.put, update)

    def put(self, update: "CameraStateUpdate") -> None:
        if update.request_id is not None:
            self._order.append(update)
        elif update.slug not in self._pending:
            self._order.append(update.slug)
            self._pending[update.slug] = update
        else:
            # Coalesced, not queued: the position this slug already holds
            # is kept, only the payload it resolves to is replaced.
            self._pending[update.slug] = update
        if self.on_put is not None:
            self.on_put()

    def try_get(self) -> Optional["CameraStateUpdate"]:
        """Non-blocking: `None` when nothing is pending. The only accessor
        — there is no awaitable `get()`, because the one consumer is the
        writer's tier scan, which must never block on one source while a
        higher tier waits."""
        if not self._order:
            return None
        item = self._order.popleft()
        if isinstance(item, str):
            return self._pending.pop(item)
        return item


def _backlog_depth(retention: RetentionConfig) -> int:
    """How many samples this datapoint's disconnect buffer may hold.

    `retention.max_buffer_values` is optional on the wire and the parser
    leaves an absent one as `None` rather than picking a number for a caller
    it cannot see. This is that caller. An absent depth is `0` — the same
    "not buffered" answer the `buffer` defaulted to, and the only reading
    that does not allocate memory on a robot for a depth nobody chose.
    `retention.enabled` is a separate switch and is passed separately;
    contracts bounds a written `max_buffer_values` at `1..100_000`, so `0`
    here always means "absent", never "the developer asked for none"."""
    return retention.max_buffer_values or 0


class BacklogStore:
    """Per-slug, bounded, drop-oldest backlog for datapoints configured with
    `retention.enabled` — filled only while disconnected (the
    subscription callback checks `buffer_enabled` and `RosRuntime._connected`
    before ever pushing here; an unbuffered datapoint never reaches this
    class at all, which is what makes its "gap" honest rather than merely
    unlucky). Drained once reconnected by the prioritized writer's tier-4
    source, which only gets a turn when every higher tier is empty — the
    50 ms floor between backfill sends that used to pace this is retired,
    because strict tier order does its job structurally (see
    `client.py`'s `_TIER_BACKFILL`). This class only holds the backlog and
    answers "is there anything"; it does not know or care about send
    ordering or connection state itself.

    A `Lock` guards every method: `configure`/`push` run on the executor
    thread (from config-apply and the subscription callback), `pop_any`/
    `has_pending` run on the asyncio side (the outgoing pump) — the same
    cross-thread shape `JobManager` has, for the same reason."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._backlogs: Dict[str, Deque[sampling.Sample]] = {}
        self._enabled: Dict[str, bool] = {}
        self._max_values: Dict[str, int] = {}

    def configure(self, slug: str, enabled: bool, max_values: int) -> None:
        """Called whenever a datapoint's config is (re)applied. Shrinking
        `max_values` below what is already buffered drops the oldest
        overflow immediately, the same rule `push` uses one at a time."""
        with self._lock:
            self._enabled[slug] = enabled
            self._max_values[slug] = max_values
            backlog = self._backlogs.setdefault(slug, deque())
            while len(backlog) > max_values:
                backlog.popleft()

    def remove(self, slug: str) -> None:
        with self._lock:
            self._backlogs.pop(slug, None)
            self._enabled.pop(slug, None)
            self._max_values.pop(slug, None)

    def push(self, slug: str, sample: sampling.Sample) -> None:
        with self._lock:
            if not self._enabled.get(slug, False):
                return  # unbuffered — the caller should not even reach here, but be sure
            max_values = self._max_values.get(slug, 0)
            if max_values <= 0:
                return
            backlog = self._backlogs.setdefault(slug, deque())
            if len(backlog) >= max_values:
                backlog.popleft()
            backlog.append(sample)

    def has_pending(self) -> bool:
        with self._lock:
            return any(self._backlogs.values())

    def pop_any(self) -> Optional[sampling.Sample]:
        """One sample from the first non-empty per-slug backlog. No
        ordering guarantee *across* slugs is promised (nothing promises an
        interleaving order between datapoints) — only within one
        slug's own backlog, which is FIFO by construction."""
        with self._lock:
            for backlog in self._backlogs.values():
                if backlog:
                    return backlog.popleft()
            return None


@dataclass
class _Subscription:
    topic: str
    type_name: str
    message_class: type
    field: Optional[str]
    field_type_str: str
    scale: Optional[float]
    offset: Optional[float]
    rate: sampling.RatePolicy
    buffer_enabled: bool = False
    #: What `rate_throttle_hz` asked for, kept alongside the policy built from
    #: it: low-bandwidth mode has to compare the configured rate against its
    #: own ceiling, and a `RatePolicy` does not say what rate it holds.
    configured_hz: Optional[float] = None
    #: `low_bandwidth: keep` — this datapoint is exempt from the mode's cap.
    keep: bool = False
    #: The mode's ceiling for this slug while it bites, else `None`. A second
    #: policy rather than a replacement for `rate`, so leaving the mode
    #: restores the configured rate without rebuilding it.
    cap: Optional[sampling.RatePolicy] = None
    handle: object = None  # the rclpy Subscription, set once created


# Every entry that builds a message carries two things the 3.0 format
# introduced: `message`, the template the developer wrote, and
# `shared_messages`, the `messages:` map a `message: ${name}` reference
# resolves against. The map is config-wide rather than per-entry, but it is
# stored per entry and refreshed on every apply so that an entry is complete on
# its own: `_invoke_action` and `_publish` run long after the apply that
# configured them, and a single
# runtime-level copy would have to be kept in step by five apply passes
# instead of by the three that actually build messages.


@dataclass
class _ActionEntry:
    ros_name: str
    type_name: str
    action_class: type
    # `{}` is an empty Goal, and it is the only spelling of one: the parser
    # normalises an absent `message` to `{}`, so None must not be reachable
    # here either — two spellings of "nothing to send" is one too many.
    message: object = field(default_factory=dict)
    parameters: dict = field(default_factory=dict)
    shared_messages: dict = field(default_factory=dict)
    client: object = None  # the rclpy ActionClient, set once created


@dataclass
class _ServiceEntry:
    ros_name: str
    type_name: str
    service_class: type
    message: object = field(default_factory=dict)  # `{}` is an empty Request; see _ActionEntry
    parameters: dict = field(default_factory=dict)
    shared_messages: dict = field(default_factory=dict)
    client: object = None  # the rclpy Client, set once created


@dataclass
class _PublisherEntry:
    topic: str
    type_name: str
    message_class: type
    message: object = field(default_factory=dict)  # the publish template; see _ActionEntry
    parameters: dict = field(default_factory=dict)
    shared_messages: dict = field(default_factory=dict)
    # The failsafe as a finished tree — already dereferenced and already
    # proven to hold no placeholder (params.resolve_failsafe_body), because
    # the moment it is needed is an emergency.
    failsafe_body: object = None
    timeout_ms: int = 0
    quiet_timeout_ms: int = 0
    handle: object = None  # the rclpy Publisher, set once created
    # Watchdog bookkeeping for the failsafe timer. `None` means
    # dormant — nobody has ever published, so there is nothing to protect
    # against and no failsafe fires no matter how long that stays true.
    # `_failsafe_sent` re-arms on the next publish: the timer fires the
    # failsafe once per silence period, not on every tick of continued
    # silence after the first.
    last_activity_at: Optional[float] = None
    _failsafe_sent: bool = False


@dataclass
class _RetiringPublisher:
    """A publisher already removed from `_publishers` whose handle is kept
    until its last failsafe is acknowledged (`PARTING_FAILSAFE_ACK_TIMEOUT_S`)."""

    slug: str
    topic: str
    type_name: str
    handle: object
    deadline: float


class UploadResult(NamedTuple):
    """What one asset upload came to — a bool would not carry it any more.

    `ok` is the only thing most callers read. The other two exist because
    the per-robot store made *why* an upload did not happen a fact the
    bridge has to relay rather than derive: `refused` marks a `409`, which
    means the bytes were never stored, and `details` carries the cloud's
    own `{store_bytes, used_bytes, size_bytes}` when it said so in terms
    this bridge could read.

    `refused=True` with `details=None` is a real and expected combination:
    a `409` whose body is not that shape — an older cloud, a proxy's error
    page — still means "never stored", and reporting it as a transfer
    failure would tell a reconciliation to retry something that will be
    refused again. Filling in numbers to avoid the empty case would be
    inventing them; the contract allows a bare `refused` for exactly this.
    """

    ok: bool
    refused: bool = False
    details: Optional[Dict[str, int]] = None


def _upload_failure(result: UploadResult) -> Tuple[str, Optional[Dict[str, int]]]:
    """The `(kind, details)` an unsuccessful `UploadResult` is reported as.

    One place, because both upload sites — a top-level mesh and a
    `.dae`-internal texture — have to classify the same outcome the same
    way, and they used to do it with a bool each and no classification at
    all."""
    if result.refused:
        return ASSET_FAILURE_KIND_REFUSED, result.details
    # The bytes exist and resolved; the transfer itself is what did not
    # succeed — transient, and the asset is still wanted.
    return ASSET_FAILURE_KIND_UPLOAD_FAILED, None


@dataclass(frozen=True)
class CameraStateUpdate:
    """One outgoing `bridgeCameraState` — see `protocol.
    bridge_camera_state_message` for the wire shape this becomes. `error`
    is `(code, message)` or `None`, the same shape `JobUpdate.error` uses.

    `cause` is required — every construction site below
    knows why it is sending this, and `{publishing: False, error: None}`
    alone used to mean three unrelated things (an answer to `camera_stop`, a
    config-change stop, a recovered source), tellable apart only by the
    cloud remembering what it last saw. No default here on purpose: a
    default would silently pick a reading for a call site nobody thought
    about, which is the exact bug this field exists to remove.

    `observed_at_ms`: when the state was *first*
    true, not when this frame happens to be sent — bridge capture time,
    same discipline `timestamp_ms` already holds for datapoint samples.
    Matters most for `report_current_camera_health`'s
    restatements: without it the cloud dated a failure from yesterday to
    the moment of a reconnect, because it used to stamp its own receive
    time instead. A construction site that is reporting something *fresh*
    (just classified, just observed) passes the current capture time; one
    that is *restating* a previously classified fact passes the time that
    fact was first true.

    `request_id`: the id of the `camera_start`/`camera_stop` this
    frame answers, or `None`. Every `cause=CAMERA_STATE_CAUSE_COMMAND` site
    below carries the command's own `request_id` through; every other
    cause passes `None` — an unsolicited report answers no request by
    definition, and that is the majority of frames on a healthy system."""

    slug: str
    publishing: bool
    error: Optional[Tuple[str, str]]
    cause: str
    observed_at_ms: int
    request_id: Optional[str]


@dataclass(frozen=True)
class AssetsAvailable:
    """One outgoing `bridgeAssetsAvailable` — see
    `protocol.bridge_assets_available_message` for the wire shape this
    becomes. **Availability only, nothing transferred** — the cloud's own
    explicit `asset_request` is what moves bytes.

    Queued from two places, deliberately not merged into one: `RosRuntime.
    _on_robot_description` queues one on a *change* in `DEFAULT_URDF_TOPIC`'s
    content (deduped by hash, not once per message the topic happens to
    redeliver), and `RosRuntime.report_current_urdf_availability` queues a
    restatement of whatever is currently cached once per *connection* —
    same split `report_current_camera_health` uses for camera state, and
    for the same reason: a change the ROS graph produces and a fact the
    cloud needs re-told on reconnect are different events, even when they
    carry the same payload.

    `meshes` carries **every** `package://` URI the URDF references,
    verbatim and unresolved — including ones this bridge cannot find in its
    workspace (contracts' own reasoning: reporting only the resolvable ones
    would make an incomplete workspace look like a complete robot)."""

    urdf: bool
    meshes: Tuple[str, ...]


@dataclass(frozen=True)
class AssetProgress:
    """One outgoing `bridgeAssetProgress` — see
    `protocol.bridge_asset_progress_message` for the wire shape. `failed`
    is cumulative as of *this* frame, not just this frame's own newly-failed
    URI — each frame is a self-consistent snapshot of the whole sync so
    far, not a delta a receiver has to fold together correctly itself.

    `failed` entries are `(reference, kind, details)` triples, not bare
    strings — `kind` is one of the `ASSET_FAILURE_KIND_*` constants in
    `protocol.py`, and `details` is non-`None` on exactly one case: an
    `ASSET_FAILURE_KIND_REFUSED` entry the robot's asset store had no room
    for, carrying the cloud's own `{"store_bytes", "used_bytes",
    "size_bytes"}`. Six producers used to write three different facts into
    a flat list of strings indistinguishably; see `_run_asset_sync`'s own
    call sites for which producer emits which kind and why.

    `state='refused_busy'` is the bridge's own single-flight backstop: the
    cloud owns refusing a concurrent sync primarily, but a request that
    slips past it anyway needs an honest
    answer, not silence or a `failed` list that overloads "did not resolve"
    with "was never attempted"."""

    sync_id: str
    done: int
    total: int
    failed: Tuple[Tuple[str, str, Optional[Dict[str, int]]], ...]
    state: str


class _RosSourceAdapter(camera_sources.CameraSourceAdapter):
    """`kind: 'ros'` — the one adapter camera_sources.py's rclpy-free module
    cannot host itself (it needs `self._node`; see that module's own
    docstring), so it lives here
    instead. Unlike the three threaded adapters, `start()`/`stop()` create
    and destroy an rclpy subscription directly rather than spinning up a
    background thread: a subscription callback already runs on the
    executor thread this whole class exists to stay on (see the module
    docstring's "all node interaction happens on the executor thread"
    rule), and creating one is fast and synchronous — there is no connect
    attempt to hand off.

    Type resolution happens in `__init__`, not `start()`: an unresolvable
    or non-image type must fail the config apply immediately (a per-slug
    `config_applied` error), the same promise the earlier code made, not
    surface later as a silent subscription to nothing.

    Decode-on-demand — decode only what someone will see — is
    scoped to this adapter only: the three threaded adapters in
    camera_sources.py decode on their own background thread at source
    rate regardless of whether anyone is watching — they must, to know a
    frame arrived at all — so there is nothing to gate there. ROS is the
    one source that can retain a deserialized message (near-free — DDS
    deserialization itself still happens, rclpy gives no way to skip
    that) without ever converting it, which is what makes `raw` worth
    having here specifically."""

    def __init__(
        self, *, node, topic: str, type_name: str, on_frame: camera_sources.OnFrame,
        on_error: camera_sources.OnError, raw: "camera.RawFrameHolder",
        conversion_wanted: Callable[[], bool], on_raw_frame: Callable[[int], None],
    ) -> None:
        try:
            message_class = get_message(type_name)
        except Exception as exc:
            raise RuntimeError("unknown type {!r}: {}".format(type_name, exc)) from exc
        # Structural, like every other config-apply check: a
        # camera's `type` is contractually one of these two shapes
        # (contracts' cameraSource.ros), not an arbitrary message —
        # camera.py only knows how to convert these two.
        if message_class not in (Image, CompressedImage):
            raise RuntimeError(
                "camera type must be sensor_msgs/msg/Image or "
                "sensor_msgs/msg/CompressedImage, got {!r}".format(type_name)
            )
        self._node = node
        self._topic = topic
        self._message_class = message_class
        self._on_frame = on_frame
        self._on_error = on_error
        self._raw = raw
        self._conversion_wanted = conversion_wanted
        self._on_raw_frame = on_raw_frame
        self._handle: Optional[object] = None

    def start(self) -> None:
        def callback(msg):
            # The capture instant, read before any conversion work — the
            # same bridge-capture-time promise a datapoint sample makes.
            # Rides with the raw message untouched when
            # nothing converts it now, so a snapshot pulled later still
            # reports capture time, never the moment it happened to be
            # decoded.
            timestamp_ms = sampling.capture_timestamp_ms()
            if self._conversion_wanted():
                try:
                    bgr = camera.to_bgr(msg)
                except camera.UnsupportedImageError:
                    # One malformed frame must not kill the subscription,
                    # same rule a datapoint sample that fails to convert
                    # follows.
                    log.exception("Could not convert a camera frame on topic %r", self._topic)
                    return
                self._on_frame(bgr, timestamp_ms)
            else:
                # Nobody is watching live right now: keep only a reference
                # to the newest deserialized message (near-free) instead of
                # paying the imgmsg->BGR conversion cost for a frame a
                # snapshot pull may never even ask for. `_on_raw_frame`
                # still runs the shared error-clear (`_clear_source_error`)
                # — a camera recovering from a bad frame must not stay
                # reported broken just because nothing is converting its
                # frames right now.
                self._raw.set(msg, timestamp_ms)
                self._on_raw_frame(timestamp_ms)

        self._handle = self._node.create_subscription(self._message_class, self._topic, callback, 10)

    def stop(self) -> None:
        if self._handle is not None:
            self._node.destroy_subscription(self._handle)
            self._handle = None


@dataclass
class _CameraEntry:
    """One configured camera. `latest` is where the
    shared `_on_source_frame` hand-off deposits every accepted frame —
    read by the snapshot watchdog and a live publish task, both
    independent of each other and of how fast frames actually arrive
    (camera.py). `adapter` is one of the four `CameraSourceAdapter`
    implementations — everything here is source-kind-agnostic, which
    is the point of the seam."""

    source: CameraSource
    credentials: Optional[camera_sources.Credentials]
    width: int
    height: int
    fps: int
    bitrate_kbps: int
    # Seconds, the unit the wire uses — see `_next_snapshot`'s dueness check
    # for why the bridge does not convert.
    snapshot_interval_seconds: int
    rate: sampling.RatePolicy
    latest: camera.LatestFrameHolder
    # The `kind: 'ros'` adapter's not-yet-converted newest message — always
    # constructed, even for the three threaded sources that
    # never write to it (they decode on their own thread regardless; see
    # `_RosSourceAdapter`'s docstring), so `_CameraEntry`'s shape stays
    # uniform across all four source kinds and `_next_snapshot`'s lazy
    # conversion never needs an `isinstance`/`hasattr` check to run its
    # dueness walk.
    raw: camera.RawFrameHolder
    adapter: camera_sources.CameraSourceAdapter
    # Watchdog bookkeeping for the snapshot timer, same shape as
    # `_PublisherEntry.last_activity_at`: `None` means "never sent one yet",
    # which is what makes the first snapshot go out as soon as a frame is
    # available rather than waiting out a full interval from config-apply
    # time — nothing was promised before there was anything to send.
    last_snapshot_at: Optional[float] = None
    # The adapter's most recent classified error, `(code, message,
    # observed_at_ms)`, or `None`. `start_live` reads this to answer an
    # actual join attempt immediately rather than opening a publisher for a
    # stream with nothing to publish. Also reported unsolicited, on every
    # transition into or out of an error, as a `camera_state` frame with
    # `cause: 'source'` — closing what the first camera release left open
    # ("no unsolicited camera_state for a background source failure with no
    # live session"). Cleared on the next accepted frame (`_on_source_frame`).
    #
    # `observed_at_ms` is when this error was first
    # classified, not "now" — `start_live`'s pre-check *reuses* this value to
    # answer a join attempt without a fresh error having just occurred, and
    # reporting "now" there would be the identical dating-a-restatement-to-
    # the-wrong-moment bug `observed_at_ms` itself exists to close, just at a
    # different call site.
    last_source_error: Optional[Tuple[str, str, int]] = None


class RosRuntime:
    """The bridge's ROS side: graph introspection, type resolution, and the
    live datapoint subscriptions, all reachable from asyncio."""

    def __init__(
        self,
        *,
        node_name: str = "fleetless_bridge",
        sample_queue_maxsize: int = DEFAULT_SAMPLE_QUEUE_SIZE,
        live_publisher_factory: Optional[Callable[..., object]] = None,
        snapshot_max_bytes: int = camera.SNAPSHOT_MAX_BYTES,
        max_tracked_jobs: int = MAX_TRACKED_JOBS,
    ) -> None:
        self._node_name = node_name
        self._max_tracked_jobs = max_tracked_jobs
        self._node = None
        self._executor: Optional[SingleThreadedExecutor] = None
        self._guard = None
        self._failsafe_timer = None
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._work_lock = threading.Lock()
        self._work_queue: "Deque[Tuple[object, concurrent.futures.Future]]" = deque()
        self._subscriptions: Dict[str, _Subscription] = {}
        self._actions: Dict[str, _ActionEntry] = {}
        self._services: Dict[str, _ServiceEntry] = {}
        self._publishers: Dict[str, _PublisherEntry] = {}
        # Removed publishers whose parting failsafe is still unacknowledged;
        # `_reap_retiring_publishers` destroys them from the watchdog tick.
        self._retiring_publishers: List[_RetiringPublisher] = []
        self._cameras: Dict[str, _CameraEntry] = {}
        # The last background health error *reported to the wire* for a
        # slug, or absent — deliberately keyed by slug, not held on
        # `_CameraEntry` (bug found end-to-end: a credential fix
        # re-sends config, which for RTSP/MJPEG changes the *resolved*
        # credential and so counts as `source_or_credentials_changed` in
        # `_apply_cameras` — the entry is destroyed and rebuilt, a fresh
        # `_CameraEntry` with `last_source_error=None` by construction. A
        # recovery report keyed off the entry's own field then has nothing
        # to transition *from*: the new entry never recorded the old
        # error, so the first good frame on the fixed credential looks
        # like an ordinary first frame, not a recovery, and nothing is
        # ever sent. This is exactly the fixed-camera-still-reads-broken
        # case this re-report exists to prevent — and it only ever showed up end to
        # end, because nothing local looks wrong: the new adapter and the
        # new entry are both behaving correctly on their own terms.
        # Survives an entry rebuild for the *same* slug on purpose; popped
        # only when the slug leaves the config entirely (`_apply_cameras`'s
        # removal branch) — a slug that reappears later is (from the wire's
        # perspective) a new camera, not a continuation.
        #
        # `(code, message, observed_at_ms)` — the
        # third element is when the error was first classified, so a later
        # restatement (`report_current_camera_health`) can say when the
        # state actually became true rather than dating it to the moment of
        # the restatement, the exact bug that field exists to close.
        self._camera_last_reported_error: Dict[str, Tuple[str, str, int]] = {}
        # job_id -> the rclpy goal handle currently pursuing it — cancel-by-
        # slug looks the job up in `self.jobs` first, then the handle here.
        self._active_goals: Dict[str, object] = {}
        self._snapshot_max_bytes = snapshot_max_bytes
        # job_id -> (slug, deadline, patience_s) for a goal sent but not yet
        # accepted or rejected — only ever touched on the executor thread
        # (set in `_invoke_action`, cleared in `_on_goal_response` or by the
        # watchdog below), same as `_active_goals`, so no lock is needed.
        # `patience_s` is this *specific* call's own patience — kept
        # alongside the deadline rather than read from one shared constant,
        # because there no longer is one: `cloudInvoke.patience_ms` is
        # required and travels with each call (see `_invoke_action`), and
        # `_check_goal_timeouts` needs the real number for its message,
        # not whatever a different, concurrent call happened to ask for.
        self._goal_deadlines: Dict[str, Tuple[str, float, float]] = {}
        # job_id -> (slug, deadline, patience_s, call_future) for a service
        # call whose ROS response has not arrived yet (2n) — same shape and
        # same watchdog cadence as `_goal_deadlines`, but a different power:
        # a service call has no accept/execute split to bound only the first
        # half of, so this bounds the *whole* call, and on timeout
        # `_check_service_timeouts` calls `entry.client.remove_pending_request
        # (call_future)` — rclpy's own documented way to guarantee a future
        # never receives its response and never runs its done callback. That
        # is stronger than what the action path can do: `_goal_deadlines`
        # can only *report* a timeout early while `_on_goal_response` may
        # still fire later for real (the `late`/`_timed_out_job_ids` dance
        # exists to handle that). A service timeout, once declared, cannot
        # be contradicted afterward — there is no "late" case to guard here.
        self._service_deadlines: Dict[str, Tuple[str, float, float, Any]] = {}
        # job_ids `_check_goal_timeouts` has already reported `goal_timeout`
        # for — never removed (job_ids are never reused, so nothing is ever
        # gained by pruning this set the way `JobManager._jobs` is pruned
        # on delivery — that set exists to bound memory, this one
        # exists to answer a yes/no question forever). Consulted by
        # `_on_goal_response` (a late accept) and
        # `_on_action_feedback` (feedback from a goal accepted late must not
        # keep reporting "running" for a job the cloud already believes
        # `goal_timeout`, or worse `lost` — that would be exactly the kind
        # of machine-moving-while-platform-reports-idle situation this fix
        # exists to prevent).
        self._timed_out_job_ids: Set[str] = set()
        # job_ids cancelled before the action server has answered
        # send_goal_async: `_active_goals[job_id]`
        # is only populated once accepted, in `_on_goal_response` — a
        # cancel arriving in that window (a real race: the cloud answers
        # the invoke's REST call the instant the job is minted, so a
        # caller can legitimately cancel before ROS has even answered)
        # used to find no goal handle and silently return, discarding a
        # cancel for the *right* job while the caller was told it worked.
        # Discarded (not left to accumulate — job_ids are never reused, but
        # this is a queue of *intent*, not a permanent record like
        # `_timed_out_job_ids`) the moment `_on_goal_response` resolves the
        # window one way or another, whether or not it was ever applied.
        self._pending_cancels: Set[str] = set()
        self._stopped = False
        # Plain bool, not a Lock: a single flag read by the subscription
        # callback and written by client.py's set_connected — an assignment
        # is already atomic under the GIL, and the only failure mode of a
        # stale read (one sample landing on the wrong side of a transition)
        # is one sample in the wrong queue, not a corrupted one.
        self._connected = False
        # Low-bandwidth mode, as the levers see it. `_lb_active` is read by the
        # subscription callback on the executor thread and written by
        # `set_low_bandwidth` on the event loop — the same single-flag
        # reasoning `_connected` above states, and the same cost when a read
        # is stale: one sample on the wrong side of a transition.
        self._lb_active = False
        self._lb_max_hz = float(LOW_BANDWIDTH_DEFAULTS["datapoint_max_hz"])
        self._lb_camera = LOW_BANDWIDTH_DEFAULTS["camera"]
        self._lb_bitrate_kbps = int(LOW_BANDWIDTH_DEFAULTS["camera_bitrate_kbps"])
        # What client.py wants told after a successful `ros2 param set`.
        self._lb_params_callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self.samples = SampleQueue(maxsize=sample_queue_maxsize)
        self.backlog = BacklogStore()
        self.jobs = JobManager()
        # slug -> the LivePublisher currently publishing it live — only
        # ever touched on the event loop thread (start_live/stop_live are
        # plain asyncio methods, never routed through _submit_async: joining
        # LiveKit is not a ROS/rclpy concern, so there is no executor-thread
        # rule to follow here the way there is for everything else in this
        # class). Injectable for the same reason `connect` is in client.py —
        # the default builds a real live.LivePublisher; tests substitute one
        # that never touches a real LiveKit server.
        self._live_publishers: Dict[str, object] = {}
        self._live_publisher_factory = live_publisher_factory or live.LivePublisher
        # One asyncio.Lock per slug — see _live_lock's docstring
        # for the start_live/stop_live race it closes.
        self._live_locks: Dict[str, asyncio.Lock] = {}
        # What start_live/stop_live made of a camera_start/camera_stop —
        # drained by client.py's tier-2 writer source into bridgeCameraState
        # frames. Not a cross-thread queue like SampleQueue: everything that
        # writes to it already runs on the event loop thread. Bounded
        # latest-per-slug; see CameraStateQueue.
        self.camera_states = CameraStateQueue()
        # Drained by client.py's own pump into bridgeAssetsAvailable frames
        # — unlike camera_states, written directly from the
        # executor thread (the URDF subscription callback), so `_push`ed
        # via `loop.call_soon_threadsafe`, the same cross-thread discipline
        # `self.jobs`/`self.samples` already use.
        self.assets: "asyncio.Queue[AssetsAvailable]" = asyncio.Queue()
        # The content hash of the last `/robot_description` this reported —
        # executor-thread-only, so no lock: dedupes a TRANSIENT_LOCAL
        # redelivery (there is none in practice) and a genuine republish
        # with identical content from ever queueing a second, redundant
        # `assets_available`.
        self._last_urdf_hash: Optional[str] = None
        # Whether `_check_urdf_availability` has ever run at
        # all — executor-thread-only, same reasoning as `_last_urdf_hash`
        # just above. Distinguishes "never actively checked" (nothing sent;
        # the cloud's own `null` covers it) from "checked and found no
        # publisher" (`urdf: false` sent, exactly once, until something
        # changes) — both read as "no cached text" from `_latest_urdf_text`
        # alone, which is why this exists as a separate flag rather than
        # being inferred from it.
        self._urdf_never_checked = True
        # The raw URDF text and its parsed `package://` references, as of
        # the last change — written only from the executor thread
        # (`_on_robot_description`), read from elsewhere later (mesh
        # resolution runs off an `asset_request`, which arrives on the
        # asyncio side). `None` until the first URDF is ever seen.
        self._urdf_lock = threading.Lock()
        self._latest_urdf_text: Optional[str] = None
        # Every `<mesh>` *and* `<texture>` `package://` reference — this is
        # what travels on the wire's `meshes` field
        # (unrenamed; see `bridgeAssetsAvailable`'s own contract comment).
        self._latest_urdf_meshes: Tuple[str, ...] = ()
        # The subset of the above that came from a `<texture>` element,
        # not a `<mesh>` one — kept separately only so `_run_asset_sync`
        # can classify a requested URI's `kind` at upload time.
        # Not on the wire: `cloud_asset_request.meshes` is unkinded, same
        # as `assets_available.meshes` that produced it.
        self._latest_urdf_texture_uris: FrozenSet[str] = frozenset()
        # Drained by client.py's own pump into bridgeAssetProgress frames
        # — written only from `sync_assets`, which runs entirely
        # on the asyncio side (no rclpy object involved), so a plain
        # `put_nowait` is correct here the same way it is in
        # `report_current_urdf_availability`.
        self.asset_progress: "asyncio.Queue[AssetProgress]" = asyncio.Queue()
        # The `sync_id` of the asset sync currently running, or `None` —
        # the bridge-side single-flight backstop (contracts: the cloud owns
        # this primarily). Checked and set synchronously, before any
        # `await`, in `sync_assets`, so two `asset_request`s dispatched
        # back to back cannot both pass the check.
        self._active_sync_id: Optional[str] = None

    # --- lifecycle --------------------------------------------------------

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        rclpy.init(args=[])
        self._node = rclpy.create_node(self._node_name)
        self._executor = SingleThreadedExecutor()
        self._executor.add_node(self._node)
        self._guard = self._node.create_guard_condition(self._drain_work)
        # Low-bandwidth parameters. Declared with the vendored defaults so
        # `ros2 param list` shows every knob and `ros2 param set
        # /fleetless_bridge low_bandwidth.mode on` works on a running robot
        # with no cloud involved. The YAML section overrides these in
        # client.py; a bad value is refused here with the sentence
        # `LowBandwidthSettings` uses everywhere else.
        for key, default in LOW_BANDWIDTH_DEFAULTS.items():
            # `datapoint_max_hz` is a number on the wire and `1` in the
            # vendored constants. Declared as it stands, ROS would type the
            # parameter as an integer and refuse `1.5` for the rest of the run.
            if key == "datapoint_max_hz":
                default = float(default)
            self._node.declare_parameter("low_bandwidth." + key, default)
        self._node.add_on_set_parameters_callback(self._on_set_parameters)
        # One shared watchdog, two independent checks on the same cadence —
        # same "one rule beats juggling a lifecycle per entity" reasoning
        # `_check_failsafes` already uses, now applied to goal acceptance
        # too rather than one rclpy Timer per in-flight goal. Runs for the
        # whole life of the node, whether or not anything is configured yet.
        self._failsafe_timer = self._node.create_timer(
            FAILSAFE_CHECK_INTERVAL_S, self._check_watchdogs
        )
        # URDF availability detection — permanent for the
        # life of the node, independent of `_apply_*`/exposed configuration
        # entirely: unlike a datapoint, this isn't a configured slug, it's
        # autonomous bridge behaviour, same category as the failsafe timer
        # just above. Created here, before the executor thread starts
        # spinning, for the same reason the timer is (this file's own rule:
        # node entity creation is only safe off-thread before the executor
        # is running or on-thread once it is).
        self._urdf_subscription = self._node.create_subscription(
            StringMsg, DEFAULT_URDF_TOPIC, self._on_robot_description, _URDF_SUBSCRIPTION_QOS
        )
        # The *active* half of URDF availability — its own,
        # coarser timer, not folded into `_check_watchdogs`: `count_
        # publishers` is a real graph query, and there is nothing safety-
        # critical here that needs a 10ms cadence. Independent of any
        # session, same as the subscription above and the failsafe timer —
        # this is what makes "asked and none" reachable at all, since the
        # subscription callback above can only ever notice a publisher
        # *sending* something, never one going away.
        self._urdf_availability_timer = self._node.create_timer(
            URDF_AVAILABILITY_CHECK_INTERVAL_S, self._check_urdf_availability
        )
        self._thread = threading.Thread(
            target=self._executor.spin, name="fleetless-ros-executor", daemon=False
        )
        self._thread.start()

    def stop(self) -> None:
        """Tear everything down. Safe to call even if `start` never ran, and
        safe to call more than once."""
        if self._node is None or self._stopped:
            return
        self._stopped = True
        # Direct _submit(), not run_in_executor — see _submit's docstring for
        # exactly why that is fine here and nowhere else. Publishers first,
        # and each step on its own: their parting failsafes are the one part
        # of shutdown a robot depends on, and a subscription that fails to go
        # must not take them down unsent.
        for teardown in (
            self._destroy_all_publishers,
            self._destroy_all_subscriptions,
            self._destroy_all_actions,
            self._destroy_all_services,
            self._destroy_all_cameras,
        ):
            try:
                self._submit(teardown, timeout=WORK_TIMEOUT_S)
            except Exception:  # noqa: BLE001 - shutting down anyway; the next step still runs
                log.exception("Error during %s on shutdown", teardown.__name__)
        self._executor.shutdown()
        self._node.destroy_node()
        rclpy.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=WORK_TIMEOUT_S)
            if self._thread.is_alive():
                log.error("The ROS executor thread did not stop within %.0fs", WORK_TIMEOUT_S)

    def _destroy_all_subscriptions(self) -> None:
        for slug in list(self._subscriptions):
            self._destroy(slug)

    def _destroy_all_actions(self) -> None:
        for slug in list(self._actions):
            self._destroy_action(slug)

    def _destroy_all_services(self) -> None:
        for slug in list(self._services):
            self._destroy_service(slug)

    def _destroy_all_publishers(self) -> None:
        for slug in list(self._publishers):
            self._destroy_publisher(slug)
        # Shutdown follows straight after, so the parting failsafes are
        # waited for here rather than on a tick that will not come.
        self._reap_retiring_publishers(block=True)

    def _destroy_all_cameras(self) -> None:
        """Only ever called from `RosRuntime.stop()`'s final teardown
        (never from `_apply_cameras`) — and, unlike a mid-session camera
        removal, this one **waits** for every camera's stop thread to
        actually finish before returning. `stop()` proceeds
        straight to `rclpy.shutdown()` after this returns; letting that
        race an adapter thread still running native code inside
        `cv2.VideoCapture` segfaulted, reproduced locally, independent of
        the mid-session credential-rotation bug the throwaway-thread
        design itself fixes. Each `adapter.stop()` already bounds its own
        wait internally (`_ThreadedSourceAdapter`'s own join timeout), so
        this join is a formality in the ordinary case, not a new source of
        slowness — and a slow-but-clean shutdown is the trade this file
        makes over a fast one that can crash."""
        stop_threads = [
            thread
            for thread in (self._destroy_camera(slug) for slug in list(self._cameras))
            if thread is not None
        ]
        for thread in stop_threads:
            thread.join(timeout=WORK_TIMEOUT_S)
            if thread.is_alive():
                log.error("A camera stop thread did not finish during shutdown within %.0fs", WORK_TIMEOUT_S)

    # --- the work queue: everything below runs ON the executor thread -----

    def _drain_work(self) -> None:
        while True:
            with self._work_lock:
                if not self._work_queue:
                    return
                fn, future = self._work_queue.popleft()
            if not future.set_running_or_notify_cancel():
                continue
            try:
                result = fn()
            except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
                future.set_exception(exc)
            else:
                future.set_result(result)

    def _enqueue(self, fn) -> "concurrent.futures.Future":
        """Queue `fn` to run on the executor thread; returns immediately with
        a `concurrent.futures.Future` for its result, without waiting.

        Deliberately synchronous — no thread hop of its own. Every
        asyncio-facing method below calls this directly, on whichever thread
        called *it*, before its own first `await` (see `_submit_async`).
        That is not an implementation detail: two commands dispatched from
        `client.py`'s receive loop (`asyncio.ensure_future(self._ros.invoke(
        ...))`, then `...cancel_job(...)`) each run synchronously up to their
        first await before the loop moves on to receive the next frame, so
        calling `_enqueue` there — rather than inside something offloaded via
        `run_in_executor` — is what makes `_work_queue` land in the same
        order the frames arrived in. The bug this fixes: the old `_submit`
        did the enqueue *and* the blocking wait
        inside one `run_in_executor` call, so the enqueue itself happened on
        a default-pool worker thread, and two such workers had no obligation
        to acquire `_work_lock` in the order their frames were dispatched —
        reproducible in a zero-gap invoke/cancel burst, invisible at any
        real gap because the first worker's append (a lock + append, not
        meaningfully slow) had already finished long before the second
        dispatch even happened."""
        future: "concurrent.futures.Future" = concurrent.futures.Future()
        with self._work_lock:
            self._work_queue.append((fn, future))
        self._guard.trigger()
        return future

    def _submit(self, fn, timeout: float = WORK_TIMEOUT_S):
        """Enqueue `fn` and block the calling thread for its result.

        Safe only where nothing else needs `_work_queue` order to reflect
        this call's position relative to some *other* concurrently-running
        caller — `stop()` is the one user, and it is the last thing that
        happens on the loop thread (`_run_client`'s `finally`, after
        `_serve` has already returned, nothing else left to dispatch), so
        there is no other caller for it to race. Everything else goes
        through `_submit_async` instead, specifically to keep the enqueue on
        the *dispatching* thread and offload only the wait."""
        return self._enqueue(fn).result(timeout=timeout)

    async def _submit_async(self, fn, timeout: float = WORK_TIMEOUT_S):
        """Enqueue `fn` synchronously (preserving the caller's dispatch
        order into `_work_queue`; see `_enqueue`) and await its result
        without blocking the event loop — the async counterpart to
        `_submit`, and what every asyncio-facing method below uses."""
        future = self._enqueue(fn)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, future.result, timeout)

    # --- asyncio-facing API -------------------------------------------------

    def low_bandwidth_params(self) -> Dict[str, Any]:
        """The eight `low_bandwidth.*` parameters, shaped for
        `LowBandwidthSettings.resolve`.

        `{}` before `start()`: the parameters live on the node, and the node
        does not exist until then. That is a real state — `main.py` builds the
        client before it starts the runtime — and an empty layer resolves to
        the defaults, which is the right answer for it."""
        if self._node is None:
            return {}
        values: Dict[str, Any] = {}
        for key in LOW_BANDWIDTH_DEFAULTS:
            values[key] = self._node.get_parameter("low_bandwidth." + key).value
        return values

    def on_low_bandwidth_params(
        self, callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """Ask to be told when a `ros2 param set` changes the section.

        The callback runs on the asyncio loop (the set arrives on whichever
        thread the ROS service handled it on) and is handed the whole section,
        not only the keys that moved: the client resolves all eight against
        the YAML layer on top of them."""
        self._lb_params_callback = callback

    def _on_set_parameters(self, params) -> SetParametersResult:
        """rclpy's pre-set hook: refuse a value the settings would reject,
        before it is stored.

        Validated by building the settings the set would produce, so
        `ros2 param set` is refused with exactly the sentence a bad YAML
        section is refused with. Nothing here mutates on a bad value — the
        parameter keeps what it had, and no callback fires."""
        touched = [p for p in params if p.name.startswith("low_bandwidth.")]
        if not touched:
            # `use_sim_time` and anything else on this node: none of this
            # mode's business, and re-applying the levers for it would be a
            # lever pull nobody asked for.
            return SetParametersResult(successful=True)
        proposed = self.low_bandwidth_params()
        for param in touched:
            proposed[param.name[len("low_bandwidth."):]] = param.value
        try:
            LowBandwidthSettings.resolve(proposed, {})
        except ValueError as exc:
            return SetParametersResult(successful=False, reason=str(exc))
        if self._loop is not None and self._lb_params_callback is not None:
            self._loop.call_soon_threadsafe(self._lb_params_callback, proposed)
        return SetParametersResult(successful=True)

    async def set_low_bandwidth(
        self, active: bool, settings: LowBandwidthSettings
    ) -> None:
        """Pull the mode's levers, or release them. Idempotent.

        client.py calls this on every transition *and* after every settings
        change that did not cause one, because the numbers can move while the
        state does not — a `datapoint_max_hz` of 5 while the mode is already
        on has to reach the subscriptions somehow.

        The cap is rebuilt on the executor thread, where every other touch of
        a `_Subscription` happens; the camera lever runs here on the event
        loop, where `stop_live` and the publishers already live. `async` and
        not a plain setter, because stopping a stream is a coroutine and a
        fire-and-forget job would let a session end with the robot still
        publishing into a room nobody can reach."""
        self._lb_active = active
        self._lb_max_hz = float(settings.datapoint_max_hz)
        self._lb_camera = settings.camera
        self._lb_bitrate_kbps = int(settings.camera_bitrate_kbps)
        if self._node is not None:
            await self._submit_async(self._rebuild_caps)
        await self._apply_camera_lever()

    def _rebuild_caps(self) -> None:
        """Executor thread: every subscription's cap, against the settings
        that just changed."""
        for entry in self._subscriptions.values():
            entry.cap = self._cap_policy(entry.configured_hz, entry.keep)

    def _cap_policy(
        self, configured_hz: Optional[float], keep: bool
    ) -> Optional[sampling.RatePolicy]:
        """The mode's ceiling for one datapoint, or `None` where it does not
        bite: the mode is off, the datapoint says `keep`, or its own
        configured rate is already at or below the ceiling.

        A falsy `configured_hz` is "no ceiling configured" (sampling.py reads
        `None` and `0` the same way), so it never counts as slower than the
        cap — `0 <= 1.0` is arithmetically true and exactly backwards."""
        if not self._lb_active or keep:
            return None
        if configured_hz and configured_hz <= self._lb_max_hz:
            return None
        return sampling.rate_policy(self._lb_max_hz)

    async def _apply_camera_lever(self) -> None:
        """`stop` ends every running stream and says why; `reduce` re-targets
        each one, and leaving the mode puts each camera's configured bitrate
        back. New streams are refused in `start_live` either way, so there is
        nothing here for a camera that is not live."""
        if self._lb_active and self._lb_camera == "stop":
            for slug in list(self._live_publishers):
                await self.stop_live(
                    slug,
                    cause=CAMERA_STATE_CAUSE_LIVE_LOST,
                    error=(
                        "low_bandwidth",
                        "Live video stopped: the bridge entered low-bandwidth mode.",
                    ),
                )
            return
        for slug, publisher in list(self._live_publishers.items()):
            if self._lb_active:
                kbps = self._lb_bitrate_kbps
            else:
                entry = self._cameras.get(slug)
                if entry is None:
                    continue  # the camera left the config while the stream ran
                kbps = entry.bitrate_kbps
            try:
                publisher.set_bitrate_kbps(kbps)
            except Exception:  # noqa: BLE001 - a lever that failed must not end the session
                log.exception("Could not re-target the live bitrate for slug %r", slug)

    def set_connected(self, connected: bool) -> None:
        """Called by client.py when a session starts (`True`, right after
        `hello_ok`) and ends (`False`, in `_converse`'s `finally`). Plain and
        synchronous — no `_submit` round trip — because it only flips a flag
        the subscription callback reads: while connected, a
        sample is live; while not, it goes to `self.backlog` if its
        datapoint is buffered, or is simply dropped (an honest gap) if not."""
        self._connected = connected

    async def apply_config(
        self, datapoints: Mapping[str, DatapointConfig]
    ) -> List[ApplyError]:
        """Diffs `datapoints` — `doc.datapoints`, a mapping keyed by slug —
        against the live subscriptions. Returns an `ApplyError` for every
        slug that failed to apply; a slug not listed either succeeded or was
        unaffected."""
        return await self._submit_async(lambda: self._apply_config(datapoints))

    async def graph_snapshot(self) -> dict:
        return await self._submit_async(lambda: introspection.graph_snapshot(self._node))

    async def resolve_types(
        self, type_names: Sequence[str]
    ) -> Tuple[List[dict], List[str]]:
        return await self._submit_async(lambda: introspection.resolve_types(type_names))

    async def apply_actions(
        self,
        actions: Mapping[str, ActionConfig],
        messages: Optional[Mapping[str, Any]] = None,
    ) -> List[ApplyError]:
        """Diffs `actions` against the live ActionClients, the same shape as
        `apply_config`. Returns an `ApplyError` for every slug that failed
        to apply.

        `messages` is the document's shared `messages:` map — what a
        `message: ${name}` on one of these entries refers to. It defaults to
        empty rather than being required, because a document that shares no
        template omits the section entirely."""
        return await self._submit_async(
            lambda: self._apply_actions(actions, messages or {})
        )

    async def invoke(self, job_id: str, slug: str, params_dict: dict, patience_ms: int) -> None:
        """Starts the job the cloud already minted `job_id` for.
        Fire-and-forget from the caller's point of view: this returns once
        the goal has been *sent*, not once it is done — completion, feedback
        and cancellation all arrive later via `self.jobs.updates`, exactly
        like a datapoint sample arrives via `self.samples`.

        `patience_ms` is this call's own goal-acceptance deadline,
        required — see `cloudInvoke.patience_ms` and `_invoke_action`."""
        await self._submit_async(lambda: self._invoke(job_id, slug, params_dict, patience_ms))

    async def cancel_job(self, slug: str, job_id: Optional[str]) -> None:
        """Cancel by slug, or by a specific job on that slug —
        a real ROS goal cancel. `job_id=None` means today's behaviour:
        whatever is running on `slug`. A `job_id` that does not match what
        is currently running is refused outright — see `_cancel_job` — and
        must never fall back to cancelling the slug's current occupant; the
        caller named an id specifically to rule that out.

        A slug with nothing running, or nothing cancellable (e.g. a service
        call, which has no ROS-level cancel), is a silent no-op: the cloud
        already knows what is running and would not ask otherwise, and a
        stale cancel for a job that just finished on its own is not an
        error."""
        await self._submit_async(lambda: self._cancel_job(slug, job_id))

    async def apply_services(
        self,
        services: Mapping[str, ServiceConfig],
        messages: Optional[Mapping[str, Any]] = None,
    ) -> List[ApplyError]:
        """Diffs `services` against the live service clients, the same shape
        as `apply_actions`, `messages` included."""
        return await self._submit_async(
            lambda: self._apply_services(services, messages or {})
        )

    async def apply_publishers(
        self,
        publishers: Mapping[str, PublisherConfig],
        messages: Optional[Mapping[str, Any]] = None,
    ) -> List[ApplyError]:
        """Diffs `publishers` against the live rclpy Publishers. Both the
        publish template and the failsafe are validated structurally here
 — a failsafe that cannot be built, or that still holds a
        placeholder nothing will ever fill, is caught now, not the first time
        the timer needs to fire it."""
        return await self._submit_async(
            lambda: self._apply_publishers(publishers, messages or {})
        )

    async def publish(self, slug: str, message_dict: dict) -> None:
        """Publishes a client's message on a configured publisher. No job
        is involved: a publish has no id, no lifecycle and no
        reply on the wire — it either lands on the topic or it does not, and
        a build failure is only ever a local log line, since there is
        nothing to report it back on. Also marks the publisher active for
        the failsafe timer."""
        await self._submit_async(lambda: self._publish(slug, message_dict))

    async def apply_cameras(
        self, cameras: Mapping[str, CameraConfig]
    ) -> List[ApplyError]:
        """Diffs `cameras` against the live subscriptions, the same
        topic/type-retarget shape `apply_config` uses for datapoints — a
        camera is slug-addressed like a datapoint, not ros_name-addressed
        like an action/service/publisher. There is no second credentials
        argument any more: the 3.0 format retired the wire-only
        map beside `doc` and put each camera's username and password inside
        its own source, so there is no longer a second place for them to
        disagree with.

        Any changed field for a slug currently live — not only a
        source retarget — stops that stream rather than leaving it frozen
        on the pre-change frame. The diff itself runs on
        the executor thread (`_apply_cameras`, same rule as every other
        config-apply here); `stop_live` is an asyncio method that must run
        on the event loop thread, so it happens here, after the diff
        returns which slugs changed — see `_apply_cameras`'s docstring for
        why the resulting brief window (an already-abandoned holder read
        for a few more milliseconds by a publisher about to be torn down)
        is an acceptable trade rather than something worth a second
        cross-thread round trip to close."""
        errors, changed_live_slugs = await self._submit_async(
            lambda: self._apply_cameras(cameras)
        )
        for slug in changed_live_slugs:
            # this is not an answer to a camera_stop command, so it
            # must not read as one — a no-op for a slug that was not live.
            await self.stop_live(slug, cause=CAMERA_STATE_CAUSE_CONFIG_CHANGE)
        return errors

    async def report_current_camera_health(self) -> None:
        """Reports every configured camera's current background-health
        state unsolicited, `cause: 'source'` — not only on the next
        transition. Meant to be called exactly once per connection, by
        client.py, right after the first config apply following a fresh
        `hello_ok` — this method does not need to know why it was called,
        only to tell the truth about everything it currently has an
        opinion on.

        Closes a defect found end to end: `RosRuntime` survives a
        WebSocket reconnect untouched — only client.py's per-session loop
        restarts, this object does not — but the *cloud's* health store
        does not survive a cloud restart, and a transition-only channel
        has nothing to say to a receiver that has forgotten everything a
        camera was ever thought to be. Verified against the running
        system: after a bridge restart the state reappears (everything is
        genuinely new), after a cloud restart it did not, because nothing
        local changed and nothing local was there to notice.

        Reports unconditionally rather than trying to detect whether the
        far end actually lost anything — the cloud's store already no-ops on
        an unchanged `(state, reason)` pair, and detecting staleness here
        would itself be exactly the kind of remembered state that caused
        the entry-rebuild bug this cluster already fixed once.

        Three-way rule per slug, deliberately not four: a known error is
        reported as itself; a confirmed frame with no known error is
        reported as recovered (`error: None`); neither yet — an adapter
        that has not resolved anything — is skipped outright. `unknown`
        is reserved for "attempted and could not classify the
        failure"; using it for "never attempted" would make one word
        carry two facts, the same mistake `cause` itself exists to
        prevent. The first real report follows within whatever the
        adapter's own connect/preflight timeout is — seconds, not
        longer, except V4L2's still-open unbounded-read gap (tracked
        separately, not new scope here)."""
        await self._submit_async(self._report_current_camera_health)

    def _report_current_camera_health(self) -> None:
        """Runs on the executor thread (via `_submit_async`) — the same
        rule every other reader of `self._cameras` follows."""
        for slug, entry in self._cameras.items():
            classified = self._camera_last_reported_error.get(slug)
            if classified is not None:
                code, message, observed_at_ms = classified
                self.camera_states.put_threadsafe(
                    self._loop,
                    CameraStateUpdate(
                        slug, False, (code, message),
                        cause=CAMERA_STATE_CAUSE_SOURCE, observed_at_ms=observed_at_ms,
                        request_id=None,
                    )
                )
                continue
            latest = entry.latest.get()
            if latest is not None:
                # Confirmed ok as of the latest frame's own capture time —
                # not "now": this is a restatement of a fact already true,
                # same reasoning as the error branch above.
                self.camera_states.put_threadsafe(
                    self._loop,
                    CameraStateUpdate(
                        slug, False, None,
                        cause=CAMERA_STATE_CAUSE_SOURCE, observed_at_ms=latest.timestamp_ms,
                        request_id=None,
                    )
                )
            # else: nothing known yet about this slug — silence is the
            # honest answer, not `unknown`. The adapter's own first
            # success or failure reports for itself shortly, through the
            # ordinary unsolicited path (_on_source_frame/_on_source_error).

    def _live_lock(self, slug: str) -> asyncio.Lock:
        """One lock per slug, created on first use and kept for the life of
        the process (bounded by the number of configured cameras, same
        reasoning `JobManager._jobs` uses for keeping its own records
        around). `start_live` and `stop_live` for the *same* slug hold it
        for their whole body: `stop_live` used to pop the slug
        from `_live_publishers` before awaiting the disconnect, so a
        `camera_start` arriving in that window saw the slug as free and
        built a second publisher — under the same cloud-minted identity —
        while the first was still disconnecting. Serialized per slug, a
        `start_live` arriving mid-stop now waits for the stop to fully
        finish instead of racing it; different slugs never block each
        other, since each gets its own lock."""
        lock = self._live_locks.get(slug)
        if lock is None:
            lock = asyncio.Lock()
            self._live_locks[slug] = lock
        return lock

    async def start_live(
        self, slug: str, url: str, room: str, token: str, request_id: str
    ) -> None:
        """Starts live for `slug` on `cloudCameraStart`. Not
        routed through `_submit_async`/the executor thread — joining
        LiveKit is not a ROS/rclpy concern, only reading `entry.latest`
        (already thread-safe on its own, camera.py) is. Idempotent for an
        already-live slug (a redelivered `camera_start` must not open a
        second connection), and a defensive `live_unavailable` for a slug
        this process has no camera configured for — the cloud enforces that
        a slug is a granted camera before ever minting a token;
        this is the fallback for a race it lost, the same shape busy-per-
        slug is for actions.

        Catches *any* exception from `publisher.start()`, not only
        `live.LiveStartError`: `live.py`'s own `start()` now
        wraps everything it can into `LiveStartError`, but a publisher that
        fails and is not cleaned up here too is a publisher `stop_live`/
        `stop_all_live` can never reach — the one shape of bug this whole
        fix exists to close. `publisher.stop()` is called unconditionally
        on any failure; it is always safe (idempotent even if `start()`
        already cleaned up internally) and is the only way to guarantee a
        half-connected publisher is never simply forgotten.

        `request_id` names this attempt; every `camera_state` frame
        this call emits echoes it (`cause=CAMERA_STATE_CAUSE_COMMAND`) so a
        late answer to an earlier, already-superseded attempt cannot be
        mistaken for one to this one."""
        async with self._live_lock(slug):
            if slug in self._live_publishers:
                return
            entry = self._cameras.get(slug)
            if entry is None:
                self.camera_states.put(
                    CameraStateUpdate(
                        slug, False,
                        ("live_unavailable", "no camera configured for slug {!r}".format(slug)),
                        cause=CAMERA_STATE_CAUSE_COMMAND,
                        observed_at_ms=sampling.capture_timestamp_ms(),
                        request_id=request_id,
                    )
                )
                return

            if self._lb_active:
                # Refused for as long as the mode holds, under `reduce` as
                # much as under `stop`: a new stream is new uplink, and the
                # mode exists because there is none to spare. After the
                # slug's own lookup, so a slug nobody configured still hears
                # the truth about itself rather than being sent chasing the
                # link.
                self.camera_states.put(
                    CameraStateUpdate(
                        slug, False,
                        (
                            "low_bandwidth",
                            "The bridge is in low-bandwidth mode; live video "
                            "waits until the link recovers.",
                        ),
                        cause=CAMERA_STATE_CAUSE_COMMAND,
                        observed_at_ms=sampling.capture_timestamp_ms(),
                        request_id=request_id,
                    )
                )
                return

            # a non-ROS source can be known-broken (auth failed,
            # unreachable) before anyone ever asked for live — it runs
            # continuously in the background to keep snapshots warm,
            # same as a ROS subscription. If it has never delivered a
            # frame at all, answer *this* join attempt with the
            # classified error right away instead of opening a LiveKit
            # publisher for a stream with nothing to publish
            # (protocol.py's own bridge_camera_state_message docstring:
            # the cloud must not leave a viewer watching a black
            # rectangle believing it is live). This is a real answer to
            # a real camera_start (`cause: 'command'`), not the
            # unsolicited background reporting `_on_source_error`/
            # `_on_source_frame` do (`cause: 'source'`) — it only
            # ever fires in response to this joiner's own request.
            if entry.latest.get() is None and entry.last_source_error is not None:
                code, message, observed_at_ms = entry.last_source_error
                self.camera_states.put(
                    CameraStateUpdate(
                        slug, False, (code, message),
                        cause=CAMERA_STATE_CAUSE_COMMAND, observed_at_ms=observed_at_ms,
                        request_id=request_id,
                    )
                )
                return

            publisher = self._live_publisher_factory(
                holder=entry.latest,
                width=entry.width,
                height=entry.height,
                fps=entry.fps,
                bitrate_kbps=entry.bitrate_kbps,
                on_lost=lambda reason, slug=slug: self._on_live_lost(slug, reason),
            )
            try:
                await publisher.start(url, room, token)
            except Exception as exc:  # noqa: BLE001 - see docstring: any failure, not only LiveStartError
                # Same class as the module-wide rule in
                # camera_sources.py: `token` is a LiveKit access token —
                # a credential — and `camera_states` becomes
                # `bridgeCameraState`, sent to the
                # cloud over the wire, not merely logged. A `LiveStartError`
                # is safe to surface as-is: live.py now builds its message
                # from type names only (same commit), never a raw SDK
                # exception, so str(exc) here is exactly what that file
                # already decided was safe to say, and dropping it would
                # only throw away detail a developer legitimately needs
                # ("no route to host" vs. a bare type name). But this
                # branch catches "any failure, not only LiveStartError" —
                # see the docstring above — and nothing guarantees an
                # *unexpected* exception from somewhere else in this call
                # chain is similarly pre-redacted, so anything else still
                # gets the type-name-only treatment.
                message = str(exc) if isinstance(exc, live.LiveStartError) else type(exc).__name__
                log.error("Could not start live for camera slug %r: %s", slug, message)
                try:
                    await publisher.stop()
                except Exception as cleanup_exc:  # noqa: BLE001 - already failed; nothing to report to
                    log.error(
                        "Error cleaning up a live publisher that failed to start for slug %r: %s",
                        slug, type(cleanup_exc).__name__,
                    )
                self.camera_states.put(
                    CameraStateUpdate(
                        slug, False, ("live_unavailable", message),
                        cause=CAMERA_STATE_CAUSE_COMMAND, observed_at_ms=sampling.capture_timestamp_ms(),
                        request_id=request_id,
                    )
                )
                return
            self._live_publishers[slug] = publisher
            self.camera_states.put(
                CameraStateUpdate(
                    slug, True, None,
                    cause=CAMERA_STATE_CAUSE_COMMAND, observed_at_ms=sampling.capture_timestamp_ms(),
                    request_id=request_id,
                )
            )

    def _on_live_lost(self, slug: str, reason: str) -> None:
        """Called by a `live.LivePublisher` (its `on_lost`) when it stops
        publishing on its own — a room disconnect neither `stop_live` nor
        `stop_all_live` asked for (nothing here observed this
        before, so the cloud's belief that a camera was still `publishing:
        true` could outlive the connection indefinitely). Runs
        synchronously, with no `await` in its body, so it cannot interleave
        with `start_live`/`stop_live`'s own dict access mid-operation —
        asyncio never preempts a coroutine (or a plain callback) between
        await points."""
        if self._live_publishers.pop(slug, None) is None:
            return  # already gone — e.g. stop_live's own pop won this race
        self.camera_states.put(
            CameraStateUpdate(
                slug, False, ("live_unavailable", reason),
                cause=CAMERA_STATE_CAUSE_LIVE_LOST, observed_at_ms=sampling.capture_timestamp_ms(),
                request_id=None,  # unsolicited — answers no camera_stop
            )
        )

    async def stop_live(
        self,
        slug: str,
        *,
        cause: str = CAMERA_STATE_CAUSE_COMMAND,
        request_id: Optional[str] = None,
        error: Optional[Tuple[str, str]] = None,
    ) -> None:
        """Stops live for `slug` on `cloudCameraStop`, or as
        the tail end of a config apply that invalidated a live stream —
        `cause` tells the two apart on the wire:
        the default answers an explicit `camera_stop` command, and
        `apply_cameras` below passes `CAMERA_STATE_CAUSE_CONFIG_CHANGE`
        instead, so the cloud never has to guess which one just happened
        from an identical `{publishing: false, error: None}` frame.

        `request_id` is the `camera_stop`'s id when `cause` is the
        default `CAMERA_STATE_CAUSE_COMMAND` — the caller (client.py) always
        passes it for a real command. `apply_cameras` never passes one: a
        config-change stop answers no request by definition, so `None` (the
        default) is the only correct value there.

        `error` is `None` for every caller today — a `camera_stop` answer
        and a config-change stop both have nothing to report, `{publishing:
        false, error: None}` being the whole point of `cause` existing at
        all (see above). It stays in the signature for the caller that
        stops a stream for a reason of its own: `cause` is the wire's
        closed enum, so a *reason* has nowhere to go but the free
        `error.code` channel.

        A slug not currently live is a silent no-op — the cloud already
        knows what is live and would not ask otherwise, the same reasoning
        `cancel_job` uses for a slug with nothing running. Holds the slug's
        publisher until `stop()` has fully finished before removing it from
        `_live_publishers` (see `_live_lock`'s docstring for the race this
        closes) — `stop()` itself is what actually disconnects; only once
        that is done is the slug genuinely free for a new `start_live`."""
        async with self._live_lock(slug):
            publisher = self._live_publishers.get(slug)
            if publisher is None:
                return
            await publisher.stop()
            self._live_publishers.pop(slug, None)
            self.camera_states.put(
                CameraStateUpdate(
                    slug, False, error, cause=cause, observed_at_ms=sampling.capture_timestamp_ms(),
                    request_id=request_id,
                )
            )

    async def stop_all_live(self) -> None:
        """Tears down every active live session — client.py calls this from
        `_converse`'s `finally`, alongside `set_connected(False)`, so a
        dropped connection never leaves the robot publishing into a room
        nobody can tell it to stop. No `camera_states` report: there is no
        socket left to carry it, and the next session starts with nothing
        live regardless, reported fresh only if the cloud asks again.
        Goes through the same per-slug lock `start_live`/`stop_live` do, so
        a session ending mid-`start_live` for some slug cannot race this."""
        for slug in list(self._live_publishers):
            async with self._live_lock(slug):
                publisher = self._live_publishers.pop(slug, None)
                if publisher is None:
                    continue  # already stopped or lost between the snapshot above and now
                await publisher.stop()

    # --- config apply: runs on the executor thread -------------------------

    def _apply_config(self, datapoints: Mapping[str, DatapointConfig]) -> List[ApplyError]:
        wanted = datapoints
        errors: List[ApplyError] = []

        # A retargeted or removed slug loses its old subscription first, so a
        # retarget never briefly holds two subscriptions for the same slug.
        for slug in list(self._subscriptions):
            if slug not in wanted:
                self._destroy(slug)
                continue
            existing = self._subscriptions[slug]
            new = wanted[slug]
            if existing.topic != new.topic or existing.type_name != new.type:
                self._destroy(slug)

        for slug, dp in wanted.items():
            try:
                if slug in self._subscriptions:
                    # Topic and type are unchanged (else it was destroyed
                    # above) — only the sampler's parameters move.
                    self._update_sampler(slug, dp)
                else:
                    self._create(slug, dp)
            except sampling.FieldPathError as exc:
                errors.append(ApplyError(
                    slug=slug, kind=APPLY_ERROR_KIND_DATAPOINT,
                    code=APPLY_ERROR_CODE_FIELD_PATH_INVALID, message=str(exc),
                ))
            except Exception as exc:  # noqa: BLE001 - one bad slug, not the whole apply
                errors.append(ApplyError(
                    slug=slug, kind=APPLY_ERROR_KIND_DATAPOINT,
                    code=APPLY_ERROR_CODE_UNKNOWN, message=str(exc),
                ))
        return errors

    def _update_sampler(self, slug: str, dp: DatapointConfig) -> None:
        entry = self._subscriptions[slug]
        entry.field_type_str = sampling.resolve_field(entry.message_class, dp.field)
        entry.field = dp.field
        entry.scale = dp.numeric.scale
        entry.offset = dp.numeric.offset
        entry.rate = sampling.rate_policy(dp.rate_throttle_hz)
        entry.buffer_enabled = dp.retention.enabled
        entry.configured_hz = dp.rate_throttle_hz
        entry.keep = dp.low_bandwidth_keep
        entry.cap = self._cap_policy(dp.rate_throttle_hz, dp.low_bandwidth_keep)
        self.backlog.configure(slug, dp.retention.enabled, _backlog_depth(dp.retention))

    def _create(self, slug: str, dp: DatapointConfig) -> None:
        try:
            message_class = get_message(dp.type)
        except Exception as exc:
            raise RuntimeError("unknown type {!r}: {}".format(dp.type, exc)) from exc

        # Raises FieldPathError (a ValueError) if `dp.field` does not resolve
        # — surfaces as this slug's error, the others still apply.
        field_type_str = sampling.resolve_field(message_class, dp.field)

        entry = _Subscription(
            topic=dp.topic,
            type_name=dp.type,
            message_class=message_class,
            field=dp.field,
            field_type_str=field_type_str,
            scale=dp.numeric.scale,
            offset=dp.numeric.offset,
            rate=sampling.rate_policy(dp.rate_throttle_hz),
            buffer_enabled=dp.retention.enabled,
            configured_hz=dp.rate_throttle_hz,
            keep=dp.low_bandwidth_keep,
            cap=self._cap_policy(dp.rate_throttle_hz, dp.low_bandwidth_keep),
        )

        def callback(msg, slug=slug):
            self._on_message(slug, msg)

        entry.handle = self._node.create_subscription(message_class, dp.topic, callback, 10)
        self._subscriptions[slug] = entry
        self.backlog.configure(slug, dp.retention.enabled, _backlog_depth(dp.retention))

    def _destroy(self, slug: str) -> None:
        entry = self._subscriptions.pop(slug, None)
        if entry is not None and entry.handle is not None:
            self._node.destroy_subscription(entry.handle)
        self.backlog.remove(slug)

    # --- sampling: runs on the executor thread, as a subscription callback -

    def _on_message(self, slug: str, msg) -> None:
        # The capture instant, read before any conversion work — this is the
        # bridge capture time the wire protocol promises.
        timestamp_ms = sampling.capture_timestamp_ms()

        entry = self._subscriptions.get(slug)
        if entry is None:
            return  # torn down between message arrival and callback dispatch

        try:
            raw = sampling.extract_value(msg, entry.field)
            value = sampling.value_to_json(raw, entry.field_type_str)
            value = sampling.apply_scale_offset(value, entry.scale, entry.offset)
        except Exception:  # noqa: BLE001 - one malformed sample must not kill the subscription
            log.exception("Could not convert a sample for slug %r", slug)
            return

        if not entry.rate.should_send(value, time.monotonic()):
            return

        sample = sampling.Sample(slug, value, timestamp_ms)
        if self._connected and entry.cap is not None and not entry.cap.should_send(
            value, time.monotonic()
        ):
            # Low-bandwidth mode: the configured rate said yes, the cap says
            # not now. Kept for backfill where the datapoint buffers, so the
            # history fills in once the link recovers — the mode spares the
            # link, not the record. Only while connected: disconnected, the
            # branches below already decide, and capping a sample nothing is
            # sending would only thin the backlog.
            if entry.buffer_enabled:
                self.backlog.push(slug, sample)
            return
        if self._connected:
            # Live: someone is actually connected to receive it right now.
            self.samples.put_threadsafe(self._loop, sample)
        elif entry.buffer_enabled:
            # Disconnected and buffered: held for backfill on reconnect.
            self.backlog.push(slug, sample)
        # else: disconnected and unbuffered — dropped. The gap is the
        # honest answer for this configuration, not a bug.

    # --- jobs orphaned by a config change --------------------------

    def _settle_orphaned_job(self, slug: str, reason: str) -> None:
        """Settles whatever job is running on `slug` as `lost`, before the
        ROS client that was running it gets torn down. Without this, `_destroy_action`/`_destroy_service`
        silently orphan it: `ActionClient.destroy()` (and
        `node.destroy_client()`) stop the pending result callback from
        ever firing again, so no terminal `job_update` is ever emitted.
        `JobManager._by_slug` is only cleared by `finish()`, only reached
        from `mark_delivered()` — with no terminal update to deliver, it
        keeps naming this job forever: the slug refuses every future
        invoke as `busy`, `hello.active_jobs` reports it `running` after
        every reconnect (and the cloud's reconciliation deliberately
        leaves a named-and-running entry alone), and the job permanently
        holds one of `MAX_TRACKED_JOBS`'s slots — 200 configuration edits
        over the life of a process, on slugs nobody watches closely
        enough to notice, and the robot refuses all work.

        `lost`, not `cancelled`: destroying the client does not itself
        cancel a goal already accepted by the server (`ActionClient.
        destroy()` only stops listening for its outcome), so the bridge
        genuinely does not know whether the robot is still executing it —
        the same honesty `bridgeJobLost` already states for a dropped
        tracker, not the stronger, unverified claim `cancelled` would be.

        A no-op when nothing is running on `slug` — most config-apply
        destroys are of an idle slug, and this must not invent a job that
        was never there. `_active_goals`/`_goal_deadlines`/`_pending_cancels`
        are cleared too (action-only; harmless no-ops for a service's
        job_id) so the goal-timeout watchdog cannot later emit a second,
        contradictory update for a job already settled here, and a cancel
        remembered by that path cannot be applied to a goal handle that will
        now never arrive. `_service_deadlines` (2n) is cleared the same way,
        service-only this time — otherwise `_check_service_timeouts` could
        find this job_id's deadline still there after the slug's service
        was destroyed and emit a second, contradictory `service_timeout`
        for a job this method already settled as `lost`. `destroy_client`
        already stops the response callback from ever firing (same
        reasoning two paragraphs up for `ActionClient.destroy`), so there
        is no future left to hand to `remove_pending_request` here — only
        this dict entry needs clearing.

        **Also a no-op when the job already reached a terminal state**
        (found the same day this method shipped, going back into
        this file after being asked what was left unfiled). `running_job_id
        (slug)` answers "not yet delivered", not "still running" — the bridge
        keeps a job named here until `mark_delivered()` runs, on purpose,
        so a job that already `succeeded` but has not been sent yet is
        still found by this method exactly as if it were genuinely
        in-flight. Emitting `lost` for it would not merely be a vaguer
        second word (the goal_timeout-then-lost case is deliberately fine,
        see the test asserting that sequence) — it would silently
        replace a **true, already-established** outcome with a false one.
        Reproduced directly, three times over one job, before this guard
        existed: a real action ran to genuine completion, `succeeded` sat
        undelivered, and two later config changes touching its slug each
        overwrote it with `lost`/`config_changed`. `lost` must mean "this
        was running and I no longer know what happened to it", never
        "this slug's outcome merely has not reached the wire yet".

        One thing this guard is honest about rather than silent on: the
        reproduction above was driven by sequential calls in a test, not
        by the two threads that would produce this for real —
        `_apply_actions`/`_apply_services` run on the executor thread,
        `mark_delivered` runs on the asyncio thread right after `ws.send()`
        succeeds with no `await` between them, so in production the window
        is at most a few bytecodes wide under the GIL. Nobody has
        demonstrated it firing there. The guard exists because the check
        was simply absent, not because the race was measured — both facts
        belong here so neither gets overclaimed later."""
        job_id = self.jobs.running_job_id(slug)
        if job_id is None:
            return
        if self.jobs.state_of(job_id) != "running":
            # Already terminal (or, if state_of returned None, already
            # delivered out from under us between the two calls above) —
            # someone else has already said, or is about to say, the true
            # last word about this job. Nothing here may say a different
            # one.
            return
        self._active_goals.pop(job_id, None)
        self._goal_deadlines.pop(job_id, None)
        self._pending_cancels.discard(job_id)
        self._service_deadlines.pop(job_id, None)
        self._emit_job(job_id, slug, "lost", error=("config_changed", reason))

    # --- actions: apply, runs on the executor thread ------------------------

    def _apply_actions(
        self, actions: Mapping[str, ActionConfig], messages: Mapping[str, Any]
    ) -> List[ApplyError]:
        wanted = actions
        errors: List[ApplyError] = []

        # Same rule as datapoints: a retargeted or removed slug loses its old
        # ActionClient first, so a retarget never briefly holds two.
        for slug in list(self._actions):
            if slug not in wanted:
                self._settle_orphaned_job(
                    slug, "the action configured for slug {!r} was removed".format(slug),
                )
                self._destroy_action(slug)
                continue
            existing = self._actions[slug]
            new = wanted[slug]
            if existing.ros_name != new.ros_name or existing.type_name != new.type:
                self._settle_orphaned_job(
                    slug,
                    "the action configured for slug {!r} was retargeted (ros_name or "
                    "type changed)".format(slug),
                )
                self._destroy_action(slug)

        for slug, cfg in wanted.items():
            try:
                if slug in self._actions:
                    # ros_name/type unchanged (else destroyed above) — only
                    # the message template and the declared parameters can
                    # have moved, and they only need re-validating, not a
                    # new client.
                    entry = self._actions[slug]
                    params.validate_template(
                        entry.action_class.Goal, cfg.message, cfg.parameters, shared=messages,
                    )
                    entry.message = cfg.message
                    entry.parameters = cfg.parameters
                    entry.shared_messages = messages
                else:
                    self._create_action(slug, cfg, messages)
            except Exception as exc:  # noqa: BLE001 - one bad slug, not the whole apply
                errors.append(ApplyError(
                    slug=slug, kind=APPLY_ERROR_KIND_ACTION,
                    code=APPLY_ERROR_CODE_UNKNOWN, message=str(exc),
                ))
        return errors

    def _create_action(
        self, slug: str, cfg: ActionConfig, messages: Mapping[str, Any]
    ) -> None:
        try:
            action_class = get_action(cfg.type)
        except Exception as exc:
            raise RuntimeError("unknown action type {!r}: {}".format(cfg.type, exc)) from exc

        # A Goal template that could never build this action's Goal is this
        # slug's error immediately — not a surprise the first time someone
        # invokes it (structural checks are the bridge's).
        params.validate_template(
            action_class.Goal, cfg.message, cfg.parameters, shared=messages,
        )

        client = ActionClient(self._node, action_class, cfg.ros_name)
        self._actions[slug] = _ActionEntry(
            ros_name=cfg.ros_name,
            type_name=cfg.type,
            action_class=action_class,
            message=cfg.message,
            parameters=cfg.parameters,
            shared_messages=messages,
            client=client,
        )

    def _destroy_action(self, slug: str) -> None:
        entry = self._actions.pop(slug, None)
        if entry is not None and entry.client is not None:
            entry.client.destroy()

    # --- commands: kicked off on the executor thread, finish via callbacks -

    def _invoke(self, job_id: str, slug: str, params_dict: dict, patience_ms: int) -> None:
        """Dispatches `slug` to whichever kind configured it — an action's
        goal lifecycle or a service's single call, both funnelled into the
        same job (only the REST/realtime layer above the bridge tells the
        two apart). An unmatched slug is `failed` rather
        than silently doing nothing — the cloud does not normally send one
        (permissions and existence are checked before the bridge ever sees a
        slug), but a config that changed underneath a stale cloud view must
        not go quiet.

        `patience_ms` bounds different things on the two paths: for an
        action it is goal-acceptance only (the run itself is unbounded, by
        design: an action runs to completion regardless of
        connection state); for a service it is the *entire* call (2n), since
        a service response has no accept/execute split to bound separately.

        Admission-checked first: this is the only place every
        `invoke` passes through regardless of kind, so it is where the
        bound on `self.jobs.tracked_count()` (`MAX_TRACKED_JOBS`) has to
        live. `_invoke` only ever runs while a session is open — `invoke`
        is dispatched from client.py's receive loop, which only exists for
        the life of a connected socket — so this bound can only ever bite
        while connected; it cannot fire against an offline bridge, because
        an offline bridge receives no invokes to admit or refuse."""
        queued = self.jobs.tracked_count()
        if queued >= self._max_tracked_jobs:
            self._emit_job(
                job_id,
                slug,
                "failed",
                error=(
                    "job_queue_full",
                    "{} jobs are already queued for report; refusing until some are "
                    "delivered".format(self._max_tracked_jobs),
                ),
                # Structured, not just formatted into the message:
                # `jobQueueFullDetails` is the
                # documented payload for this code, and a refusal that only
                # carries prose forces every consumer to parse it back out —
                # which is exactly what happened the first time (three repos
                # agreeing about a shape none of them actually exchanged).
                details={"limit": self._max_tracked_jobs, "queued": queued},
            )
            return
        if slug in self._actions:
            self._invoke_action(job_id, slug, params_dict, patience_ms)
            return
        if slug in self._services:
            self._invoke_service(job_id, slug, params_dict, patience_ms)
            return
        self._emit_job(
            job_id,
            slug,
            "failed",
            error=("unknown_slug", "no action or service configured for slug {!r}".format(slug)),
        )

    def _invoke_action(self, job_id: str, slug: str, params_dict: dict, patience_ms: int) -> None:
        existing_job_id = self.jobs.running_job_id(slug)
        if existing_job_id is not None:
            if existing_job_id == job_id:
                return  # a redelivered invoke for the job already in flight
            # The cloud enforces busy-per-slug before ever sending this;
            # this is the defensive fallback for a race it lost.
            self._emit_job(
                job_id,
                slug,
                "failed",
                error=("busy", "slug {!r} already has a running job".format(slug)),
            )
            return

        entry = self._actions[slug]
        try:
            goal_msg = params.build_from_template(
                entry.action_class.Goal,
                entry.message,
                entry.parameters,
                params_dict,
                shared=entry.shared_messages,
            )
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            self._emit_job(job_id, slug, "failed", error=("parameter_invalid", str(exc)))
            return

        self.jobs.start(job_id, slug, "action")
        # Armed before the goal is even sent: a server that never answers
        # must not wedge this slug forever. Cleared in
        # `_on_goal_response` the moment a real answer arrives, whichever
        # side wins the race with `_check_goal_timeouts`. `patience_ms`
        # is this call's own deadline — required on every invoke
        # frame, so there is no constant left to fall back to.
        patience_s = patience_ms / 1000.0
        self._goal_deadlines[job_id] = (slug, time.monotonic() + patience_s, patience_s)

        def feedback_callback(feedback_msg, job_id=job_id, slug=slug):
            self._on_action_feedback(job_id, slug, feedback_msg.feedback)

        send_future = entry.client.send_goal_async(goal_msg, feedback_callback=feedback_callback)
        send_future.add_done_callback(
            lambda fut, job_id=job_id, slug=slug: self._on_goal_response(job_id, slug, fut)
        )

    def _cancel_job(self, slug: str, job_id: Optional[str]) -> None:
        running_job_id = self.jobs.running_job_id(slug)
        if running_job_id is None:
            log.info("Cancel for slug %r: nothing running, silent no-op", slug)
            return  # nothing running for this slug
        if job_id is not None and job_id != running_job_id:
            # A caller who named an id has ruled out "whatever is running"
            # as the answer — falling back to the slug would cancel a
            # machine they did not name. Distinguishable from the
            # "nothing running" no-op above on purpose: those two silences
            # used to be the same message, which is how a stale id and an
            # idle slug both read as "cancel did nothing", indistinguishably
            # — exactly the addressing gap this method exists to close.
            log.warning(
                "Cancel for slug %r named job %r, but %r is running — "
                "refusing rather than cancelling the wrong job",
                slug, job_id, running_job_id,
            )
            return
        goal_handle = self._active_goals.get(running_job_id)
        if goal_handle is None:
            if running_job_id in self._goal_deadlines:
                # sent, not yet accepted or rejected — there is no
                # goal handle to cancel *yet*, but there will be one, or a
                # rejection that makes the question moot. Remembered rather
                # than dropped; `_on_goal_response` applies it the instant
                # the window closes either way.
                self._pending_cancels.add(running_job_id)
                log.info(
                    "Cancel for slug %r (job %r): the goal has not been "
                    "accepted yet — remembered, will apply once it is",
                    slug, running_job_id,
                )
                return
            # A service call, or any other kind with no ROS-level cancel —
            # a genuine no-op, not a dropped one; logged anyway so "cancel"
            # is discoverable in the logs for every branch that reaches
            # here, not just two of the three.
            log.info(
                "Cancel for slug %r (job %r): no ROS-level cancel exists for this job",
                slug, running_job_id,
            )
            return
        # The eventual "cancelled" job_update comes from the ordinary result
        # callback once the server confirms (STATUS_CANCELED) — this call
        # only has to ask, not report; that keeps there being exactly one
        # place a job's terminal state is decided.
        goal_handle.cancel_goal_async()

    # --- action goal lifecycle: every callback below runs on the executor --

    def _on_goal_response(self, job_id: str, slug: str, future) -> None:
        # Popped unconditionally: whatever this response turns out to be,
        # the deadline it was guarding against is resolved one way or
        # another. `late` means `_check_goal_timeouts` already got there
        # first and reported `goal_timeout` — this job_id has already been
        # told "never started" to the cloud, so a further "rejected"/
        # "running" of the ordinary kind would only contradict that; an
        # acceptance instead gets the special handling below.
        self._goal_deadlines.pop(job_id, None)
        late = job_id in self._timed_out_job_ids
        # resolved here regardless of outcome — a cancel that
        # arrived before acceptance is only actionable once we know
        # whether there is a goal to act on at all. `late`'s own automatic
        # corrective cancel below already covers that path unconditionally,
        # so this only has to be *applied* in the ordinary accepted branch.
        had_pending_cancel = job_id in self._pending_cancels
        self._pending_cancels.discard(job_id)

        try:
            goal_handle = future.result()
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            if not late:
                self._emit_job(job_id, slug, "failed", error=("goal_send_failed", str(exc)))
            return
        if not goal_handle.accepted:
            if not late:
                self._emit_job(
                    job_id, slug, "failed", error=("goal_rejected", "the action server rejected the goal")
                )
            return

        if late:
            # Accepted after the cloud was already told this job never
            # started. The robot is about to execute a goal the platform
            # does not know exists — stop it if at all possible.
            log.error(
                "Job %r (slug %r) was accepted after its goal_timeout had "
                "already been reported — asking the server to cancel it",
                job_id, slug,
            )
            cancel_future = goal_handle.cancel_goal_async()
            cancel_future.add_done_callback(
                lambda fut, job_id=job_id, slug=slug: self._on_late_goal_cancel(job_id, slug, fut)
            )
            return

        self._active_goals[job_id] = goal_handle
        self._emit_job(job_id, slug, "running")
        if had_pending_cancel:
            # The cancel that found no goal handle yet — the goal
            # exists now, so the caller's original request is honoured the
            # instant it can be. The eventual "cancelled" job_update still
            # comes from the ordinary result callback below, exactly as it
            # would have if this cancel had arrived a moment later.
            log.info(
                "Job %r (slug %r): applying the cancel that arrived before "
                "this goal was accepted",
                job_id, slug,
            )
            goal_handle.cancel_goal_async()

        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(
            lambda fut, job_id=job_id, slug=slug: self._on_action_result(job_id, slug, fut)
        )

    def _on_late_goal_cancel(self, job_id: str, slug: str, future) -> None:
        """Whether the corrective cancel `_on_goal_response` issued for a
        late acceptance actually worked. A confirmed cancel needs nothing
        further said — the cloud's `goal_timeout` stands, and the goal
        really is stopping, so it was even honestly optimistic. A cancel
        that failed or was refused leaves the goal running with nothing
        left able to stop it: worse than `goal_timeout`, which at least
        implied nothing was moving, so the cloud is told `lost` instead —
        a second, corrective job_update for a job_id it already believes
        closed (`lost` is a valid `job_update.state` on the wire)."""
        try:
            response = future.result()
        except Exception:  # noqa: BLE001 - reported to the caller, not raised here
            log.error(
                "Job %r (slug %r): the corrective cancel for a late-accepted "
                "goal itself failed — the robot may still be executing a "
                "goal the platform believes never started",
                job_id, slug, exc_info=True,
            )
            self._emit_job(
                job_id, slug, "lost",
                error=("goal_uncontrollable", "a goal accepted after its timeout could not be cancelled"),
            )
            return
        if not response.goals_canceling:
            log.error(
                "Job %r (slug %r): the action server refused to cancel a "
                "goal accepted after its timeout — the robot may still be "
                "executing a goal the platform believes never started",
                job_id, slug,
            )
            self._emit_job(
                job_id, slug, "lost",
                error=("goal_uncontrollable", "a goal accepted after its timeout could not be cancelled"),
            )
            return
        log.warning(
            "Job %r (slug %r): a goal accepted after its goal_timeout was "
            "already reported has been cancelled",
            job_id, slug,
        )

    def _on_action_feedback(self, job_id: str, slug: str, feedback_msg) -> None:
        if job_id in self._timed_out_job_ids:
            # This goal was accepted after the cloud was already told
            # `goal_timeout` (or, if the corrective cancel was refused,
            # `lost`) — its `feedback_callback` was registered before that
            # was known and rclpy keeps invoking it regardless. Reporting
            # "running" now would contradict what the cloud was already
            # told, for a job it may believe closed.
            return
        timestamp_ms = sampling.capture_timestamp_ms()
        self._emit_job(
            job_id,
            slug,
            "running",
            feedback=sampling.message_to_json(feedback_msg),
            progress=self._extract_progress(feedback_msg),
            timestamp_ms=timestamp_ms,
        )

    @staticmethod
    def _extract_progress(feedback_msg) -> Optional[float]:
        """Best-effort only: a feedback message with a top-level `progress`
        field that is a number in `0..1` is forwarded as the job's progress;
        anything else — no such field, wrong type, out of range — is `None`.
        Never clamped or rescaled: `bridgeJobUpdate.progress` is contractually
        `0..1`, and a great many ROS feedback messages use `progress` for a
        percentage or a raw count instead, so a guessed unit would silently
        corrupt the field (or, worse, get the whole frame refused by the
        cloud's schema check, losing the feedback along with it)."""
        fields = type(feedback_msg).get_fields_and_field_types()
        if "progress" not in fields:
            return None
        value = getattr(feedback_msg, "progress", None)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        if not (0.0 <= value <= 1.0):
            return None
        return float(value)

    def _on_action_result(self, job_id: str, slug: str, future) -> None:
        self._active_goals.pop(job_id, None)
        try:
            response = future.result()
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            self._emit_job(job_id, slug, "failed", error=("result_failed", str(exc)))
            return

        result_json = sampling.message_to_json(response.result)
        if response.status == GoalStatus.STATUS_SUCCEEDED:
            self._emit_job(job_id, slug, "succeeded", result=result_json)
        elif response.status == GoalStatus.STATUS_CANCELED:
            self._emit_job(job_id, slug, "cancelled", result=result_json)
        else:
            self._emit_job(
                job_id,
                slug,
                "failed",
                result=result_json,
                error=("action_failed", "the action ended with status {}".format(response.status)),
            )

    # --- services: apply, runs on the executor thread ------------------------

    def _apply_services(
        self, services: Mapping[str, ServiceConfig], messages: Mapping[str, Any]
    ) -> List[ApplyError]:
        wanted = services
        errors: List[ApplyError] = []

        for slug in list(self._services):
            if slug not in wanted:
                self._settle_orphaned_job(
                    slug, "the service configured for slug {!r} was removed".format(slug),
                )
                self._destroy_service(slug)
                continue
            existing = self._services[slug]
            new = wanted[slug]
            if existing.ros_name != new.ros_name or existing.type_name != new.type:
                self._settle_orphaned_job(
                    slug,
                    "the service configured for slug {!r} was retargeted (ros_name or "
                    "type changed)".format(slug),
                )
                self._destroy_service(slug)

        for slug, cfg in wanted.items():
            try:
                if slug in self._services:
                    entry = self._services[slug]
                    params.validate_template(
                        entry.service_class.Request, cfg.message, cfg.parameters, shared=messages,
                    )
                    entry.message = cfg.message
                    entry.parameters = cfg.parameters
                    entry.shared_messages = messages
                else:
                    self._create_service(slug, cfg, messages)
            except Exception as exc:  # noqa: BLE001 - one bad slug, not the whole apply
                errors.append(ApplyError(
                    slug=slug, kind=APPLY_ERROR_KIND_SERVICE,
                    code=APPLY_ERROR_CODE_UNKNOWN, message=str(exc),
                ))
        return errors

    def _create_service(
        self, slug: str, cfg: ServiceConfig, messages: Mapping[str, Any]
    ) -> None:
        try:
            service_class = get_service(cfg.type)
        except Exception as exc:
            raise RuntimeError("unknown service type {!r}: {}".format(cfg.type, exc)) from exc

        # A Request template that could never build this service's Request
        # is this slug's error immediately, same reasoning as an action's
        # Goal.
        params.validate_template(
            service_class.Request, cfg.message, cfg.parameters, shared=messages,
        )

        client = self._node.create_client(service_class, cfg.ros_name)
        self._services[slug] = _ServiceEntry(
            ros_name=cfg.ros_name,
            type_name=cfg.type,
            service_class=service_class,
            message=cfg.message,
            parameters=cfg.parameters,
            shared_messages=messages,
            client=client,
        )

    def _destroy_service(self, slug: str) -> None:
        entry = self._services.pop(slug, None)
        if entry is not None and entry.client is not None:
            self._node.destroy_client(entry.client)

    def _invoke_service(self, job_id: str, slug: str, params_dict: dict, patience_ms: int) -> None:
        existing_job_id = self.jobs.running_job_id(slug)
        if existing_job_id is not None:
            if existing_job_id == job_id:
                return  # a redelivered invoke for the job already in flight
            self._emit_job(
                job_id,
                slug,
                "failed",
                error=("busy", "slug {!r} already has a running job".format(slug)),
            )
            return

        entry = self._services[slug]
        if not entry.client.service_is_ready():
            # A non-blocking check, deliberately: waiting here would stall
            # the one executor thread every other slug also depends on.
            self._emit_job(
                job_id, slug, "failed", error=("service_unavailable", "the ROS service is not available")
            )
            return

        try:
            request = params.build_from_template(
                entry.service_class.Request,
                entry.message,
                entry.parameters,
                params_dict,
                shared=entry.shared_messages,
            )
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            self._emit_job(job_id, slug, "failed", error=("parameter_invalid", str(exc)))
            return

        self.jobs.start(job_id, slug, "service")
        self._emit_job(job_id, slug, "running")

        call_future = entry.client.call_async(request)
        # Armed before the response can possibly arrive, same reasoning as
        # `_invoke_action`'s `_goal_deadlines`: a service that never
        # answers must not wedge this slug forever (2n). Cleared in
        # `_on_service_result` the moment a real answer arrives, whichever
        # side wins the race with `_check_service_timeouts`.
        patience_s = patience_ms / 1000.0
        self._service_deadlines[job_id] = (slug, time.monotonic() + patience_s, patience_s, call_future)
        call_future.add_done_callback(
            lambda fut, job_id=job_id, slug=slug: self._on_service_result(job_id, slug, fut)
        )

    def _on_service_result(self, job_id: str, slug: str, future) -> None:
        # Popped unconditionally, mirroring `_on_goal_response` — whatever
        # this callback turns out to be, the deadline it was guarding is
        # resolved. Unlike the action path there is no "late" branch to
        # consider here: `_check_service_timeouts` only ever declares a
        # timeout after calling `remove_pending_request`, which rclpy
        # documents as preventing this very callback from running at all —
        # so if this method is running, the timeout path did not already
        # fire for this job_id, and popping is pure bookkeeping, not a race
        # to adjudicate.
        self._service_deadlines.pop(job_id, None)
        try:
            response = future.result()
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            self._emit_job(job_id, slug, "failed", error=("result_failed", str(exc)))
            return
        # A service call has no notion of feedback or progress, and no
        # platform-level "failed" outcome — a response-shaped `success` field
        # (e.g. std_srvs/Trigger) is domain data the caller reads themselves,
        # not something the bridge reinterprets as a job state.
        self._emit_job(job_id, slug, "succeeded", result=sampling.message_to_json(response))

    # --- publishers: apply + publish, run on the executor thread -------------

    def _apply_publishers(
        self, publishers: Mapping[str, PublisherConfig], messages: Mapping[str, Any]
    ) -> List[ApplyError]:
        wanted = publishers
        errors: List[ApplyError] = []

        for slug in list(self._publishers):
            if slug not in wanted:
                self._destroy_publisher(slug)
                continue
            existing = self._publishers[slug]
            new = wanted[slug]
            if existing.topic != new.topic or existing.type_name != new.type:
                self._destroy_publisher(slug)

        for slug, cfg in wanted.items():
            try:
                if slug in self._publishers:
                    entry = self._publishers[slug]
                    params.validate_template(
                        entry.message_class, cfg.message, cfg.parameters, shared=messages,
                    )
                    failsafe_body = params.resolve_failsafe_body(
                        entry.message_class, cfg.failsafe.message, shared=messages,
                    )
                    entry.message = cfg.message
                    entry.parameters = cfg.parameters
                    entry.shared_messages = messages
                    entry.failsafe_body = failsafe_body
                    entry.timeout_ms = cfg.failsafe.timeout_ms
                    entry.quiet_timeout_ms = cfg.quiet_timeout_ms
                else:
                    self._create_publisher(slug, cfg, messages)
            except Exception as exc:  # noqa: BLE001 - one bad slug, not the whole apply
                errors.append(ApplyError(
                    slug=slug, kind=APPLY_ERROR_KIND_PUBLISHER,
                    code=APPLY_ERROR_CODE_UNKNOWN, message=str(exc),
                ))
        return errors

    def _create_publisher(
        self, slug: str, cfg: PublisherConfig, messages: Mapping[str, Any]
    ) -> None:
        try:
            message_class = get_message(cfg.type)
        except Exception as exc:
            raise RuntimeError("unknown message type {!r}: {}".format(cfg.type, exc)) from exc

        # Both structural checks the bridge owns: the publish
        # template must be able to build this message type at all, and the
        # failsafe must resolve to a placeholder-free body that builds —
        # caught here, not the first time the timer needs to fire it, which
        # is the worst possible moment to discover it does not.
        params.validate_template(message_class, cfg.message, cfg.parameters, shared=messages)
        failsafe_body = params.resolve_failsafe_body(
            message_class, cfg.failsafe.message, shared=messages,
        )

        self._release_topic_for_type(cfg.topic, cfg.type)
        handle = self._node.create_publisher(message_class, cfg.topic, 10)
        self._publishers[slug] = _PublisherEntry(
            topic=cfg.topic,
            type_name=cfg.type,
            message_class=message_class,
            message=cfg.message,
            parameters=cfg.parameters,
            shared_messages=messages,
            failsafe_body=failsafe_body,
            timeout_ms=cfg.failsafe.timeout_ms,
            quiet_timeout_ms=cfg.quiet_timeout_ms,
            handle=handle,
        )

    def _destroy_publisher(self, slug: str) -> None:
        """Tears down a publisher — on removal, on a retarget (which is a
        destroy followed by a create under a new topic/type), and as part of
        `RosRuntime.stop()` shutting every publisher down. All three funnel
        through here, which is deliberate: an *armed* publisher (one that
        has actually been driven and has not already fired) is about to
        lose the only thing that could ever stop it — once `handle` is
        destroyed, nothing will ever publish to that topic again, so a
        client that has already gone quiet leaves the robot holding its
        last command forever, silently, with no failsafe to catch it (a
        config change or a shutdown mid-motion is
        ordinary use, not an edge case, and has no platform e-stop to
        fall back on). One last failsafe publish before teardown closes
        that hole for all three callers at once. A publisher that was never
        used gets no such parting shot — nothing was promised, so there is
        nothing to make good on."""
        entry = self._publishers.pop(slug, None)
        if entry is None or entry.handle is None:
            return
        if entry.last_activity_at is not None:
            if not entry._failsafe_sent:
                self._fire_failsafe(slug, entry)
            # Not destroyed yet, whether the failsafe fired just now or the
            # watchdog fired it a moment ago: either can still be unacknowledged
            # (PARTING_FAILSAFE_ACK_TIMEOUT_S). Kept off the executor thread's
            # back, too -- blocking here would hold up every other publisher's
            # watchdog for as long as the wait.
            self._retiring_publishers.append(_RetiringPublisher(
                slug=slug,
                topic=entry.topic,
                type_name=entry.type_name,
                handle=entry.handle,
                deadline=time.monotonic() + PARTING_FAILSAFE_ACK_TIMEOUT_S,
            ))
            return
        self._node.destroy_publisher(entry.handle)

    def _release_topic_for_type(self, topic: str, type_name: str) -> None:
        """Destroys every retiring handle on `topic` whose type differs from
        `type_name`, after at most `TYPE_CHANGE_ACK_WAIT_S` for its last
        failsafe to be acknowledged -- otherwise the new publisher could not
        be created at all."""
        keep: List[_RetiringPublisher] = []
        for retiring in self._retiring_publishers:
            if retiring.topic != topic or retiring.type_name == type_name:
                keep.append(retiring)
                continue
            wait_s = min(max(retiring.deadline - time.monotonic(), 0.0), TYPE_CHANGE_ACK_WAIT_S)
            try:
                acked = retiring.handle.wait_for_all_acked(Duration(nanoseconds=int(wait_s * 1e9)))
            except Exception:  # noqa: BLE001 - the new publisher still has to be created
                log.exception("Publisher slug %r: could not ask whether its failsafe was acknowledged", retiring.slug)
                acked = True
            if not acked:
                log.warning(
                    "Publisher slug %r: its last failsafe was not acknowledged within %.1f s; "
                    "destroying it so %s can take %s",
                    retiring.slug,
                    wait_s,
                    type_name,
                    topic,
                )
            try:
                self._node.destroy_publisher(retiring.handle)
            except Exception:  # noqa: BLE001 - creation reports its own failure if this left the topic taken
                log.exception("Publisher slug %r: its retired handle could not be destroyed", retiring.slug)
        self._retiring_publishers = keep

    def _reap_retiring_publishers(self, block: bool = False) -> None:
        """Destroys every retiring publisher whose last failsafe has been
        acknowledged, or whose time is up. From the watchdog tick it only
        looks; from shutdown (`block=True`) it waits out each deadline. One
        handle that fails to answer or to go does not stop the others."""
        if not self._retiring_publishers:
            return
        keep: List[_RetiringPublisher] = []
        for retiring in self._retiring_publishers:
            remaining = retiring.deadline - time.monotonic()
            wait_s = max(remaining, 0.0) if block else 0.0
            try:
                acked = retiring.handle.wait_for_all_acked(Duration(nanoseconds=int(wait_s * 1e9)))
            except Exception:  # noqa: BLE001 - an RMW without the call must not keep the handle forever
                log.exception(
                    "Publisher slug %r: could not ask whether its failsafe was acknowledged; "
                    "destroying the publisher now",
                    retiring.slug,
                )
                acked = True  # nothing more to learn by waiting; the exception says what happened
            if not acked and not block and remaining > 0:
                keep.append(retiring)
                continue
            if not acked:
                log.warning(
                    "Publisher slug %r: its last failsafe was not acknowledged within %.1f s; "
                    "destroying the publisher anyway",
                    retiring.slug,
                    PARTING_FAILSAFE_ACK_TIMEOUT_S,
                )
            try:
                self._node.destroy_publisher(retiring.handle)
            except Exception:  # noqa: BLE001 - the next handle still has to go
                log.exception("Publisher slug %r: its retired handle could not be destroyed", retiring.slug)
        self._retiring_publishers = keep

    def _publish(self, slug: str, message_dict: dict) -> None:
        entry = self._publishers.get(slug)
        if entry is None:
            log.warning("publish for unconfigured publisher slug %r ignored", slug)
            return
        try:
            msg = params.build_from_template(
                entry.message_class,
                entry.message,
                entry.parameters,
                message_dict,
                shared=entry.shared_messages,
            )
        except Exception:  # noqa: BLE001 - nothing to report to; log and move on
            log.exception("Could not build a message for publisher slug %r", slug)
            return
        entry.handle.publish(msg)
        # Marks the publisher active — the failsafe timer measures silence
        # from this, not from config-apply time (the format is about a
        # publisher that *was* being driven going quiet, not an unused one)
        # — and re-arms it: a fresh publish means a fresh silence period to
        # watch, so the next one that runs out gets its own failsafe.
        entry.last_activity_at = time.monotonic()
        entry._failsafe_sent = False

    # --- cameras: apply, runs on the executor thread -----

    def _resolve_camera_credentials(
        self, source: CameraSource
    ) -> Optional[camera_sources.Credentials]:
        """`None` for `ros`/`v4l2` (no credential concept); explicit-block-
        beats-URL-userinfo resolution for `rtsp`/`mjpeg`, via the same
        function camera_sources.py's own adapters would otherwise have had to
        duplicate.

        The 3.0 configuration format moved the pair inside the source itself, so there is no
        frame-level map to consult any more and nothing to thread through
        five call sites — the precedence is the same one the named ref had,
        for the same reason (a rotation must not appear to silently keep
        using the URL's old password)."""
        if isinstance(source, (RtspSource, MjpegSource)):
            return camera_sources.resolve_credentials(
                url=source.url, credentials=source.credentials
            )
        return None

    def _build_source_adapter(
        self,
        source: CameraSource,
        resolved_credentials: Optional[camera_sources.Credentials],
        on_frame: camera_sources.OnFrame,
        on_error: camera_sources.OnError,
        *,
        raw: camera.RawFrameHolder,
        conversion_wanted: Callable[[], bool],
        on_raw_frame: Callable[[int], None],
    ) -> camera_sources.CameraSourceAdapter:
        """The one place that turns a `CameraSource` into a running
        adapter. Everything below this — `_on_source_frame`, the snapshot
        watchdog, `start_live`'s backoff — takes only `(slug, bgr,
        timestamp_ms)` or a slug's `_CameraEntry`, never a source kind. If a
        future adapter needs a downstream special case, the seam moved to
        the wrong place.

        `raw`/`conversion_wanted`/`on_raw_frame` are `_RosSourceAdapter`-only
        (the decode-on-demand gate is scoped to `kind: 'ros'`, see
        its own docstring) — accepted here unconditionally anyway, from
        `_create_camera`, which builds all three regardless of source kind,
        so this stays a uniform call for all four rather than special-casing
        one of them at the call site too."""
        if isinstance(source, RosSource):
            return _RosSourceAdapter(
                node=self._node, topic=source.topic, type_name=source.type,
                on_frame=on_frame, on_error=on_error,
                raw=raw, conversion_wanted=conversion_wanted, on_raw_frame=on_raw_frame,
            )
        if isinstance(source, RtspSource):
            return camera_sources.RtspSourceAdapter(
                url=source.url, transport=source.transport, credentials=resolved_credentials,
                on_frame=on_frame, on_error=on_error,
            )
        if isinstance(source, MjpegSource):
            return camera_sources.MjpegSourceAdapter(
                url=source.url, credentials=resolved_credentials,
                on_frame=on_frame, on_error=on_error,
            )
        if isinstance(source, V4l2Source):
            return camera_sources.V4l2SourceAdapter(
                device=source.device, on_frame=on_frame, on_error=on_error,
            )
        raise RuntimeError("unknown camera source kind: {!r}".format(source))  # pragma: no cover

    def _apply_cameras(
        self, cameras: Mapping[str, CameraConfig]
    ) -> Tuple[List[ApplyError], List[str]]:
        """Diffs `cameras` against the running adapters — the same
        slug-addressed shape `_apply_config` uses for datapoints. Returns
        `(errors, changed_live_slugs)`: the second list is the live-stream
        half of this method — every slug whose configuration changed in *any*
        field, source/credentials included, which `apply_cameras` (the
        async wrapper) then stops live for once this returns. A source or
        credentials change also needs a whole new adapter (below); a
        resolution/fps/bitrate/interval-only change updates the existing
        one in place but still needs live stopped, since those values are
        baked into the running publisher, not read from `_CameraEntry` on
        each frame.

        Recreating the adapter (when the source itself changed) happens
        here, before `apply_cameras` gets a chance to call `stop_live` —
        so for a source/credentials change specifically, a live publisher
        that is about to be stopped briefly keeps reading an already-
        abandoned `LatestFrameHolder` for the last few frames rather than
        the new one. That is a strictly smaller version of the exact bug
        this guard exists to close (frozen forever vs. frozen for a few
        milliseconds before a clean stop), and closing it fully would cost
        a second cross-thread round trip for every camera apply to save
        nothing any caller can observe."""
        wanted = cameras
        errors: List[ApplyError] = []
        changed_live_slugs: List[str] = []

        for slug in list(self._cameras):
            if slug not in wanted:
                self._destroy_camera(slug)
                changed_live_slugs.append(slug)
                # A slug leaving the config entirely, not merely being
                # retargeted — `_camera_last_reported_error` deliberately
                # survives a retarget (see `_on_source_frame`'s comment)
                # but must not survive the slug itself, or a later,
                # entirely unrelated camera reusing this slug would open
                # with a stale "recovered" report for a problem it never
                # had.
                self._camera_last_reported_error.pop(slug, None)
                continue
            existing = self._cameras[slug]
            new_cfg = wanted[slug]
            new_credentials = self._resolve_camera_credentials(new_cfg.source)
            source_or_credentials_changed = (
                existing.source != new_cfg.source or existing.credentials != new_credentials
            )
            if (
                source_or_credentials_changed
                or existing.width != new_cfg.width
                or existing.height != new_cfg.height
                or existing.fps != new_cfg.fps
                or existing.bitrate_kbps != new_cfg.bitrate_kbps
                or existing.snapshot_interval_seconds != new_cfg.snapshot_interval_seconds
            ):
                changed_live_slugs.append(slug)
            if source_or_credentials_changed:
                # `_camera_last_reported_error` surviving a rebuild is
                # correct for a same-kind credential fix (see
                # `_on_source_frame`'s own comment) but
                # wrong across a *kind* change — `source_auth_failed`
                # restated for a ROS topic asserts a cause that cannot
                # exist there, ROS has no credentials at all. It also
                # re-poisons the cloud's own fix: the cloud
                # clears a camera's health entry the instant its published
                # source changes, and a stale kind-mismatched restatement
                # arriving afterwards would put the wrong belief straight
                # back. Same treatment as a full removal — see the `slug
                # not in wanted` branch above.
                if type(existing.source) is not type(new_cfg.source):
                    self._camera_last_reported_error.pop(slug, None)
                self._destroy_camera(slug)

        for slug, cfg in wanted.items():
            try:
                if slug in self._cameras:
                    # Source and credentials are unchanged (else destroyed
                    # above) — only resolution/fps/bitrate/interval can
                    # have moved, and none of those need a new adapter.
                    self._update_camera(slug, cfg)
                else:
                    self._create_camera(slug, cfg)
            except Exception as exc:  # noqa: BLE001 - one bad slug, not the whole apply
                # No field-path classification here: a camera has no
                # `parameters`/`field` to resolve, and camera.py's own
                # config-apply-time type check already rules out the one
                # other candidate (an unsupported image encoding) before
                # this ever reaches a live frame — see camera.py:38. Every
                # exception this loop actually sees today is `unknown`.
                errors.append(ApplyError(
                    slug=slug, kind=APPLY_ERROR_KIND_CAMERA,
                    code=APPLY_ERROR_CODE_UNKNOWN, message=str(exc),
                ))
        return errors, changed_live_slugs

    def _update_camera(self, slug: str, cfg: CameraConfig) -> None:
        entry = self._cameras[slug]
        entry.width = cfg.width
        entry.height = cfg.height
        entry.fps = cfg.fps
        entry.bitrate_kbps = cfg.bitrate_kbps
        entry.snapshot_interval_seconds = cfg.snapshot_interval_seconds
        entry.rate = sampling.MaxHzPolicy(cfg.fps)

    def _create_camera(self, slug: str, cfg: CameraConfig) -> None:
        resolved_credentials = self._resolve_camera_credentials(cfg.source)

        # `on_frame`/`on_error` are called from whichever thread the
        # adapter itself runs on — its own dedicated background thread for
        # RTSP/MJPEG/V4L2 (camera_sources.py's `_ThreadedSourceAdapter`),
        # never the ROS executor thread. `_on_source_frame`/
        # `_on_source_error` touch `self._cameras` and its contents with no
        # lock of their own, on the assumption (true everywhere else in
        # this file) that only the executor thread ever does — the same
        # assumption a stale/orphaned adapter thread calling back in late
        # (see `_destroy_camera`) would otherwise violate outright. Route
        # both through `self._enqueue`, exactly like every other
        # cross-thread entry point into this class (`_submit`/
        # `_submit_async`), rather than calling straight in. Fire-and-
        # forget — nothing here needs the resulting Future — so this never
        # blocks the adapter thread either. `_RosSourceAdapter`'s own
        # callback already runs on the executor thread (a real rclpy
        # subscription callback), so this is one extra queue hop for ROS
        # specifically — accepted for a uniform, easy-to-verify rule
        # ("only the executor thread ever touches `self._cameras`") over an
        # unwritten exception for one of the four sources.
        #
        # `entry` (not just `slug`) is captured by both closures and
        # threaded through to `_on_source_frame`/`_on_source_error`, which
        # verify it is still `self._cameras[slug]`'s *current* entry before
        # touching anything (a real defect, found against the
        # actual demo robot, not merely reasoned about). `_close()` is a
        # no-op by design now (see `_CvCaptureAdapter`'s docstring), so an
        # old adapter's own thread is solely responsible for noticing
        # `_stop_event` and exiting — on an established, healthy stream
        # that had been running for minutes, it can take long enough that a
        # *second* rotation lands, a new adapter takes the slug, and the
        # old one is still delivering real frames. Routing by slug alone
        # (the original shape) could not tell that stale delivery apart
        # from the current adapter's — both would be honoring the exact
        # same `self._cameras.get(slug)` lookup. Comparing against the
        # `entry` this closure was built for closes that: a slug survives a
        # retarget (same object, same identity) so an unrelated update is
        # never mistaken for staleness, but a slug's *entry* never does.
        def on_frame(bgr, timestamp_ms):
            self._enqueue(lambda: self._on_source_frame(slug, entry, bgr, timestamp_ms))

        def on_error(code, message):
            self._enqueue(lambda: self._on_source_error(slug, entry, code, message))

        # `_RosSourceAdapter`-only — see `_build_source_adapter`
        # for why these three are built unconditionally regardless of
        # source kind.
        raw = camera.RawFrameHolder()

        def conversion_wanted() -> bool:
            # `True` iff a live publish task for this slug is currently
            # running — read directly, not via `_enqueue`: the ROS
            # subscription callback that calls this already runs on the
            # executor thread, the same thread `self._cameras` is always
            # touched from, so no marshaling is needed just to read a dict.
            #
            # `_live_publishers` itself is mutated on the *loop* thread
            # (`start_live`/`stop_live`, both `_submit_async`-free asyncio
            # methods), not the executor thread this read happens on, so
            # this is a genuinely racy read across threads with no lock. It
            # is accepted deliberately: the two outcomes of losing the race
            # are a frame converted once despite live having just stopped
            # (wasted work, not wrong data) or a frame left raw and picked
            # up a tick later by `_next_snapshot`'s own lazy conversion
            # instead of eagerly here (a snapshot delayed by well under a
            # frame interval, not lost) — never a crash, never stale data
            # served as current. A lock on this hot path would cost every
            # single incoming frame to close a race whose worst case is
            # that cheap.
            return slug in self._live_publishers

        def on_raw_frame(timestamp_ms: int) -> None:
            # Runs synchronously on the executor thread (the ROS
            # subscription callback itself) — unlike on_frame/on_error,
            # which exist to marshal the *other* three source kinds'
            # background threads onto the executor, this needs no
            # `_enqueue` hop. Shares the error-clear with `_on_source_frame`
            # (`_clear_source_error`) so a camera that resumes delivering
            # frames while idle (nobody live, nothing due) is not left
            # marked broken forever just because its frames are staying
            # raw rather than being converted.
            self._clear_source_error(slug, entry, timestamp_ms)

        adapter = self._build_source_adapter(
            cfg.source, resolved_credentials, on_frame, on_error,
            raw=raw, conversion_wanted=conversion_wanted, on_raw_frame=on_raw_frame,
        )
        entry = _CameraEntry(
            source=cfg.source,
            credentials=resolved_credentials,
            width=cfg.width,
            height=cfg.height,
            fps=cfg.fps,
            bitrate_kbps=cfg.bitrate_kbps,
            snapshot_interval_seconds=cfg.snapshot_interval_seconds,
            rate=sampling.MaxHzPolicy(cfg.fps),
            latest=camera.LatestFrameHolder(),
            raw=raw,
            adapter=adapter,
        )
        adapter.start()
        self._cameras[slug] = entry

    def _destroy_camera(self, slug: str) -> Optional[threading.Thread]:
        """Returns the throwaway thread `adapter.stop()` was dispatched to,
        or `None` if there was no entry for `slug`. Almost every caller
        (`_apply_cameras`, mid-session) discards it deliberately — see
        below for why — but `_destroy_all_cameras` (final shutdown only)
        needs it back, to wait for it. Do not merge the two call shapes."""
        entry = self._cameras.pop(slug, None)
        if entry is None:
            return None
        # `adapter.stop()` (camera_sources.py) blocks the calling thread for
        # up to its own internal join timeout, and — its own documented
        # limitation — if the underlying blocking connect call genuinely
        # cannot be interrupted, the real wait can run far longer than that
        # in a thread `stop()` gives up on. This runs on the executor
        # thread here (`_destroy_camera` is only ever called from
        # `_apply_cameras`, itself only ever dispatched via `_submit_async`)
        # — the ONE thread that also serves every other camera, every
        # datapoint, every action/service/publisher, and the safety-
        # critical failsafe timer. Blocking it on one camera's
        # teardown was a real bug: a developer rotating a
        # camera credential to a wrong password could delay the failsafe
        # and everything else sharing this thread for the length of that
        # block, and if the connect call never returns at all, indefinitely
        # — for a whole robot, over one camera's password. `stop()` runs on
        # its own throwaway thread instead, so this method returns
        # immediately regardless of how long the adapter takes to actually
        # stop. A stale adapter's `on_frame`/`on_error` calling back in
        # later is already safe (see `_create_camera`'s docstring): they
        # marshal through `_enqueue` and `_on_source_frame`/
        # `_on_source_error` look the slug's *current* entry up fresh, so
        # an orphaned adapter's late callback is inert, not corrupting.
        #
        # That "discard and move on" is right for a live config apply, and
        # wrong for `RosRuntime.stop()`: the very same fire-and-forget
        # thread means `stop()` can now reach `rclpy.shutdown()` while a
        # camera's adapter thread is still genuinely inside a blocking
        # native call — an interpreter
        # tearing down a daemon thread that is mid-execution inside a C
        # extension is its own, independent way to crash the process, and
        # it reproduced locally (SIGSEGV) doing exactly that: stop a
        # runtime with an actively-streaming RTSP camera still attached.
        # `_destroy_all_cameras` below joins this thread before `stop()`
        # is allowed to proceed — a one-time, final teardown is an
        # acceptable place to wait; a mid-session credential rotation is
        # not, which is the whole reason this method does not wait itself.
        stop_thread = threading.Thread(
            target=entry.adapter.stop, name="camera-stop-{}".format(slug), daemon=True
        )
        stop_thread.start()
        return stop_thread

    # --- cameras: sampling, fed by every adapter alike ---

    def _on_source_frame(self, slug: str, expected_entry: _CameraEntry, bgr: Any, timestamp_ms: int) -> None:
        """The one hand-off every camera source feeds — ROS's own
        `_RosSourceAdapter` and the three from camera_sources.py alike.
        Resize and the `max_hz` rate limit happen here, uniformly, rather
        than per-adapter: nothing downstream of this method can tell which
        kind produced the frame, which is the actual point of the seam.

        `expected_entry` (a real defect found against the demo
        robot) is the `_CameraEntry` `_create_camera` built this adapter
        for — checked against `self._cameras[slug]`'s *current* entry
        before anything else, and dropped silently if they differ. Without
        this, a stale adapter's own frame — one whose `stop()` was called
        but whose thread has not yet noticed `_stop_event` (`_close()` is a
        deliberate no-op now; see `_CvCaptureAdapter`) — would land in the
        *new* entry `self._cameras.get(slug)` now resolves to, since a
        lookup by slug alone cannot tell the two apart. On a stream that
        had been running for minutes before a rotation, that window was
        long enough for real frames to keep flowing on a password the
        rotation was supposed to have retired — silently refreshing the
        wrong camera's freshness indefinitely, not merely for a moment.

        This does mean every source's frame is decoded before the rate
        limit ever sees it for the three threaded sources (RTSP/MJPEG/
        V4L2), which must decode on their own thread just to know a frame
        arrived at all — but no longer for ROS: `_RosSourceAdapter` only
        reaches this hand-off (via `on_frame`) when something is actually
        going to use the result — decode only what someone will see —
        otherwise it stores the raw message and clears any source
        error through `_clear_source_error` directly, bypassing this
        method's resize/rate-limit/`latest.set` entirely, since those only
        make sense for a frame that was actually converted."""
        entry = self._cameras.get(slug)
        if entry is not expected_entry:
            return  # torn down, retargeted, or a stale adapter calling back in late

        if not entry.rate.should_send(None, time.monotonic()):
            return

        try:
            frame = camera.resize_if_needed(bgr, width=entry.width, height=entry.height)
        except Exception:  # noqa: BLE001 - one bad frame must not kill the source
            log.exception("Could not resize a camera frame for slug %r", slug)
            return

        entry.latest.set(frame, timestamp_ms)
        self._clear_source_error(slug, entry, timestamp_ms)

    def _clear_source_error(self, slug: str, expected_entry: _CameraEntry, timestamp_ms: int) -> None:
        """The recovery half of accepting a frame — factored out of
        `_on_source_frame` so the ROS adapter's raw-storage path shares it
        too: storing an unconverted message must not run
        resize/rate-limit/`entry.latest.set` (those only apply to a frame
        that was actually converted, see `_on_source_frame`), but a camera
        that resumes delivering frames while idle (no live publisher,
        nothing snapshot-due, so its frames stay raw) still needs its
        `last_source_error` cleared — or it reads broken forever despite
        frames genuinely arriving, exactly the "a healthy thing reads as
        wrong" bug this recovery report exists to prevent (see below).

        Same stale-adapter guard as `_on_source_frame`; `timestamp_ms` is
        the accepted frame's own capture time (raw or converted),
        reported as `observed_at_ms` on a recovery — recovery was
        confirmed exactly when that frame was captured, not whenever this
        method happens to run.

        Reports a recovery exactly once, at the transition — not on every
        accepted frame afterwards. `had_error` is read before clearing so
        this only fires when there was something to recover from. This is
        the mirror image of an earlier defect: that one was a camera in a
        bad state reported healthy, "a wrong state reads as healthy". A
        camera that stays reported broken forever after being fixed is "a
        healthy thing reads as wrong", and it is worse — the developer has
        already done the work and is told it did not help.

        Read from `self._camera_last_reported_error`, keyed by slug, NOT
        from `entry.last_source_error` (found end to end): a
        credential fix re-sends config, which for RTSP/MJPEG changes the
        *resolved* credential and rebuilds the entry (`_apply_cameras`'s
        `source_or_credentials_changed`) — a fresh `_CameraEntry` with no
        memory of the old error. Keying off the entry would make the first
        good frame on the fixed credential look like an ordinary first
        frame, not a recovery, and nothing would ever be sent — a fixed
        camera reading broken forever, the exact case this recovery report
        exists to prevent. See `_camera_last_reported_error`'s own
        docstring in `__init__`."""
        entry = self._cameras.get(slug)
        if entry is not expected_entry:
            return  # torn down, retargeted, or a stale adapter calling back in late

        had_error = self._camera_last_reported_error.get(slug) is not None
        entry.last_source_error = None  # a good frame clears any prior classified failure
        self._camera_last_reported_error.pop(slug, None)
        if had_error:
            self.camera_states.put_threadsafe(
                self._loop,
                CameraStateUpdate(
                    slug, False, None, cause=CAMERA_STATE_CAUSE_SOURCE, observed_at_ms=timestamp_ms,
                    request_id=None,
                )
            )

    def _on_source_error(self, slug: str, expected_entry: _CameraEntry, code: str, message: str) -> None:
        """A non-ROS source's adapter failed to connect/authenticate/keep
        reading (the error path). Recorded locally (`_CameraEntry.
        last_source_error`, read by `start_live`'s own docstring for why a
        *join* answers with this directly rather than opening a publisher
        for nothing) **and** reported unsolicited — a background
        failure with no viewer watching used to be invisible until someone
        pressed "Go live", sometimes days later. `camera_sources.py`'s own
        adapters already report a given `(code, ...)` at most once per
        transition into it (`_report_once`/`_last_reported_code`), so this
        inherits that de-duplication for free — a prolonged outage becomes
        one report, not a flood. Same stale-adapter guard as
        `_on_source_frame` — see its docstring — so a superseded adapter's
        error can never overwrite the *current* adapter's own state
        either."""
        entry = self._cameras.get(slug)
        if entry is not expected_entry:
            return
        now = sampling.capture_timestamp_ms()
        entry.last_source_error = (code, message, now)
        # Slug-keyed, survives an entry rebuild — see `_on_source_frame`'s
        # matching comment and `_camera_last_reported_error`'s own
        # docstring in `__init__` for why this cannot just be `entry.
        # last_source_error` a second time.
        self._camera_last_reported_error[slug] = (code, message, now)
        self.camera_states.put_threadsafe(
            self._loop,
            CameraStateUpdate(
                slug, False, (code, message), cause=CAMERA_STATE_CAUSE_SOURCE, observed_at_ms=now,
                request_id=None,
            )
        )

    # --- the shared watchdog: one timer, runs on the executor ---------------

    def _check_watchdogs(self) -> None:
        """The one timer `start()` creates, ticking every
        `FAILSAFE_CHECK_INTERVAL_S`. Independent checks share it rather than
        each getting a timer of its own — the same reasoning
        `_check_failsafes` already used for per-publisher timers, now
        applied to per-goal and per-camera ones too."""
        # Guarded: whatever goes wrong retiring an old handle must not cost
        # the live publishers their watchdog on this tick.
        try:
            self._reap_retiring_publishers()
        except Exception:  # noqa: BLE001 - the failsafe check below matters more
            log.exception("Retiring publishers failed on this tick")
        self._check_failsafes()
        self._check_goal_timeouts()
        self._check_service_timeouts()

    def _check_failsafes(self) -> None:
        """Ticks every `FAILSAFE_CHECK_INTERVAL_S` regardless of anything
        else happening in the process — no cloud connection, no client, no
        asyncio loop involved at all (independent of the cloud is
        the whole point). A publisher that has never been published to is
        left alone; one that has and has gone quiet for `timeout_ms` gets
        its failsafe fired exactly once, not once per tick of continued
        silence."""
        now = time.monotonic()
        for slug, entry in list(self._publishers.items()):
            if entry.last_activity_at is None or entry._failsafe_sent:
                continue
            silent_ms = (now - entry.last_activity_at) * 1000.0
            if silent_ms >= entry.timeout_ms:
                self._fire_failsafe(slug, entry)
                entry._failsafe_sent = True

    def _fire_failsafe(self, slug: str, entry: "_PublisherEntry") -> None:
        try:
            msg = params.build_message(entry.message_class, entry.failsafe_body)
        except Exception:  # noqa: BLE001 - config-apply already validated this; be defensive anyway
            log.exception(
                "Publisher slug %r: could not build its failsafe message — nothing was "
                "published, which is worse than a stale value, so this is logged loudly",
                slug,
            )
            return
        entry.handle.publish(msg)
        log.warning(
            "Publisher slug %r: no publish for %d ms — the bridge published its failsafe",
            slug,
            entry.timeout_ms,
        )

    def _check_goal_timeouts(self) -> None:
        """A goal still waiting on `_goal_deadlines` past its deadline gets
        reported `failed`/`goal_timeout` — which, once delivered
        (`jobs.mark_delivered`), frees the slug the same way any other
        terminal update does. `job_id` moves into `_timed_out_job_ids` at
        the same time, which is how `_on_goal_response`, if it still runs
        later for this `job_id`, knows to treat its own answer as a late
        arrival rather than an ordinary one (see there)."""
        now = time.monotonic()
        for job_id, (slug, deadline, patience_s) in list(self._goal_deadlines.items()):
            if now < deadline:
                continue
            del self._goal_deadlines[job_id]
            self._timed_out_job_ids.add(job_id)
            self._emit_job(
                job_id,
                slug,
                "failed",
                error=(
                    "goal_timeout",
                    # patience_s: this job's own patience, not a
                    # shared constant — two concurrent invokes can carry
                    # different patience_ms, and this message must report
                    # the one that actually governed this job.
                    "the action server did not accept the goal within {:.0f}s".format(
                        patience_s
                    ),
                ),
            )

    def _check_service_timeouts(self) -> None:
        """A service call still waiting on `_service_deadlines` past its
        deadline gets reported `failed`/`service_timeout` (2n) — and,
        first, has its `call_future` abandoned via `remove_pending_request`,
        rclpy's own public, documented way to guarantee that future never
        receives a response and never runs its done callback
        (`/opt/ros/<distro>/.../rclpy/client.py::Client.remove_pending_request`,
        present in rclpy on every distribution this package builds for).
        That is what makes this safe to declare *final*, unlike
        `_check_goal_timeouts`: there is no later, contradicting callback
        possible once this has run, so there is no `_timed_out_job_ids`-style
        set to consult here.

        `entry is None` (the service was retargeted/removed from under this
        call) should not be reachable — `_settle_orphaned_job` pops this
        job's deadline entry before destroying the client — but is handled
        rather than trusted, since a future that is never abandoned would
        otherwise pin this job's slot in `_service_deadlines` forever."""
        now = time.monotonic()
        for job_id, (slug, deadline, patience_s, call_future) in list(self._service_deadlines.items()):
            if now < deadline:
                continue
            del self._service_deadlines[job_id]
            entry = self._services.get(slug)
            if entry is not None:
                entry.client.remove_pending_request(call_future)
            else:
                log.warning(
                    "Service timeout for job %r (slug %r): no service entry to abandon "
                    "the call on — the slug was retargeted or removed without settling "
                    "this deadline first",
                    job_id, slug,
                )
            self._emit_job(
                job_id,
                slug,
                "failed",
                error=(
                    "service_timeout",
                    "the service did not respond within {:.0f}s".format(patience_s),
                ),
            )

    async def next_snapshot(self, max_bytes: int) -> Optional[bytes]:
        """The wire-ready bytes of the next snapshot that is due, or `None`
        when nothing is due or nothing fits. Pulled by `client.py`'s
        `PrioritizedWriter`: snapshots are pulled and fitted, not queued and
        dropped — it asks only once every tier above 5 is
        idle, and it says how many bytes it can currently afford.

        Pulled rather than queued, which retires `SnapshotQueue` and with
        it 2.0.2's drop-and-probe machinery: a queue could only hold frames
        encoded against a guess made earlier, so a link that had slowed
        down left the pump holding frames it then had to *drop*, and a
        counter of consecutive drops had to exist to ever re-measure the
        link. Pulling asks at the moment of sending, with the rate the
        writer has just measured, so drop-oldest is implicit (there is
        never more than the newest frame) and there is nothing to probe.

        `max_bytes` is the writer's budget; the camera's own configured
        ceiling still applies, and the smaller of the two wins. Both are
        real: `self._snapshot_max_bytes` is what the *wire format* and the
        socket's `maxPayload` allow (a frame over it closes the
        connection and takes datapoints, jobs, commands and config with
        it), while `max_bytes` is what the *link* can carry inside the
        occupancy budget. Neither subsumes the other.

        Runs the walk on the executor thread via `_submit_async`, like
        every other `_cameras` reader: `entry.latest` and
        `entry.last_snapshot_at` are executor-thread state, and
        `camera.encode_snapshot_jpeg` is real CPU work that must not run on
        the event loop the writer is being served from."""
        return await self._submit_async(
            lambda: self._next_snapshot(min(max_bytes, self._snapshot_max_bytes))
        )

    def _convert_due_raw_frame(self, slug: str, entry: _CameraEntry) -> None:
        """Lazy half of the decode gate: when nobody has a live
        publisher running for `slug`, `_RosSourceAdapter`'s callback leaves
        each arriving message in `entry.raw` instead of converting it (the
        eager half — see `conversion_wanted` in `_create_camera`). Called
        only for a camera `_next_snapshot`'s dueness walk has just decided
        is actually due, so a camera nobody ever pulls a snapshot for, and
        nobody watches live, never pays the imgmsg->BGR conversion cost at
        all — the actual point of this whole task.

        Runs on the executor thread, the same thread `_RosSourceAdapter`'s
        callback runs on (this whole walk does, via `_submit_async`), so
        `entry.raw` needs no cross-thread handling beyond its own lock and
        `camera.to_bgr` is safe to call directly here.

        `entry.raw.take()` unconditionally, even when the frame turns out
        to be the "already superseded" branch below: with the executor
        thread as the only reader and the only writer of both `raw` and
        `latest`, there is no concurrent second look to lose by consuming
        it eagerly, and a raw frame older than what `latest` already holds
        has no value either way — leaving it in the slot would only make a
        later call redo this same comparison against the same stale
        answer."""
        raw = entry.raw.take()
        if raw is None:
            return
        msg, raw_timestamp_ms = raw
        latest = entry.latest.get()
        if latest is not None and latest.timestamp_ms >= raw_timestamp_ms:
            return  # `latest` is already at least as fresh as the raw frame was
        try:
            bgr = camera.to_bgr(msg)
        except camera.UnsupportedImageError:
            log.exception("Could not convert a camera frame for slug %r", slug)
            return
        # The same shared hand-off the eager (live) path uses — RatePolicy
        # and `entry.latest` stay the single source of truth for both,
        # never a second copy of either rule. `raw_timestamp_ms`
        # is the raw frame's own capture time (read in the ROS callback
        # before anything else), not this conversion's — the snapshot that
        # eventually goes out must report when the camera saw it, not when
        # somebody got around to decoding it.
        self._on_source_frame(slug, entry, bgr, raw_timestamp_ms)

    def _next_snapshot(self, max_bytes: int) -> Optional[bytes]:
        """The dueness walk, on the executor thread. Every configured
        camera, independent of live — a camera nobody is
        watching live still sends snapshots on schedule, and this loop has
        no notion of live at all to accidentally couple the two. A camera
        with nothing captured yet (`latest.get() is None`) is skipped
        without arming `last_snapshot_at`, so the first snapshot goes out
        the moment a frame *becomes* available rather than waiting out a
        full interval measured from config-apply time — nothing was
        promised before there was anything to send.

        Disconnected is no longer checked here, and does not need to
        be: the caller is the session's writer, which exists only while a
        session does. Nothing is encoded, queued or dated while
        disconnected, so the gap stays honest — a snapshot gap must never
        become a delayed burst of stale images presented as current — and
        the check is not duplicated into a second place that
        could later disagree with the first.

        At most one frame per call, first due camera wins. Arming
        `last_snapshot_at` on every camera it *tries* is what keeps that
        fair: a camera just served is not due again, so the next call
        starts at whichever camera is.

        Only *now*, for a camera that is actually due, does
        `_convert_due_raw_frame` decode anything ROS left raw —
        a camera that never has a snapshot pulled for it, and no live
        publisher either, never pays the conversion cost at all."""
        now = time.monotonic()
        for slug, entry in list(self._cameras.items()):
            if entry.last_snapshot_at is not None:
                # Seconds throughout, and deliberately so: the 3.0 format renamed the
                # wire field `snapshot_interval_ms` -> `snapshot_interval_
                # seconds` and re-scaled it, and `time.monotonic()` has
                # always been seconds. Holding the entry in seconds too
                # leaves this file with **no conversion at all** — the `*
                # 1000.0` that used to sit on this line is gone rather than
                # moved. That is the answer to "keep milliseconds
                # internally?": a wire whose number now means something
                # 1000x different is best defended against by having exactly
                # one unit in the bridge and no arithmetic to get wrong. It
                # costs nothing in precision — contracts bounds the interval
                # at 1..3600 whole seconds — and any code still reaching for
                # the old `_ms` name is a NameError, not a silent 1000x bug.
                if now - entry.last_snapshot_at < entry.snapshot_interval_seconds:
                    continue
            self._convert_due_raw_frame(slug, entry)
            frame = entry.latest.get()
            if frame is None:
                continue
            try:
                encoded = camera.encode_snapshot_jpeg(frame.bgr, max_bytes=max_bytes)
            except camera.UnsupportedImageError:
                log.exception("Could not encode a snapshot for camera slug %r", slug)
                continue
            # Counts as "tried this interval" whether or not anything was
            # actually sent — without this, a frame nothing can shrink
            # under the ceiling would be re-encoded, at every quality and
            # resolution this module tries, on every single pull.
            entry.last_snapshot_at = now
            if encoded is None:
                # Never send something that would close the socket (ws
                # enforces its own maxPayload before the frame reaches the
                # application, closing the connection with 1009 and taking
                # datapoints, jobs, commands and config down with it) — a
                # dropped snapshot is a gap, the same honest answer
                # already established for a disconnected bridge. Logged
                # loudly and every time, not just once: a camera that can
                # never fit is a configuration error a developer needs to
                # see, not a mystery buried in a single startup line.
                log.error(
                    "Camera slug %r: no JPEG encoding of this frame fits under "
                    "the %d-byte snapshot limit even at the most aggressive "
                    "quality/resolution backoff — dropping this snapshot "
                    "rather than closing the bridge socket. Check the "
                    "camera's configured resolution.",
                    slug, max_bytes,
                )
                continue
            image_bytes, width, height = encoded
            return snapshot_frame(
                slug=slug,
                mime="image/jpeg",
                width=width,
                height=height,
                # Capture time, from the frame — never the moment this was
                # encoded or sent (the standing `timestamp_ms` rule).
                timestamp_ms=frame.timestamp_ms,
                image_bytes=image_bytes,
            )
        return None

    # --- assets: URDF availability detection -----------

    async def report_current_urdf_availability(self) -> None:
        """Re-reports the currently known URDF availability, unconditionally,
        once per connection — client.py calls this right after the first
        config apply of a session, same call site and same reasoning as
        `report_current_camera_health`: `_on_robot_description`'s own
        dedup-by-hash only ever queues a report when the *content* changes,
        correct for a live connection but silent on a plain reconnect where
        nothing ROS-side changed. Without this, a cloud that does not
        durably remember `assets_available` across its own restart (or
        simply never told this particular new session) would have nothing
        to show, indistinguishably from a robot with no URDF at all.

        A no-op when nothing has ever been seen on `DEFAULT_URDF_TOPIC` —
        there is nothing honest to restate. Runs entirely on the asyncio
        side (unlike the camera-health equivalent): the cache it reads is
        already lock-protected for exactly this cross-thread read, so there
        is no ROS object to reach via `_submit_async` and `self.assets.
        put_nowait` is safe to call directly from this thread."""
        with self._urdf_lock:
            text = self._latest_urdf_text
            meshes = self._latest_urdf_meshes
        if text is None:
            return
        self.assets.put_nowait(AssetsAvailable(urdf=True, meshes=meshes))

    async def check_urdf_availability_now(self) -> None:
        """Runs one active check immediately, rather than waiting out
        `URDF_AVAILABILITY_CHECK_INTERVAL_S`'s own periodic timer. Called
        by client.py at the same call site as `report_current_urdf_
        availability`, right after a fresh connection's first config
        apply, so a robot with no `/robot_description` publisher at all is
        told to the newly-connected cloud promptly — "asked and none"
        reachable within the round trip of connecting, not within
        whichever fraction of the periodic interval happened to remain.

        `count_publishers` is a real graph query, so — same rule this
        whole file holds everywhere else — it runs on the executor thread,
        via `_submit_async`, not here directly."""
        await self._submit_async(self._check_urdf_availability)

    def _check_urdf_availability(self) -> None:
        """The active half of that check — runs on the executor thread,
        either via the periodic `_urdf_availability_timer` or via
        `check_urdf_availability_now`'s `_submit_async`. The subscription
        callback below (`_on_robot_description`) can only ever notice a
        publisher *sending* something — `urdf: true` — and has no way to
        notice one going away, or one that was never there in the first
        place. This is what makes `false` reachable at all.

        Three cases, matching the three states this can report:

        - A publisher exists: nothing to do here. Presence is still
          asserted by `_on_robot_description` alone, on actually receiving
          content — this method finding a publisher does not by itself
          mean anything has been received yet (a TRANSIENT_LOCAL message
          in flight, say), so it must not invent its own way to say
          `true`. Only records that a check has now happened at all.
        - No publisher, and this bridge currently believes `true` (cached
          text present): a genuine true → false transition. Clears the
          cache — including `_last_urdf_hash`, so a later republish of
          *identical* content after the gap is not silently deduped away
          as "unchanged" — and reports `false`.
        - No publisher, and this is the very first check ever (nothing
          cached, nothing reported before): reports `false` once — "asked
          and none" — and remembers that it has now said so, so the next
          tick with the same answer stays silent rather than repeating
          it. Before this method has run even once, nothing is reported
          at all — that gap is the cloud's own `null` ("nobody connected/
          nobody has asked yet"), not this bridge's to fill in."""
        publisher_present = self._node.count_publishers(DEFAULT_URDF_TOPIC) > 0
        if publisher_present:
            self._urdf_never_checked = False
            return
        with self._urdf_lock:
            had_cached_text = self._latest_urdf_text is not None
            if not had_cached_text and not self._urdf_never_checked:
                return  # already reported false; nothing has changed
            self._latest_urdf_text = None
            self._latest_urdf_meshes = ()
            self._latest_urdf_texture_uris = frozenset()
        self._last_urdf_hash = None
        self._urdf_never_checked = False
        self._loop.call_soon_threadsafe(
            self.assets.put_nowait, AssetsAvailable(urdf=False, meshes=())
        )

    def _on_robot_description(self, msg: StringMsg) -> None:
        """The subscription callback for `DEFAULT_URDF_TOPIC` — runs on the
        executor thread, for the life of the node, independent of any
        session being connected (same as `_check_failsafes`). Availability
        only: this queues a report, never bytes.

        Dedupes by content hash: this is the *change* channel, queueing a
        fresh report only when what's latched on the topic actually differs
        from the last one reported. The *per-connection* channel is
        `report_current_urdf_availability`, called separately by client.py
        so a reconnect with no ROS-side change still tells a cloud that may
        not remember.

        A blank latched value (no publisher has actually published yet, or
        published an empty string) is not worth reporting — `urdf: false`
        would be a stronger claim than "nothing seen" warrants, since
        `create_subscription` with TRANSIENT_LOCAL only ever delivers a
        message a publisher actually sent."""
        content = msg.data
        if not content or not content.strip():
            return
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if content_hash == self._last_urdf_hash:
            return  # unchanged since the last report — nothing new to say
        self._last_urdf_hash = content_hash
        meshes, texture_uris = self._extract_mesh_uris(content)
        with self._urdf_lock:
            self._latest_urdf_text = content
            self._latest_urdf_meshes = tuple(meshes)
            self._latest_urdf_texture_uris = texture_uris
        # call_soon_threadsafe, not a direct put_nowait: this callback runs
        # on the executor thread, and `self.assets` is read by client.py on
        # the asyncio loop — the same cross-thread discipline `self.jobs`/
        # `self.samples` already use for exactly this handoff shape.
        self._loop.call_soon_threadsafe(
            self.assets.put_nowait, AssetsAvailable(urdf=True, meshes=tuple(meshes))
        )

    @staticmethod
    def _extract_mesh_uris(urdf_text: str) -> Tuple[List[str], FrozenSet[str]]:
        """Every `package://` URI a URDF's `<mesh filename="...">` *and*
        `<texture filename="...">` elements reference (textures were
        added later — a `<texture>` inside a `<material>` was never
        offered before, while the cloud's own extractor always saw it and
        listed it in `urdfCompleteness.missing`: correctly named, permanently
        unfixable), deduplicated (first-seen order kept — a URDF commonly
        reuses the same mesh for visual and collision geometry, or the same
        texture across several materials, and a duplicate here would only
        inflate the "how many meshes" count the console shows) but not
        *resolved* — resolving against the workspace happens once the
        cloud asks for a sync, not on every URDF change.

        Returns `(all_uris, texture_uris)` — the first is what travels on
        the wire's `meshes` field (unrenamed; both kinds ride together,
        same as the cloud's own extractor does not distinguish them at this
        stage either), the second is the subset `_run_asset_sync` uses to
        classify a requested URI's upload `kind` later. Both `<mesh>` and
        `<texture>` are searched at any nesting depth (`root.iter`, not a
        fixed parent path) — matching the cloud's own
        `REWRITABLE_ELEMENTS` walk, so this bridge does not offer a
        narrower set of texture references than the cloud already expects
        to rewrite.

        Malformed XML is reported as "no meshes found", not a crash: the
        bridge still has *something* on `DEFAULT_URDF_TOPIC` worth naming
        available even if it cannot be parsed as well-formed URDF — the
        parse failure itself is the developer's problem to fix, not a
        reason to stay silent about there being content at all.

        Parsed with `defusedxml`, not stdlib `xml.etree`:
        `/robot_description` is graph input, not necessarily first-party —
        anything with publish access to this domain can put a message on
        it, and stdlib's parser resolves external entities and expands
        nested ones by default, an XXE/billion-laughs surface for zero
        benefit (a URDF has no legitimate use for either). `DefusedXml
        Exception` is `ValueError`, not `ElementTree.ParseError` — caught
        alongside it here so a *detected attack* fails exactly the same
        honest way a malformed document does, not as an uncaught exception
        that would take down this subscription callback (and, with it,
        every future URDF change) on the executor thread."""
        try:
            root = ElementTree.fromstring(urdf_text)
        except (ElementTree.ParseError, DefusedXmlException):
            log.warning(
                "Could not parse %r as XML; reporting it available with no "
                "resolvable meshes",
                DEFAULT_URDF_TOPIC,
                exc_info=True,
            )
            return [], frozenset()
        seen: Dict[str, None] = {}
        texture_uris: Set[str] = set()
        # A single walk (not one `root.iter(tag)` pass per tag) so
        # "first-seen order" means document order across *both* kinds, not
        # every mesh followed by every texture regardless of where each
        # actually appears.
        for element in root.iter():
            if element.tag not in ("mesh", "texture"):
                continue
            filename = element.get("filename")
            if filename and filename.startswith("package://"):
                seen.setdefault(filename, None)
                if element.tag == "texture":
                    texture_uris.add(filename)
        return list(seen), frozenset(texture_uris)

    async def sync_assets(
        self, sync_id: str, upload_url: str, token: str, requested_meshes: Sequence[str]
    ) -> None:
        """Dispatched from client.py on a `cloud_asset_request` — resolves
        every requested `package://` URI against the
        workspace and uploads it, plus the URDF text itself, over HTTP with
        the per-sync credential the request carried. Never the socket:
        the cloud caps a frame at 2 MiB and a mesh exceeds that routinely
        — the WebSocket only ever carried the *conversation*
        (`assets_available`, this request, `asset_progress` below).

        Runs entirely on the asyncio side, same as
        `report_current_urdf_availability`: resolving a `package://` URI and
        making an HTTP request touch no rclpy object, only this process's
        filesystem and network, so nothing here needs the executor thread —
        the blocking calls (`ament_index_python`, file reads, `urllib`) go
        through `run_in_executor` instead, so the event loop itself is
        never blocked by them.

        Single-flight, bridge-side **backstop** (contracts: the cloud owns
        this primarily and refuses a concurrent sync before minting a
        second `asset_request` at all) — `self._active_sync_id` is checked
        and set before any `await`, so two requests dispatched back to back
        by client.py's receive loop (itself single-threaded) cannot both
        pass the check.

        The `finally` below already guaranteed `_active_sync_id` clears on
        any exit from `_run_asset_sync`, including an exception — a *later*
        sync was never blocked by one that failed. What was missing was
        *this* sync ever telling anyone it failed; that guarantee
        now lives in `_run_asset_sync` itself, not here."""
        if self._active_sync_id is not None:
            self.asset_progress.put_nowait(
                AssetProgress(
                    sync_id=sync_id,
                    done=0,
                    total=1 + len(requested_meshes),
                    failed=(),
                    state="refused_busy",
                )
            )
            return
        self._active_sync_id = sync_id
        try:
            await self._run_asset_sync(sync_id, upload_url, token, requested_meshes)
        finally:
            self._active_sync_id = None

    async def _run_asset_sync(
        self, sync_id: str, upload_url: str, token: str, requested_meshes: Sequence[str]
    ) -> None:
        """Every exit path from this method reaches a **terminal** `asset_progress` frame.
        Before this, an exception anywhere in the body below propagated out
        of `sync_assets` as an unretrieved-task-exception log line and
        nothing else — `_active_sync_id` still cleared (the `finally` in
        `sync_assets` already guaranteed that), so a *later* sync was always
        accepted, but *this* one left the cloud's own sync-progress state
        waiting on a frame that would never arrive, bounded only by the
        cloud's own idle timeout rather than reported honestly and promptly.

        The `try` below is a defensive backstop, not the normal path: every
        known failure mode (an unresolvable URI, a non-2xx upload, a missing
        URDF) already degrades to an ordinary `failed` entry without raising
        — `_resolve_package_uri` and `_upload_asset_bytes`/`_upload_mesh_file`
        are all documented as "never raises". This catches the genuinely
        unexpected: a bug, a race, anything neither of those contracts
        anticipated."""
        with self._urdf_lock:
            urdf_text = self._latest_urdf_text
            cached_meshes = self._latest_urdf_meshes
            texture_uris = self._latest_urdf_texture_uris
        # `meshes` (contracts): "Which URIs to send. Empty means the URDF
        # only" — an explicit empty request, not "nothing was specified, so
        # fall back to everything last reported". `requested_meshes` is
        # already the caller's own choice either way; there is no case
        # here that substitutes `cached_meshes` for it.
        meshes = tuple(requested_meshes)
        loop = asyncio.get_event_loop()
        done = 0
        # `(reference, kind)` pairs: six producers used to
        # write three different facts into a flat list of bare strings
        # indistinguishably (a reference that resolves to nothing; a file
        # whose transfer failed; one that was never attempted).
        # `kind` is one of `ASSET_FAILURE_KIND_*` (protocol.py); only
        # `ASSET_FAILURE_KIND_UNRESOLVABLE` may ever be treated as "this
        # reference is gone for good" by a reconciliation.
        failed: List[Tuple[str, str, Optional[Dict[str, int]]]] = []
        # Provisional until the pre-scan below finalises them — correct
        # even if the exception guard fires mid-pre-scan, since nothing
        # from the pre-scan has been promised to the cloud in a `report()`
        # call yet at that point.
        total = 1 + len(meshes)
        remaining: List[str] = [URDF_ASSET_NAME, *meshes]
        # `uri -> [(texture_name, resolved_path), ...]` for every `.dae`
        # among `meshes` that references an internal image —
        # populated by the pre-scan, read by the upload loop below.
        dae_textures: Dict[str, List[Tuple[str, Optional[str]]]] = {}
        # Ceiling sentinel names, tracked separately from an ordinary unresolved
        # reference: both carry `resolved_path=None` in `dae_textures` and
        # would otherwise be indistinguishable in the loop below, but they
        # are different `kind`s on the wire — a sentinel was never
        # attempted (`refused`), an ordinary one was attempted and failed
        # to resolve (`unresolvable`).
        refused_sentinels: Set[str] = set()
        resolved: Dict[str, Optional[str]] = {}
        # A `.dae`'s own size, stated once here — in the
        # same pre-scan pass that already resolves every `uri`, before
        # anything reads its bytes — and read again by the upload loop
        # below instead of stated a second time. Closing this gap is the
        # whole reason it exists: `_extract_dae_texture_references` used to
        # run unconditionally on every resolved `.dae`, `open()`ing and
        # `.read()`ing the whole file to search for `<init_from>` tags,
        # *before* the ceiling check the upload loop performed further
        # down ever ran. The upload path was closed first and left this one
        # open — the same 194 MB, one function earlier, confirmed with a
        # same-bytes `.stl` control run that stayed at baseline while the
        # `.dae` run added ~305 MB of peak RSS.
        sizes: Dict[str, Optional[int]] = {}
        # the mesh-level half of the same bound already given to the
        # `.dae`-internal half. One shared counter, not one per code path —
        # both the ordinary per-mesh loop below *and* the exception guard's
        # own catch-all can each independently push `failed` past the
        # contract's cap (an early exception with thousands of `remaining`
        # items is the identical shape of overflow, just reached a
        # different way), so both route through `record_failure` and draw
        # on the same budget rather than each getting their own.
        mesh_failed_count = 0
        mesh_failure_overflow_reported = False

        def report() -> None:
            # `state` is what tells a receiver "this is the last progress
            # update" apart from "the sync is genuinely over" — done==total
            # after the URDF alone (no meshes requested) means this single
            # call already *is* the terminal frame, not a "running" frame
            # followed by a redundant, identical "finished" one.
            state = "finished" if done >= total else "running"
            self.asset_progress.put_nowait(
                AssetProgress(sync_id=sync_id, done=done, total=total, failed=tuple(failed), state=state)
            )

        def record_failure(
            reference: str, kind: str, details: Optional[Dict[str, int]] = None
        ) -> None:
            # appends to `failed`, respecting `MESH_MAX_FAILED_PER_
            # SYNC`. Once the ceiling is hit, every further failure
            # collapses into one already-added sentinel instead of
            # growing the array — `refused`, not `unresolvable` or
            # `upload_failed`: the items behind the ceiling could be
            # either (some may genuinely not exist, some may just be a
            # transient upload failure), and collapsing them under
            # `unresolvable` would wrongly tell a reconciliation it is
            # safe to drop something that might still be wanted. `refused`
            # claims neither — only that the wire stopped naming them
            # individually, the same honest shape the `.dae`-internal
            # ceiling sentinels already use. A store refusal collapses
            # into this sentinel like any other failure once the ceiling is
            # hit — the sentinel carries no `details`, so those three
            # numbers are lost the same way an unresolvable reference's
            # identity is: the per-sync cap is a size bound on the wire
            # frame, not a promise every individual fact survives it.
            nonlocal mesh_failed_count, mesh_failure_overflow_reported
            if mesh_failed_count < MESH_MAX_FAILED_PER_SYNC:
                failed.append((reference, kind, details))
                mesh_failed_count += 1
            elif not mesh_failure_overflow_reported:
                failed.append((
                    "more than {} mesh-level references failed to resolve "
                    "or upload in this sync — additional failures are not "
                    "listed individually".format(MESH_MAX_FAILED_PER_SYNC),
                    ASSET_FAILURE_KIND_REFUSED,
                    None,
                ))
                mesh_failure_overflow_reported = True

        try:
            # --- Pre-scan every requested mesh for `.dae`-
            # internal textures *before* any upload begins, so the first
            # `asset_progress` frame this sync ever sends already carries
            # the true `total` rather than one that grows mid-stream.
            # Every mesh is resolved exactly once here and the result
            # reused below — not re-resolved in the upload loop. Its size
            # is stated exactly once here too — reused
            # by the upload loop below, and read *by this same pass* before
            # a `.dae` is ever opened to search it for internal references.
            #
            # two ceilings, both enforced here,
            # before a single reference is added to `dae_textures` — a
            # `.dae`'s own reference count is hostile input, the same way
            # its internal paths already are — the same reasoning, one hop
            # further in. A file over its own per-file ceiling contributes
            # exactly one sentinel naming *it*, not its references; once
            # the sync-wide ceiling is hit, no further `.dae` in this sync
            # is even parsed for internal references — its own mesh still
            # resolves and uploads normally, only the internal-texture
            # scan stops. Both sentinels carry `resolved_path=None`, so the
            # existing unresolved-reference handling below (a reference
            # lands in `failed`, nothing is uploaded) covers them without
            # any change to the upload loop itself.
            #
            # A re-derivation of the arithmetic: the per-file
            # branch used to increment `sync_reference_count` without ever
            # checking it against `DAE_MAX_INTERNAL_REFERENCES_PER_SYNC`,
            # and never set `sync_budget_exhausted` either — so a sync
            # budget spent entirely on over-large `.dae` files (rather than
            # on many references inside one) was never noticed, and each
            # further such file kept adding its own sentinel unbounded.
            # Reproduced: 5 `.dae` files each over a per-file limit of 3,
            # a sync-wide cap of 2, produced 5 sentinel entries — the exact
            # shape `record_failure` was just built to close one layer out,
            # left open one layer in. `sync_overflow_sentinel` below is now
            # the one place that checks and consumes the sync-wide budget,
            # called both before a new `.dae` is even examined and mid-file
            # when a `.dae` under its own per-file limit still runs the
            # count over — one budget, one chokepoint, not two.
            seen_texture_names: Set[str] = set()
            sync_reference_count = 0
            sync_budget_exhausted = False

            def sync_overflow_sentinel(uri: str) -> Optional[Tuple[str, None]]:
                nonlocal sync_budget_exhausted, sync_reference_count
                sync_budget_exhausted = True
                sentinel = (
                    "this sync's .dae-internal image references "
                    "exceeded the {}-entry sync limit at {} — the "
                    "rest were not enumerated"
                ).format(DAE_MAX_INTERNAL_REFERENCES_PER_SYNC, uri)
                if sentinel in seen_texture_names:
                    return None
                seen_texture_names.add(sentinel)
                refused_sentinels.add(sentinel)
                sync_reference_count += 1
                return sentinel, None

            for uri in meshes:
                path = await loop.run_in_executor(None, self._resolve_package_uri, uri)
                resolved[uri] = path
                if path is not None:
                    sizes[uri] = await loop.run_in_executor(None, self._file_size_or_none, path)
                if path is None or not path.lower().endswith(".dae"):
                    continue
                # `_extract_dae_texture_references` below reads this file
                # whole and parses it; `DAE_SCAN_MAX_BYTES` is what keeps
                # that bounded now that the upload ceiling is gone. One
                # line per oversized file and no more, because this loop
                # visits each `.dae` exactly once per sync. No entry is
                # added to `dae_textures`: "its textures were not looked
                # for" is not a failure of any reference — inventing one
                # would put a name on the wire this sync never determined
                # anything about.
                dae_size = sizes.get(uri)
                if dae_size is not None and dae_size > DAE_SCAN_MAX_BYTES:
                    log.warning(
                        "Not scanning %r for internal textures: %d bytes is over "
                        "the %d-byte scan cap. Textures inside it are not "
                        "discovered; the mesh still uploads",
                        uri, dae_size, DAE_SCAN_MAX_BYTES,
                    )
                    continue
                if sync_budget_exhausted:
                    continue
                if sync_reference_count >= DAE_MAX_INTERNAL_REFERENCES_PER_SYNC:
                    # The budget was exhausted by prior files (whether via
                    # this same check, the per-file branch below, or the
                    # per-reference loop below that) and this `.dae` was
                    # never even examined for its own reference count.
                    entry = sync_overflow_sentinel(uri)
                    if entry is not None:
                        dae_textures[uri] = [entry]
                    continue
                pairs = await loop.run_in_executor(
                    None, self._extract_dae_texture_references, uri, path
                )
                if len(pairs) > DAE_MAX_INTERNAL_REFERENCES_PER_FILE:
                    sentinel = (
                        "{} references too many internal images (>{}) — "
                        "refused, not enumerated"
                    ).format(uri, DAE_MAX_INTERNAL_REFERENCES_PER_FILE)
                    if sentinel not in seen_texture_names:
                        seen_texture_names.add(sentinel)
                        refused_sentinels.add(sentinel)
                        dae_textures[uri] = [(sentinel, None)]
                        sync_reference_count += 1
                    continue
                # Global dedup across this whole sync, not just within one
                # `.dae`: two sibling mesh files in the same directory
                # commonly share a texture atlas, and their internal
                # references would otherwise normalise to the identical
                # name — one upload and one `total` slot, not two.
                unique_pairs = []
                for name, texture_path in pairs:
                    if name in seen_texture_names:
                        continue
                    if sync_reference_count >= DAE_MAX_INTERNAL_REFERENCES_PER_SYNC:
                        entry = sync_overflow_sentinel(uri)
                        if entry is not None:
                            unique_pairs.append(entry)
                        break
                    seen_texture_names.add(name)
                    unique_pairs.append((name, texture_path))
                    sync_reference_count += 1
                if unique_pairs:
                    dae_textures[uri] = unique_pairs
            total += sum(len(v) for v in dae_textures.values())
            remaining = [URDF_ASSET_NAME]
            for uri in meshes:
                remaining.append(uri)
                remaining.extend(name for name, _ in dae_textures.get(uri, ()))

            if urdf_text is not None:
                urdf_result = await loop.run_in_executor(
                    None,
                    self._upload_asset_bytes,
                    upload_url, token, sync_id, "urdf", URDF_ASSET_NAME, "application/xml",
                    urdf_text.encode("utf-8"),
                )
                if not urdf_result.ok:
                    # Never `unresolvable`: the text exists — it is cached
                    # right here — so whichever way this went, a later sync
                    # stands a real chance of delivering it. `_upload_
                    # failure` tells a store refusal from a transfer that
                    # did not succeed.
                    kind, details = _upload_failure(urdf_result)
                    failed.append((URDF_ASSET_NAME, kind, details))
            else:
                # An asset_request should not normally arrive with no URDF
                # ever detected (availability reporting tells the cloud one exists at
                # all), but a race — the URDF source was reconfigured away
                # between assets_available and this request landing — is
                # not this method's business to rule out, only to report
                # honestly. Also `upload_failed`: nothing was
                # affirmatively determined absent — a URDF may simply not
                # have arrived yet, which is not the same claim
                # `unresolvable` makes about a workspace file.
                log.warning(
                    "asset_request for sync %r arrived with no URDF cached to send", sync_id
                )
                failed.append((URDF_ASSET_NAME, ASSET_FAILURE_KIND_UPLOAD_FAILED, None))
            remaining.pop(0)
            done += 1
            report()

            for uri in meshes:
                path = resolved[uri]
                if path is None:
                    # `_resolve_package_uri` returning `None` means the
                    # reference names nothing this bridge can find, or
                    # escapes its package — permanent, and the only kind a
                    # reconciliation may treat as gone. Routed through
                    # `record_failure`, not appended directly — this is the
                    # mesh-level half of the same array-size bound
                    # already gave `.dae`-internal references.
                    record_failure(uri, ASSET_FAILURE_KIND_UNRESOLVABLE)
                else:
                    # `asset_kind`: whichever XML element this URI
                    # came from — `assets_available.meshes`/`cloud_asset_
                    # request.meshes` carry no kind of their own, so this is
                    # the only place that knows the difference. A URI the
                    # cloud requests that this bridge no longer recognises
                    # as either (the URDF changed between assets_available
                    # and this request landing) defaults to `mesh` — the
                    # prior, only-ever-meshes behaviour, not a new failure
                    # mode. (Named `asset_kind`, not `kind`, so it cannot be
                    # confused with an `ASSET_FAILURE_KIND_*` value below —
                    # two different "kind"s of two different things.)
                    asset_kind = "texture" if uri in texture_uris else "mesh"
                    media_type = mimetypes.guess_type(uri)[0] or "application/octet-stream"
                    # `size` comes from `sizes`, filled by the pre-scan
                    # above, rather than a fresh `stat` here — one `stat`
                    # per mesh, not two. It is the `Content-Length` and
                    # nothing else now: this used to be checked against a
                    # per-file ceiling before `_upload_mesh_file` was even
                    # dispatched, and the store replaced that ceiling with
                    # a number only the cloud can evaluate.
                    size = sizes.get(uri)
                    result = await loop.run_in_executor(
                        None, self._upload_mesh_file, upload_url, token, sync_id, uri, asset_kind,
                        media_type, path, size,
                    )
                    if not result.ok:
                        record_failure(uri, *_upload_failure(result))
                remaining.pop(0)
                done += 1
                report()

                # this mesh's own internal texture references,
                # if it resolved to a `.dae` with any — uploaded right
                # after the mesh itself, before the next requested item,
                # matching the order `remaining` was built in above.
                #
                # `texture_path is None`: the reference was discovered
                # but could not be resolved — contained-but-missing or an
                # escape, and the two are *deliberately* indistinguishable
                # here, same as `_resolve_package_uri` failing anywhere
                # else in this method. Reported the same way any other
                # unresolvable reference is: the name goes in `failed`,
                # nothing is uploaded, nothing about *why* is on the wire.
                # Before this, `_extract_dae_texture_references` silently
                # dropped these instead of returning them — a `.dae` with
                # a typo'd or legitimately-missing internal reference
                # produced a *successful* sync with the surface simply
                # absent, and nothing anywhere named which reference or
                # said why. Structurally identical to the `missing`
                # problem this already existed to fix one
                # layer further in — this reference can never appear in
                # `urdfCompleteness.missing` either, because nothing in
                # the URDF itself ever names it.
                #
                # `texture_path is None` covers two different `kind`s
                # that happen to share a representation in `dae_textures` —
                # an ordinary reference that was attempted and could not be
                # resolved (`unresolvable`), and a ceiling sentinel that was
                # never attempted at all because a ceiling was hit
                # (`refused`). `refused_sentinels` is what tells them apart;
                # nothing else in this tuple's shape does.
                for texture_name, texture_path in dae_textures.get(uri, ()):
                    if texture_path is None:
                        failure_kind = (
                            ASSET_FAILURE_KIND_REFUSED
                            if texture_name in refused_sentinels
                            else ASSET_FAILURE_KIND_UNRESOLVABLE
                        )
                        failed.append((texture_name, failure_kind, None))
                    else:
                        # A `.dae`-internal texture is read and uploaded
                        # the same way as a top-level mesh, through the
                        # same `_upload_mesh_file`, and is charged against
                        # the same store — so it is attempted the same way
                        # too, with no local size check in front of it.
                        texture_size = await loop.run_in_executor(
                            None, self._file_size_or_none, texture_path
                        )
                        texture_media_type = mimetypes.guess_type(texture_name)[0] or "application/octet-stream"
                        texture_result = await loop.run_in_executor(
                            None, self._upload_mesh_file, upload_url, token, sync_id,
                            texture_name, "texture", texture_media_type, texture_path, texture_size,
                        )
                        if not texture_result.ok:
                            kind, details = _upload_failure(texture_result)
                            failed.append((texture_name, kind, details))
                    remaining.pop(0)
                    done += 1
                    report()
        except Exception:  # noqa: BLE001 - deliberately broad, see docstring
            log.exception(
                "asset sync %r ended on an unhandled exception — reporting a "
                "terminal frame for everything still unaccounted for (%d of "
                "%d) rather than leaving the cloud's sync waiting on a frame "
                "that will never arrive",
                sync_id, len(remaining), total,
            )
            # `refused`, chosen deliberately, not the least-considered
            # option. `remaining` at this point is bare names with no record
            # of which stage each was at — not confirmed absent from the
            # workspace (`unresolvable` would tell reconciliation it is safe
            # to drop something that might genuinely still exist), and not
            # confirmed to have had bytes attempted (`upload_failed` would
            # claim an attempt that may never have happened). `refused`'s
            # own meaning — "never attempted, nothing is known to be
            # missing, only unexamined" — is exactly true of an item an
            # aborted sync never reached, the same as it is of a
            # ceiling sentinel.
            #
            # routed through `record_failure`, not a direct `extend` —
            # an exception firing early in a sync with thousands of
            # `remaining` items (a URDF with more meshes than this bridge
            # has ever finished attempting) is the identical overflow risk
            # the mesh loop above already guards against, reached a
            # different way. Sharing `record_failure`'s counter with the
            # ordinary path means the two draw on one combined budget, not
            # two separate ones that could together still exceed the
            # contract's cap.
            for name in remaining:
                record_failure(name, ASSET_FAILURE_KIND_REFUSED)
            done = total
            report()

    @staticmethod
    def _resolve_package_uri(uri: str) -> Optional[str]:
        """Resolves a `package://pkg/relative/path` URI to an absolute
        filesystem path in this workspace, or `None` if it cannot be — the package does not
        exist, the file inside it does not, the URI itself is malformed, or
        (below) the path climbs out of the package's own share directory.
        Never raises: every caller of this is building a `failed` list, not
        a traceback, and an unresolvable URI is an expected, not
        exceptional, outcome of an incomplete workspace — a hostile one
        included, on purpose (see the containment check below).

        **Security (reproduced): `/robot_description` is ROS
        graph input, same reasoning `_extract_mesh_uris`' `defusedxml` choice
        already states — anything with publish access to this domain can
        put a message on it, so a `package://` URI is not first-party data
        either.** `relative_path` used to be joined onto the share directory
        and read with no check that the result stayed inside it:
        `package://ament_index_python/../../../../../etc/passwd` resolves
        `ament_index_python`'s real share directory, then climbs out of it
        with `../../../../../etc/passwd`, and the bridge dutifully read and
        uploaded `/etc/passwd` into the org's asset store — arbitrary file
        read on the robot, reachable by anything that can publish one topic,
        landing in a store anyone with the `assets` capability can read.
        Reproduced directly in the dev container: resolved path escaped, first line of
        `/etc/passwd` uploaded.

        The fix: resolve both the share directory and the candidate path
        through `os.path.realpath` (collapsing `..` and any symlink) and
        require the result to stay inside the share directory. A URI that
        escapes is rejected the same way as any other unresolvable one —
        `None`, landing in `failed` exactly like a typo'd package name —
        deliberately indistinguishable on the wire: a resolver that fails
        differently for hostile input tells an attacker they found
        something."""
        package_name, relative_path = RosRuntime._split_package_uri(uri)
        if package_name is None:
            return None
        return RosRuntime._resolve_within_package(package_name, relative_path)

    @staticmethod
    def _split_package_uri(uri: str) -> Tuple[Optional[str], Optional[str]]:
        """`package://pkg/relative/path` -> `("pkg", "relative/path")`, or
        `(None, None)` if `uri` is not a well-formed `package://` URI at
        all. Factored out of `_resolve_package_uri` so the naming logic
        (which needs a `.dae` URI's own `package_name` and relative path,
        not a resolved filesystem path) can parse the identical shape
        without re-deriving the rules for what counts as malformed."""
        if not uri.startswith("package://"):
            return None, None
        rest = uri[len("package://"):]
        if "/" not in rest:
            return None, None
        package_name, relative_path = rest.split("/", 1)
        if not package_name or not relative_path:
            return None, None
        return package_name, relative_path

    @staticmethod
    def _resolve_within_package(package_name: str, relative_path: str) -> Optional[str]:
        """The containment check itself, factored out so
        there is exactly one copy of it rather than a security-critical
        check reimplemented per caller. `_resolve_package_uri` above calls
        this with a `package://` URI's own `relative_path`; the `.dae`
        texture walk calls it a second way — an `<init_from>` reference, already
        joined against the `.dae`'s own directory *within the package*
        (never against the filesystem directly), so the same "must stay
        inside this package's share directory" rule applies to both
        without the second caller re-deriving it.

        `None` if the package does not exist, the file inside it does not,
        or the resolved path climbs out of the package's own share
        directory — see `_resolve_package_uri`'s own docstring for the
        traversal this closes and why an escape must fail identically to
        an ordinary unresolvable path rather than distinguishably."""
        try:
            share_dir = get_package_share_directory(package_name)
        except PackageNotFoundError:
            return None
        # **Lexical containment, deliberately NOT `realpath`, and the
        # difference is the whole of this comment.**
        #
        # The check this replaces resolved the candidate with
        # `os.path.realpath` and required the *physical* result to stay
        # inside the share directory. That closes the `..` traversal — and
        # it also rejects every mesh on any workspace built
        # with `colcon build --symlink-install`, which is the standard ROS
        # development workflow: colcon installs each file as a **symlink
        # back into the source tree**, so `realpath` of a perfectly ordinary
        # mesh lands in `<ws>/src/<pkg>/...` and fails containment against
        # `<ws>/install/<pkg>/share/<pkg>`.
        #
        # Seen on a real robot, and the failure is the worst shape
        # available: `os.path.isfile` is True, the bytes are readable, and
        # the reference is reported as `unresolvable` — which the console
        # renders as *"not found in the robot's workspace. Add these to the
        # robot's workspace, then sync again."* Ten meshes that were in the
        # workspace, named individually, with a remedy that cannot work
        # because the premise is false. **A false cause in exactly the field
        # built to end false causes.**
        # `normpath` collapses `..` without following symlinks, so every
        # traversal test still fails closed: a URI climbing out with
        # `../` lands outside `base` lexically and is rejected, and an
        # absolute `relative_path` is rejected because `join` discards
        # `base` and the result cannot start with it.
        #
        # **The residual, stated rather than implied away:** a symlink
        # *inside* an installed package that points outside it is now
        # followed. That is a different trust level from the one this check
        # exists for — the untrusted input here is the URI string arriving
        # on `/robot_description`, which anything with publish access can
        # set; the contents of an installed package are put there by whoever
        # provisions the robot. Closing that too would mean rejecting
        # `--symlink-install`, i.e. rejecting the normal case to defend
        # against an attacker who already has write access to the
        # workspace.
        base = os.path.normpath(share_dir)
        candidate = os.path.normpath(os.path.join(base, relative_path))
        if candidate != base and not candidate.startswith(base + os.sep):
            return None
        if not os.path.isfile(candidate):
            return None
        return candidate

    @staticmethod
    def _local_tag(tag: str) -> str:
        """Strips a `{namespace}` prefix off an ElementTree tag name.
        COLLADA (`.dae`) files carry a default `xmlns`, so every element's
        `.tag` arrives as `{http://www.collada.org/2005/11/COLLADASchema}
        init_from` rather than the bare `init_from` a namespace-free URDF
        gives `_extract_mesh_uris`. Matching on the local name only is
        deliberate: pinning the exact schema URI would break on the
        handful of COLLADA schema versions actually in the wild for no
        benefit — nothing else in this element's meaning depends on it."""
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    @staticmethod
    def _extract_dae_texture_references(dae_uri: str, dae_path: str) -> List[Tuple[str, Optional[str]]]:
        """Every image a `.dae` references internally via `<init_from>` — at any nesting
        depth (`root.iter()`, matching `local_tag == 'init_from'`, the
        same any-depth reasoning the `<mesh>`/`<texture>` walk already
        uses), not only directly under `<library_images>/<image>`.

        Returns `(name, resolved_path)` pairs, already named per the
        contracts rule (`asset.name`'s own doc comment):

            name = the .dae's package:// URI, directory part,
                   joined with the internal reference, normalized

        so `package://my_robot_description/meshes/arm.dae` referencing
        `textures/skin.png` names the result
        `package://my_robot_description/meshes/textures/skin.png`. Computed as
        a plain relative-path join+normalize (`posixpath`, not
        `os.path` — a `package://` URI's path is always `/`-separated
        regardless of this process's own OS) *before* the containment
        check runs, so the two agree by construction: the same
        `texture_relative_path` both names the asset and is what gets
        resolved against the package's share directory.

        **`resolved_path` is `None` for a reference this bridge could not
        deliver — contained-but-missing (a typo, a file genuinely absent
        from the workspace) or an escape, and the two are deliberately
        indistinguishable here, exactly as `_resolve_within_package`
        already makes them for a `package://` URI anywhere else in this
        class.** The name is still returned: naming is a string
        computation with no filesystem information in it, so returning it
        leaks nothing a hostile `/robot_description` did not already put
        there itself, and the caller (`_run_asset_sync`) reports it in
        `failed` the same way any other unresolvable reference already
        is. This used to drop these entirely — a `.dae` with a typo'd
        internal reference produced a **successful** sync with the
        surface silently absent, and nothing anywhere named which
        reference or said why, because such a reference can never appear
        in `urdfCompleteness.missing` either — nothing in the URDF itself
        ever names it (the attack on this same parser; the fix keeps the containment check exactly as it
        was — refused, not renamed — and stops it being silent about
        *that* on top of silent about *why*).

        Deduplicated within this one `.dae` (first-seen order, same
        reasoning `_extract_mesh_uris` already applies at the URDF
        level — a texture atlas, or a typo, is commonly repeated across
        several materials in one file). Never raises: a malformed
        `.dae`, one with no `<init_from>` at all, or a reference carrying
        its own URI scheme (`file://`, `http://`, `data:`, ...) — not a
        package-relative path, and skipped entirely rather than reported
        as unresolved (a `data:` URI in particular is already fully
        self-contained; there is nothing to sync and nothing missing) —
        all report zero or fewer textures, never abort the sync; the mesh
        itself still uploads regardless of whether its internal
        references parse."""
        package_name, dae_relative_path = RosRuntime._split_package_uri(dae_uri)
        if package_name is None:
            return []  # unreachable in practice: dae_uri already resolved to dae_path
        try:
            with open(dae_path, "rb") as handle:
                dae_bytes = handle.read()
        except OSError:
            log.exception("Could not read resolved .dae file %r for %r", dae_path, dae_uri)
            return []
        try:
            root = ElementTree.fromstring(dae_bytes)
        except (ElementTree.ParseError, DefusedXmlException):
            log.warning(
                "Could not parse %r as XML; reporting it with no internal textures",
                dae_uri, exc_info=True,
            )
            return []
        dae_dir = posixpath.dirname(dae_relative_path)
        seen: Dict[str, Optional[str]] = {}  # name -> resolved_path (None if unresolved), first-seen order
        for element in root.iter():
            if RosRuntime._local_tag(element.tag) != "init_from":
                continue
            internal_ref = (element.text or "").strip()
            if not internal_ref or "://" in internal_ref or internal_ref.startswith("data:"):
                continue  # not a package-relative reference — nothing here to resolve or report
            texture_relative_path = posixpath.normpath(posixpath.join(dae_dir, internal_ref))
            resolved_path = RosRuntime._resolve_within_package(package_name, texture_relative_path)
            name = "package://{}/{}".format(package_name, texture_relative_path)
            seen.setdefault(name, resolved_path)
        return list(seen.items())

    @staticmethod
    def _file_size_or_none(path: str) -> Optional[int]:
        """`os.stat`, not `open()` — a file's size without putting a byte
        of it in memory. It is the upload's `Content-Length` now and
        nothing else; it used to be checked against a per-file ceiling
        first, which the per-robot store replaced.

        `None` on a race — the file vanished or became unreadable between
        `_resolve_package_uri` confirming it and this stat — is
        deliberately *not* treated as a failure of its own here: the caller
        falls through to the ordinary upload attempt, which already has its
        own honest handling of that exact race (see `_upload_mesh_file`'s
        own docstring). Inventing a new failure meaning for "could not even
        stat it" would be a second, narrower race-handling path next to one
        that already exists."""
        try:
            return os.path.getsize(path)
        except OSError:
            return None

    @staticmethod
    def _upload_mesh_file(
        upload_url: str,
        token: str,
        sync_id: str,
        uri: str,
        kind: str,
        media_type: str,
        path: str,
        size: Optional[int] = None,
    ) -> UploadResult:
        """Uploads `path` **streamed**, not read into memory first — always
        run via `run_in_executor`, never on the asyncio
        thread, same as before. `size` is normally already known: the
        caller (`_run_asset_sync`) stated the file for the `Content-Length`
        immediately before this call, and passing that number through
        avoids a second `stat`. `None` re-derives it here (a standalone
        caller, or the narrow race where the caller's own stat failed and fell
        through to attempt the upload anyway) — see `_upload_asset_stream`
        for what happens if *that* stat also fails.

        A plain `open()`ed file handed to `urllib.request.Request` as
        `data` does **not** get its `Content-Length` auto-computed the way
        `bytes` does — sent with no explicit header, the client streamed the
        body anyway while the
        server, seeing `Content-Length: 0`, read nothing and moved on,
        producing a `BrokenPipeError` on a real loopback round-trip. A
        60 MB file read into memory first measured 76.8 MB of peak RSS;
        streamed with an explicit `Content-Length`, 19.5 MB — accounted for
        by the rest of the process, not by this upload.

        A file that vanished or became unreadable between
        `_resolve_package_uri` confirming it and this call (a real, if
        narrow, race — nothing holds a lock on the workspace between the
        two) fails the same honest way an upload failure does: this URI in
        `failed`, not an unhandled exception on a background thread.

        `kind`: `'mesh'` or `'texture'`, the caller's own
        classification — this function resolves and uploads bytes, it does
        not know or care which XML element `uri` came from."""
        if size is None:
            size = RosRuntime._file_size_or_none(path)
            if size is None:
                log.exception("Could not stat resolved mesh file %r for %r", path, uri)
                return UploadResult(False)
        return RosRuntime._upload_asset_stream(upload_url, token, sync_id, kind, uri, media_type, path, size)

    @staticmethod
    def _refused(name: str, exc: "urllib.error.HTTPError") -> UploadResult:
        """A refusal from the cloud (`ASSET_REFUSAL_STATUSES`), logged and
        classified.

        Its own branch rather than a line in each caller because both
        uploaders have to say the same thing about it, and because neither
        status is a transfer failure: the cloud declined to keep these
        bytes, and telling a reconciliation to retry would mean retrying
        forever for a file that can never fit."""
        details = RosRuntime._store_refusal_details(exc)
        if details is None and exc.code == 413:
            log.warning(
                "Asset upload for %r refused: the cloud would not accept a "
                "body that large. Nothing about retrying changes that",
                name,
            )
        elif details is None:
            log.warning(
                "Asset upload for %r refused (HTTP %d): the refusal did not "
                "say how much of the robot's asset store is left",
                name, exc.code,
            )
        else:
            log.warning(
                "Asset upload for %r refused: %d bytes does not fit the "
                "robot's %d-byte asset store, %d of which is in use",
                name, details["size_bytes"], details["store_bytes"], details["used_bytes"],
            )
        return UploadResult(False, refused=True, details=details)

    @staticmethod
    def _store_refusal_details(exc: "urllib.error.HTTPError") -> Optional[Dict[str, int]]:
        """The cloud's `{store_bytes, used_bytes, size_bytes}` out of a
        `409`'s body, or `None` when it does not say.

        Read, not derived: this process cannot know what the robot's other
        assets already occupy, which is the whole reason the cap moved to
        the cloud. So every shape this cannot read — an older cloud, a
        proxy's error page, a body too large to be one of these — comes
        back `None`, and `UploadResult` still reports `refused`. Reading
        the body is bounded (`ASSET_REFUSAL_BODY_MAX_BYTES`) because an
        error body is not a payload, and an unbounded `read()` on one is
        the same mistake the per-file ceiling existed to prevent.

        All three keys or none: a partial `details` would satisfy neither
        the contract nor a reader, and two thirds of an answer is worse
        than admitting there is none."""
        try:
            raw = exc.read(ASSET_REFUSAL_BODY_MAX_BYTES)
            body = json.loads(raw.decode("utf-8"))
        except Exception:  # noqa: BLE001 - any unreadable body means "it did not say"
            return None
        details = body.get("details") if isinstance(body, dict) else None
        if not isinstance(details, dict):
            return None
        numbers = {}
        for key in ("store_bytes", "used_bytes", "size_bytes"):
            value = details.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                return None
            numbers[key] = value
        return numbers

    @staticmethod
    def _asset_upload_headers(
        token: str, sync_id: str, kind: str, name: str, media_type: str, size: int
    ) -> Dict[str, str]:
        """Builds the header dict shared by `_upload_asset_bytes` and
        `_upload_asset_stream` — everything both need except `data` itself
        and (for the stream variant) `Content-Length`, which each caller
        adds on top since only one of the two needs it stated explicitly.

        **`size` is announced, not only sent.** It is the same number as
        `Content-Length`, and it is here as well because the two are read
        at different moments: the cloud weighs this header against the
        robot's store *before* it accepts a body, and refuses a file that
        cannot fit with a `409` naming the store's three numbers. Without
        it nothing is weighed, and a file over the server's own body limit
        comes back as a bare `413` with nothing in it — which this bridge would file
        as a transfer failure and retry on every sync, forever, for a file
        that can never fit. Both callers already know the number, so
        neither pays a `stat` for it.

        Header names from `ASSET_UPLOAD_HEADERS` (vendored from the wire
        contracts' own `constants.json`), not hand-typed and not from
        a message — that constant's own docstring exists because "the
        bridge and the cloud agreed on one in conversation" is exactly
        the thing that drifts, and this package has been bitten by that
        shape before (the `x-fleetless-*` CORS gap, the three-repos-
        agreeing-about-a-payload-none-of-them-exchanged). `urllib.request.
        Request` sends `Key.capitalize()` regardless of the case used
        here — HTTP headers are case-insensitive on the wire either way.

        **Fixed rather than deferred**, and redesigned once already — the
        second header is the point, not an implementation detail.** The first
        version overloaded `name`
        itself: percent-encode on the way out, `decodeURIComponent` on the
        way in. The flaw is in the *contract*, not
        the code: `decodeURIComponent` is the identity for any string with
        no `%` in it, so an older bridge (sending a raw name) and a newer
        store (always decoding) would have agreed **by luck** — right up
        until a name genuinely contained `%2f`, which the store would then
        silently turn into a `/`. A wire change whose breakage is
        invisible in the common case and silent in the rare one is worse
        than either failure alone.

        So `name` keeps meaning exactly what it always meant, and a
        **second** header — `ASSET_UPLOAD_HEADERS['nameEncoded']` — carries
        the percent-encoded UTF-8 form, always sent alongside it. No value
        is ever ambiguous about which encoding it is in; a store that has
        not been taught about `nameEncoded` keeps working off `name`
        exactly as before. `urllib.parse.quote(name, safe='')` matches
        `encodeURIComponent` byte for byte (verified: identical output for
        a CJK name).

        `http.client.HTTPConnection.putheader` encodes every header value
        as Latin-1, so `name` itself must stay Latin-1-safe even when the
        real name is not — this is the one place the two headers are
        allowed to disagree, so it is worth saying plainly what `name`
        carries when they do: `name.encode('latin-1', errors='replace')`,
        a lossy best-effort (`?` for anything outside Latin-1) rather than
        an empty or truncated string, so an older consumer reading `name`
        alone still gets something legible and matchable to a workspace
        file, just not an exact one. `nameEncoded` is always the truth.

        Raises `UnicodeError` only for whatever this function has not been
        asked to encode (`media_type` is not, on the reasoning that a
        non-ASCII MIME type is not a real case worth widening the wire
        for) — both callers keep the guard around their own call to
        this, as a backstop for exactly that."""
        try:
            name.encode("latin-1")
            safe_name = name
        except UnicodeEncodeError:
            safe_name = name.encode("latin-1", errors="replace").decode("latin-1")
        return {
            "authorization": "Bearer {}".format(token),
            "content-type": media_type,
            ASSET_UPLOAD_HEADERS["kind"]: kind,
            ASSET_UPLOAD_HEADERS["name"]: safe_name,
            # Percent-encoded — `encodeURIComponent`'s exact
            # counterpart, not a hand-rolled scheme; see the docstring.
            ASSET_UPLOAD_HEADERS["nameEncoded"]: urllib.parse.quote(name, safe=""),
            ASSET_UPLOAD_HEADERS["syncId"]: sync_id,
            ASSET_UPLOAD_HEADERS["size"]: str(size),
        }

    @staticmethod
    def _upload_asset_bytes(
        upload_url: str, token: str, sync_id: str, kind: str, name: str, media_type: str, data: bytes
    ) -> UploadResult:
        """One `POST` to the cloud's per-sync `upload_url`, the robot's
        credential for this sync in `Authorization: Bearer` (never a signed
        URL, never a query-string token — the one authorization model).
        Always called via `run_in_executor`, never
        directly on the asyncio thread: `urllib.request` is blocking, same
        reasoning as `camera_sources.py`'s MJPEG adapter, which is also
        where this package's `urllib` precedent lives (no new HTTP
        dependency for a bridge that already has one working, blocking
        client).

        `data` is bytes, whole and in memory — the right shape for the
        URDF text (already cached in memory regardless does not check
        it — see `_run_asset_sync`'s own reasoning) but not for a mesh or
        texture file; see `_upload_asset_stream` for that path, added
        alongside this one rather than in place of it.

        Returns `True` on any 2xx, `False` on anything else — a non-2xx
        status (other than a retried-and-exhausted `429`, see below), a
        timeout, a connection failure, or `name`/`media_type` containing
        a character `http.client` cannot put in a header (see
        `_asset_upload_headers`). Every failure reason collapses to the
        same `failed` entry on the wire; this function's job is only to
        try and report whether it worked, not to explain why it did not
        (the log line is for a human looking at this process, not for the
        cloud).

        A `429 rate_limited` is retried, not treated as an
        ordinary failure. The cloud's refusal carries `retry_after_ms`
        (`rateLimitDetails`, contracts) — this waits exactly that long and
        retries the identical request, up to
        `ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES` times. A URDF with more
        meshes than the bucket's burst capacity must still sync completely
        completely, not report resolvable meshes as missing because the
        bucket happened to be empty when this bridge got to them.
        Deliberately **not** applied to a timeout or connection failure —
        only a `429` — so a genuinely unreachable cloud still fails within
        `ASSET_UPLOAD_TIMEOUT_S`, unchanged: a retry-on-timeout would
        compound a 60s wait with a further retry sleep on top of it,
        widening the gap between `asset_progress` frames for no benefit
        (an unreachable cloud does not become reachable by waiting a
        fraction of a second, the way an emptied token bucket does)."""
        headers = RosRuntime._asset_upload_headers(
            token, sync_id, kind, name, media_type, len(data)
        )
        request = urllib.request.Request(upload_url, data=data, method="POST", headers=headers)
        # `request` (bytes `data`, not a stream) is safe to hand to
        # `urlopen` more than once — nothing here consumes it. Contrast
        # `_upload_asset_stream`, which cannot reuse one `Request` this way.
        for attempt in range(ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                with urllib.request.urlopen(request, timeout=ASSET_UPLOAD_TIMEOUT_S) as response:
                    return UploadResult(200 <= response.status < 300)
            except urllib.error.HTTPError as exc:
                if exc.code == 429 and attempt < ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES:
                    wait_s = RosRuntime._rate_limit_retry_wait_s(exc)
                    log.info(
                        "Asset upload for %r rate-limited (attempt %d/%d) — "
                        "waiting %.3fs before retrying",
                        name, attempt + 1, ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES, wait_s,
                    )
                    time.sleep(wait_s)
                    continue
                if exc.code == 429:
                    log.warning(
                        "Asset upload for %r still rate-limited after %d retries — giving up",
                        name, ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES,
                    )
                    return UploadResult(False)
                if exc.code in ASSET_REFUSAL_STATUSES:
                    return RosRuntime._refused(name, exc)
                log.warning("Asset upload for %r failed: HTTP %d", name, exc.code)
                return UploadResult(False)
            except urllib.error.URLError as exc:
                log.warning("Asset upload for %r failed: %s", name, exc.reason)
                return UploadResult(False)
            except UnicodeError as exc:
                # `http.client` can't put this name (or
                # media_type) in a header at all — not a network failure
                # and not worth retrying, since the same name will fail
                # the same way every time. `%r` in the log line is safe
                # regardless: `repr()` on a str never touches Latin-1.
                log.warning(
                    "Asset upload for %r failed: name is not representable "
                    "in a header (%s)", name, exc,
                )
                return UploadResult(False)
        return UploadResult(False)  # unreachable — the loop above always returns

    @staticmethod
    def _upload_asset_stream(
        upload_url: str, token: str, sync_id: str, kind: str, name: str, media_type: str, path: str, size: int
    ) -> UploadResult:
        """`_upload_asset_bytes`'s counterpart for a file already on disk
 — a mesh or `.dae`-internal texture, streamed rather than
        read into memory first. Same status-code contract, same retry
        policy, same headers (`_asset_upload_headers`); this differs
        from `_upload_asset_bytes` in exactly two ways, both forced by
        measurement rather than assumption:

        1. **`Content-Length` must be set explicitly.** `bytes` gets its
           length computed automatically by `urllib.request`; a plain file
           object does not — measured (2026-08-19): with no explicit
           header, the client streamed the body anyway while the server
           (seeing `Content-Length: 0`) read nothing and closed, producing
           a `BrokenPipeError` on an ordinary loopback round-trip. `size`
           is a parameter rather than re-derived here because the caller
           (`_upload_mesh_file`) already has it from its own ceiling
           check — one `stat`, not two.

        2. **The file is reopened for every retry attempt, never reused.**
           `_upload_asset_bytes`'s own comment about handing one `Request`
           to `urlopen` more than once does not hold for a stream: HTTP
           sends the whole body before any response — including a `429` —
           is read, so by the time a retry decision is made the file's
           cursor is already at EOF. Retrying the same
           already-drained file object sent zero bytes against a
           `Content-Length` that still claimed the original size, and the
           server hung reading a body that would never arrive, until the
           client's own timeout fired — the exact "sync that never ends"
           shape `_run_asset_sync` exists to rule out, reintroduced one
           layer in. Reopening fresh each attempt (rather than seeking one
           handle back to 0) needs no state carried between iterations and
           cannot forget the rewind."""
        headers = dict(
            RosRuntime._asset_upload_headers(token, sync_id, kind, name, media_type, size)
        )
        headers["Content-Length"] = str(size)
        for attempt in range(ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES + 1):
            try:
                with open(path, "rb") as handle:
                    request = urllib.request.Request(upload_url, data=handle, method="POST", headers=headers)
                    with urllib.request.urlopen(request, timeout=ASSET_UPLOAD_TIMEOUT_S) as response:
                        return UploadResult(200 <= response.status < 300)
            except urllib.error.HTTPError as exc:
                if exc.code == 429 and attempt < ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES:
                    wait_s = RosRuntime._rate_limit_retry_wait_s(exc)
                    log.info(
                        "Asset upload for %r rate-limited (attempt %d/%d) — "
                        "waiting %.3fs before retrying",
                        name, attempt + 1, ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES, wait_s,
                    )
                    time.sleep(wait_s)
                    continue
                if exc.code == 429:
                    log.warning(
                        "Asset upload for %r still rate-limited after %d retries — giving up",
                        name, ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES,
                    )
                    return UploadResult(False)
                if exc.code in ASSET_REFUSAL_STATUSES:
                    return RosRuntime._refused(name, exc)
                log.warning("Asset upload for %r failed: HTTP %d", name, exc.code)
                return UploadResult(False)
            except urllib.error.URLError as exc:
                log.warning("Asset upload for %r failed: %s", name, exc.reason)
                return UploadResult(False)
            except UnicodeError as exc:
                # same guard as `_upload_asset_bytes` —
                # see `_asset_upload_headers`.
                log.warning(
                    "Asset upload for %r failed: name is not representable "
                    "in a header (%s)", name, exc,
                )
                return UploadResult(False)
            except OSError:
                # `open()` itself failing — the file vanished or became
                # unreadable since `_resolve_package_uri` confirmed it, the
                # same narrow race `_upload_mesh_file` already documents,
                # just possibly hit again on a later retry attempt rather
                # than only the first. Caught last and deliberately: both
                # `urllib.error.URLError` and its `HTTPError` subclass are
                # themselves `OSError` subclasses, so this clause only ever
                # sees what the two more specific ones above did not.
                log.exception("Could not open resolved asset file %r for %r", path, name)
                return UploadResult(False)
        return UploadResult(False)  # unreachable — the loop above always returns

    @staticmethod
    def _rate_limit_retry_wait_s(exc: "urllib.error.HTTPError") -> float:
        """Reads `retry_after_ms` off a `429`'s JSON body
        (`rateLimitDetails`, contracts: `{"code": "rate_limited", ...,
        "details": {"retry_after_ms": N}}`) — falls back to
        `ASSET_UPLOAD_RATE_LIMIT_FALLBACK_WAIT_S` for anything that is not
        exactly that shape (malformed JSON, a missing field, a body that
        is not JSON at all), so a cloud response this bridge cannot parse
        still produces a bounded wait rather than raising. The refusal
        itself is real even when its details are not."""
        try:
            body = json.loads(exc.read().decode("utf-8"))
            return max(0.0, float(body["details"]["retry_after_ms"]) / 1000.0)
        except Exception:  # noqa: BLE001 - deliberately tolerant, see docstring
            return ASSET_UPLOAD_RATE_LIMIT_FALLBACK_WAIT_S

    def _emit_job(
        self,
        job_id: str,
        slug: str,
        state: str,
        *,
        feedback: Any = None,
        progress: Optional[float] = None,
        result: Any = None,
        error: Optional[Tuple[str, str]] = None,
        details: Any = None,
        timestamp_ms: Optional[int] = None,
    ) -> None:
        self.jobs.emit(
            self._loop,
            JobUpdate(
                job_id=job_id,
                slug=slug,
                state=state,
                timestamp_ms=timestamp_ms if timestamp_ms is not None else sampling.capture_timestamp_ms(),
                feedback=feedback,
                progress=progress,
                result=result,
                error=error,
                details=details,
            ),
        )
