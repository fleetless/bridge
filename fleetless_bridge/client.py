# SPDX-License-Identifier: Apache-2.0
"""The bridge's connection to the cloud.

One long-lived WebSocket carries the whole conversation: a hello handshake
binds the connection to a robot, then the cloud drives a round-trip probe the
bridge echoes, pushes the published configuration, asks for introspection and
type data, and receives a stream of datapoint samples back.

The socket is aiohttp's, wrapped by `_Socket` below into the four calls the
rest of this file makes — `send`, `recv`, `close` and `close_code`. That
wrapper is the whole of what this file knows about the transport, and most of
the session's clocks live here too: `_handshake_timeout`, `_idle_timeout` and
`CLOSE_TIMEOUT_S` are applied through `asyncio.wait`/`asyncio.wait_for`, not
handed to the library. `KEEPALIVE_INTERVAL_S` is the one clock that is handed
to the library — aiohttp's own heartbeat, which gives up on the pong at
`KEEPALIVE_INTERVAL_S / 2` past the heartbeat itself. At the values below that
lands at exactly `IDLE_TIMEOUT_S`, a tie rather than a timer clearly slacker
than this file's own — see `KEEPALIVE_INTERVAL_S`'s own comment for what that
means when both are due at once.

`ros` (a `RosRuntime`, see ros_runtime.py) is optional and duck-typed rather
than imported for its type, the same way `connect`/`sleep` are injectable —
tests exercise the handshake/ping/reconnect behaviour without any ROS
runtime at all. Three things run concurrently for the life of a session: the
receive loop below (hello/ping/config/introspect/type/invoke/cancel/publish/
camera_start/camera_stop/asset_request), `_pump_control` (applies configs and
answers introspection out of the receive loop's way), and one
`PrioritizedWriter` — **the only caller of `ws.send()`**.

The six outgoing pumps this replaced each called `ws.send()` themselves.
Frames never interleaved mid-write — the library writes one frame in one
transport write; what nothing did was choose an *order*, and a snapshot ahead
of a `pong` in send order is what starved a real robot's control channel.
Now every producer feeds the writer instead — as a payload (`enqueue`) or as a
pull source (`ros.samples`, `ros.backlog`, `ros.jobs.updates`,
`ros.camera_states`, `ros.assets`, `ros.asset_progress`, and snapshots
pulled from `ros.next_snapshot`) — and the writer drains them in strict
tier order. See `PrioritizedWriter` and the `_TIER_*` constants.

`hello.active_jobs` (renamed and widened) is how the
cloud tells a reconnect from a restart: this process's own `RosRuntime.jobs`
is asked what it still has *every* time hello is sent, not just the first —
a job that finished and was delivered between one hello and the next must
stop being named, and a job started after a mid-session reconnect (there is
no such thing today, but nothing here assumes otherwise) would need to
start being named.

`ros.set_connected(...)` brackets every session: `True`
right after `hello_ok`, `False` in `_converse`'s `finally`, regardless of
how the session ended. This is what tells the subscription callback in
ros_runtime.py whether a sample is live (goes straight to `ros.samples`) or
buffered/dropped (`ros.backlog` or nowhere, depending on that datapoint's
config) — no writer source has to ask the connection state itself; live
samples and backfill simply sit in different tiers, and strict priority
does the rest.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import enum
import logging
import random
import time
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

import aiohttp

from fleetless_bridge import __version__
from fleetless_bridge.camera import SNAPSHOT_MAX_BYTES
from fleetless_bridge.config import BridgeConfig
from fleetless_bridge.link_mode import LinkMode, LowBandwidthSettings, Transition
from fleetless_bridge.protocol import (
    APPLY_ERROR_CODE_LOW_BANDWIDTH_INVALID,
    APPLY_ERROR_CODE_WHOLE_KIND_FAILED,
    APPLY_ERROR_KIND_ACTION,
    APPLY_ERROR_KIND_CAMERA,
    APPLY_ERROR_KIND_DATAPOINT,
    APPLY_ERROR_KIND_PUBLISHER,
    APPLY_ERROR_KIND_SERVICE,
    CLOSE_CODE_ROBOT_DELETED,
    CLOSE_CODE_SUPERSEDED,
    CLOSE_CODE_TOKEN_ROTATED,
    PROTOCOL_VERSION,
    VERSION_REFUSED_CODE,
    ApplyError,
    CloudAssetRequest,
    CloudCameraStart,
    CloudCameraStop,
    CloudCancel,
    CloudInvoke,
    CloudPublish,
    Config,
    HelloError,
    HelloOk,
    IntrospectRequest,
    Ping,
    TypeRequest,
    bridge_asset_progress_message,
    bridge_assets_available_message,
    bridge_camera_state_message,
    config_applied_message,
    datapoint_message,
    hello_message,
    introspect_message,
    job_update_message,
    link_mode_message,
    parse_cloud_message,
    pong_message,
    type_definitions_message,
)
from fleetless_bridge.sampling import capture_timestamp_ms

log = logging.getLogger(__name__)

# How long the cloud may take to answer a hello before we give up on the
# socket. Without this, a cloud that accepts the TCP connection and then says
# nothing would keep the bridge waiting forever.
HANDSHAKE_TIMEOUT_S = 10.0

# How long a connection may stay silent before we treat it as dead. The cloud
# pings far more often than this, but the bridge deliberately does not encode
# the cloud's ping cadence — this only has to bound how long a half-open socket
# can keep pretending to be alive.
IDLE_TIMEOUT_S = 30.0

BACKOFF_INITIAL_S = 1.0
BACKOFF_FACTOR = 2.0
BACKOFF_CAP_S = 30.0

# A refused protocol version is not a network hiccup: the fix is a newer
# package on disk, which this process can never load. So it waits — far
# longer than an ordinary reconnect, because nothing it does in the meantime
# can help — and then exits, and the launch file's respawn is what picks the
# upgrade up. One flat wait, equally jittered, so 60 s to 120 s; with the
# launch file's 5 s respawn that is one attempt every 65 s to 125 s, and an
# installed upgrade takes effect inside about two minutes. It does not grow:
# a process takes this wait once and then exits, so there is no schedule to
# climb.
REFUSAL_WAIT_S = 120.0

CLOSE_TIMEOUT_S = 5.0

# `PrioritizedWriter`'s occupancy budget — one prioritized writer owns the
# socket: how long any tier <= 4 payload may project to
# take before it is sent anyway, with only a warning.
#
# 2.0's `SNAPSHOT_MAX_SOCKET_SECONDS` lives on as this number, generalized
# from one frame kind to every tier. Its reasoning is unchanged and worth
# keeping: one writer owns the socket, so every second any frame spends
# on the wire is a second the `pong` queued behind it is not being sent —
# and the cloud pings every 2 s and closes the socket after three
# unanswered ones, so the whole session has about six seconds of slack for
# everything. A third of that, because the rate estimate is an estimate:
# being wrong by a factor of two must still not be what ends a session.
#
# `camera.SNAPSHOT_MAX_BYTES` bounds a snapshot in bytes, against the ws
# library's own 2 MiB ceiling. That bound knows nothing about how long
# those bytes take to leave the robot: on a 151 kB/s uplink it
# is ten seconds of exclusive use of the one socket that also carries the
# control channel. `snapshot_max_bytes()` is where the two meet.
MAX_SEND_OCCUPANCY_S = 2.0

# Only a send at least this large says anything about the *link*.
#
# `asyncio`'s default transport write high-water mark is 64 KiB. A send
# below it is accepted straight into the write buffer and returns before a
# single byte has had to leave the robot, so what `_record_send` times for
# it is per-send CPU, not drain — only a send that outlives the buffer
# measures the wire at all. This suite measured exactly that and wrote the
# number down: `test_client_writer_wiring.py:81-88` records 50 backfill
# frames of 400 kB each, 20 MB in total, all accepted by `send()` over
# loopback before a ping sent after the second frame had round-tripped.
#
# What folding every send cost, since it is the reason this bound exists:
# on a 151 kB/s uplink the steady estimate landed somewhere
# between 300 kB/s and 3.8 MB/s depending on per-send overhead, which pins
# `snapshot_max_bytes()` at the flat `SNAPSHOT_MAX_BYTES` ceiling — the
# fitted-snapshot design inert, and 2.0.2's own guard already deleted in
# its favour.
#
# The consequence, stated rather than implied: the estimate stays `None`
# until the first send this large, so `snapshot_max_bytes()` returns the
# wire-format ceiling and the **first snapshot is what establishes the
# rate** — precisely 2.0.2's original seeding semantics, which seeded from
# snapshots and nothing else. `_send`'s tier <= 4 occupancy warning is
# silent for the same window, and that is the right silence: a warning
# whose whole content is a rate cannot honestly fire before there is one.
RATE_SAMPLE_MIN_BYTES = 65536

# What a stalled ROS work queue looks like arriving out of the tier-5 pull.
# `RosRuntime._submit_async` waits on a `concurrent.futures.Future` through
# `run_in_executor(None, future.result, timeout)`, so a work queue that did
# not reach this snapshot within `WORK_TIMEOUT_S` (ten seconds) surfaces at
# `pull.result()` as `concurrent.futures.TimeoutError`.
#
# All three timeout classes are named because on Python 3.10 they are three
# distinct classes: `concurrent.futures.TimeoutError is TimeoutError` is
# `False`, and so is `asyncio.TimeoutError is concurrent.futures.
# TimeoutError`. They become aliases in 3.11. 3.10 is the interpreter of the
# OLDEST distribution this one source is built for — Humble's, checked in
# this package's own test container — and the newer two (Jazzy 3.12, Lyrical
# 3.14) are past the change, so on those catching one type would do. Naming
# all three is what makes one source right on every distribution, and it also
# says more than it has to: "a pull that timed out", whichever layer reported
# it.
_PULL_TIMEOUT_ERRORS = (
    concurrent.futures.TimeoutError,
    asyncio.TimeoutError,
    TimeoutError,
)

# The tiers, exhaustively — the ordering the product asked for: session
# liveness, then job outcomes an operator is waiting on, then telemetry and
# camera health, then developer
# tooling, then buffered history, then snapshots.
#
# Backfill's tier 4 is the placement the design weighed rather than
# assumed: beside live datapoints it would let a reconnect after a long
# outage stall an introspect click; below snapshots it would invert what
# retention means, since a snapshot is expendable by design and buffered
# history was explicitly asked for. Between them keeps every interactive
# path responsive and still drains the durable data first.
#
# `BACKFILL_MIN_INTERVAL_S`, 2.0's 50 ms floor between backfill sends, is
# retired with this: its one job — buffered history must never crowd out
# live traffic — is now structural, because every live frame sits in a
# higher tier and preempts the drain between any two items. What bounded
# how long a drain could monopolize the link is the backlog's own
# `buffer.max_values`, already enforced drop-oldest per slug.
_TIER_SESSION = 0     # pong, hello
_TIER_OUTCOME = 1     # config_applied, job_update
_TIER_TELEMETRY = 2   # datapoint (live), camera_state
_TIER_TOOLING = 3     # introspect, type_definitions, assets_available, asset_progress
_TIER_BACKFILL = 4    # datapoint (backfill)

# How often the low-bandwidth controller is asked to decide. Its timers
# advance only inside `evaluate`, so this is the resolution of `enter_after_s`
# and `exit_after_s` — one second against thresholds counted in whole seconds,
# which costs one function call a second and nothing on the wire.
LINK_MODE_TICK_S = 1.0

# Tier 5: pulled via `snapshot_source.next(max_bytes)`, never pushed through
# `enqueue()` or a `sources` entry — see `PrioritizedWriter.run`.
_SNAPSHOT_TIER = 5

# How often `PrioritizedWriter.run` re-checks for work when idle. This is
# what makes a due snapshot observable without busy-polling: without a
# ceiling on the wake-event wait, a session with a `snapshot_source` but no
# push/pull traffic at all would block on the event forever and never once
# ask for a frame.
_WRITER_TICK_S = 0.25

# Returned by _recv instead of a frame.
_STOPPED = object()
_TIMED_OUT = object()

# The largest frame this bridge will accept FROM the cloud. Every message the
# cloud sends is small — the biggest is a config document — so this is a
# ceiling and not a budget: a frame past it means something upstream is wrong,
# and the session ends rather than the robot buffering a megabyte of it.
# The bridge's own outgoing ceiling is a different number and lives elsewhere
# (`camera.SNAPSHOT_MAX_BYTES`, fitted per frame by `snapshot_max_bytes()`).
MAX_INCOMING_FRAME_BYTES = 1 << 20

# How long a socket may be completely silent before the library sends a
# WebSocket ping of its own, and half of which it then allows for the pong.
#
# **This exists because a robot's uplink sits behind NAT** and a genuinely
# idle TCP connection is reclaimed by middleboxes without either end being
# told; the cloud pings every 2 s, so on a healthy session this timer is
# reset long before it ever fires.
#
# **It is a deadline, not only a keepalive, and at these values it is not
# slacker than `IDLE_TIMEOUT_S` — it is the same deadline.** aiohttp derives
# its own pong deadline as `heartbeat / 2` from this value, so the library
# gives up at `KEEPALIVE_INTERVAL_S + KEEPALIVE_INTERVAL_S / 2` = 30 s past
# the last activity — exactly `IDLE_TIMEOUT_S` above, not a looser bound
# around it. Which of the two fires first on a given run is a scheduling
# race, not a decision this file makes: when the library's wins, `_converse`
# sees a closed socket and logs "the cloud closed the connection (code
# 1006)" — the same line a killed cloud produces — instead of "nothing
# received for 30 s", and an operator reading the robot's journal cannot
# tell a hung cloud from this file's own keepalive giving up. No test in
# this suite can see the tie: `helpers.make_client` overrides
# `idle_timeout=2.0` for every test, so the library timer never competes
# with this file's own in anything here.
KEEPALIVE_INTERVAL_S = 20.0


class ConnectionClosed(aiohttp.ClientConnectionError):
    """The socket is gone: the peer sent a close frame, or the transport
    ended under us.

    It carries no code of its own — `_Socket.close_code` is where the peer's
    close code is read, because that is the value the library maintains and
    a second copy on the exception would be a second answer to one question.
    """


class ConnectionFailed(aiohttp.ClientError):
    """The socket failed rather than closed: a protocol error, a frame over
    `MAX_INCOMING_FRAME_BYTES`, or anything else the reader could not carry
    on after.

    This class exists because of one measured detail: aiohttp reports such a
    failure as a `WSMsgType.ERROR` message whose payload is an
    `aiohttp.WebSocketError`, and that class inherits from `Exception`
    directly — it is neither an `OSError` nor an `aiohttp.ClientError`. An
    adapter that re-raised it unchanged would raise something none of this
    file's four catch sites names, and the session would die with a
    traceback instead of reconnecting. So `_Socket.recv` raises only this,
    `ConnectionClosed`, an `OSError` or an `aiohttp.ClientError`, and every
    catch site below names that set.
    """


class _Socket:
    """One WebSocket, reduced to the four things this file asks of it.

    `send`, `recv`, `close` and `close_code` are the whole surface. Most of
    the tests' own fakes implement only the three this file's own read/send
    path actually calls (`send`, `recv`, `close`) -- `close_code` is read by
    `_converse`'s own close-reason branch, not by anything a fake stands in
    for elsewhere, so a fake missing it is a stand-in for those three calls
    only, not for this class whole. A test that reaches `_converse`'s
    close-reason branch through a fake needs one that has it.

    It owns the `aiohttp.ClientSession` the socket was opened on, because
    aiohttp's session owns the connector and the socket dies with it: closing
    the socket without closing the session leaks a connector per reconnect,
    and the bridge reconnects for a living.
    """

    def __init__(self, session: "aiohttp.ClientSession", ws) -> None:
        self._session = session
        self._ws = ws

    @property
    def close_code(self) -> Optional[int]:
        """The peer's close code, once there is one. `None` until then."""
        return self._ws.close_code

    async def send(self, payload) -> None:
        """Text as text, bytes as binary — the distinction is on the wire and
        the cloud reads it (a snapshot is binary, everything else is JSON)."""
        if isinstance(payload, str):
            await self._ws.send_str(payload)
        else:
            await self._ws.send_bytes(payload)

    async def recv(self):
        """One frame: `str` for text, `bytes` for binary.

        Every other message type is an end, not a frame, and leaves here as
        an exception — see `ConnectionFailed` for why none of them may leave
        as itself.
        """
        message = await self._ws.receive()
        if message.type is aiohttp.WSMsgType.TEXT:
            return message.data
        if message.type is aiohttp.WSMsgType.BINARY:
            return message.data
        if message.type is aiohttp.WSMsgType.ERROR:
            error = message.data if isinstance(message.data, BaseException) else None
            if isinstance(error, (OSError, aiohttp.ClientError)):
                # Already one of the two kinds the catch sites name; re-raising
                # it keeps whatever it says about the failure.
                raise error
            raise ConnectionFailed(str(error) if error is not None else "socket error") from error
        if message.type in (
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.CLOSED,
        ):
            raise ConnectionClosed("the connection is closed")
        # PING and PONG do not reach here: `autoping` is left at its default,
        # so the library answers a ping and swallows a pong inside `receive()`
        # itself. Anything else is a message type this file has never seen,
        # and guessing whether it's a "frame" or an "end" would be wrong.
        raise ConnectionFailed("unexpected websocket message {}".format(message.type))

    async def close(self) -> None:
        """Close the socket, then release the session that owns it.

        The session is closed in a `finally` because this is called from
        `BridgeClient._close`, under a `wait_for` that may cancel it: a close
        handshake the peer never answers must still not leak the connector.
        `ClientSession.close()` does not wait for the peer, so it is safe
        there.
        """
        try:
            await self._ws.close()
        finally:
            await self._session.close()


async def websocket_connect(
    url: str,
    *,
    max_msg_size: int = MAX_INCOMING_FRAME_BYTES,
    heartbeat: Optional[float] = KEEPALIVE_INTERVAL_S,
) -> _Socket:
    """Open one connection to the cloud. `BridgeClient`'s default `connect`.

    The keywords are arguments rather than constants read inside so that a
    test can drive the real adapter with a ceiling it can actually cross —
    the `WSMsgType.ERROR` path is otherwise a megabyte of frame away, and a
    path no test enters is a path that stops working quietly.
    """
    # aiohttp accepts `http://` and `https://` for a WebSocket, because the
    # upgrade is an HTTP one, and this bridge does not: `FLEETLESS_CLOUD_URL`
    # names a WebSocket endpoint, and an operator who wrote `https://` has
    # made a mistake that no amount of reconnecting will fix. `_one_session`
    # can only tell them so through `InvalidURL`, so the scheme is checked
    # here — the library this replaced refused these itself, and nothing else
    # in this file would have noticed the difference.
    #
    # Deliberately a scheme check and nothing more: everything else about the
    # URL is aiohttp's to judge, and it raises the same class for it.
    scheme = url.split("://", 1)[0].lower() if "://" in url else ""
    if scheme not in ("ws", "wss"):
        raise aiohttp.InvalidURL(url)
    session = aiohttp.ClientSession()
    try:
        ws = await session.ws_connect(url, max_msg_size=max_msg_size, heartbeat=heartbeat)
    except BaseException:
        # Including cancellation: `BridgeClient._open` races this against
        # `stop()`, so the abandoned attempt is the ordinary case and not the
        # exceptional one.
        await session.close()
        raise
    return _Socket(session, ws)


class StopReason(enum.Enum):
    """Why `BridgeClient.run` returned."""

    SHUTDOWN = "shutdown"
    """We were asked to stop. Nothing is wrong."""

    REJECTED = "rejected"
    """The cloud will refuse us again until a human changes something."""


class ExponentialBackoff:
    """Delays of 1 s, 2 s, 4 s … up to a cap, between connection attempts —
    each one jittered so a fleet that loses the cloud together does not retry
    together. Without this, every bridge that dropped in the same
    moment computes the same schedule and hits the cloud in lockstep on every
    attempt, which is indistinguishable from a self-inflicted flood at the
    exact moment the cloud is recovering.

    "Equal jitter" (half the scheduled delay, plus up to the other half at
    random) rather than "full jitter" (anywhere from zero to the scheduled
    delay): full jitter can hand back a near-zero wait on any attempt,
    including the first, which defeats the point of backing off at all. Equal
    jitter keeps the floor the schedule already promises and only randomizes
    the ceiling.

    `random_func` is injectable the same way `connect`/`sleep` are on
    `BridgeClient`: it returns a value in [0, 1), defaulting to
    `random.random`, so a test can pin it (e.g. `lambda: 1.0` reproduces the
    unjittered schedule exactly) or drive two independent instances with
    their own generators to show they diverge.
    """

    def __init__(
        self,
        initial: float = BACKOFF_INITIAL_S,
        factor: float = BACKOFF_FACTOR,
        cap: float = BACKOFF_CAP_S,
        random_func: Optional[Callable[[], float]] = None,
    ) -> None:
        self._initial = initial
        self._factor = factor
        self._cap = cap
        self._next = initial
        self._random_func = random_func if random_func is not None else random.random

    def reset(self) -> None:
        self._next = self._initial

    def next_delay(self) -> float:
        scheduled = self._next
        self._next = min(self._next * self._factor, self._cap)
        return scheduled * (0.5 + 0.5 * self._random_func())


class PrioritizedWriter:
    """The one writer that owns the socket. Every other pump becomes a
    *source* feeding this class instead of calling `ws.send()` itself:
    the library writes each frame in one transport write (see the module
    docstring above), so frames never interleave mid-write — but that says
    nothing about *order*, and a snapshot ahead of a pong in send order is
    exactly what starves the cloud's ping/pong on a slow link. This class is the only
    thing between any source and the wire, and it drains strictly by tier:
    a tier is considered only when every tier above it — both the
    `enqueue()`-fed push deques and any `sources` entries — has nothing
    ready right now.

    A source in `sources` is duck-typed against three methods, matching
    this file's existing preference for duck-typing over a formal
    `Protocol` (see the module docstring's note on `ros`):

        try_next() -> payload (str | bytes) | None   # pull, non-blocking
        on_sent(item) -> None                          # e.g. job delivery tracking
        on_send_failure(item) -> None                   # e.g. job put-back

    Push tiers (`enqueue()`) have no such callback: nothing about a queued
    pong or config-ack needs delivery tracking, the way `JobManager`'s
    put-back does — see `_JobSource`'s own docstring for why that one
    matters.

    `snapshot_source`, tier 5, sits below every `sources` tier and is asked
    for a frame only once nothing else is ready — see `run`. It is async
    and separate from `sources` because encoding a frame is not free
    (`camera.encode_snapshot_jpeg` runs on the executor): pulling one is
    worth doing only after every cheaper tier has been checked.

        snapshot_source.next(max_bytes: int) -> Awaitable[bytes | None]

    Every send — push, pulled, or snapshot — is timed, and every send of
    at least `RATE_SAMPLE_MIN_BYTES` folds into one throughput estimate —
    the estimate is fed by every frame, the same fold 2.0.2's
    retired `_record_snapshot_send` used for snapshots alone:
    `(previous + observed) / 2`, seeded by the first large send. The size
    bound is not a tuning knob — a send under the transport's 64 KiB write
    high-water returns without any byte having left the robot, so folding
    it measures per-send CPU and not the link; see
    `RATE_SAMPLE_MIN_BYTES` for what that costs. `rate_estimate()`
    exposes the estimate; `snapshot_max_bytes()` is the one place it is
    spent, fitting a snapshot to `max_occupancy_s` of the current rate
    rather than only the wire-format ceiling
    (`camera.SNAPSHOT_MAX_BYTES`).

    Tiers 0-4 are must-deliver: a payload that projects to take longer than
    `max_occupancy_s` at the current rate is sent anyway, with a `log.warning`
    naming the bytes, the rate and the projected seconds — refusing it would
    break introspection on exactly the slow links this design exists for —
    a large `type_definitions` reply is the case. Tier 5 is the only tier
    ever shaped to fit: `snapshot_source` is trusted to respect what
    `snapshot_max_bytes()` told it, and nothing here adds a second gate on
    the bytes it returns: two policies for one decision, and the newer one
    is always the weaker.

    A send failure ends `run()` after telling the offending source
    (`on_send_failure`) and logging once — the socket is already gone, the
    same shape every pump above uses today, and `run()` never raises into
    its caller: whatever awaits it just sees it return."""

    def __init__(
        self,
        ws,
        *,
        sources,
        snapshot_source=None,
        max_occupancy_s: float = MAX_SEND_OCCUPANCY_S,
    ) -> None:
        self._ws = ws
        # Sorted once here, not trusted from the caller: registration
        # order must never decide scan order. `sources` is fixed for the
        # writer's whole life — nothing adds to it after construction — so
        # sorting once is enough; `_next_ready`
        # below merges this pre-sorted list against the push tiers, which
        # *are* dynamic (`enqueue()` can introduce one at any time) and so
        # get sorted fresh on every call instead.
        self._sources = sorted(sources, key=lambda pair: pair[0])
        self._snapshot_source = snapshot_source
        self._max_occupancy_s = max_occupancy_s
        self._push: Dict[int, Deque] = {}
        self._wake = asyncio.Event()
        # `None` until the first send of at least `RATE_SAMPLE_MIN_BYTES`,
        # so every frame before that one goes out unmeasured — a guessed
        # rate low enough to be safe would drop the first frame on a
        # healthy link, and there is nothing to guess from yet. In practice
        # the first snapshot is what seeds this, because it is the first
        # frame big enough to have drained the write buffer.
        self._rate_bps: Optional[float] = None
        # Whether the tier-5 pull is currently in a timeout streak, so a
        # ROS work queue stalled for minutes logs once rather than four
        # times a second. Cleared by the first pull that comes back without
        # timing out; see `_run_once`.
        self._pull_stalled = False
        # The one tier-5 pull that may be in flight, or `None`. Never
        # awaited inline; see `_run_once`.
        self._pull: Optional["asyncio.Future"] = None

    def enqueue(self, tier: int, payload) -> None:
        """Pushes onto tier `tier`'s deque and wakes `run()`. For sources
        fed through `sources` instead (pull, via `try_next`), call `wake()`
        after making something available — `enqueue` is only for payloads
        this writer owns outright, with nothing upstream to ask again."""
        dq = self._push.setdefault(tier, deque())
        dq.append(payload)
        self._wake.set()

    def rate_estimate(self) -> Optional[float]:
        """Bytes/second, folded from every completed send of at least
        `RATE_SAMPLE_MIN_BYTES`; `None` before the first such send — a
        session that has only ever sent small control frames has not
        measured the link and says so, rather than reporting the CPU cost
        of its own `send()` calls as a link speed."""
        return self._rate_bps

    def snapshot_max_bytes(self) -> int:
        """`min(camera.SNAPSHOT_MAX_BYTES, rate * max_occupancy_s)` — the
        wire-format ceiling when the rate is not yet known, otherwise
        whatever fits the occupancy budget at the current measured rate, so
        that `encode_snapshot_jpeg` receives a true target instead of a flat
        1.5 MiB.

        "Not yet known" is the ordinary state at the start of a session,
        not an edge case: the estimate only moves on sends of at least
        `RATE_SAMPLE_MIN_BYTES`, and on most robots the first of those is a
        snapshot. So the first snapshot of a session is asked for at the
        wire-format ceiling and is itself what establishes the rate every
        later one is fitted to — 2.0.2's seeding semantics, unchanged."""
        if self._rate_bps is None or self._rate_bps <= 0:
            return SNAPSHOT_MAX_BYTES
        return min(SNAPSHOT_MAX_BYTES, int(self._rate_bps * self._max_occupancy_s))

    def wake(self) -> None:
        """Lets a pull-source feeder tell `run()` "something may be ready
        now" without handing over a payload directly — `run()` still asks
        the source itself via `try_next()`; this only shortens the wait."""
        self._wake.set()

    def _next_ready(self):
        """The next `(tier, payload, source)` in strict tier order across
        both push deques and pull sources, or `None` if nothing is ready
        right now. `source` is `None` for a push item — there is nothing to
        call `on_sent`/`on_send_failure` back on.

        A merge of two already-ordered sequences — push tiers sorted fresh
        here (dynamic: `enqueue()` can introduce a new one at any time) and
        `self._sources`, sorted once in `__init__` because it never changes
        after construction. On a tie, the push side goes first.

        That tie-break is not vacuous: three tiers carry both. Tier 1 has
        `config_applied` pushed by `_handle_config` and `_JobSource`'s job
        outcomes pulled; tier 3 has `introspect` and `type_definitions`
        pushed by the two request handlers and `_AssetSource`'s
        availability/progress frames pulled. A queued
        `type_definitions` — the largest frame the bridge ever puts on the
        control channel, hundreds of kilobytes — goes out ahead of every
        `asset_progress` frame already waiting in the same tier, and an
        asset sync's progress reporting stalls for exactly as long as that
        transfer takes. Accepted rather than fixed: both are developer
        tooling of equal rank, the tooling reply is what a developer is
        actively blocked on, and progress is a level whose next frame
        corrects the gap.

        Tier 2 has no push side of its own today — both its sources are
        pulled — so the rule costs nothing there. It is stated anyway
        because `enqueue()` can introduce a push deque on any tier at any
        time, and whoever adds one to tier 2 should read what it will do
        to the two sources already there."""
        push_tiers = iter(sorted(self._push.keys()))
        sources = iter(self._sources)
        next_push = next(push_tiers, None)
        next_source = next(sources, None)
        while next_push is not None or next_source is not None:
            if next_source is None or (
                next_push is not None and next_push <= next_source[0]
            ):
                tier = next_push
                dq = self._push.get(tier)
                next_push = next(push_tiers, None)
                if dq:
                    return tier, dq.popleft(), None
                continue
            tier, source = next_source
            next_source = next(sources, None)
            payload = source.try_next()
            if payload is not None:
                return tier, payload, source
        return None

    async def run(self) -> None:
        """Drains strictly by tier for the life of the connection. When
        nothing is ready: starts a tier-5 pull if none is already in
        flight and there is a `snapshot_source`, then waits out one tick
        of the wake event — so a due snapshot is noticed without
        busy-polling, and a frame arriving in a higher tier meanwhile ends
        the wait at once. Without a `snapshot_source`, the tick alone is
        the wait, so an idle writer with no sources at all never
        busy-loops.

        The pull is deliberately *not* awaited inline. It was, and that
        was a defect: `RosRuntime.next_snapshot` goes through
        `_submit_async`, which queues behind whatever the ROS executor is
        already doing (a `resolve_types`, a `graph_snapshot`, a camera
        apply) and gives up only after `WORK_TIMEOUT_S` — ten seconds.
        This writer is the *only* sender, so those ten seconds are ten
        seconds a `pong` already sitting in tier 0 is not being sent, and
        the cloud closes the socket after about six. Encoding a snapshot
        is exactly the kind of work that must never be able to hold the
        one socket, which is the whole premise of this class.

        So: at most one pull in flight, started before the wait and
        harvested by a later scan. Tier order is unaffected — a pulled
        payload is only ever *sent* by an iteration that has already found
        tiers 0-4 empty — and a pull that never finishes costs one pending
        task rather than the session. One at a time also matters for the
        thread pool: `_submit_async` parks a default-executor thread per
        call, and starting a fresh pull every tick against a wedged ROS
        executor would exhaust that pool and take every other
        `_submit_async` caller down with it.

        `event.clear()` happens before each scan, not after — the
        check-then-wait race this avoids: enqueue()/wake() setting the
        event between "we found nothing" and "we started waiting" must
        still be seen, which a clear-after-wait ordering can lose.

        A tier-5 pull that *times out* is the one exception this loop
        handles itself rather than by closing the socket — see
        `_run_once`. Anything else unexpected escaping a source's
        `try_next`/`on_sent`/`on_send_failure`, or `snapshot_source.next`,
        is caught once here rather than left to propagate into whatever
        runs this coroutine as a bare task (`_converse` does, with
        `asyncio.ensure_future(writer.run())`; `_run_once` is what this
        loop awaits). Same shape as `_pump_control`'s 2.0.1 fix, cited in
        its own docstring: off this loop, the same error would only end
        `run()`'s own task and leave a
        connected bridge that silently stops sending anything at all —
        strictly worse than closing the socket and letting `_converse`'s
        `recv()` notice and reconnect. Not swallow-and-continue: a source
        that raises is broken, and looping back to it would just repeat
        the same error every tick."""
        try:
            while True:
                try:
                    keep_going = await self._run_once()
                except Exception:  # noqa: BLE001 - see the docstring above
                    log.exception(
                        "The prioritized writer hit an unexpected error and is "
                        "closing the connection"
                    )
                    await self._safe_close()
                    return
                if not keep_going:
                    return
        finally:
            self._cancel_pull()

    async def _run_once(self) -> bool:
        """One iteration of `run`'s loop. Returns whether to keep going —
        `False` means a send failed (already logged in `_send`) and `run`
        should end normally; an exception instead means something other
        than the socket itself broke (a source bug), which `run` handles
        separately."""
        self._wake.clear()
        item = self._next_ready()
        if item is not None:
            tier, payload, source = item
            return await self._send(tier, payload, source)

        if self._snapshot_source is None:
            await self._tick()
            return True

        if self._pull is not None and self._pull.done():
            # Cleared before the result is read: `.result()` re-raises
            # whatever the pull failed with, and `run`'s handler must not
            # then find a task it would try to cancel a second time.
            pull, self._pull = self._pull, None
            try:
                payload = pull.result()
            except _PULL_TIMEOUT_ERRORS:
                # A ROS work queue that did not get to this snapshot in
                # `WORK_TIMEOUT_S` is "nothing due this tick", not a broken
                # source. Left to `run`'s catch-all it closed the session
                # instead — and the reconnect then re-applies the whole
                # configuration, which is *more* executor work queued
                # behind the same stall, so a ten-second hiccup compounded
                # into a reconnect loop. Before this branch existed at
                # all, a stalled executor only delayed snapshots. It does
                # again.
                #
                # Socket-closure stays for every other exception: a source
                # that raises is broken, and looping back to it would just
                # repeat the same error every tick (see `run`).
                if not self._pull_stalled:
                    self._pull_stalled = True
                    log.warning(
                        "The snapshot pull timed out — the ROS work queue is "
                        "stalled. Snapshots are paused until it recovers; the "
                        "connection is unaffected. Logged once per stall, not "
                        "once per attempt"
                    )
                await self._tick()
                return True
            # The streak ends at the first pull that answers at all,
            # whether or not it had a frame — the next stall gets its own
            # log line.
            self._pull_stalled = False
            if payload is not None:
                return await self._send(_SNAPSHOT_TIER, payload, None)
            # Nothing was due. Wait out a tick before asking again: the
            # done callback below sets the wake event, so a source that
            # answers `None` immediately — the ordinary case, most ticks
            # having no camera due — would otherwise drive start-pull /
            # harvest-nothing round and round at full CPU. The tick is
            # what has always paced this loop; it just has to sit on this
            # side of the pull now.
            await self._tick()
            return True

        if self._pull is None:
            # `snapshot_max_bytes()` is spent here, when the pull starts,
            # not when its frame is sent — the estimate may have moved by
            # then. An estimate is an estimate; what matters is that the
            # target tracks the link at all, which it does.
            self._pull = asyncio.ensure_future(
                self._snapshot_source.next(self.snapshot_max_bytes())
            )
            # Without this the finished frame would wait out a full tick
            # before anything looked at it.
            self._pull.add_done_callback(lambda _task: self._wake.set())
        await self._tick()
        return True

    def _cancel_pull(self) -> None:
        """Abandons an in-flight pull when `run` ends. Synchronous on
        purpose — this runs in a `finally` that may itself be unwinding a
        cancellation, which is no place to await. The done callback
        retrieves whatever the task ends up with, so an abandoned pull
        that raised is never reported as an exception nobody read."""
        pull, self._pull = self._pull, None
        if pull is None:
            return
        pull.cancel()
        pull.add_done_callback(lambda task: task.cancelled() or task.exception())

    async def _tick(self) -> None:
        """Wait for `wake()` or `_WRITER_TICK_S`, whichever comes first.

        Deliberately **not** `asyncio.wait_for`, which this was until the
        pressure pump stopped hiding what it does: on Python 3.10 — ROS
        Humble's interpreter, the oldest this one source is built for —
        `wait_for` catches the cancellation, and if its inner future
        happens to be done already, returns that result and **drops the
        CancelledError on the floor** (CPython bpo-37658, fixed in 3.12).
        `_wake` is set by `enqueue()`, so an `Event.wait` completing in the
        same loop iteration as `_converse`'s `_cancel_pump(writer_task)` is
        not a rare interleaving — it is what the end of a busy session
        looks like. The writer then never stopped, `await task` in
        `_cancel_pump` never returned, and the session hung on teardown
        with the reconnect that would have delivered its queued frames
        never starting. The pressure pump's own cancel, one `await`
        earlier, used to move the phase enough to hide it.

        `asyncio.wait` has no such shortcut: a cancellation arriving here
        propagates. The inner task is cancelled in `finally` so a tick cut
        short by either a timeout or a cancel leaves nothing pending."""
        waiter = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({waiter}, timeout=_WRITER_TICK_S)
        finally:
            waiter.cancel()

    async def _safe_close(self) -> None:
        """Best-effort — `run` is ending regardless of whether this
        succeeds. Same shape as `BridgeClient._close`: the point is to make
        `_converse`'s `recv()` notice the session is over, not to guarantee
        a clean close."""
        try:
            await self._ws.close()
        except Exception:  # noqa: BLE001 - best-effort, see above
            pass

    async def _send(self, tier: int, payload, source) -> bool:
        """Sends one payload, timing it into the rate estimate. Returns
        whether the socket is still usable — `False` ends `run()`."""
        if tier < _SNAPSHOT_TIER and self._rate_bps:
            projected = len(payload) / self._rate_bps
            if projected > self._max_occupancy_s:
                # Must-deliver: sent anyway — shaping is
                # only for tier 5, which is fitted to the budget before it
                # is ever handed here (`snapshot_max_bytes`).
                log.warning(
                    "Sending a %d-byte tier %d frame that projects to "
                    "%.1f s at %.0f B/s, over the %.1f s occupancy budget "
                    "— sent anyway, must-deliver tiers are never dropped "
                    "for size",
                    len(payload), tier, projected, self._rate_bps,
                    self._max_occupancy_s,
                )
        started = time.monotonic()
        try:
            await self._ws.send(payload)
        except (ConnectionClosed, OSError, aiohttp.ClientError) as exc:
            log.warning("The connection dropped before a tier %d frame: %s", tier, exc)
            if source is not None:
                source.on_send_failure(payload)
            return False
        elapsed = time.monotonic() - started
        self._record_send(len(payload), elapsed)
        if source is not None:
            source.on_sent(payload)
        return True

    def _record_send(self, size: int, elapsed: float) -> None:
        """Same fold 2.0.2 used for snapshots alone, now applied to every
        frame this writer sends that is large enough to have measured the
        link rather than its own CPU (see `RATE_SAMPLE_MIN_BYTES` for why
        that bound is 64 KiB)."""
        if size < RATE_SAMPLE_MIN_BYTES:
            return
        if elapsed <= 0:
            return
        observed = size / elapsed
        self._rate_bps = (
            observed if self._rate_bps is None else (self._rate_bps + observed) / 2
        )


class _GatedSource:
    """Wraps a ROS-fed source so it produces nothing until the session is
    open — `is_open()` is what `_converse` flips at `hello_ok`.

    The pumps this design replaces were *started* at `hello_ok`; being
    started late was their gate. The writer cannot work that way: it has to
    exist from the first moment of `_converse`, because `hello` goes
    through it, a `pong` may overtake the greeting
    (test_client_pingpong.py), and a `config` can arrive ahead of
    `hello_ok` (see `_pump_control`'s note on being started before the
    receive loop). So the gate moves from *when a source starts* to *when
    it answers*, and nothing that presumes a greeted session — a datapoint,
    a job outcome, a backfill item — can leave before the cloud has said
    which robot this is.

    One wrapper rather than the same three-line check pasted into each of
    the five sources: a rule five classes each implement separately is five
    chances for one of them to forget it."""

    def __init__(self, is_open: Callable[[], bool], inner) -> None:
        self._is_open = is_open
        self._inner = inner

    def try_next(self):
        if not self._is_open():
            return None
        return self._inner.try_next()

    def on_sent(self, item) -> None:
        # Not gated: a session that closed between the send and its
        # acknowledgement must still let the source finish bookkeeping it
        # already started (`JobManager.mark_delivered`, above all).
        self._inner.on_sent(item)

    def on_send_failure(self, item) -> None:
        self._inner.on_send_failure(item)


class _JobSource:
    """Tier 1: `ros.jobs.updates` as job_update frames — what `_pump_jobs`
    used to do, split into the writer's three callbacks.

    This queue never drops a job's outcome (jobs.py), so the whole backlog
    delivers on reconnect, not just the latest state. A send that fails
    because the connection just dropped puts its update back at the front
    rather than discarding it: `try_next` already removed it, and without
    this, the very disconnect that drop-nothing promise exists to survive
    would be exactly when an update goes missing.

    `mark_delivered` runs only from `on_sent`, i.e. only after `ws.send()`
    actually succeeded — a terminal update that merely got queued must not
    yet retire its job from `active_jobs` (see jobs.py). A job that
    finishes while disconnected, with its terminal update still queued at
    the next hello, must keep being named as active until that update
    genuinely reaches the cloud, or the cloud marks it `lost`
    moments before the truthful outcome arrives for a job it has already
    given up on.

    `_inflight` holds the `JobUpdate` the wire frame was built from,
    because the writer hands `on_sent`/`on_send_failure` the *payload*, not
    the object. At most one can be outstanding: the writer sends what
    `try_next` returned before it scans any source again."""

    def __init__(self, ros) -> None:
        self._ros = ros
        self._inflight = None

    def try_next(self):
        update = self._ros.jobs.updates.try_get()
        if update is None:
            return None
        self._inflight = update
        return job_update_message(
            update.job_id,
            update.slug,
            update.state,
            timestamp_ms=update.timestamp_ms,
            feedback=update.feedback,
            progress=update.progress,
            result=update.result,
            error=update.error,
            details=update.details,
        )

    def on_sent(self, payload) -> None:
        update, self._inflight = self._inflight, None
        if update is not None:
            self._ros.jobs.mark_delivered(update)

    def on_send_failure(self, payload) -> None:
        update, self._inflight = self._inflight, None
        if update is not None:
            self._ros.jobs.updates.requeue_front(update)


class _SampleSource:
    """Tier 2: live datapoint samples. `SampleQueue` already drops oldest
    on overflow and counts it for the once-per-reconnect log line
    (`_log_dropped_samples`), so there is nothing to track here and nothing
    to put back: a sample that missed the wire has a successor a moment
    later, which is the whole reason it is allowed to drop at all.

    `on_dwell` is the one thing it does track. The gap between a sample's
    capture time and the moment it actually reached the wire is
    low-bandwidth mode's second input, and the only one that still works
    when the cloud has stopped answering — a narrow uplink shows up here as
    a growing queue long before anything else says so. Read in `on_sent`
    rather than in `try_next`: what matters is when `ws.send()` returned,
    which on a full socket buffer is the whole point.

    `_inflight_ms` holds the capture stamp the frame was built from, the
    same shape and the same reason `_JobSource._inflight` has: the writer
    hands the callbacks the payload, not the sample, and at most one can be
    outstanding."""

    def __init__(self, ros, tier: int, on_dwell=None) -> None:
        self._ros = ros
        self._tier = tier
        self._on_dwell = on_dwell
        self._inflight_ms = None

    def try_next(self):
        sample = self._ros.samples.try_get()
        if sample is None:
            return None
        self._inflight_ms = sample.timestamp_ms
        return datapoint_message(sample.slug, sample.value, sample.timestamp_ms)

    def on_sent(self, payload) -> None:
        captured_ms, self._inflight_ms = self._inflight_ms, None
        if captured_ms is not None and self._on_dwell is not None:
            self._on_dwell(captured_ms)

    def on_send_failure(self, payload) -> None:
        # A send that failed measures nothing: the session is ending, and the
        # time it took says more about the socket dying than about the link.
        self._inflight_ms = None


class _CameraStateSource:
    """Tier 2: `ros.camera_states` as bridgeCameraState frames. Unlike
    `_JobSource` there is no delivery tracking — a camera's live/not-live
    state has nothing analogous to `active_jobs` reading it — so a send
    failure just ends the session with nothing to requeue.

    Registered *ahead of* `_SampleSource` in the same tier, which is the
    one place registration order decides anything (`PrioritizedWriter.
    _next_ready` scans same-tier sources in order). Deliberate: camera
    states are rare and transition-driven, live samples are a continuous
    stream, and a robot sampling steadily must not be able to keep a
    camera's state — including the one answering an operator's
    `camera_start` — off the wire indefinitely. The reverse order has no
    such symmetric cost, since a dropped-oldest sample has a successor."""

    def __init__(self, ros, tier: int) -> None:
        self._ros = ros
        self._tier = tier

    def try_next(self):
        update = self._ros.camera_states.try_get()
        if update is None:
            return None
        return bridge_camera_state_message(
            update.slug, update.publishing, update.error,
            cause=update.cause, observed_at_ms=update.observed_at_ms,
            request_id=update.request_id,
        )

    def on_sent(self, payload) -> None:
        pass

    def on_send_failure(self, payload) -> None:
        pass


class _AssetSource:
    """Tier 3: both asset queues, `ros.assets` (bridgeAssetsAvailable)
    and `ros.asset_progress` (bridgeAssetProgress) — one source rather
    than two, because they share a tier and nothing distinguishes their
    priority. Availability first: it is the rarer frame and the one a sync
    is reported *against*.

    No delivery tracking and nothing to requeue on failure, same as
    `_CameraStateSource`. A dropped `assets_available` is made good on the
    next reconnect (`_handle_config` calls `report_current_urdf_
    availability`); a dropped `asset_progress` is a gap in what the cloud
    was told, not a stalled sync — `RosRuntime.sync_assets` has no notion
    of the WebSocket at all and keeps running regardless."""

    def __init__(self, ros) -> None:
        self._ros = ros

    def try_next(self):
        update = _drain_one(self._ros.assets)
        if update is not None:
            return bridge_assets_available_message(update.urdf, update.meshes)
        progress = _drain_one(self._ros.asset_progress)
        if progress is None:
            return None
        return bridge_asset_progress_message(
            progress.sync_id, progress.done, progress.total,
            progress.failed, progress.state,
        )

    def on_sent(self, payload) -> None:
        pass

    def on_send_failure(self, payload) -> None:
        pass


class _BackfillSource:
    """Tier 4: `ros.backlog`, the buffered history of datapoints configured
    with `buffer.enabled`, oldest first within a slug.

    Nothing here rate-limits: strict priority replaced
    `BACKFILL_MIN_INTERVAL_S` outright (see the tier constants above). The
    promise it enforced — buffered history never crowds out live traffic —
    is now a property of the scan order rather than of a timer, and holds
    between *every* pair of backfill items rather than only every 50 ms.

    `pop_any` can return `None` while `has_pending()` was true a moment ago
    (a config apply calling `backlog.remove()` races it); returning `None`
    is the right answer then, and the writer simply asks again."""

    def __init__(self, ros) -> None:
        self._ros = ros

    def try_next(self):
        sample = self._ros.backlog.pop_any()
        if sample is None:
            return None
        # `backfill=True` is this tier's whole distinguishing fact on the
        # wire: the cloud measures its datapoint lag from what arrives
        # live, and a replayed buffer carries capture timestamps from
        # before the outage. Without the flag a reconnect after an hour
        # offline reads as an hour of lag — on a link that is fine.
        return datapoint_message(
            sample.slug, sample.value, sample.timestamp_ms, backfill=True
        )

    def on_sent(self, payload) -> None:
        pass

    def on_send_failure(self, payload) -> None:
        # Deliberately not put back. `BacklogStore` is bounded per slug and
        # ordered by `timestamp_ms` on the cloud regardless; re-inserting a
        # sample whose send failed would have to choose a slug and a
        # position, and a lost history sample is the honest gap this tier
        # already accepts (unlike a job outcome, which has no successor).
        pass


class _SnapshotSource:
    """Tier 5, the pulled tier: asks the runtime for a frame encoded to
    whatever the writer can currently afford. Gated on the
    session being open for the same reason the `_GatedSource` wrapper
    exists — this one carries the check itself, since its interface is a
    single `async next` rather than the three-method pull contract."""

    def __init__(self, is_open: Callable[[], bool], ros) -> None:
        self._is_open = is_open
        self._ros = ros

    async def next(self, max_bytes: int):
        if not self._is_open():
            return None
        return await self._ros.next_snapshot(max_bytes)


def _ms(value: Optional[float]) -> str:
    """A reading for a log line, or `none` — which is a real answer here: no
    ping has arrived, or nothing has been sent, and the controller reads
    either as calm rather than as a number."""
    return "none" if value is None else "{:.0f} ms".format(value)


def _drain_one(queue: "asyncio.Queue"):
    """One item from a plain `asyncio.Queue`, or `None` — the `try_get`
    the bridge's own queue classes have and `asyncio.Queue` does not."""
    try:
        return queue.get_nowait()
    except asyncio.QueueEmpty:
        return None


class BridgeClient:
    """Keeps the robot connected to the cloud for as long as that makes sense.

    `connect` and `sleep` are injectable so the suite can drive a real
    WebSocket server without waiting out real backoff delays.
    """

    def __init__(
        self,
        config: BridgeConfig,
        *,
        connect: Optional[Callable[[str], Awaitable]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        backoff: Optional[ExponentialBackoff] = None,
        handshake_timeout: float = HANDSHAKE_TIMEOUT_S,
        idle_timeout: float = IDLE_TIMEOUT_S,
        bridge_version: str = __version__,
        ros: Optional[Any] = None,
        max_send_occupancy_s: float = MAX_SEND_OCCUPANCY_S,
        now: Optional[Callable[[], float]] = None,
        tick: float = LINK_MODE_TICK_S,
    ) -> None:
        self._config = config
        self._max_send_occupancy_s = max_send_occupancy_s
        self._connect = connect if connect is not None else websocket_connect
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._backoff = backoff if backoff is not None else ExponentialBackoff()
        # `ExponentialBackoff` with the cap at the initial value: nothing to
        # grow into, and the equal-jitter arithmetic stays in one place
        # rather than being written a second time for one wait.
        self._refusal_wait = ExponentialBackoff(
            initial=REFUSAL_WAIT_S, cap=REFUSAL_WAIT_S
        )
        # Set when the cloud refused this bridge's protocol version, cleared
        # by the next `hello_ok`: `run()` reads it to choose between
        # reconnecting and waiting out a long backoff before exiting.
        self._last_refused_for_version = False
        # Once per process, not per session: the sunset date does not move
        # between two reconnects, and a bridge on a flaky link would
        # otherwise repeat the same sentence every few minutes.
        self._warned_deprecated = False
        self._handshake_timeout = handshake_timeout
        self._idle_timeout = idle_timeout
        self._bridge_version = bridge_version
        self._ros = ros
        self._stop = asyncio.Event()
        # Whether this session has been greeted (`hello_ok` seen) — read by
        # every ROS-fed writer source through `_GatedSource`, which is
        # where the reasoning lives. Reset per session in `_converse`.
        self._session_open = False
        self.robot_id: Optional[str] = None
        # Set True by `_converse` at the top of every session; consumed and
        # cleared by `_handle_config` on that session's first `config` frame
        # — a fresh connection re-states every configured camera's
        # current background health unconditionally, not only on the next
        # transition, so a cloud that just restarted (and so lost its own
        # health store) gets the truth back rather than nothing until the
        # next thing happens to change. See RosRuntime.
        # report_current_camera_health's own docstring for why.
        self._first_config_since_hello = True
        # The most recently built session's writer, or `None` before the
        # first connection attempt (`_build_writer` has not run yet).
        # Deliberately *not* reset to `None` when a session ends: the
        # rate estimate the last session measured is a better starting
        # answer than nothing until `_build_writer` overwrites this with a
        # fresh one.
        self._last_writer: Optional[PrioritizedWriter] = None
        # --- low-bandwidth mode ------------------------------------------
        # The controller's clock and its tick, both injectable for the same
        # reason `connect` and `sleep` are: `enter_after_s` counts in whole
        # seconds and the smallest the contract allows is one, so a suite
        # that waited the timers out in real time would pay ten seconds an
        # assertion. `now` is `time.monotonic` in the bridge; a test hands in
        # a clock it moves itself.
        self._now = now if now is not None else time.monotonic
        self._tick = tick
        # The two layers the settings resolve from, in precedence order.
        # Parameters are read at `hello_ok` (the node has to exist first) and
        # again whenever a `ros2 param set` succeeds; the YAML section
        # arrives with every config.
        self._param_low_bandwidth: Dict[str, Any] = {}
        self._yaml_low_bandwidth: Dict[str, Any] = {}
        self._lb_settings = LowBandwidthSettings.resolve({}, {})
        # Deliberately per client, not per session: the mode is a property of
        # the link, and a reconnect does not make a narrow uplink wide.
        self._link_mode = LinkMode(self._lb_settings, self._now())
        self._link_mode_task: Optional["asyncio.Task"] = None
        # The last lag the cloud reported, kept here only so a transition can
        # be logged with the number that caused it. The controller treats a
        # ping reading as a level and does not keep it, and asking it to would
        # be asking it to carry state for a log line. There is deliberately no
        # counterpart for the dwell — see `_log_transition`.
        self._last_lag_ms: Optional[int] = None
        # When this session opened, in the same wall clock a sample's
        # `timestamp_ms` uses — see `_observe_dwell`. Set here too, not only
        # per session, so the attribute exists before the first connection.
        self._session_started_ms = capture_timestamp_ms()
        if self._ros is not None:
            self._ros.on_low_bandwidth_params(self._on_low_bandwidth_params)

    def stop(self) -> None:
        """Ask the bridge to shut down; safe to call from a signal handler."""
        self._stop.set()

    async def run(self) -> StopReason:
        """Connect, and keep reconnecting, until stopped or refused for good."""
        while not self._stop.is_set():
            reason = await self._one_session()
            if reason is not None:
                return reason
            if self._stop.is_set():
                break
            if self._last_refused_for_version:
                # Wait, then exit rather than reconnect. An in-process retry
                # would keep running the package the cloud just refused, so
                # an apt upgrade would never take effect; exiting hands the
                # decision to the respawn, which loads what is installed now.
                delay = self._refusal_wait.next_delay()
                log.info(
                    "Protocol refused; exiting in %.0f s so a restart can pick up "
                    "an upgraded package",
                    delay,
                )
                if await self._sleep_or_stop(delay):
                    break
                return StopReason.REJECTED
            delay = self._backoff.next_delay()
            log.info("Reconnecting in %.0f s", delay)
            if await self._sleep_or_stop(delay):
                break
        return StopReason.SHUTDOWN

    async def _one_session(self) -> Optional[StopReason]:
        """One connection attempt. `None` means: worth trying again."""
        url = self._config.cloud_url
        try:
            ws = await self._open(url)
        except asyncio.TimeoutError:
            log.warning(
                "Could not open a connection to %s within %.0f s",
                url,
                self._handshake_timeout,
            )
            return None
        except aiohttp.InvalidURL:
            # Before the branch below, and not interchangeable with it:
            # `InvalidURL` is an `aiohttp.ClientError` too, so the order is
            # what keeps a URL nobody can fix from being retried forever.
            log.error(
                "FLEETLESS_CLOUD_URL is not a usable WebSocket URL: %s "
                "(expected something like ws://host:port/bridge)",
                url,
            )
            return StopReason.REJECTED
        except (OSError, aiohttp.ClientError) as exc:
            # Everything else that can stop a connection from opening: the
            # host refusing it (`ClientConnectorError`, an `OSError`), DNS,
            # or a cloud that answers the upgrade with an HTTP response
            # (`WSServerHandshakeError`, a `ClientError`). All of them are
            # worth another attempt after a backoff.
            log.warning("Cannot reach %s: %s", url, exc)
            return None

        if ws is _STOPPED:
            return StopReason.SHUTDOWN

        log.info("Connected to %s", url)
        try:
            return await self._converse(ws)
        finally:
            await self._close(ws)

    # --- low-bandwidth mode ------------------------------------------------

    def _on_low_bandwidth_params(self, values: Dict[str, Any]) -> None:
        """A `ros2 param set` the runtime already validated and accepted.

        Runs on the event loop, hands the new parameter layer to the resolver
        and lets the task below carry the result to the wire. The runtime
        refused anything `resolve` would reject on its own, but the YAML on
        top can still cross a threshold with it, so this path survives a
        `ValueError` exactly the way the config apply does: keep what was
        running, say so in the log."""
        self._param_low_bandwidth = dict(values)
        asyncio.ensure_future(self._apply_params_change())

    async def _apply_params_change(self) -> None:
        try:
            await self._apply_low_bandwidth_settings(self._now())
        except ValueError as exc:
            log.error(
                "The low-bandwidth parameters do not combine with the published "
                "section: %s. The bridge keeps the settings it had.", exc,
            )
        except Exception:  # noqa: BLE001 - nobody awaits this job; say so here or nowhere
            log.exception("Unexpected error applying a low-bandwidth parameter change")

    async def _apply_low_bandwidth_settings(self, now: float) -> bool:
        """Re-resolve both layers, hand the result to the controller, pull the
        levers. Returns whether a transition was reported.

        Raises `ValueError` — with the sentence naming the key and the rule —
        without having changed anything, so a caller can report it and carry
        on.

        **`update_settings` is called only when the settings actually moved**,
        because it resets both of the controller's timers by contract, and
        this runs at every `hello_ok`. A link narrow enough to matter is one
        the cloud closes after three unanswered pings, about six seconds; a
        bridge that restarted `enter_after_s` on every greeting would need ten
        seconds inside one session and so would never enter the mode on
        precisely the link it exists for. `LowBandwidthSettings` is a frozen
        dataclass, so `!=` is exact and this costs one comparison."""
        settings = LowBandwidthSettings.resolve(
            self._param_low_bandwidth, self._yaml_low_bandwidth
        )
        transition = (
            self._link_mode.update_settings(settings, now)
            if settings != self._lb_settings
            else None
        )
        self._lb_settings = settings
        await self._on_transition(transition)
        return transition is not None

    async def _on_transition(self, transition: Optional[Transition]) -> None:
        """Say a crossing once, then pull the levers.

        Called with `None` too, after a settings change that did not flip the
        mode: `datapoint_max_hz` can move while the mode stays on, and the
        levers carry the numbers, not only the state. Pulling them is
        idempotent, so the extra call costs nothing and the missing one would
        cost a cap nobody applied."""
        if transition is not None:
            self._log_transition(transition)
            self._report_link_mode(transition.low_bandwidth, transition.reason)
        if self._ros is not None:
            await self._ros.set_low_bandwidth(
                self._link_mode.active, self._lb_settings
            )

    def _log_transition(self, transition: Transition) -> None:
        """One line an operator can act on: the two readings that crossed,
        the threshold they crossed, and what the robot is now doing about
        it. A `forced` transition names no reading, because none was
        consulted."""
        # The lag and the threshold, and deliberately not the queue dwell.
        # The dwell that decides anything is a p95 over five seconds, which
        # the controller keeps to itself; printing the last raw sample beside
        # a threshold would read as the number that crossed it, and one slow
        # send is exactly what the p95 exists to ignore. `reason` already
        # says when the dwell was the measure that entered the mode.
        if transition.low_bandwidth:
            log.info(
                "Low-bandwidth mode on (%s): lag %s against %d ms. Datapoints "
                "are capped to %g Hz, live video is set to %s and backfill "
                "waits.",
                transition.reason,
                _ms(self._last_lag_ms),
                self._lb_settings.enter_lag_ms,
                self._lb_settings.datapoint_max_hz,
                self._lb_settings.camera,
            )
        else:
            log.info(
                "Low-bandwidth mode off (%s): lag %s against %d ms. Configured "
                "rates, live video and backfill are back.",
                transition.reason,
                _ms(self._last_lag_ms),
                self._lb_settings.exit_lag_ms,
            )

    def _report_link_mode(self, low_bandwidth: bool, reason: str) -> None:
        """One `link_mode` frame, tier 0 — the tier a pong sits in, because a
        frame the mode exists to make room for must not queue behind the bulk
        it is about to cut.

        Silently dropped when no session is open to carry it: a transition
        while disconnected is reported by the next `hello_ok`, which states
        the mode outright."""
        writer = self._last_writer
        if writer is None or not self._session_open:
            return
        writer.enqueue(
            _TIER_SESSION,
            link_mode_message(low_bandwidth, reason, capture_timestamp_ms()),
        )

    def _observe_dwell(self, captured_ms: int) -> None:
        """How long one sample waited between capture and the wire.

        Two clocks on purpose: the dwell is the distance between two wall-clock
        stamps (the capture time the wire carries and now), while the window it
        lands in is monotonic, because a stepped system clock must not be able
        to empty or fill the tracker.

        A sample captured before this session opened is sent but not measured.
        `SampleQueue` survives a disconnect, so the first sends of a new
        session can carry stamps from before the outage, and their "dwell"
        would be the length of the outage — which says nothing about the link
        that is up now. At the default `enter_after_s` of 10 s the five-second
        window ages them out first; at 1 s every reconnect would enter the
        mode."""
        if captured_ms < self._session_started_ms:
            return
        self._link_mode.observe_dwell(
            capture_timestamp_ms() - captured_ms, self._now()
        )

    async def _start_link_mode(self) -> None:
        """At `hello_ok`: read the parameter layer, state the mode once, start
        the tick.

        The parameters only exist once `ros.start()` has run, which is after
        this client was constructed — so this is the first moment they can be
        read at all. The state goes out whether or not anything changed: the
        cloud's `bridge_state` should be right from the first second of a
        session rather than from the first transition, which on a healthy link
        never comes.

        Once, though, not twice. A parameter layer that forces the mode makes
        the resolve above return a `forced` transition, which has already been
        reported by the time the greeting below would state the same thing."""
        reported = False
        try:
            if self._ros is not None:
                self._param_low_bandwidth = dict(self._ros.low_bandwidth_params())
            reported = await self._apply_low_bandwidth_settings(self._now())
        except ValueError as exc:
            log.error(
                "The low-bandwidth settings do not resolve: %s. The bridge keeps "
                "the settings it had.", exc,
            )
        except Exception:  # noqa: BLE001 - a mode that will not engage beats a handshake that dies
            log.exception("Unexpected error starting low-bandwidth mode for this session")
        if not reported:
            if self._lb_settings.mode != "auto":
                reason = "forced"
            elif self._link_mode.active:
                # Under `auto`, a mode still on from the previous session.
                # The controller knows which measure entered it — hard-coding
                # `lag` here would report a mode entered on the local queue
                # dwell, the case where the cloud had stopped answering
                # altogether, to that same cloud as a lag problem.
                reason = self._link_mode.reason
            else:
                reason = "recovered"
            self._report_link_mode(self._link_mode.active, reason)
        self._link_mode_task = asyncio.ensure_future(self._pump_link_mode())

    async def _pump_link_mode(self) -> None:
        """`evaluate` on a fixed tick, for as long as the session lasts.

        The controller's timers advance only inside `evaluate`, so this tick
        is the resolution of both `_after_s` thresholds — twelve seconds of
        observed lag and one call would start the timer at that call, not
        twelve seconds ago."""
        while True:
            await asyncio.sleep(self._tick)
            try:
                transition = self._link_mode.evaluate(self._now())
                if transition is not None:
                    await self._on_transition(transition)
            except Exception:  # noqa: BLE001 - one bad tick must not end the mode for the session
                log.exception("Unexpected error evaluating low-bandwidth mode")

    def _build_writer(self, ws) -> PrioritizedWriter:
        """The session's one writer, with every source it will ever have.

        `sources` is fixed for a writer's life, so this builds the whole
        set up front and lets `_GatedSource` decide when each may answer —
        see its docstring for why the gate could not simply stay "start the
        pump at `hello_ok`" the way it used to be.

        Tier order is carried by the `_TIER_*` constants, not by the order
        of this list; the one thing the list order decides is which of the
        two tier-2 sources is asked first, which `_CameraStateSource`
        explains."""
        sources = []
        snapshot_source = None
        if self._ros is not None:

            def is_open() -> bool:
                return self._session_open

            def backfill_open() -> bool:
                # Tier 4 is the one tier low-bandwidth mode silences outright.
                # Buffered history has no deadline — it is already late by
                # definition — so it is the one thing that can wait for the
                # link the mode exists to spare. Read per call, so the gate
                # opens and closes with the mode and no writer is rebuilt.
                return is_open() and not self._link_mode.active

            camera_states = _CameraStateSource(self._ros, _TIER_TELEMETRY)
            samples = _SampleSource(
                self._ros, _TIER_TELEMETRY, on_dwell=self._observe_dwell
            )
            sources = [
                (_TIER_OUTCOME, _GatedSource(is_open, _JobSource(self._ros))),
                (_TIER_TELEMETRY, _GatedSource(is_open, camera_states)),
                (_TIER_TELEMETRY, _GatedSource(is_open, samples)),
                (_TIER_TOOLING, _GatedSource(is_open, _AssetSource(self._ros))),
                (_TIER_BACKFILL, _GatedSource(backfill_open, _BackfillSource(self._ros))),
            ]
            snapshot_source = _SnapshotSource(is_open, self._ros)
        writer = PrioritizedWriter(
            ws,
            sources=sources,
            snapshot_source=snapshot_source,
            max_occupancy_s=self._max_send_occupancy_s,
        )
        self._last_writer = writer
        return writer

    async def _converse(self, ws) -> Optional[StopReason]:
        """Say hello and serve the connection until it ends.

        Nothing in here calls `ws.send()`. Every frame this session emits
        is handed to the one `PrioritizedWriter` below, which is the only
        caller of `ws.send()` for the connection's whole life
        — the receive loop keeps parsing, it just stops sending. That is
        what makes tier order mean anything: one frame never interleaves
        with another, but that says nothing about *order*, and a snapshot
        ahead of a `pong` in send order is what starved a real robot's
        control channel.

        A send failure therefore no longer surfaces at each call site; it
        ends the writer, which ends the socket, which `recv()` below sees
        as the end of the session — the outcome every one of those call
        sites already produced."""
        greeted = False
        # reset once per session, same as `greeted` — see
        # `_handle_config`'s use of it and `self._first_config_since_hello`'s
        # own docstring in `__init__`.
        self._first_config_since_hello = True
        # Closed until `hello_ok`; see `_GatedSource`.
        self._session_open = False
        # Started at `hello_ok`, cancelled in `finally` — one per session, the
        # same lifecycle the control pump has.
        self._link_mode_task = None
        self._session_started_ms = capture_timestamp_ms()
        # The handshake gets one deadline for the whole of it, not one per
        # frame, so a cloud that keeps sending other traffic without ever
        # answering the hello still times out.
        deadline = time.monotonic() + self._handshake_timeout
        writer = self._build_writer(ws)
        writer_task = asyncio.ensure_future(writer.run())
        self._attach_wake(writer)
        # Started before the loop rather than at `hello_ok`, so a cloud that
        # sends a config ahead of the greeting is served in the same order
        # it always was — its sources wait for `hello_ok` because they need
        # a connected ROS runtime; this one does not.
        control_queue: "asyncio.Queue" = asyncio.Queue()
        control_task = asyncio.ensure_future(
            self._pump_control(ws, control_queue, writer)
        )
        writer.enqueue(
            _TIER_SESSION,
            hello_message(self._config.token, self._bridge_version, self._active_jobs()),
        )

        try:
            while True:
                timeout = (
                    self._idle_timeout
                    if greeted
                    else max(0.0, deadline - time.monotonic())
                )
                try:
                    frame = await self._recv(ws, timeout)
                except ConnectionClosed:
                    # `ws.close_code` rather than anything on the exception:
                    # the socket is what carries the peer's code, and
                    # `_Socket` deliberately keeps only one copy of it.
                    #
                    # This branch has to come first and that is not stylistic:
                    # `ConnectionClosed` is an `aiohttp.ClientConnectionError`
                    # and so is caught by the one below as well. Reversed,
                    # every close code — supersede and robot-deleted
                    # included — would be read as "retry", and the two bridges
                    # would trade the robot back and forth forever.
                    return self._reason_for_close(ws.close_code)
                except (OSError, aiohttp.ClientError) as exc:
                    # The socket did not close, it broke: a frame over
                    # `MAX_INCOMING_FRAME_BYTES`, a protocol error, a write
                    # that failed. Nothing here says the cloud refused us, so
                    # the session ends and the next one is tried.
                    log.warning("The connection failed: %s", exc)
                    return None

                if frame is _STOPPED:
                    return StopReason.SHUTDOWN
                if frame is _TIMED_OUT:
                    if greeted:
                        log.warning(
                            "Nothing received for %.0f s — treating the connection "
                            "as dead",
                            timeout,
                        )
                    else:
                        log.warning(
                            "The cloud did not answer the hello within %.0f s",
                            self._handshake_timeout,
                        )
                    return None

                message = parse_cloud_message(frame)
                if isinstance(message, HelloOk):
                    greeted = True
                    self.robot_id = message.robot_id
                    # Only a handshake that actually succeeded proves the cloud is
                    # healthy, so only that resets the backoff.
                    self._backoff.reset()
                    self._last_refused_for_version = False
                    log.info("Connected as robot %s", message.robot_id)
                    if message.protocol_status == "deprecated" and not self._warned_deprecated:
                        self._warned_deprecated = True
                        log.warning(
                            "This bridge speaks protocol %s, which the cloud stops serving on %s. "
                            "Upgrade before then: apt-get install --only-upgrade "
                            "ros-$ROS_DISTRO-fleetless-bridge (latest is %s).",
                            PROTOCOL_VERSION,
                            message.sunset_at or "an unannounced date",
                            message.latest_bridge_version or "unknown",
                        )
                    if self._ros is not None:
                        self._log_dropped_samples()
                        self._ros.set_connected(True)
                    # Opened after `set_connected(True)`, not before: a
                    # source must never be able to answer with something
                    # the runtime still believes nobody is connected for.
                    self._session_open = True
                    writer.wake()
                    await self._start_link_mode()
                elif isinstance(message, HelloError):
                    if message.terminal:
                        log.error(
                            "The cloud rejected this bridge (%s): %s. This will not "
                            "change on its own — fix the configuration and start "
                            "the bridge again.",
                            message.code,
                            message.message,
                        )
                        return StopReason.REJECTED
                    if message.code == VERSION_REFUSED_CODE:
                        if not self._last_refused_for_version:
                            log.error(
                                "The cloud refused this bridge's protocol version (%s). "
                                "Waiting, then exiting so a restart can pick up an "
                                "upgraded package.",
                                message.message,
                            )
                        self._last_refused_for_version = True
                        return None
                    log.warning(
                        "The cloud refused the hello for now (%s): %s",
                        message.code,
                        message.message,
                    )
                    return None
                elif isinstance(message, Ping):
                    # The ping carries the cloud's two measurements; the
                    # controller takes them both and uses `lag_ms`. Handed
                    # over before the pong only because nothing here awaits —
                    # the pong is still the first thing enqueued after the
                    # frame was read.
                    self._last_lag_ms = message.lag_ms
                    self._link_mode.observe_cloud(
                        message.lag_ms, message.latency_ms, self._now()
                    )
                    # Tier 0, and the reason the tier exists: the cloud
                    # closes the socket after three unanswered pings, so
                    # this must overtake whatever bulk is queued rather
                    # than wait its turn behind it.
                    writer.enqueue(_TIER_SESSION, pong_message(message.ts_ms))
                elif isinstance(message, (Config, IntrospectRequest, TypeRequest)):
                    # Queued, never awaited here: see `_pump_control`.
                    control_queue.put_nowait(message)
                elif isinstance(message, CloudInvoke):
                    self._dispatch_invoke(message)
                elif isinstance(message, CloudCancel):
                    self._dispatch_cancel(message)
                elif isinstance(message, CloudPublish):
                    self._dispatch_publish(message)
                elif isinstance(message, CloudCameraStart):
                    self._dispatch_camera_start(message)
                elif isinstance(message, CloudCameraStop):
                    self._dispatch_camera_stop(message)
                elif isinstance(message, CloudAssetRequest):
                    self._dispatch_asset_request(message)
                else:
                    log.debug("Ignoring a frame from the cloud: %s", message.reason)
        finally:
            # Unconditional and first: however this session ends — a clean
            # close, a timeout, an exception — the very next sample must
            # stop being "live" the instant nothing is here to receive it,
            # not once the pump tasks below have finished unwinding.
            if self._ros is not None:
                self._ros.set_connected(False)
                # A dropped connection must not leave the robot publishing
                # into a room nobody can tell it to stop —
                # torn down here, in the same breath as set_connected(False),
                # not left for whoever reconnects to notice and clean up.
                await self._ros.stop_all_live()
            # Before the writer is cancelled, so nothing can be answered
            # onto a socket this session has already given up on.
            self._session_open = False
            self._detach_wake()
            await self._cancel_pump(self._link_mode_task, "link mode")
            self._link_mode_task = None
            await self._cancel_pump(control_task, "control")
            await self._cancel_pump(writer_task, "writer")

    async def _cancel_pump(self, task: Optional["asyncio.Task"], label: str) -> None:
        """Ends one of the session's pumps and reports how it ended.

        `CancelledError` stays silent: that is this method's own cancel
        arriving, the ordinary way every session ends. Any other exception
        is the pump having *died on its own* some time earlier, and the
        await here is the first and only place that ever surfaces it — a
        pump that raised is simply gone, and the session goes on without
        it, silently, unless somebody says so here. Warning, with the
        label, so a dead pump can be read off the log the operator already
        has rather than inferred from what stopped arriving."""
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - the session is ending anyway
            log.warning(
                "The %s pump died with an error; this session ran without it",
                label,
                exc_info=True,
            )

    def _active_jobs(self) -> List[Tuple[str, str, str]]:
        if self._ros is None:
            return []
        return self._ros.jobs.active_jobs()

    def _dispatch_invoke(self, message: CloudInvoke) -> None:
        """Kicks the job off and returns immediately — completion arrives
        later as a `job_update` frame via the writer's `_JobSource`, not as
        a reply to this frame. Without a ROS runtime (tests, or a misconfigured launch)
        there is nothing to run and nothing to report; the cloud's own
        offline/timeout handling is what notices, same as it would for any
        other silence from this robot."""
        if self._ros is not None:
            asyncio.ensure_future(
                self._ros.invoke(
                    message.job_id, message.slug, message.params, message.patience_ms
                )
            )

    def _dispatch_cancel(self, message: CloudCancel) -> None:
        if self._ros is not None:
            asyncio.ensure_future(self._ros.cancel_job(message.slug, message.job_id))

    def _dispatch_publish(self, message: CloudPublish) -> None:
        """No reply is expected on the wire (a publish has no id,
        no job) — this is fire-and-forget the same way `_dispatch_invoke`
        kicks a job off, just with nothing to report back later either."""
        if self._ros is not None:
            asyncio.ensure_future(self._ros.publish(message.slug, message.message))

    def _dispatch_camera_start(self, message: CloudCameraStart) -> None:
        """Fire-and-forget, same shape as every other dispatch here — what
        `start_live` made of it (publishing or a reported failure) arrives
        later as a `bridgeCameraState` frame via the writer's
        `_CameraStateSource`, not as a reply to this frame."""
        if self._ros is not None:
            asyncio.ensure_future(
                self._ros.start_live(
                    message.slug, message.url, message.room, message.token, message.request_id
                )
            )

    def _dispatch_camera_stop(self, message: CloudCameraStop) -> None:
        if self._ros is not None:
            asyncio.ensure_future(
                self._ros.stop_live(message.slug, request_id=message.request_id)
            )

    def _dispatch_asset_request(self, message: CloudAssetRequest) -> None:
        """Fire-and-forget, same shape as every other dispatch here: what
        `sync_assets` makes of it arrives later as a stream of
        `bridgeAssetProgress` frames via the writer's `_AssetSource`, not
        as a reply to this frame."""
        if self._ros is not None:
            asyncio.ensure_future(
                self._ros.sync_assets(
                    message.sync_id, message.upload_url, message.token, message.meshes
                )
            )

    def _log_dropped_samples(self) -> None:
        """Once per reconnect, not once per drop: this only has to be
        honest that samples were lost while disconnected."""
        dropped = self._ros.samples.drain_drop_count()
        if dropped:
            log.warning(
                "%d datapoint sample(s) were dropped while disconnected "
                "(sample queue overflow)",
                dropped,
            )

    def _attach_wake(self, writer: PrioritizedWriter) -> None:
        """Points the ROS-side queues' `on_put` hooks at this session's
        writer, so a sample, job update or camera state that arrives while
        the writer is idle is scanned for immediately instead of waiting
        out its idle tick.

        A callback rather than an import: ros_runtime.py and jobs.py know
        nothing about `PrioritizedWriter`, which is what keeps the ROS side
        testable without one (their own docstrings say so).

        The two asset queues are plain `asyncio.Queue`s with no such hook,
        so an asset frame waits at most one writer tick (0.25 s) to *start*
        — after which the writer drains the rest without ticking at all,
        since a scan that finds work never waits. On-demand, developer-
        initiated frames on a robot's uplink: worth a quarter second, not
        worth a third queue class."""
        if self._ros is None:
            return
        self._ros.samples.on_put = writer.wake
        self._ros.jobs.updates.on_put = writer.wake
        self._ros.camera_states.on_put = writer.wake

    def _detach_wake(self) -> None:
        """Unhooks them again when the session ends. Waking a writer that
        has stopped would be harmless (it only sets an `Event` nobody
        waits on), but a `RosRuntime` outlives many sessions and must not
        accumulate references to the writers of dead ones."""
        if self._ros is None:
            return
        self._ros.samples.on_put = None
        self._ros.jobs.updates.on_put = None
        self._ros.camera_states.on_put = None

    async def _pump_control(
        self, ws, queue: "asyncio.Queue", writer: PrioritizedWriter
    ) -> None:
        """Applies configs and answers introspection out of the receive
        loop's way.

        These three frames are the only ones whose work the bridge has to
        finish before it can answer them, and all three used to be awaited
        inline in `_converse`'s loop. An apply that takes longer than the
        cloud's pong deadline therefore cost the whole session: the `ping`
        waiting behind it was never read, so no `pong` was sent and the
        cloud closed the socket. Since the cloud re-sends the config on
        every connect, the next session died the same way — a loop a robot
        could not leave with one camera configured on a topic carrying
        9.44 MB frames at 2 Hz.

        One worker draining one FIFO queue, rather than an `ensure_future`
        per frame like the fire-and-forget dispatches above: these three
        carry a reply and a version, and applying config 2 before config 1
        would leave the robot in whichever state won the race.

        Nothing bounds the queue. A bound would have to choose between
        dropping a config — leaving the robot running a stale one, with the
        cloud told otherwise — and blocking the receive loop, which is the
        defect this exists to fix. The cloud sends one config per connect
        and one per change, so depth is a symptom of an apply that never
        finishes, not of ordinary traffic.

        Nothing here sends: the three replies are handed to the session's
        `PrioritizedWriter` (`config_applied` at tier 1, `introspect` and
        `type_definitions` at tier 3), which is the only caller of
        `ws.send()`. That changes where a *delivery* failure appears, not
        what it costs: it used to end this worker and nothing else, leaving
        `_converse`'s own `recv` to turn a gone socket into the end of the
        session; now it ends the writer, which closes the socket, which
        that same `recv` turns into the end of the session. The `ws`
        handle stays because the unexpected-error path below still needs
        it — see the next paragraph, which is the 2.0.1 fix itself.

        One behaviour genuinely changes: a session can now end while an
        apply is still running, which was impossible when the receive loop
        was the thing blocked on it. That costs the `config_applied` ack and
        nothing else. The apply itself is already on the executor thread by
        then (`RosRuntime._submit_async`), so cancelling the coroutine
        waiting on it does not abandon it half-done, and the cloud re-sends
        the config on the next connect regardless."""
        while True:
            message = await queue.get()
            try:
                if isinstance(message, Config):
                    await self._handle_config(writer, message)
                elif isinstance(message, IntrospectRequest):
                    await self._handle_introspect_request(writer, message)
                elif isinstance(message, TypeRequest):
                    await self._handle_type_request(writer, message)
                else:
                    # Unreachable: `_converse` queues exactly the three
                    # types above. Named rather than folded into the last
                    # branch so that adding a fourth to the queue without
                    # adding it here is a log line, not a frame silently
                    # answered as if it were a type request.
                    log.error(
                        "The control queue was handed a %s, which it cannot "
                        "answer", type(message).__name__,
                    )
            except Exception:  # noqa: BLE001
                # Inline, an unexpected error here ended the session by
                # propagating out of the receive loop. Off it, the same
                # error would only end this worker and leave a connected
                # bridge that silently stops applying configs — strictly
                # worse. Close the socket and let `_converse` see it.
                log.exception(
                    "Unexpected error handling a %s frame", type(message).__name__
                )
                await self._close(ws)
                return

    async def _handle_config(self, writer: PrioritizedWriter, message: Config) -> None:
        """Applies a published config and acks it. Tier 1: the ack belongs
        with job outcomes, above telemetry — a cloud that has not been told
        the config landed keeps re-sending it, which is a session-level
        cost, not a telemetry one."""
        errors: List[ApplyError] = []
        # First, because the mode's ceiling is what the datapoint apply below
        # builds each subscription's cap against.
        errors.extend(await self._apply_low_bandwidth_section(message))
        if self._ros is not None:
            errors.extend(await self._apply_or_report(
                APPLY_ERROR_KIND_DATAPOINT, message.version,
                lambda: self._ros.apply_config(message.datapoints),
            ))
            # The three kinds that build a ROS message from a template also
            # need `doc.messages` — the shared templates a `message: ${name}`
            # refers to. Datapoints and cameras build nothing and take none.
            errors.extend(await self._apply_or_report(
                APPLY_ERROR_KIND_ACTION, message.version,
                lambda: self._ros.apply_actions(message.actions, message.messages),
            ))
            errors.extend(await self._apply_or_report(
                APPLY_ERROR_KIND_SERVICE, message.version,
                lambda: self._ros.apply_services(message.services, message.messages),
            ))
            errors.extend(await self._apply_or_report(
                APPLY_ERROR_KIND_PUBLISHER, message.version,
                lambda: self._ros.apply_publishers(message.publishers, message.messages),
            ))
            errors.extend(await self._apply_or_report(
                APPLY_ERROR_KIND_CAMERA, message.version,
                lambda: self._ros.apply_cameras(message.cameras),
            ))
            for error in errors:
                log.warning(
                    "Config version %d: kind %s slug %r: %s",
                    message.version, error.kind, error.slug, error.message,
                )
            if self._first_config_since_hello:
                # exactly once per connection, on the first config this
                # session applies — not on every later one, which would
                # defeat the transition-only dedup that already handles the
                # steady state correctly. See RosRuntime.
                # report_current_camera_health's own docstring for the
                # defect this closes (a cloud restart erases its own health
                # store; this hands it back).
                self._first_config_since_hello = False
                try:
                    await self._ros.report_current_camera_health()
                except Exception:  # noqa: BLE001 - additive; must not cost the session
                    log.exception("Unexpected error reporting current camera health")
                # settle the active truth first — a robot with no
                # `/robot_description` publisher at all must not wait out
                # the periodic timer to be told `false` — before the
                # cache-replay below, so a genuine true→false transition
                # this check makes is what the replay then (correctly)
                # finds nothing left to restate for.
                try:
                    await self._ros.check_urdf_availability_now()
                except Exception:  # noqa: BLE001 - additive; must not cost the session
                    log.exception("Unexpected error checking current URDF availability")
                # Same call site, same reasoning: a reconnect with
                # no ROS-side URDF change must still tell a cloud that may
                # not remember what a previous session already reported.
                try:
                    await self._ros.report_current_urdf_availability()
                except Exception:  # noqa: BLE001 - additive; must not cost the session
                    log.exception("Unexpected error reporting current URDF availability")
        writer.enqueue(
            _TIER_OUTCOME, config_applied_message(message.version, not errors, errors)
        )

    async def _apply_low_bandwidth_section(self, message: Config) -> List[ApplyError]:
        """The document's `low_bandwidth` section, on top of the parameters.

        A section that does not resolve is an entry in the ack and nothing
        else. It has to be: contracts compares `enter_lag_ms` against
        `exit_lag_ms` only when the section carries both, so a published,
        perfectly valid `low_bandwidth: {exit_lag_ms: 3000}` arrives here
        crossed against the default `enter_lag_ms` — and a bridge that treated
        that as fatal would be taken down by a document the console accepted.
        The previous section stays in force.

        The error rides the `datapoint` kind with slug `*`: `kind` is a closed
        enum on the wire, and the rate cap is the lever this section mostly
        governs. The message is the sentence `resolve` raised, naming the key
        and the rule."""
        previous = self._yaml_low_bandwidth
        self._yaml_low_bandwidth = dict(message.low_bandwidth)
        try:
            await self._apply_low_bandwidth_settings(self._now())
        except ValueError as exc:
            self._yaml_low_bandwidth = previous
            log.warning(
                "Config version %d: the low_bandwidth section was refused: %s",
                message.version, exc,
            )
            return [ApplyError(
                slug="*", kind=APPLY_ERROR_KIND_DATAPOINT,
                code=APPLY_ERROR_CODE_LOW_BANDWIDTH_INVALID, message=str(exc),
            )]
        return []

    async def _apply_or_report(self, kind: str, version: int, apply) -> List[ApplyError]:
        """Runs one `apply_*` call, turning an unexpected exception into a
        whole-kind `config_applied` error instead of crashing the session —
        one kind's bug must not cost the others their result, same principle
        as a single bad slug not blocking the rest within one kind.

        `kind` is one of the `APPLY_ERROR_KIND_*` constants — the actual
        exposure kind, not the human log label ("the configuration", "the
        actions", ...) this used to take, which was never the kind (`"the
        configuration"` was the datapoint pass)."""
        try:
            return await apply()
        except Exception:  # noqa: BLE001 - report as a whole-kind failure, don't crash the session
            log.exception("Unexpected error applying %s for config version %d", kind, version)
            return [ApplyError(
                slug="*", kind=kind, code=APPLY_ERROR_CODE_WHOLE_KIND_FAILED,
                message="internal error applying {}".format(kind),
            )]

    async def _handle_introspect_request(
        self, writer: PrioritizedWriter, message: IntrospectRequest
    ) -> None:
        """Tier 3, developer tooling: answered promptly on a healthy link
        and behind live telemetry on a weak one. A `type_definitions`
        answer can be hundreds of kilobytes — the design's own example of a
        must-deliver frame that projects past the occupancy budget and is
        sent anyway, because refusing it would break introspection on
        exactly the links this ordering exists for."""
        if self._ros is not None:
            graph = await self._ros.graph_snapshot()
        else:
            graph = {"topics": [], "services": [], "actions": [], "captured_at_ms": 0}
        writer.enqueue(_TIER_TOOLING, introspect_message(message.request_id, graph))

    async def _handle_type_request(
        self, writer: PrioritizedWriter, message: TypeRequest
    ) -> None:
        if self._ros is not None:
            definitions, unresolved = await self._ros.resolve_types(message.type_names)
        else:
            definitions, unresolved = [], list(message.type_names)
        writer.enqueue(
            _TIER_TOOLING,
            type_definitions_message(message.request_id, definitions, unresolved),
        )

    def _reason_for_close(self, code: Optional[int]) -> Optional[StopReason]:
        if code == CLOSE_CODE_SUPERSEDED:
            log.error(
                "Another bridge took over this robot. Stopping, so the two do "
                "not keep kicking each other off."
            )
            return StopReason.REJECTED
        if code == CLOSE_CODE_ROBOT_DELETED:
            log.error(
                "This robot was deleted from the Fleetless console. Stopping "
                "— it will not reconnect."
            )
            return StopReason.REJECTED
        if code == CLOSE_CODE_TOKEN_ROTATED:
            # Its own sentence, not the deleted one: the robot still
            # exists and there is something the operator can do, which is
            # the whole reason the cloud spends a second close code on it.
            log.error(
                "The robot's token was rotated in the console; start the "
                "bridge with the new token."
            )
            return StopReason.REJECTED
        log.info("The cloud closed the connection (code %s)", code)
        return None

    async def _open(self, url: str):
        """Open a connection, but stay interruptible by `stop()`.

        An unreachable cloud can hold a connect attempt for the whole handshake
        timeout. Without this race, a shutdown would have to wait that out —
        long enough for `docker stop` to give up and SIGKILL the container
        instead of letting it close its socket.
        """
        connector = asyncio.ensure_future(self._connect(url))
        stopper = asyncio.ensure_future(self._stop.wait())
        done, _ = await asyncio.wait(
            {connector, stopper},
            timeout=self._handshake_timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stopper in done:
            await self._discard(connector)
            return _STOPPED
        await _cancel(stopper)
        if connector in done:
            return connector.result()
        await _cancel(connector)
        raise asyncio.TimeoutError

    async def _discard(self, connector: "asyncio.Future") -> None:
        """Give up on a connection attempt, closing it if it already succeeded."""
        connector.cancel()
        try:
            ws = await connector
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001 - we are abandoning this attempt anyway
            return
        await self._close(ws)

    async def _recv(self, ws, timeout: float):
        """Receive one frame, but stay interruptible by `stop()`."""
        receiver = asyncio.ensure_future(ws.recv())
        stopper = asyncio.ensure_future(self._stop.wait())
        done, _ = await asyncio.wait(
            {receiver, stopper},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stopper in done:
            await _cancel(receiver)
            return _STOPPED
        await _cancel(stopper)
        if receiver in done:
            return receiver.result()
        await _cancel(receiver)
        return _TIMED_OUT

    async def _sleep_or_stop(self, delay: float) -> bool:
        """Wait out the backoff. Returns True if we were told to stop instead."""
        sleeper = asyncio.ensure_future(self._sleep(delay))
        stopper = asyncio.ensure_future(self._stop.wait())
        done, _ = await asyncio.wait(
            {sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED
        )
        stopped = stopper in done
        await _cancel(sleeper)
        await _cancel(stopper)
        return stopped

    async def _close(self, ws) -> None:
        try:
            await asyncio.wait_for(ws.close(), timeout=CLOSE_TIMEOUT_S)
        except (asyncio.TimeoutError, OSError, aiohttp.ClientError):
            log.debug("Closing the connection did not finish cleanly", exc_info=True)


async def _cancel(task: "asyncio.Future") -> None:
    """Cancel a helper task and absorb whatever it was about to raise."""
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:  # noqa: BLE001 - the result is deliberately discarded
        pass
