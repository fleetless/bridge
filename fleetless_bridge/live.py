# SPDX-License-Identifier: Apache-2.0
"""Live video publish to LiveKit, over aiortc and the signalling in
`livekit_signal.py`.

On `cloudCameraStart` the bridge joins the room the cloud already minted —
the **cloud** owns the room and the publisher token, for the same reason it
mints a `job_id` before asking anything: the side that owns the
refcount must own the identity of the stream, or the robot could end up
publishing into a room nobody is watching. This module only ever consumes
that identity; it never mints one.

Frames come from the same `camera.LatestFrameHolder` the snapshot path
reads — whichever frame is newest when a feed tick comes due, no
change-notification wiring, since a poll interval bounded by the camera's
own configured fps costs at most one throttle period of extra latency,
negligible next to WebRTC's own encode/network latency. Converted to RGB24
(a plain channel swap from the BGR the rest of this package already works
in) rather than a chroma-subsampled format — simplicity over the small
efficiency an extra internal conversion would otherwise buy.

**Two halves, and only one of them is here.** `livekit_signal.SignalClient`
speaks the room conversation (join, addTrack, offer/answer, trickle, leave);
this file owns the media: the peer connection, the track the encoder pulls
from, the bitrate budget, and what to do when either half ends. The seam
between them is narrow on purpose — the signalling half is provable against a
fake socket, and this half is provable against fake aiortc objects, so neither
needs a LiveKit server to be a test subject.

`connection_factory`/`track_factory`/`signal_factory` are the injectable seams
client.py's own `connect` already is: the defaults build real
`RTCPeerConnection`, `CameraTrack` and `SignalClient` objects; tests substitute
fakes, so the orchestration here (join, publish, negotiate, feed frames, stop,
and what to report when any of that fails) is provable without a real LiveKit
server. `CameraTrack` is a pure local object — safe to construct for real even
under test, since it touches no network by itself — but is still built via
`track_factory` so a fake can observe exactly which frames were pushed, which
a track being pulled by an encoder has no public API to reveal.

**No reconnect, deliberately.** The cloud already treats a bridge-side drop as
the end of the session and re-issues `camera_start`, so any transport loss —
the peer connection failing, the signalling socket ending, the server asking us
to leave — tears this session down and reports it through
`_on_room_disconnected`. An ICE restart here would be a second recovery
mechanism beside the one the cloud already runs, and the weaker of two always
wins.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

import cv2
from aiortc import (
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from aiortc.mediastreams import (
    VIDEO_CLOCK_RATE,
    VIDEO_TIME_BASE,
    MediaStreamError,
    VideoStreamTrack,
)
from aiortc.sdp import candidate_from_sdp

# `av` is imported directly (the frame type the encoder consumes) but is not
# named in package.xml: it has no rosdep key yet, and it arrives as an apt
# dependency of python3-aiortc, which is named. See package.xml's own comment.
from av import VideoFrame

from fleetless_bridge.camera import LatestFrameHolder
from fleetless_bridge.livekit_signal import IceServer, SignalClient

log = logging.getLogger(__name__)

# How often the feed loop checks the holder for a new frame — a ceiling on
# push rate, not a promise: the camera's own subscription callback
# (ros_runtime.py) already throttles to the configured fps before a frame
# ever reaches the holder, so polling any faster than that would just see
# the same frame again.
FeedInterval = float

#: The track name the cloud's viewers see. LiveKit carries it through
#: `addTrack` into the participant's track list; it is not an identifier and
#: nothing matches on it.
TRACK_NAME = "camera"

#: How often the bitrate budget is re-asserted on the encoder. The receiver's
#: own estimate (REMB) is written straight onto the same attribute by aiortc,
#: so a budget applied once is a budget the next REMB overwrites — see
#: `_hold_bitrate_budget`. The cost of the cadence is stated rather than
#: hidden: an estimate above the budget can win for up to this long.
BITRATE_HOLD_INTERVAL_S = 1.0

#: Where aiortc keeps the encoder. `RTCRtpSender` builds it lazily, on the
#: first frame it encodes, and holds it in a name-mangled private attribute;
#: 1.3.0, 1.6.0 and 1.14.0 — the three versions Ubuntu ships for the three
#: distributions this package is built for — all spell it this way, and none of
#: them offers a public route (`RTCRtpSender` has no `setParameters`, and the
#: encoder is not reachable from `getParameters`). Read through `getattr` with
#: a miss that is *reported*, so a version that renames it degrades to "the
#: budget is not applied", said out loud, rather than to a crash mid-session.
SENDER_ENCODER_ATTR = "_RTCRtpSender__encoder"

#: The clamp each of aiortc's encoders applies to `target_bitrate`, in bps --
#: `aiortc.codecs.vpx.MIN_BITRATE`/`MAX_BITRATE` and their `h264` equivalents,
#: which are module-level constants rather than attributes of the encoder
#: instance itself, so they cannot be read off `encoder` at the point a budget
#: is about to be clamped; stated here the same way `_hold_bitrate_budget`'s
#: own docstring already states them. Keyed by the encoder class name, which
#: is what negotiation leaves us holding (one track, one codec, chosen by the
#: SDP answer, not by this class).
ENCODER_CLAMP_BPS = {
    "Vp8Encoder": (250_000, 1_500_000),
    "H264Encoder": (500_000, 3_000_000),
}


class LiveStartError(Exception):
    """Live could not be started. The message becomes `bridgeCameraState`'s
    `error.message` (ros_runtime.py) — a camera that cannot start must say
    so, not leave a viewer watching a black rectangle believing it is live."""


class CameraTrack(VideoStreamTrack):
    """The media source aiortc's encoder pulls from: the newest frame
    `_push_frame` handed over, at the camera's configured rate.

    **Why the timestamps are computed here rather than by
    `VideoStreamTrack.next_timestamp`.** That helper advances a counter by a
    fixed 1/30 s per call and sleeps the difference to wall time, which is
    right for a source that is pulled continuously and wrong for one that can
    stop: after any gap — a paused publish, an encode that ran long — the
    difference is negative, `asyncio.sleep` returns at once, and the next
    several hundred calls each return a *backdated* timestamp as fast as the
    encoder can consume them. Deriving the presentation timestamp from the
    monotonic clock instead makes a gap a gap: the frames after it carry the
    time they were actually produced, and the pacing below is the only thing
    deciding how often one is produced.

    **Nothing is published before the camera has produced a frame.** `recv()`
    waits for the first push rather than inventing a black frame — the same
    reason `LiveStartError` exists: a black rectangle read as live is the
    failure this path exists to avoid.
    """

    kind = "video"

    def __init__(self, width: int, height: int, fps: int) -> None:
        super().__init__()
        self._width = width
        self._height = height
        self._interval_s = 1.0 / max(1, fps)
        self._latest = None
        # Set on the first push and never cleared: it answers "has this camera
        # ever produced a frame", not "is there a new one".
        self._have_frame = asyncio.Event()
        self._start_s: Optional[float] = None
        self._last_s: Optional[float] = None
        #: How many frames this track has handed to the encoder. Read by the
        #: browser proof to tell "the publisher produced nothing" from "the
        #: viewer never received what it produced" — two states a viewer alone
        #: cannot distinguish.
        self.frames_sent = 0

    def push(self, rgb) -> None:
        """Hand over the newest frame as an RGB24 ndarray. Overwrites whatever
        was there: a frame nobody had time to encode is a frame that is already
        out of date, and a queue here would trade latency for frames a viewer
        cannot use."""
        self._latest = rgb
        if not self._have_frame.is_set():
            self._have_frame.set()

    def stop(self) -> None:
        super().stop()
        # A `recv()` parked on the first frame must wake up and see the ended
        # state, or the sender's own task never finishes and `pc.close()`
        # waits for it.
        self._have_frame.set()

    async def recv(self) -> VideoFrame:
        if self.readyState != "live":
            raise MediaStreamError
        await self._have_frame.wait()
        if self.readyState != "live":
            raise MediaStreamError

        now = time.monotonic()
        if self._start_s is None:
            self._start_s = now
        elif self._last_s is not None:
            due = self._last_s + self._interval_s
            if due > now:
                await asyncio.sleep(due - now)
                now = due
        self._last_s = now

        frame = VideoFrame.from_ndarray(self._latest, format="rgb24")
        frame.pts = int((now - self._start_s) * VIDEO_CLOCK_RATE)
        frame.time_base = VIDEO_TIME_BASE
        self.frames_sent += 1
        return frame


def _default_connection_factory(ice_servers: Sequence[IceServer]) -> RTCPeerConnection:
    """The peer connection, configured with what `join` said.

    The ICE servers come from the server rather than from configuration here:
    the cloud knows where its TURN sits and says so in `join`, and a second
    list on the robot would be a second policy that goes stale. aiortc
    honours only the **first** STUN and the **first** TURN entry it can
    parse, so a `join` advertising two relays leaves one of them never
    tried.
    """
    return RTCPeerConnection(
        configuration=RTCConfiguration(
            iceServers=[
                RTCIceServer(
                    urls=list(server.urls),
                    username=server.username or None,
                    credential=server.credential or None,
                )
                for server in ice_servers
                if server.urls
            ]
        )
    )


def _default_track_factory(
    width: int, height: int, fps: int, bitrate_kbps: int
) -> CameraTrack:
    """`bitrate_kbps` is accepted and not used here: the bitrate lives on the
    encoder, which does not exist until the sender has a frame to encode, so it
    is applied from `_hold_bitrate_budget` rather than at construction. The
    argument stays in the signature because the frame rate beside it *is* a
    property of the track, and splitting the two across two seams would make
    the caller decide which one goes where."""
    return CameraTrack(width, height, fps)


def _default_signal_factory(url: str, token: str, **callbacks: Any) -> SignalClient:
    return SignalClient(url, token, **callbacks)


def _subscribed(payload: Any) -> Optional[bool]:
    """Whether any layer of our track is subscribed, from a
    `subscribedQualityUpdate`, or `None` when the message says nothing about
    it.

    The three-valued answer is the point. protojson omits `false`, so a layer
    that is not subscribed arrives as `{"quality": "HIGH"}` with no `enabled`
    at all — which makes "every layer says it is off" and "the message carried
    no layers" look alike at a glance, and they are opposites: the first is an
    instruction to stop encoding, the second is a message this client has no
    opinion about. A message with nothing in it must never pause a working
    stream.
    """
    qualities = list(payload.get("subscribedQualities") or [])
    for codec in payload.get("subscribedCodecs") or []:
        if isinstance(codec, dict):
            qualities.extend(codec.get("qualities") or [])
    if not qualities:
        return None
    return any(
        isinstance(entry, dict) and bool(entry.get("enabled")) for entry in qualities
    )


class LivePublisher:
    """One camera's live session: join, publish, feed frames until
    `stop()`. One instance per active `(slug, session)` — ros_runtime.py
    owns the mapping from slug to instance."""

    def __init__(
        self,
        *,
        holder: LatestFrameHolder,
        width: int,
        height: int,
        fps: int,
        bitrate_kbps: int,
        connection_factory: Optional[Callable[..., RTCPeerConnection]] = None,
        track_factory: Optional[Callable[[int, int, int, int], Any]] = None,
        signal_factory: Optional[Callable[..., Any]] = None,
        bitrate_interval_s: float = BITRATE_HOLD_INTERVAL_S,
        on_lost: Optional[Callable[[str], None]] = None,
    ) -> None:
        self._holder = holder
        self._width = width
        self._height = height
        self._fps = fps
        self._bitrate_kbps = bitrate_kbps
        self._connection_factory = connection_factory or _default_connection_factory
        self._track_factory = track_factory or _default_track_factory
        self._signal_factory = signal_factory or _default_signal_factory
        self._bitrate_interval_s = bitrate_interval_s
        # Called at most once, with a human-readable reason, when publishing
        # stops on its own — a transport loss (server restart, media
        # failure, the server asking us to leave) that nobody asked for. Never
        # called for a disconnect *we* caused (see `_stopping` below): the
        # caller of `stop()` already knows and is already handling it.
        self._on_lost = on_lost
        self._signal = None
        self._pc = None
        self._track = None
        self._sender = None
        self._track_sid: Optional[str] = None
        # Remote ICE candidates that arrived before the answer was applied,
        # and the latch that says they can be handed over -- see
        # `_on_remote_candidate`.
        self._early_candidates: List[Dict[str, Any]] = []
        self._answer_applied = False
        self._feed_task: Optional["asyncio.Task"] = None
        self._bitrate_task: Optional["asyncio.Task"] = None
        self._teardown_task: Optional["asyncio.Task"] = None
        self._stopping = False
        self._lost_reported = False
        self._paused = False
        # Set the first time `_hold_bitrate_budget` finds the configured
        # budget outside the negotiated codec's clamp -- so the log line
        # below fires once per session, not once per tick.
        self._bitrate_clamp_logged = False

    async def start(self, url: str, room_name: str, token: str) -> None:
        """Joins, publishes and negotiates. Raises `LiveStartError` on any
        failure — `room_name` is accepted for symmetry with `cloudCameraStart`
        and for logging, but is not itself sent: the room to join is implicit
        in `token` (a LiveKit access token is scoped to exactly one room), the
        same way a job's slug is implicit in its `job_id` once minted.

        The order is the one the server requires: `addTrack` before the offer,
        because LiveKit answers an offer publishing a track it was not told
        about with nothing at all.

        Everything from a successful join through the feed task starting is one
        failure domain: whatever fails in between must tear down both halves
        and surface as `LiveStartError`, or the caller (ros_runtime.start_live)
        has a live session neither `stop_live` nor `stop_all_live` can ever
        reach, because it was never recorded as this camera's publisher.

        **No underlying exception is chained, and none is quoted.** `token` is
        a LiveKit access token — a credential, exactly like a camera password —
        and this class's own docstring says the message built here becomes
        `bridgeCameraState.error.message`, sent to the cloud over the wire, not
        merely logged. The token rides in the query string of the signalling
        URL, and a refused handshake puts that whole URL in the library
        exception's own text: `str(exc)` therefore cannot be used, and neither
        can `raise ... from exc`, because the chain carries the same text into
        every traceback anything later prints. Type name only, `from None` —
        the same rule `SignalConnectError` applies one level down. `url` (the
        server address, without the query string) is not secret and stays, for
        the diagnostic value a bare type name would throw away.
        """
        signal = self._signal_factory(
            url,
            token,
            on_trickle=self._on_remote_candidate,
            on_quality_update=self._on_quality_update,
            on_leave=self._on_leave,
            on_closed=self._on_signal_closed,
        )
        self._signal = signal
        try:
            join = await signal.connect()
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            await self._teardown()
            raise LiveStartError(
                "could not connect to {} for room {!r} ({})".format(
                    url, room_name, type(exc).__name__
                )
            ) from None

        try:
            pc = self._connection_factory(join.ice_servers)
            self._pc = pc
            # Bound to this connection rather than read off `self`: teardown
            # clears the attribute, and a handler that re-read it would report
            # an AttributeError instead of the state change it was called for.
            pc.on("connectionstatechange", lambda: self._on_connection_state(pc))

            track = self._track_factory(
                self._width, self._height, self._fps, self._bitrate_kbps
            )
            self._track = track
            self._sender = pc.addTrack(track)

            published = await signal.add_track(
                track.id, TRACK_NAME, self._width, self._height,
                bitrate=self._bitrate_kbps * 1000,
            )
            self._track_sid = _published_track_sid(published)

            offer = await pc.createOffer()
            await pc.setLocalDescription(offer)
            # Local candidates are not trickled: no aiortc version among the
            # three Ubuntu ships emits a per-candidate event, and
            # `setLocalDescription` returns only once gathering has finished,
            # so every candidate this side has is already inside the offer
            # below. The server trickles its own, which arrive through
            # `_on_remote_candidate`.
            answer_sdp = await signal.send_offer(pc.localDescription.sdp)
            await pc.setRemoteDescription(
                RTCSessionDescription(sdp=answer_sdp, type="answer")
            )
            self._release_early_candidates()
        except Exception as exc:  # noqa: BLE001 - reported to the caller, not raised here
            # A half-published session must not be left behind — nothing is
            # left to clean it up.
            await self._teardown()
            raise LiveStartError(
                "could not publish the video track ({})".format(type(exc).__name__)
            ) from None

        self._bitrate_task = asyncio.ensure_future(self._hold_bitrate_budget())
        self._feed_task = asyncio.ensure_future(self._feed_frames())

    async def stop(self) -> None:
        """Stops feeding and tears both halves down. Safe to call even if
        `start()` never succeeded (or was never called) — a slug the cloud
        never actually got to start live for is not an error to stop. Sets
        `_stopping` first so the connection's own state change and the
        signalling socket's own end (both of which the teardown below causes,
        same as any other loss) are recognised as expected rather than
        reported through `on_lost`."""
        self._stopping = True
        await self._cancel_feed()
        # A loss already in flight owns the same objects; let it finish before
        # tearing down again, so the two cannot interleave inside `pc.close()`.
        teardown, self._teardown_task = self._teardown_task, None
        if teardown is not None:
            try:
                await teardown
            except asyncio.CancelledError:
                pass
        await self._teardown()

    # -- the two halves coming apart ---------------------------------------

    def _on_connection_state(self, pc) -> None:
        state = pc.connectionState
        if state in ("failed", "closed"):
            self._on_room_disconnected("peer connection {}".format(state))

    def _on_signal_closed(self, reason: str) -> None:
        self._on_room_disconnected(reason)

    def _on_leave(self, payload: Any) -> None:
        # Any `leave` ends the session, including one whose `canReconnect` says
        # a reconnect is allowed: this publisher does not reconnect, the cloud
        # re-issues `camera_start` instead, and honouring the flag here would
        # be the second recovery mechanism the module docstring rules out.
        reason = payload.get("reason") if isinstance(payload, dict) else None
        self._on_room_disconnected("the server asked us to leave ({})".format(reason))

    def _on_room_disconnected(self, reason=None) -> None:
        """The session ended and nobody here asked for it. Runs synchronously
        on the event loop thread, the same way aiortc's and the signalling
        client's own callbacks do — no cross-thread handoff needed.

        Reported once. Three independent things can notice the same loss (the
        peer connection failing, the socket ending, a `leave`), and tearing
        down makes at least one of the others fire in turn, so without the
        latch the cloud would hear about one loss two or three times."""
        if self._stopping or self._lost_reported:
            return  # our own stop() caused this, or it is already being handled
        self._lost_reported = True
        log.error("Live publish disconnected unexpectedly: reason=%s", reason)
        if self._feed_task is not None:
            self._feed_task.cancel()
            self._feed_task = None
        self._teardown_task = asyncio.ensure_future(self._teardown())
        if self._on_lost is not None:
            self._on_lost("room disconnected: {}".format(reason))

    async def _teardown(self) -> None:
        """Closes everything this session owns, in the order that leaves
        nothing waiting: the track first (so the sender's own loop stops
        pulling), then the peer connection, then the signalling socket, which
        is the only one that has anything to say to the server."""
        self._cancel_bitrate()
        track, self._track = self._track, None
        pc, self._pc = self._pc, None
        signal, self._signal = self._signal, None
        self._sender = None
        if track is not None:
            try:
                track.stop()
            except Exception as exc:  # noqa: BLE001 - already stopping; nothing to report to
                log.error("Error stopping a live video track: %s", type(exc).__name__)
        if pc is not None:
            try:
                await pc.close()
            except Exception as exc:  # noqa: BLE001 - already stopping; nothing to report to
                # Same rule as `start`'s two `except` blocks: this session was
                # opened with a LiveKit token, so no exception on this path
                # gets its raw message logged.
                log.error("Error closing a live peer connection: %s", type(exc).__name__)
        if signal is not None:
            try:
                await signal.close()
            except Exception as exc:  # noqa: BLE001 - same
                log.error("Error closing a live signalling socket: %s", type(exc).__name__)

    async def _cancel_feed(self) -> None:
        task, self._feed_task = self._feed_task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cancel_bitrate(self) -> None:
        task, self._bitrate_task = self._bitrate_task, None
        if task is not None:
            task.cancel()

    # -- what the server asks for ------------------------------------------

    def _on_remote_candidate(self, init: Dict[str, Any]) -> None:
        """A candidate the server trickled.

        **Held until the answer has been applied.** The server starts
        trickling the moment it has answered, which is before this side has
        finished applying that answer, and aiortc 1.14.0 refuses a candidate
        offered in that window — "addIceCandidate called without remote
        description", warned and dropped. 1.3.0 and 1.6.0 accept it, so the
        window is invisible on two of the three distributions and silently
        costs candidates on the third: eight of them in one 45 s run here, and
        on a robot behind NAT the relay candidate is exactly the one that can
        be in there.

        `addIceCandidate` is a coroutine and this is a plain callback, so the
        work is handed to the loop rather than awaited. A candidate that cannot
        be parsed is dropped: ICE is a set of possibilities, and one unusable
        member of it is not a session failure.
        """
        if not self._answer_applied:
            self._early_candidates.append(init)
            return
        asyncio.ensure_future(self._add_remote_candidate(init))

    def _release_early_candidates(self) -> None:
        self._answer_applied = True
        held, self._early_candidates = self._early_candidates, []
        for init in held:
            asyncio.ensure_future(self._add_remote_candidate(init))

    async def _add_remote_candidate(self, init: Dict[str, Any]) -> None:
        pc = self._pc
        if pc is None:
            return
        raw = init.get("candidate") or ""
        try:
            candidate = candidate_from_sdp(
                raw.split(":", 1)[1] if raw.startswith("candidate:") else raw
            )
            candidate.sdpMid = init.get("sdpMid")
            candidate.sdpMLineIndex = init.get("sdpMLineIndex")
            await pc.addIceCandidate(candidate)
        except Exception as exc:  # noqa: BLE001 - one candidate, not the session
            log.debug("Ignoring an unusable remote ICE candidate: %s", type(exc).__name__)

    def _on_quality_update(self, payload: Any) -> None:
        """`subscribedQualityUpdate`: nobody is watching, or somebody is again.

        **No latch on the way to a pause.** livekit-server v1.13.5's own
        update is trustworthy only because `add_track`'s `bitrate` declares one
        video layer for this track (`livekit_signal.add_track`, called from
        `start` with `bitrate=self._bitrate_kbps * 1000`): a track published
        with a declared layer gets updates that track the real subscription
        state -- an "off" once the sole viewer disconnects, an "on" once a
        viewer subscribes, and an honest "off" from the very first update when
        no viewer has ever been in the room. Withholding that first "off"
        behind a latch would be wrong in exactly that last case: it would keep
        publishing to an empty room, which is the one case pausing exists for.
        A track published with NO declared layer gets a different, unreliable
        report (see `add_track`'s own docstring) and must not reach this class
        at all -- `start` always declares one.
        """
        if not isinstance(payload, dict):
            return
        log.debug("subscribedQualityUpdate: %s", payload)
        track_sid = payload.get("trackSid")
        if track_sid and self._track_sid and track_sid != self._track_sid:
            return  # some other track in the same room
        subscribed = _subscribed(payload)
        if subscribed is None:
            return
        self._set_paused(not subscribed)

    def _set_paused(self, paused: bool) -> None:
        """Stop or resume producing video.

        One flag, two consumers, because pausing has two costs to stop and
        neither covers the other: `replaceTrack(None)` stops the *encoder* (the
        sender's loop idles instead of pulling frames), and the feed loop's own
        check stops the *colour conversion*, which is the robot's CPU and the
        actual reason a robot pauses anything.
        """
        if paused == self._paused:
            return
        self._paused = paused
        sender, track = self._sender, self._track
        if sender is None:
            return
        try:
            sender.replaceTrack(None if paused else track)
            if not paused:
                # A viewer resuming mid-stream has no reference frame, and its
                # own keyframe request costs it a visible stall. Best effort:
                # the method is aiortc's internal spelling, present in all
                # three versions, and its absence is not worth failing a resume
                # over — a PLI from the viewer gets there in the end.
                keyframe = getattr(sender, "_send_keyframe", None)
                if keyframe is not None:
                    keyframe()
        except Exception as exc:  # noqa: BLE001 - the session continues either way
            log.error("Could not %s the live track: %s",
                      "pause" if paused else "resume", type(exc).__name__)
            return
        log.info("Live publish %s (subscribed layers: %s)",
                 "paused" if paused else "resumed", "none" if paused else "at least one")

    # -- the frame feed ----------------------------------------------------

    async def _feed_frames(self) -> None:
        last_timestamp_ms: Optional[int] = None
        interval_s = 1.0 / self._fps
        while True:
            if not self._paused:
                frame = self._holder.get()
                if frame is not None and frame.timestamp_ms != last_timestamp_ms:
                    last_timestamp_ms = frame.timestamp_ms
                    try:
                        self._push_frame(frame.bgr)
                    except Exception as exc:  # noqa: BLE001 - one bad frame must not kill the feed loop
                        # (this loop had no try/except at all, so a
                        # raise here silently ended the unawaited feed task —
                        # the publisher stayed registered, the cloud still
                        # believed publishing:true, and viewers froze with no
                        # error anywhere. A genuinely dead session is caught
                        # by `_on_room_disconnected` instead, which is the
                        # authoritative signal — a single bad frame is not.)
                        # Type name only — same credential-adjacent rule as
                        # every other except in this class.
                        log.error("Could not push a live frame: %s", type(exc).__name__)
            await asyncio.sleep(interval_s)

    def _push_frame(self, bgr) -> None:
        rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
        self._track.push(rgb)

    # -- the bitrate budget ------------------------------------------------

    async def _hold_bitrate_budget(self) -> None:
        """Keep the encoder's target at `bitrate_kbps`.

        **Why this is a loop and not one assignment.** aiortc writes the
        receiver's own estimate onto the same attribute every time a REMB
        arrives, so a budget set once survives until the first feedback packet
        and no longer. Re-asserting it every tick is what makes the budget a
        budget rather than a starting value the first REMB (or the codec's own
        construction-time default, which is below most configured budgets)
        overwrites.

        **This sets the target in both directions.** A budget above the
        encoder's construction-time default (VP8 starts at 500 kbps, H264 at
        1 Mbps) used to never be reached: the old code only ever *lowered* the
        target, so a value the encoder had never risen to on its own stayed
        unapplied for the whole session. The assignment below always writes
        `budget_bps`; what stops it is stated next, not this loop.

        **What cannot be set from here, stated rather than discovered later.**
        The encoders clamp what they are given — VP8 to 250 kbps…1.5 Mbps,
        H264 to 500 kbps…3 Mbps — so a budget outside those bounds is honoured
        only as far as the bound, and that clamp is the only limit: the
        assignment below is unconditional, and `target_bitrate`'s own setter
        does the clamping. The first time that happens for a session, it is
        logged at INFO with the configured value, the clamp and the codec, so
        `bitrate_kbps: 2000` quietly encoding at VP8's 1.5 Mbps ceiling is
        something an operator can find in a log rather than something only
        this docstring says. Frame rate is not an encoder setting at all in
        aiortc; it is the track's pacing, which `CameraTrack` takes from the
        same `fps` the camera is configured with. Simulcast layers and
        per-layer resolution have no equivalent here: one track is published at
        one resolution, and `subscribedQualityUpdate` is honoured as
        publish/do-not-publish rather than as a layer choice.
        """
        sender = self._sender
        budget_bps = self._bitrate_kbps * 1000
        if sender is None:
            return
        if not hasattr(sender, SENDER_ENCODER_ATTR):
            log.warning(
                "This aiortc keeps its encoder somewhere else than %s; the %d kbps "
                "budget for this camera is not applied",
                SENDER_ENCODER_ATTR, self._bitrate_kbps,
            )
            return
        while True:
            encoder = getattr(sender, SENDER_ENCODER_ATTR, None)
            if encoder is not None and hasattr(encoder, "target_bitrate"):
                if not self._bitrate_clamp_logged:
                    self._bitrate_clamp_logged = True
                    codec_name = type(encoder).__name__
                    bounds = ENCODER_CLAMP_BPS.get(codec_name)
                    if bounds is not None:
                        lo, hi = bounds
                        if budget_bps < lo or budget_bps > hi:
                            applied = max(lo, min(budget_bps, hi))
                            log.info(
                                "bitrate_kbps=%d (%d bps) is outside %s's clamp "
                                "(%d-%d bps); this camera will encode at %d bps, not "
                                "the configured value.",
                                self._bitrate_kbps, budget_bps, codec_name, lo, hi, applied,
                            )
                try:
                    encoder.target_bitrate = budget_bps
                except Exception as exc:  # noqa: BLE001 - the session continues either way
                    log.error("Could not apply the live bitrate budget: %s",
                              type(exc).__name__)
                    return
            await asyncio.sleep(self._bitrate_interval_s)


def _published_track_sid(published: Any) -> Optional[str]:
    """The track sid out of a `trackPublished` payload, or `None`.

    `None` is a real answer and not a failure: the sid is only used to ignore
    a `subscribedQualityUpdate` about somebody else's track, so a server that
    stops sending it costs this publisher the filter, not the session.
    """
    if not isinstance(published, dict):
        return None
    track = published.get("track")
    if not isinstance(track, dict):
        return None
    sid = track.get("sid")
    return sid if isinstance(sid, str) and sid else None
