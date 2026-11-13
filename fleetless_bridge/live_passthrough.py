# SPDX-License-Identifier: Apache-2.0
"""The track, the encoder and the publisher of a passthrough camera.

aiortc's sender does one thing with a video track on all three versions this
package is built for: it awaits `track.recv()` for a frame and hands that
frame to `encoder.encode(frame, force_keyframe)`, in a worker thread, for RTP
payloads. 1.3.0 has no other way in -- no `pack()` for data that is already
encoded. So passthrough is that same path with nothing in it:

- `PassthroughTrack.recv()` returns once per access unit of the camera's
  stream, in order, with a small placeholder frame. The picture in it is never
  looked at. Its timestamp is the bridge's capture time of the unit.
- `PassthroughEncoder` sits where aiortc's own H.264 encoder would
  (`LivePublisher(encoder=...)`). It overrides `_encode_frame`, ignores the
  picture and yields the NAL units of the access unit that frame stands for.
  `encode()` stays aiortc's, so the packetization and the packet cap are
  aiortc's too. A viewer's keyframe request (`force_keyframe`) and a bitrate
  target arrive and change nothing: the bridge cannot ask a camera for a
  keyframe, and cannot shrink a stream it does not encode.

The two share a `UnitHandoff`: the track files each unit under its frame's
timestamp, the encoder takes it out by that timestamp. `recv()` runs on the
event loop and `encode()` in a worker thread, one frame at a time.

Where a stream starts is not decided here. `passthrough.PassthroughStream`
hands out nothing before a keyframe, after an attach and after a resume.

`PassthroughPublisher` is `live.LivePublisher` with that pair in it. The
session itself -- join, publish, negotiate, lose, stop -- is not touched: the
encoder goes in through the publisher's own encoder seam, the track through
its track factory. Which encoder the robot is configured to use (`live.encoder`)
has no say here: there is nothing to encode.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import OrderedDict
from typing import Callable, Iterator, List, Optional, Tuple

from aiortc.codecs.h264 import H264Encoder
from aiortc.mediastreams import (
    VIDEO_CLOCK_RATE,
    VIDEO_TIME_BASE,
    MediaStreamError,
    VideoStreamTrack,
)

# Imported as in live.py: `av` arrives with python3-aiortc.
from av import VideoFrame

from fleetless_bridge import live
from fleetless_bridge.camera import LatestFrameHolder
from fleetless_bridge.passthrough import PassthroughStream, Unit

log = logging.getLogger(__name__)

#: At most one line in this many seconds for a unit the encoder could not
#: hand on. Whatever makes one fail is likely to make the next fail too.
FAILURE_LOG_INTERVAL_S = 60.0

#: Edge length of the placeholder frame, in pixels. The smallest size every
#: PyAV version allocates without complaint for 4:2:0; nothing reads it.
PLACEHOLDER_SIZE = 16

#: How many units may wait between `recv()` and `encode()`. The sender takes
#: one frame and encodes it before it asks for the next, so one is the normal
#: number; a frame handed out and never encoded (the sender stopped in
#: between) must not stay forever.
HANDOFF_MAX_UNITS = 4


class UnitHandoff:
    """Access units between the track that handed out their frames and the
    encoder that is asked to encode them, keyed by the frame's `pts`."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._units: "OrderedDict[int, Unit]" = OrderedDict()

    def put(self, pts: int, unit: Unit) -> None:
        with self._lock:
            self._units[pts] = unit
            while len(self._units) > HANDOFF_MAX_UNITS:
                self._units.popitem(last=False)

    def take(self, pts: int) -> Optional[Unit]:
        with self._lock:
            return self._units.pop(pts, None)

    def clear(self) -> None:
        with self._lock:
            self._units.clear()


class PassthroughTrack(VideoStreamTrack):
    """One placeholder frame per access unit of `stream`, in order.

    **The stream is attached at the first `recv()`**, not at construction:
    the sender starts pulling once the connection is up, and units queued
    before that would only run into the queue's two-second bound. From the
    attach on, the stream hands out a keyframe first.

    **The timestamp is capture time.** `pts` counts 90 kHz ticks from the
    first unit's capture time to this one's, so the pace a viewer plays at is
    the pace the units arrived at -- nothing here sleeps or paces. Capture
    time is a wall clock and may stand still or step back; `pts` then moves
    on by one tick, because an RTP timestamp that repeats says "same
    picture".
    """

    kind = "video"

    def __init__(self, stream: PassthroughStream, handoff: UnitHandoff) -> None:
        super().__init__()
        self._stream = stream
        self._handoff = handoff
        self._attached = False
        self._first_ms: Optional[int] = None
        self._last_pts = -1
        #: How many frames this track has handed to the sender; read by the
        #: live proof, like `CameraTrack.frames_sent`.
        self.frames_sent = 0

    async def recv(self) -> VideoFrame:
        if self.readyState != "live":
            raise MediaStreamError
        if not self._attached:
            self._attached = True
            self._stream.attach(asyncio.get_running_loop())
        unit = await self._stream.get()
        if unit is None or self.readyState != "live":
            raise MediaStreamError

        if self._first_ms is None:
            self._first_ms = unit.timestamp_ms
        pts = (unit.timestamp_ms - self._first_ms) * VIDEO_CLOCK_RATE // 1000
        if pts <= self._last_pts:
            pts = self._last_pts + 1
        self._last_pts = pts

        frame = VideoFrame(PLACEHOLDER_SIZE, PLACEHOLDER_SIZE, "yuv420p")
        frame.pts = pts
        frame.time_base = VIDEO_TIME_BASE
        self._handoff.put(pts, unit)
        self.frames_sent += 1
        return frame

    def stop(self) -> None:
        super().stop()
        # A `recv()` parked on the stream wakes with `None` and sees the
        # ended state, or the sender's task never finishes.
        self._stream.detach()
        self._handoff.clear()


class PassthroughEncoder(H264Encoder):
    """Answers `encode()` with the access unit the frame stands for.

    A subclass of aiortc's `H264Encoder` for its `encode()`, `_packetize`
    and `target_bitrate`; its `_encode_frame` -- the one method that would
    open libx264 -- is replaced. `codec` therefore stays `None` for the life
    of the object, which is how a proof shows that nothing was encoded.

    **`encode()` never raises.** aiortc's sender catches what an encoder
    raises, logs the exception's message, stops the track for good and tells
    nobody: the session would stay up with no picture and no report. So
    whatever goes wrong with one unit is caught here, logged by type name,
    and that unit is not sent. `on_failure` is then called, and the stream
    it belongs to waits for its next keyframe, as after any hole.

    Nothing to close: no codec, no pipeline, no thread.
    """

    def __init__(
        self, handoff: UnitHandoff, on_failure: Optional[Callable[[], None]] = None
    ) -> None:
        super().__init__()
        self._handoff = handoff
        self._on_failure = on_failure
        self._failure_logged_at: Optional[float] = None
        #: Access units handed to the packetizer.
        self.units_forwarded = 0

    def _encode_frame(self, frame: VideoFrame, force_keyframe: bool) -> Iterator[bytes]:
        """The NAL units of `frame`'s access unit, without start codes.

        `force_keyframe` is a viewer's keyframe request. aiortc clears its
        flag before it calls `encode()`, so the request arrives here exactly
        once -- and is ignored, by design: the next keyframe is the source's
        next keyframe, and the stream starts a new viewer there by itself.

        A frame this encoder holds no unit for yields nothing, and nothing
        is sent for it."""
        unit = self._handoff.take(frame.pts)
        if unit is None:
            return
        self.units_forwarded += 1
        yield from unit.nals

    def encode(self, frame: VideoFrame, force_keyframe: bool = False) -> Tuple[List[bytes], int]:
        """aiortc's `encode()`, which packetizes what `_encode_frame` yields,
        with everything it could raise kept in here."""
        try:
            return super().encode(frame, force_keyframe)
        except Exception as exc:  # noqa: BLE001 - see the class docstring
            self._note_failure(exc)
            try:
                timestamp = int(frame.pts)
            except Exception:  # noqa: BLE001 - a frame without a timestamp sends nothing either
                timestamp = 0
            return [], timestamp

    def _note_failure(self, exc: Exception) -> None:
        now = time.monotonic()
        if (
            self._failure_logged_at is None
            or now - self._failure_logged_at >= FAILURE_LOG_INTERVAL_S
        ):
            self._failure_logged_at = now
            # The type and nothing else: this object sits behind a camera
            # whose URL can carry a password.
            log.error(
                "Could not forward an access unit (%s); forwarding waits for "
                "the next keyframe",
                type(exc).__name__,
            )
        if self._on_failure is not None:
            try:
                self._on_failure()
            except Exception as failure:  # noqa: BLE001 - still must not raise
                log.error("Error after a unit that was not forwarded: %s", type(failure).__name__)


def _close_unused(encoder: Optional[object]) -> None:
    """Closes an encoder a passthrough publisher was handed and has no use
    for. Off the calling thread, logged by type name if it fails, and never
    raised: a start must not fail over an object it does not use."""
    close = getattr(encoder, "close", None)
    if not callable(close):
        return

    def run() -> None:
        try:
            close()
        except Exception as exc:  # noqa: BLE001 - the type, never the text: see PassthroughEncoder
            log.error(
                "Error closing an encoder a passthrough camera has no use for: %s",
                type(exc).__name__,
            )

    threading.Thread(target=run, name="passthrough-unused-encoder-close", daemon=True).start()


class PassthroughPublisher(live.LivePublisher):
    """One passthrough camera's live session.

    `passthrough` is the camera's stream. Everything else is
    `live.LivePublisher`'s, with three differences:

    - the track is a `PassthroughTrack` and the encoder a
      `PassthroughEncoder`, handed to the base class through `track_factory`
      and `encoder`. With an encoder the base class offers H.264 only, which
      is the one codec this publisher can send;
    - no frame is fed from a holder. A passthrough camera's holder carries
      its snapshots' pictures, decoded once per interval; none of them
      belongs in the stream;
    - a pause and a resume reach the stream, which discards units while
      nobody is subscribed and starts again at a keyframe.

    `bitrate_kbps` is still what the track declares to the media server, and
    the budget loop still writes it onto the encoder, where it changes
    nothing. `width`, `height` and `fps` are declared as configured; the
    stream was checked against the first two before it got here.

    **`encoder` is taken and not used.** Whoever starts publishers may hand
    every one of them the encoder the robot's `live.encoder` names. That
    setting does not apply to a passthrough camera: nothing is encoded. An
    encoder handed over here is therefore never installed, and is closed at
    once, in a thread of its own -- the publisher is the only one left who
    could close it, and closing a hardware pipeline may take a moment.

    **`on_resume` is taken and not used either.** The base class tells its
    camera when a viewer comes back, so that the message the camera held
    while paused is converted, and keeps the track from the sender until
    that conversion has landed. A passthrough camera holds no raw message
    and converts nothing for live; its stream starts again at the source's
    next keyframe by itself. A hook handed over all the same is not kept, so
    a resume never asks for, or waits for, a conversion that does not exist.
    """

    def __init__(
        self, *, passthrough: PassthroughStream, encoder: Optional[object] = None, **kwargs
    ) -> None:
        _close_unused(encoder)
        kwargs.pop("on_resume", None)
        handoff = UnitHandoff()

        def track_factory(width: int, height: int, fps: int, bitrate_kbps: int) -> PassthroughTrack:
            return PassthroughTrack(passthrough, handoff)

        # The holder the caller passed is the camera's picture holder. This
        # publisher reads none, and is given an empty one so that nothing of
        # the base class could find a picture to feed.
        kwargs["holder"] = LatestFrameHolder()
        kwargs.setdefault("track_factory", track_factory)
        super().__init__(
            encoder=PassthroughEncoder(handoff, on_failure=passthrough.resync), **kwargs
        )
        #: The stream this publisher forwards. The runtime reads it to tell
        #: a passthrough publisher from one that encodes.
        self.passthrough = passthrough

    async def _feed_frames(self) -> None:
        """Nothing to feed: the track pulls the stream itself."""

    def _answer_keyframe_requests_at_once(self) -> None:
        """Nothing to arrange. The base class wakes its track so that a
        keyframe a viewer asked for is encoded at once; here no keyframe can
        be made to order, and the track is never asleep between units. The
        sender's own handling of the request stays as it is and reaches an
        encoder that ignores it."""

    def _set_paused(self, paused: bool) -> None:
        super()._set_paused(paused)
        # After the base class, which decides whether the state changes at
        # all; the stream follows the state it ends up with.
        self.passthrough.set_paused(self._paused)
