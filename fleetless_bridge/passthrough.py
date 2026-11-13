# SPDX-License-Identifier: Apache-2.0
"""What sits between a source that already delivers H.264 and the sender.

A passthrough camera is not decoded and not encoded: the bridge forwards the
access units it receives. `PassthroughStream` is everything that has to be
decided about those units on the way, one instance per camera:

- **Start gating.** A viewer can only start at a keyframe. After an attach
  and after a resume from pause, units are dropped until an IDR arrives. A
  keyframe request from a viewer changes nothing: neither RTSP nor a ROS topic
  can ask the source for one.
- **Parameter sets.** The newest SPS and PPS are kept and put in front of
  every IDR that does not carry them, so a source that sends them once, or
  out of band, still gives a late viewer a stream it can decode.
- **A bounded queue, and no skipping.** Frames of a stream depend on each
  other, so "newest frame only" (camera.LatestFrameHolder) is wrong here. If
  the sender falls about two seconds behind, the queue is emptied and
  forwarding waits for the next IDR.
- **The newest keyframe**, kept whether or not anybody watches, because a
  snapshot is decoded from it.
- **The refusals.** A picture size other than the configured one, B-frames,
  and ten seconds of units without a keyframe each end in `PassthroughRefused`
  with the sentence the camera reports.

Fed from a source's thread (`push`), drained from the event loop (`get`); the
lock is what makes that safe. No rclpy and no aiortc. PyAV is imported for one
thing only, `decode_keyframe`, and nothing in `PassthroughStream` calls it.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, Optional, Tuple

import av
import numpy as np

from fleetless_bridge import h264

log = logging.getLogger(__name__)

#: How far the sender may fall behind before the queue is emptied, measured
#: between the capture times of the oldest and the newest queued unit.
QUEUE_MAX_MS = 2000

#: The same bound in units, for a source whose timestamps do not move. Two
#: seconds at 240 frames a second, with room.
QUEUE_MAX_UNITS = 512

#: The queue's third bound, in bytes: several times what two seconds of the
#: highest bitrate a camera may be configured with come to (12.5 MB), so an
#: honest stream never meets it. A stream whose units are large and many
#: does, long before the other two bounds.
QUEUE_MAX_BYTES = 32 * 1024 * 1024

#: The largest access unit a camera's stream may hand over: this many bytes
#: for every pixel of the configured size, which is twice a raw 4:2:0
#: picture, and never less than the floor, for the small sizes. An encoder
#: can make a frame larger than the picture it encodes: x264 on noise, at its
#: finest quantizers, comes to a little over two bytes a pixel. Three leaves
#: room above that.
#: The one bound that keeps three things small: what the queue holds per
#: unit, the newest keyframe, and the decode a snapshot costs.
UNIT_BYTES_PER_PIXEL = 3
UNIT_MAX_BYTES_FLOOR = 4 * 1024 * 1024

#: How much of an access unit over that bound is still looked at: enough for
#: an access unit delimiter and a parameter set of the largest size in front
#: of the SPS, and the SPS itself.
OVERSIZED_HEAD_BYTES = 3 * h264.PARAMETER_SET_MAX_BYTES

#: A stream without a keyframe for this long is refused, and a refusal for
#: B-frames stands this long after the last one.
KEYFRAME_TIMEOUT_S = 10.0

#: At most one line per camera in this many seconds for units that do not
#: parse. A source that sends garbage sends it at frame rate.
PARSE_LOG_INTERVAL_S = 60.0


class PassthroughRefused(Exception):
    """This stream cannot be passed through. The message is the sentence the
    camera reports, written for the developer who configured it."""


class KeyframeDecodeError(Exception):
    """A keyframe did not decode to a picture."""


@dataclass(frozen=True)
class Unit:
    """One access unit on its way to the sender: its NAL units without start
    codes, the bridge's capture time, and whether a viewer can start here.
    A keyframe carries SPS and PPS in front of its slices."""

    nals: Tuple[bytes, ...]
    timestamp_ms: int
    keyframe: bool
    #: Bytes this unit takes in the queue and on its way out: its NAL
    #: units, the parameter sets put in front of a keyframe included.
    size: int = 0


@dataclass(frozen=True)
class Keyframe:
    """The newest IDR access unit as Annex-B bytes, parameter sets included:
    everything a decoder needs for one picture."""

    data: bytes
    timestamp_ms: int


class PassthroughStream:
    """One camera's H.264 stream between its source and its sender.

    `width` and `height` are the configured size, which the stream must
    have. `slug` appears in log lines and nowhere else. `clock` is
    `time.monotonic` unless a test wants to decide what ten seconds are.
    """

    def __init__(
        self,
        *,
        width: int,
        height: int,
        slug: str = "",
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._configured = (width, height)
        #: The largest access unit this camera's size allows, in bytes.
        self.max_unit_bytes = _max_unit_bytes(width, height)
        self._slug = slug
        self._clock = clock
        self._lock = threading.Lock()

        # The stream as the source last described it.
        self._sps: Dict[int, bytes] = {}
        self._pps: Dict[int, bytes] = {}
        self._size: Optional[Tuple[int, int]] = None
        self._keyframe: Optional[Keyframe] = None
        # A usable keyframe has been seen since the last reset or refusal.
        self._started = False

        self._refusal: Optional[str] = None
        self._refused_until = 0.0
        self._refusals = 0  # how many times a refusal was entered

        # Three clocks, all on arriving units: the last arrival, the last
        # usable keyframe, the last unit that parsed.
        self._last_arrival: Optional[float] = None
        self._keyframe_clock = 0.0
        self._parse_clock = 0.0
        self._parse_logged_at: Optional[float] = None

        # The consumer: at most one, on an event loop.
        self._queue: Deque[Unit] = deque()
        self._queue_bytes = 0
        self._attached = False
        self._paused = False
        # An IDR has been queued since the last attach, resume, overflow or
        # hole in the stream.
        self._forwarding = False
        self._overflow_logged = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._event: Optional[asyncio.Event] = None

    # -- the source's side -------------------------------------------------

    def set_parameter_sets(self, data: bytes) -> None:
        """SPS and PPS that arrived outside the stream, as Annex-B bytes --
        RTSP carries them in the session description. Raises
        `PassthroughRefused` if the size they declare is not the configured
        one, and `h264.H264ParseError` if they do not parse."""
        info = h264.inspect_access_unit(data)
        sps = h264.parse_sps(info.sps) if info.sps is not None else None
        pps_ids = [h264.pps_id(nal) for nal in info.pps]
        with self._lock:
            before = self._refusals
            self._store_parameter_sets(info, sps, pps_ids, self._clock())
            # Only a refusal these sets caused. One that stands from before
            # is the stream's to raise, at its next unit: sets that are fine
            # must not be answered with a sentence about the old stream.
            if self._refusals != before:
                raise PassthroughRefused(self._refusal)

    def push(self, data: bytes, timestamp_ms: int) -> bool:
        """One access unit from the source, with the bridge's capture time.

        Returns `True` when the unit belongs to a stream that has started:
        a usable keyframe, or anything after one. Returns `False` for what
        is dropped without being anybody's fault yet -- units before the
        first keyframe, bytes that do not parse.

        Raises `PassthroughRefused` while the stream is one that cannot be
        passed through. The refusal stands until a keyframe of an acceptable
        stream arrives, so every unit in between raises it again; the caller
        reports a refusal once.
        """
        size = len(data)
        if size > self.max_unit_bytes:
            # Dropped like a unit that does not parse: no stream of this
            # camera's size has units this large, and what follows it has a
            # hole in front. Nothing of it is read but an SPS at its head,
            # for the size it declares: a stream far larger than the camera
            # is configured as is then refused for its size, at once, and
            # not ten seconds later for keyframes that were all dropped.
            return self._push_unparseable(
                "an access unit of {} bytes, more than the {} this camera's "
                "size allows".format(size, self.max_unit_bytes),
                leading_sps=_leading_sps(data),
            )
        try:
            info = h264.inspect_access_unit(data)
            sps = h264.parse_sps(info.sps) if info.sps is not None else None
            pps_ids = [h264.pps_id(nal) for nal in info.pps]
        except h264.H264ParseError as exc:
            return self._push_unparseable(str(exc))

        with self._lock:
            now = self._clock()
            self._note_arrival(now)
            self._parse_clock = now
            self._store_parameter_sets(info, sps, pps_ids, now)

            if not info.has_slice:
                # Parameter sets travelling alone: stored, nothing to forward.
                if self._refusal is not None:
                    raise PassthroughRefused(self._refusal)
                return self._started

            if info.has_b_slice:
                self._refuse(
                    "the stream has B-frames; passthrough needs a stream "
                    "without them, which is set at the camera",
                    now, hold_s=KEYFRAME_TIMEOUT_S,
                )

            usable_keyframe = info.is_idr and bool(self._sps) and bool(self._pps)
            if usable_keyframe:
                self._keyframe_clock = now
            elif now - self._keyframe_clock >= KEYFRAME_TIMEOUT_S:
                self._refuse(
                    "no keyframe in {:.0f} s; a viewer starts at a keyframe, so "
                    "passthrough needs one every one to two seconds, with SPS and "
                    "PPS, which is set at the camera".format(KEYFRAME_TIMEOUT_S),
                    now,
                )

            if self._refusal is not None:
                acceptable = (
                    usable_keyframe
                    and not info.has_b_slice
                    and now >= self._refused_until
                    and self._size == self._configured
                )
                if not acceptable:
                    raise PassthroughRefused(self._refusal)
                self._refusal = None

            if usable_keyframe:
                nals = self._with_parameter_sets(info)
                self._keyframe = Keyframe(data=h264.join_annexb(nals), timestamp_ms=timestamp_ms)
                self._started = True
            elif not self._started:
                return False  # nothing to build on yet
            else:
                nals = info.nals
            # Counted as what is queued and sent, not as what arrived: a
            # keyframe that lacked its parameter sets carries the stored
            # ones now.
            unit = Unit(
                nals=nals, timestamp_ms=timestamp_ms, keyframe=usable_keyframe,
                size=sum(len(nal) for nal in nals),
            )

            woke = self._offer(unit)
        if woke:
            self._wake()
        return True

    def reset(self) -> None:
        """The source starts over: a new RTSP session, or a topic that
        carried something else in between. Parameter sets and the newest
        keyframe belonged to the old stream. Whoever is attached stays
        attached and gets the new stream from its first keyframe.

        A refusal stays. The stream that comes back is judged at its first
        keyframe like any other, and a refusal that is being held -- B-frames
        seen less than ten seconds ago -- must not be forgotten by a
        reconnect that fetches the same stream again."""
        with self._lock:
            self._sps.clear()
            self._pps.clear()
            self._size = None
            self._keyframe = None
            self._started = False
            self._last_arrival = None
            self._clear_queue()
            self._forwarding = False

    def set_size(self, width: int, height: int) -> None:
        """The configured size changed. What was decided against the old one
        is void: the stream is judged again, at its next keyframe."""
        with self._lock:
            self._configured = (width, height)
            self.max_unit_bytes = _max_unit_bytes(width, height)
            self._refusal = None
            self._refused_until = 0.0
            self._started = False
            self._forwarding = False
            self._clear_queue()
            self._keyframe = None
            if self._size is not None:
                self._check_size(self._clock())

    def newest_keyframe(self) -> Optional[Keyframe]:
        """The newest keyframe of an accepted stream, or `None`: no keyframe
        yet, or a stream that is refused, of which nothing is published."""
        with self._lock:
            return self._keyframe

    @property
    def refusal(self) -> Optional[str]:
        with self._lock:
            return self._refusal

    # -- the sender's side -------------------------------------------------

    def attach(self, loop: asyncio.AbstractEventLoop) -> None:
        """A sender starts pulling, on `loop`. Called on that loop. Whatever
        arrives from here on is queued, beginning with the next keyframe."""
        with self._lock:
            self._loop = loop
            self._event = asyncio.Event()
            self._attached = True
            self._paused = False
            self._forwarding = False
            self._overflow_logged = False
            self._clear_queue()

    def detach(self) -> None:
        """The sender is gone. A `get()` still waiting returns `None`."""
        with self._lock:
            self._attached = False
            self._forwarding = False
            self._clear_queue()
        self._wake()

    def set_paused(self, paused: bool) -> None:
        """Nobody is subscribed, or somebody is again. While paused, units
        are discarded as they arrive; a resume starts at the next keyframe."""
        with self._lock:
            if paused == self._paused:
                return
            self._paused = paused
            self._forwarding = False
            self._clear_queue()

    def resync(self) -> None:
        """A unit that was handed out did not reach the viewers. What
        follows it was predicted from it, so forwarding waits for the next
        keyframe. Callable from any thread."""
        with self._lock:
            self._forwarding = False
            self._clear_queue()

    def pending(self) -> int:
        """How many units wait for the sender."""
        with self._lock:
            return len(self._queue)

    def pending_bytes(self) -> int:
        """How many bytes those units are."""
        with self._lock:
            return self._queue_bytes

    async def get(self) -> Optional[Unit]:
        """The next unit, in order, waiting for one if need be. `None` once
        the stream is detached."""
        while True:
            with self._lock:
                if self._queue:
                    # The sender pulls again: an overflow episode is over.
                    self._overflow_logged = False
                    unit = self._queue.popleft()
                    self._queue_bytes -= unit.size
                    return unit
                if not self._attached:
                    return None
                event = self._event
                event.clear()
            await event.wait()

    # -- inside the lock ---------------------------------------------------

    def _clear_queue(self) -> None:
        self._queue.clear()
        self._queue_bytes = 0

    def _note_arrival(self, now: float) -> None:
        """Both timeouts count time during which units arrive. After a
        silence as long as the timeout itself they start again: a source that
        sent nothing has stalled, and a stall is the source adapter's to
        report, not a missing keyframe."""
        if self._last_arrival is None or now - self._last_arrival >= KEYFRAME_TIMEOUT_S:
            self._keyframe_clock = now
            self._parse_clock = now
        self._last_arrival = now

    def _store_parameter_sets(self, info, sps, pps_ids, now: float) -> None:
        if sps is not None:
            self._sps[sps.sps_id] = info.sps
            self._size = (sps.width, sps.height)
            self._check_size(now)
        for identifier, nal in zip(pps_ids, info.pps):
            self._pps[identifier] = nal

    def _check_size(self, now: float) -> None:
        if self._size != self._configured:
            self._refuse(
                "the stream is {}x{} and the camera is configured as {}x{}; "
                "passthrough forwards the stream as it is, so width and "
                "height must match it".format(
                    self._size[0], self._size[1], self._configured[0], self._configured[1]
                ),
                now,
            )

    def _refuse(self, message: str, now: float, hold_s: float = 0.0) -> None:
        """Nothing of a refused stream is published: the queue and the
        keyframe go with it."""
        self._refusal = message
        self._refusals += 1
        self._refused_until = max(self._refused_until, now + hold_s)
        self._started = False
        self._forwarding = False
        self._clear_queue()
        self._keyframe = None

    def _with_parameter_sets(self, info: h264.AccessUnit) -> Tuple[bytes, ...]:
        """The keyframe's NAL units with SPS and PPS in front of its slices.

        A unit that carries both is handed on as it came. One that lacks a
        set gets what the source sent earlier, and is put in the order a
        decoder needs: an access unit delimiter if the unit opens with one,
        then every SPS, then every PPS, then the rest as it stood. The order
        matters when only one of the two is in band: a PPS in front of the
        SPS it refers to is dropped by a decoder that has not seen that SPS
        yet, which is every new viewer's and the snapshot's."""
        if info.sps is not None and info.pps:
            return info.nals
        nals = list(info.nals)
        head = nals[:1] if h264.nal_type(nals[0]) == h264.NAL_AUD else []
        body = nals[len(head):]
        sps = [nal for nal in body if h264.nal_type(nal) == h264.NAL_SPS]
        pps = [nal for nal in body if h264.nal_type(nal) == h264.NAL_PPS]
        rest = [nal for nal in body if h264.nal_type(nal) not in (h264.NAL_SPS, h264.NAL_PPS)]
        if not sps:
            sps = [self._sps[key] for key in sorted(self._sps)]
        if not pps:
            pps = [self._pps[key] for key in sorted(self._pps)]
        return tuple(head + sps + pps + rest)

    def _offer(self, unit: Unit) -> bool:
        """Queues `unit` if a sender wants it. Returns whether to wake it."""
        if not self._attached or self._paused:
            return False
        if unit.keyframe:
            self._forwarding = True
        if not self._forwarding:
            return False
        if self._queue and (
            unit.timestamp_ms - self._queue[0].timestamp_ms > QUEUE_MAX_MS
            or len(self._queue) >= QUEUE_MAX_UNITS
            or self._queue_bytes + unit.size > QUEUE_MAX_BYTES
        ):
            dropped = len(self._queue)
            self._clear_queue()
            if not self._overflow_logged:
                self._overflow_logged = True
                log.warning(
                    "Camera slug %r: the sender fell behind and the passthrough "
                    "queue overflowed; dropped %d units, forwarding resumes at "
                    "the next keyframe",
                    self._slug, dropped,
                )
            if not unit.keyframe:
                self._forwarding = False
                return False
        self._queue.append(unit)
        self._queue_bytes += unit.size
        return True

    # -- outside the lock --------------------------------------------------

    def _push_unparseable(
        self, reason: str, leading_sps: Optional[Tuple[bytes, h264.Sps]] = None
    ) -> bool:
        with self._lock:
            now = self._clock()
            self._note_arrival(now)
            # A hole: what follows was predicted from a unit nobody has.
            self._forwarding = False
            if leading_sps is not None:
                nal, sps = leading_sps
                self._sps[sps.sps_id] = nal
                self._size = (sps.width, sps.height)
                self._check_size(now)
            if (
                self._parse_logged_at is None
                or now - self._parse_logged_at >= PARSE_LOG_INTERVAL_S
            ):
                self._parse_logged_at = now
                log.warning(
                    "Camera slug %r: dropped a unit that is not parseable H.264 "
                    "(%s); forwarding waits for the next keyframe",
                    self._slug, reason,
                )
            if now - self._parse_clock >= KEYFRAME_TIMEOUT_S:
                self._refuse(
                    "nothing in {:.0f} s that reads as H.264 in Annex-B form; "
                    "passthrough forwards H.264 only".format(KEYFRAME_TIMEOUT_S),
                    now,
                )
            if self._refusal is not None:
                raise PassthroughRefused(self._refusal)
        return False

    def _wake(self) -> None:
        loop, event = self._loop, self._event
        if loop is None or event is None:
            return
        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            pass  # the loop is closed; nobody is waiting on it


def _max_unit_bytes(width: int, height: int) -> int:
    return max(UNIT_MAX_BYTES_FLOOR, UNIT_BYTES_PER_PIXEL * width * height)


def _leading_sps(data: bytes) -> Optional[Tuple[bytes, h264.Sps]]:
    """The SPS at the head of an access unit too large to be read whole: the
    NAL unit and what it declares, or `None`. Only the first
    `OVERSIZED_HEAD_BYTES` are looked at, which is where an encoder puts it;
    whatever else is in them is passed over, and anything that does not read
    as H.264 there is no SPS."""
    try:
        nals = h264.split_nal_units(data[:OVERSIZED_HEAD_BYTES])
        # The last unit of a head that was cut off is cut off itself.
        for nal in nals[:-1]:
            if h264.nal_type(nal) == h264.NAL_SPS:
                return nal, h264.parse_sps(nal)
    except h264.H264ParseError:
        pass
    return None


def _new_decoder():
    """The one place this module gets a decoder from."""
    return av.CodecContext.create("h264", "r")


def decode_keyframe(data: bytes) -> "np.ndarray":
    """One keyframe, as `Keyframe.data` holds it, decoded to a BGR picture
    with PyAV's software decoder. This is the only decoding a passthrough
    camera ever costs, and it happens when a snapshot is due.

    A decoder per call: one picture every snapshot interval does not justify
    a decoder kept across streams that can change under it.

    Raises `KeyframeDecodeError` with the library error's type name, never its
    text.
    """
    try:
        decoder = _new_decoder()
        frames = list(decoder.decode(av.Packet(data)))
        frames.extend(decoder.decode(None))
        if not frames:
            raise KeyframeDecodeError("the decoder returned no picture")
        return frames[0].to_ndarray(format="bgr24")
    except KeyframeDecodeError:
        raise
    except Exception as exc:  # noqa: BLE001 - PyAV's error types differ by version
        raise KeyframeDecodeError(type(exc).__name__) from None
