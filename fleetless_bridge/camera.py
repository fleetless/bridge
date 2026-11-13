# SPDX-License-Identifier: Apache-2.0
"""Turning a ROS image message into a BGR array, and back into bytes.

Two message shapes bind to a `cameraConfig.type`: `sensor_msgs/msg/Image`
(raw pixels) and `sensor_msgs/msg/CompressedImage` (already-encoded bytes,
JPEG in practice). `to_bgr` turns either into the same representation — a
BGR `uint8` array — so everything downstream (fps throttling in
ros_runtime.py, resizing, JPEG-encoding for a snapshot, feeding a live
publisher) has exactly one shape to work with, not two.

A source that already delivers JPEG need not go through that array at all:
`jpeg_size` reads what a JPEG is from its header, and `EncodedFrameHolder`
keeps the source's own bytes beside the converted frame, so a snapshot can be
those bytes unchanged (ros_runtime.py decides when).

Deliberately rclpy-free, like sampling.py: it needs message *types* to
recognise a shape, never a node or executor — so it can be tested without
any ROS graph.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import cv2
import numpy as np
from cv_bridge import CvBridge
from sensor_msgs.msg import CompressedImage, Image

_bridge = CvBridge()


class UnsupportedImageError(ValueError):
    """A message could not be turned into a BGR array — an unrecognised
    message type, or bytes that do not decode as an image."""


def to_bgr(msg: object) -> "np.ndarray":
    """`Image` passes through `cv_bridge` (converting to `bgr8` if the
    source encoding differs); `CompressedImage` is decoded with
    `cv2.imdecode`, and so are the bare bytes of an encoded image, which is
    how an MJPEG part is held. Anything else, or bytes that fail to decode,
    raises `UnsupportedImageError` — config-apply time already validated
    `type` against the two known shapes (ros_runtime.py), so reaching this
    branch at all would mean the message on the wire disagrees with its own
    declared type, not a normal-use case."""
    if isinstance(msg, CompressedImage):
        raw = np.frombuffer(bytes(msg.data), dtype=np.uint8)
        frame = cv2.imdecode(raw, cv2.IMREAD_COLOR)
        if frame is None:
            raise UnsupportedImageError("could not decode CompressedImage data")
        return frame
    if isinstance(msg, (bytes, bytearray, memoryview)):
        frame = None
        if len(msg):  # `imdecode` raises on an empty buffer instead of returning nothing
            frame = cv2.imdecode(np.frombuffer(msg, dtype=np.uint8), cv2.IMREAD_COLOR)
        if frame is None:
            raise UnsupportedImageError("could not decode the image bytes")
        return frame
    if isinstance(msg, Image):
        try:
            return _bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")
        except Exception as exc:  # noqa: BLE001 - cv_bridge raises its own types
            raise UnsupportedImageError(str(exc)) from exc
    raise UnsupportedImageError(
        "unsupported message type {!r} — expected Image or CompressedImage".format(type(msg))
    )


def resize_if_needed(frame: "np.ndarray", *, width: int, height: int) -> "np.ndarray":
    """A no-op when `frame` is already the configured size — the common
    case for a source that natively matches the developer's configuration,
    and not worth an unconditional resize's cost."""
    current_height, current_width = frame.shape[:2]
    if (current_width, current_height) == (width, height):
        return frame
    return cv2.resize(frame, (width, height), interpolation=cv2.INTER_AREA)


def encode_jpeg(frame: "np.ndarray", *, quality: Optional[int] = None) -> bytes:
    """The snapshot binary frame's payload (protocol.py's `snapshot_frame`)
    is always JPEG — `mime: 'image/jpeg'` in the header is not a guess.
    `quality` is cv2's own 0-100 scale (its default, ~95, when omitted)."""
    params = [cv2.IMWRITE_JPEG_QUALITY, quality] if quality is not None else []
    ok, buf = cv2.imencode(".jpg", frame, params)
    if not ok:
        raise UnsupportedImageError("cv2.imencode could not produce a JPEG")
    return buf.tobytes()


# Contracts' SNAPSHOT_MAX_BYTES (src/protocol.ts, sha d678bd9): a byte bound,
# not a pixel one. width/height (cameraConfig) govern the *live* stream,
# which travels through LiveKit and never touches the /bridge socket, so
# capping resolution here would cost live quality to solve a snapshot
# problem. This bounds the one thing that actually crosses that socket.
#
# It matters because the socket's own ws library enforces a payload ceiling
# (2 MiB) *before* an oversized frame ever reaches the application: it does
# not drop the frame, it closes the connection with 1009 — taking
# datapoints, jobs, commands and config down with it, and the bridge then
# reconnects into the same configuration and gets closed again. With
# cv2 4.5.4 and real camera content a 4K JPEG lands near 2.2 MiB,
# 1080p on a noisy scene within 40% of this ceiling — not a rare edge case.
SNAPSHOT_MAX_BYTES = 1_572_864

# Tried in order at the current resolution before giving up and downscaling
# — quality first, because it costs nothing structural (the frame is still
# full-resolution) and is often enough on its own.
_QUALITY_LADDER = (85, 65, 45, 25, 10)

# Tried at each quality step once resolution itself has to give — 1.0 first
# (the ladder above already covers "no downscale needed").
_SCALE_LADDER = (1.0, 0.75, 0.5, 0.35, 0.2)


def encode_snapshot_jpeg(
    frame: "np.ndarray", *, max_bytes: int = SNAPSHOT_MAX_BYTES
) -> Optional[Tuple[bytes, int, int]]:
    """Encodes `frame` to JPEG, backing off quality first and then
    resolution until the result fits under `max_bytes` — never
    resolution first, since a snapshot that is merely low-quality is a much
    smaller compromise than one that is also smaller than configured.

    Returns `(data, width, height)` — `width`/`height` are the actually-
    encoded dimensions, which may be smaller than `frame`'s own if
    downscaling was needed to fit. The caller must report *these*, not
    `frame`'s, in the wire header: the same honesty the format already
    requires of `timestamp_ms` (capture time, never send time) applies to
    dimensions too — a header claiming the configured resolution for an
    image that was actually shrunk to fit is its own small lie.

    Returns `None` if nothing fits even at the most aggressive combination
    tried; the caller must then drop this frame rather than send something
    that would close the socket. A missing snapshot is an honest gap (the
    same shape already established for a disconnected bridge, here
    applied to a different cause) — a closed socket is not."""
    for scale in _SCALE_LADDER:
        if scale == 1.0:
            scaled = frame
        else:
            height, width = frame.shape[:2]
            scaled = resize_if_needed(
                frame,
                width=max(1, round(width * scale)),
                height=max(1, round(height * scale)),
            )
        for quality in _QUALITY_LADDER:
            data = encode_jpeg(scaled, quality=quality)
            if len(data) <= max_bytes:
                scaled_height, scaled_width = scaled.shape[:2]
                return data, scaled_width, scaled_height
    return None


# --- a JPEG, read without decoding it -------------------------------------------

_JPEG_RST0 = 0xD0
_JPEG_SOI = 0xD8
_JPEG_EOI = 0xD9
_JPEG_SOS = 0xDA
_JPEG_TEM = 0x01

#: The frame headers a snapshot may be passed through under: baseline (SOF0)
#: and progressive (SOF2), both Huffman-coded. Every browser shows these.
_JPEG_SOF_SHOWN_EVERYWHERE = (0xC0, 0xC2)

#: Every frame header there is: 0xC0 to 0xCF without the three codes in that
#: range that mean something else (Huffman tables, a reserved code, arithmetic
#: conditioning). Extended, lossless, hierarchical and arithmetic-coded files
#: are recognised here so that they can be refused by name, not skipped as an
#: unknown segment and then mistaken for a file without a frame header.
_JPEG_SOF_ANY = frozenset(range(0xC0, 0xD0)) - {0xC4, 0xC8, 0xCC}

#: The segments that may stand between the start marker and the scan of a
#: file every browser shows: application data (0xE0 to 0xEF), a comment,
#: quantisation and Huffman tables, the restart interval, and arithmetic
#: conditioning, which a decoder reads and a Huffman file ignores. Anything
#: else that is not a frame header is refused, not stepped over: libjpeg,
#: which decodes for OpenCV and for the browsers, gives up on a hierarchical
#: marker (DHP, EXP), a JPEG-extension marker (0xF0 to 0xFD) and the reserved
#: codes, so a file that carries one is a file nobody shows.
_JPEG_BEFORE_THE_SCAN = frozenset(range(0xE0, 0xF0)) | {0xFE, 0xDB, 0xC4, 0xDD, 0xCC}


def jpeg_size(data: object) -> Optional[Tuple[int, int]]:
    """`(width, height)` of a JPEG that every browser shows, read from its
    headers. `None` for anything else. Never decodes and never raises.

    What it takes to get a size back, all of it:

    - the bytes begin with the start marker and end with the end marker;
    - every segment up to the scan can be walked by its own length, none
      runs past the end, and each is of a kind a decoder accepts there:
      application data, a comment, tables, the restart interval;
    - exactly one frame header comes before the scan, baseline or
      progressive, with 8 bits a sample, one or three components and a size
      that is not zero;
    - a scan follows.

    So CMYK (four components), 12-bit, lossless and arithmetic-coded files
    get `None`, and so does a file cut short anywhere, a PNG, and bytes that
    merely begin like a JPEG. A thumbnail inside an EXIF block is a whole
    JPEG of its own, frame header included; it sits inside an application
    segment and is stepped over with it.

    The scan itself is not read: whether it decodes is the viewer's to find
    out, as it is for any picture nobody here looked at.

    `data` is anything that exposes its bytes: `bytes`, a `bytearray`, a
    `memoryview`, the `array('B')` a ROS message holds its data in.
    """
    try:
        view = memoryview(data)  # type: ignore[arg-type]
    except TypeError:
        return None
    if view.ndim != 1 or view.itemsize != 1:
        return None
    try:
        view = view.cast("B")
    except (TypeError, ValueError):
        return None
    end = len(view)
    if end < 4 or view[0] != 0xFF or view[1] != _JPEG_SOI:
        return None
    if view[end - 2] != 0xFF or view[end - 1] != _JPEG_EOI:
        return None

    size: Optional[Tuple[int, int]] = None
    at = 2
    while True:
        # A marker: 0xFF, any number of 0xFF fill bytes, then its code.
        if at >= end or view[at] != 0xFF:
            return None
        while at < end and view[at] == 0xFF:
            at += 1
        if at >= end:
            return None
        code = view[at]
        at += 1
        if code == _JPEG_TEM:
            continue  # stands alone, no length
        if code == 0x00 or _JPEG_RST0 <= code <= _JPEG_EOI:
            # A stuffed byte, a restart marker (0xD0 to 0xD7), a second
            # start or the end: none of them belongs between the start and
            # the first scan.
            return None
        if at + 2 > end:
            return None
        length = (view[at] << 8) | view[at + 1]  # counts its own two bytes
        if length < 2 or at + length > end:
            return None
        if code == _JPEG_SOS:
            return size  # `None` when no frame header came first
        if code in _JPEG_SOF_ANY:
            if size is not None or code not in _JPEG_SOF_SHOWN_EVERYWHERE or length < 8:
                return None
            precision = view[at + 2]
            height = (view[at + 3] << 8) | view[at + 4]
            width = (view[at + 5] << 8) | view[at + 6]
            components = view[at + 7]
            if (
                precision != 8
                or components not in (1, 3)
                or length != 8 + 3 * components
                or width == 0
                or height == 0
            ):
                return None
            size = (width, height)
        elif code not in _JPEG_BEFORE_THE_SCAN:
            return None
        at += length


@dataclass(frozen=True)
class EncodedFrame:
    #: The source's own bytes, as it delivered them: `bytes`, or whatever a
    #: message holds its data in. Never copied on the way here.
    data: object
    timestamp_ms: int


class EncodedFrameHolder:
    """A single slot for one camera's newest frame as the source encoded it,
    beside the converted one in `LatestFrameHolder`.

    Written on every frame a source delivers already encoded, whether or not
    anybody watches: keeping a reference costs nothing, and it is what lets
    a snapshot be the source's own JPEG. Nothing is looked at when it is
    stored; `jpeg_size` reads it when a snapshot is due.

    `get()` does not clear the slot. The same frame may be the snapshot
    again an interval later, exactly as the converted one may: a camera that
    has stopped sending is seen by the age its snapshot reports.

    Written and read on the executor thread; the lock is for whoever reads
    it from somewhere else one day, as in `RawFrameHolder`."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frame: Optional[EncodedFrame] = None

    def set(self, data: object, timestamp_ms: int) -> None:
        with self._lock:
            self._frame = EncodedFrame(data=data, timestamp_ms=timestamp_ms)

    def get(self) -> Optional[EncodedFrame]:
        with self._lock:
            return self._frame


@dataclass(frozen=True)
class LatestFrame:
    bgr: "np.ndarray"
    timestamp_ms: int


class LatestFrameHolder:
    """A single-slot, mutex-guarded holder for one camera's newest converted
    frame — deliberately not a queue. For video, an old frame has no value
    once a newer one exists (the opposite of a job result, which is why
    `JobUpdateQueue` never drops but this always overwrites; see jobs.py).

    Written from the executor thread (a subscription callback, via
    ros_runtime.py); read from wherever needs "what does this camera see
    right now" — the snapshot watchdog and a live publish task,
    neither of which is guaranteed to be the executor thread, hence the
    lock."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frame: Optional[LatestFrame] = None

    def set(self, bgr: "np.ndarray", timestamp_ms: int) -> None:
        with self._lock:
            self._frame = LatestFrame(bgr=bgr, timestamp_ms=timestamp_ms)

    def get(self) -> Optional[LatestFrame]:
        with self._lock:
            return self._frame


@dataclass(frozen=True)
class RawFrame:
    msg: object
    timestamp_ms: int
    #: `time.monotonic()` when the message arrived. Beside `timestamp_ms`,
    #: not instead of it: that one is the capture time the wire reports, on
    #: the wall clock; this one is for arithmetic with other monotonic
    #: readings, which a wall clock that can be stepped is no good for.
    arrived_s: float = 0.0


class RawFrameHolder:
    """A single-slot, mutex-guarded holder for one camera's newest *un*converted
    source message — the lazy half of "decode only what someone will see".
    `LatestFrameHolder` above holds a converted BGR frame, cheap to keep
    updating; this holds what `to_bgr` takes: the raw message of the `kind:
    'ros'` adapter (an `Image`/`CompressedImage`) or the bytes of an MJPEG
    part. Those are the two sources that can retain a frame without paying
    for its conversion; an RTSP or V4L2 adapter must decode on its own
    thread just to know a frame arrived.

    `take()` — unlike `LatestFrameHolder.get()` — clears the slot: the
    frame is converted at most once, by whichever path consumes it. There
    are two, and both go through `RosRuntime._convert_due_raw_frame`: the
    dueness walk of `_next_snapshot`, and a live start
    (`RosRuntime.convert_newest_raw_frame`). Written on the executor thread
    (the ROS subscription callback, or the hand-off of an MJPEG part);
    `take()` runs on that same thread for both readers, so the lock here is
    defensive rather than load-bearing.

    **What a live start does with it.** It takes the raw frame once, as the
    start begins, and converts it into `entry.latest`, where the publisher
    reads. The first picture a viewer gets is therefore the newest message
    the camera had sent when the start was asked for — at most one camera
    interval old at that moment — and not the next one, which is up to a
    whole interval away. Messages that arrive while the room is being joined
    stay raw; from the moment the stream is live, `_on_source_frame` converts
    every accepted frame as it arrives."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frame: Optional[RawFrame] = None

    def set(self, msg: object, timestamp_ms: int) -> None:
        arrived_s = time.monotonic()
        with self._lock:
            self._frame = RawFrame(msg=msg, timestamp_ms=timestamp_ms, arrived_s=arrived_s)

    def take_frame(self) -> Optional[RawFrame]:
        """The held message with everything known about it, and the slot
        cleared."""
        with self._lock:
            frame = self._frame
            self._frame = None
        return frame

    def take(self) -> Optional[Tuple[object, int]]:
        frame = self.take_frame()
        if frame is None:
            return None
        return frame.msg, frame.timestamp_ms
