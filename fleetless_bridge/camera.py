# SPDX-License-Identifier: Apache-2.0
"""Turning a ROS image message into a BGR array, and back into bytes.

Two message shapes bind to a `cameraConfig.type`: `sensor_msgs/msg/Image`
(raw pixels) and `sensor_msgs/msg/CompressedImage` (already-encoded bytes,
JPEG in practice). `to_bgr` turns either into the same representation — a
BGR `uint8` array — so everything downstream (fps throttling in
ros_runtime.py, resizing, JPEG-encoding for a snapshot, feeding a live
publisher) has exactly one shape to work with, not two.

Deliberately rclpy-free, like sampling.py: it needs message *types* to
recognise a shape, never a node or executor — so it can be tested without
any ROS graph.
"""
from __future__ import annotations

import threading
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
    `cv2.imdecode`. Anything else, or bytes that fail to decode, raises
    `UnsupportedImageError` — config-apply time already validated `type`
    against the two known shapes (ros_runtime.py), so reaching this branch
    at all would mean the message on the wire disagrees with its own
    declared type, not a normal-use case."""
    if isinstance(msg, CompressedImage):
        raw = np.frombuffer(bytes(msg.data), dtype=np.uint8)
        frame = cv2.imdecode(raw, cv2.IMREAD_COLOR)
        if frame is None:
            raise UnsupportedImageError("could not decode CompressedImage data")
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


class RawFrameHolder:
    """A single-slot, mutex-guarded holder for one camera's newest *un*converted
    source message — the lazy half of "decode only what someone will see".
    `LatestFrameHolder` above holds a converted BGR frame, cheap to keep
    updating; this holds the raw message (an `Image`/`CompressedImage`, for
    the `kind: 'ros'` adapter — the only source that can retain a message
    without paying the imgmsg->BGR conversion, since the three threaded
    adapters must decode on their own thread just to know a frame arrived).

    `take()` — unlike `LatestFrameHolder.get()` — clears the slot: the
    frame is converted at most once, by the one path that consumes it.
    Today that is exactly one caller, `RosRuntime._next_snapshot`'s dueness
    walk, and nothing else in the package calls `take()` at all. Written
    from the ROS subscription callback (the executor thread); `take()` runs
    on that same thread (via `_submit_async`), so the lock here is
    defensive rather than load-bearing for that one reader — it matters if
    a second reader is ever added.

    **What a live start does not do, since the obvious guess is wrong.**
    Starting a live stream does *not* take the raw frame: `_on_source_frame`
    starts eagerly converting into `entry.latest` the moment a stream is
    live, and the publisher reads `entry.latest`. So the first live frames
    a viewer sees come from whatever `latest` already held when the stream
    started — up to one capture interval stale for a camera that was idle,
    and `None` (nothing sent) for one that has never captured — until the
    first eager conversion lands. That is the join-window cost of
    converting lazily, named here rather than left to be rediscovered."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frame: Optional[RawFrame] = None

    def set(self, msg: object, timestamp_ms: int) -> None:
        with self._lock:
            self._frame = RawFrame(msg=msg, timestamp_ms=timestamp_ms)

    def take(self) -> Optional[Tuple[object, int]]:
        with self._lock:
            frame = self._frame
            self._frame = None
        if frame is None:
            return None
        return frame.msg, frame.timestamp_ms
