# SPDX-License-Identifier: Apache-2.0
"""Pixel-level camera conversion: image message to BGR, resize, JPEG
encode, and the single-slot latest-frame handoff snapshot and live both
read from. No rclpy node or executor needed — as free of ROS runtime as
sampling.py, just with image messages."""
import numpy as np
import pytest
from cv_bridge import CvBridge
from sensor_msgs.msg import BatteryState, CompressedImage, Image

from fleetless_bridge.camera import (
    LatestFrameHolder,
    RawFrameHolder,
    UnsupportedImageError,
    encode_jpeg,
    encode_snapshot_jpeg,
    resize_if_needed,
    to_bgr,
)

_bridge = CvBridge()


def _solid_frame(height=4, width=6, color=(10, 20, 30)):
    """A tiny, deterministic BGR array — real color, not zeros, so a
    channel drop or reorder would show."""
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    frame[:, :] = color
    return frame


# --- to_bgr -------------------------------------------------------------------


def test_to_bgr_passes_a_raw_image_message_through_unchanged():
    frame = _solid_frame()
    msg = _bridge.cv2_to_imgmsg(frame, encoding="bgr8")
    result = to_bgr(msg)
    assert result.shape == frame.shape
    assert np.array_equal(result, frame)


def test_to_bgr_decodes_a_compressed_image_message():
    frame = _solid_frame(height=20, width=30)
    import cv2

    ok, buf = cv2.imencode(".jpg", frame)
    assert ok
    msg = CompressedImage()
    msg.format = "jpeg"
    msg.data = buf.tobytes()
    result = to_bgr(msg)
    # JPEG is lossy — same shape, not necessarily bit-identical.
    assert result.shape == frame.shape


def test_to_bgr_rejects_an_unrecognised_message_type():
    with pytest.raises(UnsupportedImageError):
        to_bgr(BatteryState())


def test_to_bgr_rejects_undecodable_compressed_data():
    msg = CompressedImage()
    msg.format = "jpeg"
    msg.data = b"not actually a jpeg"
    with pytest.raises(UnsupportedImageError):
        to_bgr(msg)


# --- resize_if_needed -----------------------------------------------------------


def test_resize_if_needed_leaves_a_matching_frame_untouched():
    frame = _solid_frame(height=4, width=6)
    result = resize_if_needed(frame, width=6, height=4)
    assert result is frame


def test_resize_if_needed_resizes_to_the_configured_dimensions():
    frame = _solid_frame(height=10, width=20)
    result = resize_if_needed(frame, width=8, height=5)
    assert result.shape == (5, 8, 3)


# --- encode_jpeg ----------------------------------------------------------------


def test_encode_jpeg_produces_bytes_that_decode_back_to_the_same_shape():
    import cv2

    frame = _solid_frame(height=12, width=16)
    data = encode_jpeg(frame)
    assert isinstance(data, bytes)
    assert len(data) > 0
    decoded = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_COLOR)
    assert decoded.shape == frame.shape


# --- encode_snapshot_jpeg — must never exceed max_bytes ----------------------


def _noisy_frame(height, width, seed=0):
    """Noise, not a solid color: a solid frame compresses to almost
    nothing regardless of quality, proving nothing about the backoff
    ladder. Noise is close to worst-case for JPEG — the same reason the
    ceiling was checked against real (noisy) camera content, not a
    synthetic gradient."""
    rng = np.random.default_rng(seed)
    return rng.integers(0, 256, size=(height, width, 3), dtype=np.uint8)


def test_encode_snapshot_jpeg_returns_bytes_under_the_limit_for_a_normal_frame():
    frame = _solid_frame(height=48, width=64)
    result = encode_snapshot_jpeg(frame, max_bytes=1_572_864)
    assert result is not None
    data, width, height = result
    assert len(data) <= 1_572_864


def test_encode_snapshot_jpeg_prefers_full_resolution_when_it_already_fits():
    import cv2

    frame = _solid_frame(height=48, width=64)
    data, width, height = encode_snapshot_jpeg(frame, max_bytes=1_572_864)
    decoded = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_COLOR)
    # Not needlessly downscaled just because it *could* back off further.
    assert decoded.shape[:2] == frame.shape[:2]
    assert (width, height) == (frame.shape[1], frame.shape[0])


def test_encode_snapshot_jpeg_never_exceeds_max_bytes_even_under_worst_case_content():
    # Noisy and large enough that full resolution, even at the lowest
    # quality this module tries, misses a small ceiling — forces the
    # downscale ladder to actually engage.
    frame = _noisy_frame(400, 400)
    data, width, height = encode_snapshot_jpeg(frame, max_bytes=20_000)
    assert len(data) <= 20_000


def test_encode_snapshot_jpeg_downscales_when_quality_alone_does_not_fit():
    import cv2

    # Lowest quality on 400x400 noise is ~18KB (cv2 4.5.4) — a 10KB
    # ceiling makes even that not fit, forcing a downscale.
    frame = _noisy_frame(400, 400)
    data, width, height = encode_snapshot_jpeg(frame, max_bytes=10_000)
    decoded = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_COLOR)
    assert decoded.shape[:2] != frame.shape[:2]  # backed off resolution, not just quality
    # Header dimensions must match what was encoded, not what was
    # captured — a mismatch there is its own small dishonesty.
    assert (width, height) == (decoded.shape[1], decoded.shape[0])


def test_encode_snapshot_jpeg_returns_none_when_nothing_fits():
    # Nothing encodes into 10 bytes, not even a solid minimum-size frame.
    # The caller must treat None as "drop this frame" — not send
    # something that would close the socket.
    frame = _noisy_frame(400, 400)
    data = encode_snapshot_jpeg(frame, max_bytes=10)
    assert data is None


# --- LatestFrameHolder ------------------------------------------------------------


def test_latest_frame_holder_starts_empty():
    holder = LatestFrameHolder()
    assert holder.get() is None


def test_latest_frame_holder_returns_the_most_recently_set_frame():
    holder = LatestFrameHolder()
    holder.set(_solid_frame(color=(1, 1, 1)), timestamp_ms=100)
    holder.set(_solid_frame(color=(2, 2, 2)), timestamp_ms=200)
    latest = holder.get()
    assert latest.timestamp_ms == 200
    assert np.array_equal(latest.bgr, _solid_frame(color=(2, 2, 2)))


# --- RawFrameHolder ---------------------------------------------------------------


def test_raw_frame_holder_starts_empty():
    holder = RawFrameHolder()
    assert holder.take() is None


def test_raw_frame_holder_take_returns_the_set_message_and_clears_the_slot():
    holder = RawFrameHolder()
    msg = object()
    holder.set(msg, timestamp_ms=123)
    assert holder.take() == (msg, 123)
    # `take()` consumes — second call finds nothing, the "converts at
    # most once" promise `_next_snapshot`'s lazy conversion relies on.
    assert holder.take() is None


def test_raw_frame_holder_set_overwrites_a_not_yet_taken_frame():
    holder = RawFrameHolder()
    holder.set(object(), timestamp_ms=1)
    newer = object()
    holder.set(newer, timestamp_ms=2)
    assert holder.take() == (newer, 2)
