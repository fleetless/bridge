# SPDX-License-Identifier: Apache-2.0
"""Pixel-level camera conversion: image message to BGR, resize, JPEG
encode, and the single-slot latest-frame handoff snapshot and live both
read from. No rclpy node or executor needed — as free of ROS runtime as
sampling.py, just with image messages."""
import time
import numpy as np
import pytest
from cv_bridge import CvBridge
from sensor_msgs.msg import BatteryState, CompressedImage, Image

from fleetless_bridge.camera import (
    EncodedFrameHolder,
    LatestFrameHolder,
    RawFrameHolder,
    UnsupportedImageError,
    encode_jpeg,
    encode_snapshot_jpeg,
    jpeg_size,
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


def test_raw_frame_holder_remembers_when_the_message_arrived():
    """`take_frame` hands over the arrival instant with the message, on the
    monotonic clock: whoever converts the message later can tell how long it
    was held without comparing two different clocks."""
    holder = RawFrameHolder()
    before = time.monotonic()
    holder.set(object(), 123)
    after = time.monotonic()
    frame = holder.take_frame()
    assert frame.timestamp_ms == 123
    assert before <= frame.arrived_s <= after
    assert holder.take_frame() is None


# --- to_bgr, from the bytes of a JPEG -----------------------------------------


def test_to_bgr_decodes_the_bytes_of_a_jpeg():
    """An MJPEG part is held as bytes, not as a message."""
    frame = _solid_frame(height=20, width=30)
    data = encode_jpeg(frame)
    for held in (data, bytearray(data), memoryview(data)):
        assert to_bgr(held).shape == frame.shape


def test_to_bgr_rejects_bytes_that_are_not_an_image():
    with pytest.raises(UnsupportedImageError):
        to_bgr(b"--boundary\r\nContent-Type: image/jpeg\r\n\r\n")
    with pytest.raises(UnsupportedImageError):
        to_bgr(b"")


# --- jpeg_size ----------------------------------------------------------------
#
# Read from the header, never decoded. Only what every browser shows gets a
# size back: baseline or progressive, 8 bits, one or three components.


def _jpeg(width=64, height=48, *, progressive=False, grey=False, quality=80):
    import cv2

    rng = np.random.default_rng(894)
    shape = (height, width) if grey else (height, width, 3)
    picture = rng.integers(0, 255, shape, dtype=np.uint8)
    params = [cv2.IMWRITE_JPEG_QUALITY, quality]
    if progressive:
        params += [cv2.IMWRITE_JPEG_PROGRESSIVE, 1]
    ok, buf = cv2.imencode(".jpg", picture, params)
    assert ok
    return buf.tobytes()


def _segment(marker, payload):
    import struct

    return b"\xff" + bytes([marker]) + struct.pack(">H", len(payload) + 2) + payload


def _headers_only(*, sof=0xC0, precision=8, components=3, width=64, height=48):
    """The smallest thing with a JPEG's headers: a frame header, a scan
    header, two bytes of scan and the end marker. No decoder would show it,
    and the reader does not decode: what it says depends on the headers
    alone, so each field can be varied by itself."""
    import struct

    frame_header = (
        bytes([precision]) + struct.pack(">HH", height, width) + bytes([components])
        + b"".join(bytes([index + 1, 0x11, 0]) for index in range(components))
    )
    scan_header = (
        bytes([components]) + b"".join(bytes([index + 1, 0]) for index in range(components))
        + bytes([0, 63, 0])
    )
    return (
        b"\xff\xd8" + _segment(sof, frame_header) + _segment(0xDA, scan_header)
        + b"\x12\x34" + b"\xff\xd9"
    )


@pytest.mark.parametrize("progressive", [False, True])
@pytest.mark.parametrize("grey", [False, True])
@pytest.mark.parametrize("size", [(64, 48), (1024, 768), (768, 1024), (33, 17), (1, 1)])
def test_jpeg_size_reads_baseline_and_progressive_files(size, grey, progressive):
    width, height = size
    data = _jpeg(width, height, progressive=progressive, grey=grey)
    # The file is what it is meant to be: the frame header says so.
    assert (b"\xff\xc2" in data) == progressive
    assert jpeg_size(data) == (width, height)


def test_jpeg_size_reads_whatever_holds_the_bytes():
    """A ROS message carries them as `array('B')`; an MJPEG part is `bytes`."""
    import array

    data = _jpeg(40, 30)
    for held in (
        data, bytearray(data), memoryview(data), array.array("B", data),
        np.frombuffer(data, np.uint8),
    ):
        assert jpeg_size(held) == (40, 30)


def test_jpeg_size_does_not_decode():
    """A header chain with two bytes of scan: nothing could decode it."""
    import cv2

    data = _headers_only(width=640, height=480)
    assert cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR) is None
    assert jpeg_size(data) == (640, 480)
    assert jpeg_size(_headers_only(sof=0xC2, components=1, width=7, height=9)) == (7, 9)


def test_jpeg_size_skips_a_thumbnail_inside_an_application_segment():
    """An EXIF block may hold a whole small JPEG, with its own start, frame
    header and end. It sits inside a segment and is skipped by its length."""
    thumbnail = _jpeg(16, 12)
    data = _jpeg(320, 240)
    with_thumbnail = data[:2] + _segment(0xE1, b"Exif\x00\x00" + thumbnail) + data[2:]
    assert jpeg_size(with_thumbnail) == (320, 240)


def test_jpeg_size_accepts_fill_bytes_before_a_marker():
    data = _jpeg(64, 48)
    assert jpeg_size(data[:2] + b"\xff\xff\xff" + data[2:]) == (64, 48)


@pytest.mark.parametrize(
    "what, data",
    [
        ("four components, as in CMYK", _headers_only(components=4)),
        ("two components", _headers_only(components=2)),
        ("12 bits a sample", _headers_only(sof=0xC1, precision=12)),
        ("12 bits claimed in a baseline header", _headers_only(precision=12)),
        ("extended sequential", _headers_only(sof=0xC1)),
        ("lossless", _headers_only(sof=0xC3)),
        ("arithmetic coding", _headers_only(sof=0xC9)),
        ("progressive with arithmetic coding", _headers_only(sof=0xCA)),
        ("a width of zero", _headers_only(width=0)),
        ("a height of zero", _headers_only(height=0)),
    ],
)
def test_jpeg_size_refuses_what_not_every_browser_shows(what, data):
    # The same bytes with the plain values are accepted, so the refusal is
    # about the one field that differs.
    assert jpeg_size(_headers_only()) == (64, 48)
    assert jpeg_size(data) is None, what


@pytest.mark.parametrize(
    "marker, what",
    [
        (0xDE, "DHP, a hierarchical file"),
        (0xDF, "EXP, a hierarchical file"),
        (0xF0, "the first JPEG-extension marker"),
        (0xFD, "the last JPEG-extension marker"),
        (0x02, "a reserved code"),
        (0xBF, "the last reserved code"),
        (0xC8, "the reserved code among the frame headers"),
        (0xDC, "DNL, the number of lines given after the scan"),
    ],
)
def test_jpeg_size_refuses_a_segment_no_decoder_accepts_before_the_scan(marker, what):
    """A real file with one extra segment. libjpeg, which decodes for OpenCV
    and for the browsers, gives up on these, so a file that carries one is
    a file nobody shows, and it must not go out as a snapshot unlooked at.
    Stepping over it by its length, as over an application segment, is what
    this refuses."""
    data = _jpeg(64, 48)
    assert jpeg_size(data) == (64, 48)
    assert jpeg_size(data[:2] + _segment(marker, b"\x00\x01\x02\x03") + data[2:]) is None, what


@pytest.mark.parametrize("marker", [0xE0, 0xE5, 0xEF, 0xFE, 0xDD])
def test_jpeg_size_steps_over_the_segments_a_decoder_accepts(marker):
    """The control: application data, a comment and a restart interval are
    what cameras really put there, and the file is still the picture."""
    import cv2

    data = _jpeg(64, 48)
    payload = b"\x00\x00" if marker == 0xDD else b"made by a camera"
    with_segment = data[:2] + _segment(marker, payload) + data[2:]
    assert jpeg_size(with_segment) == (64, 48)
    assert cv2.imdecode(np.frombuffer(with_segment, np.uint8), cv2.IMREAD_COLOR) is not None


def test_jpeg_size_refuses_every_truncation_of_a_real_file():
    data = _jpeg(64, 48, quality=30)
    assert jpeg_size(data) == (64, 48)
    for length in range(len(data)):
        assert jpeg_size(data[:length]) is None, length


def test_jpeg_size_refuses_a_file_that_does_not_end_where_a_jpeg_ends():
    data = _jpeg(64, 48)
    assert jpeg_size(data + b"\x00") is None
    assert jpeg_size(data[:-2] + b"\xff\xd8") is None


def test_jpeg_size_refuses_what_is_not_a_jpeg():
    import cv2

    ok, png = cv2.imencode(".png", _solid_frame(height=20, width=30))
    assert ok
    for data in (
        png.tobytes(),
        b"",
        b"\xff",
        b"\xff\xd8",
        b"\xff\xd8\xff\xd9",
        b"GIF89a" + bytes(40),
        bytes(np.random.default_rng(1).integers(0, 255, 4096, dtype=np.uint8)),
        b"\xff\xd8" + bytes(64) + b"\xff\xd9",
    ):
        assert jpeg_size(data) is None
    for not_bytes in (
        None, 12, "\xff\xd8", [0xFF, 0xD8], np.zeros((4, 4), np.uint8), np.zeros(8, np.uint16),
    ):
        assert jpeg_size(not_bytes) is None


def test_jpeg_size_refuses_a_segment_that_runs_past_the_end():
    data = _jpeg(64, 48)
    # The first segment after the start marker, claiming to be longer than the file.
    assert jpeg_size(data[:4] + b"\xff\xff" + data[6:]) is None
    # And one whose length is smaller than its own two bytes.
    assert jpeg_size(data[:4] + b"\x00\x01" + data[6:]) is None


def test_jpeg_size_refuses_a_scan_without_a_frame_header_and_a_second_frame_header():
    import struct

    scan = _segment(0xDA, bytes([1, 1, 0, 0, 63, 0]))
    assert jpeg_size(b"\xff\xd8" + scan + b"\x12\xff\xd9") is None
    one = _headers_only()
    frame_header = one[2:one.index(b"\xff\xda")]
    assert jpeg_size(one[:2] + frame_header + one[2:]) is None
    # A frame header shorter than its component count says.
    short = bytes([8]) + struct.pack(">HH", 48, 64) + bytes([3]) + bytes([1, 0x11, 0])
    assert jpeg_size(b"\xff\xd8" + _segment(0xC0, short) + scan + b"\x12\xff\xd9") is None


# --- EncodedFrameHolder -------------------------------------------------------


def test_encoded_frame_holder_starts_empty():
    assert EncodedFrameHolder().get() is None


def test_encoded_frame_holder_keeps_the_newest_frame_and_does_not_hand_it_out_only_once():
    holder = EncodedFrameHolder()
    first, second = _jpeg(8, 8), _jpeg(16, 8)
    holder.set(first, 1_000)
    holder.set(second, 2_000)
    held = holder.get()
    # The same object, not a copy: keeping a frame costs a reference.
    assert held.data is second and held.timestamp_ms == 2_000
    assert holder.get() is held
