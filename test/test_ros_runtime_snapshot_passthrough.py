# SPDX-License-Identifier: Apache-2.0
"""Snapshot passthrough: a JPEG the source delivers in the configured size
goes out as the snapshot, byte for byte, and nothing is decoded or encoded
on the way.

Two sources deliver JPEG: a ROS topic of `sensor_msgs/msg/CompressedImage`
and an MJPEG stream. Both are driven for real here, the first over a real
subscription and the second through the real adapter with a response the
test feeds part by part. Reuses `test_ros_runtime.py`'s harness.

"Nothing is decoded" is counted, not assumed: `cv2.imdecode`, `cv2.imencode`,
`camera.to_bgr` and `camera.encode_snapshot_jpeg` are wrapped on their module
objects, still doing their work, and the passthrough tests assert a count of
zero while the tests of the other path assert that the same counters move.
"""
import asyncio
import queue
import threading
import time

import cv2
import numpy as np
import pytest
import rclpy
from conftest import wait_until
from helpers import by_slug
from rclpy.executors import MultiThreadedExecutor
from sensor_msgs.msg import CompressedImage

from fleetless_bridge import camera, camera_sources
from fleetless_bridge.protocol import CameraConfig, MjpegSource
from test_ros_runtime import (
    _camera_cfg,
    _fake_live_factory,
    _pull_snapshot,
    _split_snapshot_frame,
    _start_image_publisher,
    run,
)

COMPRESSED = "sensor_msgs/msg/CompressedImage"
WIDTH, HEIGHT = 64, 48


def _jpeg(width=WIDTH, height=HEIGHT, *, seed=1, quality=80, progressive=False, grey=False):
    """A JPEG of noise: no two seeds give the same bytes, and no quality
    makes it small."""
    shape = (height, width) if grey else (height, width, 3)
    picture = np.random.default_rng(seed).integers(0, 255, shape, dtype=np.uint8)
    params = [cv2.IMWRITE_JPEG_QUALITY, quality]
    if progressive:
        params += [cv2.IMWRITE_JPEG_PROGRESSIVE, 1]
    ok, buf = cv2.imencode(".jpg", picture, params)
    assert ok
    return buf.tobytes()


def _png(width=WIDTH, height=HEIGHT):
    picture = np.random.default_rng(7).integers(0, 255, (height, width, 3), dtype=np.uint8)
    ok, buf = cv2.imencode(".png", picture)
    assert ok
    return buf.tobytes()


class _Counters:
    """How often each of the four pieces of decode and encode work ran."""

    def __init__(self, monkeypatch):
        self.calls = {"imdecode": 0, "imencode": 0, "to_bgr": 0, "encode_snapshot_jpeg": 0}
        self._wrap(monkeypatch, cv2, "imdecode")
        self._wrap(monkeypatch, cv2, "imencode")
        self._wrap(monkeypatch, camera, "to_bgr")
        self._wrap(monkeypatch, camera, "encode_snapshot_jpeg")

    def _wrap(self, monkeypatch, module, name):
        original = getattr(module, name)

        def counted(*args, **kwargs):
            self.calls[name] += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(module, name, counted)

    @property
    def total(self):
        return sum(self.calls.values())


class _CompressedPublisher:
    """A real publisher of `CompressedImage` on its own node. `send(data)`
    publishes one message; nothing is sent on a timer, so a test knows which
    frame is the newest."""

    def __init__(self, topic="/image_raw/compressed"):
        self.topic = topic
        self._node = rclpy.create_node("test_jpeg_publisher_{}".format(id(self)))
        self._executor = MultiThreadedExecutor()
        self._executor.add_node(self._node)
        self._thread = threading.Thread(target=self._executor.spin, daemon=True)
        self._thread.start()
        self._pub = self._node.create_publisher(CompressedImage, topic, 10)

    def wait_for_subscriber(self):
        wait_until(lambda: self._pub.get_subscription_count() > 0, message="no subscriber on the topic")

    def send(self, data, format_="jpeg"):
        msg = CompressedImage()
        msg.format = format_
        msg.data = data
        self._pub.publish(msg)

    def stop(self):
        self._executor.shutdown()
        self._node.destroy_node()
        self._thread.join(timeout=5.0)


def _cfg(slug="front", **kwargs):
    kwargs.setdefault("topic", "/image_raw/compressed")
    kwargs.setdefault("type_", COMPRESSED)
    kwargs.setdefault("width", WIDTH)
    kwargs.setdefault("height", HEIGHT)
    return _camera_cfg(slug, **kwargs)


async def _holds(rt, slug, data, timeout=5.0):
    """Waits until the camera's encoded slot holds exactly `data`."""
    deadline = time.monotonic() + timeout
    while True:
        held = rt._cameras[slug].encoded.get()
        if held is not None and bytes(held.data) == data:
            return held
        if time.monotonic() > deadline:
            raise AssertionError("the frame never reached the camera's encoded slot")
        await asyncio.sleep(0.01)


async def _deliver(rt, pub, data, *, slug="front", format_="jpeg"):
    pub.send(data, format_)
    return await _holds(rt, slug, data)


def _with_publisher(body, **runtime_kwargs):
    """`run`, with a `CompressedImage` publisher that is torn down again."""

    async def wrapped(rt):
        pub = _CompressedPublisher()
        try:
            return await body(rt, pub)
        finally:
            pub.stop()

    return run(wrapped, **runtime_kwargs)


# --- the passthrough itself ----------------------------------------------------


@pytest.mark.parametrize("progressive, grey", [(False, False), (True, False), (False, True)])
def test_a_jpeg_in_the_configured_size_goes_out_byte_for_byte(monkeypatch, progressive, grey):
    source = _jpeg(progressive=progressive, grey=grey)
    counters = None

    async def body(rt, pub):
        nonlocal counters
        await rt.apply_cameras(by_slug([_cfg()]))
        pub.wait_for_subscriber()
        before_ms = int(time.time() * 1000)
        counters = _Counters(monkeypatch)
        held = await _deliver(rt, pub, source)
        wire = await _pull_snapshot(rt)
        return wire, held, before_ms, int(time.time() * 1000)

    wire, held, before_ms, after_ms = _with_publisher(body)
    header, image_bytes = _split_snapshot_frame(wire)
    assert image_bytes == source
    assert header["slug"] == "front"
    assert header["mime"] == "image/jpeg"
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    # The frame's own capture time: taken when the message arrived.
    assert header["timestamp_ms"] == held.timestamp_ms
    assert before_ms <= header["timestamp_ms"] <= after_ms
    # Asserted, not assumed: no decode and no encode anywhere on the way.
    assert counters.calls == {"imdecode": 0, "imencode": 0, "to_bgr": 0, "encode_snapshot_jpeg": 0}


def test_the_newest_frame_is_the_one_passed_through(monkeypatch):
    first, second = _jpeg(seed=1), _jpeg(seed=2)

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1)]))
        pub.wait_for_subscriber()
        counters = _Counters(monkeypatch)
        await _deliver(rt, pub, first)
        one = await _pull_snapshot(rt)
        # Served: not due again until the interval has passed.
        not_due = await rt.next_snapshot(10_000_000)
        await asyncio.sleep(0.05)
        held = await _deliver(rt, pub, second)
        two = await _pull_snapshot(rt)
        return one, not_due, two, held, counters.total

    one, not_due, two, held, decoded = _with_publisher(body)
    header_one, image_one = _split_snapshot_frame(one)
    header_two, image_two = _split_snapshot_frame(two)
    assert image_one == first and image_two == second
    assert not_due is None  # `last_snapshot_at` was armed by the passthrough
    assert header_two["timestamp_ms"] == held.timestamp_ms > header_one["timestamp_ms"]
    assert decoded == 0


def test_the_format_string_is_not_trusted_and_the_bytes_decide(monkeypatch):
    """A JPEG labelled `png` is a JPEG. A PNG labelled `jpeg` is a PNG and
    takes the decoding path, which turns it into the JPEG a snapshot is."""
    source, png = _jpeg(), _png()

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1)]))
        pub.wait_for_subscriber()
        counters = _Counters(monkeypatch)
        await _deliver(rt, pub, source, format_="png")
        mislabelled_jpeg = await _pull_snapshot(rt)
        after_first = dict(counters.calls)
        await _deliver(rt, pub, png, format_="jpeg")
        mislabelled_png = await _pull_snapshot(rt)
        return mislabelled_jpeg, after_first, mislabelled_png, dict(counters.calls)

    mislabelled_jpeg, after_first, mislabelled_png, after_second = _with_publisher(body)
    assert _split_snapshot_frame(mislabelled_jpeg)[1] == source
    assert sum(after_first.values()) == 0
    header, image_bytes = _split_snapshot_frame(mislabelled_png)
    assert image_bytes != png and image_bytes[:2] == b"\xff\xd8"
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    assert after_second["to_bgr"] == 1 and after_second["encode_snapshot_jpeg"] == 1


# --- when it does not apply ------------------------------------------------------


def test_a_jpeg_of_another_size_takes_the_decoding_path(monkeypatch):
    source = _jpeg(32, 24)

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg()]))
        pub.wait_for_subscriber()
        counters = _Counters(monkeypatch)
        await _deliver(rt, pub, source)
        return await _pull_snapshot(rt), dict(counters.calls)

    wire, calls = _with_publisher(body)
    header, image_bytes = _split_snapshot_frame(wire)
    assert image_bytes != source
    # Today's path: decoded once, resized to the configured size, encoded.
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    assert calls["to_bgr"] == 1 and calls["encode_snapshot_jpeg"] == 1


def test_a_jpeg_over_the_budget_of_the_pull_is_shrunk_and_one_that_fits_exactly_is_not(monkeypatch):
    source = _jpeg(quality=95)
    assert len(source) > 2000

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1)]))
        pub.wait_for_subscriber()
        counters = _Counters(monkeypatch)
        await _deliver(rt, pub, source)
        too_small = await _pull_snapshot(rt, max_bytes=len(source) - 1)
        shrunk_calls = dict(counters.calls)
        # A frame that did not fit one pull is not held against the next.
        exact = await _pull_snapshot(rt, max_bytes=len(source))
        return too_small, shrunk_calls, exact, dict(counters.calls)

    too_small, shrunk_calls, exact, calls = _with_publisher(body)
    _, shrunk = _split_snapshot_frame(too_small)
    assert shrunk != source and len(shrunk) <= len(source) - 1
    assert shrunk_calls["to_bgr"] == 1 and shrunk_calls["encode_snapshot_jpeg"] == 1
    assert _split_snapshot_frame(exact)[1] == source
    assert calls == shrunk_calls  # the second pull decoded and encoded nothing


def test_a_jpeg_over_the_wire_ceiling_is_shrunk_whatever_the_pull_allows(monkeypatch):
    source = _jpeg(quality=95)

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg()]))
        pub.wait_for_subscriber()
        counters = _Counters(monkeypatch)
        await _deliver(rt, pub, source)
        return await _pull_snapshot(rt), dict(counters.calls)

    wire, calls = _with_publisher(body, snapshot_max_bytes=len(source) - 1)
    _, image_bytes = _split_snapshot_frame(wire)
    assert image_bytes != source and len(image_bytes) <= len(source) - 1
    assert calls["encode_snapshot_jpeg"] == 1


def test_a_png_takes_the_decoding_path():
    png = _png()

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg()]))
        pub.wait_for_subscriber()
        await _deliver(rt, pub, png, format_="png")
        return await _pull_snapshot(rt)

    header, image_bytes = _split_snapshot_frame(_with_publisher(body))
    assert image_bytes[:2] == b"\xff\xd8" and image_bytes != png
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)


def test_bytes_that_only_begin_and_end_like_a_jpeg_are_not_sent():
    """A header the reader cannot follow is today's path, and today's path
    cannot decode this either: no snapshot, and the next good frame is one."""
    good = _jpeg()
    broken = b"\xff\xd8" + bytes(400) + b"\xff\xd9"

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1)]))
        pub.wait_for_subscriber()
        await _deliver(rt, pub, broken)
        nothing = await rt.next_snapshot(10_000_000)
        await _deliver(rt, pub, good)
        return nothing, await _pull_snapshot(rt)

    nothing, wire = _with_publisher(body)
    assert nothing is None
    assert _split_snapshot_frame(wire)[1] == good


def test_a_camera_of_raw_images_keeps_nothing_encoded_and_is_served_as_before(monkeypatch):
    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=WIDTH, height=HEIGHT, hz=30)
        try:
            await rt.apply_cameras(by_slug([_camera_cfg("front", width=WIDTH, height=HEIGHT)]))
            counters = _Counters(monkeypatch)
            wire = await _pull_snapshot(rt)
            return wire, rt._cameras["front"].encoded.get(), dict(counters.calls)
        finally:
            stop_pub()

    wire, encoded, calls = run(body)
    assert encoded is None
    assert _split_snapshot_frame(wire)[1][:2] == b"\xff\xd8"
    assert calls["to_bgr"] == 1 and calls["encode_snapshot_jpeg"] == 1


# --- beside live video -------------------------------------------------------------


def test_passthrough_holds_while_a_live_publisher_runs(monkeypatch):
    """The encoded frame is kept on every message, watched or not. With a
    stream live every frame is also converted for the stream; the snapshot
    is still the source's own bytes."""
    first, second = _jpeg(seed=1), _jpeg(seed=2)

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1, fps=30)]))
        pub.wait_for_subscriber()
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        assert "front" in rt._live_publishers
        counters = _Counters(monkeypatch)
        await _deliver(rt, pub, first)
        # Live converts it: the frame reaches the holder the stream reads.
        wait_until(lambda: rt._cameras["front"].latest.get() is not None)
        converted_for_live = counters.calls["to_bgr"]
        wire = await _pull_snapshot(rt)
        after_snapshot = dict(counters.calls)
        await asyncio.sleep(1.05)
        held = await _deliver(rt, pub, second)
        again = await _pull_snapshot(rt)
        return wire, converted_for_live, after_snapshot, again, held

    wire, converted_for_live, after_snapshot, again, held = _with_publisher(
        body, live_publisher_factory=_fake_live_factory()
    )
    assert converted_for_live == 1
    assert _split_snapshot_frame(wire)[1] == first
    # The snapshot added no decode of its own and no encode at all.
    assert after_snapshot["to_bgr"] == 1
    assert after_snapshot["encode_snapshot_jpeg"] == 0 and after_snapshot["imencode"] == 0
    header, image_bytes = _split_snapshot_frame(again)
    assert image_bytes == second and header["timestamp_ms"] == held.timestamp_ms


def test_a_changed_size_stops_passthrough_and_changing_it_back_resumes_it(monkeypatch):
    source = _jpeg()

    async def body(rt, pub):
        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1)]))
        pub.wait_for_subscriber()
        adapter = rt._cameras["front"].adapter
        await _deliver(rt, pub, source)
        matching = await _pull_snapshot(rt)

        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1, width=32, height=24)]))
        assert rt._cameras["front"].adapter is adapter  # same source, same subscription
        counters = _Counters(monkeypatch)
        resized = await _pull_snapshot(rt)
        resized_calls = dict(counters.calls)

        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=1)]))
        back = await _pull_snapshot(rt)
        return matching, resized, resized_calls, back, dict(counters.calls)

    matching, resized, resized_calls, back, calls = _with_publisher(body)
    assert _split_snapshot_frame(matching)[1] == source
    header, image_bytes = _split_snapshot_frame(resized)
    assert image_bytes != source and (header["width"], header["height"]) == (32, 24)
    assert resized_calls["encode_snapshot_jpeg"] == 1
    assert _split_snapshot_frame(back)[1] == source
    assert calls["encode_snapshot_jpeg"] == 1  # nothing more was encoded for it


# --- MJPEG: the same, and decoded only on demand ---------------------------------


class _Stream:
    """An MJPEG response the test feeds part by part. `read` blocks until
    there is something to give, as a socket does."""

    def __init__(self):
        self._parts = queue.Queue()
        self.closed = False

    def send(self, jpeg):
        self._parts.put(b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + jpeg + b"\r\n")

    def read(self, _n):
        while not self.closed:
            try:
                return self._parts.get(timeout=0.02)
            except queue.Empty:
                continue
        return b""

    def close(self):
        self.closed = True


def _mjpeg_cfg(slug="front", *, width=WIDTH, height=HEIGHT, snapshot_interval_seconds=1, fps=30):
    return CameraConfig(
        slug=slug,
        source=MjpegSource(url="http://192.0.2.1/stream", credentials=None),
        width=width, height=height, fps=fps, bitrate_kbps=500,
        snapshot_interval_seconds=snapshot_interval_seconds,
    )


def _with_stream(body, monkeypatch, **runtime_kwargs):
    stream = _Stream()
    monkeypatch.setattr(camera_sources, "_default_mjpeg_opener", lambda request: stream)

    async def wrapped(rt):
        try:
            return await body(rt, stream)
        finally:
            stream.close()

    return run(wrapped, **runtime_kwargs)


async def _deliver_part(rt, stream, data, slug="front"):
    stream.send(data)
    return await _holds(rt, slug, data)


def test_an_mjpeg_stream_nobody_watches_is_not_decoded(monkeypatch):
    parts = [_jpeg(seed=seed) for seed in range(1, 6)]

    async def body(rt, stream):
        await rt.apply_cameras(by_slug([_mjpeg_cfg()]))
        counters = _Counters(monkeypatch)
        for part in parts:
            await _deliver_part(rt, stream, part)
        entry = rt._cameras["front"]
        return dict(counters.calls), entry.latest.get(), entry.last_source_error

    calls, latest, error = _with_stream(body, monkeypatch)
    # Five frames arrived and are known to have arrived, and none was decoded.
    assert sum(calls.values()) == 0
    assert latest is None and error is None


def test_a_matching_mjpeg_stream_s_snapshot_is_passed_through(monkeypatch):
    first, second = _jpeg(seed=1), _jpeg(seed=2)

    async def body(rt, stream):
        await rt.apply_cameras(by_slug([_mjpeg_cfg()]))
        counters = _Counters(monkeypatch)
        await _deliver_part(rt, stream, first)
        held = await _deliver_part(rt, stream, second)
        return await _pull_snapshot(rt), held, dict(counters.calls)

    wire, held, calls = _with_stream(body, monkeypatch)
    header, image_bytes = _split_snapshot_frame(wire)
    assert image_bytes == second
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    assert header["timestamp_ms"] == held.timestamp_ms
    assert sum(calls.values()) == 0


def test_an_mjpeg_stream_of_another_size_is_decoded_once_per_due_snapshot(monkeypatch):
    parts = [_jpeg(32, 24, seed=seed) for seed in range(1, 5)]

    async def body(rt, stream):
        await rt.apply_cameras(by_slug([_mjpeg_cfg()]))
        counters = _Counters(monkeypatch)
        for part in parts:
            await _deliver_part(rt, stream, part)
        before = dict(counters.calls)
        wire = await _pull_snapshot(rt)
        return before, wire, dict(counters.calls)

    before, wire, calls = _with_stream(body, monkeypatch)
    assert sum(before.values()) == 0  # four parts, no snapshot due yet, no decode
    header, image_bytes = _split_snapshot_frame(wire)
    assert image_bytes not in parts
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    # The newest part, once: not the four that arrived.
    assert calls["to_bgr"] == 1 and calls["imdecode"] == 1
    assert calls["encode_snapshot_jpeg"] == 1


def test_a_live_start_converts_the_newest_part_and_decoding_follows_the_stream(monkeypatch):
    held_back, first_live = _jpeg(seed=1), _jpeg(seed=2)

    async def body(rt, stream):
        await rt.apply_cameras(by_slug([_mjpeg_cfg()]))
        counters = _Counters(monkeypatch)
        waiting = await _deliver_part(rt, stream, held_back)
        assert counters.total == 0

        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        # The part that was held is the first picture of the stream.
        wait_until(lambda: rt._cameras["front"].latest.get() is not None)
        first = rt._cameras["front"].latest.get()
        at_start = dict(counters.calls)

        # From here the adapter decodes every part as it arrives. Not within
        # the camera's own frame interval of the first, which the rate limit
        # would drop.
        await asyncio.sleep(0.1)
        arriving = await _deliver_part(rt, stream, first_live)
        wait_until(lambda: rt._cameras["front"].latest.get().timestamp_ms == arriving.timestamp_ms)
        while_live = dict(counters.calls)

        await rt.stop_live("front", request_id="req-2")
        for seed in (3, 4, 5):
            await _deliver_part(rt, stream, _jpeg(seed=seed))
        return first, waiting, at_start, while_live, dict(counters.calls)

    first, waiting, at_start, while_live, after_stop = _with_stream(
        body, monkeypatch, live_publisher_factory=_fake_live_factory()
    )
    assert first.timestamp_ms == waiting.timestamp_ms
    assert first.bgr.shape == (HEIGHT, WIDTH, 3)
    assert at_start["imdecode"] == 1
    assert while_live["imdecode"] == 2
    # And once nobody watches, the decoding stops again.
    assert after_stop["imdecode"] == 2


# --- a camera that is never converted is still a camera that works ----------------


def test_a_camera_whose_frames_are_only_passed_through_is_restated_as_working(monkeypatch):
    """`report_current_camera_health` says once per connection what is known
    about each camera. The converted frame alone would not do for that: a
    camera in the configured size never has one, and a camera that works
    would be the one nothing is said about. The encoded frame counts too."""
    source = _jpeg()

    async def body(rt, pub):
        from test_ros_runtime import _drain_camera_states

        await rt.apply_cameras(by_slug([_cfg()]))
        pub.wait_for_subscriber()
        counters = _Counters(monkeypatch)
        held = await _deliver(rt, pub, source)
        await _pull_snapshot(rt)
        assert rt._cameras["front"].latest.get() is None and counters.total == 0
        await _drain_camera_states(rt)

        await rt.report_current_camera_health()
        return await _drain_camera_states(rt), held

    states, held = _with_publisher(body)
    assert [(s.slug, s.publishing, s.error, s.cause) for s in states] == [
        ("front", False, None, "source")
    ]
    # Dated to the frame it knows the camera by, not to the restatement.
    assert states[0].observed_at_ms == held.timestamp_ms


def test_an_mjpeg_camera_nobody_watches_is_restated_as_working(monkeypatch):
    part = _jpeg(32, 24)  # not even in the configured size: it arrived, that is the point

    async def body(rt, stream):
        from test_ros_runtime import _drain_camera_states

        await rt.apply_cameras(by_slug([_mjpeg_cfg(snapshot_interval_seconds=3600)]))
        held = await _deliver_part(rt, stream, part)
        assert rt._cameras["front"].latest.get() is None
        await _drain_camera_states(rt)
        await rt.report_current_camera_health()
        return await _drain_camera_states(rt), held

    states, held = _with_stream(body, monkeypatch)
    assert [(s.slug, s.error) for s in states] == [("front", None)]
    assert states[0].observed_at_ms == held.timestamp_ms


def test_the_restatement_is_dated_to_the_newer_of_the_two_frames(monkeypatch):
    """A converted frame and an encoded one: whichever the camera sent last."""
    first, second = _jpeg(32, 24, seed=1), _jpeg(32, 24, seed=2)

    async def body(rt, pub):
        from test_ros_runtime import _drain_camera_states

        await rt.apply_cameras(by_slug([_cfg(snapshot_interval_seconds=3600)]))
        pub.wait_for_subscriber()
        await _deliver(rt, pub, first)
        await _pull_snapshot(rt)  # another size: converted, so `latest` holds the first
        converted = rt._cameras["front"].latest.get()
        await asyncio.sleep(0.02)
        held = await _deliver(rt, pub, second)
        await _drain_camera_states(rt)
        await rt.report_current_camera_health()
        return await _drain_camera_states(rt), converted, held

    states, converted, held = _with_publisher(body)
    assert converted is not None and held.timestamp_ms > converted.timestamp_ms
    assert [s.observed_at_ms for s in states] == [held.timestamp_ms]
