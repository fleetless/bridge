# SPDX-License-Identifier: Apache-2.0
"""H.264 passthrough where it meets the runtime: which ROS messages go to the
passthrough stream and which stay pictures, what a refused stream does to the
camera and to a stream that is running, snapshots from the newest keyframe,
and low-bandwidth mode.

The stream object and the bitstream reader have their own files
(`test_passthrough.py`, `test_h264.py`), the RTSP adapter too
(`test_camera_sources_passthrough.py`). The track and the encoder are
`test_live_passthrough.py`'s. Here the publisher is a fake that writes down
what it was handed, as in `test_ros_runtime.py`, whose harness this reuses.

Two ways a unit reaches the runtime in these tests. Over a real subscription
(`_start_compressed_image_publisher`) where the subject is the subscription
itself -- that a `CompressedImage` with format `h264` is routed, and one with
JPEG is not. Handed to `_on_camera_video` on the executor thread
(`_feed`) everywhere else: that is the call the subscription callback makes,
and it puts the test in charge of which unit arrives when.
"""
import asyncio
import pathlib
import threading
import time
from unittest import mock

import cv2
import numpy as np
import pytest
import rclpy
from rclpy.executors import MultiThreadedExecutor
from sensor_msgs.msg import CompressedImage

import h264_streams as streams
from helpers import by_slug
from test_ros_runtime import (
    _FakeLivePublisher,
    _await_camera_state,
    _camera_cfg,
    _drain_camera_states,
    _pull_snapshot,
    _split_snapshot_frame,
    run,
)

from fleetless_bridge import camera, camera_sources, live, live_passthrough, passthrough, ros_runtime
from fleetless_bridge.link_mode import LowBandwidthSettings
from fleetless_bridge.protocol import (
    CAMERA_STATE_CAUSE_COMMAND,
    CAMERA_STATE_CAUSE_CONFIG_CHANGE,
    CAMERA_STATE_CAUSE_LIVE_LOST,
    CAMERA_STATE_CAUSE_SOURCE,
    LOW_BANDWIDTH_DEFAULTS,
    CameraConfig,
    RtspSource,
)
from fleetless_bridge.ros_runtime import (
    COMPRESSED_IMAGE_TYPE,
    COMPRESSED_VIDEO_TYPE,
    CameraStateQueue,
    CameraStateUpdate,
)

ROOT = pathlib.Path(__file__).resolve().parents[1]
WIDTH, HEIGHT = 64, 48


@pytest.fixture(scope="module")
def units():
    """Thirty units, a keyframe every five."""
    return streams.encode_stream(width=WIDTH, height=HEIGHT, frames=30, gop=5)


@pytest.fixture(scope="module")
def units_with_b_frames():
    return streams.encode_stream(width=WIDTH, height=HEIGHT, frames=12, gop=12, bframes=2)


@pytest.fixture(autouse=True)
def _fresh_publisher_instances():
    _FakeLivePublisher.instances.clear()
    yield


class _Publisher(_FakeLivePublisher):
    """The fake of `test_ros_runtime.py`, with the one thing a passthrough
    camera adds to a publisher: the stream it is to forward."""

    def __init__(self, *, passthrough=None, **kwargs):
        super().__init__(**kwargs)
        self.passthrough = passthrough
        self.bitrates = []

    def set_bitrate_kbps(self, kbps):
        self.bitrates.append(kbps)


def _factory(**fixed):
    def factory(**kwargs):
        return _Publisher(**fixed, **kwargs)

    return factory


def _h264_camera(slug, *, type_=COMPRESSED_IMAGE_TYPE, topic="/camera/h264", width=WIDTH, height=HEIGHT, **kwargs):
    return _camera_cfg(slug, topic=topic, type_=type_, width=width, height=height, **kwargs)


def _settings(**overrides):
    values = dict(LOW_BANDWIDTH_DEFAULTS)
    values.update(overrides)
    return LowBandwidthSettings.resolve(values, {})


async def _eventually(condition, timeout=5.0, what="the condition"):
    """Waits for `condition` without holding the event loop: a body that
    runs on the loop and waits with `wait_until` blocks the very loop the
    thing it waits for has to run on, and passes only if that thing had
    already happened."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("{} never became true within {}s".format(what, timeout))
        await asyncio.sleep(0.01)


async def _feed(rt, slug, units, *, start_ms=1_000_000, step_ms=33, codec="h264"):
    """Hands `units` to the runtime the way the subscription callback does,
    on the executor thread, one capture time after the other."""
    entry = rt._cameras[slug]

    def work():
        for index, unit in enumerate(units):
            rt._on_camera_video(slug, entry, codec, unit, start_ms + index * step_ms)

    await rt._submit_async(work)


def _start_compressed_image_publisher(topic, payloads, *, format_, hz=60):
    """A real publisher of `sensor_msgs/msg/CompressedImage` on its own node,
    cycling through `payloads`. Returns `stop()`."""
    node = rclpy.create_node("test_compressed_publisher_{}".format(id(object())))
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    publisher = node.create_publisher(CompressedImage, topic, 10)
    state = {"index": 0}

    def tick():
        message = CompressedImage()
        message.format = format_
        message.data = payloads[state["index"] % len(payloads)]
        state["index"] += 1
        publisher.publish(message)

    timer = node.create_timer(1.0 / hz, tick)

    def stop():
        timer.cancel()
        executor.shutdown()
        node.destroy_node()
        thread.join(timeout=5.0)

    return stop


class _CompressedVideo:
    """Stands in for `foxglove_msgs/msg/CompressedVideo`, which the test
    image does not have and must not need: the same fields, and the one
    class method the bridge reads a message type's fields with."""

    __slots__ = ("timestamp", "frame_id", "data", "format")

    def __init__(self, data=b"", format="h264"):  # noqa: A002 - the field's name
        self.timestamp = None
        self.frame_id = "camera"
        self.data = data
        self.format = format

    @classmethod
    def get_fields_and_field_types(cls):
        return {
            "timestamp": "builtin_interfaces/Time",
            "frame_id": "string",
            "data": "sequence<uint8>",
            "format": "string",
        }


@pytest.fixture
def compressed_video(monkeypatch):
    """Makes the type resolvable by its name, as on a robot that has
    `foxglove_msgs`, and keeps the adapter from subscribing: rclpy cannot
    subscribe to a class that is not a generated message, and the routing of
    a message is checked by handing the adapter one."""
    real = ros_runtime.get_message

    def get_message(name):
        return _CompressedVideo if name == COMPRESSED_VIDEO_TYPE else real(name)

    monkeypatch.setattr(ros_runtime, "get_message", get_message)
    monkeypatch.setattr(ros_runtime._RosSourceAdapter, "start", lambda self: None)
    return _CompressedVideo


def _counting(monkeypatch, module, name):
    original = getattr(module, name)
    calls = []

    def wrapper(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(module, name, wrapper)
    return calls


# --- which ROS messages are passed through (spec test 5) -----------------------


def test_a_compressed_image_carrying_h264_reaches_the_stream_undecoded(units, monkeypatch):
    conversions = _counting(monkeypatch, camera, "to_bgr")
    decoders = _counting(monkeypatch, passthrough, "_new_decoder")

    async def body(rt):
        stop = _start_compressed_image_publisher("/camera/h264", units, format_="h264")
        try:
            assert await rt.apply_cameras(by_slug([_h264_camera("front")])) == []
            entry = rt._cameras["front"]
            await _eventually(
                lambda: entry.h264_stream.newest_keyframe() is not None, what="a keyframe"
            )
            return entry.passes_through, entry.h264_stream.newest_keyframe(), entry.raw.take()
        finally:
            stop()

    passes_through, keyframe, raw = run(body)
    assert passes_through is True
    assert streams.is_idr(keyframe.data)
    assert raw is None  # no H.264 message was kept as a picture to convert later
    assert conversions == [] and decoders == []  # and nothing was decoded on the way


def test_a_compressed_image_carrying_jpeg_is_still_a_picture(monkeypatch):
    decoders = _counting(monkeypatch, passthrough, "_new_decoder")
    picture = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
    picture[:, :] = (10, 120, 200)
    jpeg = camera.encode_jpeg(picture)

    async def body(rt):
        stop = _start_compressed_image_publisher("/camera/h264", [jpeg], format_="jpeg")
        try:
            await rt.apply_cameras(by_slug([_h264_camera("front")]))
            wire = await _pull_snapshot(rt)
            entry = rt._cameras["front"]
            return wire, entry.passes_through, entry.h264_stream.newest_keyframe()
        finally:
            stop()

    wire, passes_through, keyframe = run(body)
    header, _ = _split_snapshot_frame(wire)
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    assert passes_through is False and keyframe is None
    assert decoders == []


@pytest.mark.parametrize("format_, expected", [
    ("h264", "h264"),
    ("H264", "h264"),
    ("video/H264; annex-b", "h264"),
    ("jpeg", None),
    ("rgb8; jpeg compressed bgr8", None),
    ("", None),
])
def test_a_compressed_image_is_h264_when_its_format_says_so(format_, expected):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        message = CompressedImage()
        message.format = format_
        return rt._cameras["front"].adapter._video_codec(message)

    assert run(body) == expected


def test_a_raw_image_camera_has_no_stream_at_all():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        entry = rt._cameras["front"]
        return entry.h264_stream, entry.passes_through

    assert run(body) == (None, False)


def test_compressed_video_is_read_by_its_name_and_fields(units, compressed_video):
    async def body(rt):
        errors = await rt.apply_cameras(by_slug([_h264_camera("front", type_=COMPRESSED_VIDEO_TYPE)]))
        entry = rt._cameras["front"]
        from_the_start = entry.passes_through
        for unit in units[:6]:
            message = compressed_video(data=unit, format="h264")
            routed = await rt._submit_async(lambda message=message: entry.adapter._route_video(message))
            assert routed is True
        return errors, from_the_start, entry.h264_stream.newest_keyframe()

    errors, from_the_start, keyframe = run(body)
    assert errors == []
    assert from_the_start is True  # by its type; no message has to say so first
    assert streams.is_idr(keyframe.data)


def test_a_robot_without_the_package_reports_unknown_type_for_that_camera_only():
    async def body(rt):
        errors = await rt.apply_cameras(by_slug([
            _h264_camera("video", type_=COMPRESSED_VIDEO_TYPE),
            _camera_cfg("front"),
        ]))
        return errors, sorted(rt._cameras)

    errors, cameras = run(body)
    assert [(error.slug, error.kind) for error in errors] == [("video", "camera")]
    assert "unknown type" in errors[0].message
    assert cameras == ["front"]


def test_the_package_declares_no_dependency_on_foxglove_msgs():
    for name in ("package.xml", "Dockerfile.dev", "setup.py"):
        assert "foxglove" not in (ROOT / name).read_text().lower(), name
    for path in (ROOT / "debian").glob("control*"):
        assert "foxglove" not in path.read_text().lower(), path.name


def test_a_class_of_that_name_without_the_fields_is_refused_when_applied(monkeypatch):
    class Impostor:
        @classmethod
        def get_fields_and_field_types(cls):
            return {"data": "sequence<uint8>"}

    real = ros_runtime.get_message
    monkeypatch.setattr(
        ros_runtime, "get_message",
        lambda name: Impostor if name == COMPRESSED_VIDEO_TYPE else real(name),
    )

    async def body(rt):
        return await rt.apply_cameras(by_slug([_h264_camera("video", type_=COMPRESSED_VIDEO_TYPE)]))

    errors = run(body)
    assert len(errors) == 1
    assert "format" in errors[0].message and "timestamp" in errors[0].message


def test_another_camera_type_is_refused_naming_all_three():
    async def body(rt):
        return await rt.apply_cameras(by_slug([_camera_cfg("bad", type_="sensor_msgs/msg/BatteryState")]))

    (error,) = run(body)
    for name in ("sensor_msgs/msg/Image", COMPRESSED_IMAGE_TYPE, COMPRESSED_VIDEO_TYPE):
        assert name in error.message


# --- the RTSP source picks its adapter (spec test 6) ---------------------------


class _RecordingAdapter:
    built = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        type(self).built.append(self)

    def start(self):
        pass

    def stop(self):
        pass


def _rtsp_camera(slug, passthrough_):
    return CameraConfig(
        slug=slug,
        source=RtspSource(
            url="rtsp://cam.example.test/stream", transport="tcp", credentials=None,
            passthrough=passthrough_,
        ),
        width=WIDTH, height=HEIGHT, fps=10, bitrate_kbps=500, snapshot_interval_seconds=1,
    )


def test_an_rtsp_source_with_passthrough_gets_the_passthrough_adapter_and_the_stream():
    class Passthrough(_RecordingAdapter):
        built = []

    class Decoding(_RecordingAdapter):
        built = []

    async def body(rt):
        with mock.patch.object(camera_sources, "RtspPassthroughSourceAdapter", Passthrough), \
                mock.patch.object(camera_sources, "RtspSourceAdapter", Decoding):
            await rt.apply_cameras(by_slug([_rtsp_camera("yard", True)]))
            entry = rt._cameras["yard"]
            return entry.passes_through, entry.h264_stream

    passes_through, stream = run(body)
    assert Decoding.built == []
    (adapter,) = Passthrough.built
    assert passes_through is True
    assert adapter.kwargs["stream"] is stream and stream is not None
    assert adapter.kwargs["url"] == "rtsp://cam.example.test/stream"
    assert callable(adapter.kwargs["on_healthy"]) and callable(adapter.kwargs["on_error"])
    assert "on_frame" not in adapter.kwargs  # it has no pictures to hand over


def test_the_same_url_without_passthrough_still_gets_the_decoding_adapter():
    class Passthrough(_RecordingAdapter):
        built = []

    class Decoding(_RecordingAdapter):
        built = []

    async def body(rt):
        with mock.patch.object(camera_sources, "RtspPassthroughSourceAdapter", Passthrough), \
                mock.patch.object(camera_sources, "RtspSourceAdapter", Decoding):
            await rt.apply_cameras(by_slug([_rtsp_camera("yard", False)]))
            entry = rt._cameras["yard"]
            return entry.passes_through, entry.h264_stream

    assert run(body) == (False, None)
    assert Passthrough.built == []
    assert len(Decoding.built) == 1 and "on_frame" in Decoding.built[0].kwargs


def test_switching_passthrough_on_rebuilds_the_camera():
    """`passthrough` is part of the source, so changing it is a source
    change: the old adapter goes, the new one comes, a running stream ends."""
    class Passthrough(_RecordingAdapter):
        built = []

    class Decoding(_RecordingAdapter):
        built = []

    async def body(rt):
        with mock.patch.object(camera_sources, "RtspPassthroughSourceAdapter", Passthrough), \
                mock.patch.object(camera_sources, "RtspSourceAdapter", Decoding):
            await rt.apply_cameras(by_slug([_rtsp_camera("yard", False)]))
            await rt.apply_cameras(by_slug([_rtsp_camera("yard", True)]))
            return rt._cameras["yard"].passes_through

    assert run(body) is True
    assert len(Decoding.built) == 1 and len(Passthrough.built) == 1


# --- refusals (spec test 7) ----------------------------------------------------


def test_a_size_mismatch_is_a_camera_error_naming_both_sizes(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", width=1280, height=720)]))
        await _feed(rt, "front", units)
        states = await _drain_camera_states(rt)
        return states, rt._cameras["front"].h264_stream.newest_keyframe()

    states, keyframe = run(body)
    (state,) = states  # thirty refused units, one report
    assert state.publishing is False and state.cause == CAMERA_STATE_CAUSE_SOURCE
    code, message = state.error
    assert code == "source_error"
    assert "64x48" in message and "1280x720" in message
    assert state.request_id is None
    assert keyframe is None  # nothing is published, snapshots included


def test_a_refusal_is_one_line_in_the_robots_own_log(units):
    """The console shows that a camera's source is not answering, not why.
    The robot's log is where the sentence can be read without a debugger."""
    import logging

    lines = []

    class Handler(logging.Handler):
        def emit(self, record):
            lines.append(record.getMessage())

    handler = Handler(level=logging.DEBUG)
    logger = logging.getLogger("fleetless_bridge.ros_runtime")
    logger.addHandler(handler)

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", width=1280, height=720)]))
        await _feed(rt, "front", units)
        await _drain_camera_states(rt)

    try:
        run(body)
    finally:
        logger.removeHandler(handler)
    refusals = [line for line in lines if "cannot be passed through" in line]
    assert len(refusals) == 1  # thirty refused units, one line
    assert "'front'" in refusals[0] and "64x48" in refusals[0] and "1280x720" in refusals[0]


class _RuntimeLog:
    """The lines the runtime's logger writes while the block runs. (`caplog`
    sees nothing here: the package's loggers do not propagate in this suite.)"""

    def __init__(self):
        self.lines = []

    def __enter__(self):
        import logging

        lines = self.lines

        class Handler(logging.Handler):
            def emit(self, record):
                lines.append(record.getMessage())

        self._handler = Handler(level=logging.DEBUG)
        self._logger = logging.getLogger("fleetless_bridge.ros_runtime")
        self._logger.addHandler(self._handler)
        return self

    def __exit__(self, *exc):
        self._logger.removeHandler(self._handler)

    def refusals(self):
        # With the colon: the line that says the reason keeps changing has
        # the same words and names no reason.
        return [line for line in self.lines if "cannot be passed through: " in line]


def _refusing(rt, slug, clock):
    """`refuse(codec, at_s)`: one `CompressedVideo` message of `codec` for
    `slug`, with the runtime's monotonic clock at `at_s` seconds."""
    entry = rt._cameras[slug]

    async def refuse(codec, at_s):
        clock[0] = 5000.0 + at_s
        await rt._submit_async(lambda: rt._on_camera_video(slug, entry, codec, b"", 1_000_000))

    return refuse


def test_a_reason_that_changes_within_the_minute_is_logged_at_once(units, compressed_video, monkeypatch):
    """The camera's state carries the sentence, and the console shows only
    its kind: the robot's log is where the reason can be read. A limit of
    one line a minute per camera held the second reason back, and nobody
    offered it again -- the log said H.265 for as long as the topic carried
    VP9."""
    clock = [5000.0]
    monkeypatch.setattr(ros_runtime.time, "monotonic", lambda: clock[0])

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", type_=COMPRESSED_VIDEO_TYPE)]))
        refuse = _refusing(rt, "front", clock)
        await refuse("h265", 0)
        for at_s in range(20, 320, 20):  # VP9 from twenty seconds on, for five minutes
            await refuse("vp9", at_s)
        return await _drain_camera_states(rt)

    with _RuntimeLog() as log_:
        states = run(body)
    first, second = log_.refusals()
    assert "carries H.265 and not H.264" in first
    assert "carries VP9 and not H.264" in second
    assert states[-1].error[1].startswith("the topic carries VP9")


def test_the_same_reason_is_logged_at_most_once_a_minute(units, compressed_video, monkeypatch):
    """Two publishers on one topic that disagree about the codec make a new
    sentence with every message. The camera's state follows them; the log
    writes each of the two once a minute."""
    clock = [5000.0]
    monkeypatch.setattr(ros_runtime.time, "monotonic", lambda: clock[0])

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", type_=COMPRESSED_VIDEO_TYPE)]))
        refuse = _refusing(rt, "front", clock)
        for index in range(40):  # twenty of each within twenty seconds
            await refuse("h265" if index % 2 == 0 else "vp9", index * 0.5)
        within_the_minute = len(log_.refusals())
        await refuse("h265", 61)   # its line is a minute old
        await refuse("vp9", 61.2)  # and so is this one's
        await refuse("h265", 61.4)
        return within_the_minute, await _drain_camera_states(rt)

    with _RuntimeLog() as log_:
        within_the_minute, states = run(body)
    assert within_the_minute == 2
    assert len(log_.refusals()) == 4
    assert states[-1].error[1].startswith("the topic carries H.265")  # the state still says the latest


def test_a_camera_whose_reason_never_settles_is_a_bounded_number_of_lines(units, compressed_video, monkeypatch):
    """A `format` that is a new word with every message is a new sentence
    with every message. Eight are written within a minute, then one line
    that says the rest is not, and where the reason still is."""
    clock = [5000.0]
    monkeypatch.setattr(ros_runtime.time, "monotonic", lambda: clock[0])
    limit = ros_runtime.PASSTHROUGH_REFUSAL_LOG_SENTENCES

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", type_=COMPRESSED_VIDEO_TYPE)]))
        refuse = _refusing(rt, "front", clock)
        for index in range(200):
            await refuse("codec{}".format(index), index * 0.1)  # twenty seconds
        within_the_minute = (len(log_.refusals()), [line for line in log_.lines if "keeps changing" in line])
        await refuse("codec-late", 90)  # the minute is over
        return within_the_minute, await _drain_camera_states(rt)

    with _RuntimeLog() as log_:
        (written, overflow), states = run(body)
    assert written == limit
    assert len(overflow) == 1 and "front" in overflow[0] and "state" in overflow[0]
    assert len(log_.refusals()) == limit + 1 and "codec-late" in log_.refusals()[-1]
    assert states[-1].error[1].startswith("the topic carries codec-late")


def test_a_refused_camera_answers_a_start_with_its_sentence_and_opens_no_publisher(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", width=1280, height=720)]))
        await _feed(rt, "front", units)
        await _drain_camera_states(rt)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt), dict(rt._live_publishers)

    states, live = run(body, live_publisher_factory=_factory())
    (state,) = states
    assert state.cause == CAMERA_STATE_CAUSE_COMMAND and state.request_id == "req-1"
    assert state.publishing is False
    assert state.error[0] == "source_error" and "1280x720" in state.error[1]
    assert live == {} and _FakeLivePublisher.instances == []


def test_a_camera_refused_after_it_worked_opens_no_publisher_either(units):
    """A picture camera with an error is still started when it holds a last
    frame to show. A passthrough camera has a last frame too -- its last
    snapshot -- and nothing to forward: it is answered before a room is
    joined, not after."""
    larger = streams.encode_stream(width=128, height=96, frames=5, gop=5)

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units)
        await _pull_snapshot(rt)
        assert rt._cameras["front"].latest.get() is not None
        await _feed(rt, "front", larger, start_ms=2_000_000)
        await _drain_camera_states(rt)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    (state,) = run(body, live_publisher_factory=_factory())
    assert state.cause == CAMERA_STATE_CAUSE_COMMAND and state.request_id == "req-1"
    assert state.error[0] == "source_error" and "128x96" in state.error[1]
    assert _FakeLivePublisher.instances == []


def test_no_snapshot_is_sent_of_a_refused_stream(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", width=1280, height=720)]))
        await _feed(rt, "front", units)
        return await rt.next_snapshot(10_000_000)

    assert run(body) is None


def test_b_frames_from_the_start_are_a_camera_error(units_with_b_frames):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units_with_b_frames)
        return await _drain_camera_states(rt)

    (state,) = run(body)
    assert state.error[0] == "source_error" and "B-frames" in state.error[1]
    assert state.cause == CAMERA_STATE_CAUSE_SOURCE


def test_b_frames_that_appear_mid_stream_end_the_running_stream(units, units_with_b_frames):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        started = await _drain_camera_states(rt)
        publisher = _FakeLivePublisher.instances[-1]

        await _feed(rt, "front", units_with_b_frames, start_ms=2_000_000)
        await _eventually(lambda: publisher.stop_calls == 1, what="the publisher's stop")
        await _eventually(lambda: "front" not in rt._live_publishers, what="the stream's end")
        return started, await _drain_camera_states(rt), publisher

    started, states, publisher = run(body, live_publisher_factory=_factory())
    assert [state.publishing for state in started] == [True]
    # Both facts, each once: what the source is, and what became of the
    # stream. The second is the one that ends the cloud's sessions.
    by_cause = {state.cause: state for state in states}
    assert set(by_cause) == {CAMERA_STATE_CAUSE_SOURCE, CAMERA_STATE_CAUSE_LIVE_LOST}
    for state in states:
        assert state.publishing is False
        assert state.error[0] == "source_error" and "B-frames" in state.error[1]
        assert state.request_id is None
    assert publisher.stop_calls == 1


def test_a_new_size_mid_stream_ends_the_running_stream(units):
    larger = streams.encode_stream(width=128, height=96, frames=5, gop=5)

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        publisher = _FakeLivePublisher.instances[-1]
        await _feed(rt, "front", larger, start_ms=2_000_000)
        await _eventually(lambda: publisher.stop_calls == 1, what="the publisher's stop")
        await _eventually(lambda: "front" not in rt._live_publishers, what="the stream's end")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_factory())
    lost = next(state for state in states if state.cause == CAMERA_STATE_CAUSE_LIVE_LOST)
    assert "128x96" in lost.error[1] and "64x48" in lost.error[1]


def test_a_codec_other_than_h264_is_refused_with_its_sentence(units, compressed_video):
    hevc = streams.encode_stream(width=WIDTH, height=HEIGHT, frames=6, gop=3, codec="libx265")

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", type_=COMPRESSED_VIDEO_TYPE)]))
        entry = rt._cameras["front"]
        for unit in hevc:
            message = compressed_video(data=unit, format="h265")
            await rt._submit_async(lambda message=message: entry.adapter._route_video(message))
        refused = await _drain_camera_states(rt)
        keyframe = entry.h264_stream.newest_keyframe()
        # The publisher is switched to H.264: the camera recovers by itself.
        for index, unit in enumerate(units[:6]):
            message = compressed_video(data=unit, format="h264")
            await rt._submit_async(lambda message=message: entry.adapter._route_video(message))
        return refused, keyframe, await _drain_camera_states(rt)

    refused, keyframe, recovered = run(body)
    (state,) = refused  # six messages, one report
    assert state.error == (
        "source_error", "the topic carries H.265 and not H.264; passthrough forwards H.264 only"
    )
    assert keyframe is None  # not one byte of it reached the stream
    (state,) = recovered
    assert state.error is None and state.cause == CAMERA_STATE_CAUSE_SOURCE


@pytest.mark.parametrize("written, shown", [
    ("vp9", "VP9"),
    ("mystery-codec", "mystery-codec"),
    ("x" * 40, "another codec"),
    ("<script>alert(1)</script>", "another codec"),
    ("", "another codec"),
])
def test_a_format_field_is_not_repeated_unless_it_is_a_plain_word(written, shown):
    """`format` is free text from whoever publishes the topic, and the camera
    error travels to the cloud and into a console."""
    assert ros_runtime._codec_display_name(written) == shown


def test_no_keyframe_in_ten_seconds_is_a_camera_error(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        entry = rt._cameras["front"]
        now = [100.0]
        entry.h264_stream._clock = lambda: now[0]
        dependent = [unit for unit in units if not streams.is_idr(unit)]
        for second in range(12):  # a frame a second, never a keyframe
            await _feed(rt, "front", [dependent[second]], start_ms=1_000_000 + second * 1000)
            now[0] += 1.0
        refused = await _drain_camera_states(rt)
        await _feed(rt, "front", units[:1], start_ms=2_000_000)  # and then one arrives
        return refused, await _drain_camera_states(rt)

    refused, recovered = run(body)
    (state,) = refused
    assert state.error[0] == "source_error"
    assert "keyframe" in state.error[1] and "10 s" in state.error[1]
    (state,) = recovered
    assert state.error is None


def test_a_recovered_stream_clears_the_error_once(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", width=1280, height=720)]))
        await _feed(rt, "front", units)
        refused = await _drain_camera_states(rt)
        # The developer corrects the size. Nothing about the source changed,
        # so the subscription stays and the stream is judged again.
        adapter = rt._cameras["front"].adapter
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        assert rt._cameras["front"].adapter is adapter
        await _drain_camera_states(rt)
        await _feed(rt, "front", units, start_ms=2_000_000)
        return refused, await _drain_camera_states(rt)

    refused, recovered = run(body)
    assert refused[0].error[0] == "source_error"
    (state,) = recovered  # thirty accepted units, one report
    assert state.error is None and state.publishing is False
    assert state.cause == CAMERA_STATE_CAUSE_SOURCE
    assert state.observed_at_ms == 2_000_000  # the keyframe that proved it


def test_a_refusal_that_arrives_during_a_join_does_not_commit_the_stream(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units)
        entry = rt._cameras["front"]
        start = asyncio.ensure_future(
            rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        )
        await asyncio.sleep(0.05)  # the join is in flight
        assert "front" not in rt._live_publishers
        larger = streams.encode_stream(width=128, height=96, frames=5, gop=5)
        await _feed(rt, "front", larger, start_ms=2_000_000)
        await start
        return await _drain_camera_states(rt), dict(rt._live_publishers), entry

    states, live, _ = run(body, live_publisher_factory=_factory(start_delay=0.3))
    assert live == {}
    assert _FakeLivePublisher.instances[-1].stop_calls == 1
    answer = next(state for state in states if state.cause == CAMERA_STATE_CAUSE_COMMAND)
    assert answer.publishing is False and answer.request_id == "req-1"
    assert answer.error[0] == "source_error" and "128x96" in answer.error[1]


def test_a_topic_that_turns_from_h264_to_pictures_ends_the_running_stream(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        publisher = _FakeLivePublisher.instances[-1]
        entry = rt._cameras["front"]
        await rt._submit_async(lambda: rt._on_camera_picture("front", entry))
        await _eventually(lambda: publisher.stop_calls == 1, what="the publisher's stop")
        await _eventually(lambda: "front" not in rt._live_publishers, what="the stream's end")
        return await _drain_camera_states(rt), entry.passes_through, entry.h264_stream.newest_keyframe()

    states, passes_through, keyframe = run(body, live_publisher_factory=_factory())
    (state,) = states
    assert state.cause == CAMERA_STATE_CAUSE_LIVE_LOST
    assert state.error[0] == "live_unavailable"
    assert "H.264 to pictures" in state.error[1]
    assert passes_through is False and keyframe is None


def test_a_topic_that_changes_kind_during_a_join_does_not_commit_the_stream(units):
    """The first H.264 message of a `CompressedImage` topic arrives while a
    picture publisher is joining. Nothing runs yet for the change to end, so
    the join ends itself."""
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        start = asyncio.ensure_future(
            rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        )
        await asyncio.sleep(0.05)
        await _feed(rt, "front", units[:3])
        await start
        return await _drain_camera_states(rt), dict(rt._live_publishers)

    states, live = run(body, live_publisher_factory=_factory(start_delay=0.3))
    assert live == {}
    (publisher,) = _FakeLivePublisher.instances
    assert publisher.passthrough is None and publisher.stop_calls == 1
    (state,) = states
    assert state.cause == CAMERA_STATE_CAUSE_COMMAND and state.request_id == "req-1"
    assert state.error[0] == "live_unavailable" and "start it again" in state.error[1]


# --- the publisher a camera gets (the runtime's half of the track) -------------


def test_a_passthrough_camera_hands_its_stream_to_the_publisher(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video"), _camera_cfg("front")]))
        await _feed(rt, "video", units)
        await rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.start_live("front", "wss://media.example", "room-2", "tok-2", "req-2")
        return rt._cameras["video"].h264_stream

    stream = run(body, live_publisher_factory=_factory())
    video, front = _FakeLivePublisher.instances
    assert video.passthrough is stream
    assert front.passthrough is None
    # A picture camera's publisher is told how to have the held message
    # converted when a viewer comes back. A passthrough camera holds none,
    # and its publisher is handed no such hook to wait for.
    assert callable(front.on_resume)
    assert video.on_resume is None
    # What the track declares to the media server is still the configured
    # bitrate; the configured size is the stream's own.
    assert (video.width, video.height, video.bitrate_kbps) == (WIDTH, HEIGHT, 500)


def test_a_picture_camera_gets_exactly_the_publisher_arguments_it_always_got():
    """An existing publisher factory -- every fake in the suite, and
    `live.LivePublisher` before it learned about streams -- is not handed a
    keyword it does not know."""
    seen = {}

    def factory(**kwargs):
        seen.update(kwargs)
        return _Publisher(**kwargs)

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")

    run(body, live_publisher_factory=factory)
    assert "passthrough" not in seen


def _publishing_without_a_network(monkeypatch):
    """The real publisher classes, started for real, with the two things
    that would reach a LiveKit server replaced: the signalling client and the
    peer connection. Everything between `start_live` and a started publisher
    is then the shipped code -- the default factory, the publisher's own
    constructor, the encoder seam."""
    from test_live_encoder_seam import _SeamConnection, _SeamSignal

    made = {"connections": [], "signals": []}

    def connection_factory(ice_servers):
        connection = _SeamConnection()
        made["connections"].append(connection)
        return connection

    def signal_factory(url, token, **callbacks):
        signal = _SeamSignal()
        signal.callbacks = callbacks
        made["signals"].append(signal)
        return signal

    monkeypatch.setattr(live, "_default_connection_factory", connection_factory)
    monkeypatch.setattr(live, "_default_signal_factory", signal_factory)
    return made


def test_start_live_starts_the_real_passthrough_publisher_through_the_default_factory(units, monkeypatch):
    """No fake factory here, on purpose. Every other runtime test hands
    `RosRuntime` a fake publisher, so a keyword the real constructors cannot
    take together would leave the whole suite green while every passthrough
    start on a robot answered `TypeError`. This goes from `camera_start` to
    a started `PassthroughPublisher`."""
    made = _publishing_without_a_network(monkeypatch)

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video"), _camera_cfg("front")]))
        await _feed(rt, "video", units)
        await rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.start_live("front", "wss://media.example", "room-2", "tok-2", "req-2")
        states = await _drain_camera_states(rt)
        video, front = rt._live_publishers["video"], rt._live_publishers["front"]
        stream = rt._cameras["video"].h264_stream
        installed = [
            getattr(connection.sender, live.SENDER_ENCODER_ATTR, None)
            for connection in made["connections"]
        ]
        await rt.stop_all_live()
        return states, video, front, stream, installed

    states, video, front, stream, installed = run(body)
    assert [(state.slug, state.publishing, state.error) for state in states] == [
        ("video", True, None), ("front", True, None),
    ]
    assert type(video) is live_passthrough.PassthroughPublisher and video.passthrough is stream
    assert type(front) is live.LivePublisher
    # The passthrough camera's sender carries the encoder that forwards, and
    # the picture camera's none of ours: aiortc builds its own there.
    assert isinstance(installed[0], live_passthrough.PassthroughEncoder)
    assert installed[1] is None
    # Only the picture camera's publisher asks its camera for the held
    # message at a resume.
    assert video._on_resume is None and callable(front._on_resume)


@pytest.mark.parametrize("handed", ["none", "an encoder"])
def test_the_default_factory_gives_a_passthrough_camera_its_own_encoder_whatever_it_is_handed(handed):
    """How `encoder=` and `passthrough=` combine: a passthrough camera never
    takes the robot's `live.encoder`. The factory may be called with both
    keywords -- one call serves every camera -- and for a passthrough camera
    the handed encoder is closed unused."""
    closed = threading.Event()

    class Foreign:
        def close(self):
            closed.set()

    foreign = Foreign() if handed == "an encoder" else None
    stream = passthrough.PassthroughStream(width=WIDTH, height=HEIGHT, slug="video")
    publisher = ros_runtime._default_live_publisher_factory(
        holder=camera.LatestFrameHolder(), width=WIDTH, height=HEIGHT, fps=10, bitrate_kbps=500,
        on_lost=lambda reason: None, slug="video", received_s=None,
        encoder=foreign, passthrough=stream,
    )
    assert type(publisher) is live_passthrough.PassthroughPublisher
    assert isinstance(publisher._encoder, live_passthrough.PassthroughEncoder)
    if foreign is not None:
        assert closed.wait(timeout=5.0)

    # And a picture camera keeps the encoder it is handed, as the seam says.
    kept = Foreign()
    picture = ros_runtime._default_live_publisher_factory(
        holder=camera.LatestFrameHolder(), width=WIDTH, height=HEIGHT, fps=10, bitrate_kbps=500,
        on_lost=lambda reason: None, slug="front", received_s=None, encoder=kept,
    )
    assert type(picture) is live.LivePublisher and picture._encoder is kept


# --- snapshots (spec test 8) ---------------------------------------------------


def test_a_snapshot_is_the_newest_keyframe_decoded_once(units, monkeypatch):
    decoders = _counting(monkeypatch, passthrough, "_new_decoder")

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        entry = rt._cameras["front"]
        await _feed(rt, "front", units[:13], start_ms=1_000_000, step_ms=10)
        before_any_pull = len(decoders)

        first = await _pull_snapshot(rt)
        after_first = len(decoders)

        # Due again, and no keyframe has arrived since: the picture already
        # decoded goes out again.
        entry.last_snapshot_at = None
        second = await _pull_snapshot(rt)
        after_second = len(decoders)

        await _feed(rt, "front", units[13:20], start_ms=2_000_000, step_ms=10)
        entry.last_snapshot_at = None
        entry.rate = ros_runtime.sampling.MaxHzPolicy(1000)  # not the subject here
        third = await _pull_snapshot(rt)
        return before_any_pull, first, after_first, second, after_second, third, len(decoders)

    before_any_pull, first, after_first, second, after_second, third, after_third = run(body)
    assert before_any_pull == 0  # thirteen units, three keyframes, nobody asked: nothing decoded
    assert after_first == 1 and after_second == 1 and after_third == 2

    header, jpeg = _split_snapshot_frame(first)
    assert header["slug"] == "front" and header["mime"] == "image/jpeg"
    assert (header["width"], header["height"]) == (WIDTH, HEIGHT)
    # Units 0, 5 and 10 are keyframes; the newest was captured at 1_000_100.
    assert header["timestamp_ms"] == 1_000_000 + 10 * 10
    picture = cv2.imdecode(np.frombuffer(jpeg, dtype=np.uint8), cv2.IMREAD_COLOR)
    assert picture.shape == (HEIGHT, WIDTH, 3)
    # It is that keyframe's picture, give or take what JPEG does to it.
    expected = streams.decode_all(units)[10]
    assert np.abs(picture.astype(int) - expected.astype(int)).mean() < 12

    assert _split_snapshot_frame(second)[0]["timestamp_ms"] == header["timestamp_ms"]
    assert _split_snapshot_frame(third)[0]["timestamp_ms"] == 2_000_000 + 2 * 10  # unit 15


def test_a_camera_without_a_keyframe_sends_no_snapshot_and_stays_due(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units[1:5])  # mid-group: nothing a picture could come from
        nothing = await rt.next_snapshot(10_000_000)
        entry = rt._cameras["front"]
        still_due = entry.last_snapshot_at is None
        await _feed(rt, "front", units[5:6], start_ms=2_000_000)
        return nothing, still_due, await rt.next_snapshot(10_000_000)

    nothing, still_due, wire = run(body)
    assert nothing is None and still_due
    assert _split_snapshot_frame(wire)[0]["timestamp_ms"] == 2_000_000  # at once, not an interval later


def test_fps_still_bounds_how_often_a_snapshot_is_taken(units, monkeypatch):
    decoders = _counting(monkeypatch, passthrough, "_new_decoder")

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", fps=1)]))
        entry = rt._cameras["front"]
        await _feed(rt, "front", units[:6], start_ms=1_000_000, step_ms=10)
        first = await _pull_snapshot(rt)
        await _feed(rt, "front", units[6:11], start_ms=2_000_000, step_ms=10)
        entry.last_snapshot_at = None
        second = await _pull_snapshot(rt)
        return first, second

    first, second = run(body)
    # A second keyframe arrived within the same second: at `fps: 1` the
    # snapshot that went out is still the first picture.
    assert _split_snapshot_frame(second)[0]["timestamp_ms"] == _split_snapshot_frame(first)[0]["timestamp_ms"]


def test_a_keyframe_that_does_not_decode_is_logged_and_tried_once(units, monkeypatch):
    attempts = []

    def broken(data):
        attempts.append(data)
        raise passthrough.KeyframeDecodeError("InvalidDataError")

    monkeypatch.setattr(passthrough, "decode_keyframe", broken)

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        await _feed(rt, "front", units[:3])
        results = [await rt.next_snapshot(10_000_000) for _ in range(5)]
        return results

    assert run(body) == [None] * 5
    assert len(attempts) == 1  # not once per pull


# --- a still is not a frame arriving -------------------------------------------


class _InertPassthroughAdapter(_RecordingAdapter):
    built = []


async def _rtsp_passthrough_camera(rt, slug="yard"):
    """An RTSP passthrough camera whose adapter does nothing by itself: the
    test plays the adapter's thread, which pushes into the camera's stream
    and reports through `_on_source_error`."""
    with mock.patch.object(camera_sources, "RtspPassthroughSourceAdapter", _InertPassthroughAdapter):
        await rt.apply_cameras(by_slug([_rtsp_camera(slug, True)]))
    return rt._cameras[slug]


def test_a_still_from_a_keyframe_kept_across_an_outage_does_not_report_the_camera_recovered(units):
    """The camera delivered keyframes and then dropped. The stream still
    holds the last one. A snapshot that falls due must not decode it as if a
    frame had arrived: the error stands, nothing says "recovered", and no
    picture of the stream that is gone goes out as the camera's snapshot."""
    async def body(rt):
        entry = await _rtsp_passthrough_camera(rt)
        for index, unit in enumerate(units[:6]):
            assert entry.h264_stream.push(unit, 1_000_000 + index * 33) is True
        await rt._submit_async(lambda: rt._on_source_error(
            "yard", entry, "source_unavailable", "the camera stopped delivering packets",
        ))
        reported = await _drain_camera_states(rt)
        snapshot = await rt.next_snapshot(10_000_000)
        after_the_snapshot = await _drain_camera_states(rt)
        return reported, snapshot, after_the_snapshot, entry.last_source_error

    reported, snapshot, after_the_snapshot, standing = run(body)
    assert [(state.cause, state.error[0]) for state in reported] == [
        (CAMERA_STATE_CAUSE_SOURCE, "source_unavailable")
    ]
    assert snapshot is None
    assert after_the_snapshot == []  # in particular: no `cause: source, error: null`
    assert standing is not None and standing[0] == "source_unavailable"


def test_a_start_after_an_outage_is_answered_with_the_error_also_when_a_snapshot_was_due(units):
    """The gate for a passthrough camera with a standing error only holds
    if nothing lifts the error behind its back. Before, the due snapshot
    did, and the next `camera_start` opened a publisher with nothing to
    forward."""
    async def body(rt):
        entry = await _rtsp_passthrough_camera(rt)
        for index, unit in enumerate(units[:6]):
            entry.h264_stream.push(unit, 1_000_000 + index * 33)
        await rt._submit_async(lambda: rt._on_source_error(
            "yard", entry, "source_unavailable", "the camera stopped delivering packets",
        ))
        await rt.next_snapshot(10_000_000)
        await _drain_camera_states(rt)
        await rt.start_live("yard", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt), dict(rt._live_publishers)

    states, live = run(body, live_publisher_factory=_factory())
    (answer,) = states
    assert answer.cause == CAMERA_STATE_CAUSE_COMMAND and answer.request_id == "req-1"
    assert answer.publishing is False and answer.error[0] == "source_unavailable"
    assert live == {} and _FakeLivePublisher.instances == []


def test_a_still_taken_while_the_camera_works_changes_nothing_about_its_health(units, monkeypatch):
    """The other half: with no error standing, a snapshot is decoded and
    sent, and it queues no state. Health is reported by units that arrive."""
    decoders = _counting(monkeypatch, passthrough, "_new_decoder")

    async def body(rt):
        entry = await _rtsp_passthrough_camera(rt)
        for index, unit in enumerate(units[:6]):
            entry.h264_stream.push(unit, 1_000_000 + index * 33)
        wire = await _pull_snapshot(rt)
        return wire, await _drain_camera_states(rt)

    wire, states = run(body)
    assert _split_snapshot_frame(wire)[0]["timestamp_ms"] == 1_000_000 + 5 * 33
    assert states == [] and len(decoders) == 1


def test_a_topic_that_turns_to_another_codec_takes_its_last_keyframe_with_it(units, compressed_video):
    """`CompressedVideo` carried H.264 and now says `h265`. What the stream
    still holds is the old stream's keyframe; of a camera that is refused,
    nothing is published."""
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front", type_=COMPRESSED_VIDEO_TYPE)]))
        entry = rt._cameras["front"]
        await _feed(rt, "front", units[:6])
        assert entry.h264_stream.newest_keyframe() is not None
        message = compressed_video(data=b"\x00\x00\x00\x01\x40\x01", format="h265")
        await rt._submit_async(lambda: entry.adapter._route_video(message))
        return entry.h264_stream.newest_keyframe(), await rt.next_snapshot(10_000_000)

    assert run(body) == (None, None)


# --- beside the JPEG a camera sends as its own snapshot -------------------------


def test_a_topic_that_turned_from_jpeg_to_h264_sends_the_keyframe_and_not_the_old_jpeg(units):
    """A `CompressedImage` topic's JPEG is kept as the source encoded it and
    goes out as the snapshot unchanged. When the topic turns to H.264 that
    JPEG is still held, in the configured size, and would go out on every
    pull, for good: the keyframe would never be asked."""
    picture = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
    picture[:, :] = (10, 120, 200)
    jpeg = camera.encode_jpeg(picture)

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front")]))
        entry = rt._cameras["front"]
        entry.encoded.set(jpeg, 900_000)  # what the last picture message left behind
        await _feed(rt, "front", units[:6], start_ms=1_000_000)
        return await _pull_snapshot(rt)

    header, image = _split_snapshot_frame(run(body))
    assert header["timestamp_ms"] == 1_000_000 + 5 * 33  # the newest keyframe's, not the JPEG's
    assert image != jpeg


def test_the_health_restatement_knows_a_passthrough_camera_by_its_newest_keyframe(units):
    """After a reconnect the bridge says again what it knows of every
    camera. A passthrough camera nobody took a snapshot of yet has no
    picture anywhere; its newest keyframe is how it is known to work."""
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("front"), _camera_cfg("silent", topic="/nobody")]))
        await _feed(rt, "front", units[:8], start_ms=1_000_000)
        await _drain_camera_states(rt)
        await rt.report_current_camera_health()
        return await _drain_camera_states(rt)

    (state,) = run(body)  # and nothing about the camera that has delivered nothing
    assert (state.slug, state.publishing, state.error, state.cause) == (
        "front", False, None, CAMERA_STATE_CAUSE_SOURCE,
    )
    assert state.observed_at_ms == 1_000_000 + 5 * 33


# --- low-bandwidth mode (spec test 10) -----------------------------------------


def test_reduce_ends_a_passthrough_stream_and_retargets_the_encoded_one_beside_it(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video"), _camera_cfg("front")]))
        await _feed(rt, "video", units)
        await rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.start_live("front", "wss://media.example", "room-2", "tok-2", "req-2")
        await _drain_camera_states(rt)
        await rt.set_low_bandwidth(True, _settings(camera="reduce", camera_bitrate_kbps=300))
        return await _drain_camera_states(rt), sorted(rt._live_publishers)

    states, live = run(body, live_publisher_factory=_factory())
    video, front = _FakeLivePublisher.instances
    assert live == ["front"]
    assert video.stop_calls == 1 and video.bitrates == []
    assert front.stop_calls == 0 and front.bitrates == [300]  # reduced as today
    (state,) = states
    assert state.slug == "video" and state.publishing is False
    assert state.cause == CAMERA_STATE_CAUSE_LIVE_LOST
    assert state.error[0] == "low_bandwidth"
    assert "passthrough" in state.error[1]


def test_stop_ends_both(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video"), _camera_cfg("front")]))
        await _feed(rt, "video", units)
        await rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.start_live("front", "wss://media.example", "room-2", "tok-2", "req-2")
        await _drain_camera_states(rt)
        await rt.set_low_bandwidth(True, _settings(camera="stop"))
        return await _drain_camera_states(rt), sorted(rt._live_publishers)

    states, live = run(body, live_publisher_factory=_factory())
    assert live == []
    assert sorted(state.slug for state in states) == ["front", "video"]
    assert all(state.error[0] == "low_bandwidth" for state in states)


def test_leaving_the_mode_does_not_bring_a_passthrough_stream_back(units):
    """It ended; the cloud asks again when somebody watches."""
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video")]))
        await _feed(rt, "video", units)
        await rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.set_low_bandwidth(True, _settings(camera="reduce"))
        await rt.set_low_bandwidth(False, _settings(camera="reduce"))
        return sorted(rt._live_publishers)

    assert run(body, live_publisher_factory=_factory()) == []
    assert len(_FakeLivePublisher.instances) == 1


def test_a_join_that_completes_during_the_mode_ends_a_passthrough_stream_under_reduce(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video")]))
        await _feed(rt, "video", units)
        start = asyncio.ensure_future(
            rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        )
        await asyncio.sleep(0.05)
        await rt.set_low_bandwidth(True, _settings(camera="reduce"))
        await start
        return await _drain_camera_states(rt), dict(rt._live_publishers)

    states, live = run(body, live_publisher_factory=_factory(start_delay=0.3))
    assert live == {}
    (publisher,) = _FakeLivePublisher.instances
    assert publisher.stop_calls == 1 and publisher.bitrates == []
    (state,) = states
    assert state.cause == CAMERA_STATE_CAUSE_COMMAND and state.request_id == "req-1"
    assert state.error[0] == "low_bandwidth"


def test_a_new_passthrough_stream_is_refused_in_the_mode_as_any_other(units):
    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video")]))
        await _feed(rt, "video", units)
        await rt.set_low_bandwidth(True, _settings(camera="reduce"))
        await rt.start_live("video", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    (state,) = run(body, live_publisher_factory=_factory())
    assert state.error[0] == "low_bandwidth" and state.publishing is False
    assert _FakeLivePublisher.instances == []


# --- two levels per camera in the state queue ---------------------------------


def _state(cause, error=None, slug="front"):
    return CameraStateUpdate(slug, False, error, cause=cause, observed_at_ms=1, request_id=None)


def test_what_the_source_is_and_what_became_of_the_stream_are_both_delivered():
    """A stream that ends because its source turned unusable reports both in
    one moment. The cloud keeps them as two facets and ends its sessions on
    the second only, so neither may replace the other in the queue."""
    queue = CameraStateQueue()
    queue.put(_state(CAMERA_STATE_CAUSE_SOURCE, ("source_error", "B-frames")))
    queue.put(_state(CAMERA_STATE_CAUSE_LIVE_LOST, ("source_error", "B-frames")))
    first, second = queue.try_get(), queue.try_get()
    assert (first.cause, second.cause) == (CAMERA_STATE_CAUSE_SOURCE, CAMERA_STATE_CAUSE_LIVE_LOST)
    assert queue.try_get() is None


def test_a_level_still_replaces_the_one_before_it():
    """What the queue was built for is unchanged: a source that flaps is one
    entry, the latest, in the position the first one took."""
    queue = CameraStateQueue()
    queue.put(_state(CAMERA_STATE_CAUSE_SOURCE, ("source_unavailable", "gone")))
    queue.put(_state(CAMERA_STATE_CAUSE_SOURCE, None, slug="rear"))
    queue.put(_state(CAMERA_STATE_CAUSE_SOURCE, None))
    queue.put(_state(CAMERA_STATE_CAUSE_SOURCE, ("source_unavailable", "gone again")))
    first, second = queue.try_get(), queue.try_get()
    assert (first.slug, first.error) == ("front", ("source_unavailable", "gone again"))
    assert second.slug == "rear"
    assert queue.try_get() is None

    # And the two publish-side causes share a level, as they did.
    queue.put(_state(CAMERA_STATE_CAUSE_LIVE_LOST, ("live_unavailable", "x")))
    queue.put(_state(CAMERA_STATE_CAUSE_CONFIG_CHANGE))
    (only,) = [queue.try_get()]
    assert only.cause == CAMERA_STATE_CAUSE_CONFIG_CHANGE and queue.try_get() is None
