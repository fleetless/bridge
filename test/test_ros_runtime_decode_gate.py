# SPDX-License-Identifier: Apache-2.0
"""Decode gate for the `kind: 'ros'` camera adapter: conversion
(imgmsg -> BGR) runs only when a live stream is active or a snapshot pull is
due — otherwise the callback keeps just a reference to the newest raw
message (`entry.raw`, camera.py's `RawFrameHolder`) and never decodes it.
Reuses `test_ros_runtime.py`'s harness (`run`, `_camera_cfg`,
`_start_image_publisher`, `_pull_snapshot`, `_fake_live_factory`,
`_split_snapshot_frame`) rather than duplicating it.

Every claim is proven by counting real calls to `camera.to_bgr`,
monkeypatched on the shared module object (`from fleetless_bridge import
camera`) so the patch is visible to `ros_runtime.py` regardless of which
module holds its own reference. The wrapper still delegates to real
conversion — stubbing it out entirely would make "no calls" trivially true
for the wrong reason (nothing could ever convert) — so several tests below
also confirm frames genuinely arrived (the raw slot is non-empty) or that a
real, correctly decoded image came back.
"""
import asyncio
import logging
import time

import numpy as np
from conftest import wait_until
from cv_bridge import CvBridge
from helpers import by_slug

from fleetless_bridge import camera
from fleetless_bridge.live import CameraTrack, LivePublisher
from fleetless_bridge.ros_runtime import RosRuntime
from test_live import _FakeConnection, _FakeSender, _FakeSignal, _FakeTrack, _quality
from test_ros_runtime import (
    _camera_cfg,
    _drain_camera_states,
    _fake_live_factory,
    _pull_snapshot,
    _split_snapshot_frame,
    _start_image_publisher,
    run,
)


def _counting_to_bgr(monkeypatch, *, delay: float = 0.0):
    """Wraps `camera.to_bgr` to count calls without changing its behaviour.
    `delay`, when given, sleeps before delegating — used by the capture-time
    test so an "encode time instead of capture time" bug shows up as a
    large, unmissable gap between the reported timestamp and when
    conversion actually finishes."""
    original = camera.to_bgr
    calls = []

    def wrapper(msg):
        if delay:
            time.sleep(delay)
        calls.append(msg)
        return original(msg)

    monkeypatch.setattr(camera, "to_bgr", wrapper)
    return calls


def test_idle_camera_converts_nothing_while_frames_arrive(monkeypatch):
    """Core claim: with a real, fast-publishing source and nobody watching
    (no live publisher, no snapshot pulled), not a single frame gets
    decoded. Break: drop the `conversion_wanted` gate in
    `_RosSourceAdapter`'s callback (convert unconditionally) — turns this
    red on the count alone."""
    calls = None

    async def body(rt):
        nonlocal calls
        calls = _counting_to_bgr(monkeypatch)
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=30)
        try:
            await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12)]))
            # Long enough for several 30Hz frames to arrive and land raw —
            # not a wait for conversion, since none should happen here.
            await asyncio.sleep(0.3)
        finally:
            stop_pub()
        # Proves frames arrived without touching `to_bgr`: the raw slot
        # holds a real, unconverted message. Without this, a publisher
        # that silently never connected could pass the count assertion
        # below for the wrong reason.
        return rt._cameras["front"].raw.take()

    raw = run(body)
    assert raw is not None  # frames really did arrive
    assert calls == []  # and none of them were ever decoded


def test_snapshot_pull_converts_the_latest_raw_frame_once(monkeypatch):
    """A due snapshot pull is the one thing idle conversion is *for* —
    exactly one conversion for the frame that gets pulled, not one per
    frame arriving at source rate while the pull is pending."""
    calls = None

    async def body(rt):
        nonlocal calls
        calls = _counting_to_bgr(monkeypatch)
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=30)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, snapshot_interval_seconds=1)]
            ))
            return await _pull_snapshot(rt)
        finally:
            stop_pub()

    wire = run(body)
    header, image_bytes = _split_snapshot_frame(wire)
    assert header["slug"] == "front"
    assert len(image_bytes) > 0
    assert len(calls) == 1  # one raw frame, converted at most once


def test_live_start_switches_to_eager_conversion(monkeypatch):
    """Once a live publish task runs for a slug, every accepted frame
    converts as it arrives — no snapshot pull needed. Proven by the count
    climbing on its own once `start_live` completes, after first
    reconfirming the idle-converts-nothing claim above, so this test's own
    setup can't be mistaken for what makes it pass."""
    calls = None

    async def body(rt):
        nonlocal calls
        calls = _counting_to_bgr(monkeypatch)
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=30)
        try:
            await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=30)]))
            await asyncio.sleep(0.1)
            assert calls == []  # idle so far — same claim the test above makes

            await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
            wait_until(lambda: "front" in rt._live_publishers)

            wait_until(lambda: len(calls) >= 3, timeout=2.0)
        finally:
            stop_pub()

    run(body, live_publisher_factory=_fake_live_factory())
    assert len(calls) >= 3  # ongoing eager conversion, not a one-off


def test_timestamp_is_capture_time_from_the_raw_path(monkeypatch):
    """`timestamp_ms` must be the raw frame's own arrival stamp — read in
    the ROS callback — never the moment `_convert_due_raw_frame` decodes
    it, which can be arbitrarily later for a frame that sat raw a while.
    Break: stamp it at encode time (read after `camera.to_bgr` returns
    instead of carrying the raw frame's own stamp through) — turns this
    red, landing after the artificial encode delay below instead of
    before it."""

    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=30)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, snapshot_interval_seconds=1)]
            ))
            # Several 30Hz frames land raw, unconverted, before to_bgr is
            # patched below — their capture timestamps are fixed before
            # the delay exists.
            await asyncio.sleep(0.15)
            _counting_to_bgr(monkeypatch, delay=0.3)

            before_pull = int(time.time() * 1000)
            wire = await _pull_snapshot(rt)
            after_pull = int(time.time() * 1000)
            return wire, before_pull, after_pull
        finally:
            stop_pub()

    wire, before_pull, after_pull = run(body)
    header, _ = _split_snapshot_frame(wire)
    # Captured around the start of the pull, well before the 300ms encode
    # delay finishes — an encode-time bug would land this at or after
    # `after_pull`.
    #
    # One publish interval of slack, and it is not padding: the publisher
    # above keeps running at 30Hz through the pull, so a frame that lands
    # 8ms after `before_pull` is read carries a capture stamp 8ms later —
    # still a capture stamp, and still correct. A bare `< before_pull`
    # tests the publisher's cadence against a clock read, which it loses
    # roughly one run in three on a shared runner. The break this guards
    # against lands ~300ms out, so 100ms still separates them by a
    # factor of three.
    assert header["timestamp_ms"] < before_pull + 100
    assert after_pull - header["timestamp_ms"] > 250


# --- a live start: the newest message already held is the first frame ----------


def _image_msg(color, *, width=16, height=12, encoding="bgr8"):
    """A `sensor_msgs/msg/Image` of one solid BGR colour, built by hand and
    never published: a test that puts it into `entry.raw` itself knows no
    second message can arrive behind it."""
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    frame[:, :] = color
    msg = CvBridge().cv2_to_imgmsg(frame, encoding="bgr8")
    msg.encoding = encoding
    return msg


def _publisher_factory_with_fake_peers(tracks, signals=None):
    """The shipped `LivePublisher` and its own feed loop, joined to a fake
    room. `_fake_live_factory` would only show that `latest` was filled; what
    a viewer gets is what the publisher pushes to its track, and that is read
    here. `signals` collects each publisher's signal, for a test that plays
    the room's side: `signals[0].fire("on_quality_update", ...)`."""

    def factory(**kwargs):
        signal, track = _FakeSignal(), _FakeTrack()
        tracks.append(track)
        if signals is not None:
            signals.append(signal)

        def signal_factory(url, token, **callbacks):
            signal.callbacks = callbacks
            return signal

        return LivePublisher(
            signal_factory=signal_factory,
            connection_factory=lambda ice_servers: _FakeConnection(),
            track_factory=lambda width, height, fps, bitrate_kbps: track,
            **kwargs,
        )

    return factory


async def _eventually(condition, timeout=2.0):
    """`wait_until` for something the event loop itself has to do: that one
    sleeps the thread, and the publisher's feed loop runs on it."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never became true within {}s".format(timeout)
        await asyncio.sleep(0.01)


async def _executor_caught_up(rt):
    """Returns once everything queued on the executor thread before this call
    has run -- the queue is first in, first out."""
    await rt._submit_async(lambda: None)


def test_a_live_start_publishes_the_raw_message_already_held(monkeypatch):
    """An idle camera keeps its newest message unconverted. A live start used
    to leave it there and publish nothing until the next message had arrived
    and been converted: up to a whole camera interval of a viewer looking at
    nothing. Here no next message exists at all, and the picture goes out."""
    tracks = []

    async def body(rt):
        calls = _counting_to_bgr(monkeypatch)
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=50)]))
        entry = rt._cameras["front"]
        entry.raw.set(_image_msg((10, 20, 30)), 1_000)
        assert entry.latest.get() is None

        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _executor_caught_up(rt)
        latest = entry.latest.get()
        await _eventually(lambda: tracks[0].pushed)
        pushed = list(tracks[0].pushed)
        await rt.stop_live("front", request_id="req-2")
        return latest, pushed, len(calls)

    latest, pushed, conversions = run(
        body, live_publisher_factory=_publisher_factory_with_fake_peers(tracks)
    )
    assert latest is not None and latest.timestamp_ms == 1_000  # its own capture time
    assert conversions == 1
    # BGR (10, 20, 30) reaches the track as RGB, like every other frame.
    assert pushed[0].shape == (12, 16, 3)
    assert pushed[0][0, 0].tolist() == [30, 20, 10]


def test_a_live_start_leaves_a_raw_message_older_than_latest_unconverted(monkeypatch):
    """`latest` can be the newer of the two: a snapshot converted a message,
    and what sits in the raw slot now is older than that. Converting it would
    put the older picture in front of the newer one."""
    tracks = []

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=50)]))
        entry = rt._cameras["front"]
        newer = np.zeros((12, 16, 3), dtype=np.uint8)
        newer[:, :] = (1, 2, 3)
        rt._on_source_frame("front", entry, newer, 2_000)
        entry.raw.set(_image_msg((200, 200, 200)), 1_000)
        calls = _counting_to_bgr(monkeypatch)

        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _executor_caught_up(rt)
        await _eventually(lambda: tracks[0].pushed)
        pushed = list(tracks[0].pushed)
        await rt.stop_live("front", request_id="req-2")
        return entry.latest.get(), pushed, len(calls), entry.raw.take()

    latest, pushed, conversions, left_raw = run(
        body, live_publisher_factory=_publisher_factory_with_fake_peers(tracks)
    )
    assert conversions == 0
    assert latest.timestamp_ms == 2_000
    assert [frame[0, 0].tolist() for frame in pushed] == [[3, 2, 1]]
    assert left_raw is None  # looked at once, and not kept for a second look


def test_a_raw_message_that_cannot_be_converted_does_not_fail_the_start():
    """The start goes ahead and the first picture is the next message's, as
    it always was. Said in the log, by the line a bad frame already gets."""
    tracks = []

    class _Capture(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.DEBUG)
            self.messages = []

        def emit(self, record):
            self.messages.append(record.getMessage())

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=50)]))
        entry = rt._cameras["front"]
        entry.raw.set(_image_msg((10, 20, 30), encoding="not-an-encoding"), 1_000)
        await _drain_camera_states(rt)

        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _executor_caught_up(rt)
        states = await _drain_camera_states(rt)
        await asyncio.sleep(0.1)  # several feed ticks
        pushed = list(tracks[0].pushed)
        await rt.stop_live("front", request_id="req-2")
        return states, pushed, entry.latest.get()

    handler = _Capture()
    watched = logging.getLogger("fleetless_bridge.ros_runtime")
    watched.addHandler(handler)
    try:
        states, pushed, latest = run(
            body, live_publisher_factory=_publisher_factory_with_fake_peers(tracks)
        )
    finally:
        watched.removeHandler(handler)
    assert [(s.publishing, s.error, s.request_id) for s in states] == [(True, None, "req-1")]
    assert pushed == [] and latest is None
    assert len([m for m in handler.messages if "Could not convert a camera frame" in m]) == 1


def test_an_unexpected_conversion_failure_at_a_start_is_logged_and_not_raised(monkeypatch):
    """The conversion is queued and nobody waits for its result, so an
    exception that escaped it would be lost without a line anywhere."""

    class _Capture(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.DEBUG)
            self.messages = []

        def emit(self, record):
            self.messages.append(record.getMessage())

    def explode(msg):
        raise RuntimeError("decoder fell over")

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12)]))
        rt._cameras["front"].raw.set(_image_msg((10, 20, 30)), 1_000)
        monkeypatch.setattr(camera, "to_bgr", explode)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _executor_caught_up(rt)
        return "front" in rt._live_publishers

    handler = _Capture()
    watched = logging.getLogger("fleetless_bridge.ros_runtime")
    watched.addHandler(handler)
    try:
        live = run(body, live_publisher_factory=_fake_live_factory())
    finally:
        watched.removeHandler(handler)
    assert live is True
    assert len([m for m in handler.messages if "newest camera frame" in m and "front" in m]) == 1


def test_convert_newest_raw_frame_converts_on_request_and_only_what_is_newer(monkeypatch):
    """The entry point on its own, as anything else that makes conversion
    wanted would call it: no live start anywhere in this test."""

    async def body(rt):
        calls = _counting_to_bgr(monkeypatch)
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12)]))
        entry = rt._cameras["front"]

        entry.raw.set(_image_msg((10, 20, 30)), 1_000)
        rt.convert_newest_raw_frame("front").result(timeout=5.0)
        first = entry.latest.get()

        rt.convert_newest_raw_frame("front").result(timeout=5.0)  # nothing held now
        entry.raw.set(_image_msg((40, 50, 60)), 900)  # older than what `latest` has
        rt.convert_newest_raw_frame("front").result(timeout=5.0)
        return first, entry.latest.get(), len(calls)

    first, latest, conversions = run(body)
    assert first.timestamp_ms == 1_000
    assert first.bgr[0, 0].tolist() == [10, 20, 30]
    assert latest.timestamp_ms == 1_000
    assert conversions == 1


def test_a_held_message_converted_late_does_not_cost_the_cameras_next_message():
    """The frame limit lets one frame through per `1/fps`. Counted from the
    moment a held message was *converted*, a camera publishing at about
    `fps` loses the message after it: that one arrives a full interval after
    the held one did, and only a fraction of an interval after the
    conversion. The picture a viewer was just given would then stay up for
    two intervals. Counted from when the message arrived, both go through."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=10)]))
        entry = rt._cameras["front"]
        following = np.zeros((12, 16, 3), dtype=np.uint8)

        entry.raw.set(_image_msg((10, 20, 30)), 1_000)  # arrives now
        await asyncio.sleep(0.07)  # held for most of the 100 ms interval
        rt.convert_newest_raw_frame("front").result(timeout=5.0)
        first = entry.latest.get().timestamp_ms
        await asyncio.sleep(0.04)  # 110 ms after the held message arrived
        await rt._submit_async(lambda: rt._on_source_frame("front", entry, following, 1_110))
        return first, entry.latest.get().timestamp_ms

    first, then = run(body)
    assert first == 1_000
    assert then == 1_110


def test_the_frame_limit_still_holds_right_after_a_held_message():
    """The control for the test above: a message that really is too early --
    a few milliseconds behind the held one -- is still refused. At 2 fps, so
    that "a few milliseconds" stays far inside the interval on any machine."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=2)]))
        entry = rt._cameras["front"]
        following = np.zeros((12, 16, 3), dtype=np.uint8)

        entry.raw.set(_image_msg((10, 20, 30)), 1_000)
        rt.convert_newest_raw_frame("front").result(timeout=5.0)
        await rt._submit_async(lambda: rt._on_source_frame("front", entry, following, 1_005))
        return entry.latest.get().timestamp_ms

    assert run(body) == 1_000


def test_convert_newest_raw_frame_is_a_no_op_for_a_slug_without_a_camera():
    async def body(rt):
        return rt.convert_newest_raw_frame("nobody").result(timeout=5.0)

    assert run(body) is None


def test_convert_newest_raw_frame_before_the_runtime_has_started_is_a_no_op():
    """`main.py` builds the runtime before it starts the node. Asking that
    early used to queue the work and then fail on the guard condition that
    does not exist yet: an `AttributeError` for the caller, and an item left
    in the queue for whenever the executor first drained it."""
    rt = RosRuntime(node_name="test_convert_unstarted")
    try:
        future = rt.convert_newest_raw_frame("front")
        assert future.result(timeout=1.0) is None
        assert len(rt._work_queue) == 0
    finally:
        rt.stop()  # never started; must not raise


def test_convert_newest_raw_frame_after_the_runtime_has_stopped_is_a_no_op():
    """A publisher can resume, or a start can be dispatched, while the
    process is on its way down. The node is destroyed by then, and so is the
    guard condition `_enqueue` would trigger. Nothing is queued and nothing
    is triggered; the caller gets a future that is already done."""
    held = {}

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12)]))
        rt._cameras["front"].raw.set(_image_msg((10, 20, 30)), 1_000)
        held["rt"] = rt

    run(body)
    rt = held["rt"]  # stopped by the harness

    class _DestroyedGuard:
        triggered = 0

        def trigger(self):
            _DestroyedGuard.triggered += 1
            raise RuntimeError("the guard condition of a destroyed node")

    rt._guard = _DestroyedGuard()
    future = rt.convert_newest_raw_frame("front")
    assert future.result(timeout=1.0) is None
    assert _DestroyedGuard.triggered == 0
    assert len(rt._work_queue) == 0


def test_a_held_message_with_no_arrival_time_counts_as_arriving_when_it_is_converted():
    """A `RawFrame` that does not say when it arrived is rate-limited as
    arriving now, like every frame that comes with no arrival time. Read as
    "arrived when the monotonic clock started", it would never count against
    the frame limit at all, and the message right behind it would go through
    inside the same interval."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=2)]))
        entry = rt._cameras["front"]
        following = np.zeros((12, 16, 3), dtype=np.uint8)

        with entry.raw._lock:
            entry.raw._frame = camera.RawFrame(msg=_image_msg((10, 20, 30)), timestamp_ms=1_000)
        rt.convert_newest_raw_frame("front").result(timeout=5.0)
        await rt._submit_async(lambda: rt._on_source_frame("front", entry, following, 1_005))
        return entry.latest.get().timestamp_ms

    assert run(body) == 1_000


# --- a paused publisher needs no frames (fleetless/fleetless#1118) ----------
#
# "A publisher exists and nobody is subscribed" happens between a
# `camera_start` and the first viewer, and while a viewer's client has
# unsubscribed. The cloud can also keep a robot in its room after the last
# viewer (off unless configured), and then it is up to half a minute after
# every viewing. The publisher has always paused its encoder and its feed for
# it. The camera source had not: it converted every message for as long as a
# publisher existed.

_NOBODY = (False, False, False)
_SOMEBODY = (False, False, True)


def test_a_paused_publisher_leaves_the_cameras_messages_raw(monkeypatch):
    """With a publisher that nobody is subscribed to, messages arrive and not
    one is converted -- the claim the idle test above makes for a camera with
    no publisher at all, now for one whose publisher is paused. Break: read
    only `slug in self._live_publishers` in `conversion_wanted`."""
    tracks, signals = [], []

    async def body(rt):
        calls = _counting_to_bgr(monkeypatch)
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=30)
        try:
            await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=30)]))
            await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
            await _eventually(lambda: len(calls) >= 3)  # live and unpaused: every message converts

            signals[0].fire("on_quality_update", _quality(*_NOBODY))
            await _executor_caught_up(rt)  # a message already in its callback finishes
            entry = rt._cameras["front"]
            entry.raw.take()
            at_pause = len(calls)
            await asyncio.sleep(0.3)  # some nine messages at 30 Hz
            held = entry.raw.take()
            during_pause = len(calls) - at_pause

            signals[0].fire("on_quality_update", _quality(*_SOMEBODY))
            await _eventually(lambda: len(calls) >= at_pause + 3)  # and eager again once somebody watches
            await rt.stop_live("front", request_id="req-2")
            return held, during_pause
        finally:
            stop_pub()

    held, during_pause = run(
        body, live_publisher_factory=_publisher_factory_with_fake_peers(tracks, signals)
    )
    assert held is not None  # messages really did keep arriving
    assert during_pause == 0  # and none of them was decoded


def test_a_resume_converts_the_newest_held_message_before_the_next_one_arrives(monkeypatch):
    """No next message exists in this test at all. The viewer who comes back
    gets the newest picture the camera had sent, converted on the resume, and
    not whatever `latest` held from before the pause until the camera's next
    message. Break: drop `on_resume` from the publisher `start_live` builds."""
    tracks, signals = [], []

    async def body(rt):
        calls = _counting_to_bgr(monkeypatch)
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=50)]))
        entry = rt._cameras["front"]
        entry.raw.set(_image_msg((10, 20, 30)), 1_000)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _executor_caught_up(rt)
        await _eventually(lambda: tracks[0].pushed)  # the start's own first picture

        signals[0].fire("on_quality_update", _quality(*_NOBODY))
        # The frame limit counts between arrivals, and lets a message through
        # only a whole frame interval (20 ms at 50 fps) after the one before
        # it. Without this wait the gap was whatever the start happened to
        # take: about one feed tick when the feed looked first, 2 ms when the
        # start's conversion landed first -- and then the held message below
        # was refused, rightly, and this test failed for it. A sleep only
        # gets longer on a busy machine.
        await asyncio.sleep(0.05)
        entry.raw.set(_image_msg((40, 50, 60)), 2_000)  # arrives while nobody watches
        await _executor_caught_up(rt)
        while_paused = (entry.latest.get().timestamp_ms, len(calls), len(tracks[0].pushed))

        signals[0].fire("on_quality_update", _quality(*_SOMEBODY))
        await _executor_caught_up(rt)
        latest = entry.latest.get()
        await _eventually(lambda: len(tracks[0].pushed) >= 2)
        pushed = list(tracks[0].pushed)
        await rt.stop_live("front", request_id="req-2")
        return while_paused, latest, pushed, len(calls)

    while_paused, latest, pushed, conversions = run(
        body, live_publisher_factory=_publisher_factory_with_fake_peers(tracks, signals)
    )
    assert while_paused == (1_000, 1, 1)  # held, not converted, nothing pushed
    assert latest.timestamp_ms == 2_000  # its own capture time
    assert conversions == 2
    assert [frame[0, 0].tolist() for frame in pushed] == [[30, 20, 10], [60, 50, 40]]


def test_snapshots_keep_working_while_the_publisher_is_paused(monkeypatch):
    """A snapshot is due whether or not anyone watches live, and a paused
    publisher changes nothing about that: the dueness walk converts the one
    message it sends, as it does for a camera with no publisher."""
    tracks, signals = [], []

    async def body(rt):
        calls = _counting_to_bgr(monkeypatch)
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=30)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, fps=30, snapshot_interval_seconds=1)]
            ))
            await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
            signals[0].fire("on_quality_update", _quality(*_NOBODY))
            await _executor_caught_up(rt)
            await _pull_snapshot(rt)  # whatever was due from before the pause
            rt._cameras["front"].raw.take()
            before = len(calls)

            await asyncio.sleep(1.1)  # the next one falls due, with the publisher still paused
            wire = await _pull_snapshot(rt)
            conversions = len(calls) - before
            await rt.stop_live("front", request_id="req-2")
            return wire, conversions
        finally:
            stop_pub()

    wire, conversions = run(
        body, live_publisher_factory=_publisher_factory_with_fake_peers(tracks, signals)
    )
    header, image_bytes = _split_snapshot_frame(wire)
    assert header["slug"] == "front"
    assert len(image_bytes) > 0
    assert conversions == 1  # the one frame the snapshot sent, not thirty a second


class _PullingSender(_FakeSender):
    """Pulls one frame from the track each time it is handed one, as aiortc's
    sender does the moment it has a track: `recv()` returns at once after a
    pause, because the pacing interval is long over."""

    def __init__(self, pulled):
        super().__init__()
        self._pulled = pulled
        self.pulls = []

    def pull(self, track):
        self.pulls.append(asyncio.ensure_future(self._pull(track)))

    def replaceTrack(self, track):
        super().replaceTrack(track)
        if track is not None:
            self.pull(track)

    async def _pull(self, track):
        frame = await track.recv()
        self._pulled.append(frame.to_ndarray(format="rgb24")[0, 0].tolist())


class _PullingConnection(_FakeConnection):
    def addTrack(self, track):
        sender = super().addTrack(track)
        sender.pull(track)
        return sender


def _publisher_factory_with_a_pulling_sender(pulled, signals):
    """The shipped `LivePublisher` with the shipped `CameraTrack`: what a
    viewer is sent is what the sender pulls from that track, and that is
    recorded here."""

    def factory(**kwargs):
        signal = _FakeSignal()
        signals.append(signal)

        def signal_factory(url, token, **callbacks):
            signal.callbacks = callbacks
            return signal

        connection = _PullingConnection(sender=_PullingSender(pulled))
        return LivePublisher(
            signal_factory=signal_factory,
            connection_factory=lambda ice_servers: connection,
            track_factory=lambda width, height, fps, bitrate_kbps: CameraTrack(width, height, fps),
            **kwargs,
        )

    return factory


def test_the_first_frame_after_a_resume_is_the_held_message_not_the_one_from_before_the_pause():
    """The viewer who comes back sees the newest picture the camera has, at
    once. The track keeps the last frame it was given, and the sender pulls
    the moment it has the track back: handed back straight away, the track
    gave out the picture from before the pause, as the resume's keyframe, and
    the held message followed a frame interval later -- half a second at
    2 fps. So the track goes back only when the held message is on it.

    At 5 fps, with a pause longer than a frame interval: both the frame
    limit and the track's pacing are out of the way."""
    pulled, signals = [], []

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=16, height=12, fps=5)]))
        entry = rt._cameras["front"]
        entry.raw.set(_image_msg((10, 20, 30)), 1_000)  # before the start
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _eventually(lambda: len(pulled) >= 1)  # the start's picture

        signals[0].fire("on_quality_update", _quality(*_NOBODY))
        await asyncio.sleep(0.4)
        entry.raw.set(_image_msg((40, 50, 60)), 2_000)  # held while nobody watches
        signals[0].fire("on_quality_update", _quality(*_SOMEBODY))
        await _eventually(lambda: len(pulled) >= 2)
        after_resume = list(pulled)
        await rt.stop_live("front", request_id="req-2")
        return after_resume

    after_resume = run(body, live_publisher_factory=_publisher_factory_with_a_pulling_sender(pulled, signals))
    # BGR (10, 20, 30) and (40, 50, 60), as RGB.
    assert after_resume == [[30, 20, 10], [60, 50, 40]]
