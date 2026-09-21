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
import time

from conftest import wait_until
from helpers import by_slug

from fleetless_bridge import camera
from test_ros_runtime import (
    _camera_cfg,
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
