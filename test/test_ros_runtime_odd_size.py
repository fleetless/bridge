# SPDX-License-Identifier: Apache-2.0
"""A hardware `live.encoder` and a camera of odd size: the live start is
refused before anything is built.

With any `live.encoder` but `software` the bridge sends H.264 only, and
H.264 as it sends it takes no odd width or height. The cloud refuses such a
document; this is the bridge's own answer where the setting came from the
ROS parameter, or the document from a cloud that does not know the rule.
"""
import asyncio
import logging

import pytest
from helpers import by_slug
from test_ros_runtime import (
    _UNLIMITED_BUDGET,
    _camera_cfg,
    _drain_camera_states,
    _pull_snapshot,
    _split_snapshot_frame,
    _start_image_publisher,
    run,
)

from test_ros_runtime_passthrough import compressed_video  # noqa: F401 - a fixture, used by name

from fleetless_bridge import live_encoder
from fleetless_bridge.live_settings import LiveSettings

HARDWARE = ["auto", "nvenc", "vaapi", "v4l2"]


def _message(setting, width, height):
    return (
        "With live.encoder: {} the bridge sends H.264, which takes no odd size: set an "
        "even width and height instead of {}x{}, or live.encoder: software."
    ).format(setting, width, height)


class _Publisher:
    """What `start_live` builds, recording what it was handed."""

    built = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        _Publisher.built.append(self)

    async def start(self, url, room, token):
        pass

    async def stop(self):
        pass

    def set_bitrate_kbps(self, kbps):
        pass


class _Warnings(logging.Handler):
    """The runtime's warnings; `caplog` does not reach this logger here (see
    `test_ros_runtime_low_bandwidth._refusal_messages`)."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@pytest.fixture
def publishers():
    _Publisher.built = []
    live_encoder.forget_chains()
    yield _Publisher.built
    for publisher in _Publisher.built:
        encoder = publisher.kwargs.get("encoder")
        if encoder is not None:
            encoder.close()
    live_encoder.forget_chains()


@pytest.fixture
def warnings():
    handler = _Warnings()
    logger = logging.getLogger("fleetless_bridge.ros_runtime")
    logger.addHandler(handler)
    try:
        yield handler.messages
    finally:
        logger.removeHandler(handler)


@pytest.fixture
def builds(monkeypatch):
    """Every `build_encoder` call `start_live` makes."""
    calls = []
    real = live_encoder.build_encoder

    def recording(setting, **kwargs):
        calls.append(setting)
        return real(setting, **kwargs)

    monkeypatch.setattr(live_encoder, "build_encoder", recording)
    return calls


def _odd_size_warnings(messages):
    return [m for m in messages if "takes no odd size" in m]


@pytest.mark.parametrize("setting", HARDWARE)
@pytest.mark.parametrize("width,height", [(641, 480), (640, 481), (641, 481)])
def test_a_hardware_setting_refuses_the_live_start_of_a_camera_of_odd_size(
    publishers, builds, warnings, setting, width, height
):
    jetson = live_encoder.CHAINS["nvenc"][0]

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=width, height=height)]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder=setting))
        # Something learned about the chains before, which the refusal must
        # leave as it is: the chip is not at fault.
        live_encoder.MEMORY.cannot_be_built(jetson, "GStreamer has no element 'nvvidconv'")
        before = live_encoder.MEMORY.candidates(live_encoder.chains_for("auto"), (width, height))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        after = live_encoder.MEMORY.candidates(live_encoder.chains_for("auto"), (width, height))
        return await _drain_camera_states(rt), dict(rt._live_publishers), before, after

    states, live_publishers, before, after = run(body, live_publisher_factory=_Publisher)
    assert len(states) == 1
    state = states[0]
    assert state.slug == "front" and state.publishing is False
    assert state.error == ("live_unavailable", _message(setting, width, height))
    assert state.cause == "command" and state.request_id == "req-1"
    # Nothing was built: no publisher, no encoder, no pipeline.
    assert publishers == [] and live_publishers == {}
    assert builds == []
    assert before == after and after[1] == {"nvenc/jetson": "nvenc/jetson cannot be built: GStreamer has no element 'nvvidconv'"}
    assert _odd_size_warnings(warnings) == ["Camera 'front': " + _message(setting, width, height)]


def test_a_camera_of_even_size_beside_it_starts_with_its_encoder(publishers, builds):
    async def body(rt):
        await rt.apply_cameras(by_slug([
            _camera_cfg("front", width=641, height=481),
            _camera_cfg("rear", topic="/rear", width=640, height=480),
        ]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        await rt.start_live("rear", "ws://livekit.invalid", "room", "token", "req-2")
        return await _drain_camera_states(rt), sorted(rt._live_publishers)

    states, live_slugs = run(body, live_publisher_factory=_Publisher)
    assert live_slugs == ["rear"]
    assert [p.kwargs["slug"] for p in publishers] == ["rear"]
    assert type(publishers[0].kwargs["encoder"]) is live_encoder.HardwareH264Encoder
    assert builds == ["nvenc"]
    by = {state.slug: state for state in states}
    assert by["front"].error[0] == "live_unavailable"
    assert by["rear"].publishing is True and by["rear"].error is None


@pytest.mark.parametrize("width,height", [(641, 480), (640, 481), (641, 481)])
def test_software_starts_a_camera_of_odd_size_as_before(publishers, warnings, width, height):
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=width, height=height)]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="software"))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        return await _drain_camera_states(rt), sorted(rt._live_publishers)

    states, live_slugs = run(body, live_publisher_factory=_Publisher)
    assert live_slugs == ["front"]
    assert publishers[0].kwargs["encoder"] is None
    assert states[-1].publishing is True and states[-1].error is None
    assert _odd_size_warnings(warnings) == []


def test_the_warning_is_logged_once_per_setting_and_size_not_per_attempt(publishers, warnings):
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=641, height=480)]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        for attempt in range(3):
            await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-a{}".format(attempt))
        seen = [len(_odd_size_warnings(warnings))]
        # Another hardware setting: said again, once.
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="vaapi"))
        for attempt in range(2):
            await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-b{}".format(attempt))
        seen.append(len(_odd_size_warnings(warnings)))
        # Another odd size: said again, once.
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=641, height=481)]))
        for attempt in range(2):
            await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-c{}".format(attempt))
        seen.append(len(_odd_size_warnings(warnings)))
        states = await _drain_camera_states(rt)
        return seen, states

    seen, states = run(body, live_publisher_factory=_Publisher)
    assert seen == [1, 2, 3]
    assert _odd_size_warnings(warnings) == [
        "Camera 'front': " + _message("nvenc", 641, 480),
        "Camera 'front': " + _message("vaapi", 641, 480),
        "Camera 'front': " + _message("vaapi", 641, 481),
    ]
    # Every attempt is answered, though; only the log is spared.
    assert publishers == []
    assert states[-1].error == ("live_unavailable", _message("vaapi", 641, 481))


def test_a_start_that_passes_forgets_the_warning(publishers, warnings):
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=641, height=480)]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="software"))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-2")
        await rt.stop_live("front", request_id="req-3")
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-4")

    run(body, live_publisher_factory=_Publisher)
    assert len(_odd_size_warnings(warnings)) == 2
    assert len(publishers) == 1 and publishers[0].kwargs["encoder"] is None


def test_a_camera_made_even_starts_at_the_next_attempt(publishers):
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=641, height=481)]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="auto"))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
        refused = await _drain_camera_states(rt)
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=640, height=480)]))
        await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-2")
        return refused, await _drain_camera_states(rt), sorted(rt._live_publishers)

    refused, states, live_slugs = run(body, live_publisher_factory=_Publisher)
    assert refused[-1].error[0] == "live_unavailable"
    assert live_slugs == ["front"]
    assert states[-1].publishing is True and states[-1].request_id == "req-2"
    assert type(publishers[0].kwargs["encoder"]) is live_encoder.HardwareH264Encoder


def test_the_snapshots_of_a_refused_camera_go_on(publishers):
    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=64, height=48, hz=30)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=33, height=24, snapshot_interval_seconds=1)]
            ))
            rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
            await rt.start_live("front", "ws://livekit.invalid", "room", "token", "req-1")
            states = await _drain_camera_states(rt)
            return states, await _pull_snapshot(rt, _UNLIMITED_BUDGET)
        finally:
            stop_pub()

    states, wire = run(body, live_publisher_factory=_Publisher)
    assert any(state.error and state.error[0] == "live_unavailable" for state in states)
    header, _ = _split_snapshot_frame(wire)
    assert (header["slug"], header["width"], header["height"]) == ("front", 33, 24)
    assert publishers == []


def test_a_passthrough_camera_of_odd_size_is_not_refused_here(compressed_video, publishers, warnings):
    """`live.encoder` does not apply to a camera whose H.264 is passed
    through: nothing encodes it. Whatever is wrong with its size is the
    passthrough's own finding (its stream must have the configured size),
    not this refusal. A `CompressedVideo` camera passes through from the
    start, before any message has said so."""
    from test_ros_runtime_passthrough import _h264_camera

    from fleetless_bridge.ros_runtime import COMPRESSED_VIDEO_TYPE

    async def body(rt):
        await rt.apply_cameras(by_slug([_h264_camera("video", type_=COMPRESSED_VIDEO_TYPE, width=641, height=481)]))
        rt.set_live_settings(LiveSettings(max_packet_bytes=1200, encoder="nvenc"))
        passes_through = rt._cameras["video"].passes_through
        await rt.start_live("video", "ws://livekit.invalid", "room", "token", "req-1")
        return passes_through, await _drain_camera_states(rt), sorted(rt._live_publishers)

    passes_through, states, live_slugs = run(body, live_publisher_factory=_Publisher)
    assert passes_through is True
    assert live_slugs == ["video"]
    assert "passthrough" in publishers[0].kwargs and "encoder" not in publishers[0].kwargs
    assert not any(state.error and "takes no odd size" in state.error[1] for state in states)
    assert _odd_size_warnings(warnings) == []
