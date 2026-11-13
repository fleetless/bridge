# SPDX-License-Identifier: Apache-2.0
"""`HardwareH264Encoder`: the encoder a live session gets when `live.encoder`
is not `software`.

Three things are its own and are tested here: which chain it picks and what
it remembers about the ones that failed, the fallback to aiortc's libx264
inside the same object, and the bitrate rule. The pipeline it drives is
`test_live_encoder_pipeline.py`'s subject, the seam it is installed through
is `test_live_encoder_seam.py`'s.

Where the question is what comes out, the real pipeline runs over the
software stand-in, and at the end of the file the real sender of this
distribution's aiortc sends it to a second peer connection. Where the
question is which chain gets built, a pipeline factory records the answer.
"""
import asyncio
import json
import logging
import pathlib
import subprocess
import sys
import threading
import time

import av
import numpy as np
import pytest
from aiortc.codecs import h264
from aiortc.codecs.h264 import H264Encoder

from aiortc.rtp import is_rtcp

from fleetless_bridge import live, live_encoder
from fleetless_bridge.live import ENCODER_CLAMP_BPS, SENDER_ENCODER_ATTR, LiveStartError
from fleetless_bridge.protocol import LIVE_DEFAULTS
from fleetless_bridge.live_encoder import (
    CHAINS,
    MAX_BITRATE_BPS,
    MIN_BITRATE_BPS,
    STAND_IN,
    ChainFailed,
    ChainMemory,
    HardwareH264Encoder,
    Pipeline,
    build_encoder,
    chains_for,
    forget_chains,
)
from test_live import _FakeSignal, _FakeTrack, _publisher, _turn_failure, _turn_join
from test_live_encoder_pipeline import (
    FPS,
    HEIGHT,
    NAL_IDR,
    NAL_PPS,
    NAL_SLICE,
    NAL_SPS,
    WIDTH,
    _variant,
    moving_frame,
    moving_picture,
)
from test_live_encoder_seam import (
    NAL_FU_A,
    _loopback,
    _SeamConnection,
    _until,
)

JETSON = CHAINS["nvenc"][0]


@pytest.fixture(autouse=True)
def _a_process_that_has_learned_nothing():
    """The memory of the chains is the process's. No test starts with what
    another one learned, and none leaves anything behind."""
    forget_chains()
    yield
    forget_chains()


class _Logged(logging.Handler):
    """What `fleetless_bridge.live_encoder` logs, on a handler of the
    test's own (pytest disables propagation for the loggers here)."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append((record.levelno, record.getMessage()))

    def at(self, level):
        return [message for levelno, message in self.records if levelno == level]


@pytest.fixture
def logged():
    handler = _Logged()
    watched = logging.getLogger("fleetless_bridge.live_encoder")
    previous = watched.level
    watched.addHandler(handler)
    watched.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        watched.removeHandler(handler)
        watched.setLevel(previous)


class _Recording(HardwareH264Encoder):
    """The encoder, with a record of what each frame became: whether a
    keyframe was asked for, and the NAL unit types that came out."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []
        self.units = []

    def _units(self, frame, force_keyframe):
        units = super()._units(frame, force_keyframe)
        self.calls.append((force_keyframe, [unit[0] & 0x1F for unit in units]))
        self.units.append(units)
        return units


def _stand_in(cls=_Recording, chains=(STAND_IN,), backend="nvenc", **kwargs):
    kwargs.setdefault("fps", FPS)
    kwargs.setdefault("memory", ChainMemory())
    return cls(backend, chains, **kwargs)


def _decodes(units_per_frame):
    """How many pictures aiortc's own decoder library makes of each frame's
    NAL units, as one stream."""
    decoder = av.CodecContext.create("h264", "r")
    return [
        len(decoder.decode(av.Packet(b"".join(b"\x00\x00\x00\x01" + unit for unit in units))))
        for units in units_per_frame
    ]


# --- a pipeline factory that only records ---------------------------------------

_CANNED_UNIT = (
    b"\x00\x00\x00\x01\x67sps" + b"\x00\x00\x00\x01\x68pps" + b"\x00\x00\x00\x01\x65picture"
)


class _FakePipeline:
    def __init__(self, chain, *, width, height, bitrate_bps, fail_encode=None, fail_after=None):
        self.chain = chain
        self.size = (width, height)
        self.bitrate_bps = bitrate_bps
        self.bitrates = []
        self.frames = 0
        self.close_calls = 0
        self._fail_encode = fail_encode
        self._fail_after = fail_after

    def encode(self, frame, force_keyframe):
        self.frames += 1
        if self._fail_encode is not None:
            raise ChainFailed(self._fail_encode)
        if self._fail_after is not None and self.frames > self._fail_after:
            raise ChainFailed("the chip gave up")
        return _CANNED_UNIT

    def set_bitrate(self, bitrate_bps):
        self.bitrate_bps = bitrate_bps
        self.bitrates.append(bitrate_bps)

    def close(self):
        self.close_calls += 1


class _Factory:
    """Builds `_FakePipeline`s and says which chains it was asked for.
    `outcomes` maps a chain's name to what happens: `works`, a reason it
    cannot be built for, `silent`, or `works-then-fails`. Anything not named
    cannot be built."""

    def __init__(self, outcomes=None):
        self.outcomes = dict(outcomes or {})
        self.asked = []
        self.built = []

    def __call__(self, chain, *, width, height, fps, bitrate_bps, frame_timeout_s):
        self.asked.append(chain.name)
        outcome = self.outcomes.get(chain.name, "GStreamer has no element {!r}".format(chain.encoder))
        kwargs = {}
        if outcome == "silent":
            kwargs["fail_encode"] = "no access unit within 1 s of a frame"
        elif outcome == "works-then-fails":
            kwargs["fail_after"] = 3
        elif outcome != "works":
            raise ChainFailed(outcome)
        pipeline = _FakePipeline(chain, width=width, height=height, bitrate_bps=bitrate_bps, **kwargs)
        self.built.append(pipeline)
        return pipeline


# --- selection ---------------------------------------------------------------------


def test_software_builds_no_encoder_at_all():
    """`software` is aiortc's own encoder, VP8 or H.264 as negotiated: the
    publisher is handed nothing, so it sets nothing and restricts nothing."""
    assert build_encoder("software", fps=15) is None


@pytest.mark.parametrize("setting", ["auto", "nvenc", "vaapi", "v4l2"])
def test_every_other_setting_builds_the_encoder_over_its_chains_and_nothing_else_yet(setting):
    encoder = build_encoder(setting, fps=2)
    try:
        assert type(encoder) is HardwareH264Encoder and isinstance(encoder, H264Encoder)
        assert encoder._chains == chains_for(setting) and encoder._chains
        assert encoder._fps == 2
        # No pipeline, no probe and no `gi` before the first frame.
        assert encoder.chain is None and not encoder.software
    finally:
        encoder.close()


def test_auto_tries_nvenc_then_vaapi_then_v4l2_and_then_software(logged):
    factory = _Factory()
    encoder = _stand_in(chains=chains_for("auto"), backend="auto", pipeline_factory=factory)
    payloads, _ = encoder.encode(moving_frame(0), False)
    encoder.close()
    assert factory.asked == ["nvenc/jetson", "nvenc/desktop", "vaapi/va", "vaapi/vaapi", "v4l2"]
    assert encoder.software and payloads
    warnings = logged.at(logging.WARNING)
    assert len(warnings) == 1
    assert warnings[0].startswith("live.encoder=auto: no hardware encoder could be used (")
    for name, element in [
        ("nvenc/jetson", "nvv4l2h264enc"), ("nvenc/desktop", "nvh264enc"),
        ("vaapi/va", "vah264enc"), ("vaapi/vaapi", "vaapih264enc"), ("v4l2", "v4l2h264enc"),
    ]:
        assert "{}: GStreamer has no element '{}'".format(name, element) in warnings[0]
    assert "software H.264" in warnings[0]


def test_the_first_chain_that_encodes_the_first_frame_carries_the_session(logged):
    factory = _Factory({"nvenc/desktop": "works", "vaapi/va": "works"})
    encoder = _stand_in(chains=chains_for("auto"), backend="auto", pipeline_factory=factory)
    for index in range(4):
        encoder.encode(moving_frame(index), False)
    assert factory.asked == ["nvenc/jetson", "nvenc/desktop"]  # built once, and nothing after it
    assert encoder.chain.name == "nvenc/desktop" and not encoder.software
    assert factory.built[0].frames == 4
    assert logged.at(logging.WARNING) == []
    assert logged.at(logging.INFO) == ["live.encoder=auto: encoding with nvenc/desktop"]
    encoder.close()
    assert factory.built[0].close_calls == 1 and encoder.chain is None


def test_a_chain_that_builds_and_returns_nothing_is_a_failed_probe_and_the_next_is_tried():
    factory = _Factory({"nvenc/jetson": "silent", "nvenc/desktop": "works"})
    encoder = _stand_in(chains=CHAINS["nvenc"], pipeline_factory=factory)
    encoder.encode(moving_frame(0), False)
    assert factory.asked == ["nvenc/jetson", "nvenc/desktop"]
    assert factory.built[0].close_calls == 1  # the one that failed is not left running
    assert encoder.chain.name == "nvenc/desktop"
    encoder.close()


# --- what the process remembers -------------------------------------------------


def test_a_chain_that_failed_is_not_built_again_in_the_same_process(logged):
    memory = ChainMemory()
    first = _Factory({"nvenc/desktop": "works"})
    encoder = _stand_in(chains=CHAINS["nvenc"], memory=memory, pipeline_factory=first)
    encoder.encode(moving_frame(0), False)
    encoder.close()
    assert first.asked == ["nvenc/jetson", "nvenc/desktop"]

    second = _Factory({"nvenc/desktop": "works"})
    encoder = _stand_in(chains=CHAINS["nvenc"], memory=memory, pipeline_factory=second)
    encoder.encode(moving_frame(0), False)
    encoder.close()
    assert second.asked == ["nvenc/desktop"]
    assert memory.working == "nvenc/desktop"
    assert logged.at(logging.WARNING) == []


def test_with_every_chain_known_bad_nothing_is_probed_and_the_session_still_says_why(logged):
    memory = ChainMemory()
    _stand_in(chains=CHAINS["nvenc"], memory=memory, pipeline_factory=_Factory()).encode(
        moving_frame(0), False
    )
    assert len(logged.at(logging.WARNING)) == 1

    again = _Factory()
    encoder = _stand_in(chains=CHAINS["nvenc"], memory=memory, pipeline_factory=again)
    payloads, _ = encoder.encode(moving_frame(0), False)
    encoder.close()
    assert again.asked == []  # a missing chip is not looked for at every start
    assert payloads and encoder.software
    warnings = logged.at(logging.WARNING)
    assert len(warnings) == 2
    assert "nvenc/jetson: GStreamer has no element 'nvv4l2h264enc' (known from an earlier start)" in warnings[1]
    assert "not tried again until live.encoder changes or the bridge restarts" in warnings[1]


def test_forgetting_the_chains_makes_the_next_start_probe_again():
    """What the runtime does when `live.encoder` changes. The default memory
    is the process's, so this one test uses it."""
    factory = _Factory()
    encoder = HardwareH264Encoder("nvenc", CHAINS["nvenc"], fps=FPS, pipeline_factory=factory)
    encoder.encode(moving_frame(0), False)
    encoder.close()
    assert encoder._memory is live_encoder.MEMORY
    assert live_encoder.MEMORY.candidates(CHAINS["nvenc"])[0] == []

    forget_chains()
    to_try, known_bad = live_encoder.MEMORY.candidates(CHAINS["nvenc"])
    assert [chain.name for chain in to_try] == ["nvenc/jetson", "nvenc/desktop"] and known_bad == {}
    factory = _Factory()
    encoder = HardwareH264Encoder("nvenc", CHAINS["nvenc"], fps=FPS, pipeline_factory=factory)
    encoder.encode(moving_frame(0), False)
    encoder.close()
    assert factory.asked == ["nvenc/jetson", "nvenc/desktop"]


def test_the_chain_that_worked_is_tried_first():
    memory = ChainMemory()
    memory.worked(CHAINS["v4l2"][0])
    to_try, known_bad = memory.candidates(chains_for("auto"))
    assert [chain.name for chain in to_try] == [
        "v4l2", "nvenc/jetson", "nvenc/desktop", "vaapi/va", "vaapi/vaapi",
    ]
    assert known_bad == {}
    memory.failed(CHAINS["v4l2"][0], "gone")
    assert memory.working is None
    assert [chain.name for chain in memory.candidates(chains_for("auto"))[0]][0] == "nvenc/jetson"


def test_a_chain_that_stops_after_its_probe_is_probed_again_at_the_next_start(logged):
    memory = ChainMemory()
    factory = _Factory({"nvenc/jetson": "works-then-fails"})
    encoder = _stand_in(chains=CHAINS["nvenc"], memory=memory, pipeline_factory=factory)
    for index in range(6):
        payloads, _ = encoder.encode(moving_frame(index), False)
        assert payloads
    encoder.close()
    assert encoder.software and factory.built[0].close_calls == 1
    warnings = logged.at(logging.WARNING)
    assert warnings == [
        "live.encoder=nvenc: nvenc/jetson stopped working (the chip gave up). "
        "The rest of this session is encoded in software H.264."
    ]
    # Forgotten as working, not marked bad: minutes of video are not a
    # missing chip.
    assert memory.working is None
    assert [chain.name for chain in memory.candidates(CHAINS["nvenc"])[0]] == [
        "nvenc/jetson", "nvenc/desktop",
    ]
    # And the mid-session failure did not go looking for another chain.
    assert factory.asked == ["nvenc/jetson"]


# --- the fallback, with real pipelines and real libx264 -------------------------


def _erroring(after):
    return _variant(
        name="stand-in/errors",
        convert="videoconvert ! identity name=breaks error-after={}".format(after),
        elements=("videoconvert", "identity", "x264enc"),
    )


_SILENT = _variant(
    name="stand-in/silent",
    convert="videoconvert ! identity drop-probability=1.0",
    elements=("videoconvert", "identity", "x264enc"),
)


@pytest.mark.parametrize(
    "chain, reason",
    [
        (JETSON, "nvenc/jetson: GStreamer has no element 'nvvidconv'"),
        (_erroring(1), "stand-in/errors: breaks: "),
        (_SILENT, "stand-in/silent: no access unit within 0.2 s of a frame"),
    ],
    ids=["cannot-be-built", "errors-on-the-bus", "delivers-nothing"],
)
def test_a_chain_that_fails_its_probe_leaves_software_h264_from_that_very_frame(
    logged, chain, reason
):
    memory = ChainMemory()
    encoder = _stand_in(chains=(chain,), memory=memory, frame_timeout_s=0.2)
    try:
        payloads = [encoder.encode(moving_frame(index), index == 6)[0] for index in range(10)]
    finally:
        encoder.close()
    # Nothing is lost: the frame the probe failed on is already software.
    assert all(payloads)
    assert encoder.software
    assert _decodes(encoder.units) == [1] * 10
    # The first software frame is a keyframe with its parameter sets, and
    # so is one a viewer asks for later, on every aiortc: 1.3.0 would not
    # have made the second by itself.
    assert {NAL_SPS, NAL_PPS, NAL_IDR} <= set(encoder.calls[0][1])
    assert NAL_IDR in encoder.calls[6][1]
    assert [index for index, (_, types) in enumerate(encoder.calls) if NAL_IDR in types] == [0, 6]
    # One warning for the session: the backend, the chain, the reason.
    warnings = logged.at(logging.WARNING)
    assert len(warnings) == 1
    assert warnings[0].startswith("live.encoder=nvenc: no hardware encoder could be used (")
    assert reason in warnings[0]
    assert memory.candidates((chain,))[0] == []


def test_a_chain_that_fails_mid_session_is_followed_by_a_software_keyframe(logged):
    chain = _erroring(4)
    built = []

    def factory(chain, **kwargs):
        built.append(Pipeline(chain, **kwargs))
        return built[-1]

    encoder = _stand_in(chains=(chain,), frame_timeout_s=0.3, pipeline_factory=factory)
    try:
        for index in range(8):
            payloads, _ = encoder.encode(moving_frame(index), False)
            assert payloads, index
            if index == 0:
                assert encoder.chain is chain and not encoder.software
    finally:
        encoder.close()
    assert encoder.software
    # Hardware until the element gave up, software from the frame it gave
    # up on, which nobody forced and which is a keyframe all the same.
    failed_at = next(
        index for index, (_, types) in enumerate(encoder.calls) if index and NAL_IDR in types
    )
    assert 2 <= failed_at <= 4
    assert {NAL_SPS, NAL_PPS, NAL_IDR} <= set(encoder.calls[failed_at][1])
    assert all(NAL_SLICE in types for _, types in encoder.calls[failed_at + 1:])
    # Two streams, each decodable from its own first keyframe.
    assert _decodes(encoder.units[:failed_at]) == [1] * failed_at
    assert _decodes(encoder.units[failed_at:]) == [1] * (8 - failed_at)
    warnings = logged.at(logging.WARNING)
    assert len(warnings) == 1
    assert warnings[0].startswith("live.encoder=nvenc: stand-in/errors stopped working (breaks: ")
    # The pipeline that was given up is not left holding the device.
    assert [pipeline.final_state for pipeline in built] == ["null"]


def _no_gi(monkeypatch):
    # `None` in `sys.modules` makes `import gi` raise ImportError.
    monkeypatch.setitem(sys.modules, "gi", None)


def _no_typelib(monkeypatch):
    import gi

    def refuse(namespace, version):
        raise ValueError("Namespace {} not available".format(namespace))

    monkeypatch.setattr(gi, "require_version", refuse)


@pytest.mark.parametrize(
    "take_away, said",
    [(_no_gi, "the Python module gi is not installed"),
     (_no_typelib, "a GStreamer typelib is not installed")],
    ids=["no-gi", "no-typelib"],
)
def test_without_gstreamer_the_session_is_software_and_the_warning_names_the_packages(
    monkeypatch, logged, take_away, said
):
    """GStreamer is not a dependency of the package. A robot without it has
    asked for something it has not installed, and is told what."""
    monkeypatch.setattr(live_encoder, "_gst", None)
    take_away(monkeypatch)
    memory = ChainMemory()
    encoder = _stand_in(chains=chains_for("auto"), backend="auto", memory=memory)
    try:
        payloads = [encoder.encode(moving_frame(index), False)[0] for index in range(3)]
    finally:
        encoder.close()
    assert all(payloads) and encoder.software
    assert _decodes(encoder.units) == [1, 1, 1]
    warnings = logged.at(logging.WARNING)
    assert len(warnings) == 1
    assert "GStreamer cannot be loaded: {}.".format(said) in warnings[0]
    for package in ("python3-gi", "gir1.2-gstreamer-1.0", "gir1.2-gst-plugins-base-1.0",
                    "gstreamer1.0-plugins-base", "gstreamer1.0-plugins-bad"):
        assert package in warnings[0]
    # Said once, not once per chain; and none of the five is tried again.
    assert warnings[0].count("cannot be loaded") == 1
    assert memory.candidates(chains_for("auto"))[0] == []


# --- nothing is raised out of the encoder ----------------------------------------
#
# aiortc ends its sending loop for good on an exception from `encode()`, tells
# nobody, and logs the traceback with its message. So whatever goes wrong
# inside, `encode()` returns, and the log gets a type and never a message.

_SECRET = "rtsp://user:hunter2@camera.local/stream"


class _Exploding(_FakePipeline):
    """A pipeline that raises something nobody foresaw, with a message that
    must not be repeated."""

    def encode(self, frame, force_keyframe):
        self.frames += 1
        if self.frames > self.explode_after:
            raise RuntimeError("pipeline failed for " + _SECRET)
        return _CANNED_UNIT


def _exploding_factory(explode_after, built):
    def factory(chain, *, width, height, fps, bitrate_bps, frame_timeout_s):
        pipeline = _Exploding(chain, width=width, height=height, bitrate_bps=bitrate_bps)
        pipeline.explode_after = explode_after
        built.append(pipeline)
        return pipeline

    return factory


@pytest.mark.parametrize("explode_after", [0, 3], ids=["in-the-probe", "mid-session"])
def test_an_exception_nobody_foresaw_is_a_chain_that_failed_and_its_message_is_not_logged(
    logged, explode_after
):
    built = []
    memory = ChainMemory()
    encoder = _stand_in(
        chains=(JETSON,), memory=memory, pipeline_factory=_exploding_factory(explode_after, built)
    )
    try:
        payloads = [encoder.encode(moving_frame(index), False)[0] for index in range(6)]
    finally:
        encoder.close()
    # Every call returned, and with a frame: software took over at once.
    assert all(payloads) and encoder.software
    assert built[0].close_calls == 1
    assert NAL_IDR in encoder.calls[explode_after][1]
    warnings = logged.at(logging.WARNING)
    assert len(warnings) == 1
    assert "failed unexpectedly (RuntimeError)" in warnings[0]
    assert not any("hunter2" in message or "camera.local" in message for _, message in logged.records)
    # In the probe it is a failed chain like any other and is remembered; a
    # chain that had worked is probed again next time.
    assert (memory.candidates((JETSON,))[0] == []) == (explode_after == 0)


def test_when_software_cannot_encode_either_the_frame_is_dropped_and_the_next_one_is_a_keyframe(
    monkeypatch, logged
):
    """The last line of defence. libx264 failing is not something a robot
    recovers from by itself, but it must not take the sender with it, and
    its message stays out of the log."""
    encoder = _stand_in(chains=(JETSON,))  # no such chain here: software from the first frame
    real = H264Encoder._encode_frame
    failing = {"on": False, "calls": 0}

    def sometimes_failing(self, frame, force_keyframe):
        if failing["on"]:
            failing["calls"] += 1
            raise RuntimeError("could not open " + _SECRET)
        return real(self, frame, force_keyframe)

    monkeypatch.setattr(H264Encoder, "_encode_frame", sometimes_failing)
    try:
        good = [encoder.encode(moving_frame(index), False) for index in range(3)]
        failing["on"] = True
        # A viewer asks for a keyframe on a frame that cannot be made.
        dropped = [encoder.encode(moving_frame(3), True)] + [
            encoder.encode(moving_frame(index), False) for index in range(4, 7)
        ]
        failing["on"] = False
        after = [encoder.encode(moving_frame(index), False) for index in range(7, 10)]
    finally:
        encoder.close()
    assert all(payloads for payloads, _ in good)
    # Four frames that could not be encoded: four returns, no payloads, and
    # the timestamps aiortc expects all the same.
    assert [payloads for payloads, _ in dropped] == [[], [], [], []]
    assert [timestamp for _, timestamp in dropped] == [index * 6000 for index in range(3, 7)]
    assert failing["calls"] == 4
    assert all(payloads for payloads, _ in after)
    # The keyframe that was asked for and could not be made was owed, and
    # is the first frame that could be: nobody asked again. (`calls` has no
    # entry for a frame that could not be encoded: three, then the three after.)
    assert len(encoder.calls) == 6
    assert encoder.calls[3][0] is True and NAL_IDR in encoder.calls[3][1]
    assert all(
        forced is False and NAL_IDR not in types for forced, types in encoder.calls[4:]
    )
    # Said once for the run of them, by type.
    errors = logged.at(logging.ERROR)
    assert errors == [
        "live.encoder=nvenc: software H.264 could not encode a frame (RuntimeError). "
        "Frames are dropped until one can be encoded."
    ]
    assert not any("hunter2" in message for _, message in logged.records)


def test_a_keyframe_asked_for_on_a_frame_that_produced_nothing_is_owed_once():
    class _Skipping(_FakePipeline):
        """An encoder that returns no unit for one frame."""

        def encode(self, frame, force_keyframe):
            self.frames += 1
            self.forced = getattr(self, "forced", []) + [force_keyframe]
            return b"" if self.frames == 3 else _CANNED_UNIT

    built = []

    def factory(chain, *, width, height, fps, bitrate_bps, frame_timeout_s):
        built.append(_Skipping(chain, width=width, height=height, bitrate_bps=bitrate_bps))
        return built[-1]

    encoder = _stand_in(chains=(JETSON,), pipeline_factory=factory)
    try:
        for index in range(6):
            encoder.encode(moving_frame(index), index == 2)
    finally:
        encoder.close()
    # Frame 0 is the probe (always a keyframe); the request came with the
    # frame that produced nothing, so the next one carries it, and only that one.
    assert built[0].forced == [True, False, True, True, False, False]


def test_importing_the_package_does_not_import_gi():
    """`gi` is imported on the first frame of a session that needs it. A
    fresh interpreter, so the answer is about the import and not about what
    another test loaded."""
    out = subprocess.run(
        [
            sys.executable, "-c",
            "import sys\n"
            "import fleetless_bridge.live, fleetless_bridge.live_encoder\n"
            "from fleetless_bridge.live_encoder import build_encoder\n"
            "encoder = build_encoder('auto', fps=2)\n"
            "print('gi' in sys.modules, 'gi.repository' in sys.modules)",
        ],
        check=True, capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).resolve().parents[1]),
    ).stdout.split()
    assert out == ["False", "False"]


# --- the stand-in, through the encoder class -------------------------------------


def test_the_real_pipeline_carries_a_session_and_follows_a_keyframe_request(logged):
    built = []

    def factory(chain, **kwargs):
        built.append(Pipeline(chain, **kwargs))
        return built[-1]

    encoder = _stand_in(pipeline_factory=factory)
    try:
        results = [encoder.encode(moving_frame(index), index == 5) for index in range(10)]
        assert encoder.chain is STAND_IN and not encoder.software
    finally:
        encoder.close()
    assert len(built) == 1 and built[0].final_state == "null"
    assert _decodes(encoder.units) == [1] * 10
    assert [index for index, (_, types) in enumerate(encoder.calls) if NAL_IDR in types] == [0, 5]
    # aiortc's `encode()` did the rest: payloads under its packet size, and
    # the RTP timestamp from the frame's own.
    assert all(len(payload) <= h264.PACKET_MAX for payloads, _ in results for payload in payloads)
    assert [timestamp for _, timestamp in results] == [index * 6000 for index in range(10)]
    assert logged.at(logging.WARNING) == []


def test_a_frame_of_another_size_gets_a_pipeline_of_its_own():
    built = []

    def factory(chain, **kwargs):
        built.append(Pipeline(chain, **kwargs))
        return built[-1]

    encoder = _stand_in(pipeline_factory=factory)
    try:
        for index in range(3):
            encoder.encode(moving_frame(index), False)
        for index in range(3, 6):
            encoder.encode(moving_frame(index, width=160, height=120), False)
    finally:
        encoder.close()
    assert [pipeline.size for pipeline in built] == [(WIDTH, HEIGHT), (160, 120)]
    assert [pipeline.final_state for pipeline in built] == ["null", "null"]
    # The new stream starts with a keyframe, as a new stream must.
    assert NAL_IDR in encoder.calls[3][1]
    assert _decodes(encoder.units[3:]) == [1, 1, 1]


def test_after_close_a_frame_becomes_nothing():
    encoder = _stand_in()
    assert encoder.encode(moving_frame(0), False)[0]
    encoder.close()
    encoder.close()
    assert encoder.encode(moving_frame(1), False)[0] == []
    assert encoder.chain is None


def test_close_waits_for_the_frame_in_flight_and_nothing_breaks():
    """The publisher closes in one thread while aiortc may still be inside
    `encode()` in another."""
    built = []

    def factory(chain, **kwargs):
        built.append(Pipeline(chain, **kwargs))
        return built[-1]

    encoder = _stand_in(pipeline_factory=factory)
    failures = []
    encoded = []

    def keep_encoding():
        try:
            for index in range(400):
                payloads, _ = encoder.encode(moving_frame(index), False)
                if not payloads:
                    return
                encoded.append(index)
        except Exception as exc:  # noqa: BLE001 - whatever it is, it is the finding
            failures.append(exc)

    worker = threading.Thread(target=keep_encoding)
    worker.start()
    deadline = time.monotonic() + 10.0
    while len(encoded) < 20 and time.monotonic() < deadline:
        time.sleep(0.005)
    encoder.close()
    worker.join(timeout=10.0)
    assert not worker.is_alive()
    assert failures == []
    assert 20 <= len(encoded) < 400
    assert built[0].final_state == "null"


# --- the bitrate -----------------------------------------------------------------


def test_the_target_takes_what_the_configuration_allows_not_what_libx264_is_clamped_to():
    encoder = _stand_in()
    try:
        for asked, kept in [
            (300_000, 300_000), (8_000_000, 8_000_000), (MIN_BITRATE_BPS, MIN_BITRATE_BPS),
            (MAX_BITRATE_BPS, MAX_BITRATE_BPS), (1, MIN_BITRATE_BPS), (0, MIN_BITRATE_BPS),
            (90_000_000, MAX_BITRATE_BPS),
        ]:
            encoder.target_bitrate = asked
            assert encoder.target_bitrate == kept
        # aiortc's own encoder, for comparison: the clamp this class leaves behind.
        plain = H264Encoder()
        plain.target_bitrate = 8_000_000
        assert plain.target_bitrate == 3_000_000
    finally:
        encoder.close()
    assert (MIN_BITRATE_BPS, MAX_BITRATE_BPS) == (1_000, 50_000_000)


def test_the_bounds_are_those_of_bitrate_kbps_in_the_configuration_document():
    """Contracts exports no constant for them, so the numbers are stated in
    `live_encoder.py` and held here against the vendored schema."""
    schema = json.loads(
        (pathlib.Path(__file__).parent / "contracts" / "schema" / "cloud-config.schema.json").read_text()
    )
    camera = schema["properties"]["doc"]["properties"]["cameras"]["additionalProperties"]
    bitrate_kbps = camera["properties"]["bitrate_kbps"]
    assert bitrate_kbps["type"] == "integer" and bitrate_kbps["exclusiveMinimum"] == 0
    assert MIN_BITRATE_BPS == 1 * 1000  # the smallest positive integer, in kbps
    assert MAX_BITRATE_BPS == bitrate_kbps["maximum"] * 1000


def test_the_log_line_about_a_clamped_budget_knows_this_encoder():
    """`_hold_bitrate_budget` says once when a configured budget is outside
    what the encoder takes. With the wide bounds nothing a camera can be
    configured with is outside, so the line stays true by staying silent."""
    assert ENCODER_CLAMP_BPS["HardwareH264Encoder"] == (MIN_BITRATE_BPS, MAX_BITRATE_BPS)
    assert ENCODER_CLAMP_BPS["H264Encoder"] == (500_000, 3_000_000)


def test_a_change_of_a_tenth_or_less_is_not_passed_to_the_pipeline_and_a_larger_one_is():
    factory = _Factory({"nvenc/jetson": "works"})
    encoder = _stand_in(chains=CHAINS["nvenc"], pipeline_factory=factory)
    encoder.target_bitrate = 1_000_000
    encoder.encode(moving_frame(0), False)
    pipeline = factory.built[0]
    assert pipeline.bitrate_bps == 1_000_000  # built at the target, nothing to change

    for index, target in enumerate([1_050_000, 950_000, 1_100_000, 900_000], start=1):
        encoder.target_bitrate = target
        encoder.encode(moving_frame(index), False)
    assert pipeline.bitrates == []

    # Low-bandwidth mode's `reduce`, and the way back.
    encoder.target_bitrate = 300_000
    encoder.encode(moving_frame(5), False)
    encoder.target_bitrate = 1_000_000
    encoder.encode(moving_frame(6), False)
    encoder.close()
    assert pipeline.bitrates == [300_000, 1_000_000]


def test_a_lower_target_makes_the_real_stream_smaller():
    encoder = _stand_in()
    try:
        encoder.target_bitrate = 2_000_000
        high = [
            sum(len(p) for p in encoder.encode(moving_frame(index), False)[0]) for index in range(45)
        ]
        encoder.target_bitrate = 100_000
        low = [
            sum(len(p) for p in encoder.encode(moving_frame(index), False)[0])
            for index in range(45, 90)
        ]
    finally:
        encoder.close()
    assert sum(low[-15:]) * 4 < sum(high[-15:])


# --- teardown through the publisher -----------------------------------------------


def _probed(built):
    """An encoder that already holds a running pipeline, as it does once a
    session has sent a frame."""

    def factory(chain, **kwargs):
        built.append(Pipeline(chain, **kwargs))
        return built[-1]

    encoder = _stand_in(pipeline_factory=factory)
    encoder.encode(moving_frame(0), False)
    assert built and not built[0].closed
    return encoder


def test_a_failed_start_leaves_the_pipeline_in_null():
    built = []

    async def scenario():
        encoder = _probed(built)
        pub = _publisher(
            _FakeSignal(fail_offer=RuntimeError("no answer")), _SeamConnection(), _FakeTrack(),
            encoder=encoder,
        )
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "test-token")
        return [pipeline.final_state for pipeline in built]

    assert asyncio.run(scenario()) == ["null"]


def test_the_turn_fallback_keeps_the_pipeline_and_the_stop_after_it_leaves_it_in_null():
    built = []

    async def scenario():
        encoder = _probed(built)
        first = _SeamConnection(fail_local=_turn_failure())
        second = _SeamConnection()
        pub = _publisher(
            _FakeSignal(join=_turn_join()), [first, second], _FakeTrack(), encoder=encoder
        )
        await pub.start("wss://media.example", "room-1", "test-token")
        running = (first.close_calls, built[0].closed, getattr(second.sender, SENDER_ENCODER_ATTR))
        await pub.stop()
        return encoder, running, [pipeline.final_state for pipeline in built]

    encoder, running, states = asyncio.run(scenario())
    # The first connection is gone and the pipeline is not: the session goes on.
    assert running == (1, False, encoder)
    assert states == ["null"]


# --- through the real sender of this distribution's aiortc -----------------------


def test_the_real_sender_sends_the_stand_in_s_stream_and_aiortc_decodes_it(monkeypatch):
    """Spec test 4 with the bridge's own encoder class: aiortc's sender
    calls it, aiortc's depacketizer and decoder on the other side show the
    moving pattern, a keyframe request is honoured, and the pipeline is in
    NULL once the publisher has stopped."""
    cap = 500
    monkeypatch.setattr(h264, "PACKET_MAX", cap)
    built = []

    def factory(chain, **kwargs):
        built.append(Pipeline(chain, **kwargs))
        return built[-1]

    encoder = _stand_in(pipeline_factory=factory)
    result = {}

    async def body(*, sender, track, source, signal, recorder):
        index = 0

        async def paint():
            nonlocal index
            while True:
                # BGR for the holder, as a camera delivers it.
                source.picture = moving_picture(index)[:, :, ::-1].copy()
                index += 1
                await asyncio.sleep(1 / FPS)

        painting = asyncio.ensure_future(paint())
        try:
            shown = []
            while len(shown) < 20:
                frame = await asyncio.wait_for(track.recv(), 20.0)
                shown.append(frame.to_ndarray(format="rgb24"))
            await _until(lambda: len(encoder.calls) >= 10, 10.0, "a few frames")
            before = len(encoder.calls)
            sender._send_keyframe()
            await _until(
                lambda: any(forced for forced, _ in encoder.calls[before:]), 10.0, "the forced frame"
            )
            await _until(lambda: len(recorder.packets) > 60, 20.0, "packets")
            result.update(
                shown=shown, before=before, offer=signal.offer_sdp,
                on_sender=getattr(sender, SENDER_ENCODER_ATTR), packets=list(recorder.packets),
                running=(encoder.chain, encoder.software),
            )
        finally:
            painting.cancel()

    asyncio.run(_loopback(body, encoder=encoder))

    assert result["on_sender"] is encoder
    assert result["running"] == (STAND_IN, False)
    assert "H264/90000" in result["offer"] and "VP8" not in result["offer"]
    # The moving pattern arrived and moved: the bar is somewhere else in
    # the last picture than in the first, and each is one of the frames sent.
    first, last = result["shown"][0].astype(np.int16), result["shown"][-1].astype(np.int16)
    assert first.shape == (HEIGHT, WIDTH, 3)
    assert np.abs(first - last).mean() > 10
    assert min(np.abs(last - moving_picture(index)).mean() for index in range(200)) < 14
    # The keyframe request made an IDR picture with its parameter sets.
    forced = [types for was_forced, types in encoder.calls[result["before"]:] if was_forced]
    assert forced and all({NAL_SPS, NAL_PPS, NAL_IDR} <= set(types) for types in forced)
    # aiortc's packetizer cut the stand-in's stream to its packet size.
    media = result["packets"][0][0]
    payloads = [payload for payload_type, _, payload in result["packets"] if payload_type == media]
    assert max(len(payload) for payload in payloads) <= cap
    assert any(payload[0] & 0x1F == NAL_FU_A for payload in payloads)
    # And the publisher's stop put the pipeline away.
    assert [pipeline.final_state for pipeline in built] == ["null"]


@pytest.fixture
def the_default_packet_size_afterwards():
    """The cap is a module constant of aiortc's, for the whole process."""
    yield
    live.set_max_packet_bytes(LIVE_DEFAULTS["max_packet_bytes"])


@pytest.mark.parametrize("setting", [1200, 576, 1350])
def test_no_packet_of_the_bridge_s_own_encoder_exceeds_live_max_packet_bytes(
    setting, the_default_packet_size_afterwards
):
    """Whole RTP packets, as the sender hands them to the transport before
    SRTP: header, header extensions and payload. The encoder inherits
    aiortc's packetizer, so the cap that holds for aiortc's own encoders
    holds for this one, at the default and at both ends of its range."""
    live.set_max_packet_bytes(setting)
    encoder = _stand_in()
    result = {}

    async def body(*, sender, track, source, signal, recorder):
        sizes = []
        send = sender.transport._send_rtp

        async def measured(data):
            if not is_rtcp(data):
                sizes.append(len(data))
            await send(data)

        sender.transport._send_rtp = measured
        source.noise = True  # many packets a frame
        await _until(lambda: len(sizes) > 100, 20.0, "packets of noise")
        sender._send_keyframe()
        await _until(lambda: len(sizes) > 400, 30.0, "enough packets")
        result["sizes"] = list(sizes)
        result["running"] = (encoder.chain, encoder.software)

    asyncio.run(_loopback(body, encoder=encoder))

    assert result["running"] == (STAND_IN, False)
    sizes = result["sizes"]
    assert len(sizes) > 400
    assert max(sizes) <= setting
    # The cap is used, not merely respected: H.264 cuts a NAL unit into
    # equal fragments, so the largest packet is over half the cap.
    assert max(sizes) > setting // 2
