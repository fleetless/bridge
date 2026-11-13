# SPDX-License-Identifier: Apache-2.0
"""The encoder seam: a `LivePublisher` that is handed an encoder puts it into
aiortc's sender, offers H.264 only, and closes it when the session ends.

Two halves, as in `test_live.py`. The first drives the publisher against
fakes, where the order of things and every way a session ends can be reached.
The second runs it against the real aiortc of this distribution: two peer
connections in one process, the publisher on the sending side, with a test
encoder that is a subclass of aiortc's `H264Encoder` overriding
`_encode_frame`. That is the shape every encoder of the bridge has, and the
question it answers is whether aiortc 1.3.0, 1.6.0 and 1.14.0 really call
`encode()` on an object they did not build.

No GStreamer here and no setting: the encoder is whatever the test hands over.
"""
import asyncio
import gc
import logging
import pathlib
import subprocess
import sys
import threading
import time
from fractions import Fraction

import av
import numpy as np
import pytest
from aiortc import RTCConfiguration, RTCPeerConnection, RTCRtpSender, RTCSessionDescription
from aiortc.codecs import h264
from aiortc.codecs.h264 import H264Encoder
from aiortc.rtp import RtpPacket, is_rtcp

from fleetless_bridge import live
from fleetless_bridge.camera import LatestFrameHolder
from fleetless_bridge.live import SENDER_ENCODER_ATTR, LivePublisher, LiveStartError
from fleetless_bridge.livekit_signal import JoinInfo
from test_live import (
    _capturing,
    _FakeConnection,
    _FakeSender,
    _FakeSignal,
    _FakeTrack,
    _publisher,
    _turn_failure,
    _turn_join,
)

# --- fakes -------------------------------------------------------------------


class _Encoder:
    """What the publisher touches of an encoder: the bitrate target and
    `close()`. Records which thread closed it and what was already closed by
    then."""

    def __init__(self, *, fail_close=None, events=None):
        self.target_bitrate = 1_000_000
        self.close_calls = 0
        self.close_threads = []
        self._fail_close = fail_close
        self._events = events if events is not None else []

    def close(self):
        self.close_calls += 1
        self.close_threads.append(threading.get_ident())
        self._events.append("encoder.close")
        if self._fail_close is not None:
            raise self._fail_close


class _Transceiver:
    def __init__(self, sender, events):
        self.sender = sender
        self.preferences = None
        self._events = events

    def setCodecPreferences(self, codecs):
        self._events.append("setCodecPreferences")
        self.preferences = list(codecs)


class _SeamConnection(_FakeConnection):
    """`_FakeConnection` with the transceiver list a real connection has, and
    a record of the order things happened in."""

    def __init__(self, *, events=None, **kwargs):
        super().__init__(**kwargs)
        self.events = events if events is not None else []
        # Somebody else's transceiver first: the publisher has to find its
        # own by the sender, not take the first one.
        self.other = _Transceiver(_FakeSender(), self.events)
        self.transceiver = _Transceiver(self.sender, self.events)
        self.transceiver_reads = 0

    def addTrack(self, track):
        self.events.append("addTrack")
        return super().addTrack(track)

    def getTransceivers(self):
        self.transceiver_reads += 1
        return [self.other, self.transceiver]

    async def createOffer(self):
        # What the sender holds at the moment the offer is made.
        self.events.append(("createOffer", getattr(self.sender, SENDER_ENCODER_ATTR, "absent")))
        return await super().createOffer()

    async def close(self):
        self.events.append("pc.close")
        await super().close()


class _SeamSignal(_FakeSignal):
    def __init__(self, *, events=None, **kwargs):
        super().__init__(**kwargs)
        self._events = events if events is not None else []

    async def close(self):
        self._events.append("signal.close")
        await super().close()


def _h264_and_rtx():
    return [
        codec
        for codec in RTCRtpSender.getCapabilities("video").codecs
        if codec.mimeType.lower() in ("video/h264", "video/rtx")
    ]


# --- the encoder goes onto the sender ----------------------------------------


def test_the_encoder_is_on_the_sender_before_the_offer_is_made():
    async def scenario():
        encoder = _Encoder()
        connection = _SeamConnection()
        pub = _publisher(_FakeSignal(), connection, _FakeTrack(), encoder=encoder)
        await pub.start("wss://media.example", "room-1", "test-token")
        await pub.stop()
        return encoder, connection

    encoder, connection = asyncio.run(scenario())
    assert getattr(connection.sender, SENDER_ENCODER_ATTR) is encoder
    # Right after `addTrack`, and before the offer: aiortc builds an encoder
    # only when it finds none, on the first frame.
    assert connection.events[:3] == ["addTrack", "setCodecPreferences", ("createOffer", encoder)]


def test_with_an_encoder_the_offer_is_h264_and_its_retransmission_only():
    async def scenario():
        connection = _SeamConnection()
        pub = _publisher(_FakeSignal(), connection, _FakeTrack(), encoder=_Encoder())
        await pub.start("wss://media.example", "room-1", "test-token")
        await pub.stop()
        return connection

    connection = asyncio.run(scenario())
    preferences = connection.transceiver.preferences
    assert preferences == _h264_and_rtx()
    assert {codec.mimeType.lower() for codec in preferences} == {"video/h264", "video/rtx"}
    # This aiortc can do VP8, so leaving it out is a restriction and not an
    # accident of the build.
    assert "video/vp8" in {
        codec.mimeType.lower() for codec in RTCRtpSender.getCapabilities("video").codecs
    }
    # The publisher's own transceiver, found by its sender.
    assert connection.other.preferences is None


def test_without_an_encoder_the_sender_and_the_offer_are_left_alone():
    """`software`: aiortc builds its own encoder, VP8 or H.264 as the answer
    picks. Nothing is set and nothing is asked of the connection."""

    async def scenario():
        connection = _SeamConnection()
        pub = _publisher(_FakeSignal(), connection, _FakeTrack())
        await pub.start("wss://media.example", "room-1", "test-token")
        await pub.stop()
        return connection

    connection = asyncio.run(scenario())
    assert getattr(connection.sender, SENDER_ENCODER_ATTR) is None
    assert connection.transceiver_reads == 0
    assert connection.transceiver.preferences is None
    assert connection.events[:2] == ["addTrack", ("createOffer", None)]


def test_the_connection_after_a_turn_failure_gets_the_same_encoder_and_restriction():
    async def scenario():
        events = []
        encoder = _Encoder(events=events)
        first = _SeamConnection(fail_local=_turn_failure(), events=events)
        second = _SeamConnection(events=events)
        pub = _publisher(
            _FakeSignal(join=_turn_join()), [first, second], _FakeTrack(), encoder=encoder
        )
        await pub.start("wss://media.example", "room-1", "test-token")
        closes_while_running = encoder.close_calls
        await pub.stop()
        return encoder, first, second, events, closes_while_running

    encoder, first, second, events, closes_while_running = asyncio.run(scenario())
    assert first.close_calls == 1
    assert getattr(second.sender, SENDER_ENCODER_ATTR) is encoder
    assert second.transceiver.preferences == _h264_and_rtx()
    # Giving up the first connection is not the end of the session: the
    # encoder is still needed and is closed once, at the stop.
    assert closes_while_running == 0
    assert encoder.close_calls == 1
    assert events.index("encoder.close") > max(
        i for i, event in enumerate(events) if event == "pc.close"
    )


def test_a_sender_that_keeps_its_encoder_elsewhere_fails_the_start():
    """An aiortc that renamed the attribute. Setting it anyway would leave the
    encoder unused and aiortc encoding with one of its own, which for an
    encoder that does not encode the track's frames at all is a wrong picture.
    So the start fails, says why, and the encoder is closed."""

    async def scenario():
        encoder = _Encoder()
        connection = _SeamConnection(sender=_FakeSender(with_encoder_attr=False))
        signal = _FakeSignal()
        handler, watched = _capturing()
        try:
            pub = _publisher(signal, connection, _FakeTrack(), encoder=encoder)
            with pytest.raises(LiveStartError) as refused:
                await pub.start("wss://media.example", "room-1", "test-token")
            return encoder, connection, signal, str(refused.value), handler.messages[:]
        finally:
            watched.removeHandler(handler)

    encoder, connection, signal, message, logged = asyncio.run(scenario())
    assert message == "could not publish the video track (EncoderNotInstalled)"
    assert not hasattr(connection.sender, SENDER_ENCODER_ATTR)
    assert encoder.close_calls == 1
    assert connection.close_calls == 1 and signal.close_calls == 1
    # No offer was made for a session that could not have worked.
    assert not any(isinstance(call, tuple) and call[0] == "send_offer" for call in signal.calls)
    assert any(SENDER_ENCODER_ATTR in line and "encoder" in line for line in logged)


def test_a_start_after_the_encoder_was_closed_fails_and_uses_no_other_encoder():
    """A publisher that was handed an encoder never publishes without it.
    After a stop the encoder is closed and gone; a second start must not let
    aiortc build an encoder of its own and offer VP8 in its place."""

    async def scenario():
        encoder = _Encoder()
        first = _SeamConnection()
        second = _SeamConnection()
        signal = _FakeSignal()
        handler, watched = _capturing()
        try:
            pub = _publisher(signal, [first, second], _FakeTrack(), encoder=encoder)
            await pub.start("wss://media.example", "room-1", "test-token")
            await pub.stop()
            with pytest.raises(LiveStartError) as refused:
                await pub.start("wss://media.example", "room-1", "test-token")
            return encoder, second, str(refused.value), handler.messages[:]
        finally:
            watched.removeHandler(handler)

    encoder, second, message, logged = asyncio.run(scenario())
    assert message == "could not publish the video track (EncoderNotInstalled)"
    assert encoder.close_calls == 1
    # Nothing was offered for the second start: no encoder on its sender,
    # no preference, no offer.
    assert getattr(second.sender, SENDER_ENCODER_ATTR) is None
    assert second.transceiver.preferences is None
    assert not any(isinstance(event, tuple) and event[0] == "createOffer" for event in second.events)
    assert any("already closed" in line for line in logged)


def test_the_bitrate_budget_is_held_on_the_encoder_that_was_handed_over():
    async def scenario():
        encoder = _Encoder()
        connection = _SeamConnection()
        pub = _publisher(
            _FakeSignal(), connection, _FakeTrack(),
            encoder=encoder, bitrate_kbps=4000, bitrate_interval_s=0.01,
        )
        await pub.start("wss://media.example", "room-1", "test-token")
        await asyncio.sleep(0.05)
        first = encoder.target_bitrate
        encoder.target_bitrate = 123  # a receiver's estimate, written by aiortc
        pub.set_bitrate_kbps(300)
        await asyncio.sleep(0.05)
        second = encoder.target_bitrate
        await pub.stop()
        return first, second

    assert asyncio.run(scenario()) == (4_000_000, 300_000)


# --- the encoder is closed ---------------------------------------------------


def test_stop_closes_the_encoder_once_after_the_connection_and_off_the_loop():
    async def scenario():
        events = []
        encoder = _Encoder(events=events)
        connection = _SeamConnection(events=events)
        signal = _SeamSignal(events=events)
        pub = _publisher(signal, connection, _FakeTrack(), encoder=encoder)
        await pub.start("wss://media.example", "room-1", "test-token")
        assert encoder.close_calls == 0
        await pub.stop()
        await pub.stop()
        return encoder, events, threading.get_ident()

    encoder, events, loop_thread = asyncio.run(scenario())
    assert encoder.close_calls == 1
    # After the connection: its sender is what calls `encode()`. Before the
    # signalling socket, which is the last thing with anything to say.
    closing = [e for e in events if e in ("pc.close", "encoder.close", "signal.close")]
    assert closing == ["pc.close", "encoder.close", "signal.close"]
    # In a thread of its own: closing a pipeline can wait for a frame in
    # flight, and the loop has other cameras to serve.
    assert len(encoder.close_threads) == 1 and encoder.close_threads != [loop_thread]


def test_a_failed_join_closes_the_encoder():
    async def scenario():
        encoder = _Encoder()
        pub = _publisher(
            _FakeSignal(fail_connect=OSError("refused")), _SeamConnection(), _FakeTrack(),
            encoder=encoder,
        )
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "test-token")
        after_start = encoder.close_calls
        await pub.stop()
        return after_start, encoder.close_calls

    assert asyncio.run(scenario()) == (1, 1)


def test_a_failed_publish_closes_the_encoder():
    async def scenario():
        encoder = _Encoder()
        pub = _publisher(
            _FakeSignal(fail_offer=RuntimeError("no answer")), _SeamConnection(), _FakeTrack(),
            encoder=encoder,
        )
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "test-token")
        after_start = encoder.close_calls
        await pub.stop()
        return after_start, encoder.close_calls

    assert asyncio.run(scenario()) == (1, 1)


def test_a_session_that_is_lost_closes_the_encoder():
    async def scenario():
        encoder = _Encoder()
        connection = _SeamConnection()
        lost = []
        handler, watched = _capturing()
        try:
            pub = _publisher(
                _FakeSignal(), connection, _FakeTrack(), encoder=encoder, on_lost=lost.append
            )
            await pub.start("wss://media.example", "room-1", "test-token")
            connection.connectionState = "failed"
            connection.fire("connectionstatechange")
            for _ in range(200):
                if encoder.close_calls:
                    break
                await asyncio.sleep(0.01)
            after_loss = encoder.close_calls
            await pub.stop()
            return after_loss, encoder.close_calls, lost
        finally:
            watched.removeHandler(handler)

    after_loss, in_the_end, lost = asyncio.run(scenario())
    assert (after_loss, in_the_end) == (1, 1)
    assert len(lost) == 1


def test_stop_without_a_start_closes_the_encoder():
    """Whoever hands an encoder to a publisher has given it away: `stop()` is
    the one call that is always made, so it is the one that closes."""

    async def scenario():
        encoder = _Encoder()
        pub = _publisher(_FakeSignal(), _SeamConnection(), _FakeTrack(), encoder=encoder)
        await pub.stop()
        return encoder.close_calls

    assert asyncio.run(scenario()) == 1


def test_a_close_that_raises_is_logged_by_type_name_and_stop_returns():
    async def scenario():
        events = []
        encoder = _Encoder(
            fail_close=RuntimeError("rtsp://user:secret@camera/ could not reach NULL"),
            events=events,
        )
        signal = _SeamSignal(events=events)
        handler, watched = _capturing()
        try:
            pub = _publisher(signal, _SeamConnection(events=events), _FakeTrack(), encoder=encoder)
            await pub.start("wss://media.example", "room-1", "test-token")
            await pub.stop()
            return signal, events, handler.messages[:]
        finally:
            watched.removeHandler(handler)

    signal, events, logged = asyncio.run(scenario())
    assert signal.close_calls == 1
    assert events[-1] == "signal.close"
    said = [line for line in logged if "encoder" in line.lower()]
    assert said == ["Error closing a live encoder: RuntimeError"]
    assert not any("secret" in line for line in logged)


def test_a_close_that_does_not_return_is_left_behind_after_the_wait(monkeypatch):
    monkeypatch.setattr(live, "ENCODER_CLOSE_TIMEOUT_S", 0.05)

    class _Stuck(_Encoder):
        def __init__(self):
            super().__init__()
            self.release = threading.Event()

        def close(self):
            super().close()
            self.release.wait(10.0)

    async def scenario():
        encoder = _Stuck()
        signal = _FakeSignal()
        handler, watched = _capturing()
        try:
            pub = _publisher(signal, _SeamConnection(), _FakeTrack(), encoder=encoder)
            await pub.start("wss://media.example", "room-1", "test-token")
            started = asyncio.get_event_loop().time()
            await pub.stop()
            took = asyncio.get_event_loop().time() - started
            return encoder, signal, took, handler.messages[:]
        finally:
            encoder.release.set()
            watched.removeHandler(handler)

    encoder, signal, took, logged = asyncio.run(scenario())
    assert encoder.close_calls == 1
    assert took < 5.0
    assert signal.close_calls == 1
    assert any("did not finish closing" in line for line in logged)


def test_a_close_that_never_returns_does_not_hold_the_process_at_exit():
    """The bridge runs inside `asyncio.run()`, and `asyncio.run()` waits for
    the loop's own worker threads when it ends. A close that is stuck there
    would hold every stop of the bridge until something killed it. So the
    close has a thread of its own that nothing waits for.

    A fresh interpreter with a time limit, because the thing under test is
    how the process ends: the close below blocks for a minute, and the whole
    run has to be over in a fraction of that."""
    script = """
import asyncio, sys, threading, time
sys.path.insert(0, "test")
from fleetless_bridge import live
from test_live import _FakeSignal, _FakeTrack, _publisher
from test_live_encoder_seam import _SeamConnection

live.ENCODER_CLOSE_TIMEOUT_S = 0.2
entered = threading.Event()

class Stuck:
    target_bitrate = 1_000_000
    def close(self):
        entered.set()
        time.sleep(60)

async def scenario():
    pub = _publisher(_FakeSignal(), _SeamConnection(), _FakeTrack(), encoder=Stuck())
    await pub.start("wss://media.example", "room-1", "test-token")
    await pub.stop()

began = time.monotonic()
asyncio.run(scenario())
print("returned", entered.is_set(), round(time.monotonic() - began, 1), flush=True)
"""
    began = time.monotonic()
    done = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=30,
        cwd=str(pathlib.Path(__file__).resolve().parents[1]),
    )
    took = time.monotonic() - began
    assert done.returncode == 0, done.stderr[-2000:]
    said = done.stdout.split()
    # `close()` was really entered and is still inside its minute...
    assert said[:2] == ["returned", "True"], done.stdout
    # ...and neither `asyncio.run()` nor the interpreter waited for it.
    assert float(said[2]) < 10.0
    assert took < 20.0


def test_a_close_that_finishes_after_the_wait_is_over_is_not_reported_twice(monkeypatch):
    """Left behind, and then it returns after all, or fails after all: that
    late end goes to nobody, and above all not to a log with its message.

    Two places could still print it. This package's own logger is one. The
    other is asyncio: a future that ends with an exception nobody asks for
    is reported when it is collected, "Future exception was never
    retrieved", with the exception and its message, through the loop's
    exception handler and the `asyncio` logger. Both are watched."""
    monkeypatch.setattr(live, "ENCODER_CLOSE_TIMEOUT_S", 0.05)

    class _Late(_Encoder):
        def __init__(self):
            super().__init__()
            self.release = threading.Event()
            self.left = threading.Event()

        def close(self):
            super().close()
            self.release.wait(10.0)
            try:
                raise RuntimeError("rtsp://user:secret@camera/ went away")
            finally:
                self.left.set()

    class _Records(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.DEBUG)
            self.lines = []

        def emit(self, record):
            self.lines.append(self.format(record))

    async def scenario():
        encoder = _Late()
        handler, watched = _capturing()
        asyncio_log = logging.getLogger("asyncio")
        asyncio_records = _Records()
        asyncio_log.addHandler(asyncio_records)
        reported_to_the_loop = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: reported_to_the_loop.append(context)
        )
        try:
            pub = _publisher(_FakeSignal(), _SeamConnection(), _FakeTrack(), encoder=encoder)
            await pub.start("wss://media.example", "room-1", "test-token")
            await pub.stop()
            encoder.release.set()
            for _ in range(200):
                if encoder.left.is_set():
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.05)  # the loop gets to hear about it
            del pub
            gc.collect()  # where an unasked-for exception of a future is reported
            await asyncio.sleep(0.05)
            return encoder.left.is_set(), handler.messages[:], reported_to_the_loop, asyncio_records.lines
        finally:
            encoder.release.set()
            watched.removeHandler(handler)
            asyncio_log.removeHandler(asyncio_records)

    left, logged, reported_to_the_loop, asyncio_lines = asyncio.run(scenario())
    assert left
    about_the_encoder = [line for line in logged if "encoder" in line.lower()]
    assert len(about_the_encoder) == 1 and "did not finish closing" in about_the_encoder[0]
    assert not any("secret" in line for line in logged)
    assert reported_to_the_loop == []
    assert not any("secret" in line or "never retrieved" in line for line in asyncio_lines)


def test_an_encoder_without_a_close_is_not_an_error():
    """aiortc's own encoders have none."""

    class _NoClose:
        target_bitrate = 1_000_000

    async def scenario():
        handler, watched = _capturing()
        try:
            pub = _publisher(_FakeSignal(), _SeamConnection(), _FakeTrack(), encoder=_NoClose())
            await pub.start("wss://media.example", "room-1", "test-token")
            await pub.stop()
            return handler.messages[:]
        finally:
            watched.removeHandler(handler)

    assert not any("encoder" in line.lower() for line in asyncio.run(scenario()))


# --- against the real aiortc of this distribution ------------------------------

WIDTH, HEIGHT, FPS = 320, 240, 15

#: BGR, as the holder carries it. Far apart, so a decoded frame is one or the
#: other after any amount of H.264.
FIRST_COLOUR = (40, 60, 200)
SECOND_COLOUR = (200, 140, 30)

NAL_SLICE_IDR, NAL_SPS, NAL_PPS, NAL_FU_A = 5, 7, 8, 28


class _TestEncoder(H264Encoder):
    """The shape of every encoder of the bridge: aiortc's `H264Encoder` with
    `_encode_frame` replaced. `encode()` is aiortc's own and is not touched,
    so the packetization is aiortc's too.

    It encodes with libx264 in a codec context of its own, where the base
    class would have used `self.codec`: what comes out is a real stream that
    aiortc's decoder can turn back into the picture, made by code aiortc does
    not have.
    """

    def __init__(self):
        super().__init__()
        self.calls = []  # (force_keyframe, [NAL unit type, ...]) per frame
        self.threads = set()
        self.close_calls = 0
        self._context = None

    def _encode_frame(self, frame, force_keyframe):
        self.threads.add(threading.get_ident())
        assert isinstance(frame, av.VideoFrame)
        if self._context is None:
            context = av.CodecContext.create("libx264", "w")
            context.width = frame.width
            context.height = frame.height
            context.pix_fmt = "yuv420p"
            context.framerate = Fraction(30, 1)
            context.time_base = Fraction(1, 30)
            context.options = {
                "profile": "baseline", "tune": "zerolatency", "preset": "ultrafast",
                # No keyframe of the encoder's own choosing, at a change of
                # scene or after a number of frames: the only ones are the
                # first and the forced.
                "x264-params": "scenecut=0:keyint=100000",
            }
            context.open()
            self._context = context
        picture = frame.reformat(format="yuv420p")
        picture.pts = len(self.calls)
        picture.time_base = Fraction(1, 30)
        picture.pict_type = (
            av.video.frame.PictureType.I if force_keyframe else av.video.frame.PictureType.NONE
        )
        data = b"".join(bytes(packet) for packet in self._context.encode(picture))
        units = list(self._split_bitstream(data))
        self.calls.append((force_keyframe, [unit[0] & 0x1F for unit in units]))
        yield from units

    def close(self):
        self.close_calls += 1
        self._context = None


class _LoopbackSignal:
    """The room conversation, played by a second peer connection in this
    process: the offer goes into it and its answer comes back."""

    def __init__(self, receiving):
        self._receiving = receiving
        self.offer_sdp = None
        self.answer_sdp = None

    async def connect(self):
        return JoinInfo(room_sid="RM_1", room_name="room-1", server_version="1.13.5")

    async def add_track(self, cid, name, width, height, *, source="CAMERA", bitrate=None):
        return {"cid": cid, "track": {"sid": "TR_camera", "name": name}}

    async def send_offer(self, sdp):
        self.offer_sdp = sdp
        await self._receiving.setRemoteDescription(RTCSessionDescription(sdp=sdp, type="offer"))
        await self._receiving.setLocalDescription(await self._receiving.createAnswer())
        self.answer_sdp = self._receiving.localDescription.sdp
        return self.answer_sdp

    async def close(self):
        pass


class _Recorder:
    """Every RTP packet handed to the transport, before SRTP."""

    def __init__(self, transport):
        self.packets = []  # (payload type, RTP timestamp, payload)
        self._send = transport._send_rtp
        transport._send_rtp = self._record

    async def _record(self, data):
        if not is_rtcp(data):
            packet = RtpPacket.parse(data)
            self.packets.append((packet.payload_type, packet.timestamp, packet.payload))
        await self._send(data)


async def _until(condition, timeout, what):
    deadline = asyncio.get_event_loop().time() + timeout
    while not condition():
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("timed out waiting for " + what)
        await asyncio.sleep(0.02)


class _Source:
    """Feeds the holder the way a camera does: a new frame, a new timestamp."""

    def __init__(self, holder):
        self._holder = holder
        self.picture = np.full((HEIGHT, WIDTH, 3), FIRST_COLOUR, dtype=np.uint8)
        self.noise = False
        self._rng = np.random.default_rng(894)

    async def run(self):
        timestamp_ms = 1_000
        while True:
            if self.noise:
                # An image no encoder compresses well: many packets a frame.
                picture = self._rng.integers(0, 255, (HEIGHT, WIDTH, 3), dtype=np.uint8)
            else:
                picture = self.picture
            self._holder.set(picture, timestamp_ms)
            timestamp_ms += 1000 // FPS
            await asyncio.sleep(1 / FPS)


async def _loopback(body, *, encoder):
    """A real `LivePublisher` with real aiortc on both ends. No ICE servers:
    the two connections find each other on the host's own addresses."""
    receiving = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    received = []
    receiving.on("track", received.append)
    sending = []

    def connection_factory(ice_servers):
        pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        sending.append(pc)
        return pc

    holder = LatestFrameHolder()
    source = _Source(holder)
    feed = asyncio.ensure_future(source.run())
    signal = _LoopbackSignal(receiving)
    kwargs = {"encoder": encoder} if encoder is not None else {}
    pub = LivePublisher(
        holder=holder, width=WIDTH, height=HEIGHT, fps=FPS, bitrate_kbps=1000,
        connection_factory=connection_factory,
        signal_factory=lambda url, token, **callbacks: signal,
        **kwargs,
    )
    try:
        await pub.start("wss://media.example", "room-1", "test-token")
        sender = sending[0].getSenders()[0]
        await _until(lambda: received, 10.0, "the track on the receiving side")
        await body(
            sender=sender, track=received[0], source=source, signal=signal,
            recorder=_Recorder(sender.transport),
        )
    finally:
        feed.cancel()
        await pub.stop()
        await receiving.close()


async def _decoded_colour(track, colour, timeout=20.0):
    """Reads decoded frames until one shows `colour`; returns it as BGR."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise AssertionError("no decoded frame showed {}".format(colour))
        frame = await asyncio.wait_for(track.recv(), remaining)
        bgr = frame.to_ndarray(format="bgr24")
        if bgr.shape == (HEIGHT, WIDTH, 3) and np.abs(
            bgr.reshape(-1, 3).mean(axis=0) - np.array(colour)
        ).max() < 16:
            return bgr


def test_a_real_sender_encodes_with_the_encoder_it_was_handed():
    """The seam itself, end to end: aiortc's sender calls `encode()` on the
    object the publisher installed, and aiortc's H.264 depacketizer and
    decoder on the other side turn the packets back into the picture."""
    encoder = _TestEncoder()
    result = {}

    async def body(*, sender, track, source, signal, recorder):
        result["first"] = await _decoded_colour(track, FIRST_COLOUR)
        source.picture = np.full((HEIGHT, WIDTH, 3), SECOND_COLOUR, dtype=np.uint8)
        result["second"] = await _decoded_colour(track, SECOND_COLOUR)
        result["on_sender"] = getattr(sender, SENDER_ENCODER_ATTR)
        result["offer"], result["answer"] = signal.offer_sdp, signal.answer_sdp
        result["loop_thread"] = threading.get_ident()

    asyncio.run(_loopback(body, encoder=encoder))

    # aiortc kept the encoder it found instead of building one.
    assert result["on_sender"] is encoder
    assert len(encoder.calls) >= 2
    # `encode()` ran where aiortc runs every encoder: in a worker thread.
    assert encoder.threads and result["loop_thread"] not in encoder.threads
    # The first frame of a stream is a keyframe with its parameter sets.
    assert {NAL_SPS, NAL_PPS, NAL_SLICE_IDR} <= set(encoder.calls[0][1])
    # H.264 was the only codec on offer, so it is what was agreed.
    assert "H264/90000" in result["offer"] and "VP8" not in result["offer"]
    assert "rtx/90000" in result["offer"]
    assert "H264/90000" in result["answer"] and "VP8" not in result["answer"]
    # The publisher closed it.
    assert encoder.close_calls == 1


def test_a_keyframe_request_reaches_the_encoder_as_force_keyframe():
    """A viewer's PLI ends in aiortc's `_send_keyframe`; the next `encode()`
    carries the flag, and the encoder that honours it sends an IDR picture
    with its parameter sets.

    Requests are counted where aiortc receives them, so a PLI of the
    receiving side's own, on a machine too busy to keep up, is a request like
    the one made here and not a failure of this test."""
    encoder = _TestEncoder()
    result = {}

    async def body(*, sender, track, source, signal, recorder):
        requests = []
        flag_keyframe = sender._send_keyframe

        def counted():
            requests.append(len(encoder.calls))
            flag_keyframe()

        sender._send_keyframe = counted
        await _decoded_colour(track, FIRST_COLOUR)
        await _until(lambda: len(encoder.calls) >= 5, 10.0, "a few frames")
        before = len(encoder.calls)
        sender._send_keyframe()
        await _until(
            lambda: any(forced for forced, _ in encoder.calls[before:]), 10.0,
            "the forced frame",
        )
        result["before"] = before
        # And the stream is still a picture afterwards.
        source.picture = np.full((HEIGHT, WIDTH, 3), SECOND_COLOUR, dtype=np.uint8)
        await _decoded_colour(track, SECOND_COLOUR)
        result["requests"] = list(requests)

    asyncio.run(_loopback(body, encoder=encoder))

    before, requests = result["before"], result["requests"]
    forced_at = [index for index, (forced, _) in enumerate(encoder.calls) if forced]
    # The request made here was answered at once: by the next frame, or by
    # the one after it when a frame was already being encoded.
    assert any(index in (before, before + 1) for index in forced_at)
    # Every forced frame answers a request, and none came without one: the
    # flag is aiortc's, passed through, not something the encoder made up.
    assert len(forced_at) <= len(requests)
    assert all(any(asked <= index for asked in requests) for index in forced_at)
    # A forced frame is an IDR picture with its parameter sets.
    for index in forced_at:
        assert {NAL_SPS, NAL_PPS, NAL_SLICE_IDR} <= set(encoder.calls[index][1])
    # And an unforced one is not: the flag is what made the difference.
    unforced = [units for forced, units in encoder.calls[1:] if not forced]
    assert unforced and all(NAL_SLICE_IDR not in units for units in unforced)


def test_the_packets_of_a_handed_encoder_keep_to_aiortc_s_packet_size(monkeypatch):
    """`encode()` is inherited, so the payloads are cut by aiortc's packetizer
    against `PACKET_MAX`, read when a frame is packetized. That constant is
    what caps the bridge's packets; an encoder that packetized by itself would
    slip out from under it."""
    cap = 400
    monkeypatch.setattr(h264, "PACKET_MAX", cap)
    encoder = _TestEncoder()
    result = {}

    async def body(*, sender, track, source, signal, recorder):
        source.noise = True
        await _until(
            lambda: len({timestamp for _, timestamp, _ in recorder.packets}) >= 5, 20.0,
            "a few frames of noise",
        )
        sender._send_keyframe()
        await _until(lambda: len(recorder.packets) > 200, 20.0, "enough packets")
        result["packets"] = list(recorder.packets)

    asyncio.run(_loopback(body, encoder=encoder))

    media = result["packets"][0][0]
    payloads = [payload for payload_type, _, payload in result["packets"] if payload_type == media]
    assert len(payloads) > 150, "too few packets to say anything"
    assert max(len(payload) for payload in payloads) <= cap
    # The cap is used, not merely respected: frames were cut into fragments,
    # each between half the cap and the cap.
    fragments = [payload for payload in payloads if payload[0] & 0x1F == NAL_FU_A]
    assert len(fragments) > 100
    assert max(len(payload) for payload in fragments) > cap // 2


def test_without_an_encoder_the_same_loopback_offers_vp8_and_aiortc_builds_its_own():
    """The control. If this stopped holding, the tests above would prove less
    than they say: the restriction and the installed encoder are what make
    them H.264 from the test encoder."""
    result = {}

    async def body(*, sender, track, source, signal, recorder):
        await _decoded_colour(track, FIRST_COLOUR)
        result["on_sender"] = getattr(sender, SENDER_ENCODER_ATTR)
        result["offer"] = signal.offer_sdp

    asyncio.run(_loopback(body, encoder=None))

    assert "VP8/90000" in result["offer"] and "H264/90000" in result["offer"]
    assert type(result["on_sender"]).__module__.startswith("aiortc.codecs.")


def test_the_sender_of_this_aiortc_has_the_attribute_the_encoder_goes_into():
    """`SENDER_ENCODER_ATTR` is a private name. This is where a version that
    renames it shows first, by name."""

    async def scenario():
        pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        try:
            sender = pc.addTransceiver("video").sender
            return hasattr(sender, SENDER_ENCODER_ATTR), getattr(sender, SENDER_ENCODER_ATTR, 0)
        finally:
            await pc.close()

    assert asyncio.run(scenario()) == (True, None)
