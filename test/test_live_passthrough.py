# SPDX-License-Identifier: Apache-2.0
"""The track and the encoder of a passthrough camera.

`PassthroughTrack` and `PassthroughEncoder` are tested for real, like
`CameraTrack`: they are local objects, and what they decide -- which unit a
frame stands for, what timestamp it carries, which bytes come out -- is what a
fake would have to invent.

The encoder's `encode()` is aiortc's own, of whichever version the
distribution ships, and so are the depacketizer and the decoder the tests
check its output with. What goes in are access units libx264 wrote; what must
come out, after aiortc has packetized, depacketized and decoded them, are the
pictures those units decode to. Bit for bit: nothing was encoded on the way.
"""
import asyncio
import logging

import numpy as np
import pytest
from aiortc import RTCConfiguration, RTCPeerConnection
from aiortc.codecs import h264 as aiortc_h264
from aiortc.jitterbuffer import JitterFrame
from aiortc.mediastreams import VIDEO_TIME_BASE, MediaStreamError
from aiortc.rtp import RtpPacket, is_rtcp

import h264_streams as streams
from fleetless_bridge import live
from fleetless_bridge.camera import LatestFrameHolder
from fleetless_bridge.live import SENDER_ENCODER_ATTR
from fleetless_bridge.live_passthrough import (
    HANDOFF_MAX_UNITS,
    PLACEHOLDER_SIZE,
    PassthroughEncoder,
    PassthroughPublisher,
    PassthroughTrack,
    UnitHandoff,
)
from fleetless_bridge.passthrough import PassthroughStream, Unit
from fleetless_bridge.protocol import LIVE_DEFAULTS
from test_live_encoder_seam import (
    _h264_and_rtx,
    _LoopbackSignal,
    _SeamConnection,
    _SeamSignal,
    _until,
)

WIDTH, HEIGHT = 320, 240


@pytest.fixture(autouse=True)
def _default_packet_size():
    """The packet cap is one value for the process. A test that sets it
    leaves the default behind."""
    yield
    live.set_max_packet_bytes(LIVE_DEFAULTS["max_packet_bytes"])


@pytest.fixture(scope="module")
def units():
    """Twenty units, a keyframe every five, of noise: the keyframes are
    several kilobytes each and do not fit an RTP packet."""
    return streams.encode_stream(width=WIDTH, height=HEIGHT, frames=20, gop=5, noisy=True)


def _pair(width=WIDTH, height=HEIGHT):
    stream = PassthroughStream(width=width, height=height, slug="front")
    handoff = UnitHandoff()
    return stream, PassthroughTrack(stream, handoff), PassthroughEncoder(handoff)


async def _recv(track, timeout=2.0):
    return await asyncio.wait_for(track.recv(), timeout=timeout)


async def _parked(track):
    """A `recv()` that has reached the stream and waits there, as the
    sender's does between units."""
    task = asyncio.ensure_future(track.recv())
    await asyncio.sleep(0.01)
    assert not task.done()
    return task


async def _send(stream, track, encoder, units, *, start_ms=1_000, step_ms=40):
    """What aiortc's sender does with a track and an encoder, for `units`:
    one `recv()`, then that frame's `encode()` in a worker thread, then the
    next. Returns `(payloads, timestamp)` per unit."""
    loop = asyncio.get_running_loop()
    waiting = await _parked(track)
    for index, unit in enumerate(units):
        stream.push(unit, start_ms + index * step_ms)
    out = []
    for _ in units:
        frame = await asyncio.wait_for(waiting, timeout=2.0)
        out.append(await loop.run_in_executor(None, encoder.encode, frame, False))
        waiting = asyncio.ensure_future(track.recv())
    waiting.cancel()
    return out


def _depacketize(payloads):
    """aiortc's own way back from RTP payloads to an Annex-B access unit."""
    return b"".join(aiortc_h264.h264_depayload(payload) for payload in payloads)


# --- the track ---------------------------------------------------------------


def test_recv_returns_one_placeholder_frame_per_unit_in_order(units):
    async def scenario():
        stream, track, encoder = _pair()
        first = await _parked(track)  # attaches the stream
        for index, unit in enumerate(units):
            stream.push(unit, 5_000_000 + index * 40)
        frames = [await first] + [await _recv(track) for _ in units[1:]]
        return frames, track.frames_sent, stream.pending()

    frames, frames_sent, pending = asyncio.run(scenario())
    assert len(frames) == 20 and frames_sent == 20 and pending == 0
    for index, frame in enumerate(frames):
        # A picture nobody looks at, as small as a frame gets.
        assert (frame.width, frame.height) == (PLACEHOLDER_SIZE, PLACEHOLDER_SIZE)
        assert frame.time_base == VIDEO_TIME_BASE
        # 40 ms of capture time between two units are 3600 ticks of 90 kHz.
        assert frame.pts == index * 3600


def test_the_stream_is_attached_at_the_first_recv_and_starts_at_a_keyframe(units):
    async def scenario():
        stream, track, encoder = _pair()
        for index, unit in enumerate(units[:7]):  # before anybody pulls
            stream.push(unit, 1_000 + index)
        queued_before = stream.pending()
        waiting = await _parked(track)
        for index, unit in enumerate(units[7:12]):  # 7, 8, 9 depend on a keyframe never sent
            stream.push(unit, 2_000 + index)
        frame = await asyncio.wait_for(waiting, timeout=2.0)
        payloads, _ = encoder.encode(frame)
        return queued_before, _depacketize(payloads)

    queued_before, first_unit = asyncio.run(scenario())
    assert queued_before == 0
    assert streams.split(first_unit) == streams.split(units[10])  # the keyframe, not unit 7


def test_timestamps_never_stand_still_or_go_back():
    """Capture time is a wall clock. Two units in one millisecond, or a clock
    stepped back by NTP, must not produce an RTP timestamp that repeats: to a
    receiver that is one picture, not two."""
    units = streams.encode_stream(frames=6, gop=1)

    async def scenario():
        stream, track, encoder = _pair(64, 48)
        first = await _parked(track)
        for unit, stamp in zip(units, [1000, 1000, 1001, 990, 990, 1100]):
            stream.push(unit, stamp)
        return [(await first).pts] + [(await _recv(track)).pts for _ in units[1:]]

    stamps = asyncio.run(scenario())
    assert stamps == sorted(set(stamps)) and len(stamps) == 6
    assert stamps[:3] == [0, 1, 90]
    assert stamps[-1] == 100 * 90  # and a clock that moves on is followed again


def test_a_stopped_track_wakes_its_recv_and_lets_go_of_the_stream(units):
    async def scenario():
        stream, track, encoder = _pair()
        waiting = await _parked(track)
        track.stop()
        with pytest.raises(MediaStreamError):
            await asyncio.wait_for(waiting, timeout=2.0)
        with pytest.raises(MediaStreamError):
            await track.recv()
        stream.push(units[0], 1_000)  # the source keeps delivering
        return stream.pending(), stream.newest_keyframe()

    pending, keyframe = asyncio.run(scenario())
    assert pending == 0  # nothing queues for a sender that is gone
    assert keyframe is not None  # and the snapshot's keyframe is still kept


def test_nothing_is_handed_out_while_paused_and_a_resume_starts_at_a_keyframe(units):
    async def scenario():
        stream, track, encoder = _pair()
        first = await _parked(track)
        for index, unit in enumerate(units[:3]):
            stream.push(unit, 1_000 + index * 40)
        for frame in [await first, await _recv(track), await _recv(track)]:
            encoder.encode(frame)

        stream.set_paused(True)
        for index, unit in enumerate(units[3:8]):
            stream.push(unit, 2_000 + index * 40)
        waiting = await _parked(track)  # nothing came of five units

        stream.set_paused(False)
        for index, unit in enumerate(units[8:12]):  # 8 and 9 are mid-group
            stream.push(unit, 3_000 + index * 40)
        frame = await asyncio.wait_for(waiting, timeout=2.0)
        payloads, _ = encoder.encode(frame)
        return _depacketize(payloads)

    assert streams.split(asyncio.run(scenario())) == streams.split(units[10])


# --- the encoder -------------------------------------------------------------


def test_what_comes_out_of_aiortcs_packetizer_decodes_to_the_source_pictures(units):
    """Units in, RTP payloads out, through aiortc's `encode()`; back through
    aiortc's depacketizer and decoder. The pictures are the ones the units
    decode to, exactly, and every unit arrives once and in order."""
    async def scenario():
        stream, track, encoder = _pair()
        return await _send(stream, track, encoder, units), encoder

    encoded, encoder = asyncio.run(scenario())
    assert encoder.units_forwarded == 20

    decoder = aiortc_h264.H264Decoder()
    pictures = []
    for index, (payloads, timestamp) in enumerate(encoded):
        assert timestamp == index * 3600  # the frame's capture-time pts, as the RTP timestamp
        assert streams.split(_depacketize(payloads)) == streams.split(units[index])
        for frame in decoder.decode(JitterFrame(data=_depacketize(payloads), timestamp=timestamp)):
            pictures.append(frame.to_ndarray(format="bgr24"))

    expected = streams.decode_all(units)
    assert len(pictures) == len(expected) == 20
    for index, (picture, original) in enumerate(zip(pictures, expected)):
        assert np.array_equal(picture, original), "picture {} differs".format(index)


@pytest.mark.parametrize("setting", [576, 1200, 1350])
def test_no_payload_exceeds_the_packet_size_and_large_units_are_split(units, setting):
    """`live.max_packet_bytes` holds for a passthrough stream: the units are
    cut by aiortc's packetizer, against the payload size the setting leaves
    after the RTP header and its extensions."""
    live.set_max_packet_bytes(setting)
    payload_max = setting - live.RTP_OVERHEAD_BYTES
    assert aiortc_h264.PACKET_MAX == payload_max

    async def scenario():
        stream, track, encoder = _pair()
        return [payloads for payloads, _ in await _send(stream, track, encoder, units)]

    all_payloads = asyncio.run(scenario())
    sizes = [len(payload) for payloads in all_payloads for payload in payloads]
    assert max(sizes) <= payload_max
    # The cap was in play: the first keyframe is larger than one packet and
    # left as several. aiortc cuts a NAL unit into fragments of equal size,
    # so they come close to the cap without having to reach it.
    assert len(units[0]) > payload_max
    assert len(all_payloads[0]) > len(streams.split(units[0]))
    assert max(sizes) > payload_max // 2
    # Cut, and still whole: every unit comes back out as it went in.
    for index, payloads in enumerate(all_payloads):
        assert streams.split(_depacketize(payloads)) == streams.split(units[index])


def test_a_keyframe_request_and_a_bitrate_target_change_nothing(units):
    async def scenario():
        stream, track, encoder = _pair()
        first = await _parked(track)
        for index, unit in enumerate(units[:4]):
            stream.push(unit, 1_000 + index * 40)
        frames = [await first] + [await _recv(track) for _ in range(3)]
        out = []
        for index, frame in enumerate(frames):
            encoder.target_bitrate = 500_000 + index * 1_000_000  # what a REMB does
            out.append(encoder.encode(frame, force_keyframe=True)[0])  # what a PLI does
        return out, encoder

    out, encoder = asyncio.run(scenario())
    for index, payloads in enumerate(out):
        assert streams.split(_depacketize(payloads)) == streams.split(units[index])
    # Units 1 to 3 are not keyframes and did not become any.
    assert all(5 not in streams.nal_types(_depacketize(payloads)) for payloads in out[1:])
    # libx264 was never opened: the context aiortc's encoder creates on its
    # first frame does not exist.
    assert encoder.codec is None


def test_a_frame_the_encoder_holds_no_unit_for_sends_nothing(units):
    async def scenario():
        stream, track, encoder = _pair()
        first = await _parked(track)
        stream.push(units[0], 1_000)
        frame = await first
        once = encoder.encode(frame)[0]
        again = encoder.encode(frame)[0]  # the unit was taken the first time
        return once, again

    once, again = asyncio.run(scenario())
    assert once and again == []


class _Poison:
    """NAL units that cannot be iterated. The message is what a library
    error on this path could carry: the camera's URL."""

    def __iter__(self):
        raise RuntimeError("could not read rtsp://camuser:not-a-real-secret@cam.local/stream")


class _Lines(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def test_encode_never_raises_and_a_unit_that_fails_makes_the_stream_wait_for_a_keyframe(units):
    """aiortc's sender catches what an encoder raises, logs the message,
    stops the track for good and reports nothing. So `encode()` must not
    raise, whatever a unit does to it: the unit is dropped, the type of the
    error is logged and nothing else, and the stream starts again at its
    next keyframe."""
    handler = _Lines()
    logger = logging.getLogger("fleetless_bridge.live_passthrough")
    logger.addHandler(handler)

    async def scenario():
        stream = PassthroughStream(width=WIDTH, height=HEIGHT, slug="front")
        handoff = UnitHandoff()
        track = PassthroughTrack(stream, handoff)
        encoder = PassthroughEncoder(handoff, on_failure=stream.resync)
        waiting = await _parked(track)
        for index, unit in enumerate(units[:4]):
            stream.push(unit, 1_000 + index * 40)
        first = await waiting
        good = encoder.encode(first)

        second = await _recv(track)
        handoff.put(second.pts, Unit(nals=_Poison(), timestamp_ms=0, keyframe=False))
        bad = encoder.encode(second)
        bad_again = encoder.encode(await_frame := second)  # nothing left for it: no error, no unit
        del await_frame
        queued_after_failure = stream.pending()

        # Units 2 and 3 were queued and are gone; 4 depends on what was lost.
        waiting = await _parked(track)
        for index, unit in enumerate(units[4:7]):
            stream.push(unit, 2_000 + index * 40)
        resumed = encoder.encode(await asyncio.wait_for(waiting, timeout=2.0))
        return good, bad, bad_again, second.pts, queued_after_failure, resumed

    try:
        good, bad, bad_again, pts, queued_after_failure, resumed = asyncio.run(scenario())
    finally:
        logger.removeHandler(handler)

    assert good[0]
    assert bad == ([], pts) and bad_again == ([], pts)
    assert queued_after_failure == 0
    assert streams.split(_depacketize(resumed[0])) == streams.split(units[5])  # the next keyframe
    assert len(handler.messages) == 1  # and one line for it, not one per frame
    assert "RuntimeError" in handler.messages[0]
    assert "not-a-real-secret" not in handler.messages[0] and "rtsp://" not in handler.messages[0]


def test_an_on_failure_that_raises_does_not_get_out_either():
    def explode():
        raise ValueError("no")

    handoff = UnitHandoff()
    encoder = PassthroughEncoder(handoff, on_failure=explode)

    async def scenario():
        stream = PassthroughStream(width=64, height=48, slug="front")
        track = PassthroughTrack(stream, handoff)
        waiting = await _parked(track)
        stream.push(streams.encode_stream(frames=1)[0], 1_000)
        frame = await waiting
        handoff.put(frame.pts, Unit(nals=_Poison(), timestamp_ms=0, keyframe=False))
        return encoder.encode(frame, force_keyframe=True), frame.pts

    result, pts = asyncio.run(scenario())
    assert result == ([], pts)


def test_the_handoff_keeps_no_more_than_a_few_units():
    """A frame handed out and never encoded -- the sender was disabled, or
    stopped, in between -- must not hold its unit for the life of the track."""
    handoff = UnitHandoff()
    for pts in range(HANDOFF_MAX_UNITS + 10):
        handoff.put(pts, Unit(nals=(b"\x65",), timestamp_ms=pts, keyframe=True))
    assert handoff.take(0) is None  # the oldest went
    newest = HANDOFF_MAX_UNITS + 9
    assert handoff.take(newest).timestamp_ms == newest
    handoff.clear()
    assert handoff.take(newest - 1) is None


# --- the publisher -----------------------------------------------------------


class _HolderThatMustNotBeRead:
    def __init__(self):
        self.reads = 0

    def get(self):
        self.reads += 1
        return None


class _StreamSpy:
    """Stands where the camera's stream stands and writes down what the
    publisher and its track do with it."""

    def __init__(self):
        self.calls = []
        self._released = asyncio.Event()

    def attach(self, loop):
        self.calls.append("attach")

    def detach(self):
        self.calls.append("detach")
        self._released.set()

    def set_paused(self, paused):
        self.calls.append(("set_paused", paused))

    def resync(self):
        self.calls.append("resync")

    async def get(self):
        await self._released.wait()
        return None


def _passthrough_publisher(stream, *, holder=None, **kwargs):
    events = []
    connection = _SeamConnection(events=events)
    signal = _SeamSignal(events=events)

    def signal_factory(url, token, **callbacks):
        signal.callbacks = callbacks
        return signal

    publisher = PassthroughPublisher(
        passthrough=stream,
        holder=holder if holder is not None else LatestFrameHolder(),
        width=WIDTH, height=HEIGHT, fps=30, bitrate_kbps=1500,
        connection_factory=lambda ice_servers: connection,
        signal_factory=signal_factory,
        **kwargs,
    )
    return publisher, connection, signal, events


def test_the_publisher_brings_its_own_track_and_encoder_and_offers_h264_only():
    async def scenario():
        stream = _StreamSpy()
        publisher, connection, signal, events = _passthrough_publisher(stream)
        await publisher.start("wss://media.example", "room-1", "tok-1")
        track = connection.added_tracks[0]
        installed = getattr(connection.sender, SENDER_ENCODER_ATTR)
        preferences = connection.transceiver.preferences
        declared = [call for call in signal.calls if call[0] == "add_track"]
        await publisher.stop()
        return publisher, stream, track, installed, preferences, events, declared

    publisher, stream, track, installed, preferences, events, declared = asyncio.run(scenario())
    assert publisher.passthrough is stream
    assert isinstance(track, PassthroughTrack)
    assert isinstance(installed, PassthroughEncoder)
    # On the sender before the offer is made, and the offer is H.264 with its
    # retransmission entries: the seam's own promises, kept for this encoder.
    created = next(event for event in events if isinstance(event, tuple) and event[0] == "createOffer")
    assert created[1] is installed
    assert preferences == _h264_and_rtx()
    # What the track declares to the media server is the configured size and
    # bitrate; the bridge enforces neither on a stream it does not encode.
    assert declared == [("add_track", track.id, live.TRACK_NAME, WIDTH, HEIGHT, 1_500_000)]


def test_a_start_does_not_warn_about_a_keyframe_hook_it_has_no_use_for():
    """The base class warns when it cannot wake its track for a keyframe
    request. For this publisher that is not a shortcoming to report on every
    start: a request changes nothing, by design."""
    handler = _Lines()
    logger = logging.getLogger("fleetless_bridge.live")
    logger.addHandler(handler)

    async def scenario():
        publisher, connection, signal, events = _passthrough_publisher(_StreamSpy())
        send_keyframe = connection.sender._send_keyframe
        await publisher.start("wss://media.example", "room-1", "tok-1")
        untouched = connection.sender._send_keyframe == send_keyframe
        await publisher.stop()
        return untouched

    try:
        untouched = asyncio.run(scenario())
    finally:
        logger.removeHandler(handler)
    assert untouched
    assert not [line for line in handler.messages if "keyframe" in line.lower()]


def test_the_publisher_feeds_no_picture_from_any_holder():
    """A passthrough camera's holder has pictures in it: its snapshots. None
    of them may reach the stream, and nothing polls for one."""
    async def scenario():
        holder = _HolderThatMustNotBeRead()
        publisher, connection, signal, events = _passthrough_publisher(_StreamSpy(), holder=holder)
        await publisher.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.2)  # six feed ticks at 30 fps, if there were a feed
        await publisher.stop()
        return holder.reads

    assert asyncio.run(scenario()) == 0


def test_a_pause_and_a_resume_reach_the_stream():
    async def scenario():
        stream = _StreamSpy()
        publisher, connection, signal, events = _passthrough_publisher(stream)
        await publisher.start("wss://media.example", "room-1", "tok-1")
        track = connection.added_tracks[0]
        off = {"trackSid": "TR_camera", "subscribedQualities": [{"quality": "HIGH"}]}
        on = {"trackSid": "TR_camera", "subscribedQualities": [{"quality": "HIGH", "enabled": True}]}
        signal.fire("on_quality_update", off)
        signal.fire("on_quality_update", off)  # said twice: one pause
        signal.fire("on_quality_update", on)
        replaced = list(connection.sender.replaced)
        await publisher.stop()
        return stream.calls, replaced, track

    calls, replaced, track = asyncio.run(scenario())
    states = [call[1] for call in calls if isinstance(call, tuple)]
    # Paused, then resumed, in that order and never resumed in between.
    assert states[0] is True and states[-1] is False
    assert states.count(False) == 1
    # The sender stopped pulling and pulls again, as for any camera.
    assert replaced == [None, track]


def test_stopping_the_publisher_releases_the_stream():
    async def scenario():
        stream = _StreamSpy()
        publisher, connection, signal, events = _passthrough_publisher(stream)
        await publisher.start("wss://media.example", "room-1", "tok-1")
        track = connection.added_tracks[0]
        waiting = asyncio.ensure_future(track.recv())  # the sender, between two units
        await asyncio.sleep(0.01)
        await publisher.stop()
        with pytest.raises(MediaStreamError):
            await asyncio.wait_for(waiting, timeout=2.0)
        return stream.calls

    calls = asyncio.run(scenario())
    assert calls[0] == "attach" and "detach" in calls


def test_a_start_that_fails_never_touched_the_stream():
    async def scenario():
        stream = _StreamSpy()
        events = []
        signal = _SeamSignal(events=events, fail_connect=OSError("refused"))
        publisher = PassthroughPublisher(
            passthrough=stream, holder=LatestFrameHolder(),
            width=WIDTH, height=HEIGHT, fps=30, bitrate_kbps=1500,
            connection_factory=lambda ice_servers: _SeamConnection(events=events),
            signal_factory=lambda url, token, **callbacks: signal,
        )
        with pytest.raises(live.LiveStartError):
            await publisher.start("wss://media.example", "room-1", "tok-1")
        await publisher.stop()
        return stream.calls

    assert asyncio.run(scenario()) == []


# --- through the real sender (spec test 4) -----------------------------------


class _PacketSizes:
    """The size of every media packet handed to the transport, before SRTP:
    fixed header, header extensions and payload -- what `live.max_packet_bytes`
    counts."""

    def __init__(self, transport):
        self.sizes = []
        self.payloads = []
        self._send = transport._send_rtp
        transport._send_rtp = self._record

    async def _record(self, data):
        if not is_rtcp(data):
            self.sizes.append(len(data))
            self.payloads.append(RtpPacket.parse(data).payload)
        await self._send(data)


async def _source(stream, units, fps):
    """A camera: the units over and over, a new capture time each."""
    timestamp_ms = 1_000_000
    index = 0
    while True:
        stream.push(units[index % len(units)], timestamp_ms)
        index += 1
        timestamp_ms += 1000 // fps
        await asyncio.sleep(1 / fps)


def test_a_real_sender_forwards_the_units_and_a_real_receiver_decodes_the_pictures(units):
    """A `PassthroughPublisher` with real aiortc on both ends, of whichever
    version this distribution ships. Units go in at the stream; what comes
    out of the other connection's decoder are the pictures those units
    decode to, in order. Between the two: aiortc's sender, its packetizer,
    SRTP over the loopback, its jitter buffer, depacketizer and decoder --
    and no encoder."""
    expected = streams.decode_all(units)
    result = {}
    cap = 700
    live.set_max_packet_bytes(cap)

    async def scenario():
        receiving = RTCPeerConnection(RTCConfiguration(iceServers=[]))
        received = []
        receiving.on("track", received.append)
        sending = []

        def connection_factory(ice_servers):
            pc = RTCPeerConnection(RTCConfiguration(iceServers=[]))
            sending.append(pc)
            return pc

        stream = PassthroughStream(width=WIDTH, height=HEIGHT, slug="front")
        signal = _LoopbackSignal(receiving)
        publisher = PassthroughPublisher(
            passthrough=stream, holder=LatestFrameHolder(),
            width=WIDTH, height=HEIGHT, fps=30, bitrate_kbps=1000,
            connection_factory=connection_factory,
            signal_factory=lambda url, token, **callbacks: signal,
        )
        feed = None
        try:
            await publisher.start("wss://media.example", "room-1", "test-token")
            sender = sending[0].getSenders()[0]
            encoder = getattr(sender, SENDER_ENCODER_ATTR)
            recorder = _PacketSizes(sender.transport)
            await _until(lambda: received, 10.0, "the track on the receiving side")
            feed = asyncio.ensure_future(_source(stream, units, 30))
            pictures = []
            while len(pictures) < 30:
                frame = await asyncio.wait_for(received[0].recv(), timeout=20.0)
                pictures.append(frame.to_ndarray(format="bgr24"))
            result.update(
                pictures=pictures, sizes=list(recorder.sizes), payloads=list(recorder.payloads),
                offer=signal.offer_sdp,
                encoder=encoder, forwarded=encoder.units_forwarded, codec=encoder.codec,
            )
        finally:
            if feed is not None:
                feed.cancel()
            await publisher.stop()
            await receiving.close()

    asyncio.run(scenario())

    # The offer names H.264 and no other video codec.
    assert "H264/90000" in result["offer"] and "VP8/90000" not in result["offer"]

    # The first picture decoded is a keyframe's, and every one after it is
    # the next picture of the stream: nothing skipped, nothing repeated,
    # nothing re-encoded -- the arrays are equal, not close.
    pictures = result["pictures"]
    start = next(
        index for index, picture in enumerate(expected) if np.array_equal(picture, pictures[0])
    )
    assert start % 5 == 0, "the stream did not start at a keyframe"
    for offset, picture in enumerate(pictures):
        assert picture.shape == (HEIGHT, WIDTH, 3)
        assert np.array_equal(picture, expected[(start + offset) % len(expected)]), (
            "picture {} is not the stream's next one".format(offset)
        )

    # No packet exceeds `live.max_packet_bytes`, counted as the setting
    # counts: the whole RTP packet before SRTP. And the cap was in play:
    # the keyframes, several kilobytes of noise each, left as fragments
    # of more than half of it.
    assert len(result["sizes"]) > 100
    assert max(result["sizes"]) <= cap
    assert max(result["sizes"]) > cap // 2
    assert any(payload[0] & 0x1F == 28 for payload in result["payloads"])  # FU-A

    # And libx264 was never opened on the sending side.
    assert isinstance(result["encoder"], PassthroughEncoder)
    assert result["forwarded"] >= 30 and result["codec"] is None
