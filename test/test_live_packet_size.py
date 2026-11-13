# SPDX-License-Identifier: Apache-2.0
"""The largest RTP packet the bridge sends is `live.max_packet_bytes`.

aiortc packetizes VP8 and H.264 against a module constant that caps the
*payload*; the sender then adds the RTP header and a header extension block,
and a retransmission adds two bytes more. `live.set_max_packet_bytes` assigns
that constant so that the whole packet, not the payload, stays under the
setting. These tests measure whole packets, with aiortc's real encoders and
its real sender, and they are the arbiter of `RTP_OVERHEAD_BYTES`: an aiortc
that starts writing more bytes around the payload fails here instead of
exceeding the cap on a robot, where the only symptom is a viewer who never
gets a frame.
"""
import asyncio
import logging
import subprocess
import sys
from fractions import Fraction

import numpy as np
import pytest
from aiortc import RTCConfiguration, RTCPeerConnection, RTCRtpSender
from aiortc.codecs import h264, vpx
from aiortc.rtcrtpparameters import RTCRtpHeaderExtensionParameters, RTCRtpParameters
from aiortc.rtcrtpsender import RTP_HISTORY_SIZE
from aiortc.rtp import HeaderExtensionsMap, RtpPacket, is_rtcp, wrap_rtx
from av import VideoFrame

from fleetless_bridge import live, live_settings
from fleetless_bridge.live import (
    RTP_OVERHEAD_BYTES,
    SRTP_MAX_PACKET_BYTES,
    CameraTrack,
    set_max_packet_bytes,
)
from fleetless_bridge.live_settings import MAX_PACKET_BYTES_MAX
from fleetless_bridge.protocol import LIVE_DEFAULTS

DEFAULT = LIVE_DEFAULTS["max_packet_bytes"]
WIDTH, HEIGHT = 1024, 768

#: The two header extensions aiortc negotiates for video and writes on every
#: packet. Asserted against its own capabilities below, so a version that
#: grows a third is noticed here and not only as a few more bytes.
MID_URI = "urn:ietf:params:rtp-hdrext:sdes:mid"
ABS_SEND_TIME_URI = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time"

ENCODERS = {"vp8": vpx.Vp8Encoder, "h264": h264.H264Encoder}
MIME = {"vp8": "video/VP8", "h264": "video/H264"}


@pytest.fixture(autouse=True)
def _the_default_afterwards():
    """The constant is process-wide. Every test here leaves it where an
    import of `live.py` puts it, and with the once-only warning re-armed."""
    yield
    live._cap_not_applied.clear()
    set_max_packet_bytes(DEFAULT)


def _noise(seed: int):
    """An image no encoder compresses well: every frame is many packets."""
    return np.random.default_rng(seed).integers(0, 255, (HEIGHT, WIDTH, 3), dtype=np.uint8)


def _frame(seed: int, pts: int = 0) -> VideoFrame:
    frame = VideoFrame.from_ndarray(_noise(seed), format="rgb24")
    frame.pts = pts
    frame.time_base = Fraction(1, 90000)
    return frame


def _keyframe_payloads(codec: str, seed: int = 1):
    """A fresh encoder per call: its first frame is a keyframe on every
    aiortc version, whatever `force_keyframe` does on that version."""
    payloads, _timestamp = ENCODERS[codec]().encode(_frame(seed), force_keyframe=True)
    return payloads


def _extension_map() -> HeaderExtensionsMap:
    extensions = HeaderExtensionsMap()
    extensions.configure(
        RTCRtpParameters(
            headerExtensions=[
                RTCRtpHeaderExtensionParameters(id=1, uri=MID_URI),
                RTCRtpHeaderExtensionParameters(id=2, uri=ABS_SEND_TIME_URI),
            ]
        )
    )
    return extensions


def _serialized_sizes(payloads, mid: str = "0"):
    """`(packet, retransmission)` lengths, built the way `RTCRtpSender` builds
    them: an `RtpPacket` with abs-send-time and mid, serialized through the
    header extension map, and the same packet through `wrap_rtx`."""
    extensions = _extension_map()
    sizes = []
    for index, payload in enumerate(payloads):
        packet = RtpPacket(payload_type=96, sequence_number=4000 + index, timestamp=90000)
        packet.ssrc = 1234
        packet.payload = payload
        packet.marker = 1 if index == len(payloads) - 1 else 0
        packet.extensions.abs_send_time = 0x00ABCDEF
        packet.extensions.mid = mid
        retransmission = wrap_rtx(packet, payload_type=97, sequence_number=index, ssrc=5678)
        sizes.append(
            (len(packet.serialize(extensions)), len(retransmission.serialize(extensions)))
        )
    return sizes


# --- what the constant is ------------------------------------------------------


def test_importing_live_applies_the_default_before_any_sender_runs():
    """No sender ever runs under aiortc's own 1300. A fresh interpreter, so
    the answer is about the import and not about what a neighbouring test
    left behind."""
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "import fleetless_bridge.live as live\n"
            "from aiortc.codecs import h264, vpx\n"
            "print(vpx.PACKET_MAX, h264.PACKET_MAX, live.RTP_OVERHEAD_BYTES)",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    assert out == [str(DEFAULT - RTP_OVERHEAD_BYTES)] * 2 + [str(RTP_OVERHEAD_BYTES)]
    assert DEFAULT == 1200


def test_setting_the_value_assigns_both_packetizers():
    assert set_max_packet_bytes(1000) == 1000
    assert vpx.PACKET_MAX == 1000 - RTP_OVERHEAD_BYTES
    assert h264.PACKET_MAX == 1000 - RTP_OVERHEAD_BYTES


def test_the_ceiling_of_the_setting_is_under_what_srtp_protects():
    """Why the bounds end at 1350: with every byte of the margin spent, a
    packet under the highest setting is still one the SRTP layer takes."""
    assert MAX_PACKET_BYTES_MAX == 1350
    assert MAX_PACKET_BYTES_MAX <= SRTP_MAX_PACKET_BYTES == 1356
    assert set_max_packet_bytes(MAX_PACKET_BYTES_MAX) == MAX_PACKET_BYTES_MAX


@pytest.mark.parametrize("setting", [1357, 1400])
def test_a_raised_ceiling_would_still_stop_at_what_srtp_protects(monkeypatch, setting):
    """The backstop. If the bounds are ever raised past the library's limit,
    the bridge packetizes for the limit: one packet above it ends the stream,
    and a cap that is merely lower than asked ends nothing."""
    monkeypatch.setattr(live_settings, "MAX_PACKET_BYTES_MAX", 1400)
    assert set_max_packet_bytes(setting) == SRTP_MAX_PACKET_BYTES
    assert vpx.PACKET_MAX == h264.PACKET_MAX == SRTP_MAX_PACKET_BYTES - RTP_OVERHEAD_BYTES


def test_the_srtp_layer_takes_a_packet_of_the_size_the_bridge_stops_at():
    """`SRTP_MAX_PACKET_BYTES` is pylibsrtp's limit, read off its behaviour.
    If a version takes less, the clamp is too high and this fails here
    rather than as a stream that ends on its first keyframe."""
    from pylibsrtp import Policy, Session

    session = Session(policy=Policy(key=bytes(30), ssrc_type=Policy.SSRC_ANY_OUTBOUND))
    header = bytes([0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1])
    packet = header + bytes(SRTP_MAX_PACKET_BYTES - len(header))
    assert len(session.protect(packet)) == SRTP_MAX_PACKET_BYTES + 10


def test_the_two_extensions_measured_here_are_the_ones_aiortc_writes():
    """The sizes below are built with mid and abs-send-time. If aiortc offers
    a third extension for video, the sender writes it and these measurements
    no longer describe a real packet."""
    offered = {e.uri for e in RTCRtpSender.getCapabilities("video").headerExtensions}
    assert offered == {MID_URI, ABS_SEND_TIME_URI}


@pytest.mark.parametrize("value", [575, 1351, 1400, 0, 32, -1, 1200.0, "1200", True, None])
def test_a_value_outside_the_bounds_is_refused_and_changes_nothing(value):
    """The resolver refuses these long before; this is the last door. A
    payload cap of zero or less would not send small packets, it would loop
    forever inside aiortc's packetizer."""
    set_max_packet_bytes(1000)
    with pytest.raises(ValueError) as refused:
        set_max_packet_bytes(value)
    assert str(refused.value) == "live.max_packet_bytes must be an integer in [576, 1350]"
    assert vpx.PACKET_MAX == h264.PACKET_MAX == 1000 - RTP_OVERHEAD_BYTES


# --- packet size, real codecs --------------------------------------------------


@pytest.mark.parametrize("setting", [1200, 576, 1350])
@pytest.mark.parametrize("codec", ["vp8", "h264"])
def test_no_serialized_packet_of_a_keyframe_exceeds_the_setting(codec, setting):
    set_max_packet_bytes(setting)
    payloads = _keyframe_payloads(codec)
    sizes = _serialized_sizes(payloads)

    assert len(payloads) > 10, "the frame was not fragmented; nothing was measured"
    largest_packet = max(plain for plain, _ in sizes)
    largest_retransmission = max(rtx for _, rtx in sizes)
    assert largest_packet <= setting
    assert largest_retransmission <= setting
    # A retransmission carries the original sequence number in front of the
    # payload: two bytes, on every packet.
    assert all(rtx == plain + 2 for plain, rtx in sizes)

    # The cap is used, not merely respected: a constant ten times too large
    # would pass everything above and send packets nobody needs to be that
    # small.
    largest_payload = max(len(p) for p in payloads)
    assert largest_payload <= setting - RTP_OVERHEAD_BYTES
    assert largest_retransmission <= SRTP_MAX_PACKET_BYTES
    if codec == "vp8":
        # VP8 fills every packet but the last.
        assert largest_payload == setting - RTP_OVERHEAD_BYTES
        assert setting - largest_retransmission < RTP_OVERHEAD_BYTES
    else:
        # H.264 splits a NAL unit into fragments of equal size, so its
        # largest packet is between half the cap and the cap.
        assert largest_payload > (setting - RTP_OVERHEAD_BYTES) // 2


@pytest.mark.parametrize("codec", ["vp8", "h264"])
def test_without_the_cap_the_same_frame_exceeds_1280_on_the_wire(codec):
    """The defect, kept as a control: under aiortc's own 1300 a packet is
    1324 bytes before SRTP, and 1362 with the SRTP tag, UDP and IPv4 — or
    1382 over IPv6. If this stops being true the tests above prove less
    than they say."""
    vpx.PACKET_MAX = h264.PACKET_MAX = 1300
    largest = max(plain for plain, _ in _serialized_sizes(_keyframe_payloads(codec)))
    assert largest > 1280
    assert largest + 10 + 8 + 20 > 1280


@pytest.mark.parametrize("mid", ["0", "12", "123", "1234567"])
def test_the_overhead_covers_a_longer_mid(mid):
    """aiortc names its media sections `0`, `1`, … when it offers, which the
    bridge always does. The margin in `RTP_OVERHEAD_BYTES` covers a mid of up
    to seven characters; this is where an eighth would show."""
    set_max_packet_bytes(DEFAULT)
    sizes = _serialized_sizes(_keyframe_payloads("vp8"), mid=mid)
    assert max(rtx for _, rtx in sizes) <= DEFAULT


# --- a running encoder ---------------------------------------------------------


@pytest.mark.parametrize("codec", ["vp8", "h264"])
def test_a_change_holds_from_the_next_frame_of_a_running_encoder(codec):
    """Both packetizers read the constant at call time, so an encoder that
    already exists obeys a value set after it was built."""
    encoder = ENCODERS[codec]()
    set_max_packet_bytes(1200)
    first, _ = encoder.encode(_frame(1, pts=0), force_keyframe=True)
    assert 576 < max(rtx for _, rtx in _serialized_sizes(first)) <= 1200

    set_max_packet_bytes(576)
    second, _ = encoder.encode(_frame(2, pts=3000), force_keyframe=True)
    assert len(second) > 10
    assert max(rtx for _, rtx in _serialized_sizes(second)) <= 576

    set_max_packet_bytes(1350)
    third, _ = encoder.encode(_frame(3, pts=6000), force_keyframe=True)
    assert 1200 < max(rtx for _, rtx in _serialized_sizes(third)) <= 1350


# --- degradation ---------------------------------------------------------------


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def live_log():
    """What `fleetless_bridge.live` logs, on a handler of the test's own."""
    handler = _Records()
    watched = logging.getLogger("fleetless_bridge.live")
    watched.addHandler(handler)
    try:
        yield handler.records
    finally:
        watched.removeHandler(handler)


@pytest.mark.parametrize("missing, other", [(vpx, h264), (h264, vpx)])
def test_a_codec_module_without_the_constant_is_one_warning_and_no_exception(
    monkeypatch, live_log, missing, other
):
    """A future aiortc that packetizes some other way. The bridge cannot cap
    that codec, says so once, and keeps capping the other one."""
    monkeypatch.delattr(missing, "PACKET_MAX")
    set_max_packet_bytes(1000)
    set_max_packet_bytes(900)
    assert [r.levelno for r in live_log] == [logging.WARNING]
    assert missing.__name__ in live_log[0].getMessage()
    assert "not applied" in live_log[0].getMessage()
    assert not hasattr(missing, "PACKET_MAX")
    assert other.PACKET_MAX == 900 - RTP_OVERHEAD_BYTES


def test_a_codec_module_that_cannot_be_imported_is_the_same_warning(monkeypatch, live_log):
    monkeypatch.setattr(live, "_PACKETIZERS", ("aiortc.codecs.gone", "aiortc.codecs.h264"))
    set_max_packet_bytes(1000)
    set_max_packet_bytes(1000)
    assert [r.levelno for r in live_log] == [logging.WARNING]
    assert "aiortc.codecs.gone" in live_log[0].getMessage()
    assert h264.PACKET_MAX == 1000 - RTP_OVERHEAD_BYTES


def test_with_both_packetizers_present_nothing_is_logged(live_log):
    set_max_packet_bytes(1000)
    assert live_log == []


# --- what the sender really sends ----------------------------------------------


class _Recorder:
    """Every RTP buffer handed to the transport, before SRTP: its length, its
    sequence number and the frame it belongs to."""

    def __init__(self, transport) -> None:
        self.packets = []  # (length, sequence_number, rtp_timestamp, payload_type)
        self._send = transport._send_rtp
        transport._send_rtp = self._record

    async def _record(self, data: bytes) -> None:
        if not is_rtcp(data):
            self.packets.append(
                (
                    len(data),
                    int.from_bytes(data[2:4], "big"),
                    int.from_bytes(data[4:8], "big"),
                    data[1] & 0x7F,
                )
            )
        await self._send(data)

    def lengths(self, start: int = 0):
        return [length for length, _, _, _ in self.packets[start:]]

    def frames(self, start: int = 0):
        seen = []
        for _, _, timestamp, _ in self.packets[start:]:
            if timestamp not in seen:
                seen.append(timestamp)
        return seen


async def _until(condition, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while not condition():
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("timed out waiting for " + what)
        await asyncio.sleep(0.02)


async def _feed(track: CameraTrack) -> None:
    seed = 100
    while True:
        track.push(_noise(seed))
        seed += 1
        await asyncio.sleep(1 / 15)


async def _session(codec: str, body) -> None:
    """Two peer connections in one process, the bridge's `CameraTrack` on the
    sending side. No ICE servers: the two find each other on the host's own
    addresses, and the test needs no network."""
    sending = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    receiving = RTCPeerConnection(RTCConfiguration(iceServers=[]))
    track = CameraTrack(WIDTH, HEIGHT, 15)
    feed = asyncio.ensure_future(_feed(track))
    try:
        sender = sending.addTrack(track)
        sending.getTransceivers()[0].setCodecPreferences(
            [
                c
                for c in RTCRtpSender.getCapabilities("video").codecs
                if c.mimeType in (MIME[codec], "video/rtx")
            ]
        )
        await sending.setLocalDescription(await sending.createOffer())
        await receiving.setRemoteDescription(sending.localDescription)
        await receiving.setLocalDescription(await receiving.createAnswer())
        await sending.setRemoteDescription(receiving.localDescription)
        recorder = _Recorder(sender.transport)
        await body(sender, recorder)
    finally:
        feed.cancel()
        track.stop()
        await sending.close()
        await receiving.close()


@pytest.mark.parametrize("setting", [1200, 576, 1350])
@pytest.mark.parametrize("codec", ["vp8", "h264"])
def test_no_buffer_handed_to_the_transport_exceeds_the_setting(codec, setting):
    """What the sender really sends, a forced keyframe and a retransmission
    included."""
    result = {}

    async def body(sender, recorder):
        await _until(lambda: len(recorder.frames()) >= 3, 20.0, "the first frames")

        # A forced keyframe, as a viewer's PLI asks for one.
        before = len(recorder.packets)
        sender._send_keyframe()
        await _until(
            lambda: len(recorder.packets) - before > 30 and len(recorder.frames(before)) >= 3,
            20.0,
            "the forced keyframe",
        )

        # One retransmission, of the largest packet the sender still holds.
        # It keeps the last `RTP_HISTORY_SIZE` packets and answers a request
        # for an older one with nothing; a keyframe at the smallest setting
        # is more packets than that, so the largest packet of the whole
        # session may already be gone. Half the history is safely inside it.
        # Nothing is awaited between choosing the packet and asking for it,
        # so the sender cannot have moved on in between.
        media = recorder.packets[0][3]
        held = [p for p in recorder.packets[-(RTP_HISTORY_SIZE // 2):] if p[3] == media]
        largest = max(held)
        count = len(recorder.packets)
        await sender._retransmit(largest[1])
        result["largest"] = largest
        result["retransmission"] = next(
            (p for p in recorder.packets[count:] if p[3] != media), None
        )
        # Two seconds at 15 frames a second.
        await _until(lambda: len(recorder.frames()) >= 30, 30.0, "a few seconds of video")
        result["lengths"] = recorder.lengths()
        result["media"] = [p[0] for p in recorder.packets if p[3] == media]

    set_max_packet_bytes(setting)
    asyncio.run(_session(codec, body))

    lengths = result["lengths"]
    assert len(lengths) > 100, "too few packets to say anything"
    assert max(lengths) <= setting
    # The retransmission went out under the RTX payload type and is the
    # original plus its two-byte sequence number.
    assert result["retransmission"] is not None, "the sender retransmitted nothing"
    assert result["retransmission"][0] == result["largest"][0] + 2
    assert result["retransmission"][0] <= setting
    assert result["retransmission"][0] in lengths
    # And the largest packet of the whole session, retransmitted, would fit
    # too: the two bytes are the same on every packet.
    assert max(result["media"]) + 2 <= setting
    # The cap is used. VP8 fills its packets to within the constant's margin;
    # H.264 cuts a NAL unit into equal fragments, between half the cap and
    # the cap.
    if codec == "vp8":
        assert setting - max(lengths) < RTP_OVERHEAD_BYTES
    else:
        assert max(lengths) > setting // 2


def test_a_change_holds_from_the_next_frame_of_a_running_stream():
    """`ros2 param set` on a robot with viewers: the packets of the next frame
    obey the new value, with no renegotiation and no restart."""
    result = {}

    async def body(sender, recorder):
        await _until(lambda: len(recorder.frames()) >= 3, 20.0, "the first frames")
        result["before"] = recorder.lengths()

        set_max_packet_bytes(576)
        changed_at = len(recorder.packets)
        # The frame in flight was packetized under the old value and may
        # still be leaving; everything from the frame after it is new.
        await _until(lambda: len(recorder.frames(changed_at)) >= 2, 20.0, "the next frame")
        in_flight = recorder.frames(changed_at)[0]
        settled = next(
            i for i in range(changed_at, len(recorder.packets))
            if recorder.packets[i][2] != in_flight
        )
        sender._send_keyframe()
        await _until(
            lambda: len(recorder.packets) - settled > 60 and len(recorder.frames(settled)) >= 4,
            20.0,
            "frames under the new value",
        )
        result["after"] = recorder.lengths(settled)

    set_max_packet_bytes(1200)
    asyncio.run(_session("vp8", body))

    assert 576 < max(result["before"]) <= 1200
    assert max(result["after"]) <= 576
    assert 576 - max(result["after"]) < RTP_OVERHEAD_BYTES
