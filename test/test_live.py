# SPDX-License-Identifier: Apache-2.0
"""Live video publish to LiveKit — the orchestration only: join, publish,
negotiate, feed frames from camera.LatestFrameHolder, and what to report when
any of that fails. `signal_factory`/`connection_factory`/`track_factory` are
injectable seams shaped like client.py's own `connect`, so none of this needs
a real LiveKit server. The room conversation itself is test_livekit_signal.py's
subject; the one real smoke test against the dev server lives outside the
automated suite, same split as the physical webcam.

`CameraTrack` is tested for real, not through a fake: it is a local object
with no network of its own, and the two things it decides — when a frame is
produced and what timestamp it carries — are exactly what a fake would have
to invent.
"""
import asyncio
import logging
import time
import traceback

import numpy as np
import pytest
from aiortc.mediastreams import MediaStreamError

from fleetless_bridge.camera import LatestFrameHolder
from fleetless_bridge.live import (
    SENDER_ENCODER_ATTR,
    CameraTrack,
    LivePublisher,
    LiveStartError,
)
from fleetless_bridge.livekit_signal import IceServer, JoinInfo

ANSWER_SDP = "v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n"


# --- fakes -------------------------------------------------------------------


class _FakeEncoder:
    def __init__(self, target_bitrate=3_000_000):
        self.target_bitrate = target_bitrate


class _FakeSender:
    """The `RTCRtpSender` half `LivePublisher` touches: the track it pulls
    from, the keyframe request, and the private attribute the encoder lives
    behind."""

    def __init__(self, *, encoder=None, with_encoder_attr=True):
        self.replaced = []
        self.keyframes = 0
        if with_encoder_attr:
            setattr(self, SENDER_ENCODER_ATTR, encoder)

    def replaceTrack(self, track):
        self.replaced.append(track)

    def _send_keyframe(self):
        self.keyframes += 1


class _FakeOffer:
    def __init__(self, sdp):
        self.sdp = sdp
        self.type = "offer"


class _FakeConnection:
    def __init__(self, *, ice_servers=(), fail_offer=False, sender=None):
        self.ice_servers = ice_servers
        self.added_tracks = []
        self.local_descriptions = []
        self.remote_descriptions = []
        self.remote_candidates = []
        self.close_calls = 0
        self.connectionState = "new"
        self.localDescription = _FakeOffer("v=0\r\nlocal-offer\r\n")
        self._handlers = {}
        self._fail_offer = fail_offer
        self.sender = sender if sender is not None else _FakeSender()

    def on(self, event, callback):
        self._handlers[event] = callback
        return callback

    def fire(self, event, *args):
        handler = self._handlers.get(event)
        if handler is not None:
            handler(*args)

    def addTrack(self, track):
        self.added_tracks.append(track)
        return self.sender

    async def createOffer(self):
        if self._fail_offer:
            raise RuntimeError("no transceiver")
        return _FakeOffer("v=0\r\nlocal-offer\r\n")

    async def setLocalDescription(self, description):
        self.local_descriptions.append(description)

    async def setRemoteDescription(self, description):
        # The real one awaits I/O, which lets anything already queued on the
        # loop run *before* the description is applied. Without this yield the
        # harness cannot reach that state at all: a candidate handed over too
        # early would still be delivered late enough to be accepted, and the
        # test would pass for a publisher that does not hold candidates.
        await asyncio.sleep(0)
        self.remote_descriptions.append(description)

    async def addIceCandidate(self, candidate):
        # aiortc 1.14.0 refuses a candidate offered before the remote
        # description is applied — "addIceCandidate called without remote
        # description", warned and dropped. Modelled here rather than
        # described, because without it a candidate handed over too early
        # arrives anyway and the test cannot tell the two states apart.
        if not self.remote_descriptions:
            raise RuntimeError("addIceCandidate called without remote description")
        self.remote_candidates.append(candidate)

    async def close(self):
        self.close_calls += 1
        self.connectionState = "closed"


class _FakeSignal:
    """The `SignalClient` half `LivePublisher` drives, plus `fire_*` so a test
    can play the server's side of a callback."""

    def __init__(self, *, join=None, fail_connect=None, fail_add_track=None,
                 fail_offer=None, track_sid="TR_camera",
                 trickle_with_answer=()):
        self.calls = []
        self._trickle_with_answer = list(trickle_with_answer)
        self.close_calls = 0
        self.callbacks = {}
        #: Set by `_publisher`'s `signal_factory` the moment `start()` builds
        #: this signal -- (url, token) as start() actually forwarded them.
        self.connect_args = None
        self._join = join or JoinInfo(
            room_sid="RM_1", room_name="room-1", server_version="1.13.5",
            ice_servers=(IceServer(urls=("stun:stun.example:3478",)),),
        )
        self._fail_connect = fail_connect
        self._fail_add_track = fail_add_track
        self._fail_offer = fail_offer
        self._track_sid = track_sid

    async def connect(self):
        self.calls.append("connect")
        if self._fail_connect is not None:
            raise self._fail_connect
        return self._join

    async def add_track(self, cid, name, width, height, *, source="CAMERA", bitrate=None):
        self.calls.append(("add_track", cid, name, width, height, bitrate))
        if self._fail_add_track is not None:
            raise self._fail_add_track
        return {"cid": cid, "track": {"sid": self._track_sid, "name": name}}

    async def send_offer(self, sdp):
        self.calls.append(("send_offer", sdp))
        if self._fail_offer is not None:
            raise self._fail_offer
        # The real server trickles the moment it answers — before this side has
        # finished applying that answer. Reproduced here, not just described:
        # that window is where the candidates aiortc 1.14.0 refuses arrive.
        for init in self._trickle_with_answer:
            self.fire("on_trickle", init)
        return ANSWER_SDP

    async def close(self):
        self.close_calls += 1

    def fire(self, which, argument):
        callback = self.callbacks.get(which)
        if callback is not None:
            callback(argument)


class _FakeTrack:
    """Stands in for `CameraTrack` so a test can see exactly which frames were
    pushed, which a track being pulled by a real encoder cannot reveal."""

    def __init__(self, *, fail_first_n=0):
        self.id = "fake-track-id"
        self.pushed = []
        self.stop_calls = 0
        self._fail_first_n = fail_first_n

    def push(self, rgb):
        if self._fail_first_n > 0:
            self._fail_first_n -= 1
            raise RuntimeError("push failed")
        self.pushed.append(rgb)

    def stop(self):
        self.stop_calls += 1


def _solid_frame(height=4, width=6):
    return np.zeros((height, width, 3), dtype=np.uint8)


def _publisher(signal, connection, track, **kwargs):
    def signal_factory(url, token, **callbacks):
        # This hop has changed shape before (room.connect(url, token) ->
        # signal_factory(url, token) -> signal_url()) -- exactly what an
        # assertion is for. Checks start() forwarded what it was given, not,
        # say, room_name where token belongs (adjacent parameters, same type).
        signal.connect_args = (url, token)
        signal.callbacks = callbacks
        return signal

    def connection_factory(ice_servers):
        connection.ice_servers = tuple(ice_servers)
        return connection

    def track_factory(width, height, fps, bitrate_kbps):
        if isinstance(track, Exception):
            raise track
        return track

    return LivePublisher(
        holder=kwargs.pop("holder", LatestFrameHolder()),
        width=kwargs.pop("width", 64),
        height=kwargs.pop("height", 48),
        fps=kwargs.pop("fps", 10),
        bitrate_kbps=kwargs.pop("bitrate_kbps", 500),
        signal_factory=signal_factory,
        connection_factory=connection_factory,
        track_factory=track_factory,
        **kwargs,
    )


class _CapturingHandler(logging.Handler):
    """Collects everything this module logs, at any level.

    Not `caplog`: this suite has established repeatedly (`test_main.py`,
    `test_client_writer.py`, `test_livekit_signal.py`) that pytest disables
    propagation for every logger here, so a `caplog` assertion would pass
    unconditionally -- the worst state for a check whose job is noticing a
    missing line.
    """

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def _capturing():
    handler = _CapturingHandler()
    watched = logging.getLogger("fleetless_bridge.live")
    watched.addHandler(handler)
    watched.setLevel(logging.DEBUG)
    return handler, watched


def _started(**kwargs):
    """A publisher that has completed `start()`, with its three fakes."""
    signal = kwargs.pop("signal", None) or _FakeSignal()
    connection = kwargs.pop("connection", None) or _FakeConnection()
    track = kwargs.pop("track", None) or _FakeTrack()
    pub = _publisher(signal, connection, track, **kwargs)
    return pub, signal, connection, track


# --- start -------------------------------------------------------------------


def test_start_joins_publishes_and_negotiates():
    async def scenario():
        pub, signal, connection, track = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        await pub.stop()
        return signal, connection, track

    signal, connection, track = asyncio.run(scenario())
    # start() must forward exactly the url and token it was given -- not
    # room_name (adjacent parameter, same type), not a stale token from an
    # earlier call.
    assert signal.connect_args == ("wss://media.example", "tok-1")
    assert signal.calls == [
        "connect",
        # `addTrack` before the offer: LiveKit answers an offer for an
        # untold-about track with nothing at all -- order is the contract,
        # not a preference.
        ("add_track", "fake-track-id", "camera", 64, 48, 500_000),
        ("send_offer", "v=0\r\nlocal-offer\r\n"),
    ]
    assert connection.added_tracks == [track]
    assert len(connection.local_descriptions) == 1
    assert [d.sdp for d in connection.remote_descriptions] == [ANSWER_SDP]
    assert connection.remote_descriptions[0].type == "answer"


def test_the_configured_bitrate_reaches_add_track_in_bits_per_second():
    """`bitrate_kbps` is the constructor's unit; `add_track`'s `bitrate` is
    bits per second. This is the one call site doing that conversion -- a
    swapped factor would under- or over-declare the layer by 1000x, while
    every other test's default (500 kbps -> 500_000) stays right by
    coincidence."""

    async def scenario():
        pub, signal, connection, track = _started(bitrate_kbps=250)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await pub.stop()
        return signal

    signal = asyncio.run(scenario())
    assert ("add_track", "fake-track-id", "camera", 64, 48, 250_000) in signal.calls


def test_the_ice_servers_from_join_reach_the_connection_factory():
    """The cloud says where its TURN is, in `join`. A publisher that built its
    peer connection without them would work on the dev stack (host-networked,
    every candidate local) and fail on a robot behind NAT — the one case the
    servers exist for."""

    async def scenario():
        signal = _FakeSignal(join=JoinInfo(
            room_sid="RM_1", room_name="room-1",
            ice_servers=(
                IceServer(urls=("stun:stun.example:3478",)),
                IceServer(urls=("turn:turn.example:3478",), username="u", credential="c"),
            ),
        ))
        pub, signal, connection, _ = _started(signal=signal)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await pub.stop()
        return connection

    connection = asyncio.run(scenario())
    assert [s.urls for s in connection.ice_servers] == [
        ("stun:stun.example:3478",), ("turn:turn.example:3478",)
    ]
    assert connection.ice_servers[1].username == "u"
    assert connection.ice_servers[1].credential == "c"


def test_start_raises_live_start_error_when_the_join_fails():
    async def scenario():
        signal = _FakeSignal(fail_connect=RuntimeError("connection refused"))
        pub, signal, _, _ = _started(signal=signal)
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "tok-1")
        return signal

    signal = asyncio.run(scenario())
    assert signal.close_calls == 1  # nothing half-open is left behind


def test_a_join_failures_raw_message_reaches_neither_the_error_nor_its_traceback():
    """`token` is a credential, and LiveStartError's own docstring says its
    message becomes `bridgeCameraState.error.message`, sent to the cloud, not
    merely logged. The token rides in the signalling URL's query string, and a
    refused WebSocket handshake puts that whole URL into the library
    exception's text. So the message must not quote it **and** the exception
    must not chain it: a chained cause prints in full in every later
    traceback."""

    # Deliberately low-entropy and says what it is: a realistic secret here
    # is a secret-scanner finding forever, and an always-red instrument stops
    # being read. The test needs a distinctive string, not a realistic one.
    token = "not-a-real-token"

    async def scenario():
        signal = _FakeSignal(fail_connect=RuntimeError(
            "401, url=URL('ws://media.example/rtc?access_token=" + token + "')"
        ))
        pub, _, _, _ = _started(signal=signal)
        with pytest.raises(LiveStartError) as excinfo:
            await pub.start("wss://media.example", "room-1", token)
        error = excinfo.value
        return str(error), "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )

    message, rendered = asyncio.run(scenario())
    assert token not in message
    assert token not in rendered
    assert "RuntimeError" in message  # the type name is what is kept instead


def test_a_publish_failures_raw_message_reaches_neither_the_error_nor_its_traceback():
    """The same rule past the join. The session is authenticated by then, so an
    exception from here is less likely to carry the token — but "less likely"
    is not the standard when the message goes to the cloud."""

    token = "not-a-real-token"

    async def scenario():
        signal = _FakeSignal(fail_offer=RuntimeError(
            "offer refused for ws://media.example/rtc?access_token=" + token
        ))
        pub, _, _, _ = _started(signal=signal)
        with pytest.raises(LiveStartError) as excinfo:
            await pub.start("wss://media.example", "room-1", token)
        error = excinfo.value
        return str(error), "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )

    message, rendered = asyncio.run(scenario())
    assert token not in message
    assert token not in rendered


def test_a_failed_add_track_is_reported_and_tears_both_halves_down():
    async def scenario():
        signal = _FakeSignal(fail_add_track=RuntimeError("publish refused"))
        pub, signal, connection, track = _started(signal=signal)
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "tok-1")
        return signal, connection, track

    signal, connection, track = asyncio.run(scenario())
    # A track that could not be published must not leave a half-open session
    # behind — nothing left listening for it to clean up.
    assert signal.close_calls == 1
    assert connection.close_calls == 1
    assert track.stop_calls == 1


def test_a_failed_offer_is_reported_and_tears_both_halves_down():
    async def scenario():
        pub, signal, connection, _ = _started(
            connection=_FakeConnection(fail_offer=True)
        )
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "tok-1")
        return signal, connection

    signal, connection = asyncio.run(scenario())
    assert signal.close_calls == 1
    assert connection.close_calls == 1


def test_a_track_factory_failure_is_reported_as_live_start_error_and_tears_down():
    """A raise from `track_factory` used to escape `start()` as a plain
    exception — past the point the session was already open, which
    ros_runtime.py's `except live.LiveStartError` never caught, leaving a
    connected session neither `stop_live` nor `stop_all_live` could ever
    reach."""

    async def scenario():
        pub, signal, connection, _ = _started(
            track=RuntimeError("no camera hardware")
        )
        with pytest.raises(LiveStartError):
            await pub.start("wss://media.example", "room-1", "tok-1")
        return signal, connection

    signal, connection = asyncio.run(scenario())
    assert signal.close_calls == 1
    assert connection.close_calls == 1


# --- stop ---------------------------------------------------------------------


def test_stop_closes_the_connection_and_the_signalling_socket():
    async def scenario():
        pub, signal, connection, track = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        await pub.stop()
        return signal, connection, track

    signal, connection, track = asyncio.run(scenario())
    assert connection.close_calls == 1
    assert signal.close_calls == 1
    assert track.stop_calls == 1


def test_stop_before_start_does_not_raise():
    async def scenario():
        pub, _, _, _ = _started()
        await pub.stop()  # must not raise

    asyncio.run(scenario())


def test_stop_is_idempotent():
    async def scenario():
        pub, signal, connection, _ = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        await pub.stop()
        await pub.stop()
        return signal, connection

    signal, connection = asyncio.run(scenario())
    assert connection.close_calls == 1
    assert signal.close_calls == 1


# --- frame feed -----------------------------------------------------------------


def test_a_frame_already_in_the_holder_at_start_is_pushed():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, _, _, track = _started(holder=holder, fps=50)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.1)
        await pub.stop()
        return track

    track = asyncio.run(scenario())
    assert len(track.pushed) >= 1


def test_a_pushed_frame_is_rgb():
    """`_push_frame` takes BGR (the rest of this package's format); the
    encoder gets RGB24. Skip the swap and nothing downstream reports it -- a
    swapped frame encodes and decodes perfectly and only looks wrong."""

    async def scenario():
        holder = LatestFrameHolder()
        bgr = np.zeros((2, 2, 3), dtype=np.uint8)
        bgr[:, :, 0] = 255  # blue in BGR
        holder.set(bgr, timestamp_ms=100)
        pub, _, _, track = _started(holder=holder, fps=50)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.1)
        await pub.stop()
        return track

    track = asyncio.run(scenario())
    assert track.pushed
    # Blue in BGR must arrive as blue in RGB: the last channel, not the first.
    assert track.pushed[0][0, 0].tolist() == [0, 0, 255]


def test_the_same_frame_is_not_pushed_twice():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, _, _, track = _started(holder=holder, fps=50)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.15)  # several feed ticks, no new frame arrives
        await pub.stop()
        return track

    track = asyncio.run(scenario())
    assert len(track.pushed) == 1


def test_a_new_frame_replaces_the_old_one_in_the_next_push():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, _, _, track = _started(holder=holder, fps=50)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.05)
        holder.set(_solid_frame(), timestamp_ms=200)
        await asyncio.sleep(0.05)
        await pub.stop()
        return track

    track = asyncio.run(scenario())
    assert len(track.pushed) >= 2


def test_nothing_is_pushed_before_any_frame_exists():
    async def scenario():
        pub, _, _, track = _started(holder=LatestFrameHolder(), fps=50)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.1)
        await pub.stop()
        return track

    track = asyncio.run(scenario())
    assert track.pushed == []


def test_feed_frames_survives_a_push_failure_and_keeps_running():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        # Fails on the very first push; a crashed loop would never get to the
        # second, later frame at all.
        pub, _, _, track = _started(
            holder=holder, fps=50, track=_FakeTrack(fail_first_n=1)
        )
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.05)
        holder.set(_solid_frame(), timestamp_ms=200)
        await asyncio.sleep(0.05)
        await pub.stop()
        return track

    track = asyncio.run(scenario())
    # The first push failed and was swallowed; the second (a genuinely new
    # frame) still reached the track — proof the loop kept running rather than
    # dying silently on the first bad push.
    assert len(track.pushed) >= 1


# --- a loss must be reported, not inferred ---------------------------------


def test_a_signalling_socket_that_ends_invokes_on_lost():
    async def scenario():
        lost = []
        pub, signal, _, _ = _started(on_lost=lost.append)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_closed", "the signalling socket failed (ClientError)")
        await asyncio.sleep(0)
        await pub.stop()
        return lost

    lost = asyncio.run(scenario())
    assert len(lost) == 1
    assert "ClientError" in lost[0]


def test_a_failed_peer_connection_invokes_on_lost():
    async def scenario():
        lost = []
        pub, _, connection, _ = _started(on_lost=lost.append)
        await pub.start("wss://media.example", "room-1", "tok-1")
        connection.connectionState = "failed"
        connection.fire("connectionstatechange")
        await asyncio.sleep(0)
        await pub.stop()
        return lost

    lost = asyncio.run(scenario())
    assert len(lost) == 1
    assert "failed" in lost[0]


def test_a_server_initiated_leave_invokes_on_lost():
    async def scenario():
        lost = []
        pub, signal, _, _ = _started(on_lost=lost.append)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_leave", {"reason": "SERVER_SHUTDOWN"})
        await asyncio.sleep(0)
        await pub.stop()
        return lost

    lost = asyncio.run(scenario())
    assert len(lost) == 1
    assert "SERVER_SHUTDOWN" in lost[0]


def test_one_loss_seen_three_ways_is_reported_once():
    """Tearing down after a loss makes the *other* two notices fire in turn:
    closing the peer connection is a state change, closing the socket is a
    socket that ended. Without the latch the cloud hears about one loss three
    times, and `_on_live_lost` answers each with a `bridgeCameraState`."""

    async def scenario():
        lost = []
        pub, signal, connection, _ = _started(on_lost=lost.append)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_closed", "the signalling socket ended")
        connection.connectionState = "failed"
        connection.fire("connectionstatechange")
        signal.fire("on_leave", {"reason": "SERVER_SHUTDOWN"})
        await asyncio.sleep(0)
        await pub.stop()
        return lost

    lost = asyncio.run(scenario())
    assert len(lost) == 1


def test_calling_stop_does_not_invoke_on_lost():
    """A disconnect *we* asked for (stop()) is not a loss to report — the
    caller (ros_runtime.stop_live/stop_all_live) already knows and is already
    handling it; on_lost exists for the disconnect nobody asked for."""

    async def scenario():
        lost = []
        pub, signal, connection, _ = _started(on_lost=lost.append)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await pub.stop()
        # Both halves also report an end for a self-initiated stop; fired here
        # because the fakes do not do so on their own.
        signal.fire("on_closed", "closed by the bridge")
        connection.fire("connectionstatechange")
        return lost

    lost = asyncio.run(scenario())
    assert lost == []


def test_a_loss_tears_the_session_down_and_stops_the_feed():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, signal, connection, track = _started(
            holder=holder, fps=200, on_lost=lambda reason: None
        )
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.02)
        before = len(track.pushed)
        signal.fire("on_closed", "the signalling socket ended")
        await asyncio.sleep(0.1)
        after = len(track.pushed)
        await pub.stop()
        return before, after, signal, connection

    before, after, signal, connection = asyncio.run(scenario())
    # No more frames pushed into a track whose session is already gone, and
    # both halves closed exactly once across the loss and the stop after it.
    assert after == before
    assert connection.close_calls == 1
    assert signal.close_calls == 1


# --- remote ICE candidates -----------------------------------------------------


def test_a_remote_candidate_reaches_the_peer_connection():
    async def scenario():
        pub, signal, connection, _ = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_trickle", {
            "candidate": "candidate:1 1 udp 2130706431 10.0.0.1 42000 typ host",
            "sdpMid": "0",
            "sdpMLineIndex": 0,
        })
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await pub.stop()
        return connection

    connection = asyncio.run(scenario())
    assert len(connection.remote_candidates) == 1
    assert connection.remote_candidates[0].sdpMid == "0"


def test_a_candidate_that_arrives_before_the_answer_is_applied_is_not_dropped():
    """The server trickles as soon as it has answered. aiortc 1.14.0 refuses a
    candidate offered before `setRemoteDescription` has been applied and warns
    instead; 1.3.0 and 1.6.0 accept it, so the window is invisible on two of
    the three distributions and costs candidates on the third — and behind NAT
    the relay candidate can be one of them."""

    early = {"candidate": "candidate:9 1 udp 41885439 203.0.113.7 3478 typ relay",
             "sdpMid": "0", "sdpMLineIndex": 0}

    async def scenario():
        signal = _FakeSignal(trickle_with_answer=[early])
        pub, _, connection, _ = _started(signal=signal)
        await pub.start("wss://media.example", "room-1", "tok-1")
        # Ordering, not just arrival: the candidate must reach the connection
        # after the answer, which is the whole point of holding it.
        applied_before = len(connection.remote_descriptions)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await pub.stop()
        return connection, applied_before

    connection, applied_before = asyncio.run(scenario())
    assert applied_before == 1
    assert [c.foundation for c in connection.remote_candidates] == ["9"]


def test_an_unusable_remote_candidate_does_not_end_the_session():
    async def scenario():
        lost = []
        pub, signal, connection, _ = _started(on_lost=lost.append)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_trickle", {"candidate": "nonsense", "sdpMid": "0"})
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await pub.stop()
        return connection, lost

    connection, lost = asyncio.run(scenario())
    assert connection.remote_candidates == []
    assert lost == []


# --- subscribedQualityUpdate ---------------------------------------------------


def _quality(*enabled, track_sid="TR_camera"):
    return {
        "trackSid": track_sid,
        # protojson omits `false`, so an unsubscribed layer arrives with no
        # `enabled` key at all — which is what these fixtures reproduce.
        "subscribedQualities": [
            ({"quality": q, "enabled": True} if on else {"quality": q})
            for q, on in zip(("LOW", "MEDIUM", "HIGH"), enabled)
        ],
    }


def test_no_subscribed_layer_pauses_the_encoder_and_the_feed():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, signal, connection, track = _started(holder=holder, fps=200)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_quality_update", _quality(False, False, True))
        await asyncio.sleep(0.02)
        signal.fire("on_quality_update", _quality(False, False, False))
        before = len(track.pushed)
        holder.set(_solid_frame(), timestamp_ms=200)
        await asyncio.sleep(0.05)
        after = len(track.pushed)
        await pub.stop()
        return connection.sender, before, after

    sender, before, after = asyncio.run(scenario())
    assert sender.replaced[-1] is None  # the encoder has nothing to pull from
    assert after == before  # and the colour conversion stopped too


def test_an_all_off_update_with_no_prior_subscription_pauses_the_feed():
    """livekit-server v1.13.5, given a track published with no declared
    layers, sends exactly one all-off `subscribedQualityUpdate` and never
    another, whatever the real subscription state is -- a fact 3c.3's
    workaround handled by holding that message until a layer had once
    reported subscribed, before ever pausing. `add_track`'s `bitrate` (3c.4)
    declares one layer instead, fixing the problem at its source: a track
    published to an empty room now gets an honest all-off as its first
    update, on a real LiveKit server. A latch tuned to the old bug would
    still keep publishing into that empty room -- the one case pausing
    exists for -- so there is no latch any more, and the very first update
    this publisher sees is already enough to pause it."""

    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, signal, connection, track = _started(holder=holder, fps=200)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_quality_update", _quality(False, False, False))
        before = len(track.pushed)
        holder.set(_solid_frame(), timestamp_ms=200)
        await asyncio.sleep(0.05)
        after = len(track.pushed)
        await pub.stop()
        return connection.sender, before, after

    sender, before, after = asyncio.run(scenario())
    assert sender.replaced[-1] is None  # paused on the first update it ever saw
    assert after == before  # and the colour conversion stopped too


def test_a_subscribed_layer_resumes_the_feed_and_asks_for_a_keyframe():
    async def scenario():
        holder = LatestFrameHolder()
        holder.set(_solid_frame(), timestamp_ms=100)
        pub, signal, connection, track = _started(holder=holder, fps=200)
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_quality_update", _quality(False, False, True))
        await asyncio.sleep(0.02)
        signal.fire("on_quality_update", _quality(False, False, False))
        holder.set(_solid_frame(), timestamp_ms=200)
        await asyncio.sleep(0.02)
        while_paused = len(track.pushed)
        signal.fire("on_quality_update", _quality(False, False, True))
        holder.set(_solid_frame(), timestamp_ms=300)
        await asyncio.sleep(0.05)
        await pub.stop()
        return connection.sender, track, while_paused

    sender, track, while_paused = asyncio.run(scenario())
    assert sender.replaced == [None, track]
    assert sender.keyframes == 1
    # The frame set while paused was never pushed; the one set after the resume
    # was. Without the second half this passes for a publisher that resumed the
    # sender and left the feed loop paused for ever.
    assert while_paused == 1
    assert len(track.pushed) == 2


def test_a_quality_update_carrying_no_layers_changes_nothing():
    """`{}` and "every layer is off" look alike — protojson omits `false` — and
    they are opposites. A message with nothing in it must not pause a working
    stream."""

    async def scenario():
        pub, signal, connection, _ = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_quality_update", {"trackSid": "TR_camera"})
        signal.fire("on_quality_update", {"trackSid": "TR_camera",
                                          "subscribedQualities": []})
        await asyncio.sleep(0)
        await pub.stop()
        return connection.sender

    sender = asyncio.run(scenario())
    assert sender.replaced == []


def test_a_quality_update_about_another_track_is_ignored():
    async def scenario():
        pub, signal, connection, _ = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        # Our own track is subscribed, and an all-off update WOULD pause —
        # which is what makes the sid the only reason nothing happens below.
        signal.fire("on_quality_update", _quality(False, False, True))
        signal.fire("on_quality_update",
                    _quality(False, False, False, track_sid="TR_somebody_else"))
        await asyncio.sleep(0)
        await pub.stop()
        return connection.sender

    sender = asyncio.run(scenario())
    assert sender.replaced == []  # resume is a no-op when nothing was paused


def test_the_same_subscription_state_twice_does_not_touch_the_sender_twice():
    async def scenario():
        pub, signal, connection, _ = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_quality_update", _quality(False, False, True))
        signal.fire("on_quality_update", _quality(False, False, False))
        signal.fire("on_quality_update", _quality(False, False, False))
        await asyncio.sleep(0)
        await pub.stop()
        return connection.sender

    sender = asyncio.run(scenario())
    assert sender.replaced == [None]


def test_subscribed_codecs_are_read_as_well_as_subscribed_qualities():
    """A multi-codec publish reports the layers under `subscribedCodecs`
    instead. Reading only the flat list would pause a stream somebody is
    watching."""

    async def scenario():
        pub, signal, connection, _ = _started()
        await pub.start("wss://media.example", "room-1", "tok-1")
        signal.fire("on_quality_update", {
            "trackSid": "TR_camera",
            "subscribedCodecs": [
                {"codec": "vp8", "qualities": [{"quality": "HIGH", "enabled": True}]}
            ],
        })
        await asyncio.sleep(0)
        signal.fire("on_quality_update", {
            "trackSid": "TR_camera",
            "subscribedCodecs": [
                {"codec": "vp8", "qualities": [{"quality": "HIGH"}]}
            ],
        })
        await asyncio.sleep(0)
        await pub.stop()
        return connection.sender

    sender = asyncio.run(scenario())
    # The first said "watched" and the second did not. Reading only
    # `subscribedQualities` would have seen neither.
    assert sender.replaced == [None]


# --- the bitrate budget --------------------------------------------------------


def test_the_budget_is_applied_to_the_encoder():
    async def scenario():
        encoder = _FakeEncoder(target_bitrate=3_000_000)
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()
        return encoder

    encoder = asyncio.run(scenario())
    assert encoder.target_bitrate == 800_000


def test_a_below_budget_starting_value_is_raised_to_the_budget():
    """The old code only ever *lowered* the target (`if target >
    budget_bps`), so a budget above the encoder's construction-time default
    (VP8 500 kbps, H264 1 Mbps) was never reached -- 800 kbps configured,
    500 kbps encoded for the whole session, however long it ran. The fix sets
    the target unconditionally, so a below-budget starting value rises to
    meet it, within the codec's own clamp (which _FakeEncoder does not model,
    by design -- proved instead against the real aiortc encoder in
    tools/live-proof)."""

    async def scenario():
        encoder = _FakeEncoder(target_bitrate=500_000)  # aiortc's VP8 default
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()
        return encoder

    encoder = asyncio.run(scenario())
    assert encoder.target_bitrate == 800_000


def test_an_estimate_below_the_budget_is_reasserted_back_to_it():
    """A REMB that estimates less than the budget is not left alone: the
    budget is reasserted every tick regardless of direction, so the encoder's
    target tracks the configured budget, not whatever a receiver last
    reported. (What actually bounds the robot's uplink under real congestion
    is the REMB continuing to arrive and being re-read next tick, same as the
    raise-direction case above -- there is no separate "respect a lower REMB"
    path any more.)"""

    async def scenario():
        encoder = _FakeEncoder(target_bitrate=300_000)  # what a REMB does
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()
        return encoder

    encoder = asyncio.run(scenario())
    assert encoder.target_bitrate == 800_000


def test_the_budget_is_reasserted_after_the_receivers_estimate_overwrites_it():
    """aiortc writes the receiver's estimate straight onto the same attribute
    on every REMB, so a budget applied once survives until the first feedback
    packet and no longer."""

    async def scenario():
        encoder = _FakeEncoder(target_bitrate=3_000_000)
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        applied_once = encoder.target_bitrate
        encoder.target_bitrate = 2_500_000  # what a REMB does
        await asyncio.sleep(0.05)
        await pub.stop()
        return applied_once, encoder.target_bitrate

    applied_once, after_remb = asyncio.run(scenario())
    assert applied_once == 800_000
    assert after_remb == 800_000


def test_set_bitrate_kbps_moves_the_budget_the_loop_re_asserts():
    """Low-bandwidth mode's `reduce` lever. The budget used to be read once,
    before the loop, so a setter that only wrote the field would have changed
    nothing for the rest of the session."""

    async def scenario():
        encoder = _FakeEncoder(target_bitrate=3_000_000)
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        configured = encoder.target_bitrate
        pub.set_bitrate_kbps(300)
        await asyncio.sleep(0.05)
        reduced = encoder.target_bitrate
        pub.set_bitrate_kbps(800)
        await asyncio.sleep(0.05)
        await pub.stop()
        return configured, reduced, encoder.target_bitrate

    configured, reduced, restored = asyncio.run(scenario())
    assert (configured, reduced, restored) == (800_000, 300_000, 800_000)


def test_an_encoder_that_does_not_exist_yet_is_not_an_error():
    """The sender builds its encoder on the first frame it encodes, so a camera
    that has not produced one has a `None` there for as long as that lasts."""

    async def scenario():
        connection = _FakeConnection(sender=_FakeSender(encoder=None))
        pub, _, _, _ = _started(connection=connection, bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()

    asyncio.run(scenario())  # must not raise


def test_an_aiortc_that_hides_its_encoder_elsewhere_says_so():
    """The attribute is aiortc's own private spelling. A version that renames
    it must degrade to "the budget is not applied", said out loud, rather than
    to a silent full-rate publish."""

    async def scenario():
        connection = _FakeConnection(sender=_FakeSender(with_encoder_attr=False))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    warnings = [m for m in handler.messages if SENDER_ENCODER_ATTR in m]
    assert len(warnings) == 1
    assert "800" in warnings[0]  # the budget that is not being applied


def test_an_aiortc_that_keeps_its_encoder_where_expected_says_nothing():
    """The other half: a warning that fires on the ordinary case is a warning
    nobody reads, and this pair is what separates "the guard noticed" from
    "the guard always fires"."""

    async def scenario():
        connection = _FakeConnection(sender=_FakeSender(encoder=_FakeEncoder()))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    assert [m for m in handler.messages if SENDER_ENCODER_ATTR in m] == []


# `_FakeEncoder`'s class name matches neither key in `ENCODER_CLAMP_BPS`
# (by design -- it models what LivePublisher reads and writes, not aiortc's
# class hierarchy), so the clamp-logging tests below need a fake whose
# `type(...).__name__` is one aiortc actually uses. Subclassing rather than
# renaming keeps every test above unaffected.
class Vp8Encoder(_FakeEncoder):
    pass


class H264Encoder(_FakeEncoder):
    pass


def test_a_configured_bitrate_above_the_codecs_clamp_is_logged_once():
    """`bitrate_kbps: 2000` encodes at VP8's 1.5 Mbps ceiling with nothing
    at runtime saying so -- the docstring states the clamp, the log didn't.
    `_bitrate_interval_s` is fast enough that `_hold_bitrate_budget` ticks
    several times over the sleep below; the log line must appear exactly
    once regardless."""

    async def scenario():
        encoder = Vp8Encoder(target_bitrate=500_000)
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=2000,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.05)  # several ticks of the 0.01s hold interval
        await pub.stop()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    lines = [m for m in handler.messages if "clamp" in m]
    assert len(lines) == 1, lines
    assert "2000" in lines[0] and "2000000" in lines[0]  # configured kbps and bps
    assert "1500000" in lines[0]  # VP8's own ceiling
    assert "Vp8Encoder" in lines[0]


def test_a_configured_bitrate_within_the_codecs_clamp_is_not_logged():
    """The control for the test above: a budget inside VP8's own bounds is
    the ordinary case, and must not print anything about a clamp."""

    async def scenario():
        encoder = Vp8Encoder(target_bitrate=500_000)
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=800,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.05)
        await pub.stop()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    assert [m for m in handler.messages if "clamp" in m] == []


def test_a_configured_bitrate_above_h264s_clamp_names_h264_not_vp8():
    """The clamp differs per codec (VP8 1.5 Mbps, H264 3 Mbps) -- proves the
    log reads the negotiated encoder's own class rather than always
    reporting VP8's bound regardless of which codec is actually in use."""

    async def scenario():
        encoder = H264Encoder(target_bitrate=1_000_000)
        connection = _FakeConnection(sender=_FakeSender(encoder=encoder))
        pub, _, _, _ = _started(connection=connection, bitrate_kbps=4000,
                                bitrate_interval_s=0.01)
        await pub.start("wss://media.example", "room-1", "tok-1")
        await asyncio.sleep(0.03)
        await pub.stop()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    lines = [m for m in handler.messages if "clamp" in m]
    assert len(lines) == 1, lines
    assert "H264Encoder" in lines[0]
    assert "3000000" in lines[0]  # H264's own ceiling, not VP8's


# --- CameraTrack ---------------------------------------------------------------


def test_the_track_produces_nothing_before_the_camera_has():
    """No black rectangle: a viewer watching one and believing it is live is
    the failure the whole live path exists to avoid."""

    async def scenario():
        track = CameraTrack(4, 6, 30)
        pending = asyncio.ensure_future(track.recv())
        await asyncio.sleep(0.05)
        started = pending.done()
        track.push(np.zeros((6, 4, 3), dtype=np.uint8))
        frame = await asyncio.wait_for(pending, 1.0)
        return started, frame

    started, frame = asyncio.run(scenario())
    assert started is False
    assert (frame.width, frame.height) == (4, 6)


def test_the_track_hands_over_the_pixels_it_was_given():
    async def scenario():
        track = CameraTrack(2, 2, 30)
        rgb = np.zeros((2, 2, 3), dtype=np.uint8)
        rgb[:, :, 0] = 200  # red
        track.push(rgb)
        frame = await track.recv()
        return frame.to_ndarray(format="rgb24")

    out = asyncio.run(scenario())
    assert out[0, 0].tolist() == [200, 0, 0]


def test_the_track_paces_itself_at_the_configured_rate():
    async def scenario():
        track = CameraTrack(2, 2, 20)  # 50 ms apart
        track.push(np.zeros((2, 2, 3), dtype=np.uint8))
        await track.recv()
        started = time.monotonic()
        await track.recv()
        return time.monotonic() - started

    elapsed = asyncio.run(scenario())
    assert elapsed >= 0.04


def test_a_gap_does_not_become_a_burst_of_backdated_frames():
    """The reason the timestamps are not `VideoStreamTrack.next_timestamp`'s:
    that helper advances a fixed step per call and sleeps the difference to
    wall time, so after a gap it returns frames as fast as the encoder can take
    them, each stamped in the past. Here a gap is a gap."""

    async def scenario():
        track = CameraTrack(2, 2, 20)
        track.push(np.zeros((2, 2, 3), dtype=np.uint8))
        first = await track.recv()
        await asyncio.sleep(0.3)  # the encoder was not pulling
        started = time.monotonic()
        second = await track.recv()
        immediate = time.monotonic() - started
        third_started = time.monotonic()
        await track.recv()
        third = time.monotonic() - third_started
        return first.pts, second.pts, immediate, third

    first_pts, second_pts, immediate, third = asyncio.run(scenario())
    # The frame after the gap is produced at once (nothing to catch up on) and
    # carries the time that actually passed, not one fixed step.
    assert immediate < 0.05
    assert second_pts - first_pts > 0.25 * 90000
    # And the one after it is paced normally again rather than racing.
    assert third >= 0.04


def test_stopping_the_track_wakes_a_recv_waiting_for_the_first_frame():
    """Otherwise the sender's own task never finishes and `pc.close()` waits
    for it — a stop that hangs on a camera that never produced a frame."""

    async def scenario():
        track = CameraTrack(2, 2, 30)
        pending = asyncio.ensure_future(track.recv())
        await asyncio.sleep(0.01)
        track.stop()
        with pytest.raises(MediaStreamError):
            await asyncio.wait_for(pending, 1.0)

    asyncio.run(scenario())


def test_a_stopped_track_refuses_to_produce_more_frames():
    async def scenario():
        track = CameraTrack(2, 2, 30)
        track.push(np.zeros((2, 2, 3), dtype=np.uint8))
        await track.recv()
        track.stop()
        with pytest.raises(MediaStreamError):
            await track.recv()

    asyncio.run(scenario())
