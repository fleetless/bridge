# SPDX-License-Identifier: Apache-2.0
"""The LiveKit signalling client: the tag walker, and the conversation.

Two halves, and they fail for different reasons.

**The walker** is pure and is tested against files. The recorded `join` and
`refreshToken` came off a real livekit-server v1.13.5 and are the only evidence
that this protocol looks the way this module says it does; everything else in
`test/fixtures/livekit/` is built by `livekit_fixtures.py` and regenerated here,
so a hand-edited fixture is red. The interleaved frame matters most: the walker
exists so an unknown field cannot stop it reading the ones it knows, and a
suite fed only today's frame would say nothing about that.

**The conversation** is tested against a fake socket with the same four calls
the real one has, which is what makes `connect` an injectable seam. What it can
prove without a server: the first frame is text, the reply-matching is by `cid`
and not by arrival order, an unknown message key is ignored rather than raised,
a callback that raises does not kill the read loop, and every pending wait is
failed when the socket ends. What it cannot: that a real LiveKit server accepts
any of it. Only a round trip against a running server answers that; no unit
test stands in for one.
"""
import asyncio
import json
import logging
from types import SimpleNamespace

import aiohttp
import pytest

from livekit_fixtures import built_fixtures, load
from fleetless_bridge import __version__
from fleetless_bridge.livekit_signal import (
    DEFAULT_PING_INTERVAL_S,
    PROTOCOL_VERSION,
    IceServer,
    JoinInfo,
    SignalClient,
    SignalClosed,
    SignalConnectError,
    SignalProtocolError,
    SignalTimeout,
    _SignalSocket,
    parse_join,
    pb_fields,
    signal_url,
)

# --------------------------------------------------------------------------
# The walker
# --------------------------------------------------------------------------

#: What the recorded frame says, field by field. Written out rather than
#: compared against a re-decode of the same bytes: a test that decodes the
#: fixture twice and compares the two answers passes for a walker that returns
#: the same wrong thing every time.
RECORDED = JoinInfo(
    room_sid="RM_xBGV4aTs9RDD",
    room_name="spike-humble-vp8-11742",
    server_version="1.13.5",
    subscriber_primary=False,
    ice_servers=(
        IceServer(
            urls=(
                "stun:global.stun.twilio.com:3478",
                "stun:stun.l.google.com:19302",
                "stun:stun1.l.google.com:19302",
            ),
            username="",
            credential="",
        ),
    ),
    ping_interval_s=5,
    ping_timeout_s=15,
)


def test_the_recorded_join_frame_decodes_field_by_field():
    assert parse_join(load("join-recorded.txt")) == RECORDED


def test_the_server_version_comes_from_server_info_and_not_from_the_flat_field():
    # `join` carries the version twice: the deprecated flat `server_version`
    # (field 4) and `serverInfo.version` (field 12, sub-field 2). The spike read
    # the flat one; both agree on v1.13.5, so the recorded frame alone can't
    # tell which was read. This blanks field 4 and asserts the answer holds.
    raw = bytearray(load("join-recorded.txt"))
    flat = b"\x22\x06" + b"1.13.5"  # field 4, wire type 2, "1.13.5"
    assert raw.count(flat) == 1
    without = bytes(raw).replace(flat, b"\x22\x06" + b"9.9.9x")
    decoded = parse_join(without)
    assert decoded.server_version == "1.13.5"


def test_an_unknown_field_before_every_known_one_changes_nothing():
    interleaved = load("join-unknown-fields.txt")
    recorded = load("join-recorded.txt")
    # The fixture has to actually differ, or this asserts that two copies of
    # one frame decode the same way.
    assert interleaved != recorded
    assert len(interleaved) > len(recorded)
    assert parse_join(interleaved) == RECORDED


def test_the_interleaved_fixture_carries_unknown_fields_at_every_level():
    # "Unknown fields were inserted" is a claim about the fixture: the
    # assertion above would pass just as well if they sat only at the top
    # level, where none of the fields that matter live. So the four wire
    # types are checked at each depth the walker descends to.
    interleaved = load("join-unknown-fields.txt")

    def wire_types_of_unknowns(data):
        return {wire for number, wire, _ in pb_fields(data) if number >= 500}

    top = list(pb_fields(interleaved))
    # `SignalResponse` carries one field here, so exactly one unknown field can
    # precede it -- an empty set would mean the top level was skipped.
    assert wire_types_of_unknowns(interleaved) == {0}
    body = next(value for number, wire, value in top if number == 1 and wire == 2)
    assert wire_types_of_unknowns(body) == {0, 1, 2, 5}
    for number, wire, value in pb_fields(body):
        if number in (1, 5, 12) and wire == 2:
            assert wire_types_of_unknowns(value), "no unknown field inside field {}".format(number)


def test_a_turn_server_and_subscriber_primary_are_read():
    # The recorded frame has three credential-free STUN URLs and leaves
    # `subscriberPrimary` at its proto3 default, so it exercises neither the
    # username/credential pair nor a true boolean. Production LiveKit is behind
    # TURN, which makes those exactly the fields a robot on a NAT depends on.
    decoded = parse_join(load("join-turn.txt"))
    assert decoded.subscriber_primary is True
    assert decoded.ping_interval_s == 4
    assert decoded.ping_timeout_s == 20
    assert decoded.ice_servers == (
        IceServer(urls=("stun:stun.example.net:3478",), username="", credential=""),
        IceServer(
            urls=(
                "turn:turn.example.net:3478?transport=udp",
                "turns:turn.example.net:5349?transport=tcp",
            ),
            username="a-turn-user",
            credential="a-turn-credential",
        ),
    )


def test_the_refresh_token_frame_is_a_refresh_token_and_is_ignored():
    frame = load("refresh-token-recorded.txt")
    # Both halves: it really is the message the module says it ignores (field
    # 16 of SignalResponse, carrying a token), and the walker really returns
    # None for it. Without the first half, "None" is indistinguishable from
    # "these bytes were garbage".
    numbers = {number for number, _, _ in pb_fields(frame)}
    assert numbers == {16}
    assert parse_join(frame) is None


def test_an_empty_frame_is_not_a_join_and_does_not_raise():
    assert parse_join(load("empty.txt")) is None


@pytest.mark.parametrize(
    "name, message",
    [
        ("truncated-length.txt", "claims 624 bytes"),
        ("truncated-varint.txt", "truncated varint"),
        ("overlong-varint.txt", "longer than 64 bits"),
        ("group-wire-type.txt", "wire type 3"),
    ],
)
def test_a_frame_that_does_not_decode_raises(name, message):
    with pytest.raises(SignalProtocolError) as excinfo:
        parse_join(load(name))
    assert message in str(excinfo.value)


def test_truncation_after_the_join_field_still_raises():
    # The walker consumes every field, including the ones nothing reads,
    # precisely so this case cannot return a plausible answer. Returning early
    # on field 1 -- which is the obvious way to write it, and is how the spike's
    # throwaway version was written -- makes this frame decode cleanly.
    with pytest.raises(SignalProtocolError):
        parse_join(load("join-recorded.txt") + bytes([0x08, 0x80]))


def test_fixed_width_fields_are_skipped_rather_than_refused():
    # Nothing in this protocol uses wire types 1 and 5 today. A walker that
    # refused them would fail on the first server that added a float, which is
    # the opposite of what a tag walker is for.
    data = bytes([0x0D]) + b"\x01\x02\x03\x04" + bytes([0x11]) + b"\x01" * 8 + bytes([0x18, 0x07])
    assert list(pb_fields(data)) == [
        (1, 5, b"\x01\x02\x03\x04"),
        (2, 1, b"\x01" * 8),
        (3, 0, 7),
    ]


def test_every_built_fixture_matches_its_recipe():
    # The committed bytes and the code that makes them cannot drift: edit one
    # and this is red. It says nothing about whether the recipe is right --
    # the assertions above are what say that.
    for name, expected in built_fixtures().items():
        assert load(name) == expected, "{} is not what livekit_fixtures.py builds".format(name)


# --------------------------------------------------------------------------
# The URL, and the token in it
# --------------------------------------------------------------------------


def test_the_signalling_url_carries_everything_the_server_reads():
    url = signal_url("ws://localhost:7880", "a.token.value")
    head, _, query = url.partition("?")
    assert head == "ws://localhost:7880/rtc"
    assert dict(part.split("=", 1) for part in query.split("&")) == {
        "access_token": "a.token.value",
        "auto_subscribe": "0",
        "protocol": str(PROTOCOL_VERSION),
        "sdk": "python",
        "version": __version__,
    }
    assert PROTOCOL_VERSION == 17


def test_a_trailing_slash_does_not_produce_a_double_path():
    assert signal_url("ws://localhost:7880/", "t").startswith("ws://localhost:7880/rtc?")


def _returning(value):
    """An async callable that answers with `value`, however it is called."""

    async def call(*args, **kwargs):
        return value

    return call


def test_a_refused_handshake_reports_the_type_and_never_the_url_it_tried():
    # When a LiveKit server refuses the token, aiohttp raises
    # `WSServerHandshakeError: 401 ... url=URL('ws://.../rtc?access_token=eyJ...')`
    # -- the whole token in `str(exc)`, and in the traceback behind it. This is
    # the exception that must not reach a log or a `bridgeCameraState` message.
    token = "eyJhbGciOiJIUzI1NiJ9.a-real-looking-token.and-a-signature"
    refused = aiohttp.WSServerHandshakeError(
        SimpleNamespace(real_url="x"),
        (),
        status=401,
        message="Invalid response status",
    )
    refused.args = ("401, url=ws://livekit.invalid:7880/rtc?access_token=" + token,)

    async def failing(url):
        raise refused

    async def scenario():
        client = SignalClient("ws://livekit.invalid:7880", token, connect=failing)
        with pytest.raises(SignalConnectError) as excinfo:
            await client.connect()
        return excinfo.value

    error = asyncio.run(scenario())
    assert "WSServerHandshakeError" in str(error)
    assert "ws://livekit.invalid:7880" in str(error)
    assert token not in str(error)
    assert "access_token" not in str(error)
    # The chain is cut too: `raise ... from exc` would put the same URL in the
    # traceback of anything that prints this.
    assert error.__cause__ is None


def test_a_refused_handshake_does_not_orphan_the_join_future():
    # connect() creates self._join_future before opening the socket. When
    # _connect raised here, SignalConnectError used to propagate straight out
    # -- the retrieval guard 25 lines down only wraps the send/await after the
    # socket exists. The future stayed pending; close() -> _finish() (called
    # later by the caller's own _teardown) sets SignalClosed on it, which
    # nobody reads, so Future.__del__ logs "exception was never retrieved"
    # whenever GC runs -- an ERROR traceback naming no camera and no cause.
    # `_log_traceback` is the flag that warning is gated on: True after
    # set_exception(), False once something reads .exception(). Tested
    # directly since a real GC pass is not a moment a test controls.
    refused = aiohttp.WSServerHandshakeError(
        SimpleNamespace(real_url="x"), (), status=401, message="Invalid response status",
    )

    async def failing(url):
        raise refused

    async def scenario():
        client = SignalClient("ws://livekit.invalid:7880", "tok-1", connect=failing)
        with pytest.raises(SignalConnectError):
            await client.connect()
        return client._join_future

    join_future = asyncio.run(scenario())
    assert join_future.done()
    assert join_future._log_traceback is False, (
        "the join future's exception was never retrieved; this future would "
        "log an ERROR traceback when garbage collected"
    )


def test_a_socket_failure_reports_the_exception_type_and_never_its_text():
    # The whole signalling URL is a credential: the token is a query parameter
    # in it. aiohttp's own exceptions routinely quote the URL they tried, so
    # the rule here is the same one `live.py` and `camera_sources.py` follow --
    # type name only, never `str(exc)`.
    secret = "eyJhbGciOiJIUzI1NiJ9.a-real-looking-token"
    underlying = ValueError("cannot connect to ws://host/rtc?access_token=" + secret)
    message = SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=underlying)
    socket = _SignalSocket(
        SimpleNamespace(close=_returning(None)),
        SimpleNamespace(receive=_returning(message), close_code=None),
    )
    with pytest.raises(SignalClosed) as excinfo:
        asyncio.run(socket.recv())
    assert "ValueError" in str(excinfo.value)
    assert secret not in str(excinfo.value)


# --------------------------------------------------------------------------
# The conversation
# --------------------------------------------------------------------------


class FakeSocket:
    """The four calls `_SignalSocket` has, plus a server side a test drives.

    `sent` keeps the raw strings, not only the parsed messages, because "the
    first frame is TEXT" is the measured fact the whole JSON conversation rests
    on and a parsed copy could not tell text from binary.
    """

    def __init__(self):
        self.sent = []
        self.closed = False
        self.close_code = None
        self._inbox = asyncio.Queue()

    # -- the client's side
    async def send(self, payload):
        if self.closed:
            raise SignalClosed("the signalling socket is closed")
        assert isinstance(payload, str), "signalling frames are text"
        self.sent.append(payload)

    async def recv(self):
        item = await self._inbox.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def close(self):
        if not self.closed:
            self.closed = True
            self.close_code = 1000
            self._inbox.put_nowait(SignalClosed("the signalling socket closed (CLOSED)"))
            # A real aiohttp `ws.close()` hands the parked reader a CLOSING
            # message and then awaits -- it yields, giving the reader a
            # chance to run before the closer resumes. A fake whose close()
            # cannot yield can never lose that race, which is exactly what
            # let C2 ship: the ping loop's own reason always "won" here while
            # the real socket's reason won in production.
            await asyncio.sleep(0)
            await asyncio.sleep(0)

    # -- the test's side
    def deliver(self, message):
        self._inbox.put_nowait(json.dumps(message))

    def deliver_binary(self, data):
        self._inbox.put_nowait(data)

    def deliver_text(self, text):
        self._inbox.put_nowait(text)

    def fail(self, exception):
        self._inbox.put_nowait(exception)

    @property
    def messages(self):
        return [json.loads(frame) for frame in self.sent]

    def keys(self):
        return [next(iter(message)) for message in self.messages]


async def wait_until(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("the condition never became true")
        await asyncio.sleep(0.005)


def make_client(socket, **overrides):
    overrides.setdefault("join_timeout", 1.0)
    overrides.setdefault("request_timeout", 1.0)
    connected = {}

    async def connect(url):
        connected["url"] = url
        return socket

    client = SignalClient("ws://livekit.invalid:7880", "a.token.value", connect=connect, **overrides)
    client.test_connected = connected
    return client


async def joined(socket, **overrides):
    """A connected client, with the recorded `join` already on the wire."""
    client = make_client(socket, **overrides)
    socket.deliver_binary(load("join-recorded.txt"))
    await client.connect()
    return client


class Ticker:
    """Stands in for `asyncio.sleep` in the ping loop: records the interval and
    waits until the test releases exactly one tick.

    A fake sleep that returns immediately cannot be used here. The ping loop
    would then spin as fast as the event loop allows, and "three unanswered
    pings ended the session" would be true long before a test could answer one
    -- which reddens the *pong* test for a reason that has nothing to do with
    the policy under test.
    """

    def __init__(self):
        self.delays = []
        self._queue = asyncio.Queue()

    async def __call__(self, delay):
        self.delays.append(delay)
        await self._queue.get()

    def tick(self):
        self._queue.put_nowait(None)


def test_the_first_frame_is_a_text_ping_and_join_comes_back_decoded():
    async def scenario():
        socket = FakeSocket()
        client = await joined(socket)
        try:
            return client, socket
        finally:
            await client.close()

    client, socket = asyncio.run(scenario())
    assert socket.keys()[0] == "pingReq"
    assert client.test_connected["url"] == signal_url("ws://livekit.invalid:7880", "a.token.value")
    assert client.join == RECORDED


def test_a_server_that_says_nothing_times_out_and_the_socket_is_closed():
    async def scenario():
        socket = FakeSocket()
        client = make_client(socket, join_timeout=0.05)
        with pytest.raises(SignalTimeout) as excinfo:
            await client.connect()
        return socket, str(excinfo.value)

    socket, message = asyncio.run(scenario())
    assert "no join" in message
    assert "a.token.value" not in message
    assert socket.closed


def test_a_json_join_says_so_instead_of_timing_out():
    # Never seen from v1.13.5, and the failure it would otherwise produce is a
    # timeout that blames the network for a protocol change.
    async def scenario():
        socket = FakeSocket()
        client = make_client(socket, join_timeout=1.0)
        socket.deliver({"join": {"room": {"sid": "RM_x"}}})
        with pytest.raises(SignalProtocolError) as excinfo:
            await client.connect()
        return str(excinfo.value)

    assert "as JSON" in asyncio.run(scenario())


def test_an_undecodable_binary_frame_fails_the_connect():
    async def scenario():
        socket = FakeSocket()
        client = make_client(socket, join_timeout=1.0)
        socket.deliver_binary(load("truncated-length.txt"))
        with pytest.raises(SignalProtocolError):
            await client.connect()

    asyncio.run(scenario())


def test_add_track_sends_the_publish_request_and_waits_for_its_own_cid():
    async def scenario():
        socket = FakeSocket()
        client = await joined(socket)
        task = asyncio.ensure_future(client.add_track("cid-a", "camera", 640, 480))
        await wait_until(lambda: "addTrack" in socket.keys())
        # A reply for a track this call did not ask about must not resolve it.
        socket.deliver({"trackPublished": {"cid": "cid-b", "track": {"sid": "TR_wrong"}}})
        await asyncio.sleep(0.02)
        assert not task.done()
        socket.deliver({"trackPublished": {"cid": "cid-a", "track": {"sid": "TR_right"}}})
        published = await asyncio.wait_for(task, 1.0)
        await client.close()
        return socket.messages, published

    messages, published = asyncio.run(scenario())
    assert messages[1] == {
        "addTrack": {
            "cid": "cid-a",
            "name": "camera",
            "type": "VIDEO",
            "source": "CAMERA",
            "width": 640,
            "height": 480,
        }
    }
    assert published["track"]["sid"] == "TR_right"


def test_add_track_declares_one_layer_when_a_bitrate_is_given():
    """The root fix for the all-off `subscribedQualityUpdate` bug (see
    `add_track`'s own docstring): a track published with no `layers` gets
    exactly one bogus update from v1.13.5 and never another. `bitrate=None`
    (the default, unused by this test) must keep reproducing the old request
    byte for byte -- see the test below."""

    async def scenario():
        socket = FakeSocket()
        client = await joined(socket)
        task = asyncio.ensure_future(
            client.add_track("cid-a", "camera", 640, 480, bitrate=800_000)
        )
        await wait_until(lambda: "addTrack" in socket.keys())
        socket.deliver({"trackPublished": {"cid": "cid-a", "track": {"sid": "TR_right"}}})
        await asyncio.wait_for(task, 1.0)
        await client.close()
        return socket.messages

    messages = asyncio.run(scenario())
    assert messages[1] == {
        "addTrack": {
            "cid": "cid-a",
            "name": "camera",
            "type": "VIDEO",
            "source": "CAMERA",
            "width": 640,
            "height": 480,
            "layers": [
                {"quality": "HIGH", "width": 640, "height": 480, "bitrate": 800_000}
            ],
        }
    }


def test_add_track_without_a_bitrate_sends_no_layers():
    """The default reproduces the pre-3c.4 request exactly, so a caller with
    nothing to say about bitrate does not change what goes over the wire --
    this is `test_add_track_sends_the_publish_request_and_waits_for_its_own_cid`'s
    own assertion, repeated here as the thing this test is about rather than
    as a side effect of it."""

    async def scenario():
        socket = FakeSocket()
        client = await joined(socket)
        task = asyncio.ensure_future(client.add_track("cid-a", "camera", 640, 480))
        await wait_until(lambda: "addTrack" in socket.keys())
        socket.deliver({"trackPublished": {"cid": "cid-a", "track": {"sid": "TR_right"}}})
        await asyncio.wait_for(task, 1.0)
        await client.close()
        return socket.messages

    messages = asyncio.run(scenario())
    assert "layers" not in messages[1]["addTrack"]


def test_add_track_times_out_when_nothing_answers():
    async def scenario():
        socket = FakeSocket()
        client = await joined(socket, request_timeout=0.05)
        with pytest.raises(SignalTimeout) as excinfo:
            await client.add_track("cid-a", "camera", 640, 480)
        await client.close()
        return str(excinfo.value)

    assert "no trackPublished" in asyncio.run(scenario())


def test_send_offer_returns_the_answers_sdp():
    async def scenario():
        socket = FakeSocket()
        client = await joined(socket)
        task = asyncio.ensure_future(client.send_offer("v=0\r\nfake offer\r\n"))
        await wait_until(lambda: "offer" in socket.keys())
        socket.deliver({"answer": {"type": "answer", "sdp": "v=0\r\nfake answer\r\n"}})
        sdp = await asyncio.wait_for(task, 1.0)
        await client.close()
        return socket.messages, sdp

    messages, sdp = asyncio.run(scenario())
    assert messages[1] == {"offer": {"sdp": "v=0\r\nfake offer\r\n", "type": "offer"}}
    assert sdp == "v=0\r\nfake answer\r\n"


def test_an_answer_with_no_sdp_is_refused():
    async def scenario():
        socket = FakeSocket()
        client = await joined(socket)
        task = asyncio.ensure_future(client.send_offer("v=0\r\n"))
        await wait_until(lambda: "offer" in socket.keys())
        socket.deliver({"answer": {"type": "answer"}})
        with pytest.raises(SignalProtocolError):
            await asyncio.wait_for(task, 1.0)
        await client.close()

    asyncio.run(scenario())


def test_the_callbacks_fire_for_the_messages_they_name():
    async def scenario():
        seen = {"trickle": [], "quality": [], "leave": []}
        socket = FakeSocket()
        client = await joined(
            socket,
            on_trickle=seen["trickle"].append,
            on_quality_update=seen["quality"].append,
            on_leave=seen["leave"].append,
        )
        socket.deliver({"trickle": {"candidateInit": json.dumps(
            {"candidate": "candidate:2 1 udp 1 10.0.0.2 1 typ host", "sdpMid": "0",
             "sdpMLineIndex": 0})}})
        socket.deliver({"subscribedQualityUpdate": {"trackSid": "TR_a", "subscribedQualities": []}})
        socket.deliver({"leave": {"reason": "ROOM_CLOSED"}})
        await wait_until(lambda: all(seen.values()))
        await client.close()
        return seen

    seen = asyncio.run(scenario())
    assert seen["trickle"][0]["candidate"].startswith("candidate:2 ")
    assert seen["quality"][0]["trackSid"] == "TR_a"
    assert seen["leave"][0] == {"reason": "ROOM_CLOSED"}


def test_a_trickle_that_carries_no_candidate_is_dropped_rather_than_raised():
    async def scenario():
        seen = []
        socket = FakeSocket()
        client = await joined(socket, on_trickle=seen.append)
        socket.deliver({"trickle": {"candidateInit": "not json at all"}})
        socket.deliver({"trickle": {"candidateInit": json.dumps({"candidate": ""})}})
        socket.deliver({"trickle": {}})
        # candidateInit sent as a JSON object, number or array rather
        # than the string every server sends today. json.loads raises
        # TypeError on these, not ValueError -- if the handler caught only
        # ValueError, each of these would climb out of _on_text and end the
        # session, which is exactly what happened before this fix.
        socket.deliver({"trickle": {"candidateInit": {"candidate": "x"}}})
        socket.deliver({"trickle": {"candidateInit": 12345}})
        socket.deliver({"trickle": {"candidateInit": ["candidate:x"]}})
        # A message that does reach the callback, delivered last, proves the
        # read loop survived all six -- without it this passes just as well
        # for a loop that died on the first.
        socket.deliver({"trickle": {"candidateInit": json.dumps({"candidate": "candidate:3 x"})}})
        await wait_until(lambda: seen)
        await client.close()
        return seen

    seen = asyncio.run(scenario())
    assert [candidate["candidate"] for candidate in seen] == ["candidate:3 x"]


def test_an_unhashable_trackpublished_cid_is_dropped_rather_than_raised():
    async def scenario():
        seen = []
        socket = FakeSocket()
        client = await joined(socket, on_trickle=seen.append)
        # A server-chosen cid the client cannot hash. dict.get(cid)
        # raises TypeError on these -- if _on_track_published caught nothing,
        # each would climb out of _on_text and end the session the same way
        # the candidateInit shapes above did.
        socket.deliver({"trackPublished": {"cid": {"nested": "dict"}, "track": {"sid": "TR_x"}}})
        socket.deliver({"trackPublished": {"cid": ["a", "list"], "track": {"sid": "TR_x"}}})
        # The canary: a trickle that does reach its callback, delivered
        # last, proves the read loop survived both.
        socket.deliver({"trickle": {"candidateInit": json.dumps({"candidate": "candidate:3 x"})}})
        await wait_until(lambda: seen)
        await client.close()
        return seen

    seen = asyncio.run(scenario())
    assert [candidate["candidate"] for candidate in seen] == ["candidate:3 x"]


class _CapturingHandler(logging.Handler):
    """Collects everything this module logs, at any level.

    Not `caplog`. This suite has established several times over (see
    `test_main.py`, `test_client_writer.py`, `test_camera_sources.py`) that
    pytest disables propagation for every logger in these images, so a
    `caplog`-based assertion here passes unconditionally -- which is the worst
    available state for a check whose whole job is to notice a missing line.
    """

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


def _capturing():
    handler = _CapturingHandler()
    watched = logging.getLogger("fleetless_bridge.livekit_signal")
    watched.addHandler(handler)
    watched.setLevel(logging.DEBUG)
    return handler, watched


def test_an_unknown_message_is_logged_at_debug_and_ignored():
    async def scenario():
        alive = []
        socket = FakeSocket()
        client = await joined(socket, on_quality_update=alive.append)
        socket.deliver({"aMessageFromAFutureServer": {"anything": True}})
        socket.deliver_text("not json either")
        socket.deliver({"connectionQuality": {"updates": []}})
        socket.deliver({"pongResp": {"timestamp": "1"}})
        # Delivered last: a message that does reach a callback is what
        # separates "the four above were ignored" from "the read loop died on
        # the first of them".
        socket.deliver({"subscribedQualityUpdate": {"trackSid": "TR_alive"}})
        await wait_until(lambda: alive)
        await client.close()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    text = "\n".join(handler.messages)
    assert "aMessageFromAFutureServer" in text
    assert "not JSON" in text


def test_the_token_stays_out_of_the_logs_even_with_debug_turned_on():
    # The signalling URL is a credential -- the token is a query parameter in
    # it -- and the whole conversation runs through one module logger. This
    # drives a complete session at DEBUG and asserts the token is in none of it.
    token = "eyJhbGciOiJIUzI1NiJ9.a-real-looking-token.and-a-signature"

    async def scenario():
        socket = FakeSocket()
        client = SignalClient(
            "ws://livekit.invalid:7880",
            token,
            connect=_returning(socket),
            join_timeout=1.0,
            request_timeout=1.0,
        )
        socket.deliver_binary(load("join-recorded.txt"))
        await client.connect()
        task = asyncio.ensure_future(client.add_track("cid-a", "camera", 640, 480))
        await wait_until(lambda: "addTrack" in socket.keys())
        socket.deliver({"trackPublished": {"cid": "cid-a", "track": {"sid": "TR_a"}}})
        await asyncio.wait_for(task, 1.0)
        socket.fail(SignalClosed("the signalling socket failed (ClientError)"))
        await wait_until(lambda: client.closed)
        await client.close()

    handler, watched = _capturing()
    try:
        asyncio.run(scenario())
    finally:
        watched.removeHandler(handler)
    assert handler.messages, "nothing was logged at all; this check saw nothing"
    for message in handler.messages:
        assert token not in message, message
        assert "access_token" not in message, message


def test_an_unexpected_end_reports_it_once_and_fails_everything_waiting():
    async def scenario():
        closed = []
        socket = FakeSocket()
        client = await joined(socket, on_closed=closed.append)
        pending = asyncio.ensure_future(client.add_track("cid-a", "camera", 640, 480))
        await wait_until(lambda: "addTrack" in socket.keys())
        socket.fail(SignalClosed("the signalling socket closed (CLOSED)"))
        with pytest.raises(SignalClosed):
            await asyncio.wait_for(pending, 1.0)
        await wait_until(lambda: closed)
        # close() after the fact must not report a second disconnect.
        await client.close()
        return closed, client.closed

    closed, is_closed = asyncio.run(scenario())
    assert len(closed) == 1
    assert "closed" in closed[0]
    assert is_closed


def test_an_unexpected_end_fails_the_answer_future_too():
    # _finish's guarantee -- "every pending wait is failed here" -- is
    # asserted above for only one of the three future kinds it iterates
    # (`_join_future`, `_answer_future`, `_track_futures.values()`):
    # test_an_unexpected_end_reports_it_once_and_fails_everything_waiting
    # creates only an add_track future. This covers the answer future,
    # which send_offer() creates and which _finish reaches through the
    # middle element of that list.
    async def scenario():
        closed = []
        socket = FakeSocket()
        client = await joined(socket, on_closed=closed.append)
        pending = asyncio.ensure_future(client.send_offer("v=0\r\nsdp\r\n"))
        await wait_until(lambda: "offer" in socket.keys())
        socket.fail(SignalClosed("the signalling socket closed (CLOSED)"))
        with pytest.raises(SignalClosed):
            await asyncio.wait_for(pending, 1.0)
        await wait_until(lambda: closed)
        await client.close()
        return closed

    closed = asyncio.run(scenario())
    assert len(closed) == 1


def test_an_unexpected_end_fails_every_kind_of_pending_future_together():
    # Stronger than the two tests above: a join future, an answer future and
    # a track future all pending AT ONCE when the socket ends, proving
    # _finish's list iterates all three together rather than one kind at a
    # time. (The join future is pending here in the sense that matters for
    # _finish's own list -- the connect-time case is covered separately by
    # test_a_refused_handshake_does_not_orphan_the_join_future.)
    async def scenario():
        closed = []
        socket = FakeSocket()
        client = await joined(socket, on_closed=closed.append)
        track_pending = asyncio.ensure_future(client.add_track("cid-a", "camera", 640, 480))
        answer_pending = asyncio.ensure_future(client.send_offer("v=0\r\nsdp\r\n"))
        await wait_until(lambda: "addTrack" in socket.keys() and "offer" in socket.keys())
        socket.fail(SignalClosed("the signalling socket closed (CLOSED)"))
        track_result = answer_result = None
        try:
            await asyncio.wait_for(track_pending, 1.0)
        except SignalClosed as exc:
            track_result = exc
        try:
            await asyncio.wait_for(answer_pending, 1.0)
        except SignalClosed as exc:
            answer_result = exc
        await wait_until(lambda: closed)
        await client.close()
        return track_result, answer_result, closed

    track_result, answer_result, closed = asyncio.run(scenario())
    assert isinstance(track_result, SignalClosed)
    assert isinstance(answer_result, SignalClosed)
    assert len(closed) == 1


def test_close_sends_leave_and_is_not_reported_as_a_disconnect():
    async def scenario():
        closed = []
        socket = FakeSocket()
        client = await joined(socket, on_closed=closed.append)
        await client.close()
        await client.close()  # twice is safe
        return socket.keys(), socket.closed, closed

    keys, socket_closed, closed = asyncio.run(scenario())
    assert keys == ["pingReq", "leave"]
    assert socket_closed
    assert closed == []


def test_closing_a_client_that_never_connected_is_safe():
    asyncio.run(make_client(FakeSocket()).close())


def test_three_unanswered_pings_end_the_session():
    async def scenario():
        closed = []
        ticker = Ticker()
        socket = FakeSocket()
        await joined(socket, sleep=ticker, on_closed=closed.append)
        # One tick per sleep, so the loop's own pace cannot outrun the test.
        for _ in range(4):
            await wait_until(lambda: len(ticker.delays) >= 1 + len(closed))
            ticker.tick()
            await asyncio.sleep(0.01)
            if closed:
                break
        await wait_until(lambda: closed)
        return ticker.delays, closed, socket.keys()

    delays, closed, keys = asyncio.run(scenario())
    # `join` says pingInterval 5 and pingTimeout 15, so three go unanswered.
    assert set(delays) == {5.0}
    assert keys.count("pingReq") == 3
    assert "no pongResp" in closed[0]


def test_a_pong_keeps_the_session_alive_past_the_point_silence_would_end_it():
    async def scenario():
        closed = []
        ticker = Ticker()
        socket = FakeSocket()
        client = await joined(socket, sleep=ticker, on_closed=closed.append)
        for round_number in range(10):
            await wait_until(lambda: len(ticker.delays) == round_number + 1)
            socket.deliver({"pongResp": {"timestamp": "1"}})
            await asyncio.sleep(0.01)  # the reader consumes it before the next check
            ticker.tick()
        await asyncio.sleep(0.01)
        alive = not closed
        await client.close()
        return alive, len(ticker.delays)

    alive, rounds = asyncio.run(scenario())
    # Ten rounds is more than three: without the pongs this session is over.
    assert rounds >= 10
    assert alive


def test_the_default_interval_is_used_when_join_does_not_say():
    async def scenario():
        delays = []

        async def sleep(delay):
            delays.append(delay)
            await asyncio.sleep(0.01)

        socket = FakeSocket()
        client = make_client(socket, sleep=sleep)
        # A join with no pingInterval and no pingTimeout: both are proto3
        # scalars, so "absent" and "zero" are the same frame.
        socket.deliver_binary(bytes([0x0A, 0x02, 0x62, 0x00]))
        await client.connect()
        await wait_until(lambda: delays)
        await client.close()
        return delays

    assert asyncio.run(scenario())[0] == DEFAULT_PING_INTERVAL_S


def test_a_callback_that_raises_does_not_kill_the_read_loop():
    async def scenario():
        seen = []

        def explode(payload):
            seen.append(payload)
            raise RuntimeError("a callback that raises")

        socket = FakeSocket()
        client = await joined(socket, on_quality_update=explode)
        socket.deliver({"subscribedQualityUpdate": {"trackSid": "TR_a"}})
        await wait_until(lambda: seen)
        # The read loop is still there: a second message reaches the callback.
        socket.deliver({"subscribedQualityUpdate": {"trackSid": "TR_b"}})
        await wait_until(lambda: len(seen) == 2)
        await client.close()
        return seen

    assert [payload["trackSid"] for payload in asyncio.run(scenario())] == ["TR_a", "TR_b"]
