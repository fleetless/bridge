# SPDX-License-Identifier: Apache-2.0
"""The LiveKit signalling conversation, spoken directly rather than through the SDK.

One WebSocket to `<url>/rtc` carries the whole publish negotiation: the server
announces the room (`join`), we ask for a track (`addTrack` -> `trackPublished`),
we send the SDP offer (`offer` -> `answer`), both sides trickle ICE candidates,
and a ping keeps the session observable. `live.py` drives the media half with
aiortc; this file knows nothing about media.

**Why this file exists at all.** `livekit` (the SDK) is a pip-only wheel with a
28 MB native blob and a protobuf floor no supported Ubuntu ships, so a bridge
that depends on it can never be a `.deb` in the ROS index. Everything here is
`aiohttp` and `json` -- both apt packages with rosdep keys -- plus the one
awkward exception below.

**The wire quirk, measured, and the whole reason there is protobuf in here.**
livekit-server v1.13.5 writes `join` before it has read a single byte from the
client, and it chooses its wire format from the client's *first* frame. Connect
and stay silent and everything is protobuf forever; send one TEXT frame the
instant the socket opens and everything from the third message on is protojson
(lowerCamelCase, the oneof as the single top-level key, enums as strings) -- but
`join` and the `refreshToken` behind it were already on their way as binary.
So this client sends `pingReq` as its first frame and reads exactly two binary
frames with the tag walker below. There is no protobuf library and no `.proto`
file; there are about thirty lines that know six field numbers.

`refreshToken` is **ignored by design**: a session's token lifetime is bounded
by the cloud's own `publisherTtlSeconds`, and a session that outlives it is torn
down and restarted by the cloud like any other loss. Decoding a second binary
message to extend a session the cloud already re-issues would be code with no
reader. It reaches `parse_join` like any other frame and comes back `None`.

**The token is a credential.** It rides in the query string of the signalling
URL, which means the built URL is a secret and the base URL is not. Nothing here
logs a built URL, puts one in an exception, or lets a library exception's own
text out: every failure this module reports names an exception *type*, never its
message, for the same reason `live.py` and `camera_sources.py` do.

**What this client deliberately does not do.** No reconnect and no ICE restart:
the cloud already treats a bridge-side drop as the end of the session and
re-issues `camera_start`, so any transport loss ends this session through
`on_closed` and the cloud starts a new one. No subscriber peer connection:
`auto_subscribe=0` and a token without `canSubscribe` mean the server never
sends an offer of its own (measured: `subscriberPrimary` is false and no
`offer` ever arrived), so there is one peer connection and `trickle.target`
carries no information this client could act on.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlencode

import aiohttp

from fleetless_bridge import __version__

log = logging.getLogger(__name__)

#: The signalling protocol version this client claims. 17 is what the spike
#: measured v1.13.5 accepting and what the server echoes back in
#: `serverInfo.protocol`; it is a number the server reads, so it is pinned here
#: rather than derived from anything.
PROTOCOL_VERSION = 17

#: The path appended to the LiveKit server URL the cloud hands us. The cloud
#: sends a base (`wss://media.example`), never an endpoint, so this is added
#: here and appending it twice is the caller's mistake to make.
SIGNAL_PATH = "/rtc"

#: How long `connect()` waits for the `join` frame. The server writes it before
#: reading anything, so in practice it arrives with the handshake; this bound
#: exists for the server that accepts the socket and then says nothing.
JOIN_TIMEOUT_S = 15.0

#: How long `add_track()` and `send_offer()` wait for their reply.
REQUEST_TIMEOUT_S = 15.0

#: Used when `join` does not carry `pingInterval` / `pingTimeout` (both are
#: proto3 scalars, so "the server did not say" and "the server said zero" are
#: the same frame). v1.13.5 sends 5 and 15.
DEFAULT_PING_INTERVAL_S = 5.0
DEFAULT_PING_TIMEOUT_S = 15.0

#: How long `close()` waits for the WebSocket close handshake before giving up
#: on it. A peer that will not answer must not keep the camera's stop path.
CLOSE_TIMEOUT_S = 5.0


class SignalError(Exception):
    """Anything this module reports about the signalling conversation."""


class SignalConnectError(SignalError):
    """The signalling socket could not be opened.

    A class of its own rather than the library's exception, because that
    exception's *text* cannot be allowed out. When the server refuses the
    token, aiohttp raises
    `WSServerHandshakeError: 401 ... url=URL('ws://.../rtc?access_token=eyJ...')`
    -- the whole token, in `str(exc)` and in the traceback. So the underlying
    exception is reduced to its type name here and the chain is cut, at the one
    place that knows the URL is a credential rather than an address."""


class SignalClosed(SignalError):
    """The signalling socket ended -- the peer closed it, it failed, or
    `close()` was called -- while something was still waiting on it."""


class SignalProtocolError(SignalError):
    """A frame did not decode: truncated protobuf, an unreadable wire type, a
    reply missing the one field it exists to carry."""


class SignalTimeout(SignalError):
    """A reply the conversation cannot continue without did not arrive."""


# --------------------------------------------------------------------------
# The tag walker: the two binary frames, and nothing else in this file
# --------------------------------------------------------------------------

# Field numbers, from the wire rather than from a generated module. Each is
# named once here so the walker below reads as protocol and not as arithmetic.
_RESPONSE_JOIN = 1  # SignalResponse.join
_JOIN_ROOM = 1  # JoinResponse.room
_JOIN_ICE_SERVERS = 5  # JoinResponse.ice_servers (repeated)
_JOIN_SUBSCRIBER_PRIMARY = 6  # JoinResponse.subscriber_primary
_JOIN_PING_TIMEOUT = 10  # JoinResponse.ping_timeout (seconds)
_JOIN_PING_INTERVAL = 11  # JoinResponse.ping_interval (seconds)
_JOIN_SERVER_INFO = 12  # JoinResponse.server_info
_ROOM_SID = 1
_ROOM_NAME = 2
_SERVER_INFO_VERSION = 2
_ICE_SERVER_URLS = 1  # repeated string
_ICE_SERVER_USERNAME = 2
_ICE_SERVER_CREDENTIAL = 3

#: A varint is at most ten bytes; the tenth is written at shift 63. Anything
#: past that is not a number this decoder will ever produce a right answer for,
#: so it is refused rather than silently wrapped.
_MAX_VARINT_SHIFT = 63


def _varint(data: bytes, index: int) -> Tuple[int, int]:
    """One base-128 varint at `index`. Returns `(value, next_index)`."""
    value = 0
    shift = 0
    while True:
        if index >= len(data):
            raise SignalProtocolError("truncated varint at byte {}".format(index))
        byte = data[index]
        index += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, index
        shift += 7
        if shift > _MAX_VARINT_SHIFT:
            raise SignalProtocolError("varint longer than 64 bits")


def pb_fields(data: bytes) -> Iterator[Tuple[int, int, Any]]:
    """Yield `(field_number, wire_type, value)` for one protobuf message.

    `value` is an `int` for wire type 0 and `bytes` for 2; for the fixed-width
    types 1 and 5 it is the raw bytes, undecoded, because nothing here reads
    one -- they are walked past so that a field this client has never heard of
    cannot stop it reading the fields it has. That is the entire point of a tag
    walker, and it is why the two wire types this file *reads* are not the only
    two it *skips*. Groups (3 and 4) are refused: no modern protobuf encoder
    emits them and guessing at a length for one would desynchronise everything
    after it.

    Truncation raises rather than returning what was decoded so far. A frame cut
    short is not a frame with fewer fields, and the difference is exactly the
    one a caller cannot recover on its own.
    """
    index = 0
    end = len(data)
    while index < end:
        key, index = _varint(data, index)
        number, wire = key >> 3, key & 7
        if number == 0:
            raise SignalProtocolError("field number 0 is not a legal tag")
        if wire == 0:
            value, index = _varint(data, index)
        elif wire == 2:
            length, index = _varint(data, index)
            if length > end - index:
                raise SignalProtocolError(
                    "field {} claims {} bytes, {} remain".format(number, length, end - index)
                )
            value = data[index:index + length]
            index += length
        elif wire in (1, 5):
            width = 8 if wire == 1 else 4
            if width > end - index:
                raise SignalProtocolError(
                    "field {} is a truncated fixed-width value".format(number)
                )
            value = data[index:index + width]
            index += width
        else:
            raise SignalProtocolError("field {} has wire type {}".format(number, wire))
        yield number, wire, value


def _text(raw: bytes) -> str:
    # protobuf `string` is UTF-8 by definition; `replace` rather than `strict`
    # because a server that broke that promise should not be able to turn a
    # room name into an exception on the bridge's start path.
    return raw.decode("utf-8", "replace")


@dataclass(frozen=True)
class IceServer:
    """One `iceServers` entry from `join`. `username` and `credential` are
    empty for a STUN server and set for TURN -- which is the entry that matters
    on a robot behind NAT, and the one the dev stack never sends."""

    urls: Tuple[str, ...] = ()
    username: str = ""
    credential: str = ""


@dataclass(frozen=True)
class JoinInfo:
    """What `join` said, and nothing more -- a transcript, not a policy. What
    to do when `ping_interval_s` is zero is `SignalClient`'s decision and is
    made there, so that this stays comparable with the recorded frame."""

    room_sid: str = ""
    room_name: str = ""
    server_version: str = ""
    subscriber_primary: bool = False
    ice_servers: Tuple[IceServer, ...] = ()
    ping_interval_s: int = 0
    ping_timeout_s: int = 0


def _parse_ice_server(data: bytes) -> IceServer:
    urls: List[str] = []
    username = credential = ""
    for number, wire, value in pb_fields(data):
        if number == _ICE_SERVER_URLS and wire == 2:
            urls.append(_text(value))
        elif number == _ICE_SERVER_USERNAME and wire == 2:
            username = _text(value)
        elif number == _ICE_SERVER_CREDENTIAL and wire == 2:
            credential = _text(value)
    return IceServer(tuple(urls), username, credential)


def parse_join(data: bytes) -> Optional[JoinInfo]:
    """The `join` inside a binary `SignalResponse`, or `None` if this frame is
    not one -- which is how `refreshToken`, the second binary frame, is ignored
    without a second code path.

    Raises `SignalProtocolError` on bytes that are not a well-formed protobuf
    message. Every field is walked, including the ones nothing reads, so that a
    frame truncated *after* `join` still raises instead of returning a
    plausible answer.
    """
    body: Optional[bytes] = None
    for number, wire, value in pb_fields(data):
        if number == _RESPONSE_JOIN and wire == 2:
            body = value
    if body is None:
        return None
    room_sid = room_name = server_version = ""
    subscriber_primary = False
    ping_interval = ping_timeout = 0
    ice_servers: List[IceServer] = []
    for number, wire, value in pb_fields(body):
        if number == _JOIN_ROOM and wire == 2:
            for inner, inner_wire, inner_value in pb_fields(value):
                if inner == _ROOM_SID and inner_wire == 2:
                    room_sid = _text(inner_value)
                elif inner == _ROOM_NAME and inner_wire == 2:
                    room_name = _text(inner_value)
        elif number == _JOIN_ICE_SERVERS and wire == 2:
            ice_servers.append(_parse_ice_server(value))
        elif number == _JOIN_SUBSCRIBER_PRIMARY and wire == 0:
            subscriber_primary = bool(value)
        elif number == _JOIN_PING_TIMEOUT and wire == 0:
            ping_timeout = value
        elif number == _JOIN_PING_INTERVAL and wire == 0:
            ping_interval = value
        elif number == _JOIN_SERVER_INFO and wire == 2:
            for inner, inner_wire, inner_value in pb_fields(value):
                if inner == _SERVER_INFO_VERSION and inner_wire == 2:
                    server_version = _text(inner_value)
    return JoinInfo(
        room_sid=room_sid,
        room_name=room_name,
        server_version=server_version,
        subscriber_primary=subscriber_primary,
        ice_servers=tuple(ice_servers),
        ping_interval_s=ping_interval,
        ping_timeout_s=ping_timeout,
    )


# --------------------------------------------------------------------------
# The socket
# --------------------------------------------------------------------------


def signal_url(url: str, token: str, *, version: str = __version__) -> str:
    """The signalling URL for a base LiveKit URL and an access token.

    **The result is a credential**: `access_token` is in it. It is built here,
    handed straight to the connector, and never logged, never put in an
    exception, and never returned to anything that reports failures.

    `auto_subscribe=0` because the bridge publishes and never watches; the
    server then opens no subscriber peer connection and sends no offer of its
    own. `sdk`/`version` are diagnostic only -- they appear in the server's own
    session log -- and an unknown `sdk` is accepted (measured).
    """
    query = urlencode(
        {
            "access_token": token,
            "auto_subscribe": "0",
            "protocol": str(PROTOCOL_VERSION),
            "sdk": "python",
            "version": version,
        }
    )
    return "{}{}?{}".format(url.rstrip("/"), SIGNAL_PATH, query)


class _SignalSocket:
    """One WebSocket, reduced to the three things this file asks of it.

    Deliberately not `client.py`'s `_Socket`, though it is built to the same
    shape: that one carries the cloud session's own contract (binary snapshots,
    the four exception classes `BridgeClient` catches, a keepalive tuned to the
    cloud's ping cadence) and this conversation shares none of it -- everything
    outgoing here is text, and every failure leaves as a `SignalError`. Two
    protocols sharing one adapter would mean one of them is described by
    somebody else's comments.

    It owns the `aiohttp.ClientSession` for the same reason that one does:
    aiohttp's session owns the connector, so closing the socket without closing
    the session leaks one per live session.
    """

    def __init__(self, session: "aiohttp.ClientSession", ws) -> None:
        self._session = session
        self._ws = ws

    @property
    def close_code(self) -> Optional[int]:
        return self._ws.close_code

    async def send(self, payload: str) -> None:
        await self._ws.send_str(payload)

    async def recv(self):
        """One frame: `str` for text, `bytes` for binary. Everything else is an
        end, and leaves as `SignalClosed` -- including `WSMsgType.ERROR`, whose
        payload is an `aiohttp.WebSocketError` that inherits from `Exception`
        directly and would otherwise escape every catch site as itself."""
        message = await self._ws.receive()
        if message.type is aiohttp.WSMsgType.TEXT:
            return message.data
        if message.type is aiohttp.WSMsgType.BINARY:
            return message.data
        if message.type is aiohttp.WSMsgType.ERROR:
            error = message.data if isinstance(message.data, BaseException) else None
            # Type name only: an aiohttp exception's text can carry the URL it
            # tried, and that URL is the token.
            raise SignalClosed(
                "the signalling socket failed ({})".format(
                    type(error).__name__ if error is not None else "unknown"
                )
            )
        raise SignalClosed("the signalling socket closed ({})".format(message.type.name))

    async def close(self) -> None:
        try:
            await self._ws.close()
        finally:
            await self._session.close()


async def websocket_connect(url: str) -> _SignalSocket:
    """Open one signalling socket. `SignalClient`'s default `connect`.

    `heartbeat=None`: the server pings every ten seconds on its own and aiohttp
    answers those inside `receive()`, so a second timer here would be a second
    liveness policy beside the `pingReq` one, which is the one `join` actually
    parameterises.
    """
    session = aiohttp.ClientSession()
    try:
        ws = await session.ws_connect(url, heartbeat=None)
    except BaseException:
        await session.close()
        raise
    return _SignalSocket(session, ws)


# --------------------------------------------------------------------------
# The client
# --------------------------------------------------------------------------


class SignalClient:
    """The signalling half of one live publish session.

    Lifecycle: `connect()` (returns the `JoinInfo`), then `add_track()` and
    `send_offer()` in that order -- LiveKit answers an offer that publishes a
    track it was not told about with nothing at all -- then `close()`. There
    is no `send_trickle`: every aiortc version this package ships for (1.3.0,
    1.6.0, 1.14.0, TURN included -- see `relay_probe.py`'s own proof) gathers
    every local candidate, relay candidates too, before `setLocalDescription`
    returns, so the offer this client sends already carries everything this
    side has to say. The server's own candidates arrive over this same
    connection, through `on_trickle`; only this side never has anything to
    trickle.

    The callbacks run synchronously on the event loop thread, the same way
    `live.py`'s own do, and an exception from one is logged (type name only) and
    swallowed: a callback must not be able to kill the read loop.

    `on_closed(reason)` fires exactly once, and **only for an end this client
    did not ask for** -- `close()` is not a disconnect. That is the same
    distinction `LivePublisher._stopping` draws today, moved to the side that
    can see the socket.

    `connect` (the constructor argument) is the injectable seam `client.py`'s
    own is: it takes the built signalling URL and returns anything with `send`,
    `recv`, `close` and `close_code`, so the whole conversation is provable
    against a fake socket with no LiveKit server anywhere.
    """

    def __init__(
        self,
        url: str,
        token: str,
        *,
        on_trickle: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_quality_update: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_leave: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_closed: Optional[Callable[[str], None]] = None,
        connect: Optional[Callable[[str], Awaitable[Any]]] = None,
        sleep: Optional[Callable[[float], Awaitable[None]]] = None,
        join_timeout: float = JOIN_TIMEOUT_S,
        request_timeout: float = REQUEST_TIMEOUT_S,
    ) -> None:
        self._url = url
        self._token = token
        self._on_trickle = on_trickle
        self._on_quality_update = on_quality_update
        self._on_leave = on_leave
        self._on_closed = on_closed
        self._connect = connect or websocket_connect
        self._sleep = sleep or asyncio.sleep
        self._join_timeout = join_timeout
        self._request_timeout = request_timeout

        self._socket = None
        self._join: Optional[JoinInfo] = None
        self._join_future: Optional["asyncio.Future"] = None
        self._answer_future: Optional["asyncio.Future"] = None
        self._track_futures: Dict[str, "asyncio.Future"] = {}
        self._reader_task: Optional["asyncio.Task"] = None
        self._ping_task: Optional["asyncio.Task"] = None
        self._unanswered_pings = 0
        self._closing = False
        self._finished = False

    # -- state ------------------------------------------------------------

    @property
    def join(self) -> Optional[JoinInfo]:
        """What `join` said, once `connect()` has returned. `None` before."""
        return self._join

    @property
    def closed(self) -> bool:
        """True once the session has ended, however it ended."""
        return self._finished

    # -- the conversation --------------------------------------------------

    async def connect(self) -> JoinInfo:
        """Open the socket, send the frame that flips the server to JSON, and
        return what `join` said.

        The order here is the measured one: the reader starts first so that
        `join` -- which the server writes before reading anything -- cannot be
        missed, and `pingReq` goes out as the very first client frame so that
        the server has flipped to JSON before this client has anything else to
        say.

        **What the flip is actually keyed on, measured rather than assumed:**
        *any* first TEXT frame flips it, not `pingReq` specifically. Removing
        this send and running the full handshake against v1.13.5 still
        succeeded, because `addTrack` is a TEXT frame and flipped the server
        itself. `pingReq` is still what belongs here -- it is the one frame
        this client owes the server anyway, and sending it at connect time
        means the flip does not depend on the caller having something to
        publish, or on how long it takes to decide to -- but the sentence
        "without this, nothing works" would have been wrong, so it is not
        written.
        """
        loop = asyncio.get_running_loop()
        self._join_future = loop.create_future()
        # Built here and passed straight in. It carries the token; see
        # `signal_url` and `SignalConnectError`.
        try:
            self._socket = await self._connect(signal_url(self._url, self._token))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - reduced to a type name; see below
            # A socket that never opened leaves `self._join_future` pending --
            # created above, with no reader and no ping loop to ever resolve
            # it. `close()` -> `_finish()` sets `SignalClosed` on it (the
            # caller's own `_teardown` calls `close()` after this raises),
            # and nothing then reads that exception: `Future.__del__` logs
            # asyncio's "exception was never retrieved" on a robot, naming no
            # camera and no cause, whenever the GC later runs it. Retrieved
            # here the same way the send-path guard 25 lines down already
            # does, so a failure on either path is silent the same way.
            await self.close()
            if self._join_future.done() and not self._join_future.cancelled():
                self._join_future.exception()
            # `from None`, not `from exc`: the chained exception's own text and
            # traceback carry the built URL, and the built URL is the token.
            raise SignalConnectError(
                "could not open the LiveKit signalling socket to {} ({})".format(
                    self._url, type(exc).__name__
                )
            ) from None
        self._reader_task = asyncio.ensure_future(self._read_loop(self._socket))
        try:
            await self._send_ping()
            join = await self._await(self._join_future, self._join_timeout, "join")
        except BaseException:
            # A half-open socket with a reader task on it is exactly the leak
            # `LivePublisher.start` was written to prevent one level up.
            await self.close()
            # `_finish` sets an exception on this future if nothing had yet;
            # retrieved here so a failure on the send path does not also print
            # asyncio's "exception was never retrieved" on a robot.
            if self._join_future.done() and not self._join_future.cancelled():
                self._join_future.exception()
            raise
        self._join = join
        log.info(
            "LiveKit signalling joined: room %s (%s), server %s, %d ICE server(s)",
            join.room_name,
            join.room_sid,
            join.server_version or "unknown",
            len(join.ice_servers),
        )
        self._ping_task = asyncio.ensure_future(
            self._ping_loop(self._ping_interval(), self._allowed_missed_pings())
        )
        return join

    async def add_track(
        self,
        cid: str,
        name: str,
        width: int,
        height: int,
        *,
        source: str = "CAMERA",
        bitrate: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Ask the server to publish a video track, and wait for the
        `trackPublished` that names the same `cid`.

        `cid` is the aiortc track's own id: the server echoes it back, which is
        what lets the reply be matched to the request rather than to whichever
        `trackPublished` happened to arrive next.

        **`bitrate`, in bits per second, declares one video layer.** Against
        livekit-server v1.13.5, a track published with no `layers` at all gets
        exactly one `subscribedQualityUpdate` in its whole session -- every
        layer off -- and never another, whether or not a viewer is in the room
        and subscribed. The same room and the same viewer, with one `HIGH`
        layer declared here, get a first update that already reports the
        viewer's real subscription, and every change after that (a viewer
        leaving, a viewer joining) is reported too. `live.py` trusts the
        update it is given, once this is sent -- it does not work around the
        server here.

        `bitrate=None` (the default) omits `layers` entirely and reproduces the
        old request byte for byte, so a caller with nothing to say about it
        does not change what goes over the wire.
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._track_futures[cid] = future
        try:
            message: Dict[str, Any] = {
                "addTrack": {
                    "cid": cid,
                    "name": name,
                    "type": "VIDEO",
                    "source": source,
                    "width": width,
                    "height": height,
                }
            }
            if bitrate is not None:
                message["addTrack"]["layers"] = [
                    {"quality": "HIGH", "width": width, "height": height, "bitrate": bitrate}
                ]
            await self._send(message)
            return await self._await(future, self._request_timeout, "trackPublished")
        finally:
            self._track_futures.pop(cid, None)

    async def send_offer(self, sdp: str) -> str:
        """Send the SDP offer and return the answer's SDP.

        The answer's `type` is not returned: there is exactly one thing an
        `answer` can be, and a caller that had to check it would be checking
        against a constant.
        """
        loop = asyncio.get_running_loop()
        self._answer_future = loop.create_future()
        await self._send({"offer": {"sdp": sdp, "type": "offer"}})
        answer = await self._await(self._answer_future, self._request_timeout, "answer")
        if not isinstance(answer, dict) or not answer.get("sdp"):
            raise SignalProtocolError("the server's answer carried no sdp")
        return answer["sdp"]

    async def close(self) -> None:
        """End the session. Safe to call more than once, and safe to call on a
        client that never connected.

        `leave` is best effort: it is a courtesy to the server's room
        bookkeeping, and a socket that is already gone is exactly the case
        where it cannot be sent and does not matter.
        """
        if self._closing:
            return
        self._closing = True
        self._cancel_ping()
        socket, self._socket = self._socket, None
        if socket is not None:
            try:
                await socket.send(json.dumps({"leave": {"reason": "CLIENT_INITIATED"}}))
            except Exception as exc:  # noqa: BLE001 - already leaving; nothing to report to
                log.debug("Could not send leave: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(socket.close(), CLOSE_TIMEOUT_S)
            except Exception as exc:  # noqa: BLE001 - same
                log.debug("Could not close the signalling socket: %s", type(exc).__name__)
        await self._stop_reader()
        self._finish("closed by the bridge")

    # -- the read loop -----------------------------------------------------

    async def _read_loop(self, socket) -> None:
        # The socket is an argument rather than `self._socket`: `close()` and
        # the ping loop both clear that attribute the moment they decide the
        # session is over, and a loop that re-read it would fail with an
        # AttributeError instead of with the reason it ended.
        reason = "the signalling socket ended"
        try:
            while True:
                frame = await socket.recv()
                if isinstance(frame, (bytes, bytearray)):
                    self._on_binary(bytes(frame))
                else:
                    self._on_text(frame)
        except asyncio.CancelledError:
            raise
        except SignalError as exc:
            reason = str(exc)
        except Exception as exc:  # noqa: BLE001 - the session ends; it does not raise
            # Type name only: this exception came out of a library that was
            # handed a URL containing the token.
            reason = "the signalling socket failed ({})".format(type(exc).__name__)
        finally:
            self._finish(reason)

    def _on_binary(self, frame: bytes) -> None:
        """The two frames the server sent before it saw our first TEXT frame.

        `parse_join` returns `None` for `refreshToken`, which is the whole of
        how that message is ignored. A frame that does not decode fails
        `connect()` while it is still waiting for `join` -- which is the only
        moment it can mean anything -- and is logged and dropped after that:
        the two binary frames both arrive before the flip, so undecodable bytes
        later are not a message this client was going to read anyway.
        """
        try:
            info = parse_join(frame)
        except SignalProtocolError as exc:
            self._fail_join(exc)
            log.error("Undecodable binary signalling frame: %s", exc)
            return
        if info is None:
            log.debug("Ignoring a binary signalling frame that is not join (%d bytes)", len(frame))
            return
        if self._join_future is not None and not self._join_future.done():
            self._join_future.set_result(info)

    def _on_text(self, frame: str) -> None:
        try:
            message = json.loads(frame)
        except ValueError:
            log.debug("Ignoring a signalling frame that is not JSON (%d bytes)", len(frame))
            return
        if not isinstance(message, dict) or not message:
            log.debug("Ignoring an empty signalling message")
            return
        key = next(iter(message))
        payload = message[key]
        if key == "trackPublished":
            self._on_track_published(payload)
        elif key == "answer":
            if self._answer_future is not None and not self._answer_future.done():
                self._answer_future.set_result(payload)
        elif key == "trickle":
            self._on_remote_trickle(payload)
        elif key == "subscribedQualityUpdate":
            self._invoke(self._on_quality_update, payload, "subscribedQualityUpdate")
        elif key == "leave":
            log.info("LiveKit asked the bridge to leave: %s", json.dumps(payload))
            self._invoke(self._on_leave, payload, "leave")
        elif key == "pongResp":
            self._unanswered_pings = 0
        elif key == "join":
            # Never seen from v1.13.5: `join` is written before the server has
            # read our first frame, so it is always binary. If a future server
            # sends it as JSON, this client does not silently wait out
            # `JOIN_TIMEOUT_S` and blame the network -- it says which of the two
            # happened. A second JSON reader for `join` is deliberately not
            # here: one decoder for one message, and a named failure when the
            # premise it rests on stops holding.
            self._fail_join(
                SignalProtocolError(
                    "the server sent `join` as JSON; this client reads it as protobuf"
                )
            )
        else:
            # Everything the bridge does not act on -- `update`, `roomUpdate`,
            # `connectionQuality`, `trackSubscribed`, `refreshToken`, and
            # whatever a later server adds. Logged, never raised: a client that
            # dies on an unknown message is a client that dies on an upgrade.
            log.debug("Ignoring signalling message %r", key)

    def _on_track_published(self, payload: Any) -> None:
        cid = payload.get("cid") if isinstance(payload, dict) else None
        try:
            future = self._track_futures.get(cid) if cid is not None else None
        except TypeError:
            # A server that sends an unhashable cid (a dict, a list) -- this
            # dict lookup is the only thing standing between that and an
            # uncaught TypeError climbing out of _on_text and ending the
            # session over a shape this handler exists to ignore, the same
            # as any other cid it does not recognise.
            log.debug("Ignoring trackPublished for an unhashable cid %r", cid)
            return
        if future is None:
            log.debug("Ignoring trackPublished for an unknown cid %r", cid)
            return
        if not future.done():
            future.set_result(payload)

    def _on_remote_trickle(self, payload: Any) -> None:
        raw = payload.get("candidateInit") if isinstance(payload, dict) else None
        if not raw:
            log.debug("Ignoring a trickle with no candidateInit")
            return
        try:
            init = json.loads(raw)
        except (ValueError, TypeError):
            # TypeError alongside ValueError: json.loads raises TypeError,
            # not ValueError, when handed anything that is not
            # str/bytes/bytearray -- a candidateInit sent as a JSON object,
            # number or array rather than the string this server always
            # sends today. Left uncaught, that TypeError climbs out of
            # _on_text and out of _read_loop's catch-all, ending the session
            # and blaming "the signalling socket failed" on a bug in this
            # module's own handler rather than in the transport.
            log.debug("Ignoring a trickle whose candidateInit is not JSON")
            return
        if not isinstance(init, dict) or not init.get("candidate"):
            log.debug("Ignoring a trickle with no candidate")
            return
        self._invoke(self._on_trickle, init, "trickle")

    # -- pings -------------------------------------------------------------

    def _ping_interval(self) -> float:
        if self._join is not None and self._join.ping_interval_s > 0:
            return float(self._join.ping_interval_s)
        return DEFAULT_PING_INTERVAL_S

    def _allowed_missed_pings(self) -> int:
        """How many pings may go unanswered before the session is dead.

        Counted rather than timed. A clock would have to be faked to test this,
        and a faked clock beside an injected `sleep` is a second mechanism for
        one question; counting unanswered pings needs neither and is what the
        interval and the timeout together already say. The cost is stated
        rather than hidden: a loop that is starved for longer than the timeout
        is not detected by this, and the socket's own end is what catches that.
        """
        timeout = DEFAULT_PING_TIMEOUT_S
        if self._join is not None and self._join.ping_timeout_s > 0:
            timeout = float(self._join.ping_timeout_s)
        return max(1, int(timeout // self._ping_interval()))

    async def _ping_loop(self, interval: float, allowed: int) -> None:
        while True:
            await self._sleep(interval)
            if self._unanswered_pings >= allowed:
                log.error(
                    "LiveKit signalling is silent: %d pings unanswered; ending the session",
                    self._unanswered_pings,
                )
                # _finish is synchronous and latches: called before the socket
                # is closed, it fixes THIS reason as the one on_closed (and the
                # cloud) sees. Reversed, `await socket.close()` yields to the
                # event loop first -- aiohttp hands the parked reader a CLOSING
                # message immediately, so _read_loop's own `finally` reaches
                # _finish first with "the signalling socket closed", and the
                # one distinction this loop exists to make (the server went
                # silent, versus the peer closed the socket) is lost every
                # time.
                self._finish("no pongResp for {} pings".format(self._unanswered_pings))
                socket, self._socket = self._socket, None
                if socket is not None:
                    try:
                        await socket.close()
                    except Exception as exc:  # noqa: BLE001 - the session is over either way
                        log.debug("Could not close a silent socket: %s", type(exc).__name__)
                return
            try:
                await self._send_ping()
            except SignalError:
                return
            except Exception as exc:  # noqa: BLE001 - the read loop reports the end
                log.debug("Could not send a pingReq: %s", type(exc).__name__)
                return

    async def _send_ping(self) -> None:
        self._unanswered_pings += 1
        await self._send(
            {"pingReq": {"timestamp": str(int(time.time() * 1000)), "rtt": "0"}}
        )

    def _cancel_ping(self) -> None:
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None

    # -- plumbing ----------------------------------------------------------

    async def _send(self, message: Dict[str, Any]) -> None:
        socket = self._socket
        if socket is None:
            raise SignalClosed("the signalling socket is not open")
        log.debug("LiveKit signalling out: %s", next(iter(message), "?"))
        await socket.send(json.dumps(message))

    async def _await(self, future: "asyncio.Future", timeout: float, what: str) -> Any:
        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            raise SignalTimeout("no {} within {:g} s".format(what, timeout)) from None

    def _fail_join(self, exc: SignalError) -> None:
        if self._join_future is not None and not self._join_future.done():
            self._join_future.set_exception(exc)

    def _finish(self, reason: str) -> None:
        """The single place a session ends. Every pending wait is failed here,
        so nothing can be left holding a future the read loop will never
        resolve, and `on_closed` fires from here or not at all."""
        if self._finished:
            return
        self._finished = True
        self._cancel_ping()
        closed = SignalClosed(reason)
        for future in [self._join_future, self._answer_future] + list(self._track_futures.values()):
            if future is not None and not future.done():
                future.set_exception(closed)
        if not self._closing:
            self._invoke(self._on_closed, reason, "closed")

    async def _stop_reader(self) -> None:
        task, self._reader_task = self._reader_task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001 - the reader reports through _finish
            log.debug("The signalling reader ended with %s", type(exc).__name__)

    def _invoke(self, callback: Optional[Callable[[Any], None]], argument: Any, what: str) -> None:
        if callback is None:
            return
        try:
            callback(argument)
        except Exception as exc:  # noqa: BLE001 - a callback must not kill the read loop
            log.error("A LiveKit signalling %s callback raised %s", what, type(exc).__name__)
