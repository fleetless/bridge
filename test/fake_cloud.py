# SPDX-License-Identifier: Apache-2.0
"""An in-process stand-in for the cloud's /bridge endpoint.

The suite drives the real client against a real WebSocket server on a real
ephemeral port, so what the tests exercise is wire behaviour rather than a mock
of it. Every frame crossing the fake is validated against the vendored
contracts artifacts, so the fake cannot quietly drift into a cloud that does
not exist.

A test supplies a *behaviour*: an async function that receives a `Session` per
connection and scripts what the cloud does on it.
"""
import asyncio
import itertools
import json
import socket
import struct
from typing import Optional

from aiohttp import WSMsgType, web

from schemas import validate_frame

RECV_TIMEOUT_S = 5.0

# What a real cloud accepts per frame — stated here, not inherited from a
# library default: `ws/bridge.ts` sets `maxPayload` on /bridge, and a fake
# whose ceiling moved with a dependency's default would model the dependency,
# not the cloud. Overridable per test so an oversize proof can run small.
DEFAULT_MAX_FRAME_BYTES = 1 << 20


class ConnectionClosed(Exception):
    """The bridge under test hung up. Raised by `_ServerSocket.recv` so a
    behaviour written as a straight `recv`/`send` line ends by raising, not
    by every behaviour inspecting a message type.

    Deliberately not `client.py`'s own class: sharing an exception type with
    the code under test would let the fake agree with it about a mistake.
    """


class _ServerSocket:
    """The cloud's end of one connection, reduced to the three calls
    `Session` makes: `recv`, `send`, `close`.

    `web.WebSocketResponse` speaks typed messages; `Session` and every
    behaviour here speak payloads and exceptions — what the bridge's own
    socket speaks too. One adapter keeps that difference out of thirty
    behaviours.
    """

    def __init__(self, ws: "web.WebSocketResponse", transport=None) -> None:
        self._ws = ws
        self._transport = transport

    def abort(self) -> None:
        """Rip the socket out from under the bridge with no close frame at
        all — a cloud process being killed, not a cloud hanging up. There is
        no close code for the bridge to read afterwards, which is the whole
        point of the state."""
        self._transport.abort()

    async def recv(self):
        message = await self._ws.receive()
        if message.type in (WSMsgType.TEXT, WSMsgType.BINARY):
            return message.data
        if message.type is WSMsgType.ERROR:
            raise ConnectionClosed(
                "the socket failed: {}".format(message.data)
            )
        # CLOSE, CLOSING, CLOSED — the bridge went away, or this side is
        # being torn down by `FakeCloud.__aexit__` while a behaviour sits in
        # `drain()`.
        raise ConnectionClosed("the bridge closed the connection")

    async def send(self, payload) -> None:
        if isinstance(payload, str):
            await self._ws.send_str(payload)
        else:
            await self._ws.send_bytes(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._ws.close(code=code, message=reason.encode())


# The cloud's hello_ok carries a real robot uuid, and the schema enforces the
# RFC 4122 variant bits, so the fixtures have to be genuine uuids too.
ROBOT_ID = "2c89f740-49a6-437e-bfdc-8895a057b347"
OTHER_ROBOT_ID = "656c3e20-721d-4ae0-aee6-7c772147c2f0"


class Session:
    """One bridge connection, seen from the cloud's side."""

    def __init__(self, cloud: "FakeCloud", ws) -> None:
        self._cloud = cloud
        self._ws = ws

    async def recv(self) -> dict:
        raw = await asyncio.wait_for(self._ws.recv(), timeout=RECV_TIMEOUT_S)
        return json.loads(raw)

    async def recv_raw(self):
        """The frame exactly as it arrived: `str` for text, `bytes` for
        binary. `recv()` assumes text since every other frame kind is JSON —
        snapshot is the one exception, and JSON-decoding it would raise on
        the binary payload rather than parse it."""
        return await asyncio.wait_for(self._ws.recv(), timeout=RECV_TIMEOUT_S)

    async def recv_snapshot(self):
        """Unpacks a binary snapshot frame — `[4-byte BE header length]
        [UTF-8 JSON header][image bytes]` — validates the header against
        the vendored `snapshot-header` schema, and returns `(header,
        image_bytes)`."""
        wire = await self.recv_raw()
        (header_len,) = struct.unpack(">I", wire[:4])
        header = validate_frame("snapshot-header", json.loads(wire[4 : 4 + header_len]))
        image_bytes = wire[4 + header_len :]
        self._cloud.snapshots.append((header, image_bytes))
        return header, image_bytes

    async def recv_hello(self) -> dict:
        payload = validate_frame("bridge-hello", await self.recv())
        self._cloud.hellos.append(payload)
        return payload

    async def recv_pong(self) -> dict:
        payload = validate_frame("bridge-pong", await self.recv())
        self._cloud.pongs.append(payload)
        return payload

    async def recv_config_applied(self) -> dict:
        payload = validate_frame("bridge-config-applied", await self.recv())
        self._cloud.config_applied.append(payload)
        return payload

    async def recv_introspect(self) -> dict:
        payload = validate_frame("bridge-introspect", await self.recv())
        self._cloud.introspects.append(payload)
        return payload

    async def recv_type_definitions(self) -> dict:
        payload = validate_frame("bridge-type-definitions", await self.recv())
        self._cloud.type_definitions.append(payload)
        return payload

    async def recv_datapoint(self) -> dict:
        payload = validate_frame("datapoint-frame", await self.recv())
        self._cloud.datapoints.append(payload)
        return payload

    async def recv_job_update(self) -> dict:
        payload = validate_frame("bridge-job-update", await self.recv())
        self._cloud.job_updates.append(payload)
        return payload

    async def recv_job_lost(self) -> dict:
        payload = validate_frame("bridge-job-lost", await self.recv())
        self._cloud.job_lost.append(payload)
        return payload

    async def accept(self, robot_id: str = ROBOT_ID) -> None:
        await self._send("cloud-hello-ok", {"type": "hello_ok", "robot_id": robot_id})

    async def reject(self, code: str, message: str = "refused by the fake cloud") -> None:
        await self._send(
            "cloud-hello-error",
            {"type": "hello_error", "code": code, "message": message},
        )

    async def ping(self, ts_ms: int) -> None:
        await self._send("cloud-ping", {"type": "ping", "ts_ms": ts_ms})

    async def send_config(
        self,
        version: int,
        datapoints: dict,
        *,
        actions: dict = None,
        services: dict = None,
        publishers: dict = None,
        cameras: dict = None,
        messages: dict = None,
    ) -> None:
        # Every section is a slug-keyed mapping and optional, so an omitted
        # section stays omitted rather than empty — the shape a real cloud
        # sends for a robot with no cameras, and what the vendored schema
        # checks here. `fleetless` is the format version, the one key `doc`
        # cannot be sent without.
        doc = {"fleetless": 1, "datapoints": datapoints}
        for key, section in (
            ("messages", messages),
            ("actions", actions),
            ("services", services),
            ("publishers", publishers),
            ("cameras", cameras),
        ):
            if section is not None:
                doc[key] = section
        await self._send(
            "cloud-config",
            {"type": "config", "version": version, "doc": doc},
        )

    async def send_introspect_request(self, request_id: str) -> None:
        await self._send(
            "cloud-introspect-request",
            {"type": "introspect_request", "request_id": request_id},
        )

    async def send_type_request(self, request_id: str, type_names: list) -> None:
        await self._send(
            "cloud-type-request",
            {
                "type": "type_request",
                "request_id": request_id,
                "type_names": type_names,
            },
        )

    async def send_invoke(
        self, job_id: str, slug: str, params: dict, patience_ms: int = 15_000
    ) -> None:
        # patience_ms defaults to 15s (DEFAULT_PATIENCE_MS in
        # contracts) — required on the wire, but most callers of this helper
        # aren't testing patience itself and shouldn't all have to say so.
        await self._send(
            "cloud-invoke",
            {
                "type": "invoke",
                "job_id": job_id,
                "slug": slug,
                "params": params,
                "patience_ms": patience_ms,
            },
        )

    async def send_cancel(self, slug: str, job_id: Optional[str] = None) -> None:
        # job_id defaults to None — "cancel whatever is running on
        # this slug", today's behaviour, and the meaning every existing
        # caller of this helper already relies on.
        await self._send("cloud-cancel", {"type": "cancel", "slug": slug, "job_id": job_id})

    async def send_publish(self, slug: str, message: dict) -> None:
        await self._send(
            "cloud-publish", {"type": "publish", "slug": slug, "message": message}
        )

    async def send_camera_start(
        self, slug: str, url: str, room: str, token: str, request_id: str = "req-1"
    ) -> None:
        await self._send(
            "cloud-camera-start",
            {
                "type": "camera_start",
                "slug": slug,
                "url": url,
                "room": room,
                "token": token,
                "request_id": request_id,
            },
        )

    async def send_camera_stop(self, slug: str, request_id: str = "req-1") -> None:
        await self._send(
            "cloud-camera-stop",
            {"type": "camera_stop", "slug": slug, "request_id": request_id},
        )

    async def send_asset_request(
        self, sync_id: str, upload_url: str, token: str, meshes=()
    ) -> None:
        await self._send(
            "cloud-asset-request",
            {
                "type": "asset_request",
                "sync_id": sync_id,
                "upload_url": upload_url,
                "token": token,
                "meshes": list(meshes),
            },
        )

    async def recv_camera_state(self) -> dict:
        payload = validate_frame("bridge-camera-state", await self.recv())
        self._cloud.camera_states.append(payload)
        return payload

    async def recv_assets_available(self) -> dict:
        payload = validate_frame("bridge-assets-available", await self.recv())
        self._cloud.assets_available.append(payload)
        return payload

    async def recv_asset_progress(self) -> dict:
        payload = validate_frame("bridge-asset-progress", await self.recv())
        self._cloud.asset_progress.append(payload)
        return payload

    async def send_raw(self, text: str) -> None:
        """Send something the contracts do not describe: garbage, or a message
        from a future protocol version."""
        await self._ws.send(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._ws.close(code, reason)

    def abort(self) -> None:
        """The cloud's process dies. See `_ServerSocket.abort`."""
        self._ws.abort()

    async def drain(self) -> None:
        """Hold the connection open until the bridge goes away, saying nothing."""
        while True:
            await self._ws.recv()

    async def _send(self, schema: str, payload: dict) -> None:
        validate_frame(schema, payload)
        await self._ws.send(json.dumps(payload))


class FakeCloud:
    """An async context manager serving `behavior` on an ephemeral port."""

    def __init__(self, behavior, *, max_size=None) -> None:
        self._behavior = behavior
        # None means `DEFAULT_MAX_FRAME_BYTES` — the same kind of ceiling
        # `ws/bridge.ts`'s `maxPayload` enforces on the real cloud (2 MiB).
        # Overridable so a test can use a small ceiling and stay fast,
        # proving an oversized frame gets the connection closed, or that the
        # bridge never sends one.
        self._max_size = max_size if max_size is not None else DEFAULT_MAX_FRAME_BYTES
        self._runner = None
        self._url = None
        self._open_sockets = set()
        self.hellos = []
        self.pongs = []
        self.config_applied = []
        self.introspects = []
        self.type_definitions = []
        self.datapoints = []
        self.job_updates = []
        self.job_lost = []
        self.snapshots = []
        self.camera_states = []
        self.assets_available = []
        self.asset_progress = []
        self.connections = 0
        self.errors = []

    async def __aenter__(self) -> "FakeCloud":
        app = web.Application()
        # Any path: the bridge connects to `/bridge`, and a test that points
        # it somewhere else is testing something other than routing.
        app.router.add_get("/{tail:.*}", self._handle)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        # The listening socket is created here, not by `TCPSite`: port 0 lets
        # the kernel pick, and asking the site which port it got means reading
        # a private attribute off it. This asks the socket instead — the
        # thing that actually knows.
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        self._url = "ws://127.0.0.1:{}".format(listener.getsockname()[1])
        await web.SockSite(self._runner, listener).start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        # Every live socket is closed before the runner: a behaviour parked in
        # `drain()` waits for a frame that will never come, and
        # `AppRunner.cleanup()` waits for its handler. Closing the socket ends
        # that wait — the handler's `recv` raises `ConnectionClosed` and
        # returns.
        #
        # Bounded, because closing is a handshake: the bridge under test may
        # already be gone without a FIN, and then this would wait out the
        # library's own close timeout (ten seconds) for a reply nobody sends.
        # A test's teardown must not pay that cost.
        for ws in list(self._open_sockets):
            try:
                await asyncio.wait_for(ws.close(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
        await self._runner.cleanup()
        # A failed assertion (or: a schema validation failure) inside a
        # behaviour would otherwise vanish into the server task and leave
        # the test passing for the wrong reason — that is what the `if
        # self.errors` half below still catches, unchanged.
        #
        # The half that was missing: if the `async with` block *also*
        # raised (most commonly `run_until`'s own timeout, because the
        # client never got a reply from a handler that had already died),
        # the old code discarded the handler's exception outright and let
        # only the block's propagate — converting a precise, actionable
        # error ("config frame failed schema validation: 'credentials' is
        # a required property") into a vague symptom ("the condition never
        # became true"). That masking cost real debugging time. Chain the
        # handler's exception onto the block's
        # instead of picking one: it is usually the actual root cause (the
        # server-side failure that made the client wait forever), so it
        # becomes the exception raised, with the block's own — what the
        # client actually observed — attached as `__cause__` so both
        # tracebacks are visible.
        if self.errors:
            handler_exc = self.errors[0]
            if exc_type is not None:
                raise handler_exc from exc
            raise handler_exc

    @property
    def url(self) -> str:
        return self._url

    async def _handle(self, request):
        ws = web.WebSocketResponse(max_msg_size=self._max_size)
        await ws.prepare(request)
        self._open_sockets.add(ws)
        self.connections += 1
        try:
            await self._behavior(Session(self, _ServerSocket(ws, request.transport)))
        except (ConnectionClosed, asyncio.TimeoutError):
            pass
        except Exception as exc:  # noqa: BLE001 - re-raised by __aexit__
            self.errors.append(exc)
        finally:
            self._open_sockets.discard(ws)
        # Returned, not closed here: the response object is what aiohttp
        # finishes the request with, and a behaviour that closed the socket
        # itself (`accepts_then_closes`) has already sent its own code.
        return ws


def sequence(*behaviors):
    """Use one behaviour per connection, in order; the last one repeats."""
    counter = itertools.count()

    async def dispatch(session):
        index = min(next(counter), len(behaviors) - 1)
        await behaviors[index](session)

    return dispatch


def accepts(robot_id: str = ROBOT_ID, then=None):
    """Answer the hello with hello_ok, then run `then` (or just stay open)."""

    async def behavior(session):
        await session.recv_hello()
        await session.accept(robot_id)
        if then is None:
            await session.drain()
        else:
            await then(session)

    return behavior


def rejects(code: str, message: str = "refused by the fake cloud"):
    async def behavior(session):
        await session.recv_hello()
        await session.reject(code, message)
        await session.drain()

    return behavior


def rejects_and_closes(code: str, close_code: int = 1008):
    """Refuse the hello and hang up straight away, as the real cloud does."""

    async def behavior(session):
        await session.recv_hello()
        await session.reject(code)
        await session.close(close_code, code)

    return behavior


def silent(read_hello: bool = True):
    """Accept the connection and then say nothing at all."""

    async def behavior(session):
        if read_hello:
            await session.recv_hello()
        await session.drain()

    return behavior


def closes_after_hello(code: int = 1000, reason: str = ""):
    """Accept the connection, then drop it without ever answering."""

    async def behavior(session):
        await session.recv_hello()
        await session.close(code, reason)

    return behavior


def accepts_then_closes(code: int, robot_id: str = ROBOT_ID):
    """Complete the handshake, then close with `code`."""

    async def behavior(session):
        await session.recv_hello()
        await session.accept(robot_id)
        await session.close(code, "")

    return behavior


def accepts_then_vanishes(robot_id: str = ROBOT_ID):
    """Complete the handshake, then take the socket away without closing it.

    The distinction from `accepts_then_closes` is the whole reason this
    exists: no close frame means no close code, so the bridge reaches its
    "the connection is gone" path with nothing to read off the socket — the
    state a killed cloud process leaves behind, and one a fake that only ever
    closes politely can never produce.
    """

    async def behavior(session):
        await session.recv_hello()
        await session.accept(robot_id)
        session.abort()

    return behavior


def pings(*timestamps: int, robot_id: str = ROBOT_ID):
    """Complete the handshake, then probe with each timestamp in turn."""

    async def behavior(session):
        await session.recv_hello()
        await session.accept(robot_id)
        for ts_ms in timestamps:
            await session.ping(ts_ms)
            await session.recv_pong()
        await session.drain()

    return behavior
