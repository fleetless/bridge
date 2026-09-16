# SPDX-License-Identifier: Apache-2.0
"""The one-frame-at-a-time WebSocket server the three `capture_*.py` tools run.

Each of those tools is a one-shot instrument: it listens on a port, a real
bridge is pointed at it by hand, and it prints what crossed the wire. They all
want the same three calls — `recv`, `send`, `close` — over a server socket, and
`aiohttp.web` speaks typed messages instead. This is that adapter, written once
rather than three times, and it is deliberately the smallest thing that serves
the three: no routing, no state, no lifetime beyond `close()`.

Not part of the installed package (`setup.py` ships `fleetless_bridge/` only,
and `tools/` is a development fixture — see the comment in `setup.py`).
"""
import asyncio

from aiohttp import WSMsgType, web


class ConnectionClosed(Exception):
    """The bridge hung up. `recv()` raises it rather than returning a
    sentinel, so a handler written as a straight line of `recv`/`send` ends
    where it stops being able to continue."""


class Socket:
    """One connection, as the capture handlers speak to it."""

    def __init__(self, ws: "web.WebSocketResponse") -> None:
        self._ws = ws

    async def recv(self):
        """The frame: `str` for text, `bytes` for binary."""
        message = await self._ws.receive()
        if message.type in (WSMsgType.TEXT, WSMsgType.BINARY):
            return message.data
        raise ConnectionClosed("the connection ended: {}".format(message.type))

    async def send(self, payload) -> None:
        if isinstance(payload, str):
            await self._ws.send_str(payload)
        else:
            await self._ws.send_bytes(payload)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._ws.close(code=code, message=reason.encode())


class Server:
    """What `serve()` hands back: `close()` then `await wait_closed()`."""

    def __init__(self, runner: "web.AppRunner") -> None:
        self._runner = runner
        self._closing = None

    def close(self) -> None:
        self._closing = asyncio.ensure_future(self._runner.cleanup())

    async def wait_closed(self) -> None:
        if self._closing is None:
            self.close()
        await self._closing


async def serve(handler, host: str, port: int) -> Server:
    """Serve `handler(socket)` on every connection to `host:port`.

    Any path is accepted: the bridge connects to `/bridge`, and a capture run
    that points it somewhere else is not testing routing.
    """

    async def route(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            await handler(Socket(ws))
        except ConnectionClosed as exc:
            print("connection ended: {}".format(exc))
        return ws

    app = web.Application()
    app.router.add_get("/{tail:.*}", route)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    return Server(runner)
