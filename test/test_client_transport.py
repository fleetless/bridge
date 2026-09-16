# SPDX-License-Identifier: Apache-2.0
"""The socket under `client.py`, and the four ways a session can fail on it.

`client.py` catches exactly four kinds and gives each a different answer:

===========================  ==========================================
raised where                 answer
===========================  ==========================================
`_one_session`, bad URL      stop for good — nobody will fix this by
                             retrying
`_one_session`, anything     retry after a backoff
else
`_converse`, closed          read the close code and let it decide
`_converse`, failed          retry after a backoff
===========================  ==========================================

Those four are the contract; the library beneath them is not — it was
replaced, which is why this file exists. Each test drives ONE of the four
through a real socket, not an injected fake: the thing under test is the
mapping from what the library raises to what these branches name, and a fake
that raises the right exception proves only the branch, not the mapping.

**The two that were unreachable before this file.** A frame over the client's
own ceiling arrives as `WSMsgType.ERROR` carrying an `aiohttp.WebSocketError`
— neither an `OSError` nor an `aiohttp.ClientError`, so re-raising it
unchanged escapes all four branches and kills the session with a traceback. A
cloud that is killed rather than closed leaves no close code at all. Neither
state had a test, and neither can come from a fake that hangs up politely.
"""
import asyncio
import socket

import aiohttp
import fleetless_bridge.client as client_module
import pytest
from fake_cloud import FakeCloud, accepts, accepts_then_vanishes, sequence
from fleetless_bridge.client import (
    ConnectionClosed,
    ConnectionFailed,
    StopReason,
    websocket_connect,
)
from fleetless_bridge.config import BridgeConfig
from helpers import RecordingSleep, make_client, run_until
# Borrowed, not copied: `client.py` logs through one module-level `log`,
# and this suite already has a double recording every level of it. `caplog`
# cannot be used here — it passes unconditionally in this environment,
# having recorded nothing (see that file's own docstring).
from test_client_credential_audit import _RecordingLog


def _closed_port() -> int:
    """A port nothing is listening on: bind to learn the number, then
    release it. Something else could grab it in the gap — hence the test
    asserts a retry, not a specific errno; any way the connection can fail to
    open belongs to the same branch."""
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


# --- _one_session: the URL nobody can fix by retrying -----------------------


def test_a_cloud_url_that_will_not_parse_stops_the_bridge():
    """`aiohttp.InvalidURL`, and the branch that must stay ahead of the
    retry one: `InvalidURL` is an `aiohttp.ClientError` too, so with the two
    branches in the other order this URL would be retried forever."""

    async def scenario():
        client = client_module.BridgeClient(
            BridgeConfig(token="frt_test_token", cloud_url="not a real url"),
            sleep=RecordingSleep(),
        )
        return await asyncio.wait_for(client.run(), timeout=5)

    assert asyncio.run(scenario()) is StopReason.REJECTED


def test_an_http_url_is_refused_rather_than_retried():
    """The same branch, and the case that earned its own test.

    `https://host/bridge` is a URL a WebSocket library may legitimately
    accept — the upgrade is an HTTP request, so aiohttp will happily try it.
    This bridge must not: `FLEETLESS_CLOUD_URL` names a WebSocket endpoint,
    and an operator with the wrong scheme needs to be told, not reconnected
    forever against a host that will never speak this protocol. Without the
    scheme check in `websocket_connect`, this test does not fail — it never
    returns.
    """

    async def scenario():
        client = client_module.BridgeClient(
            BridgeConfig(
                token="frt_test_token", cloud_url="https://api.example.test/bridge"
            ),
            sleep=RecordingSleep(),
        )
        return await asyncio.wait_for(client.run(), timeout=5)

    assert asyncio.run(scenario()) is StopReason.REJECTED


# --- _one_session: everything else, which is worth another go ---------------


def test_a_refused_connection_is_retried():
    """`aiohttp.ClientConnectorError`, which is an `OSError`."""

    async def scenario():
        sleep = RecordingSleep()
        client = client_module.BridgeClient(
            BridgeConfig(
                token="frt_test_token",
                cloud_url="ws://127.0.0.1:{}/bridge".format(_closed_port()),
            ),
            sleep=sleep,
        )
        task = asyncio.ensure_future(client.run())
        deadline = asyncio.get_event_loop().time() + 10.0
        while len(sleep.delays) < 2 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.01)
        client.stop()
        return await asyncio.wait_for(task, timeout=5), sleep.delays

    reason, delays = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    # Two attempts, so the first failure ended in "try again" rather than in
    # a stop or a traceback.
    assert len(delays) >= 2


def test_a_cloud_that_refuses_the_upgrade_is_retried():
    """`aiohttp.WSServerHandshakeError` — a `ClientError` that is **not** an
    `OSError`, which is why that branch names both. A proxy that answers the
    upgrade with an ordinary HTTP response lands here; `except OSError` alone
    would let it through as a crash.
    """

    async def scenario():
        from aiohttp import web

        async def not_a_websocket(_request):
            return web.Response(text="not here")

        app = web.Application()
        app.router.add_get("/{tail:.*}", not_a_websocket)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        await web.SockSite(runner, listener).start()
        try:
            sleep = RecordingSleep()
            client = client_module.BridgeClient(
                BridgeConfig(
                    token="frt_test_token",
                    cloud_url="ws://127.0.0.1:{}/bridge".format(port),
                ),
                sleep=sleep,
            )
            task = asyncio.ensure_future(client.run())
            deadline = asyncio.get_event_loop().time() + 10.0
            while len(sleep.delays) < 2 and asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.01)
            client.stop()
            return await asyncio.wait_for(task, timeout=5), sleep.delays
        finally:
            await runner.cleanup()

    reason, delays = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    assert len(delays) >= 2


# --- _converse: the socket closed -------------------------------------------


def test_a_cloud_that_vanishes_without_a_close_frame_is_retried(monkeypatch):
    """The killed-cloud state: no close frame, so `_reason_for_close` gets
    whatever the library could work out on its own, and the session ends in
    a reconnect rather than a stop.

    Distinct from `test_client_reconnect.py`'s close-code tests, which all
    hang up politely — this is the state no fake cloud could produce before.

    **A reconnect alone would not prove this.** A bridge that never noticed
    the socket was gone would also reconnect: its idle timeout fires two
    seconds later and ends the session for a different reason. The log line
    is the assertion; the absent one is the point of it.
    """
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        async with FakeCloud(sequence(accepts_then_vanishes(), accepts())) as cloud:
            client = make_client(cloud)
            reason = await run_until(client, lambda: cloud.connections >= 2)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    assert connections >= 2
    assert [m for m in recording_log.messages if m.startswith("The cloud closed the connection")], (
        "the session did not end through the closed branch: {}".format(recording_log.messages)
    )
    assert not [m for m in recording_log.messages if "treating the connection as dead" in m], (
        "the socket's end went unnoticed and the idle timeout ended the session "
        "instead: {}".format(recording_log.messages)
    )


# --- _converse: the socket failed -------------------------------------------


def _connect_with_ceiling(max_msg_size: int):
    async def connect(url):
        return await websocket_connect(url, max_msg_size=max_msg_size)

    return connect


def test_a_frame_over_the_ceiling_ends_the_session_and_the_bridge_reconnects(monkeypatch):
    """The `WSMsgType.ERROR` path — what `ConnectionFailed` exists for. The
    cloud sends a frame past what this client will read; the library hands
    `recv()` an `aiohttp.WebSocketError`, which inherits from `Exception` and
    nothing else. This asserts the session ended through the branch that logs
    "The connection failed" — not a traceback, not the close branch.
    """
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def oversized(session):
        await session.recv_hello()
        await session.accept()
        await session.send_raw("x" * 20_000)
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(oversized, accepts())) as cloud:
            client = make_client(cloud, connect=_connect_with_ceiling(4096))
            reason = await run_until(client, lambda: cloud.connections >= 2)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    assert connections >= 2
    # Named: "The connection failed" is logged by this branch and no other
    # in the file.
    assert [m for m in recording_log.messages if m.startswith("The connection failed")], (
        "the session ended somewhere other than the failed-socket branch: {}".format(
            recording_log.messages
        )
    )


# --- the adapter's own contract ---------------------------------------------
#
# Above: what a session does. These two ask the narrower question the
# branches rest on — that `recv()` never raises outside the set they name.
# A class that escapes that set is not a wrong answer, it's an unhandled
# exception in a running robot.

_CAUGHT_BY_THE_CLIENT = (ConnectionClosed, OSError, aiohttp.ClientError)


def test_recv_raises_connection_closed_when_the_cloud_hangs_up():
    async def scenario():
        async def closes_at_once(session):
            await session.close(4000, "")

        async with FakeCloud(closes_at_once) as cloud:
            sock = await websocket_connect(cloud.url)
            try:
                with pytest.raises(ConnectionClosed) as excinfo:
                    await asyncio.wait_for(sock.recv(), timeout=5)
                return excinfo.value, sock.close_code
            finally:
                await sock.close()

    error, close_code = asyncio.run(scenario())
    assert isinstance(error, _CAUGHT_BY_THE_CLIENT)
    # The code the cloud sent, not one the library invented — the value
    # `_reason_for_close` decides a supersede on.
    assert close_code == 4000


def test_recv_raises_a_caught_kind_when_a_frame_is_over_the_ceiling():
    async def scenario():
        async def sends_too_much(session):
            await session.send_raw("x" * 20_000)
            await session.drain()

        async with FakeCloud(sends_too_much) as cloud:
            sock = await websocket_connect(cloud.url, max_msg_size=4096)
            try:
                with pytest.raises(ConnectionFailed) as excinfo:
                    await asyncio.wait_for(sock.recv(), timeout=5)
                return excinfo.value
            finally:
                await sock.close()

    error = asyncio.run(scenario())
    assert isinstance(error, _CAUGHT_BY_THE_CLIENT)
    # Specifically: it arrived as `aiohttp.ClientError`, what the `_converse`
    # branch names. `aiohttp.WebSocketError` — what the library actually
    # handed over — is not one, which is why this class is wrapped rather
    # than re-raised.
    assert isinstance(error, aiohttp.ClientError)
    assert not isinstance(error, aiohttp.WebSocketError)
