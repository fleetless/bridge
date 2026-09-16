# SPDX-License-Identifier: Apache-2.0
"""`client.py` was read structurally, not audited for credentials. The
three leaks that were found shared one mechanism: a secret embedded in a
connection string, echoed back by the parser that choked on it.
`client.py` doesn't have that shape: its one credential
(`BridgeConfig.token`) never rides in a URL, and only reaches
`hello_message(...)` — a pure JSON-string builder, never anything that
raises or formats an exception. A read is not a clearance; this file
provokes the leak instead of assuming its absence.

Every test below drives a real failure path in `client.py` that logs an
exception or error string, with a deliberately distinctive token, and
checks every log line produced (all levels, via a recording double — the
suite's README notes `caplog` passes unconditionally here, testing
nothing). If a future change logs `self._config` directly, or embeds the
token in a URL, this is the test that turns red."""
import asyncio

import fleetless_bridge.client as client_module
from fake_cloud import FakeCloud, rejects, silent
from fleetless_bridge.client import BridgeClient
from fleetless_bridge.config import BridgeConfig
from helpers import RecordingSleep, make_client

# Distinctive enough that an accidental substring match can't produce a
# false negative.
_SECRET_TOKEN = "frt_MUST_NEVER_APPEAR_IN_A_LOG_LINE_9f8e7d6c"


class _RecordingLog:
    """Captures every level, formatted. `test_client_datapoints.py`'s
    `_RecordingLog` only captures `warning` — enough there, not here:
    several paths this audit cares about (terminal rejection, being
    superseded, being deleted) log at `error`."""

    def __init__(self) -> None:
        self.messages = []

    def _record(self, fmt, args):
        try:
            self.messages.append(fmt % args if args else str(fmt))
        except Exception:  # noqa: BLE001 - formatting itself must never hide a message from this audit
            self.messages.append(repr((fmt, args)))

    def info(self, fmt, *args, **kwargs):
        self._record(fmt, args)

    def warning(self, fmt, *args, **kwargs):
        self._record(fmt, args)

    def error(self, fmt, *args, **kwargs):
        self._record(fmt, args)

    def debug(self, fmt, *args, **kwargs):
        self._record(fmt, args)

    def exception(self, fmt, *args, **kwargs):
        self._record(fmt, args)


def _assert_token_never_logged(recording_log: _RecordingLog) -> None:
    for message in recording_log.messages:
        assert _SECRET_TOKEN not in message, "token leaked into a log line: {!r}".format(message)


def test_bridge_config_repr_never_includes_the_token():
    """The guard this audit found already in place: a hand-written
    `__repr__` on `BridgeConfig`, because the generated one would carry
    the token into any log line, traceback frame or debugger transcript
    that touched a config object. Locked in as a regression test, not
    just noted in a message."""
    config = BridgeConfig(token=_SECRET_TOKEN, cloud_url="wss://example.test/bridge")
    assert _SECRET_TOKEN not in repr(config)
    assert _SECRET_TOKEN not in str(config)


def test_an_unreachable_cloud_never_logs_the_token(monkeypatch):
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def connect_refused(_url):
        raise ConnectionRefusedError("connection refused")

    async def scenario():
        client = BridgeClient(
            BridgeConfig(token=_SECRET_TOKEN, cloud_url="ws://127.0.0.1:1/bridge"),
            connect=connect_refused,
            sleep=RecordingSleep(),
        )
        task = asyncio.ensure_future(client.run())
        await asyncio.sleep(0.05)
        client.stop()
        await asyncio.wait_for(task, timeout=3)

    asyncio.run(scenario())
    _assert_token_never_logged(recording_log)


def test_a_cloud_url_that_will_not_parse_never_logs_the_token(monkeypatch):
    """`_one_session`'s `InvalidURI` branch — the one place client.py logs
    `self._config.cloud_url` itself. Not a secret today (the token
    travels in-band in the hello JSON body, never the URL), but proven,
    not assumed: if a future change made the URL token-bearing, this is
    the log line that would leak it."""
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        client = BridgeClient(
            BridgeConfig(token=_SECRET_TOKEN, cloud_url="not a real url"),
            sleep=RecordingSleep(),
        )
        return await asyncio.wait_for(client.run(), timeout=3)

    asyncio.run(scenario())
    _assert_token_never_logged(recording_log)


def test_a_connection_dropped_before_the_hello_never_logs_the_token(monkeypatch):
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    class DropsBeforeSend:
        async def send(self, *_args):
            raise ConnectionError("dropped before send")

        async def recv(self):
            await asyncio.Event().wait()  # never returns — the test stops the client instead

        async def close(self):
            pass

    async def connect(_url):
        return DropsBeforeSend()

    async def scenario():
        client = BridgeClient(
            BridgeConfig(token=_SECRET_TOKEN, cloud_url="ws://x/bridge"),
            connect=connect,
            sleep=RecordingSleep(),
        )
        task = asyncio.ensure_future(client.run())
        await asyncio.sleep(0.1)
        client.stop()
        await asyncio.wait_for(task, timeout=3)

    asyncio.run(scenario())
    _assert_token_never_logged(recording_log)


def test_a_handshake_that_never_completes_never_logs_the_token(monkeypatch):
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        async with FakeCloud(silent()) as cloud:
            client = make_client(cloud, token=_SECRET_TOKEN, handshake_timeout=0.2)
            task = asyncio.ensure_future(client.run())
            await asyncio.sleep(0.5)  # a real handshake timeout, at least once
            client.stop()
            await asyncio.wait_for(task, timeout=3)

    asyncio.run(scenario())
    _assert_token_never_logged(recording_log)


def test_a_terminal_rejection_never_logs_the_token(monkeypatch):
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        async with FakeCloud(rejects("invalid_token", "bad token")) as cloud:
            client = make_client(cloud, token=_SECRET_TOKEN)
            return await asyncio.wait_for(client.run(), timeout=3)

    asyncio.run(scenario())
    _assert_token_never_logged(recording_log)
