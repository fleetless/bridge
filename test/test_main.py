# SPDX-License-Identifier: Apache-2.0
"""The entry point: what a supervisor learns from the exit code, and whether
the bridge stops when the container tells it to."""
import asyncio
import logging
import os
import signal
import types

import pytest
from fake_cloud import FakeCloud, accepts
from fleetless_bridge import main as entry
from fleetless_bridge.client import BridgeClient, StopReason
from fleetless_bridge.config import BridgeConfig


def test_a_missing_token_is_reported_as_unfixable(monkeypatch):
    monkeypatch.delenv("FLEETLESS_TOKEN", raising=False)
    assert entry.run() == entry.EXIT_WONT_RETRY


def test_an_unusable_cloud_url_is_reported_as_unfixable(monkeypatch):
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_abc")
    # A plain https URL is not a WebSocket URL. Retrying would never fix it.
    monkeypatch.setenv("FLEETLESS_CLOUD_URL", "https://api.example.test/bridge")
    assert entry.run() == entry.EXIT_WONT_RETRY


def test_a_rejection_is_reported_as_unfixable(monkeypatch):
    monkeypatch.setattr(entry, "_serve", _returning(StopReason.REJECTED))
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_abc")
    assert entry.run() == entry.EXIT_WONT_RETRY


def test_a_requested_shutdown_is_reported_as_success(monkeypatch):
    monkeypatch.setattr(entry, "_serve", _returning(StopReason.SHUTDOWN))
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_abc")
    assert entry.run() == entry.EXIT_OK


def test_an_unexpected_failure_is_reported_as_an_error(monkeypatch):
    async def explode(_client):
        raise RuntimeError("something nobody thought of")

    monkeypatch.setattr(entry, "_serve", explode)
    monkeypatch.setenv("FLEETLESS_TOKEN", "frt_abc")
    assert entry.run() == entry.EXIT_ERROR


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_the_bridge_shuts_down_cleanly_on_a_signal(sig):
    async def scenario():
        async with FakeCloud(accepts()) as cloud:
            client = BridgeClient(
                BridgeConfig(token="frt_abc", cloud_url=cloud.url),
                handshake_timeout=2.0,
            )
            task = asyncio.ensure_future(entry._serve(client))
            # Wait for the handshake, so the signal hits a connection that is
            # actually up.
            while client.robot_id is None and not task.done():
                await asyncio.sleep(0.01)
            os.kill(os.getpid(), sig)
            return await asyncio.wait_for(task, timeout=5)

    assert asyncio.run(scenario()) is StopReason.SHUTDOWN


class _CapturingHandler(logging.Handler):
    """Collects everything anyone logs, at any level."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(self.format(record))


def test_the_token_stays_out_of_the_logs_even_with_debug_turned_on():
    token = "frt_deadbeefdeadbeefdeadbeefdeadbeef"

    async def scenario():
        async with FakeCloud(accepts()) as cloud:
            client = BridgeClient(
                BridgeConfig(token=token, cloud_url=cloud.url),
                handshake_timeout=2.0,
            )
            task = asyncio.ensure_future(client.run())
            while client.robot_id is None and not task.done():
                await asyncio.sleep(0.01)
            client.stop()
            await asyncio.wait_for(task, timeout=5)

    capture = _CapturingHandler()
    root = logging.getLogger()
    previous_level = root.level
    # Attached to the logger, not the root: pytest disables propagation for
    # every logger in this image, so root never sees records here
    # (production does).
    watched = logging.getLogger("fleetless_bridge.client")
    watched.addHandler(capture)
    root.setLevel(logging.DEBUG)
    try:
        entry._configure_logging()
        asyncio.run(scenario())
    finally:
        watched.removeHandler(capture)
        root.setLevel(previous_level)

    assert capture.messages, "nothing was logged at all, so this proves nothing"
    assert any("Connected as robot" in line for line in capture.messages)
    assert not [line for line in capture.messages if token in line]


def test_debug_logging_cannot_make_the_transport_dump_the_hello_frame():
    root = logging.getLogger()
    previous_level = root.level
    # An operator chasing a connection problem turns on debug logging.
    root.setLevel(logging.DEBUG)
    try:
        entry._configure_logging()
        # The hello frame carries the token, so no transport library may be
        # left free to log what it sends. aiohttp does not do so today; the
        # library it replaced did, with the token inside the elided middle of
        # a truncated payload, one field reorder away from being written out
        # in full. The level check covers the whole hierarchy, whichever child
        # logger the library ends up using (they are created lazily, so they
        # cannot be enumerated up front) and whichever version the robot's
        # distribution ships.
        for name in ("aiohttp", "aiohttp.client", "aiohttp.websocket"):
            assert not logging.getLogger(name).isEnabledFor(logging.DEBUG), name
        # The bridge's own logging must survive that muting.
        assert logging.getLogger("fleetless_bridge.client").isEnabledFor(logging.INFO)
    finally:
        root.setLevel(previous_level)


def _returning(reason):
    async def serve(_client):
        return reason

    return serve


def test_shadowed_numpy_warning_names_a_numpy_in_user_site(monkeypatch):
    # Not `import numpy`, not a real pip install: the function itself must
    # never trigger the crash it warns about (verified live — a real numpy
    # 2.x in a real user site-packages made `import cv2` itself raise
    # `AttributeError: _ARRAY_API not found`, before this file's own
    # `from fleetless_bridge.ros_runtime import RosRuntime` line could ever
    # run). So this pins the detection (a path-prefix check against
    # `site.getusersitepackages()`) with a fake `importlib.util.find_spec`
    # result instead.
    user_site = "/root/.local/lib/python3.10/site-packages"
    numpy_origin = user_site + "/numpy/__init__.py"
    monkeypatch.setattr(entry.site, "getusersitepackages", lambda: user_site)
    monkeypatch.setattr(
        entry.importlib.util, "find_spec", lambda name: types.SimpleNamespace(origin=numpy_origin)
    )
    message = entry.shadowed_numpy_warning()
    assert message is not None
    assert numpy_origin in message
    # Not a second spelling of the line above (`numpy_origin` contains
    # "numpy" by construction, so that assertion alone could not fail): the
    # message must also name the one remedy an operator can act on without
    # reinstalling anything.
    assert "PYTHONNOUSERSITE=1" in message


def test_shadowed_numpy_warning_is_none_for_a_system_numpy(monkeypatch):
    monkeypatch.setattr(
        entry.site, "getusersitepackages", lambda: "/root/.local/lib/python3.10/site-packages"
    )
    monkeypatch.setattr(
        entry.importlib.util,
        "find_spec",
        lambda name: types.SimpleNamespace(origin="/usr/local/lib/python3.10/dist-packages/numpy/__init__.py"),
    )
    assert entry.shadowed_numpy_warning() is None


def test_shadowed_numpy_warning_is_none_when_numpy_is_not_installed(monkeypatch):
    monkeypatch.setattr(entry.importlib.util, "find_spec", lambda name: None)
    assert entry.shadowed_numpy_warning() is None
