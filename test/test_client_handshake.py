# SPDX-License-Identifier: Apache-2.0
"""The handshake: how the bridge introduces itself and what it does when the
cloud says no."""
import asyncio

import fleetless_bridge.client as client_module
from fake_cloud import (
    OTHER_ROBOT_ID,
    FakeCloud,
    accepts,
    rejects,
    rejects_and_closes,
    sequence,
    silent,
)
from fleetless_bridge import __version__
from fleetless_bridge.client import StopReason
from fleetless_bridge.protocol import PROTOCOL_VERSION
from helpers import RecordingSleep, make_client, run_until


class _RecordingLog:
    """Every level, formatted, in order. `caplog` passes unconditionally in
    this suite (see the suite README), so the deprecation tests below read a
    double instead of trusting a handler to be there."""

    def __init__(self) -> None:
        self.messages = []

    def _record(self, fmt, args):
        self.messages.append(fmt % args if args else str(fmt))

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


async def _close_now(session):
    """A `then` for `accepts`: hang up right after the handshake, so the
    client reconnects and meets the window a second time."""
    await session.close()


def test_the_bridge_introduces_itself_with_its_token_and_version():
    async def scenario():
        async with FakeCloud(accepts()) as cloud:
            client = make_client(cloud, token="frt_abc123")
            await run_until(client, lambda: cloud.hellos)
            return cloud.hellos[0]

    hello = asyncio.run(scenario())
    assert hello["token"] == "frt_abc123"
    assert hello["protocol_version"] == PROTOCOL_VERSION
    assert hello["bridge_version"] == __version__


def test_hello_ok_binds_the_connection_to_a_robot():
    async def scenario():
        async with FakeCloud(accepts(robot_id=OTHER_ROBOT_ID)) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: client.robot_id is not None)
            return client.robot_id

    assert asyncio.run(scenario()) == OTHER_ROBOT_ID


def test_a_rejected_token_stops_the_bridge_instead_of_hammering_the_cloud():
    async def scenario():
        async with FakeCloud(rejects("invalid_token", "unknown token")) as cloud:
            client = make_client(cloud)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.REJECTED
    # Exactly one attempt — proven by the server's own connection count, not
    # a mock.
    assert connections == 1


def test_a_protocol_mismatch_waits_long_and_then_exits_so_a_respawn_can_upgrade():
    """A refused version is about the package on disk, so the fix arrives as
    an apt upgrade — which this process would never load. It waits out a long
    backoff so it is not hammering the cloud, then exits, and the launch
    file's respawn starts a fresh process on whatever is installed by then."""

    async def scenario():
        async with FakeCloud(rejects("protocol_mismatch", "bridge too old")) as cloud:
            sleep = RecordingSleep()
            client = make_client(cloud, sleep=sleep)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, sleep.delays, cloud.connections

    reason, delays, connections = asyncio.run(scenario())
    assert reason is StopReason.REJECTED
    assert connections == 1, "no second attempt inside this process"
    assert delays and delays[-1] >= 15, delays  # 30 s scheduled, equal jitter floors at half


def test_a_deprecated_protocol_is_warned_about_once_per_process(monkeypatch):
    """Once, not once per session. The sunset date does not move between two
    reconnects, and a bridge that drops every few minutes would otherwise
    fill the journal with the same sentence."""
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    def deprecated(then=None):
        return accepts(
            then=then,
            protocol={"status": "deprecated", "sunset_at": "2026-12-20"},
            bridge={"latest_version": "3.9.0"},
        )

    async def scenario():
        async with FakeCloud(sequence(deprecated(then=_close_now), deprecated())) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: cloud.connections == 2)

    asyncio.run(scenario())
    warned = [m for m in recording_log.messages if "2026-12-20" in m]
    assert len(warned) == 1, recording_log.messages
    assert "3.9.0" in warned[0], warned[0]


def test_a_current_protocol_says_nothing_about_a_sunset(monkeypatch):
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        async with FakeCloud(
            accepts(protocol={"status": "current", "sunset_at": None})
        ) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: client.robot_id is not None)

    asyncio.run(scenario())
    assert not [m for m in recording_log.messages if "sunset" in m or "stops serving" in m], (
        recording_log.messages
    )


def test_a_rejection_the_cloud_hangs_up_on_is_still_read_first():
    async def scenario():
        # Real cloud: hello_error, then close 1008 right after. Notice the
        # close first and a bad token reads as an ordinary disconnect —
        # retried forever.
        async with FakeCloud(rejects_and_closes("invalid_token")) as cloud:
            client = make_client(cloud)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.REJECTED
    assert connections == 1


def test_an_unfamiliar_rejection_is_retried():
    async def scenario():
        behavior = sequence(rejects("server_busy", "come back later"), accepts())
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud)
            reason = await run_until(client, lambda: client.robot_id is not None)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    assert connections >= 2


def test_a_cloud_that_never_answers_the_hello_is_abandoned_and_retried():
    async def scenario():
        # The first connection is accepted at the socket level and then goes
        # quiet — the failure mode a plain connect() check cannot see.
        behavior = sequence(silent(), accepts())
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, handshake_timeout=0.3)
            reason = await run_until(client, lambda: client.robot_id is not None)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    assert connections >= 2


def test_a_hello_that_cannot_even_be_sent_is_retried():
    async def scenario():
        # The cloud drops the connection during the handshake, before reading.
        async def hangs_up_immediately(session):
            await session.close(1001, "going away")

        behavior = sequence(hangs_up_immediately, accepts())
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: client.robot_id is not None)
            return cloud.connections

    assert asyncio.run(scenario()) >= 2
