# SPDX-License-Identifier: Apache-2.0
"""The handshake: how the bridge introduces itself and what it does when the
cloud says no."""
import asyncio

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
from helpers import make_client, run_until


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


def test_a_protocol_mismatch_stops_the_bridge():
    async def scenario():
        async with FakeCloud(rejects("protocol_mismatch", "bridge too old")) as cloud:
            client = make_client(cloud)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.REJECTED
    assert connections == 1


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
