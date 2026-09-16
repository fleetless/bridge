# SPDX-License-Identifier: Apache-2.0
"""The RTT probe: the cloud pings, the bridge echoes, the cloud gets latency."""
import asyncio

from fake_cloud import ROBOT_ID, FakeCloud, accepts, pings
from helpers import make_client, run_until


def test_a_ping_comes_back_as_a_pong_with_the_same_timestamp():
    async def scenario():
        async with FakeCloud(pings(1754800000123)) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: cloud.pongs)
            return cloud.pongs[0]

    # Echoed unchanged: the cloud subtracts this from its own clock. Invented
    # or rounded, and the latency is wrong.
    assert asyncio.run(scenario()) == {"type": "pong", "ts_ms": 1754800000123}


def test_every_ping_is_answered_in_order():
    async def scenario():
        async with FakeCloud(pings(10, 20, 30)) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: len(cloud.pongs) == 3)
            return cloud.pongs

    assert [pong["ts_ms"] for pong in asyncio.run(scenario())] == [10, 20, 30]


def test_a_ping_that_overtakes_the_hello_is_still_answered():
    async def scenario():
        async def pings_before_greeting(session):
            await session.recv_hello()
            await session.ping(5)
            await session.recv_pong()
            await session.accept(ROBOT_ID)
            await session.drain()

        async with FakeCloud(pings_before_greeting) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: cloud.pongs)
            return cloud.pongs[0]["ts_ms"]

    assert asyncio.run(scenario()) == 5


def test_frames_the_bridge_does_not_understand_do_not_break_the_connection():
    async def scenario():
        async def talks_nonsense(session):
            await session.recv_hello()
            await session.accept(ROBOT_ID)
            await session.send_raw("this is not JSON")
            await session.send_raw('{"type":"something_from_2027","payload":{}}')
            await session.send_raw("[1,2,3]")
            # If any of those had killed the client, no pong would follow.
            await session.ping(99)
            await session.recv_pong()
            await session.drain()

        async with FakeCloud(talks_nonsense) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: cloud.pongs)
            return cloud.pongs[0]["ts_ms"], cloud.connections

    ts_ms, connections = asyncio.run(scenario())
    assert ts_ms == 99
    assert connections == 1  # the same connection survived all of it
