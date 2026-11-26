# SPDX-License-Identifier: Apache-2.0
"""The `failsafe_watchdog` frame on the wire: the runtime's state once after
every `hello_ok`, and again on every change while a session is open."""
import asyncio

from fake_cloud import FakeCloud, sequence
from helpers import make_client, run_until
from test_client_datapoints import FakeRos

from fleetless_bridge.protocol import failsafe_watchdog_message


def test_after_hello_the_bridge_states_its_failsafe_watchdog_once():
    ros = FakeRos()
    ros.failsafe_state_value = ("ok", None, 0.42)

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            box["first"] = await session.recv_failsafe_watchdog()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: "first" in box)
            await asyncio.sleep(0.1)
            box["all"] = list(cloud.failsafe_watchdogs)
        return box

    box = asyncio.run(scenario())
    first = box["first"]
    assert first["type"] == "failsafe_watchdog"
    assert first["state"] == "ok" and first["reason"] is None
    assert first["lateness_ms"] == 0.42
    assert isinstance(first["at_ms"], int)
    assert len(box["all"]) == 1


def test_a_change_of_the_watchdog_is_sent_while_a_session_is_open():
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_failsafe_watchdog()  # the state at hello_ok
            ros.failsafe_callback("failed", "late_fire", 23.5)
            box["failed"] = await session.recv_failsafe_watchdog()
            ros.failsafe_callback("ok", None, 0.3)
            box["ok"] = await session.recv_failsafe_watchdog()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: "ok" in box)
        return box

    box = asyncio.run(scenario())
    assert (box["failed"]["state"], box["failed"]["reason"], box["failed"]["lateness_ms"]) == (
        "failed", "late_fire", 23.5)
    assert box["ok"]["state"] == "ok" and box["ok"]["reason"] is None


def test_a_watchdog_still_starting_says_nothing():
    ros = FakeRos()
    ros.failsafe_state_value = ("starting", None, None)

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.ping(1, latency_ms=5, lag_ms=0)
            await session.recv_pong()
            box["done"] = True
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: "done" in box)
            box["all"] = list(cloud.failsafe_watchdogs)
        return box

    assert asyncio.run(scenario())["all"] == []


def test_a_change_while_no_session_is_open_is_sent_by_the_next_hello_ok_only():
    """Nothing goes out while the socket is closed: the next `hello_ok`
    restates the state as it is then, once."""
    ros = FakeRos()
    ros.failsafe_state_value = ("ok", None, 0.4)

    async def scenario():
        box = {}

        async def first(session):
            await session.recv_hello()
            await session.accept()
            box["first"] = await session.recv_failsafe_watchdog()
            await session.close()

        async def second(session):
            await session.recv_hello()
            # Connected again, but no session until `hello_ok`: the change
            # here goes nowhere, and `hello_ok` restates it.
            ros.failsafe_state_value = ("failed", "late_fire", 23.5)
            ros.failsafe_callback("failed", "late_fire", 23.5)
            await session.accept()
            box["second"] = await session.recv_failsafe_watchdog()
            await asyncio.sleep(0.1)
            box["done"] = True
            await session.drain()

        async with FakeCloud(sequence(first, second)) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: "done" in box)
            box["all"] = [(f["state"], f["reason"]) for f in cloud.failsafe_watchdogs]
        return box

    box = asyncio.run(scenario())
    assert box["first"]["state"] == "ok"
    assert (box["second"]["state"], box["second"]["reason"]) == ("failed", "late_fire")
    assert box["all"] == [("ok", None), ("failed", "late_fire")]


def test_the_frame_carries_no_negative_lateness():
    """An early wake (a self-test answered a hair before its deadline by the
    clock's rounding) is no lateness; the frame says 0."""
    import json

    frame = json.loads(failsafe_watchdog_message("ok", None, -0.0004, 1))
    assert frame["lateness_ms"] == 0.0
