# SPDX-License-Identifier: Apache-2.0
"""Coming back after a disconnect: how long the bridge waits, when it starts
over, and when it stays down on purpose."""
import asyncio

import pytest
from fake_cloud import (
    FakeCloud,
    accepts,
    accepts_then_closes,
    closes_after_hello,
    rejects,
    sequence,
    silent,
)
from fleetless_bridge.client import BridgeClient, ExponentialBackoff, StopReason
from fleetless_bridge.config import BridgeConfig
from fleetless_bridge.protocol import (
    CLOSE_CODE_ROBOT_DELETED,
    CLOSE_CODE_SUPERSEDED,
    CLOSE_CODE_TOKEN_ROTATED,
)
from helpers import RecordingSleep, make_client, run_until


def test_the_delay_doubles_up_to_the_cap():
    # random_func pinned to its ceiling: multiplier (0.5 + 0.5*r) is exactly
    # 1.0, reproducing the unjittered schedule.
    backoff = ExponentialBackoff(random_func=lambda: 1.0)
    delays = [backoff.next_delay() for _ in range(8)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0]


def test_a_successful_handshake_starts_the_delay_over():
    backoff = ExponentialBackoff(random_func=lambda: 1.0)
    backoff.next_delay()
    backoff.next_delay()
    backoff.reset()
    assert backoff.next_delay() == 1.0


def test_the_jitter_never_goes_below_half_the_scheduled_delay():
    # random_func pinned to its floor: multiplier is exactly 0.5, the worst
    # case "equal jitter" promises — a backing-off fleet never retries
    # faster than half its nominal schedule.
    backoff = ExponentialBackoff(random_func=lambda: 0.0)
    delays = [backoff.next_delay() for _ in range(4)]
    assert delays == [0.5, 1.0, 2.0, 4.0]


def test_two_fleet_members_losing_the_cloud_together_do_not_retry_together():
    # Two robots dropped in the same instant, each with its own default
    # (real) random source, must not compute the same reconnect schedule.
    a = ExponentialBackoff()
    b = ExponentialBackoff()
    delays_a = [a.next_delay() for _ in range(5)]
    delays_b = [b.next_delay() for _ in range(5)]
    assert delays_a != delays_b


def test_a_dropped_connection_is_retried_with_a_fresh_hello():
    async def scenario():
        behavior = sequence(accepts_then_closes(1000), accepts())
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud)
            await run_until(client, lambda: len(cloud.hellos) >= 2)
            return cloud.hellos

    hellos = asyncio.run(scenario())
    # Every attempt introduces itself again; the cloud never has to remember a
    # half-finished handshake.
    assert len(hellos) >= 2
    assert all(hello["type"] == "hello" for hello in hellos)


def test_repeated_failures_back_off_further_each_time():
    async def scenario():
        sleep = RecordingSleep()
        # Never gets past the handshake, so the backoff never resets. Jitter
        # pinned to its ceiling (see ExponentialBackoff) so the schedule is
        # exact; the jitter itself is covered separately.
        behavior = rejects("server_busy", "not now")
        async with FakeCloud(behavior) as cloud:
            client = make_client(
                cloud, sleep=sleep, backoff=ExponentialBackoff(random_func=lambda: 1.0)
            )
            await run_until(client, lambda: len(sleep.delays) >= 4)
            return sleep.delays[:4]

    assert asyncio.run(scenario()) == [1.0, 2.0, 4.0, 8.0]


def test_a_connection_that_worked_resets_the_backoff():
    async def scenario():
        sleep = RecordingSleep()
        # Each attempt completes the handshake then is dropped, so every
        # retry restarts at one second instead of creeping toward 30. Jitter
        # pinned to its ceiling, as above.
        async with FakeCloud(accepts_then_closes(1000)) as cloud:
            client = make_client(
                cloud, sleep=sleep, backoff=ExponentialBackoff(random_func=lambda: 1.0)
            )
            await run_until(client, lambda: len(sleep.delays) >= 3)
            return sleep.delays[:3]

    assert asyncio.run(scenario()) == [1.0, 1.0, 1.0]


def test_a_handshake_that_never_completes_does_not_reset_the_backoff():
    async def scenario():
        sleep = RecordingSleep()
        async with FakeCloud(closes_after_hello(1000)) as cloud:
            client = make_client(
                cloud, sleep=sleep, backoff=ExponentialBackoff(random_func=lambda: 1.0)
            )
            await run_until(client, lambda: len(sleep.delays) >= 3)
            return sleep.delays[:3]

    assert asyncio.run(scenario()) == [1.0, 2.0, 4.0]


def test_the_supersede_close_code_is_the_one_the_cloud_sends():
    # Frozen with the cloud: 4000 supersede, 4002 hello timeout, 4003
    # pong timeout. Pinned to the literal here, because a constant that only
    # agrees with itself would let the bridge miss a real supersede.
    assert CLOSE_CODE_SUPERSEDED == 4000


def test_being_superseded_stops_the_bridge():
    async def scenario():
        async with FakeCloud(accepts_then_closes(4000)) as cloud:
            client = make_client(cloud)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    # Another bridge now owns this robot. Reconnecting would kick that one off
    # in turn, and the two would trade the robot back and forth forever.
    assert reason is StopReason.REJECTED
    assert connections == 1


def test_the_robot_deleted_close_code_is_the_one_the_cloud_sends():
    # Frozen with the cloud: 4004, distinct from every other close code —
    # see protocol.py's docstring. Pinned to the literal for the same reason
    # as CLOSE_CODE_SUPERSEDED: agreeing only with itself would miss a real
    # deletion.
    assert CLOSE_CODE_ROBOT_DELETED == 4004


def test_a_deleted_robot_stops_the_bridge_and_does_not_reconnect():
    async def scenario():
        async with FakeCloud(accepts_then_closes(4004)) as cloud:
            client = make_client(cloud)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    # A token valid a second ago looks identical to a revoked one unless the
    # cloud says which — without this, a deleted robot reconnects forever
    # against one that never comes back.
    assert reason is StopReason.REJECTED
    assert connections == 1


def test_the_token_rotated_close_code_is_the_one_the_cloud_sends():
    # Frozen with the cloud: 4005, and its own code rather than 4004 —
    # a rotated token and a deleted robot need different sentences on the
    # robot's own logs, because only one of them has a fix the operator
    # can carry out.
    assert CLOSE_CODE_TOKEN_ROTATED == 4005


def test_a_rotated_token_stops_the_bridge_and_does_not_reconnect():
    """The console handed out a new token and closed this socket. The one
    in this process will be refused from now on, so reconnecting is a loop
    with no exit — the fix is a restart with the new token, which is a
    person's job, and exit 2 is how the launch file is told not to
    respawn into the same wall."""

    async def scenario():
        async with FakeCloud(accepts_then_closes(4005)) as cloud:
            client = make_client(cloud)
            reason = await asyncio.wait_for(client.run(), timeout=10)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.REJECTED
    assert connections == 1


@pytest.mark.parametrize("code", [4002, 4003])
def test_the_clouds_other_close_codes_are_retried(code):
    # 4002 (hello timeout) and 4003 (offline kick) are transient by nature:
    # only 4000, supersede, means another bridge has taken the robot over.
    async def scenario():
        async with FakeCloud(sequence(accepts_then_closes(code), accepts())) as cloud:
            client = make_client(cloud)
            reason = await run_until(client, lambda: cloud.connections >= 2)
            return reason, cloud.connections

    reason, connections = asyncio.run(scenario())
    assert reason is StopReason.SHUTDOWN
    assert connections >= 2


def test_a_connection_that_goes_quiet_is_replaced():
    async def scenario():
        # Half-open socket: the handshake succeeded, so the bridge believes it
        # is online, and then nothing ever arrives again. Only the idle timeout
        # notices — the socket itself still looks perfectly healthy.
        async with FakeCloud(accepts()) as cloud:
            client = make_client(cloud, idle_timeout=0.3)
            await run_until(client, lambda: cloud.connections >= 2, timeout=15)
            return cloud.connections

    assert asyncio.run(scenario()) >= 2


def test_stopping_while_waiting_to_reconnect_does_not_hang():
    async def scenario():
        async with FakeCloud(rejects("server_busy")) as cloud:
            # A real sleep, so the client is genuinely parked in the backoff
            # wait when the stop arrives.
            client = make_client(cloud, sleep=asyncio.sleep)
            task = asyncio.ensure_future(client.run())
            await asyncio.sleep(0.2)
            client.stop()
            return await asyncio.wait_for(task, timeout=3)

    assert asyncio.run(scenario()) is StopReason.SHUTDOWN


def test_stopping_while_a_connect_attempt_hangs_does_not_wait_it_out():
    async def scenario():
        never_finishes = asyncio.Event()

        async def connect_that_hangs(_url):
            await never_finishes.wait()

        # A cloud that accepts the TCP connection then stalls can hold a
        # connect attempt for the whole handshake timeout. Shutdown must not
        # wait for it, or `docker stop`'s grace period expires and SIGKILLs
        # us before the socket closes.
        client = BridgeClient(
            BridgeConfig(token="frt_test_token", cloud_url="ws://127.0.0.1:1/bridge"),
            connect=connect_that_hangs,
            handshake_timeout=30.0,
        )
        task = asyncio.ensure_future(client.run())
        await asyncio.sleep(0.05)
        client.stop()
        return await asyncio.wait_for(task, timeout=2)

    assert asyncio.run(scenario()) is StopReason.SHUTDOWN


def test_a_connection_that_opens_as_we_stop_is_not_left_open():
    async def scenario():
        class OpenedSocket:
            def __init__(self):
                self.closed = False

            async def close(self):
                self.closed = True

            async def recv(self):
                await asyncio.Event().wait()

        opened = OpenedSocket()
        holder = {}

        async def connect_and_stop(_url):
            # Stop lands while the connection is being established: the
            # attempt succeeds and is abandoned in the same breath — the
            # moment a socket gets dropped on the floor still open.
            holder["client"].stop()
            await asyncio.sleep(0)
            return opened

        client = BridgeClient(
            BridgeConfig(token="frt_test_token", cloud_url="ws://127.0.0.1:1/bridge"),
            connect=connect_and_stop,
            handshake_timeout=30.0,
        )
        holder["client"] = client
        await asyncio.wait_for(client.run(), timeout=2)
        return opened.closed

    assert asyncio.run(scenario()) is True


def test_stopping_while_connected_closes_the_connection():
    async def scenario():
        async with FakeCloud(accepts()) as cloud:
            client = make_client(cloud)
            reason = await run_until(client, lambda: client.robot_id is not None)
            return reason

    assert asyncio.run(scenario()) is StopReason.SHUTDOWN


def test_a_cloud_that_is_not_listening_is_retried():
    async def scenario():
        sleep = RecordingSleep()
        async with FakeCloud(silent()) as cloud:
            url = cloud.url
        # The server is gone now: connect fails outright, which must back off
        # like any other failure rather than crash the bridge. Jitter pinned
        # to its ceiling, as above.
        client = BridgeClient(
            BridgeConfig(token="frt_test_token", cloud_url=url),
            sleep=sleep,
            backoff=ExponentialBackoff(random_func=lambda: 1.0),
            handshake_timeout=1.0,
        )
        task = asyncio.ensure_future(client.run())
        while len(sleep.delays) < 2:
            await asyncio.sleep(0.01)
        client.stop()
        await asyncio.wait_for(task, timeout=5)
        return sleep.delays[:2]

    assert asyncio.run(scenario()) == [1.0, 2.0]
