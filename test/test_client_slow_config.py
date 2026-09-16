# SPDX-License-Identifier: Apache-2.0
"""A config that is slow to apply must not cost the connection.

On a real robot, one camera on a topic carrying 9.44 MB frames at 2 Hz made
the config apply take longer than the cloud's pong deadline. The apply ran
inline in the receive loop, so the `ping` that arrived meanwhile was never
read, the cloud closed the socket after three missed pongs, and the
reconnect was answered with the same config — a loop the robot could not
leave.

The rule these tests pin: the receive loop stays free to answer pings while
a config is applying, and configs are still applied in the order they
arrived.
"""
import asyncio

from fake_cloud import ROBOT_ID, FakeCloud
from helpers import make_client, run_until
from test_client_datapoints import FakeRos

CAMERA = {
    "source": {"kind": "ros", "topic": "/cam/image_color", "type": "sensor_msgs/msg/Image"},
    "fps": 2,
    "width": 1280,
    "height": 720,
    "bitrate_kbps": 2000,
    "snapshot_interval_seconds": 5,
}


class BlockingRos(FakeRos):
    """`apply_cameras` stops until the test lets it go — standing in for a
    real robot's camera apply, which is where those twelve seconds went."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def apply_cameras(self, cameras):
        # Only the first apply blocks — a later one must complete the
        # instant it's released, or overtaking its predecessor is a state
        # this test can never reach, and an apply that always blocks
        # cannot tell an ordered worker from an unordered one.
        self.calls += 1
        if self.calls == 1:
            self.entered.set()
            await self.release.wait()
        return await super().apply_cameras(cameras)


def test_a_ping_is_answered_while_a_slow_config_is_still_applying():
    async def scenario():
        ros = BlockingRos()

        async def behaviour(session):
            await session.recv_hello()
            await session.accept(ROBOT_ID)
            await session.send_config(1, {}, cameras={"front": CAMERA})
            # Only ping once the apply is provably in flight, so a pong
            # cannot be explained by the config having already finished.
            await ros.entered.wait()
            await session.ping(7)
            await session.recv_pong()
            ros.release.set()
            await session.recv_config_applied()
            await session.drain()

        async with FakeCloud(behaviour) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: cloud.pongs)
            return cloud.pongs

    assert [pong["ts_ms"] for pong in asyncio.run(scenario())] == [7]


def test_configs_are_applied_in_the_order_they_arrived_even_when_slow():
    async def scenario():
        ros = BlockingRos()
        applied = []

        async def behaviour(session):
            await session.recv_hello()
            await session.accept(ROBOT_ID)
            await session.send_config(1, {}, cameras={"front": CAMERA})
            await ros.entered.wait()
            # Sent while version 1 is still applying: it must queue behind
            # it, not overtake it.
            await session.send_config(2, {}, cameras={"front": CAMERA})
            # Version 1 is still held — give version 2 every chance to
            # overtake before releasing it. Without this the test passes
            # whether or not anything preserves the order.
            for _ in range(50):
                await asyncio.sleep(0.005)
            ros.release.set()
            applied.append((await session.recv_config_applied())["version"])
            applied.append((await session.recv_config_applied())["version"])
            await session.drain()

        async with FakeCloud(behaviour) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: len(applied) == 2)
            return applied

    assert asyncio.run(scenario()) == [1, 2]
