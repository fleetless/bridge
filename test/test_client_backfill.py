# SPDX-License-Identifier: Apache-2.0
"""One half of the datapoint pump: live-first, rate-limited backfill —
against the fake cloud, sharing `FakeRos`/`_run_one_exchange` with
test_client_datapoints.py. Subscription and buffering are ros_runtime.py's
own suite; this file only covers what the outgoing pump sends, and when."""
import asyncio

import pytest
from fake_cloud import FakeCloud, sequence
from helpers import make_client, run_until
from test_client_datapoints import FakeRos, _run_one_exchange

from fleetless_bridge.ros_runtime import _backlog_depth
from fleetless_bridge.sampling import Sample


async def _after_the_config(session):
    """The session's first config, applied: no backfill leaves before it."""
    await session.send_config(1, {})
    await session.recv_config_applied()


def test_hello_ok_marks_the_runtime_connected_and_the_session_ending_marks_it_not():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.ping(1)
        return await session.recv_pong()

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.connected_calls == [True, False]


def test_a_live_sample_is_sent_before_a_pending_backfill_item():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await _after_the_config(session)
        loop = asyncio.get_event_loop()
        # Backfill queued first, live sample second — live must still
        # reach the wire first.
        fake_ros.backlog.configure("old", True, 10)
        fake_ros.backlog.push("old", Sample(slug="old", value=1, timestamp_ms=1))
        fake_ros.samples.put_threadsafe(loop, Sample(slug="new", value=2, timestamp_ms=2))
        first = await session.recv_datapoint()
        second = await session.recv_datapoint()
        return first, second

    first, second = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert first["slug"] == "new"
    assert second["slug"] == "old"


def test_backfill_items_deliver_in_order():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await _after_the_config(session)
        fake_ros.backlog.configure("old", True, 10)
        for i in range(3):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        return [await session.recv_datapoint() for _ in range(3)]

    received = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert [r["value"] for r in received] == [0, 1, 2]


def test_with_no_live_traffic_at_all_backfill_still_drains_eventually():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await _after_the_config(session)
        fake_ros.backlog.configure("old", True, 5)
        fake_ros.backlog.push("old", Sample(slug="old", value=1, timestamp_ms=1))
        return await session.recv_datapoint()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {
        "type": "datapoint", "slug": "old", "value": 1, "timestamp_ms": 1,
        "backfill": True,
    }


def test_a_backfill_datapoint_says_so_and_a_live_one_does_not():
    """The cloud measures its datapoint lag from what arrives live, so a
    replay of an hour-old buffer must not read as an hour of lag. The flag
    is the only thing that tells the two apart on the wire — the timestamp
    cannot, because a genuinely late live sample looks identical."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await _after_the_config(session)
        loop = asyncio.get_event_loop()
        fake_ros.backlog.configure("old", True, 10)
        fake_ros.backlog.push("old", Sample(slug="old", value=1, timestamp_ms=1))
        fake_ros.samples.put_threadsafe(loop, Sample(slug="new", value=2, timestamp_ms=2))
        return await session.recv_datapoint(), await session.recv_datapoint()

    live, replayed = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert live["slug"] == "new" and "backfill" not in live
    assert replayed["slug"] == "old" and replayed["backfill"] is True


# --- the session's first config comes before any backfill (#1262) -----------

_BATTERY = {"topic": "/battery", "type": "sensor_msgs/msg/BatteryState", "field": "percentage"}


def _battery(enabled):
    return {
        "battery": dict(_BATTERY, retention={"enabled": enabled, "max_buffer_values": 10}),
    }


class _RetentionFakeRos(FakeRos):
    """`FakeRos` whose datapoint apply configures the backlog the way
    `RosRuntime`'s does: `_create` and `_update_sampler` both end in
    `backlog.configure`, so a switch-off empties the buffer here as it does
    on a robot."""

    async def apply_config(self, datapoints):
        errors = await super().apply_config(datapoints)
        for slug, dp in datapoints.items():
            self.backlog.configure(slug, dp.retention.enabled, _backlog_depth(dp.retention))
        return errors


async def _frames_until(session, frame_type):
    """Every frame up to and including the first of `frame_type`."""
    frames = []
    while True:
        frame = await session.recv()
        frames.append(frame)
        if frame["type"] == frame_type:
            return frames


def _reconnect(fake_ros, second):
    """Session one applies `battery` with retention on, and the cloud ends it.
    Three values are buffered while the bridge is offline, before session
    two's `hello_ok`. `second` serves session two from there; the client
    stops once it returns."""
    done = {}

    async def first(session):
        await session.recv_hello()
        await session.accept()
        await session.send_config(1, _battery(True))
        await session.recv_config_applied()
        await session.close(1000)

    async def reconnected(session):
        await session.recv_hello()
        for i in range(3):
            fake_ros.backlog.push("battery", Sample(slug="battery", value=i, timestamp_ms=i))
        await session.accept()
        await second(session)
        done["done"] = True
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(first, reconnected)) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "done" in done)

    asyncio.run(scenario())


def test_a_switch_off_published_while_offline_backfills_nothing_on_reconnect():
    """The cloud sends its config right after `hello_ok`, and the apply is what
    empties a switched-off buffer. A drain opened at `hello_ok` sent all of it
    before the apply ran: the review saw 23 of 23 values leave within 11 ms."""
    fake_ros = _RetentionFakeRos()
    box = {}

    async def second(session):
        await session.send_config(2, _battery(False))
        box["until_applied"] = await _frames_until(session, "config_applied")
        loop = asyncio.get_event_loop()
        fake_ros.samples.put_threadsafe(loop, Sample(slug="live", value=9, timestamp_ms=9))
        box["next"] = await session.recv_datapoint()

    _reconnect(fake_ros, second)
    assert [f for f in box["until_applied"] if f["type"] == "datapoint"] == []
    assert box["until_applied"][-1]["version"] == 2
    assert box["next"]["slug"] == "live" and "backfill" not in box["next"]
    assert not fake_ros.backlog.has_pending()


def test_with_retention_still_on_the_backfill_follows_the_config_apply():
    """The control: the same reconnect with `retention.enabled` kept on sends
    the buffer, and only after `config_applied`."""
    fake_ros = _RetentionFakeRos()
    box = {}

    async def second(session):
        await session.send_config(2, _battery(True))
        box["until_applied"] = await _frames_until(session, "config_applied")
        box["replayed"] = [await session.recv_datapoint() for _ in range(3)]

    _reconnect(fake_ros, second)
    assert [f for f in box["until_applied"] if f["type"] == "datapoint"] == []
    assert [(f["slug"], f["value"], f["backfill"]) for f in box["replayed"]] == [
        ("battery", 0, True), ("battery", 1, True), ("battery", 2, True),
    ]


@pytest.mark.parametrize(
    "ahead_of_the_ping",
    [None, '{"type": "config", "version": 3, "doc": {"fleetless": 2}}'],
    ids=["no config", "a config the parser refuses"],
)
def test_with_no_config_before_the_first_ping_the_backfill_follows_the_ping(ahead_of_the_ping):
    """An older cloud, or a config this bridge cannot read, applies nothing,
    and the tier must not stay closed for good. The cloud pings only after it
    has sent its config, so a ping with no config ahead of it opens the tier,
    and the buffer goes out under the config already applied."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        fake_ros.backlog.configure("old", True, 10)
        for i in range(3):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        if ahead_of_the_ping is not None:
            await session.send_raw(ahead_of_the_ping)
        fake_ros.samples.put_threadsafe(loop, Sample(slug="live", value=9, timestamp_ms=9))
        live = await session.recv_datapoint()
        await session.ping(1)
        until_pong = await _frames_until(session, "pong")
        replayed = [await session.recv_datapoint() for _ in range(3)]
        return live, until_pong, replayed

    live, until_pong, replayed = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert live["slug"] == "live" and "backfill" not in live
    assert [f["type"] for f in until_pong] == ["pong"]
    assert [(f["value"], f["backfill"]) for f in replayed] == [(0, True), (1, True), (2, True)]
    assert fake_ros.applied_calls == []
