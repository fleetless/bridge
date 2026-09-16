# SPDX-License-Identifier: Apache-2.0
"""One half of the datapoint pump: live-first, rate-limited backfill —
against the fake cloud, sharing `FakeRos`/`_run_one_exchange` with
test_client_datapoints.py. Subscription and buffering are ros_runtime.py's
own suite; this file only covers what the outgoing pump sends, and when."""
import asyncio

from test_client_datapoints import FakeRos, _run_one_exchange

from fleetless_bridge.sampling import Sample


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
        fake_ros.backlog.configure("old", True, 10)
        for i in range(3):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        return [await session.recv_datapoint() for _ in range(3)]

    received = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert [r["value"] for r in received] == [0, 1, 2]


def test_with_no_live_traffic_at_all_backfill_still_drains_eventually():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.backlog.configure("old", True, 5)
        fake_ros.backlog.push("old", Sample(slug="old", value=1, timestamp_ms=1))
        return await session.recv_datapoint()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {"type": "datapoint", "slug": "old", "value": 1, "timestamp_ms": 1}
