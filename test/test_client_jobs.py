# SPDX-License-Identifier: Apache-2.0
"""The half of the client: invoke/cancel dispatch, the job_update pump,
and `hello.active_jobs` — driven against the fake cloud with
test_client_datapoints.py's duck-typed `FakeRos` double (real ROS goal
lifecycle is ros_runtime.py's own suite; this file is wire protocol and
session plumbing only)."""
import asyncio

from fake_cloud import FakeCloud, sequence
from helpers import make_client, run_until
from test_client_datapoints import FakeRos, _run_one_exchange

from fleetless_bridge.jobs import JobManager, JobUpdate
from fleetless_bridge.protocol import (
    APPLY_ERROR_CODE_UNKNOWN,
    APPLY_ERROR_KIND_CAMERA,
    APPLY_ERROR_KIND_PUBLISHER,
    ApplyError,
    JobStatusEntry,
)


def test_an_invoke_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_invoke(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "drive_to", {"speed": 0.5}, patience_ms=5000
        )
        # Nothing comes back synchronously; give the dispatched coroutine a
        # turn, then check what it did.
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.invoke_calls == [
        ("3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "drive_to", {"speed": 0.5}, 5000)
    ]


def test_a_cancel_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_cancel("drive_to")
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.cancel_calls == [("drive_to", None)]


def test_a_cancel_by_job_id_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_cancel("drive_to", "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f")
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.cancel_calls == [("drive_to", "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f")]


def test_a_job_query_is_answered_from_the_ros_runtime():
    fake_ros = FakeRos(
        job_query_result=(
            [JobStatusEntry(job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", state="running")],
            ["4a2f9b3d-7e5c-4f1b-9d9f-2c3b4d5e6f70"],
        )
    )

    async def send_and_recv(session):
        await session.send_job_query(
            "req-1", ["3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "4a2f9b3d-7e5c-4f1b-9d9f-2c3b4d5e6f70"]
        )
        return await session.recv_job_status()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["request_id"] == "req-1"
    assert payload["jobs"][0]["job_id"] == "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
    assert payload["unknown_job_ids"] == ["4a2f9b3d-7e5c-4f1b-9d9f-2c3b4d5e6f70"]
    assert fake_ros.job_query_calls == [
        ["3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "4a2f9b3d-7e5c-4f1b-9d9f-2c3b4d5e6f70"]
    ]


def test_a_job_query_without_a_ros_runtime_answers_every_id_unknown():
    async def send_and_recv(session):
        await session.send_job_query("req-1", ["3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"])
        return await session.recv_job_status()

    payload = _run_one_exchange(send_and_recv, ros=None)
    assert payload["jobs"] == []
    assert payload["unknown_job_ids"] == ["3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"]


def test_a_publish_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_publish("drive", {"linear.x": 0.5, "angular.z": 0.0})
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.publish_calls == [("drive", {"linear.x": 0.5, "angular.z": 0.0})]


def test_invoke_cancel_and_publish_without_a_ros_runtime_do_not_crash_the_session():
    async def send_and_recv(session):
        await session.send_invoke("3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "drive_to", {})
        await session.send_cancel("drive_to")
        await session.send_publish("drive", {})
        # The session must still be alive to answer a plain ping afterwards.
        await session.ping(1)
        return await session.recv_pong()

    payload = _run_one_exchange(send_and_recv, ros=None)
    assert payload == {"type": "pong", "ts_ms": 1}


def test_hello_names_the_ros_runtimes_active_jobs_with_slug_and_state():
    fake_ros = FakeRos()
    fake_ros.jobs._active = [("3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "drive_to", "running")]

    async def scenario():
        hello_box = {}

        async def behavior(session):
            hello_box["hello"] = await session.recv_hello()
            await session.accept()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "hello" in hello_box)
        return hello_box["hello"]

    hello = asyncio.run(scenario())
    assert hello["active_jobs"] == [
        {"job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "slug": "drive_to", "state": "running"}
    ]


def test_hello_names_a_terminal_job_with_its_actual_state():
    # A bridge holding a terminal result reports it as such, not
    # `running` — see jobs.py's own suite for where the state comes
    # from; this is only the wire-framing half.
    fake_ros = FakeRos()
    fake_ros.jobs._active = [("3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "drive_to", "succeeded")]

    async def scenario():
        hello_box = {}

        async def behavior(session):
            hello_box["hello"] = await session.recv_hello()
            await session.accept()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "hello" in hello_box)
        return hello_box["hello"]

    hello = asyncio.run(scenario())
    assert hello["active_jobs"] == [
        {"job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "slug": "drive_to", "state": "succeeded"}
    ]


def test_hello_leaves_an_external_job_out():
    fake_ros = FakeRos()
    fake_ros.jobs = JobManager()
    fake_ros.jobs.start("3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "drive_to", "action")
    fake_ros.jobs.register_external("4a2f9b3d-7e5c-4f1b-9d9f-2c3b4d5e6f70", "drive_to")

    async def scenario():
        hello_box = {}

        async def behavior(session):
            hello_box["hello"] = await session.recv_hello()
            await session.accept()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "hello" in hello_box)
        return hello_box["hello"]

    hello = asyncio.run(scenario())
    assert hello["active_jobs"] == [
        {"job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", "slug": "drive_to", "state": "running"}
    ]


def test_hello_names_no_jobs_when_there_are_none():
    async def scenario():
        hello_box = {}

        async def behavior(session):
            hello_box["hello"] = await session.recv_hello()
            await session.accept()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=None)
            await run_until(client, lambda: "hello" in hello_box)
        return hello_box["hello"]

    hello = asyncio.run(scenario())
    assert hello["active_jobs"] == []


# --- config apply now covers all four kinds -------------------------------------


def test_config_applies_actions_services_and_publishers_too():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {}, actions={}, services={}, publishers={})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {"type": "config_applied", "version": 1, "ok": True, "errors": []}
    assert fake_ros.applied_action_calls == [{}]
    assert fake_ros.applied_service_calls == [{}]
    assert fake_ros.applied_publisher_calls == [{}]


def test_a_publisher_apply_error_is_reported_ok_false():
    fake_ros = FakeRos(apply_publisher_errors=[
        ApplyError(
            slug="drive", kind=APPLY_ERROR_KIND_PUBLISHER,
            code=APPLY_ERROR_CODE_UNKNOWN, message="unbuildable failsafe",
        )
    ])

    async def send_and_recv(session):
        await session.send_config(1, {})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["ok"] is False
    assert payload["errors"] == [
        {"slug": "drive", "kind": "publisher", "code": "unknown", "message": "unbuildable failsafe"}
    ]


def test_config_applies_cameras_too():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {}, cameras={})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {"type": "config_applied", "version": 1, "ok": True, "errors": []}
    assert fake_ros.applied_camera_calls == [{}]


def test_a_camera_apply_error_is_reported_ok_false():
    fake_ros = FakeRos(apply_camera_errors=[
        ApplyError(
            slug="front_cam", kind=APPLY_ERROR_KIND_CAMERA,
            code=APPLY_ERROR_CODE_UNKNOWN, message="unknown type",
        )
    ])

    async def send_and_recv(session):
        await session.send_config(1, {})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["ok"] is False
    assert payload["errors"] == [
        {"slug": "front_cam", "kind": "camera", "code": "unknown", "message": "unknown type"}
    ]


# --- the job_update pump ------------------------------------------------------


def test_job_updates_are_pumped_out_once_connected():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="running",
                timestamp_ms=1754800000123,
                feedback={"distance": 1.5},
                progress=0.3,
            ),
        )
        return await session.recv_job_update()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {
        "type": "job_update",
        "job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
        "slug": "drive_to",
        "state": "running",
        "origin": "fleetless",
        "goal_id": None,
        "feedback": {"distance": 1.5},
        "progress": 0.3,
        "result": None,
        "error": None,
        "timestamp_ms": 1754800000123,
    }


def test_job_updates_and_datapoints_can_be_pumped_concurrently():
    from fleetless_bridge.sampling import Sample

    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        fake_ros.samples.put_threadsafe(loop, Sample(slug="x", value=1, timestamp_ms=1))
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="succeeded",
                timestamp_ms=2,
                result={"ok": True},
            ),
        )
        first = await session.recv()
        second = await session.recv()
        return {first["type"], second["type"]}

    frame_types = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert frame_types == {"datapoint", "job_update"}


# --- disconnect survival: jobs continue, delivered late on reconnect ------
#
# RosRuntime never learns the websocket exists — a goal's callbacks write to
# the unbounded `self.jobs.updates` queue regardless of whether anything is
# draining it. Nothing here is reconnect-specific code; it's what "the queue
# isn't tied to the connection's lifecycle" already gives for free. These
# tests prove that property at the wire level, not just assert it.


def test_a_job_update_queued_during_a_disconnect_is_delivered_on_reconnect():
    fake_ros = FakeRos()
    box = {}

    async def first_connection(session):
        await session.recv_hello()
        await session.accept()
        await session.close(1000)
        # Pushed only once the first connection is provably gone: it cannot
        # have delivered this, so the second connection's delivery is
        # unambiguous.
        loop = asyncio.get_event_loop()
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="succeeded",
                timestamp_ms=1786400000000,
                result={"sequence": [0, 1, 1, 2, 3]},
            ),
        )

    async def second_connection(session):
        await session.recv_hello()
        await session.accept()
        box["update"] = await session.recv_job_update()
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(first_connection, second_connection)) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "update" in box)

    asyncio.run(scenario())
    assert box["update"]["job_id"] == "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
    assert box["update"]["state"] == "succeeded"
    assert box["update"]["result"] == {"sequence": [0, 1, 1, 2, 3]}


def test_a_chatty_backlog_that_finishes_while_disconnected_delivers_only_the_result():
    """2m, deliberately overturning what this test used to assert a second
    time.

    2m's version of this test (see
    `test_non_terminal_updates_for_the_same_job_coalesce_to_the_latest` in
    test_jobs.py) pushed these same five updates and asserted **two**
    arrived — the latest coalesced "running" frame (i=3) and the terminal
    result (i=4). That was already an improvement over an earlier version
    that delivered all five, but it is still one frame more than the
    outcome carries any information for: once the job's real result is
    known, the "running" frame immediately ahead of it says nothing the
    result does not already say better, and a caller watching the wire
    would see the job reported "still running" for one frame, immediately
    contradicted by the frame right behind it. The fix
    (`JobUpdateQueue._push` in jobs.py) makes a job's terminal update
    collapse its own still-pending non-terminal marker, not just future
    non-terminal arrivals — so `i in (0, 1, 2, 3)` never reach the wire and
    only `i == 4`, the terminal result, does."""
    fake_ros = FakeRos()
    box = {"updates": []}

    async def first_connection(session):
        await session.recv_hello()
        await session.accept()
        await session.close(1000)
        loop = asyncio.get_event_loop()
        for i in range(5):
            fake_ros.jobs.updates.put_threadsafe(
                loop,
                JobUpdate(
                    job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                    slug="drive_to",
                    state="running" if i < 4 else "succeeded",
                    timestamp_ms=i,
                    feedback={"step": i} if i < 4 else None,
                    result={"ok": True} if i == 4 else None,
                ),
            )

    async def second_connection(session):
        await session.recv_hello()
        await session.accept()
        box["updates"].append(await session.recv_job_update())
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(first_connection, second_connection)) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: len(box["updates"]) >= 1)

    asyncio.run(scenario())
    # Only the terminal result (i=4) — i=0..3, including the latest
    # coalesced "running" frame, were collapsed away, on purpose.
    assert [u["timestamp_ms"] for u in box["updates"]] == [4]
    assert box["updates"][0]["state"] == "succeeded"
    assert box["updates"][0]["result"] == {"ok": True}


def test_a_job_that_finishes_during_a_disconnect_delivers_only_its_terminal_update():
    """Dead-zone collapsing (jobs.py), at the wire level: a job that
    heartbeats a couple of times while disconnected and then genuinely
    finishes must deliver **one** `job_update` on reconnect — its terminal
    outcome — never the stale `running` marker first, contradicted by the
    real result right behind it. Sibling to the "chatty backlog" test
    above, which proves the coalescing of *several* non-terminal updates;
    this one proves a *terminal* update collapses whatever non-terminal
    marker was still pending for the same job, not merely future
    non-terminal arrivals."""
    fake_ros = FakeRos()
    box = {"updates": []}

    async def first_connection(session):
        await session.recv_hello()
        await session.accept()
        await session.close(1000)
        loop = asyncio.get_event_loop()
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="running",
                timestamp_ms=1,
                progress=0.4,
            ),
        )
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="running",
                timestamp_ms=2,
                progress=0.8,
            ),
        )
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="succeeded",
                timestamp_ms=3,
                result={"ok": True},
            ),
        )

    async def second_connection(session):
        await session.recv_hello()
        await session.accept()
        box["updates"].append(await session.recv_job_update())
        # Same pattern as the sibling tests above: hold the connection open
        # (tolerant of anything else that might land, e.g. a link_mode
        # frame) rather than asserting silence with a timing-sensitive
        # timeout — the real proof that there is exactly one `job_update`
        # is the assertion on `box["updates"]`'s length below, backed by
        # `JobUpdateQueue`'s own guarantee (test_jobs.py) that a terminal
        # update retires its job's pending non-terminal marker.
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(first_connection, second_connection)) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: len(box["updates"]) >= 1)

    asyncio.run(scenario())
    assert len(box["updates"]) == 1
    assert box["updates"][0]["state"] == "succeeded"
    assert box["updates"][0]["timestamp_ms"] == 3


def test_terminal_updates_for_different_jobs_stay_drop_nothing_and_in_order():
    """The half of 2m's decision that did *not* change: results stay
    full-fidelity, and delivery order across distinct jobs stays arrival
    order — coalescing only ever collapses a job's *own* non-terminal
    updates against each other, never a terminal one, never across jobs."""
    fake_ros = FakeRos()
    box = {"updates": []}
    job_a = "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
    job_b = "4a2f9b3d-7e5c-4f1b-9d9f-2c3b4d5e6f70"

    async def first_connection(session):
        await session.recv_hello()
        await session.accept()
        await session.close(1000)
        loop = asyncio.get_event_loop()
        for job_id, timestamp_ms in [(job_a, 0), (job_b, 1), (job_a, 2)]:
            fake_ros.jobs.updates.put_threadsafe(
                loop,
                JobUpdate(
                    job_id=job_id,
                    slug="drive_to",
                    state="succeeded",
                    timestamp_ms=timestamp_ms,
                    result={"ok": True},
                ),
            )

    async def second_connection(session):
        await session.recv_hello()
        await session.accept()
        for _ in range(3):
            box["updates"].append(await session.recv_job_update())
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(first_connection, second_connection)) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: len(box["updates"]) >= 3)

    asyncio.run(scenario())
    assert [(u["job_id"], u["timestamp_ms"]) for u in box["updates"]] == [
        (job_a, 0),
        (job_b, 1),
        (job_a, 2),
    ]


def test_a_successfully_sent_update_is_reported_delivered():
    """At the wire level: `_pump_jobs` must tell `JobManager` once a frame
    is actually sent, not merely dequeued — only then may a terminal update
    retire its job from `active_jobs`."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        update = JobUpdate(
            job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            slug="drive_to",
            state="succeeded",
            timestamp_ms=1,
            result={"ok": True},
        )
        fake_ros.jobs.updates.put_threadsafe(loop, update)
        await session.recv_job_update()
        return update

    update = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.jobs.delivered_calls == [update]


def test_a_failed_send_is_not_reported_delivered_only_the_eventual_success_is():
    fake_ros = FakeRos()
    box = {}

    async def first_connection(session):
        await session.recv_hello()
        await session.accept()
        await session.close(1000)
        loop = asyncio.get_event_loop()
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to",
                state="succeeded",
                timestamp_ms=1,
                result={"ok": True},
            ),
        )

    async def second_connection(session):
        await session.recv_hello()
        await session.accept()
        box["update"] = await session.recv_job_update()
        await session.drain()

    async def scenario():
        async with FakeCloud(sequence(first_connection, second_connection)) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "update" in box)

    asyncio.run(scenario())
    # Exactly once — from the connection that actually delivered it, not
    # from whatever the first (closed) connection's pump attempted.
    assert len(fake_ros.jobs.delivered_calls) == 1
    assert fake_ros.jobs.delivered_calls[0].job_id == "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
