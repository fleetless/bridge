# SPDX-License-Identifier: Apache-2.0
"""JobManager in isolation — no rclpy; it never touches a ROS object
(ros_runtime.py owns those). Just the bookkeeping: which job runs which
slug, and what `hello.active_jobs` says."""
import asyncio

from fleetless_bridge.jobs import JobManager, JobUpdate


def test_a_started_job_is_the_running_job_for_its_slug():
    jobs = JobManager()
    jobs.start("job-1", "drive_to", "action")
    assert jobs.running_job_id("drive_to") == "job-1"


def test_a_slug_with_no_job_has_none_running():
    jobs = JobManager()
    assert jobs.running_job_id("drive_to") is None


def test_finishing_a_job_clears_it_from_its_slug():
    jobs = JobManager()
    jobs.start("job-1", "drive_to", "action")
    jobs.finish("job-1", "succeeded")
    assert jobs.running_job_id("drive_to") is None


def test_active_jobs_names_slug_and_state_for_every_job_still_held():
    jobs = JobManager()
    jobs.start("job-1", "drive_to", "action")
    jobs.start("job-2", "reset-odom", "service")
    jobs.finish("job-2", "succeeded")
    # job-2 retires the instant `finish` (delivery) confirms it — see
    # test_finish_retires_the_job_entirely below.
    assert jobs.active_jobs() == [("job-1", "drive_to", "running")]


def test_a_fresh_manager_has_no_active_jobs():
    assert JobManager().active_jobs() == []


def test_tracked_count_reflects_every_job_still_held():
    """`tracked_count()` bounds `RosRuntime`'s admission guard — a
    terminal-but-undelivered job (still owed a report) counts the same as
    a running one, and stops counting the instant `finish` retires it."""
    jobs = JobManager()
    assert jobs.tracked_count() == 0
    jobs.start("job-1", "drive_to", "action")
    assert jobs.tracked_count() == 1
    jobs.start("job-2", "reset-odom", "service")
    assert jobs.tracked_count() == 2

    async def scenario():
        loop = asyncio.get_event_loop()
        jobs.emit(
            loop,
            JobUpdate(job_id="job-1", slug="drive_to", state="succeeded", timestamp_ms=1),
        )
        await asyncio.wait_for(jobs.updates.get(), timeout=1.0)

    asyncio.run(scenario())
    # Terminal but not yet delivered — still counted.
    assert jobs.tracked_count() == 2

    jobs.finish("job-1", "succeeded")
    assert jobs.tracked_count() == 1  # retired, no longer held


def test_starting_the_same_job_id_twice_is_a_no_op():
    # A redelivered invoke (e.g. a reconnect race) must not stomp an
    # already-tracked job or duplicate its slug entry.
    jobs = JobManager()
    jobs.start("job-1", "drive_to", "action")
    jobs.start("job-1", "drive_to", "action")
    assert jobs.active_jobs() == [("job-1", "drive_to", "running")]


def test_finishing_an_unknown_job_id_does_not_raise():
    JobManager().finish("no-such-job", "failed")  # must not raise


def test_finish_retires_the_job_entirely():
    """`finish` (reached only via `mark_delivered`) removes the record,
    not just flips a state field — keeps `_jobs` from growing for the
    life of the process."""
    jobs = JobManager()
    jobs.start("job-1", "drive_to", "action")
    jobs.finish("job-1", "succeeded")
    assert jobs.active_jobs() == []


def test_emit_alone_queues_the_update_but_does_not_yet_retire_the_job():
    """`emit` only queues the frame and records the state — a queued-but-
    undelivered terminal update must not retire the job yet: one that
    finishes while disconnected stays named in `active_jobs` until its
    outcome reaches the cloud."""

    async def scenario():
        jobs = JobManager()
        jobs.start("job-1", "drive_to", "action")
        loop = asyncio.get_event_loop()
        jobs.emit(
            loop,
            JobUpdate(job_id="job-1", slug="drive_to", state="succeeded", timestamp_ms=1),
        )
        update = await asyncio.wait_for(jobs.updates.get(), timeout=1.0)
        return jobs, update

    jobs, update = asyncio.run(scenario())
    assert update.state == "succeeded"
    # Still held — queued, not yet confirmed delivered.
    assert jobs.running_job_id("drive_to") == "job-1"
    assert jobs.active_jobs() == [("job-1", "drive_to", "succeeded")]


def test_emit_reports_the_terminal_state_immediately_not_running():
    """The bridge's own answer must be visible to `active_jobs()` as soon
    as it's known — a job that has in fact succeeded, but whose
    `job_update` frame is unsent, is named `succeeded` here, not
    `running`. Reporting `running` for a job the bridge knows is done is
    exactly the asserted-but-unverified state this settlement removes."""

    async def scenario():
        jobs = JobManager()
        jobs.start("job-1", "drive_to", "action")
        loop = asyncio.get_event_loop()
        jobs.emit(
            loop,
            JobUpdate(
                job_id="job-1", slug="drive_to", state="failed", timestamp_ms=1,
                error=("action_failed", "boom"),
            ),
        )
        return jobs

    jobs = asyncio.run(scenario())
    assert jobs.active_jobs() == [("job-1", "drive_to", "failed")]


def test_mark_delivered_retires_the_job_once_its_terminal_update_is_sent():
    async def scenario():
        jobs = JobManager()
        jobs.start("job-1", "drive_to", "action")
        loop = asyncio.get_event_loop()
        update = JobUpdate(job_id="job-1", slug="drive_to", state="succeeded", timestamp_ms=1)
        jobs.emit(loop, update)
        await asyncio.wait_for(jobs.updates.get(), timeout=1.0)
        jobs.mark_delivered(update)
        return jobs

    jobs = asyncio.run(scenario())
    assert jobs.running_job_id("drive_to") is None
    assert jobs.active_jobs() == []


def test_a_terminal_update_that_never_gets_delivered_leaves_the_job_active_forever():
    """Reproduced directly: a job that finishes while disconnected must
    not be reported lost on a queued-but-undelivered update alone."""

    async def scenario():
        jobs = JobManager()
        jobs.start("job-1", "drive_to", "action")
        loop = asyncio.get_event_loop()
        jobs.emit(
            loop,
            JobUpdate(job_id="job-1", slug="drive_to", state="succeeded", timestamp_ms=1),
        )
        # No mark_delivered — as if the connection never came back.
        return jobs

    jobs = asyncio.run(scenario())
    assert jobs.active_jobs() == [("job-1", "drive_to", "succeeded")]


def test_mark_delivered_of_a_non_terminal_update_does_nothing():
    async def scenario():
        jobs = JobManager()
        jobs.start("job-1", "drive_to", "action")
        loop = asyncio.get_event_loop()
        update = JobUpdate(job_id="job-1", slug="drive_to", state="running", timestamp_ms=1)
        jobs.emit(loop, update)
        await asyncio.wait_for(jobs.updates.get(), timeout=1.0)
        jobs.mark_delivered(update)
        return jobs

    jobs = asyncio.run(scenario())
    assert jobs.active_jobs() == [("job-1", "drive_to", "running")]


def test_emit_of_a_running_update_leaves_the_job_active():
    async def scenario():
        jobs = JobManager()
        jobs.start("job-1", "drive_to", "action")
        loop = asyncio.get_event_loop()
        jobs.emit(
            loop,
            JobUpdate(
                job_id="job-1", slug="drive_to", state="running",
                timestamp_ms=1, feedback={"distance": 1.0}, progress=0.2,
            ),
        )
        await asyncio.wait_for(jobs.updates.get(), timeout=1.0)
        return jobs

    jobs = asyncio.run(scenario())
    assert jobs.running_job_id("drive_to") == "job-1"
    assert jobs.active_jobs() == [("job-1", "drive_to", "running")]


def test_terminal_updates_across_many_jobs_never_drop_and_deliver_in_order():
    """Results (2m): unbounded, drop-nothing, arrival order — exactly as
    before the coalescing change, because a terminal update is never a
    marker candidate (`_push` only treats `state in _TERMINAL_STATES` this
    way; see the "same job coalesces" test below for the other half)."""

    async def scenario():
        jobs = JobManager()
        loop = asyncio.get_event_loop()
        for i in range(50):
            jobs.updates.put_threadsafe(
                loop,
                JobUpdate(job_id="job-{}".format(i), slug="drive_to", state="succeeded", timestamp_ms=i),
            )
        received = [await jobs.updates.get() for _ in range(50)]
        return received

    received = asyncio.run(scenario())
    assert [u.timestamp_ms for u in received] == list(range(50))


def test_non_terminal_updates_for_the_same_job_coalesce_to_the_latest():
    """2m: opposite of the terminal case above, and the fix's whole
    point — 50 feedback frames for one disconnected job must not become
    50 queued frames. Only the latest is delivered; the other 49 are
    genuinely gone, not summarized (see `JobUpdateQueue`'s docstring for
    why that trade is right)."""

    async def scenario():
        jobs = JobManager()
        loop = asyncio.get_event_loop()
        for i in range(50):
            jobs.updates.put_threadsafe(
                loop,
                JobUpdate(
                    job_id="job-1", slug="drive_to", state="running",
                    timestamp_ms=i, feedback={"step": i},
                ),
            )
        await asyncio.sleep(0)  # let every threadsafe callback land before get()
        received = await jobs.updates.get()
        assert jobs.updates.empty()  # nothing else queued — all 50 collapsed into this one
        return received

    received = asyncio.run(scenario())
    assert received.timestamp_ms == 49
    assert received.feedback == {"step": 49}


def test_requeue_front_of_a_terminal_update_puts_it_back_ahead_of_the_rest():
    # This is what a failed send does (client.py's _pump_jobs): get()
    # already dequeued the update, the send failed, and it must go back
    # where it would have been handed out next — not behind updates that
    # arrived after it. Two different jobs, so this is about ordering, not
    # coalescing (see the non-terminal requeue tests below for that).
    async def scenario():
        jobs = JobManager()
        loop = asyncio.get_event_loop()
        jobs.updates.put_threadsafe(
            loop, JobUpdate(job_id="job-1", slug="drive_to", state="succeeded", timestamp_ms=1)
        )
        jobs.updates.put_threadsafe(
            loop, JobUpdate(job_id="job-2", slug="dock", state="succeeded", timestamp_ms=2)
        )
        dequeued = await jobs.updates.get()  # job-1 — the send for this one "fails"
        jobs.updates.requeue_front(dequeued)
        return [await jobs.updates.get() for _ in range(2)]

    received = asyncio.run(scenario())
    assert [u.job_id for u in received] == ["job-1", "job-2"]


def test_requeue_front_of_a_non_terminal_update_puts_it_back_when_nothing_superseded_it():
    async def scenario():
        jobs = JobManager()
        loop = asyncio.get_event_loop()
        jobs.updates.put_threadsafe(
            loop, JobUpdate(job_id="job-1", slug="drive_to", state="running", timestamp_ms=1)
        )
        dequeued = await jobs.updates.get()  # the send for this one "fails"
        jobs.updates.requeue_front(dequeued)
        return await jobs.updates.get()

    received = asyncio.run(scenario())
    assert received.timestamp_ms == 1


def test_requeue_front_of_a_stale_non_terminal_update_is_dropped_if_superseded():
    """The race `requeue_front` resolves: `get()` already removed job-1's
    marker and `_pending_feedback` entry, so a fresher feedback frame for
    the same job — arriving before the failed send is requeued, since
    `_pump_jobs` awaits `ws.send()` between the two — finds nothing to
    coalesce against and installs its own marker and entry. Requeuing the
    stale copy must not double-queue or clobber the newer one; it must
    simply lose, as if it had arrived first and been superseded."""

    async def scenario():
        jobs = JobManager()
        loop = asyncio.get_event_loop()
        jobs.updates.put_threadsafe(
            loop, JobUpdate(job_id="job-1", slug="drive_to", state="running", timestamp_ms=1)
        )
        stale = await jobs.updates.get()  # the send for this one "fails"
        # A fresher feedback frame for the same job lands before the requeue.
        jobs.updates.put_threadsafe(
            loop, JobUpdate(job_id="job-1", slug="drive_to", state="running", timestamp_ms=2)
        )
        await asyncio.sleep(0)  # let the threadsafe callback land
        jobs.updates.requeue_front(stale)
        received = await jobs.updates.get()
        assert jobs.updates.empty()  # the stale copy did not also queue itself
        return received

    received = asyncio.run(scenario())
    assert received.timestamp_ms == 2


def test_a_fresh_queue_is_empty():
    assert JobManager().updates.empty() is True


def test_a_queue_with_a_pending_update_is_not_empty():
    async def scenario():
        jobs = JobManager()
        loop = asyncio.get_event_loop()
        jobs.updates.put_threadsafe(
            loop, JobUpdate(job_id="job-1", slug="drive_to", state="running", timestamp_ms=1)
        )
        await asyncio.sleep(0)  # let the threadsafe callback land
        return jobs.updates.empty()

    assert asyncio.run(scenario()) is False
