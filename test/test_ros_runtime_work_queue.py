# SPDX-License-Identifier: Apache-2.0
"""The work queue's account of itself (fleetless/fleetless#1238): a job whose
waiter timed out gets one WARNING line that says why — H1 lost wake-up, H2
head-of-line, H3 slow job, H4 waiter — and the queue writes an INFO summary
an hour.

Each cause is made to happen for real, against a spinning runtime (the
`run(body)` harness of `test_ros_runtime.py`), with a short timeout:

- H1: the `_trigger_guard` seam loses one wake-up;
- H2: a job ahead of it holds the executor;
- H3: the job itself outlasts the timeout;
- H4: the waiter is made to give up on a job that has finished.

The log is read by a handler on the module logger: `caplog` does not see
this module's records in this test container (see `test_ros_runtime.py`).
"""
import asyncio
import concurrent.futures
import functools
import logging
import re
import threading
import time
from unittest import mock

import pytest
import rclpy
from conftest import wait_until
from helpers import by_slug
from sensor_msgs.msg import BatteryState

from fleetless_bridge import ros_runtime
from fleetless_bridge.ros_runtime import (
    _WAIT_TIMEOUT_ERRORS,
    WORK_SUMMARY_INTERVAL_S,
    RosRuntime,
    _work_kind,
)
from test_ros_runtime import _dp, run

_LINE = re.compile(r"^work queue: job (?P<kind>\S+) waited (?P<waited>[0-9.]+) s: (?P<cause>H[1-4]) \((?P<reason>.*)\)$")


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def messages(self, level):
        return [r.getMessage() for r in self.records if r.levelno == level]

    def stalls(self):
        return [_LINE.match(m) for m in self.messages(logging.WARNING) if m.startswith("work queue: job ")]

    def summaries(self):
        return [m for m in self.messages(logging.INFO) if m.startswith("work queue, last ")]


@pytest.fixture
def records():
    handler = _Records()
    logger = logging.getLogger("fleetless_bridge.ros_runtime")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


def _one_stall(records):
    stalls = records.stalls()
    assert len(stalls) == 1, records.messages(logging.WARNING)
    assert stalls[0] is not None, records.messages(logging.WARNING)
    return stalls[0]


def _keep_publishing(topic, hz):
    """Publishes a battery value on `topic` at `hz` from a thread of its
    own until the returned function is called: the executor then has a
    subscription callback to serve all the while."""
    stop = threading.Event()
    node = rclpy.create_node("test_work_queue_publisher_{}".format(id(stop)))
    publisher = node.create_publisher(BatteryState, topic, 10)

    def loop():
        msg = BatteryState()
        while not stop.is_set():
            msg.percentage = (msg.percentage + 1.0) % 100.0
            publisher.publish(msg)
            stop.wait(1.0 / hz)

    thread = threading.Thread(target=loop, name="test-work-queue-publisher", daemon=True)
    thread.start()

    def done():
        stop.set()
        thread.join(timeout=5.0)
        node.destroy_node()

    return done


# --- the four causes ---------------------------------------------------------


def test_a_lost_wake_up_is_h1_while_the_executor_serves_subscriptions(records):
    """The guard condition is never triggered for one job, so no drain runs
    for it, while a 20 Hz subscription keeps the executor busy. The line
    says H1, that no drain looked, and how old the newest subscription
    callback is: under 2 s, the evidence that the executor was not stuck.
    The lost job runs with the next wake-up."""
    ran = []

    async def body(rt):
        await rt.apply_config(by_slug([_dp("battery", topic="/work_queue_battery")]))
        done = _keep_publishing("/work_queue_battery", hz=20)
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, wait_until, lambda: rt._last_subscription_at is not None
            )
            rt._trigger_guard = lambda: None
            try:
                with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                    await rt._submit_async(lambda: ran.append("lost"), timeout=0.5)
            finally:
                del rt._trigger_guard  # the class's own again
            assert ran == []
            assert await rt._submit_async(lambda: "next") == "next"
            assert ran == ["lost"]
        finally:
            done()

    run(body)
    line = _one_stall(records)
    assert line["cause"] == "H1"
    assert line["kind"] == "test_a_lost_wake_up_is_h1_while_the_executor_serves_subscriptions.body"
    assert float(line["waited"]) >= 0.5
    assert line["reason"].startswith("no drain since its enqueue, 1 jobs queued, newest subscription callback ")
    age = float(re.search(r"newest subscription callback ([0-9.]+) s ago", line["reason"]).group(1))
    assert age < 2.0


def test_a_lost_wake_up_without_subscriptions_says_so(records):
    async def body(rt):
        rt._trigger_guard = lambda: None
        try:
            with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                await rt._submit_async(lambda: None, timeout=0.3)
        finally:
            del rt._trigger_guard

    run(body)
    line = _one_stall(records)
    assert line["cause"] == "H1"
    assert line["reason"] == "no drain since its enqueue, 1 jobs queued, no subscription callback yet"


def test_a_job_behind_one_that_holds_the_executor_is_h2(records):
    release = threading.Event()

    def hold_the_executor():
        release.wait(5.0)

    async def body(rt):
        rt._enqueue(hold_the_executor)
        await asyncio.get_running_loop().run_in_executor(
            None, wait_until, lambda: rt._work_running is not None
        )
        try:
            with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                await rt._submit_async(lambda: None, timeout=0.5)
        finally:
            release.set()

    run(body)
    line = _one_stall(records)
    assert line["cause"] == "H2"
    assert re.fullmatch(
        r"behind test_a_job_behind_one_that_holds_the_executor_is_h2\.hold_the_executor "
        r"running for [0-9.]+ s, 1 jobs queued",
        line["reason"],
    ), line["reason"]


def test_a_job_that_outlasts_its_timeout_is_h3(records):
    async def body(rt):
        with pytest.raises(_WAIT_TIMEOUT_ERRORS):
            await rt._submit_async(lambda: time.sleep(1.0), timeout=0.5)

    run(body)
    line = _one_stall(records)
    assert line["cause"] == "H3"
    assert re.fullmatch(r"taken after [0-9.]+ s, running for [0-9.]+ s", line["reason"]), line["reason"]


def test_a_finished_job_whose_waiter_timed_out_is_h4(records):
    """The waiter is made to wait for the job and then give up on it: the
    job finished in time, and the waiter did not see it."""

    async def body(rt):
        loop = asyncio.get_running_loop()
        real = loop.run_in_executor

        def late_waiter(executor, result, timeout):
            def wait_then_give_up():
                result(5.0)
                raise concurrent.futures.TimeoutError()

            return real(executor, wait_then_give_up)

        with mock.patch.object(loop, "run_in_executor", late_waiter):
            with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                await rt._submit_async(lambda: "done", timeout=0.5)

    run(body)
    line = _one_stall(records)
    assert line["cause"] == "H4"
    assert re.fullmatch(r"done after [0-9.]+ s; the waiter did not see it", line["reason"]), line["reason"]


def test_the_blocking_waiter_of_stop_reports_the_same_way(records):
    async def body(rt):
        rt._trigger_guard = lambda: None
        try:
            with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                rt._submit(lambda: None, timeout=0.3)
        finally:
            del rt._trigger_guard

    run(body)
    line = _one_stall(records)
    assert line["cause"] == "H1"
    assert line["kind"] == "test_the_blocking_waiter_of_stop_reports_the_same_way.body"


def test_a_job_that_raises_a_timeout_of_its_own_writes_no_line(records):
    """A job may itself raise a timeout (a socket's, a nested future's);
    the waiter re-raises it as it is, and that is no stall."""

    def own_timeout():
        raise TimeoutError("the job's own")

    def nested_timeout():
        concurrent.futures.Future().result(timeout=0.01)

    async def body(rt):
        with pytest.raises(TimeoutError, match="the job's own"):
            await rt._submit_async(own_timeout, timeout=2.0)
        with pytest.raises(_WAIT_TIMEOUT_ERRORS):
            await rt._submit_async(nested_timeout, timeout=2.0)
        with pytest.raises(TimeoutError, match="the job's own"):
            rt._submit(own_timeout, timeout=2.0)

    run(body)
    assert records.stalls() == []


def test_a_job_that_answers_in_time_writes_no_line(records):
    async def body(rt):
        for _ in range(20):
            assert await rt._submit_async(lambda: 1, timeout=2.0) == 1

    run(body)
    assert records.stalls() == []


# --- what a job is called ------------------------------------------------------


def _named(qualname):
    def fn():
        return None

    fn.__qualname__ = qualname
    return fn


def test_the_kind_is_the_method_a_job_was_written_in_never_its_arguments():
    assert _work_kind(_named("RosRuntime.next_snapshot.<locals>.<lambda>")) == "next_snapshot"
    assert _work_kind(_named("RosRuntime._rebuild_caps")) == "_rebuild_caps"
    assert _work_kind(_named("RosRuntime.stop.<locals>.teardown")) == "stop.teardown"
    assert _work_kind(_named("<lambda>")) == "<lambda>"
    assert _work_kind(RosRuntime._destroy_all_cameras) == "_destroy_all_cameras"
    assert _work_kind(functools.partial(print, "frt_secret")) == "partial"
    secret = "job-with-a-secret"
    assert secret not in _work_kind(lambda: print(secret))


# --- the hourly summary --------------------------------------------------------


def test_the_summary_names_the_periods_counters_and_starts_again(records):
    async def body(rt):
        assert rt._work_summary_timer.timer_period_ns == int(WORK_SUMMARY_INTERVAL_S * 1e9)
        for _ in range(5):
            await rt._submit_async(lambda: None)
        rt._trigger_guard = lambda: None
        try:
            with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                await rt._submit_async(lambda: None, timeout=0.3)
        finally:
            del rt._trigger_guard
        await rt._submit_async(rt._log_work_summary)
        await rt._submit_async(rt._log_work_summary)

    run(body)
    first, second = records.summaries()
    match = re.fullmatch(
        r"work queue, last \d+ min: (\d+) triggers, (\d+) drains \((\d+) empty\), (\d+) jobs taken, "
        r"longest wait to be taken ([0-9.]+) s, stalls H1 1, H2 0, H3 0, H4 0",
        first,
    )
    assert match, first
    triggers, drains, empty, taken, longest = match.groups()
    assert int(triggers) >= 7 and int(taken) >= 7 and int(drains) >= 1
    assert float(longest) >= 0.3  # the lost job waited for the next wake-up
    assert re.fullmatch(
        r"work queue, last 0 min: 1 triggers, 1 drains \(0 empty\), 1 jobs taken, "
        r"longest wait to be taken [0-9.]+ s, stalls H1 0, H2 0, H3 0, H4 0",
        second,
    ), second


def test_a_failure_to_record_reaches_no_caller_and_is_logged_once(records, monkeypatch):
    def broken(fn):
        raise RuntimeError("the bookkeeping broke")

    async def body(rt):
        monkeypatch.setattr(ros_runtime, "_work_kind", broken)
        rt._trigger_guard = lambda: None
        try:
            for _ in range(2):
                with pytest.raises(_WAIT_TIMEOUT_ERRORS):
                    await rt._submit_async(lambda: None, timeout=0.2)
        finally:
            del rt._trigger_guard
        assert await rt._submit_async(lambda: "still works") == "still works"
        await rt._submit_async(rt._log_work_summary)

    run(body)
    assert records.stalls() == []
    errors = [m for m in records.messages(logging.ERROR) if m.startswith("work queue:")]
    assert errors == [
        "work queue: a stall could not be recorded; it is counted in the hourly summary "
        "from now on, not logged again"
    ]
    (summary,) = records.summaries()
    assert summary.endswith("stalls H1 0, H2 0, H3 0, H4 0, 2 recording failures"), summary


# --- the fast path -------------------------------------------------------------


def test_every_job_from_many_threads_is_taken_and_finished(records):
    jobs = []
    lock = threading.Lock()

    async def body(rt):
        def enqueue_many(start):
            mine = [rt._enqueue_job(lambda i=i: i) for i in range(start, start + 500)]
            with lock:
                jobs.extend(mine)

        loop = asyncio.get_running_loop()
        await asyncio.gather(*(loop.run_in_executor(None, enqueue_many, n * 500) for n in range(4)))
        results = await loop.run_in_executor(
            None, lambda: sorted(job.future.result(timeout=10.0) for job in jobs)
        )
        assert results == list(range(2000))

    run(body)
    assert len(jobs) == 2000
    for job in jobs:
        assert job.enqueued_at <= job.taken_at <= job.done_at
    assert records.stalls() == []
