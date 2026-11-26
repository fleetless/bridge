# SPDX-License-Identifier: Apache-2.0
"""The failsafe process, end to end through a real `RosRuntime`.

Lateness is measured where a robot would see it: by a subscriber in a process
of its own (`test/witness_process.py`). That process does not share the GIL
the tests below load on purpose. A failsafe's lateness is the time from the
last client message to the failsafe, both as that subscriber received them,
minus `timeout_ms`.

The bound is `LATENESS_BOUND_MS`, absolute, and it holds whatever the bridge's
own process is doing: idle, with its executor busy in a pure-Python callback,
and with threads that keep its GIL busy.
"""
import asyncio
import os
import signal
import threading
import time

import pytest
from geometry_msgs.msg import Twist

from fleetless_bridge.failsafe import (
    REASON_CRASHED,
    REASON_NOT_ALIVE,
    REASON_SELF_TEST,
    STATE_FAILED,
    STATE_OK,
)
from fleetless_bridge.failsafe_process import LATENESS_BOUND_MS
from helpers import by_slug
from test_ros_runtime import (
    WITNESS_PROCESS_READY_TIMEOUT_S,
    _publisher_cfg,
    _start_witness_process,
    _wait_until_matched,
    _writer_of,
    run,
    wait_until,
)

TOPIC = "/failsafe_test/cmd_vel"
TIMEOUT_MS = 100
#: Silences per measured run: enough to see a tail, short enough for CI.
SILENCES = 20


async def _until(condition, timeout, what):
    """`wait_until` for a condition the asyncio loop itself must make true:
    yields to the loop instead of blocking it."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("{} within {}s".format(what, timeout))
        await asyncio.sleep(0.02)


def _rx(witness):
    """`(t, x)` for every sample the witness received so far."""
    return [(e["t"], e["x"]) for e in list(witness.events) if e["ev"] == "rx"]


async def _matched_witness(rt, slug="drive", topic=TOPIC):
    witness = _start_witness_process(topic)
    witness.wait_for("ready", WITNESS_PROCESS_READY_TIMEOUT_S, "the witness process never became ready")
    wait_until(lambda: _writer_of(rt, slug).get_subscription_count() > 0,
               timeout=WITNESS_PROCESS_READY_TIMEOUT_S)
    witness.wait_for("matched", WITNESS_PROCESS_READY_TIMEOUT_S, "the witness never matched the writer")
    return witness


async def _silences(rt, witness, count, during=None):
    """`count` silences: one client message, then nothing until its
    failsafe arrives. `during()` runs right after each publish, for the load.
    Returns each failsafe's lateness in ms."""
    lateness = []
    for _ in range(count):
        before = len(_rx(witness))
        await rt.publish("drive", {"speed": 1.0})
        if during is not None:
            during()
        wait_until(lambda: len(_rx(witness)) >= before + 2, timeout=5.0)
        await asyncio.sleep(0.05)  # nothing else may follow
        samples = _rx(witness)[before:]
        assert [x for _, x in samples] == [pytest.approx(1.0), pytest.approx(0.0)], samples
        lateness.append((samples[1][0] - samples[0][0]) * 1000.0 - TIMEOUT_MS)
    return lateness


def _report(name, lateness):
    lateness = sorted(lateness)
    print("{}: n={} median={:.2f} max={:.2f} ms".format(
        name, len(lateness), lateness[len(lateness) // 2], lateness[-1]))


def _cfg(timeout_ms=TIMEOUT_MS):
    return by_slug([_publisher_cfg("drive", topic=TOPIC, timeout_ms=timeout_ms)])


def _busy(seconds):
    """Pure Python: holds the GIL except where the interpreter switches."""
    end = time.monotonic() + seconds
    x = 0
    while time.monotonic() < end:
        x += 1


# --- the bound ------------------------------------------------------------------


def test_a_failsafe_goes_out_within_the_bound_when_the_bridge_is_idle():
    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        witness = await _matched_witness(rt)
        try:
            return await _silences(rt, witness, SILENCES)
        finally:
            witness.stop()

    lateness = run(body)
    _report("idle", lateness)
    assert max(lateness) <= LATENESS_BOUND_MS


def test_a_failsafe_goes_out_within_the_bound_while_the_executor_is_busy():
    """The executor is busy in a 500 ms pure-Python callback across every
    deadline: the old in-executor watchdog would have been half a second
    late here."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        witness = await _matched_witness(rt)
        try:
            return await _silences(rt, witness, SILENCES, during=lambda: rt._enqueue(lambda: _busy(0.5)))
        finally:
            witness.stop()

    lateness = run(body)
    _report("executor busy", lateness)
    assert max(lateness) <= LATENESS_BOUND_MS


def test_a_failsafe_goes_out_within_the_bound_under_gil_load():
    """Threads in the bridge's process that keep its GIL busy, as software
    encoding does: a deadline thread in this interpreter was measured up to
    hundreds of milliseconds late under four of them. Two cores are left
    for the failsafe process and the witness."""
    hogs = max(1, min(4, (os.cpu_count() or 2) - 2))
    stop = threading.Event()

    def hog():
        while not stop.is_set():
            sum(i * i for i in range(10000))

    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        witness = await _matched_witness(rt)
        threads = [threading.Thread(target=hog, daemon=True) for _ in range(hogs)]
        for t in threads:
            t.start()
        try:
            return await _silences(rt, witness, SILENCES, during=lambda: rt._enqueue(lambda: _busy(0.3)))
        finally:
            stop.set()
            witness.stop()

    lateness = run(body)
    _report("GIL load ({} threads + busy executor)".format(hogs), lateness)
    assert max(lateness) <= LATENESS_BOUND_MS


# --- once, the race, the order ------------------------------------------------------


def test_a_publish_racing_the_deadline_gives_one_failsafe_or_none_never_two():
    """Publishes land just before, at and just after the deadline. Whatever
    the order, two failsafes never follow each other: each silence has one."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg(timeout_ms=50)) == []
        witness = await _matched_witness(rt)
        try:
            for i in range(30):
                await rt.publish("drive", {"speed": 1.0})
                await asyncio.sleep(0.045 + (i % 5) * 0.0025)
            wait_until(lambda: _rx(witness) and _rx(witness)[-1][1] == pytest.approx(0.0), timeout=5.0)
            await asyncio.sleep(0.2)
            return [x for _, x in _rx(witness)]
        finally:
            witness.stop()

    xs = run(body)
    assert xs.count(1.0) == 30
    for a, b in zip(xs, xs[1:]):
        assert not (a == 0.0 and b == 0.0), xs  # never two failsafes for one silence
    assert xs[-1] == 0.0  # the topic never ends on a client's command


def test_a_command_that_reaches_the_process_late_is_followed_by_a_failsafe():
    """The order a single writer keeps: a client message the process
    receives after its silence's deadline (held back here in the bridge's
    send) comes after that failsafe, and gets a failsafe of its own within
    the bound. The topic has one writer, so no resend can reorder them."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        witness = await _matched_witness(rt)
        try:
            await rt.publish("drive", {"speed": 1.0})
            original = rt._failsafe._send

            def late_send(message):
                if message[0] == "publish":
                    time.sleep((TIMEOUT_MS + 50) / 1000.0)  # past the first silence's deadline
                return original(message)

            rt._failsafe._send = late_send
            await rt.publish("drive", {"speed": 1.0})
            rt._failsafe._send = original
            wait_until(lambda: len(_rx(witness)) >= 4, timeout=5.0)
            await asyncio.sleep(0.2)
            return _rx(witness), rt._node.count_publishers(TOPIC)
        finally:
            witness.stop()

    rx, writers = run(body)
    assert [x for _, x in rx] == [1.0, 0.0, 1.0, 0.0]
    assert (rx[3][0] - rx[2][0]) * 1000.0 - TIMEOUT_MS <= LATENESS_BOUND_MS
    assert writers == 1


def test_a_changed_timeout_at_apply_re_arms_from_the_last_publish():
    async def body(rt):
        assert await rt.apply_publishers(_cfg(timeout_ms=5000)) == []
        witness = await _matched_witness(rt)
        try:
            await rt.publish("drive", {"speed": 1.0})
            assert await rt.apply_publishers(_cfg(timeout_ms=200)) == []
            wait_until(lambda: len(_rx(witness)) >= 2, timeout=2.0)
            return _rx(witness)
        finally:
            witness.stop()

    rx = run(body)
    assert [x for _, x in rx] == [1.0, 0.0]
    assert (rx[1][0] - rx[0][0]) * 1000.0 < 1000.0  # 200 ms, not 5 s


# --- visible from the start, and at all times ------------------------------------------


def test_the_self_test_passes_at_start_and_publishes_are_taken():
    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        return rt.failsafe_state()

    state, reason, lateness_ms = run(body)
    assert state == STATE_OK
    assert reason is None
    assert 0.0 <= lateness_ms <= LATENESS_BOUND_MS


def test_a_late_self_test_refuses_publishes_until_the_watchdog_recovers():
    async def body(rt):
        reported = []
        first = rt.failsafe_state()
        assert await rt.apply_publishers(_cfg()) == []
        await rt.publish("drive", {"speed": 1.0})
        refused = rt._publishers["drive"].last_activity_at is None
        rt.on_failsafe_state(lambda *s: reported.append(s))
        await _until(lambda: reported and reported[-1][0] == STATE_OK, 15.0, "no ok report")
        await rt.publish("drive", {"speed": 1.0})
        return first, refused, rt._publishers["drive"].last_activity_at, reported

    first, refused, taken_at, reported = run(body, failsafe_options={"block_before_self_test_s": 0.1})
    assert first[0] == STATE_FAILED and first[1] == REASON_SELF_TEST
    assert first[2] >= LATENESS_BOUND_MS
    assert refused
    assert taken_at is not None
    assert reported and reported[-1][0] == STATE_OK


def test_a_failsafe_process_that_stops_answering_is_failed_within_two_seconds():
    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        pid = rt._failsafe.pid
        os.kill(pid, signal.SIGSTOP)  # our own child, by its PID
        stopped_at = time.monotonic()
        wait_until(lambda: rt.failsafe_state()[0] == STATE_FAILED, timeout=3.0)
        failed_after = time.monotonic() - stopped_at
        reason = rt.failsafe_state()[1]
        await rt.publish("drive", {"speed": 1.0})
        refused = rt._publishers["drive"].last_activity_at is None
        wait_until(lambda: rt.failsafe_state()[0] == STATE_OK, timeout=20.0)
        new_pid = rt._failsafe.pid
        await rt.publish("drive", {"speed": 1.0})
        return failed_after, reason, refused, pid, new_pid, rt._publishers["drive"].last_activity_at

    failed_after, reason, refused, pid, new_pid, taken_at = run(body)
    assert failed_after <= 2.0
    assert reason == REASON_NOT_ALIVE
    assert refused
    assert new_pid != pid
    assert taken_at is not None


def test_a_killed_failsafe_process_is_failed_and_replaced():
    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        pid = rt._failsafe.pid
        os.kill(pid, signal.SIGKILL)  # our own child, by its PID
        wait_until(lambda: rt.failsafe_state()[0] == STATE_FAILED, timeout=3.0)
        reason = rt.failsafe_state()[1]
        wait_until(lambda: rt.failsafe_state()[0] == STATE_OK, timeout=20.0)
        witness = await _matched_witness(rt)
        try:
            lateness = await _silences(rt, witness, 3)
        finally:
            witness.stop()
        return reason, lateness

    reason, lateness = run(body)
    assert reason == REASON_CRASHED
    assert max(lateness) <= LATENESS_BOUND_MS


def test_the_failsafe_process_fires_when_the_bridge_is_gone():
    """The pipe closes, as it does when the bridge freezes, crashes or is
    `kill -9`'d: every armed failsafe goes out at once."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg(timeout_ms=60_000)) == []
        witness = await _matched_witness(rt)
        try:
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(_rx(witness)) >= 1, timeout=5.0)
            closed_at = time.monotonic()
            rt._failsafe._conn.close()
            wait_until(lambda: len(_rx(witness)) >= 2, timeout=5.0)
            return _rx(witness), closed_at
        finally:
            witness.stop()

    rx, closed_at = run(body)
    assert [x for _, x in rx] == [1.0, 0.0]
    assert rx[1][0] - closed_at < 1.0  # at once, not after the minute


# --- idle -----------------------------------------------------------------------------


def test_an_idle_bridge_has_no_watchdog_tick_and_its_failsafe_process_sleeps():
    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        await asyncio.sleep(0.5)
        with open("/proc/{}/stat".format(rt._failsafe.pid)) as f:
            process_state = f.read().rsplit(")", 1)[1].split()[0]
        return rt._watchdog_timer, process_state

    timer, process_state = run(body)
    assert timer is None
    assert process_state == "S"  # asleep in its wait, not spinning


def test_the_failsafe_state_reaches_the_loop_on_every_change():
    async def body(rt):
        reported = []
        rt.on_failsafe_state(lambda *s: reported.append(s))
        os.kill(rt._failsafe.pid, signal.SIGKILL)
        await _until(lambda: len(reported) >= 2 and reported[-1][0] == STATE_OK, 20.0,
                     "no failed and ok report")
        return reported

    reported = run(body)
    assert reported[0][0] == STATE_FAILED and reported[0][1] == REASON_CRASHED
    assert reported[-1][0] == STATE_OK
