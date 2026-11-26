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
from std_msgs.msg import String

from fleetless_bridge.failsafe import (
    ALIVE_TIMEOUT_S,
    REASON_CRASHED,
    REASON_NOT_ALIVE,
    REASON_SELF_TEST,
    STATE_FAILED,
    STATE_OK,
    SUPERVISE_INTERVAL_S,
    TERM_ANSWER_TIMEOUT_S,
)
from fleetless_bridge.failsafe_process import LATENESS_BOUND_MS
from fleetless_bridge.protocol import ParameterSpec
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


def _gone(pid):
    """Whether our child `pid` has exited (gone, or a zombie not reaped yet)."""
    try:
        with open("/proc/{}/stat".format(pid)) as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except FileNotFoundError:
        return True


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


def test_a_failsafe_goes_out_within_the_bound_beside_large_publishes_under_gil_load():
    """A second publisher takes 200 KB every 10 ms while the GIL is loaded.
    A message over 16 KiB reaches the process in pieces, each written only
    when the bridge's sender gets the GIL back. A process that waited for
    the rest of a message held every deadline meanwhile; review measured
    `late_fire` at 11.6 to 24.7 ms. This one reads what arrived and fires
    in between."""
    hogs = max(1, min(4, (os.cpu_count() or 2) - 2))
    stop = threading.Event()
    big = String(data="x" * 200_000)

    def hog():
        while not stop.is_set():
            sum(i * i for i in range(10000))

    async def body(rt):
        cfg = _cfg()
        cfg.update(by_slug([_publisher_cfg(
            "say", topic="/failsafe_test/chatter", type_="std_msgs/msg/String",
            message={"data": "${text}"}, parameters={"text": ParameterSpec(type="string")},
            failsafe={"data": ""}, timeout_ms=60_000,
        )]))
        assert await rt.apply_publishers(cfg) == []
        witness = await _matched_witness(rt)
        sent = []

        def large():
            # The runtime's own send path, from a thread: the loop thread is
            # blocked in `_silences`' waits.
            while not stop.is_set():
                sent.append(rt._failsafe.publish("say", big))
                time.sleep(0.01)

        threads = [threading.Thread(target=hog, daemon=True) for _ in range(hogs)]
        threads.append(threading.Thread(target=large, daemon=True))
        for t in threads:
            t.start()
        try:
            lateness = await _silences(rt, witness, SILENCES, during=lambda: rt._enqueue(lambda: _busy(0.3)))
        finally:
            stop.set()
            witness.stop()
        return lateness, rt.failsafe_state(), sent

    lateness, state, sent = run(body)
    _report("large publishes + GIL load ({} threads + busy executor)".format(hogs), lateness)
    assert state[0] == STATE_OK, state
    assert sent and all(sent)
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
            # The graph can lag the samples on a loaded machine: wait for the
            # one writer, then watch that a second one never shows.
            wait_until(lambda: rt._node.count_publishers(TOPIC) >= 1, timeout=10.0)
            counts = []
            for _ in range(10):
                counts.append(rt._node.count_publishers(TOPIC))
                await asyncio.sleep(0.02)
            return _rx(witness), counts
        finally:
            witness.stop()

    rx, counts = run(body)
    assert [x for _, x in rx] == [1.0, 0.0, 1.0, 0.0]
    assert (rx[3][0] - rx[2][0]) * 1000.0 - TIMEOUT_MS <= LATENESS_BOUND_MS
    assert max(counts) == 1


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


def test_a_publish_while_the_watchdog_is_starting_is_refused():
    """Until the first self-test passed, nothing is published: the same
    refusal as a failed watchdog, before the state was ever `ok`."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg()) == []
        state = rt.failsafe_state()[0]
        sent = []
        original = rt._failsafe._send
        rt._failsafe._send = lambda message: sent.append(message[0]) or original(message)
        await rt.publish("drive", {"speed": 1.0})
        rt._failsafe._send = original
        return state, rt._publishers["drive"].last_activity_at, sent

    # Held in `starting` on purpose: the first self-test waits 2 s.
    state, taken_at, sent = run(body, failsafe_options={"block_before_self_test_s": 2.0},
                                wait_for_failsafe=False)
    assert state == "starting"
    assert taken_at is None
    assert "publish" not in sent


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


def test_a_stopped_failsafe_process_is_killed_within_its_answer_time_and_its_failsafe_follows():
    """A frozen process runs no SIGTERM handler. It is SIGKILLed once it did
    not answer within `TERM_ANSWER_TIMEOUT_S`, not after the acknowledgement
    wait a healthy one gets, and the new process fires the failsafe it owed.
    From the stop, the failsafe reaches the robot after the second without
    a word, the answer time, the new process's start and its reader match."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg(timeout_ms=60_000)) == []
        witness = await _matched_witness(rt)
        try:
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(_rx(witness)) >= 1, timeout=5.0)
            pid = rt._failsafe.pid
            os.kill(pid, signal.SIGSTOP)  # our own child, by its PID
            stopped_at = time.monotonic()
            wait_until(lambda: _gone(pid), timeout=10.0)
            gone_after = time.monotonic() - stopped_at
            wait_until(lambda: len(_rx(witness)) >= 2, timeout=20.0)
            return _rx(witness), stopped_at, gone_after
        finally:
            witness.stop()

    rx, stopped_at, gone_after = run(body)
    assert [x for _, x in rx] == [1.0, 0.0]
    # One second without a word, a supervisor turn, the answer time, slack.
    assert gone_after <= ALIVE_TIMEOUT_S + SUPERVISE_INTERVAL_S + TERM_ANSWER_TIMEOUT_S + 1.0
    assert rx[1][0] - stopped_at <= 10.0


def test_a_sigterm_amid_publishes_never_leaves_the_topic_on_a_command():
    """SIGTERM while client messages stream in: the handler only sets a
    flag, the loop acts on it between messages, and every command before it
    is followed by a failsafe."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg(timeout_ms=60_000)) == []
        witness = await _matched_witness(rt)
        try:
            pid = rt._failsafe.pid
            for i in range(200):
                await rt.publish("drive", {"speed": 1.0})
                if i == 150:
                    os.kill(pid, signal.SIGTERM)  # our own child, by its PID
            wait_until(lambda: _rx(witness) and _rx(witness)[-1][1] == pytest.approx(0.0), timeout=10.0)
            await asyncio.sleep(0.3)
            return [x for _, x in _rx(witness)]
        finally:
            witness.stop()

    xs = run(body)
    assert 0.0 in xs
    assert xs[-1] == 0.0


def test_a_killed_failsafe_process_s_owed_failsafe_comes_from_its_replacement():
    """SIGKILL right after a client's command: the killed process fires
    nothing, and the new one sends the failsafe it owed, once a reader
    matched its fresh writer."""

    async def body(rt):
        assert await rt.apply_publishers(_cfg(timeout_ms=60_000)) == []
        witness = await _matched_witness(rt)
        try:
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(_rx(witness)) >= 1, timeout=5.0)
            killed_at = time.monotonic()
            os.kill(rt._failsafe.pid, signal.SIGKILL)  # our own child, by its PID
            wait_until(lambda: len(_rx(witness)) >= 2, timeout=30.0)
            await asyncio.sleep(0.3)
            return _rx(witness), killed_at
        finally:
            witness.stop()

    rx, killed_at = run(body)
    assert [x for _, x in rx] == [1.0, 0.0]
    assert rx[1][0] - killed_at <= 10.0  # the restart and a match, not the minute


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
