# SPDX-License-Identifier: Apache-2.0
"""`PrioritizedWriter` — the one writer that owns the socket.

Tests it directly here, against `TimedWs` (a fake socket whose `send()`
takes a fixed, known time, so the throughput estimate is predictable) and
small `FakeSource` stand-ins for the duck-typed `WriterSource` contract
(`try_next`/`on_sent`/`on_send_failure`). `test_client_writer_wiring.py`
covers `BridgeClient` building and driving a real one.
"""
import asyncio
import re

import pytest
from helpers import TimedWs

import fleetless_bridge.client as client_module
from fleetless_bridge.camera import SNAPSHOT_MAX_BYTES
from fleetless_bridge.client import (
    MAX_SEND_OCCUPANCY_S,
    RATE_SAMPLE_MIN_BYTES,
    PrioritizedWriter,
)


class FakeSource:
    """A pull source: `try_next()` hands back queued items one at a time,
    `None` once empty. Records what the writer reports back through
    `on_sent`/`on_send_failure`, the same way `JobManager` tracks delivery
    and put-back."""

    def __init__(self, items=None):
        self._items = list(items or [])
        self.sent = []
        self.failed = []

    def try_next(self):
        if self._items:
            return self._items.pop(0)
        return None

    def on_sent(self, item):
        self.sent.append(item)

    def on_send_failure(self, item):
        self.failed.append(item)


class FailingWs:
    """A socket whose every send raises immediately — for exercising the
    writer's failure path without waiting out a real disconnect."""

    async def send(self, payload) -> None:
        raise OSError("connection reset")


async def _run_briefly(writer: PrioritizedWriter, seconds: float = 0.3) -> None:
    """Runs `writer.run()` for a bounded window, then cancels it — for
    tests that only care about a burst of sends, not about `run()` ending
    on its own."""
    task = asyncio.ensure_future(writer.run())
    await asyncio.sleep(seconds)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_a_lower_tier_item_enqueued_later_still_goes_first():
    # A send is already in flight (TimedWs: 0.1 s) when tier 3 ("bulk")
    # then tier 0 ("pong") are enqueued. Tier order, not arrival order,
    # must decide what goes out next: pong before bulk.
    async def scenario():
        ws = TimedWs(0.1)
        writer = PrioritizedWriter(ws, sources=[])
        writer.enqueue(0, "first-accepted")
        task = asyncio.ensure_future(writer.run())
        await asyncio.sleep(0.02)  # the first send is now in flight
        writer.enqueue(3, "bulk")
        writer.enqueue(0, "pong")
        await asyncio.sleep(0.3)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return ws.sent

    sent = asyncio.run(scenario())
    assert sent == ["first-accepted", "pong", "bulk"]


def test_every_large_send_feeds_the_rate_estimate():
    # A push send and a pulled-source send, both timed by the same fake
    # socket, must both fold into one estimate — not just the snapshot path
    # 2.0.2 measured. Both are over `RATE_SAMPLE_MIN_BYTES`, which is what
    # makes them measurements of the link at all (see the two tests below).
    # seconds_per_send=0.1: a 100_000-byte payload observes ~1_000_000 B/s,
    # a 300_000-byte payload observes ~3_000_000 B/s; the second folds onto
    # the first exactly like `_record_snapshot_send`'s `(prev+observed)/2`.
    # Real wall-clock sleeps, so `approx` — not exact equality — is the
    # honest tolerance for a measured rate.
    async def scenario():
        ws = TimedWs(0.1)
        source = FakeSource([b"y" * 300_000])
        writer = PrioritizedWriter(ws, sources=[(1, source)])
        writer.enqueue(0, b"x" * 100_000)
        await _run_briefly(writer, 0.5)
        return writer.rate_estimate()

    rate = asyncio.run(scenario())
    assert rate == pytest.approx((1_000_000.0 + 3_000_000.0) / 2, rel=0.05)


def test_a_mix_of_small_sends_never_sets_the_rate_estimate():
    """The defect, from the writer's side: a send under the transport's
    64 KiB write high-water returns before a byte leaves the robot, so
    timing it measures per-send CPU, not the link. On a 151 kB/s uplink,
    folding those put the steady estimate between 300 kB/s and 3.8 MB/s
    — pinning `snapshot_max_bytes()` at the wire-format ceiling and
    making the fitted-snapshot design inert, with 2.0.2's own guard
    already deleted in its favour.

    Twelve sends, four tiers, both feed paths (push and pulled), all
    under `RATE_SAMPLE_MIN_BYTES`: the estimate must stay `None` — a
    session that has only sent control frames has not measured its link
    and must say so, not report its own `send()` cost as a link speed.

    `snapshot_max_bytes()` is asserted too: that's where the estimate is
    actually *spent* — `None` there means the wire-format ceiling,
    2.0.2's seeding semantics (the first snapshot of a session
    establishes the rate)."""

    async def scenario():
        ws = TimedWs(0.01)
        source = FakeSource([b"p" * 900, b"q" * 4000, b"r" * (RATE_SAMPLE_MIN_BYTES - 1)])
        writer = PrioritizedWriter(ws, sources=[(1, source)])
        for size in (10, 200, 900, 4000, 30_000, RATE_SAMPLE_MIN_BYTES - 1):
            writer.enqueue(0, b"x" * size)
        for size in (120, 8000, RATE_SAMPLE_MIN_BYTES - 1):
            writer.enqueue(3, b"z" * size)
        await _run_briefly(writer, 0.5)
        return writer.rate_estimate(), writer.snapshot_max_bytes(), len(ws.sent)

    rate, max_bytes, sent_count = asyncio.run(scenario())
    assert sent_count == 12, "the writer did not actually send the frames: {}".format(
        sent_count
    )
    assert rate is None, rate
    assert max_bytes == SNAPSHOT_MAX_BYTES


def test_small_sends_after_a_large_one_do_not_drag_the_estimate():
    """The bias the size bound exists to keep out, made visible.

    `TimedWs` charges the same wall-clock time per send regardless of
    payload, so a small frame reads as a very slow link — 900 bytes in
    0.1 s is 9 kB/s against the large send's 1 MB/s. Opposite sign from
    the real defect (a buffered send returns in microseconds, so the real
    one reads *fast*), but the same point: the number is about something
    other than the link.

    So the claim is `==`, not "close to": the estimate after eight small
    sends must be the *unchanged* value the large send established. One
    folded small send would halve it — no tolerance would hide that."""

    async def scenario():
        ws = TimedWs(0.1)
        writer = PrioritizedWriter(ws, sources=[])
        writer.enqueue(0, b"x" * 100_000)  # ~1_000_000 B/s
        await _run_briefly(writer, 0.25)
        established = writer.rate_estimate()
        for _ in range(8):
            writer.enqueue(0, b"s" * 900)  # ~9_000 B/s each, if folded
        await _run_briefly(writer, 1.2)
        return established, writer.rate_estimate(), len(ws.sent)

    established, after, sent_count = asyncio.run(scenario())
    assert established == pytest.approx(1_000_000, rel=0.05), established
    assert sent_count == 9, "the small sends never happened: {}".format(sent_count)
    assert after == established


def test_snapshot_max_bytes_follows_the_rate():
    async def scenario():
        ws = TimedWs(0.1)
        writer = PrioritizedWriter(ws, sources=[])
        # Unknown rate: falls back to the wire-format ceiling.
        before = writer.snapshot_max_bytes()
        # One send of 70_000 bytes (over `RATE_SAMPLE_MIN_BYTES`, so it
        # counts) in ~0.1 s -> ~700_000 B/s. Deliberately a rate whose
        # occupancy budget still lands under `SNAPSHOT_MAX_BYTES`: a faster
        # one would be clamped by the ceiling and this test would pass
        # without the rate ever being consulted.
        writer.enqueue(0, b"x" * 70_000)
        await _run_briefly(writer, 0.3)
        return before, writer.snapshot_max_bytes()

    before, after = asyncio.run(scenario())
    assert before == SNAPSHOT_MAX_BYTES
    assert after == pytest.approx(700_000 * MAX_SEND_OCCUPANCY_S, rel=0.05)
    assert after < SNAPSHOT_MAX_BYTES, "clamped by the ceiling; the rate was never spent"


def test_an_oversized_tier3_payload_is_sent_with_a_warning(monkeypatch):
    # Rate is pinned to ~1_000_000 B/s by a priming send; a 2_000_000-byte
    # tier-3 payload then projects to ~2 s, well over a 1 s test budget.
    # Tier <= 4 is must-deliver: it goes out anyway, with a warning naming
    # bytes, rate and projected seconds.
    #
    # The priming send is 100_000 bytes rather than the 10 it used to be
    # because only a send of at least `RATE_SAMPLE_MIN_BYTES` folds into
    # the estimate at all — and this warning cannot fire before there is an
    # estimate, which is the accepted consequence recorded on that constant.
    #
    # Not `caplog`: this suite has already found (see
    # `test_client_datapoints.py`'s `_RecordingLog`, `test_camera_sources.py`'s
    # `capfd`-based tests) that `caplog` does not reliably see records from
    # this module in this environment — a `caplog`-only version would be the
    # exact "cannot fail" instrument shape to avoid. Monkeypatch
    # the module-level `log` instead, the pattern already established for
    # `client.py`.
    class RecordingLog:
        def __init__(self):
            self.warnings = []

        def warning(self, fmt, *args):
            self.warnings.append(fmt % args if args else fmt)

        def __getattr__(self, name):
            return lambda *a, **kw: None

    recording_log = RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        ws = TimedWs(0.1)  # fixed per-send time, independent of payload size
        writer = PrioritizedWriter(ws, sources=[], max_occupancy_s=1.0)
        # 100_000 bytes / ~0.1 s = ~1_000_000 B/s, seeds the rate.
        writer.enqueue(0, b"x" * 100_000)
        await _run_briefly(writer, 0.25)
        writer.enqueue(3, b"y" * 2_000_000)
        await _run_briefly(writer, 0.3)
        return ws.sent

    sent = asyncio.run(scenario())

    assert b"y" * 2_000_000 in sent
    assert len(recording_log.warnings) == 1
    message = recording_log.warnings[0]
    assert "2000000" in message  # the payload size
    assert "tier 3" in message
    assert "occupancy budget" in message  # names the budget it exceeded
    # The priming send's rate is a real (scheduler-jittered) wall-clock
    # measurement, not an exact 1_000_000 B/s — assert it landed in the
    # neighbourhood, not the literal value.
    match = re.search(r"at (\d+) B/s", message)
    assert match is not None
    assert 500_000 <= int(match.group(1)) <= 2_000_000


def test_send_failure_calls_on_send_failure_and_ends_run():
    async def scenario():
        source = FakeSource(["doomed"])
        writer = PrioritizedWriter(FailingWs(), sources=[(0, source)])
        # run() must return on its own — not raise, not hang — the moment
        # the send fails.
        await asyncio.wait_for(writer.run(), timeout=2.0)
        return source

    source = asyncio.run(scenario())
    assert source.failed == ["doomed"]
    assert source.sent == []


def test_sources_are_scanned_in_tier_order_not_registration_order():
    # Registered with the higher tier first; the writer must still ask the
    # tier-0 source before the tier-3 one.
    async def scenario():
        ws = TimedWs(0.01)
        tier3 = FakeSource(["bulk"])
        tier0 = FakeSource(["pong"])
        writer = PrioritizedWriter(ws, sources=[(3, tier3), (0, tier0)])
        await _run_briefly(writer, 0.2)
        return ws.sent

    sent = asyncio.run(scenario())
    assert sent == ["pong", "bulk"]


def test_a_hanging_snapshot_pull_never_delays_a_pong():
    """The tier-5 pull must not be awaited inline. `RosRuntime.
    next_snapshot` goes through `_submit_async`, which queues behind
    whatever the ROS executor is already doing and gives up only after
    `WORK_TIMEOUT_S` (ten seconds). This writer is the only sender, so an
    inline await would leave a `pong` already sitting in tier 0 unsent for
    up to those ten seconds — and the cloud closes the socket after about
    six. The pull runs as its own task instead: at most one in flight,
    harvested by a later scan.

    `started == 1` is the second half of the claim, not incidental: a
    fresh pull per tick would park a default-executor thread per tick
    against a wedged ROS executor and exhaust that pool, taking every
    other `_submit_async` caller down with it."""

    class HangingSnapshots:
        def __init__(self):
            self.started = 0

        async def next(self, max_bytes):
            self.started += 1
            await asyncio.sleep(30)  # far longer than this test, or the cloud's patience
            return b"never arrives"

    async def scenario():
        ws = TimedWs(0.01)
        snapshots = HangingSnapshots()
        writer = PrioritizedWriter(ws, sources=[], snapshot_source=snapshots)
        task = asyncio.ensure_future(writer.run())
        # Several ticks: the pull is in flight and going nowhere.
        await asyncio.sleep(0.9)
        writer.enqueue(0, "pong")
        await asyncio.sleep(0.2)
        alive = not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return ws.sent, alive, snapshots.started

    sent, alive, started = asyncio.run(scenario())
    assert sent == ["pong"]
    assert alive, "run() ended while a pull was merely slow"
    assert started == 1, started


def test_a_raising_source_closes_the_socket_and_ends_run(monkeypatch):
    # A broken source (try_next raises) must not leave run()'s exception
    # unhandled — `client.py` drives run() as fire-and-forget, and an
    # unretrieved exception there would end the writer silently while the
    # bridge stays connected and stops sending anything. Same defect shape
    # `_pump_control`'s 2.0.1 fix closed off in the receive loop (see
    # client.py's own docstring there); same fix here: catch once, log,
    # close the socket so `_converse` notices, and return.
    class RaisingSource:
        def try_next(self):
            raise RuntimeError("source is broken")

        def on_sent(self, item):
            pass

        def on_send_failure(self, item):
            pass

    class RecordingCloseWs:
        def __init__(self):
            self.closed = False

        async def send(self, payload):
            raise AssertionError("nothing should ever be sent")

        async def close(self):
            self.closed = True

    class RecordingLog:
        def __init__(self):
            self.exceptions = []

        def exception(self, fmt, *args):
            self.exceptions.append(fmt % args if args else fmt)

        def __getattr__(self, name):
            return lambda *a, **kw: None

    recording_log = RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def scenario():
        ws = RecordingCloseWs()
        writer = PrioritizedWriter(ws, sources=[(0, RaisingSource())])
        # run() must return on its own — not raise, not hang.
        await asyncio.wait_for(writer.run(), timeout=2.0)
        return ws

    ws = asyncio.run(scenario())
    assert ws.closed
    assert len(recording_log.exceptions) == 1


def test_cancelling_the_writer_stops_it_even_when_a_wake_just_fired():
    """A cancel arriving in the same loop iteration as a `wake()` must still
    end `run()`.

    This is the interleaving `BridgeClient._converse`'s `finally` produces
    at the end of every busy session — `enqueue()` sets `_wake`, then the
    teardown cancels the writer — and `_tick` used to lose it. On Python
    3.10, ROS Humble's interpreter, `asyncio.wait_for` catches the
    cancellation and, if its inner future is already done, returns that
    result instead of re-raising (CPython bpo-37658). The writer then ran
    on forever, `_cancel_pump`'s `await task` never returned, and the
    session hung on teardown with no reconnect to deliver what was queued.

    `wait_for(..., 2.0)` rather than a bare `await`: the failure mode is a
    hang, and a test that reproduces a hang by hanging reports nothing."""

    async def scenario():
        writer = PrioritizedWriter(TimedWs(0.0), sources=[])
        task = asyncio.ensure_future(writer.run())
        # Long enough for the writer to be parked in `_tick` with an
        # `Event.wait` of its own outstanding — the only state in which
        # anything could be swallowed.
        await asyncio.sleep(0.05)
        # Both in one synchronous stretch, so the event resolves and the
        # cancel is delivered without a loop turn between them.
        writer.enqueue(0, "x")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)

    asyncio.run(scenario())
