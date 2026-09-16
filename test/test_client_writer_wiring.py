# SPDX-License-Identifier: Apache-2.0
"""The session on the prioritized writer: one writer owns the socket, and
snapshots are pulled and fitted rather than pushed.

`test_client_writer.py` proves what `PrioritizedWriter` does when driven
directly. This file proves that `BridgeClient` actually hands it the whole
session: that a `pong` is tier 0 and a backfill datapoint is tier 4, that
`camera_states` is the bounded latest-per-slug queue, and that snapshots
are pulled with the writer's own byte budget instead of queued. Nothing
here reaches into the writer's internals — the
claims are made where a defect would be felt: on the wire the fake cloud
sees, or in what `FakeRos` was asked for.

Two shapes of test, deliberately:

* Through `FakeCloud` (a real WebSocket on a real port) wherever the claim
  is about *which frames leave, and in what relative order*. The queues are
  filled **before** the cloud answers the hello: the writer's ROS-fed
  sources stay shut until `hello_ok` (see `BridgeClient._build_writer`), so
  a test that fills them first is racing nothing — every item is in place
  before the writer's first scan.
* Against `TimedWs` (helpers.py: every send takes a known time) wherever
  the claim needs a link that is actually slow — the measured rate estimate,
  or a drain that is still running when something overtakes it. A loopback
  socket gives neither: `ws.send()` there returns without blocking, which
  the first test below records in detail. Those tests still build the
  session's real writer through `BridgeClient._build_writer` rather than a
  hand-assembled one, so the wiring under test is the production wiring and
  not a second copy of it.
"""
import asyncio
import concurrent.futures
import json

import pytest
from fake_cloud import FakeCloud
from helpers import TimedWs, make_client, run_until
from test_client_datapoints import FakeRos, _run_one_exchange

import fleetless_bridge.client as client_module
from fleetless_bridge.camera import SNAPSHOT_MAX_BYTES
from fleetless_bridge.client import (
    MAX_SEND_OCCUPANCY_S,
    BridgeClient,
    PrioritizedWriter,
)
from fleetless_bridge.config import BridgeConfig
from fleetless_bridge.jobs import JobUpdate
from fleetless_bridge.protocol import pong_message, snapshot_frame
from fleetless_bridge.ros_runtime import AssetsAvailable, CameraStateUpdate, SampleQueue
from fleetless_bridge.sampling import Sample

_BACKFILL_ITEMS = 50


def _offline_client(ros) -> BridgeClient:
    """A client that never connects — for the `TimedWs` tests, which drive
    the session's writer directly rather than a whole session."""
    return BridgeClient(
        BridgeConfig(token="frt_test_token", cloud_url="ws://example.invalid/bridge"),
        ros=ros,
    )


async def _run_briefly(writer, seconds: float) -> None:
    task = asyncio.ensure_future(writer.run())
    await asyncio.sleep(seconds)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# --- tier 0 beats tier 4: a pong is answered mid-drain -------------------------


def test_a_ping_during_a_backfill_drain_is_answered_before_the_drain_ends():
    """The ordering the whole design exists for: session liveness first.
    A reconnect with a full backlog is exactly when the cloud's ping has to
    be answered, and the cloud closes the socket after three unanswered
    ones. The pong must overtake the queued backfill, not wait behind it.

    `TimedWs`, not `FakeCloud`: over a
    loopback socket `ws.send()` does not block. 50 backfill frames of
    400 kB each, 20 MB in total, were all accepted by `send()` before a ping
    sent after the second frame had round-tripped, so the pong went out 51st no
    matter what the writer did.
    A `FakeCloud` version of this claim would have been the instrument that
    cannot fail: green for a writer with no tier ordering at all. What is
    exercised here instead is the client's own source assembly
    (`_build_writer`) and the exact line `_converse` runs on a `Ping` — the
    remaining half, that a real session's receive loop reaches that line
    rather than sending the pong itself, is what `test_client_pingpong.py`
    still proves end to end."""

    async def scenario():
        fake_ros = FakeRos()
        fake_ros.backlog.configure("old", True, _BACKFILL_ITEMS)
        for i in range(_BACKFILL_ITEMS):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        client = _offline_client(fake_ros)
        client._session_open = True
        ws = TimedWs(0.01)  # 50 items is half a second of drain
        writer = client._build_writer(ws)
        task = asyncio.ensure_future(writer.run())
        await asyncio.sleep(0.05)  # mid-drain: a few items are through
        writer.enqueue(0, pong_message(7))  # the line `_converse` runs on a Ping
        await asyncio.sleep(0.7)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return ws.sent

    sent = asyncio.run(scenario())
    kinds = [json.loads(frame)["type"] for frame in sent]
    assert "pong" in kinds, kinds
    position = kinds.index("pong")
    # Mid-drain on both sides: some backfill went first (the ping had not
    # arrived yet), and most of it is still owed afterwards.
    assert position >= 1
    assert len(kinds) - position - 1 >= _BACKFILL_ITEMS // 2, (
        "only {} of {} backfill items were still owed when the pong went "
        "out".format(len(kinds) - position - 1, _BACKFILL_ITEMS)
    )


# --- strict tier order across live, tooling and backfill ----------------------


def test_backfill_drains_only_when_no_live_sample_is_ready():
    """Backfill is tier 4: below live datapoints (tier 2) **and** below the
    on-demand tooling and asset reports (tier 3). The middle placement is
    the decision the design weighed explicitly — a reconnect's backlog must
    not stall an interactive path, and must still outrank the expendable
    snapshot tier."""
    fake_ros = FakeRos()
    received = []

    async def behavior(session):
        await session.recv_hello()
        loop = asyncio.get_event_loop()
        fake_ros.backlog.configure("old", True, 10)
        for i in range(3):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        fake_ros.assets.put_nowait(AssetsAvailable(urdf=True, meshes=()))
        for i in range(2):
            fake_ros.samples.put_threadsafe(
                loop, Sample(slug="live", value=i, timestamp_ms=100 + i)
            )
        await asyncio.sleep(0.02)  # let call_soon_threadsafe land before hello_ok
        await session.accept()
        for _ in range(6):
            received.append(await session.recv())
        await session.drain()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: len(received) == 6)

    asyncio.run(scenario())
    kinds = [
        frame["type"] if frame["type"] != "datapoint" else frame["slug"]
        for frame in received
    ]
    assert kinds == ["live", "live", "assets_available", "old", "old", "old"], kinds


def test_backfill_is_oldest_first():
    """Within one slug the backlog is FIFO and stays FIFO — retiring
    `BACKFILL_MIN_INTERVAL_S` changed *when* the drain runs, never the
    order it runs in."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        fake_ros.backlog.configure("old", True, 10)
        for i in range(3):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        return [await session.recv_datapoint() for _ in range(3)]

    received = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert [frame["value"] for frame in received] == [0, 1, 2]


# --- camera_states: bounded, latest-per-slug ----------------------------------


def test_camera_states_keep_only_the_latest_per_slug():
    """The design's inventory table calls today's `camera_states` an
    "unbounded `asyncio.Queue`" and fixes it here: a camera flapping while
    the writer is busy must cost one queue slot, not one per transition,
    and what reaches the cloud must be the state that is currently true —
    not a replay of every state it passed through."""
    fake_ros = FakeRos()
    received = []

    async def behavior(session):
        await session.recv_hello()
        # Distinct `observed_at_ms` per put; the *last* is what the
        # assertion names. Three states differing only in `publishing`
        # would let a keep-*first* queue pass this test as easily as a
        # keep-latest one — the exact shape of check that cannot fail.
        for observed_at_ms, publishing in (
            (1786400000000, True), (1786400000001, False), (1786400000002, True)
        ):
            fake_ros.camera_states.put(
                CameraStateUpdate(
                    "front", publishing, None, cause="source",
                    observed_at_ms=observed_at_ms,
                    request_id=None,
                )
            )
        fake_ros.camera_states.put(
            CameraStateUpdate(
                "back", False, None, cause="command",
                observed_at_ms=1786400000009, request_id="req-9",
            )
        )
        await session.accept()
        received.append(await session.recv_camera_state())
        received.append(await session.recv_camera_state())
        # Proof there is no third one queued behind them: a ping sent now
        # comes back as a pong, and the next frame after the two states is
        # that pong rather than a stale `front` transition.
        await session.ping(11)
        received.append(await session.recv())
        await session.drain()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: len(received) == 3)

    asyncio.run(scenario())
    assert [frame["slug"] for frame in received[:2]] == ["front", "back"]
    # The *third* front, not the first: three puts, one frame, and the one
    # that survives is identifiable by its own timestamp.
    assert received[0]["observed_at_ms"] == 1786400000002
    assert received[0]["publishing"] is True
    assert received[2]["type"] == "pong"


def test_nothing_ros_fed_leaves_before_hello_ok():
    """The gate `_GatedSource` and `_SnapshotSource` share. The six pumps
    this design replaced were *started* at `hello_ok`; the writer cannot
    be, because `hello` goes through it and a `pong` may overtake the
    greeting. So the gate moved from when a source starts to when it
    answers — and with the two "no snapshot while disconnected" tests
    retired along with the snapshot queue, nothing else covers it.

    Everything a greeted session would send is queued *before* the cloud
    answers the hello, and the cloud then listens long enough for several
    writer ticks. Silence is the claim; the same three frames arriving the
    moment `hello_ok` lands is what stops silence from meaning "the
    sources were never wired"."""
    fake_ros = FakeRos()
    box = {}

    async def behavior(session):
        await session.recv_hello()
        loop = asyncio.get_event_loop()
        fake_ros.samples.put_threadsafe(
            loop, Sample(slug="live", value=1, timestamp_ms=1)
        )
        fake_ros.jobs.updates.put_threadsafe(
            loop,
            JobUpdate(
                job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
                slug="drive_to", state="succeeded", timestamp_ms=2,
                result={"ok": True},
            ),
        )
        fake_ros.snapshots.put_threadsafe(
            loop,
            snapshot_frame(
                slug="front", mime="image/jpeg", width=1, height=1,
                timestamp_ms=3, image_bytes=b"\x00",
            ),
        )
        try:
            # `recv_raw`, not `recv`: the question is whether *anything*
            # left, and a leaked snapshot is a binary frame that `recv`
            # would fail to JSON-decode before this could report it.
            box["before_hello_ok"] = await asyncio.wait_for(
                session.recv_raw(), timeout=1.0
            )
        except asyncio.TimeoutError:
            box["before_hello_ok"] = None
        # Direct, not timing-dependent: tier 5 is pulled, so a gate that
        # leaked would show up as the runtime having been asked at all.
        box["snapshot_pulls_before_hello_ok"] = len(fake_ros.snapshot_max_bytes_asked)

        await session.accept()
        after = []
        # Bounded, and `after` is recorded either way: a gate that leaked
        # has already spent frames before `accept()`, and a plain
        # `for _ in range(3)` would then block until `run_until` gave up —
        # reddening this test with "the condition never became true"
        # rather than with the claim it is actually about.
        try:
            for _ in range(3):
                raw = await asyncio.wait_for(session.recv_raw(), timeout=2.0)
                after.append("snapshot" if isinstance(raw, (bytes, bytearray))
                             else json.loads(raw)["type"])
        except asyncio.TimeoutError:
            pass
        box["after"] = set(after)
        await session.drain()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: "after" in box)

    asyncio.run(scenario())
    assert box["before_hello_ok"] is None, box["before_hello_ok"]
    assert box["snapshot_pulls_before_hello_ok"] == 0
    assert box["after"] == {"datapoint", "job_update", "snapshot"}


# --- a live sample preempts the next backfill item, mid-drain -----------------


class _WatchedWs(TimedWs):
    """`TimedWs` with a hook that fires when a send *starts*, before its
    own delay.

    Polling `ws.sent` instead would be a race that decides this test's
    claim: by the time the Nth frame has been appended, the writer has
    already scanned and may already be inside the (N+1)th send, so a live
    sample put then could legitimately land after it. The hook fires while
    the Nth send is still in flight, which is exactly the window "arriving
    MID-DRAIN" names — the sample is queued during a send, and the very
    next scan is the one whose ordering is under test."""

    def __init__(self, seconds_per_send: float, on_send_start) -> None:
        super().__init__(seconds_per_send)
        self._on_send_start = on_send_start

    async def send(self, payload) -> None:
        self._on_send_start(payload)
        await super().send(payload)


def test_a_live_sample_arriving_mid_drain_preempts_the_next_backfill_item():
    """Spec test-table row 6. Not "live goes first when both are already
    queued" — that is the row above, and a drain that runs to completion
    once started passes it — but: a drain is *running*, a live sample
    arrives while one of its frames is on the wire, and the very next frame
    is that sample rather than the next backfill item.

    The difference is the whole point of retiring `BACKFILL_MIN_INTERVAL_S`:
    live traffic always sits in a higher tier and preempts the drain
    between any two backfill items. What makes it structural is
    that the writer re-scans every tier between any two sends; a writer
    that drained a source it had started would deliver this sample only
    after all 20 backfill items, and nothing else in the suite would
    notice."""

    live_after = 3

    async def scenario():
        fake_ros = FakeRos()
        fake_ros.backlog.configure("old", True, 20)
        for i in range(20):
            fake_ros.backlog.push("old", Sample(slug="old", value=i, timestamp_ms=i))
        client = _offline_client(fake_ros)
        client._session_open = True
        loop = asyncio.get_event_loop()
        state = {"sends_started": 0, "put": False}

        def on_send_start(payload):
            state["sends_started"] += 1
            if state["sends_started"] == live_after and not state["put"]:
                state["put"] = True
                fake_ros.samples.put_threadsafe(
                    loop, Sample(slug="live", value=99, timestamp_ms=99)
                )

        ws = _WatchedWs(0.03, on_send_start)
        writer = client._build_writer(ws)
        await _run_briefly(writer, 0.6)
        return ws.sent, state["put"]

    sent, put = asyncio.run(scenario())
    assert put, "the live sample was never put — the drain never reached frame {}".format(
        live_after
    )
    slugs = [json.loads(frame)["slug"] for frame in sent]
    # Everything sent is a datapoint; the only question is the order.
    assert slugs[:live_after] == ["old"] * live_after, slugs
    assert slugs[live_after] == "live", slugs
    # And the drain then resumes rather than being abandoned — otherwise
    # "live went first" would be indistinguishable from "backfill stopped".
    assert slugs[live_after + 1] == "old", slugs
    assert slugs.count("old") >= live_after + 2, slugs


# --- a stalled ROS work queue pauses snapshots, it does not end the session ---


def test_a_snapshot_pull_that_times_out_does_not_close_the_session():
    """`RosRuntime.next_snapshot` goes through `_submit_async`, which gives
    up after `WORK_TIMEOUT_S` (ten seconds) if the ROS executor is busy —
    a `resolve_types` over a large graph, a camera apply, a `graph_
    snapshot`. That timeout surfaces at `pull.result()`, and before this it
    reached `run()`'s catch-all, which closes the socket. The reconnect
    then re-applies the whole configuration, which is *more* executor work
    queued behind the same stall: a ten-second hiccup compounding into a
    reconnect loop. Before the pressure work a stalled executor only
    delayed snapshots; it must again.

    The exception type is established here rather than assumed, because on
    Python 3.10 — ROS Humble's interpreter — `concurrent.futures.
    TimeoutError` is neither the builtin `TimeoutError` nor
    `asyncio.TimeoutError`; all three are distinct classes and a catch
    written against the wrong one is a guard that cannot fire. This test
    raises the one `_submit_async` actually produces.

    Socket-closure is deliberately *not* weakened for anything else: the
    raising-source test in test_client_writer.py still holds."""

    class _StallingSnapshots:
        """Times out like a stalled `_submit_async`, then recovers."""

        def __init__(self, timeouts: int) -> None:
            self.calls = 0
            self._timeouts = timeouts

        async def next(self, max_bytes):
            self.calls += 1
            if self.calls <= self._timeouts:
                raise concurrent.futures.TimeoutError()
            return None

    class _RecordingCloseWs(TimedWs):
        def __init__(self, seconds_per_send: float) -> None:
            super().__init__(seconds_per_send)
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    async def scenario():
        snapshots = _StallingSnapshots(timeouts=3)
        ws = _RecordingCloseWs(0.01)
        writer = PrioritizedWriter(ws, sources=[], snapshot_source=snapshots)
        task = asyncio.ensure_future(writer.run())
        # Long enough for several ticks, i.e. several timed-out pulls.
        await asyncio.sleep(1.0)
        still_running = not task.done()
        # The claim that matters: the session is still usable afterwards.
        writer.enqueue(0, pong_message(5))
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return ws.sent, ws.closed, still_running, snapshots.calls

    sent, closed, still_running, calls = asyncio.run(scenario())
    assert calls >= 2, "only {} pulls — the stall was never retried".format(calls)
    assert still_running, "run() ended on a timed-out pull"
    assert not closed, "the writer closed the socket over a stalled ROS work queue"
    assert [json.loads(frame)["type"] for frame in sent] == ["pong"]


def test_a_snapshot_pull_stall_is_logged_once_per_streak(monkeypatch):
    """Once per stall, not once per tick. The writer wakes four times a
    second (`_WRITER_TICK_S`), so a ROS executor wedged for a minute would
    otherwise write 240 identical lines — and a log line that appears 240
    times stops being read, which is the same "an instrument stops being
    read by its third run" failure this package has already paid for.

    The second half is what stops this being satisfied by a writer that
    never logs at all: the streak ends, and a *later* stall gets its own
    line."""

    class RecordingLog:
        def __init__(self):
            self.warnings = []

        def warning(self, fmt, *args):
            self.warnings.append(fmt % args if args else fmt)

        def __getattr__(self, name):
            return lambda *a, **kw: None

    recording_log = RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    class _TwoStreaks:
        """Times out, recovers for one pull, then times out again."""

        def __init__(self) -> None:
            self.calls = 0

        async def next(self, max_bytes):
            self.calls += 1
            if self.calls in (4,):
                return None  # the pull that ends the first streak
            raise concurrent.futures.TimeoutError()

    async def scenario():
        snapshots = _TwoStreaks()
        writer = PrioritizedWriter(TimedWs(0.01), sources=[], snapshot_source=snapshots)
        task = asyncio.ensure_future(writer.run())
        await asyncio.sleep(2.0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return snapshots.calls

    calls = asyncio.run(scenario())
    assert calls >= 6, "only {} pulls — not enough ticks to tell once from per-tick".format(
        calls
    )
    stall_lines = [w for w in recording_log.warnings if "ROS work queue is stalled" in w]
    assert len(stall_lines) == 2, (
        "{} stall warnings for two streaks over {} pulls".format(len(stall_lines), calls)
    )


# --- tier 2's high-water mark comes from the source queues -------------------


def test_a_camera_state_queue_depth_shows_up_in_pressure_stats():
    """Before bridge 2.2.0's pressure pump, tier 2 had no push deque at
    all — `enqueue()` was never called for it — so the writer's own
    `high_water` bookkeeping could only ever report 0 there, whatever the
    robot actually queued. The console would render a dead gauge and
    nobody could tell it from a calm robot. This test still drives
    `_build_writer` directly rather than through `_converse`, so no pump
    ever starts here (that needs a real `hello_ok`) — the source-side
    reporting this test is about is exactly the half that still matters
    when the pump's own deque has not reached this depth itself.

    Four uncoalescible camera states (each answering a command, so each
    takes its own place in the queue — see `CameraStateQueue`) are queued
    before the writer runs, and tier 2's high-water mark must show that
    depth."""

    async def scenario():
        fake_ros = FakeRos()
        client = _offline_client(fake_ros)
        client._session_open = True
        for i in range(4):
            fake_ros.camera_states.put(
                CameraStateUpdate(
                    "front", True, None, cause="command",
                    observed_at_ms=1786400000000 + i,
                    request_id="req-{}".format(i),
                )
            )
        assert fake_ros.camera_states.high_water() == 4
        writer = client._build_writer(TimedWs(0.01))
        await _run_briefly(writer, 0.3)
        return client.pressure_stats()

    stats = asyncio.run(scenario())
    assert stats["tiers"][2]["sent"] == 4, stats["tiers"][2]
    assert stats["tiers"][2]["high_water"] == 4, stats["tiers"][2]


# --- tier 5: pulled, and fitted to the writer's own budget --------------------


def test_a_snapshot_is_pulled_with_the_writer_budget():
    """`encode_snapshot_jpeg` "finally receives a true target instead
    of the flat 1.5 MiB". The number the runtime is asked for must be
    `rate x MAX_SEND_OCCUPANCY_S`, estimated on this socket — not
    `camera.SNAPSHOT_MAX_BYTES`, which knows only the wire format."""

    async def scenario():
        fake_ros = FakeRos()
        client = _offline_client(fake_ros)
        # The gate `_converse` opens at hello_ok; opened by hand because
        # this test drives the writer without a session around it.
        client._session_open = True
        ws = TimedWs(0.1)
        writer = client._build_writer(ws)
        # 70_000 bytes in ~0.1 s seeds the estimate at ~700_000 B/s. Over
        # `RATE_SAMPLE_MIN_BYTES`, which is the only kind of send that
        # counts — a smaller one is accepted into the socket's write buffer
        # and times CPU rather than the link (see the constant). And the
        # occupancy budget still lands under `SNAPSHOT_MAX_BYTES`, so the
        # assertion below is about rate, not ceiling.
        writer.enqueue(0, b"x" * 70_000)
        await _run_briefly(writer, 0.6)
        return fake_ros.snapshot_max_bytes_asked

    asked = asyncio.run(scenario())
    assert asked, "the writer never asked the runtime for a snapshot at all"
    assert asked[-1] == pytest.approx(700_000 * MAX_SEND_OCCUPANCY_S, rel=0.15)
    assert asked[-1] < SNAPSHOT_MAX_BYTES, (
        "clamped by the wire-format ceiling; the measured rate was never spent"
    )


def test_the_first_snapshot_of_a_session_is_asked_for_at_the_wire_ceiling():
    """The other half of the same claim, and the one `RATE_SAMPLE_MIN_BYTES`
    made true again: a session's *first* snapshot is pulled before anything
    has measured the link, so the number the runtime is asked for is
    `camera.SNAPSHOT_MAX_BYTES` — and that snapshot is what establishes the
    rate every later one is fitted to. 2.0.2's seeding semantics, unchanged
    by the writer taking the measurement over.

    Without this, the honest reading of the test above would be "the budget
    tracks the rate *once there is one*" with nothing saying what happens
    before that — which is every session's first frames."""

    async def scenario():
        fake_ros = FakeRos()
        client = _offline_client(fake_ros)
        client._session_open = True
        ws = TimedWs(0.05)
        writer = client._build_writer(ws)
        # Control-sized traffic only: real pongs and hellos, none of them
        # anywhere near the write high-water.
        writer.enqueue(0, pong_message(1))
        writer.enqueue(0, pong_message(2))
        await _run_briefly(writer, 0.6)
        return fake_ros.snapshot_max_bytes_asked, writer.rate_estimate()

    asked, rate = asyncio.run(scenario())
    assert asked, "the writer never asked the runtime for a snapshot at all"
    assert rate is None, rate
    assert set(asked) == {SNAPSHOT_MAX_BYTES}, asked


def test_no_snapshot_is_pulled_while_lower_tiers_have_traffic():
    """Strict priority, stated from the snapshot's side: a link fully
    consumed by tier 2 delivers no snapshots, and that is the ordered
    behaviour rather than starvation to apologize for. The second half is
    what stops this being an instrument that cannot fail — once the live
    traffic stops, a snapshot *is* pulled."""

    async def scenario():
        fake_ros = FakeRos()
        # Wide enough that the stream below never runs dry mid-test; the
        # default FakeRos queue holds 10 and would empty in a blink.
        fake_ros.samples = SampleQueue(maxsize=2000)
        client = _offline_client(fake_ros)
        client._session_open = True
        ws = TimedWs(0.005)
        writer = client._build_writer(ws)
        loop = asyncio.get_event_loop()
        for i in range(2000):
            fake_ros.samples.put_threadsafe(loop, Sample(slug="live", value=i, timestamp_ms=i))
        await _run_briefly(writer, 0.4)
        under_load = list(fake_ros.snapshot_max_bytes_asked)

        # Now the same writer with nothing above tier 5 to do at all.
        while fake_ros.samples.try_get() is not None:
            pass
        await _run_briefly(writer, 0.6)
        return under_load, fake_ros.snapshot_max_bytes_asked

    under_load, afterwards = asyncio.run(scenario())
    assert under_load == []
    assert afterwards, "no snapshot was pulled even with every tier above it idle"
