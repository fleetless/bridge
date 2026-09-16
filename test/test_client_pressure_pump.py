# SPDX-License-Identifier: Apache-2.0
"""The pressure pump (new in bridge 2.2.0): a `bridge-pressure` datapoint
every `PRESSURE_INTERVAL_S`, plus a per-session tier-2 `high_water`
reading. `SampleQueue`/`CameraStateQueue` outlive the session, so
`high_water()` used to leak across reconnects — this keeps it per-session.

`pressure_wire_value` is pure and tested directly against the vendored
schema; the pump itself is observed only on the wire, via `FakeCloud` —
same discipline as `test_client_writer_wiring.py`.
"""
import asyncio

from fake_cloud import FakeCloud, sequence
from helpers import RecordingSleep, make_client, run_until
from schemas import validate_frame
from test_client_datapoints import FakeRos

import fleetless_bridge.client as client_module
from fleetless_bridge.client import (
    PRESSURE_INTERVAL_S,
    PRESSURE_SLUG,
    BridgeClient,
    pressure_wire_value,
)
from fleetless_bridge.config import BridgeConfig
from fleetless_bridge.ros_runtime import CameraStateQueue, CameraStateUpdate, SampleQueue
from fleetless_bridge.sampling import Sample


def _stats_fixture() -> dict:
    """A `pressure_stats()`-shaped dict hitting every schema branch:
    non-null `rate_bps` and video numbers, complementing the all-zero/
    all-null shape `test_client_pressure_stats.py` already covers.
    `timestamp_ms` is included because `pressure_stats()` always carries
    it — `pressure_wire_value` is what drops it."""
    return {
        "timestamp_ms": 1786400000123,
        "tiers": {
            0: {"sent": 10, "bytes": 500, "drops": 0, "high_water": 2},
            1: {"sent": 3, "bytes": 900, "drops": 1, "high_water": 1},
            2: {"sent": 42, "bytes": 12000, "drops": 0, "high_water": 5},
            3: {"sent": 1, "bytes": 200, "drops": 0, "high_water": 1},
            4: {"sent": 0, "bytes": 0, "drops": 0, "high_water": 0},
            5: {"sent": 0, "bytes": 0, "drops": 0, "high_water": 0},
        },
        "rate_bps": 125000.5,
        "snapshot_max_bytes": 1_500_000,
        "video": {
            "active_streams": 1,
            "bitrate_sum_kbps": 500,
            "uplink_kbps": 2000,
            "override_kbps": None,
            "video_budget_kbps": 1600,
            "reserve_kbps": 400,
        },
    }


def _zeroed_stats_fixture() -> dict:
    """The same shape with the zeros a correctly-behaving bridge really
    emits — the input the contract used to refuse.

    `FLEETLESS_UPLINK_KBPS=0` means "no video budget at all", and
    `PressureBudget` passes that 0 straight through; `snapshot_max_bytes`
    derives from `rate_bps` and floors to 0 under half a byte/sec. Both
    fields were `.positive()` in the contract, so a zero here passed the
    robot's own validation but failed the console's `safeParse` —
    rendering a deliberately video-less or genuinely struggling robot as
    "bridge too old for pressure telemetry".

    Exists so this input reaches the vendored schema from the producer's
    side: re-tightening either bound reds here."""
    stats = _stats_fixture()
    stats["rate_bps"] = 0.0
    stats["snapshot_max_bytes"] = 0
    stats["video"] = {
        "active_streams": 0,
        "bitrate_sum_kbps": 0,
        "uplink_kbps": 0,
        "override_kbps": None,
        "video_budget_kbps": 0,
        "reserve_kbps": 0,
    }
    return stats


def test_the_wire_value_matches_the_vendored_schema():
    value = pressure_wire_value(_stats_fixture())
    validate_frame("bridge-pressure", value)
    # Asserted directly, not just implied by schema validity: a schema
    # that also accepted `int` keys, or made `timestamp_ms` merely
    # optional, would let a wrong projection pass.
    assert set(value["tiers"].keys()) == {"0", "1", "2", "3", "4", "5"}
    assert "timestamp_ms" not in value
    assert value["link"] == {"rate_bps": 125000.5, "snapshot_max_bytes": 1_500_000}


def test_the_zeros_a_real_bridge_sends_pass_the_vendored_schema():
    """`uplink_kbps: 0` ("no video") and `snapshot_max_bytes: 0` (a link
    under 0.5 B/s) are readings this bridge produces by working correctly,
    not malformed output — so the contract has to accept them."""
    value = pressure_wire_value(_zeroed_stats_fixture())
    validate_frame("bridge-pressure", value)
    # Asserted, not merely implied above: the console reads a zero as a
    # zero — `null` would mean something else ("not configured", not
    # "configured to nothing").
    assert value["video"]["uplink_kbps"] == 0
    assert value["link"]["snapshot_max_bytes"] == 0


def test_pressure_frames_flow_every_interval_while_connected():
    """FakeCloud + RecordingSleep, passed as `pressure_sleep` — not
    `sleep`, which paces the reconnect backoff and which `helpers.
    make_client` defaults to its own `RecordingSleep()` for every test in
    this suite. Sharing that default with the pump's cadence turned every
    pre-existing `ros`-connected test into a tier-2 flood starving
    `assets_available`/backfill/etc below it (see the constructor's own
    comment in `client.py`).

    Collects a handful of consecutive frames off the wire, each a valid
    `bridge-pressure` datapoint; the pump's `pressure_sleep` calls show
    the documented interval — not real time, since `RecordingSleep`
    never actually waits it out."""
    wanted = 4
    box = {}

    async def behavior(session):
        await session.recv_hello()
        await session.accept()
        box["frames"] = [await session.recv_datapoint() for _ in range(wanted)]
        await session.drain()

    sleep = RecordingSleep()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, pressure_sleep=sleep)
            await run_until(client, lambda: "frames" in box)

    asyncio.run(scenario())

    frames = box["frames"]
    assert len(frames) == wanted
    for frame in frames:
        assert frame["type"] == "datapoint"
        assert frame["slug"] == PRESSURE_SLUG
        validate_frame("bridge-pressure", frame["value"])
    assert sleep.delays.count(PRESSURE_INTERVAL_S) >= wanted - 1


def test_no_pressure_frame_before_hello_ok_or_after_disconnect():
    """Two sessions in a row. The first proves the pump is running, then
    disconnects mid-cycle. The second's hello is answered only after a
    real-time pause — long enough that a pump wired to the wrong lifetime
    (started too early, not torn down with its session, or reading
    `self._last_writer` instead of its own writer) would have put
    something on the wire by then, whether from session 2 racing its own
    `hello_ok` or session 1's pump never being cancelled. Silence across
    that pause is the claim."""
    box = {}

    async def first_session(session):
        await session.recv_hello()
        await session.accept()
        frame = await session.recv_datapoint()
        assert frame["slug"] == PRESSURE_SLUG
        await session.close(1000, "")

    async def second_session(session):
        await session.recv_hello()
        try:
            box["leaked"] = await asyncio.wait_for(session.recv_raw(), timeout=0.3)
        except asyncio.TimeoutError:
            box["leaked"] = None
        await session.accept()
        frame = await session.recv_datapoint()
        assert frame["slug"] == PRESSURE_SLUG
        box["done"] = True
        await session.drain()

    async def scenario():
        behavior = sequence(first_session, second_session)
        async with FakeCloud(behavior) as cloud:
            client = make_client(
                cloud, sleep=RecordingSleep(), pressure_sleep=RecordingSleep()
            )
            await run_until(client, lambda: box.get("done"))

    asyncio.run(scenario())
    assert box["leaked"] is None, box["leaked"]


def test_a_duplicate_hello_ok_still_runs_only_one_pump():
    """I1: a well-behaved cloud never sends two `hello_ok`s in one
    session, but nothing here assumes that. Before the guard, the second
    `hello_ok` overwrote `pressure_task` with a fresh `ensure_future`,
    leaking the first — never cancelled, its `while True` loop keeps
    calling `writer.enqueue()` on the same writer forever (both tasks
    close over the identical `writer` local from the same `_converse`
    call).

    `writer.run()` *is* cancelled cleanly at session end, so the leak is
    invisible on the wire from there — nothing drains the deque the
    leaked task keeps appending to. So the claim is made where the leak
    actually lives: the writer's tier-2 push deque must stop growing once
    the session is over, not just stop being *sent* — checked over a real
    wait window with `pressure_sleep` left fast, so a leftover task gets
    every chance to give itself away."""
    box = {}

    async def behavior(session):
        await session.recv_hello()
        await session.accept()
        await session.accept()  # the duplicate hello_ok
        frame = await session.recv_datapoint()
        assert frame["slug"] == PRESSURE_SLUG
        box["done"] = True
        await session.drain()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, pressure_sleep=RecordingSleep())
            await run_until(client, lambda: box.get("done"))
            writer = client._last_writer
            depth_1 = len(writer._push.get(client_module._TIER_TELEMETRY, ()))
            await asyncio.sleep(0.05)
            depth_2 = len(writer._push.get(client_module._TIER_TELEMETRY, ()))
        return depth_1, depth_2

    depth_1, depth_2 = asyncio.run(scenario())
    assert depth_2 == depth_1, (depth_1, depth_2)


def test_disconnecting_drains_both_queues_high_water_exactly_once():
    """I3: `_converse`'s `finally` is the one call site for both
    `SampleQueue.drain_high_water()` and `CameraStateQueue.
    drain_high_water()`. The camera-state half is exercised through real
    backlog semantics by `test_a_camera_state_backlog_queued_while_
    disconnected_shows_up_on_reconnect` below, but nothing else in this
    suite calls the sample-side one — deleting that call site would leave
    the suite green. Two thin subclasses count calls and delegate to the
    real queue otherwise, so the writer's ordinary tier-2 machinery
    (`try_next`, `high_water`, `drain_drop_count`) works exactly as it
    would against the genuine classes."""

    class _CountingSampleQueue(SampleQueue):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.drain_high_water_calls = 0

        def drain_high_water(self):
            self.drain_high_water_calls += 1
            return super().drain_high_water()

    class _CountingCameraStateQueue(CameraStateQueue):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.drain_high_water_calls = 0

        def drain_high_water(self):
            self.drain_high_water_calls += 1
            return super().drain_high_water()

    fake_ros = FakeRos()
    fake_ros.samples = _CountingSampleQueue(maxsize=10)
    fake_ros.camera_states = _CountingCameraStateQueue()
    box = {}

    async def behavior(session):
        await session.recv_hello()
        await session.accept()
        box["accepted"] = True
        await session.drain()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros, sleep=RecordingSleep())
            # `run_until` stops the moment the condition holds — one
            # session, never a retry, so a second `hello_ok` (and drain)
            # has no chance to happen here.
            await run_until(client, lambda: box.get("accepted"))

    asyncio.run(scenario())
    assert fake_ros.samples.drain_high_water_calls == 1
    assert fake_ros.camera_states.drain_high_water_calls == 1


def test_a_camera_state_backlog_still_queued_at_session_end_shows_up_on_reconnect():
    """I2, the discriminating case. `test_a_reconnect_resets_tier2_
    high_water` below waits long enough for the writer to drain its own
    backlog before session 1 closes, so "reset to 0" and "reset to
    current depth" both read 0 there — indistinguishable. An earlier
    version of *this* test tried pushing the backlog into the *gap*
    between sessions instead; that doesn't work either, because
    `CameraStateQueue.put()` recomputes its own running max on every call
    regardless of what a prior reset seeded — a fresh push after a wrong
    reset-to-0 silently repairs the number before session 2 reads it
    (confirmed by running that version against a reverted
    `drain_high_water` and watching it stay green).

    So: session 1 closes the *instant* its backlog is queued, before the
    writer has any real chance to drain it, and nothing pushes anything
    new after. The five items are still genuinely sitting in
    `CameraStateQueue._order` when `_converse`'s `finally` runs — reset to
    0 unconditionally would report a calm robot on reconnect, uncorrected;
    reset to `len(self._order)` (shipped) reports the real, still-queued
    backlog. Session 2's first pressure frame is where that has to show
    up."""
    fake_ros = FakeRos()
    box = {}
    backlog_slugs = tuple("stuck{}".format(i) for i in range(5))

    async def first_session(session):
        await session.recv_hello()
        # No `accept()` at all: `hello_ok` never arrives, so
        # `self._session_open` never becomes True and `_GatedSource`
        # keeps tier 2's pull side shut for this session's entire life —
        # the writer cannot drain even one of these five no matter how
        # many scans it gets. A "push then immediately close" version
        # (tried first) couldn't guarantee that: the close handshake is
        # async and gave the writer just enough of a window to send one
        # item before session 1 ended, making the carried-over depth
        # timing-dependent instead of the deterministic 5 this version
        # gets by construction.
        for slug in backlog_slugs:
            fake_ros.camera_states.put(
                CameraStateUpdate(
                    slug, True, None, cause="source",
                    observed_at_ms=1786400000000, request_id=None,
                )
            )
        await session.close(1000, "")

    async def second_session(session):
        await session.recv_hello()
        await session.accept()
        # The writer may drain some (or all) of the backlog itself first —
        # correct behaviour, not a race to work around:
        # `_CameraStateSource.try_next()` calls `record_high_water` before
        # checking whether there's an item to return, so the pressure
        # reading is unaffected by how many real `camera_state` frames
        # precede it on the wire. Skip past those to find the first
        # `datapoint`.
        frame = await session.recv()
        while frame.get("type") == "camera_state":
            frame = await session.recv()
        assert frame["type"] == "datapoint", frame
        box["first_frame"] = frame
        box["done"] = True
        await session.drain()

    async def paced_sleep(seconds: float) -> None:
        # A small *real* delay, not `RecordingSleep`: the writer's scan of
        # tier 2's pull sources (which calls `record_high_water`, folding
        # this backlog into the counters `pressure_stats()` reads) has to
        # win a scheduling race against the pump's own first `await` if
        # that resolves instantly — real time gives the writer's task a
        # realistic chance to scan first, same reasoning
        # `test_a_reconnect_resets_tier2_high_water` documents for its own
        # `paced_sleep`.
        await asyncio.sleep(0.03)

    async def scenario():
        behavior = sequence(first_session, second_session)
        async with FakeCloud(behavior) as cloud:
            client = make_client(
                cloud, ros=fake_ros, sleep=RecordingSleep(), pressure_sleep=paced_sleep
            )
            await run_until(client, lambda: box.get("done"))

    asyncio.run(scenario())

    frame = box["first_frame"]
    assert frame["slug"] == PRESSURE_SLUG, frame
    assert frame["value"]["tiers"]["2"]["high_water"] == len(backlog_slugs), (
        frame["value"]["tiers"]["2"]
    )


def test_a_reconnect_resets_tier2_high_water():
    """`SampleQueue`/`CameraStateQueue` outlive the session — `_build_writer`
    has to baseline them at construction, or a peak from session 1 keeps
    reading back on every session after. Nine distinct slugs (latest-
    per-slug would otherwise coalesce same-slug pushes down to a
    high-water of 1) push `CameraStateQueue` to 9 before session 1 closes
    — far past anything the pump's own tier-2 push side can produce on its
    own (see below), so a leaked mark is unmistakable.

    Passed as `pressure_sleep`, paired with a `RecordingSleep()` for the
    ordinary `sleep` (backoff) so the reconnect still happens promptly. A
    small *real* sleep, not `RecordingSleep`, for the pump: its instant,
    un-paced yield lets the pump outrun the writer's actual send over the
    loopback socket, backing up its own push-side tier-2 deque —
    `enqueue()` folds that depth into the same `high_water` counter this
    test reads. A single frame in flight at a time (which paced sends
    give it) can only ever bump that counter to 1, so this test's bound
    (< 9, comfortably above that) still catches a leak while tolerating
    the pump's own ordinary self-bump. 30 ms is far more than a small
    JSON frame needs to clear a local socket, established by this suite's
    own `TimedWs`/`asyncio.sleep` pacing elsewhere. Session 1 also waits a
    real beat before closing, so the writer has time to drain the nine
    queued camera-state updates instead of leaving them to surface as
    stray `camera_state` frames once session 2 starts.

    Does not, on its own, tell "reset to 0" apart from "reset to current
    depth" (I2): session 1 drains its own backlog to empty before
    closing, so both read 0 here. `test_a_camera_state_backlog_
    still_queued_at_session_end_shows_up_on_reconnect` above tells them
    apart, by never letting a session drain the backlog at all."""
    fake_ros = FakeRos()
    box = {}
    seed_slugs = tuple("cam{}".format(i) for i in range(9))

    async def paced_sleep(seconds: float) -> None:
        await asyncio.sleep(0.03)

    async def first_session(session):
        await session.recv_hello()
        await session.accept()
        for slug in seed_slugs:
            fake_ros.camera_states.put(
                CameraStateUpdate(
                    slug, True, None, cause="source",
                    observed_at_ms=1786400000000, request_id=None,
                )
            )
        assert fake_ros.camera_states.high_water() == len(seed_slugs)
        await asyncio.sleep(0.3)  # let the writer drain them
        await session.close(1000, "")

    async def second_session(session):
        await session.recv_hello()
        await session.accept()
        box["frames"] = [await session.recv_datapoint() for _ in range(5)]
        box["done"] = True
        await session.drain()

    async def scenario():
        behavior = sequence(first_session, second_session)
        async with FakeCloud(behavior) as cloud:
            client = make_client(
                cloud, ros=fake_ros, sleep=RecordingSleep(), pressure_sleep=paced_sleep
            )
            await run_until(client, lambda: box.get("done"))

    asyncio.run(scenario())

    for frame in box["frames"]:
        assert frame["slug"] == PRESSURE_SLUG, frame
    high_waters = [frame["value"]["tiers"]["2"]["high_water"] for frame in box["frames"]]
    assert high_waters[0] == 0, high_waters
    assert max(high_waters) < len(seed_slugs), high_waters


# --- drain_high_water: read-and-reset-to-current-depth, unit level ------------


def test_camera_state_queue_drain_high_water_reads_the_peak_and_resets_to_current_depth():
    """I3, direct: the same read-and-reset shape `CameraStateQueue.
    drain_drop_count` already has (test_ros_runtime.py's precedent) —
    except the value it resets *to* is `len(self._order)`, not 0 (I2).
    Three uncoalescible pushes reach a peak of 3; popping one drops the
    *current* depth to 2 without touching the peak (`high_water` only
    moves on `put`) — so a drain that returns 3 and then resets to 2
    proves both halves at once: the old peak was read, and the reset
    value is the real depth now, neither 0 nor the stale peak."""
    q = CameraStateQueue()
    for i, slug in enumerate(("a", "b", "c")):
        q.put(
            CameraStateUpdate(
                slug, True, None, cause="command",
                observed_at_ms=i, request_id="req-{}".format(i),
            )
        )
    assert q.high_water() == 3
    q.try_get()  # one item leaves; the peak does not move for a pop
    assert q.drain_high_water() == 3
    assert q.high_water() == 2, "reset to the current depth (2), not 0 and not the old peak (3)"


def test_sample_queue_drain_high_water_reads_the_peak_and_resets_to_current_depth():
    """I3, direct: `SampleQueue`'s half of the same claim. `put_threadsafe`
    is the only public writer and needs a running loop, though nothing
    here is actually cross-thread — matching how the rest of this suite
    seeds a `SampleQueue` (`test_client_writer_wiring.py`'s own pattern)."""

    async def scenario():
        q = SampleQueue(maxsize=10)
        loop = asyncio.get_event_loop()
        for i in range(3):
            q.put_threadsafe(loop, Sample(slug="s", value=i, timestamp_ms=i))
        await asyncio.sleep(0)  # let call_soon_threadsafe land
        assert q.high_water() == 3
        q.try_get()  # depth drops to 2; the peak does not move for a pop
        drained = q.drain_high_water()
        return drained, q.high_water()

    drained, reset_to = asyncio.run(scenario())
    assert drained == 3
    assert reset_to == 2, "reset to the current depth (2), not 0 and not the old peak (3)"


def test_a_pump_that_died_of_a_real_exception_is_logged_at_warning(monkeypatch):
    """A pump that raised is gone, and the session carries on without it —
    for the pressure pump that's precisely the state the console renders
    as "no pressure feed", i.e. as a bridge too old to have the feature.
    At `debug` the robot's own log couldn't tell the two apart either.

    Not `caplog`: this suite has already established (see
    `test_client_writer.py`'s own note, `test_client_datapoints.py`'s
    `_RecordingLog`) that `caplog` doesn't reliably see records from this
    module — a `caplog`-only assertion would be an instrument that cannot
    fail. Monkeypatch the module-level `log` instead.
    """

    class RecordingLog:
        def __init__(self):
            self.warnings = []
            self.debugs = []

        def warning(self, fmt, *args, **kwargs):
            self.warnings.append(fmt % args if args else fmt)

        def debug(self, fmt, *args, **kwargs):
            self.debugs.append(fmt % args if args else fmt)

        def __getattr__(self, name):
            return lambda *a, **kw: None

    recording_log = RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    # No FakeCloud and no `run()`: `_cancel_pump` needs neither a socket
    # nor a session, and giving it one would put a whole connection between
    # the defect and the assertion.
    client = BridgeClient(BridgeConfig(token="frt_test_token", cloud_url="ws://127.0.0.1:1/bridge"))

    async def scenario():
        async def dies():
            raise RuntimeError("the counters went away")

        died = asyncio.ensure_future(dies())
        # Let it raise before the cancel lands: the "died on its own"
        # path, not the ordinary CancelledError one.
        await asyncio.sleep(0)
        await client._cancel_pump(died, "pressure")

        # The ordinary end of every session: cancelled, not dead. It must
        # stay silent, or the warning stops meaning anything.
        async def forever():
            await asyncio.sleep(3600)

        running = asyncio.ensure_future(forever())
        await asyncio.sleep(0)
        await client._cancel_pump(running, "pressure")

    asyncio.run(scenario())

    assert len(recording_log.warnings) == 1, recording_log.warnings
    message = recording_log.warnings[0]
    assert "pressure" in message  # names WHICH pump, not just "a pump"
    assert "died" in message
    # The cancel path left nothing behind at any level.
    assert recording_log.debugs == []
