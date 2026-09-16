# SPDX-License-Identifier: Apache-2.0
"""Live video against the uplink budget:
`start_live`'s admission check runs before any join attempt, the live
`/fleetless/uplink_kbps` topic overrides the env-configured budget at
runtime, and a budget lowered below the running sum stops the most
recently started stream first, repeatedly, until the sum fits.

**`cause` stays the wire's closed enum; `error.code` is the free channel.**
An early version of this file invented `cause:
"uplink_budget"`, which is not a value `bridgeCameraState.cause` accepts —
the wire contracts and the vendored copy at `test/contracts/
schema*/bridge-camera-state.schema.json` both fix it to `z.enum(['command',
'source', 'config_change', 'live_lost'])`, and the cloud's own
`parseBridgeFrame` silently drops any frame that fails validation (logged
warn, returns null) — so that cause would have made every one of these
frames vanish in production. The corrected shape: an admission refusal
answers *this* `camera_start` (`cause: 'command'`, `request_id` echoed —
the contracts cross-field rule requires exactly that pairing), and an
enforcement stop is unsolicited (`cause: 'live_lost'`, `request_id: None`,
the same wire case an unexpected LiveKit disconnect uses). Both carry *why*
in `error.code`, the one field the schema leaves as a plain
`z.string().min(1)`.

A later fix closed a cross-slug admission race: admission read
`active_sum` from `_live_publishers` alone, which only gains an entry
*after* a join completes — so two concurrent `start_live` calls for two
different slugs (client.py dispatches `camera_start` fire-and-forget, and
the per-slug lock never serializes across slugs) could both read the same
stale, pre-join sum and both admit, jointly exceeding the budget.
`test_two_concurrent_starts_for_different_slugs_only_admit_one` reproduces
it directly, using `_fake_live_factory`'s `start_delay` to hold the first
join open past the point where the second slug's own admission check runs.

Two more interleaving bugs live in `_enforce_uplink_budget` itself, and
reproducing either needs precise control over when a join commits or a
disconnect resolves relative to the enforcement loop's own progress —
sleep-based delays could not reproduce them deterministically, so
`_GatedLivePublisher`/`_gated_factory` (below) block on an explicit
`asyncio.Event` instead: `test_a_joiner_committing_mid_stop_does_not_fool_
the_progress_guard` (a count-based progress guard, fooled by an
unrelated commit landing during the same `stop_live` await) and
`test_a_larger_budget_applied_mid_pass_stops_the_stale_pass_from_over_
stopping` (`video_budget_kbps` read once before the loop instead of
fresh every iteration, so a later, larger override applied by a second
overlapping call went unseen).

Reuses `test_ros_runtime.py`'s harness (`run`, `_camera_cfg`,
`_fake_live_factory`, `_drain_camera_states`, `_await_camera_state`) rather
than duplicating it, same as `schemas.py`/`fake_cloud.py` already get
imported across files for. Every test here is a real, spinning `RosRuntime`
(see `test_ros_runtime.py`'s module docstring for why it skips conftest.py's
`ros`/`spun_node` fixtures), with a fake LiveKit publisher standing in for
`live.LivePublisher`.
"""
import asyncio
import json

import rclpy
from conftest import wait_until
from helpers import by_slug
from std_msgs.msg import UInt32

from fleetless_bridge.pressure import UplinkBudget
from fleetless_bridge.protocol import bridge_camera_state_message
from schemas import validate_frame
from test_ros_runtime import (
    _await_camera_state,
    _camera_cfg,
    _drain_camera_states,
    _fake_live_factory,
    run,
)


def _publish_uplink_kbps(rt, kbps):
    """A real publisher against `/fleetless/uplink_kbps`, the topic
    `RosRuntime.start()` subscribes to unconditionally — same discovery-wait
    pattern `test_ros_runtime.py`'s datapoint-publishing helpers use
    (`wait_until(lambda: pub.get_subscription_count() > 0)`): publish before
    DDS discovery finishes and the message is lost before `_on_uplink_kbps`
    ever sees it."""
    pub_node = rclpy.create_node("test_uplink_publisher_{}".format(id(rt)))
    try:
        pub = pub_node.create_publisher(UInt32, "/fleetless/uplink_kbps", 10)
        wait_until(lambda: pub.get_subscription_count() > 0)
        pub.publish(UInt32(data=kbps))
    finally:
        pub_node.destroy_node()


def _validate_camera_state_wire_shape(state):
    """Rebuilds the wire frame `bridge_camera_state_message` would send for
    this `CameraStateUpdate` and schema-validates it — so an invalid
    `cause` (the defect an early version of this file shipped, see this
    module's docstring) cannot ship silently again.

    Called from both frames this feature *mints*: the admission refusal
    (`cause: 'command'`, `request_id` echoed), where that defect actually
    lived, and the enforcement stop (`cause: 'live_lost'`,
    `request_id: None`), a separately constructed `CameraStateUpdate`
    covered elsewhere only by field assertions — those check the expected
    value, not that the wire accepts it, which is the gap this closes."""
    payload = json.loads(
        bridge_camera_state_message(
            state.slug,
            state.publishing,
            state.error,
            cause=state.cause,
            observed_at_ms=state.observed_at_ms,
            request_id=state.request_id,
        )
    )
    validate_frame("bridge-camera-state", payload)


class _GatedLivePublisher:
    """A fake `live.LivePublisher` whose `start()`/`stop()` block on an
    explicit `asyncio.Event` the test controls, not a fixed sleep duration
    (`test_ros_runtime._FakeLivePublisher`'s `start_delay`/`stop_delay`) —
    for a regression test that needs to freeze a join or disconnect at an
    *exact* point and resume it on cue, with no timing assumption to tune
    or flake on.

    `on_start_blocked`/`on_stop_blocked` (`asyncio.Event`s the *test*
    awaits, distinct from `start_gate`/`stop_gate`, which the *coroutine
    here* awaits) are set the instant this publisher is about to block —
    the only certain way to know "the coroutine under test has reached the
    gate", instead of guessing it with a fixed number of
    `asyncio.sleep(0)` turns.

    `rt._live_publisher_factory` is reassigned between `start_live` calls
    in these tests (same reassign-mid-test pattern
    `test_ros_runtime._retarget_cameras_and_join` uses for
    `rt._destroy_camera`), so each publisher gets its own independent
    gates."""

    def __init__(
        self, *, holder, width, height, fps, bitrate_kbps, on_lost=None,
        start_gate=None, stop_gate=None, on_start_blocked=None, on_stop_blocked=None,
    ):
        self.bitrate_kbps = bitrate_kbps
        self.on_lost = on_lost
        self.start_calls = []
        self.stop_calls = 0
        self._start_gate = start_gate
        self._stop_gate = stop_gate
        self._on_start_blocked = on_start_blocked
        self._on_stop_blocked = on_stop_blocked

    async def start(self, url, room, token):
        self.start_calls.append((url, room, token))
        if self._start_gate is not None:
            if self._on_start_blocked is not None:
                self._on_start_blocked.set()
            await self._start_gate.wait()

    async def stop(self):
        if self._stop_gate is not None:
            if self._on_stop_blocked is not None:
                self._on_stop_blocked.set()
            await self._stop_gate.wait()
        self.stop_calls += 1


def _gated_factory(*, start_gate=None, stop_gate=None, on_start_blocked=None, on_stop_blocked=None):
    def factory(**kwargs):
        return _GatedLivePublisher(
            start_gate=start_gate, stop_gate=stop_gate,
            on_start_blocked=on_start_blocked, on_stop_blocked=on_stop_blocked,
            **kwargs,
        )

    return factory


def test_a_start_over_budget_is_refused_with_uplink_budget_cause():
    """UplinkBudget(2000): reserve = max(20% of 2000, 128) = 400, so the
    video budget is 1600 kbps. A 2500 kbps camera_start, with nothing else
    live, cannot fit — refused before any join attempt. This *answers* the
    camera_start (`cause: 'command'`, echoing its `request_id` — the wire's
    own closed enum has no `uplink_budget` member, see this module's
    docstring); the reason lives in `error.code`, with a message naming the
    requested/available/budget numbers (the format's own example sentence
    shape)."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=2500)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    states = run(
        body,
        uplink_budget=UplinkBudget(2000),
        live_publisher_factory=_fake_live_factory(),
    )
    assert len(states) == 1
    assert states[0].slug == "front"
    assert states[0].publishing is False
    assert states[0].cause == "command"
    assert states[0].request_id == "req-1"
    assert states[0].error == (
        "uplink_budget",
        "2500 kbps requested, 1600 available of a 1600 kbps budget",
    )
    _validate_camera_state_wire_shape(states[0])


def test_admission_counts_only_streams_actually_publishing():
    """`side` is configured with a bitrate that would blow the budget on its
    own (5000 kbps) but is never started — it must not count toward
    `active_sum`. Only `front`, which actually holds a live publisher, may
    count. Budget 2000 -> video budget 1600: front (1000) admits on its
    own; back (500) must then also admit, since the correct active_sum at
    that point is front's 1000, not front+side's 6000."""

    async def body(rt):
        await rt.apply_cameras(by_slug(
            [
                _camera_cfg("front", bitrate_kbps=1000),
                _camera_cfg("side", bitrate_kbps=5000),
                _camera_cfg("back", bitrate_kbps=500),
            ]
        ))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-front")
        await _drain_camera_states(rt)  # front's own admitted-and-started state
        await rt.start_live("back", "wss://media.example", "room-1", "tok-back", "req-back")
        return await _drain_camera_states(rt)

    states = run(
        body,
        uplink_budget=UplinkBudget(2000),
        live_publisher_factory=_fake_live_factory(),
    )
    assert len(states) == 1
    assert states[0].slug == "back"
    assert states[0].publishing is True
    assert states[0].error is None


def test_two_concurrent_starts_for_different_slugs_only_admit_one():
    """Admission must not read `active_sum` from `_live_publishers` alone.
    That set only gains an entry *after* `await
    publisher.start(...)` returns. `camera_start` dispatches are
    fire-and-forget (client.py's `_dispatch_camera_start`, `ensure_
    future`), and `_live_lock` only serializes *within* one slug — so two
    concurrent `start_live` calls for two *different* slugs could both
    read the same stale, pre-join sum and both admit, jointly exceeding
    the budget the second one was supposed to be checked against.

    UplinkBudget(1300): reserve = max(20% of 1300, 128) = 260, video
    budget 1040. Two 1000 kbps streams together (2000) do not fit; one
    alone (1000) does. `front`'s join is held open (`start_delay`) past
    the point where `back`'s own admission check runs — reproducing the
    exact interleaving `asyncio.gather` on two fire-and-forget dispatches
    would produce. With reserve-then-commit, `back` sees `front`'s
    *pending* reservation (1000) even though `front` has not committed
    yet, and is correctly refused rather than jointly admitted."""

    async def body(rt):
        await rt.apply_cameras(by_slug(
            [_camera_cfg("front", bitrate_kbps=1000), _camera_cfg("back", bitrate_kbps=1000)]
        ))
        await asyncio.gather(
            rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-front"),
            rt.start_live("back", "wss://media.example", "room-1", "tok-back", "req-back"),
        )
        return await _drain_camera_states(rt)

    states = run(
        body,
        uplink_budget=UplinkBudget(1300),
        live_publisher_factory=_fake_live_factory(start_delay=0.1),
    )
    states_by_slug = {state.slug: state for state in states}
    assert set(states_by_slug) == {"front", "back"}

    assert states_by_slug["front"].publishing is True
    assert states_by_slug["front"].error is None

    assert states_by_slug["back"].publishing is False
    assert states_by_slug["back"].cause == "command"
    assert states_by_slug["back"].request_id == "req-back"
    assert states_by_slug["back"].error == (
        "uplink_budget",
        "1000 kbps requested, 40 available of a 1040 kbps budget",
    )


def test_a_joiner_committing_mid_stop_does_not_fool_the_progress_guard():
    """The old progress guard compared `len(self._live_publishers)`
    before and after a `stop_live` call — a **count**,
    not a check that the *targeted* slug was actually removed. `front` and
    `back` are already live (`back` started second, so it is "newest").
    `mid` is a third camera whose `start_live` is in flight (admitted,
    reserved, gated open on `start_gate`) when a lowered budget triggers
    enforcement to stop `back`. `back`'s own `stop()` is gated on
    `stop_gate`; while it is pending, `mid`'s join is released and commits
    — `_live_publishers` gains `mid` at essentially the same moment `back`
    is removed, so the *count* is unchanged even though `back` genuinely
    stopped. The old guard read that as "no progress" and returned early,
    leaving `front` (500) + `mid` (500) = 1000 kbps live against a 560 kbps
    budget. The fix (`if newest_slug in self._live_publishers: return`)
    checks `back` specifically, sees it gone, and keeps enforcing — `mid`,
    now the newest *committed* stream, is stopped next, converging on
    `front` alone (500 <= 560): the budget is actually met, not merely
    counted as met.

    Every handoff below is event-controlled (`arrived.wait()`, `await
    <task>`), not a fixed number of `asyncio.sleep(0)` turns — precise
    regardless of how many loop iterations a given suspension actually
    costs."""

    async def body(rt):
        await rt.apply_cameras(by_slug(
            [
                _camera_cfg("front", bitrate_kbps=500),
                _camera_cfg("back", bitrate_kbps=500),
                _camera_cfg("mid", bitrate_kbps=500),
            ]
        ))
        stop_gate = asyncio.Event()
        back_blocked = asyncio.Event()
        start_gate = asyncio.Event()
        mid_blocked = asyncio.Event()

        # front: plain, instant.
        rt._live_publisher_factory = _gated_factory()
        await rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-front")

        # back: instant start, but its stop is gated. The factory swap
        # must happen before this start_live call — `_stop_gate` is fixed
        # once `_live_publisher_factory` builds the publisher;
        # reassigning it later only affects publishers built afterward.
        rt._live_publisher_factory = _gated_factory(
            stop_gate=stop_gate, on_stop_blocked=back_blocked
        )
        await rt.start_live("back", "wss://media.example", "room-1", "tok-back", "req-back")
        await _drain_camera_states(rt)  # front's and back's own start states

        # `mid`'s own join is gated open (holds it "in flight" — reserved
        # but not yet committed) until `start_gate.set()` below.
        rt._live_publisher_factory = _gated_factory(
            start_gate=start_gate, on_start_blocked=mid_blocked
        )
        mid_task = asyncio.ensure_future(
            rt.start_live("mid", "wss://media.example", "room-1", "tok-mid", "req-mid")
        )
        await mid_blocked.wait()  # mid has been admitted, reserved, and is now blocked pre-join

        # `back` is the one enforcement will pick (newest committed) — its
        # disconnect was gated when its publisher was created, above.
        enforce_task = asyncio.ensure_future(rt._enforce_uplink_budget(700))
        await back_blocked.wait()  # enforcement picked "back" (newest) and is now blocked mid-stop

        # Release mid's join before back's stop resolves, and wait for it
        # to fully commit — `_live_publishers` gains "mid" while "back" is
        # still present.
        start_gate.set()
        await mid_task

        # Now release back's stop — it is removed, at the same instant the
        # dict's size (with mid already added) looks unchanged from
        # `before_count`.
        stop_gate.set()
        await enforce_task

        await _drain_camera_states(rt)
        # Computed here, inside `body`, not after `run()` returns: `rt.stop()`
        # (called by the harness's own `finally`, after `body` returns but
        # before `run()`'s caller sees the result) clears `self._cameras`,
        # so `rt._cameras[slug].bitrate_kbps` would raise `KeyError` if read
        # afterward — unlike `_live_publishers`, which survives teardown
        # untouched (the existing membership assertions below rely on that).
        committed_kbps = sum(rt._cameras[s].bitrate_kbps for s in rt._live_publishers)
        return rt, committed_kbps

    rt, committed_kbps = run(
        body,
        uplink_budget=UplinkBudget(2000),
        live_publisher_factory=_gated_factory(),
    )
    assert "front" in rt._live_publishers
    assert "back" not in rt._live_publishers
    assert "mid" not in rt._live_publishers
    assert committed_kbps <= 560  # the budget is actually met, not merely counted as met


def test_a_larger_budget_applied_mid_pass_stops_the_stale_pass_from_over_stopping():
    """`video_budget_kbps` used to be read once, before the loop, and
    reused across every iteration — including across the
    `await stop_live(...)` inside it. `front`, `back` and `third` are all
    live (`third` started last, so it is "newest"). Enforcement pass A
    (`_enforce_uplink_budget(700)`, video budget 560) stops `third` first
    — its own disconnect is gated open. While A is mid-stop, pass B
    (`_enforce_uplink_budget(5000)`, video budget 4000) runs to completion
    immediately: `front` + `back` + `third` (1500, `third` not yet popped)
    already fits comfortably under 4000, so B stops nothing and simply
    applies the larger override. Once `third`'s stop is released, A's old
    behaviour would keep comparing against its own stale, pre-read 560 and
    go on to stop `back` too (1000 > 560) — needlessly, since the *current*
    budget (4000, applied by B) already admits 1000. The fix re-reads
    `video_budget_kbps()` at the top of A's next iteration, sees 4000, and
    stops enforcing: only `third` is ever stopped.

    Event-controlled, not sleep-based: `third_blocked.wait()` proves A has
    already selected "third" and is blocked mid-stop before B ever runs;
    `await task_b` proves B has fully applied its override (B needs no gate
    of its own — it never blocks at all, since 1500 already fits 4000)."""

    async def body(rt):
        await rt.apply_cameras(by_slug(
            [
                _camera_cfg("front", bitrate_kbps=500),
                _camera_cfg("back", bitrate_kbps=500),
                _camera_cfg("third", bitrate_kbps=500),
            ]
        ))
        rt._live_publisher_factory = _gated_factory()
        await rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-front")
        await rt.start_live("back", "wss://media.example", "room-1", "tok-back", "req-back")

        stop_gate = asyncio.Event()
        third_blocked = asyncio.Event()
        rt._live_publisher_factory = _gated_factory(
            stop_gate=stop_gate, on_stop_blocked=third_blocked
        )
        await rt.start_live("third", "wss://media.example", "room-1", "tok-third", "req-third")
        await _drain_camera_states(rt)  # front's, back's and third's own start states

        # Pass A: budget 560; picks "third" (newest), blocks on its gated
        # disconnect.
        task_a = asyncio.ensure_future(rt._enforce_uplink_budget(700))
        await third_blocked.wait()  # A has selected "third" and is now blocked mid-stop

        # Pass B: budget 4000, applied while A is still mid-stop. 1500
        # (all three still present, "third" not yet popped) already fits,
        # so B stops nothing and returns without ever blocking on anything.
        task_b = asyncio.ensure_future(rt._enforce_uplink_budget(5000))
        await task_b  # B has fully applied its override and returned

        stop_gate.set()
        await task_a

        await _drain_camera_states(rt)
        return rt

    rt = run(
        body,
        uplink_budget=UplinkBudget(3000),
        live_publisher_factory=_gated_factory(),
    )
    assert "front" in rt._live_publishers
    assert "back" in rt._live_publishers  # not over-stopped by A's stale 560 reading
    assert "third" not in rt._live_publishers


def test_topic_message_overrides_and_enforcement_stops_newest_first():
    """Unbudgeted at construction (env unset), so both starts succeed
    regardless of bitrate — then `/fleetless/uplink_kbps` publishes 700,
    live, overriding the env value ("last value wins"). Reserve =
    max(20% of 700, 128) = 140, so the video budget becomes 560: the
    running sum of two 500 kbps streams (1000) no longer fits, and exactly
    one must stop — the one started *second* ("back"), per the format's
    newest-first rule. "front", started first, survives. The stop answers
    no command (`cause: 'live_lost'`, `request_id: None`); the reason is in
    `error.code`."""

    async def body(rt):
        await rt.apply_cameras(by_slug(
            [_camera_cfg("front", bitrate_kbps=500), _camera_cfg("back", bitrate_kbps=500)]
        ))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-front")
        await rt.start_live("back", "wss://media.example", "room-1", "tok-back", "req-back")
        await _drain_camera_states(rt)  # both admitted-and-started states

        _publish_uplink_kbps(rt, 700)
        stopped = await _await_camera_state(rt, timeout=5.0)
        return stopped, rt

    stopped, rt = run(body, uplink_budget=UplinkBudget(None), live_publisher_factory=_fake_live_factory())
    _validate_camera_state_wire_shape(stopped)
    assert stopped.slug == "back"
    assert stopped.publishing is False
    assert stopped.cause == "live_lost"
    assert stopped.request_id is None
    assert stopped.error == (
        "uplink_budget",
        "500 kbps stopped: 1000 kbps running exceeds the 560 kbps budget",
    )
    assert "front" in rt._live_publishers
    assert "back" not in rt._live_publishers


def test_zero_stops_everything_and_admits_nothing():
    """The live topic publishing 0 means "no video at all":
    every running stream must stop (unsolicited, `cause: 'live_lost'`), and
    a subsequent camera_start must be refused (answering that command,
    `cause: 'command'`) — both with `error.code == 'uplink_budget'`."""

    async def body(rt):
        await rt.apply_cameras(by_slug(
            [_camera_cfg("front", bitrate_kbps=500), _camera_cfg("back", bitrate_kbps=500)]
        ))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-front", "req-front")
        await rt.start_live("back", "wss://media.example", "room-1", "tok-back", "req-back")
        await _drain_camera_states(rt)  # both admitted-and-started states

        _publish_uplink_kbps(rt, 0)
        first_stop = await _await_camera_state(rt, timeout=5.0)
        second_stop = await _await_camera_state(rt, timeout=5.0)

        await rt.start_live(
            "front", "wss://media.example", "room-1", "tok-retry", "req-retry"
        )
        refusal = await _await_camera_state(rt, timeout=5.0)
        return first_stop, second_stop, refusal

    first_stop, second_stop, refusal = run(
        body, uplink_budget=UplinkBudget(2000), live_publisher_factory=_fake_live_factory()
    )
    # Newest-first: "back" (started second) stops before "front".
    assert first_stop.slug == "back"
    assert first_stop.publishing is False
    assert first_stop.cause == "live_lost"
    assert first_stop.request_id is None
    assert first_stop.error == (
        "uplink_budget",
        "500 kbps stopped: 1000 kbps running exceeds the 0 kbps budget",
    )

    assert second_stop.slug == "front"
    assert second_stop.publishing is False
    assert second_stop.cause == "live_lost"
    assert second_stop.request_id is None
    assert second_stop.error == (
        "uplink_budget",
        "500 kbps stopped: 500 kbps running exceeds the 0 kbps budget",
    )

    assert refusal.slug == "front"
    assert refusal.publishing is False
    assert refusal.cause == "command"
    assert refusal.request_id == "req-retry"
    assert refusal.error == (
        "uplink_budget",
        "500 kbps requested, 0 available of a 0 kbps budget",
    )


def test_no_budget_means_no_enforcement_and_no_refusal():
    """`uplink_budget` omitted entirely (defaults to `None`, same as every
    RosRuntime built without opting in) — a bitrate that would blow any
    ordinary budget still admits, and publishing 0 to the live topic has no
    effect at all: `_on_uplink_kbps` no-ops on a `None` budget."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=999_999)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        states = await _drain_camera_states(rt)

        _publish_uplink_kbps(rt, 0)
        await asyncio.sleep(0.2)  # give a wrongly-scheduled enforcement a chance to show up
        after = await _drain_camera_states(rt)
        return states, after, rt

    states, after, rt = run(body, live_publisher_factory=_fake_live_factory())
    assert len(states) == 1
    assert states[0].publishing is True
    assert states[0].error is None
    assert after == []  # nothing was enforced, nothing was reported
    assert "front" in rt._live_publishers  # still live, never stopped
