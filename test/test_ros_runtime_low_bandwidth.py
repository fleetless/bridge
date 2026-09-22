# SPDX-License-Identifier: Apache-2.0
"""Low-bandwidth mode where it touches ROS: the eight parameters on a real
node, the datapoint cap over a real subscription, and the camera lever.

The decision itself lives in `test_link_mode.py` and the session wiring in
`test_client_link_mode.py`. What is left here is what only a real node can
answer — that `ros2 param set` reaches the validator, that a refused value
stays unset, and that a capped sample lands in the backlog rather than
nowhere.
"""
import asyncio
import json
import time

import pytest
import rclpy
from rclpy.parameter import Parameter
from sensor_msgs.msg import BatteryState
from test_ros_runtime import _battery, _camera_cfg, _drain_camera_states, run

from helpers import by_slug

from fleetless_bridge.link_mode import LowBandwidthSettings
from fleetless_bridge.sampling import AverageHzPolicy, MaxHzPolicy, rate_policy
from fleetless_bridge.protocol import (
    LOW_BANDWIDTH_DEFAULTS,
    DatapointConfig,
    DatapointNumeric,
    RetentionConfig,
)


def _dp(slug, *, rate_throttle_hz=None, keep=False, retention=None):
    return DatapointConfig(
        slug=slug,
        topic="/battery",
        type="sensor_msgs/msg/BatteryState",
        field="percentage",
        rate_throttle_hz=rate_throttle_hz,
        numeric=DatapointNumeric(),
        retention=retention or RetentionConfig(enabled=False),
        low_bandwidth_keep=keep,
    )


def _settings(**overrides):
    values = dict(LOW_BANDWIDTH_DEFAULTS)
    values.update(overrides)
    return LowBandwidthSettings.resolve(values, {})


def _publish(count):
    """`count` battery messages on `/battery` from a throwaway node, as fast
    as they go out — well inside one second, so a 1 Hz cap admits exactly the
    first of them.

    Returns the node rather than destroying it: the executor takes the
    samples asynchronously, and a witness on the bridge's own context loses
    one it has not taken yet when the publisher goes. Every caller here
    asserts an exact count, so the node is destroyed after the drain."""
    node = rclpy.create_node("test_lb_publisher")
    pub = node.create_publisher(BatteryState, "/battery", 10)
    for i in range(count):
        pub.publish(_battery(0.1 * i))
    return node


class _LivePublisher:
    """A stand-in for `live.LivePublisher` with the one method this file is
    about. Local rather than `test_ros_runtime.py`'s own fake: the camera
    lever is the only caller of `set_bitrate_kbps`, and a shared fake would
    put a method on every camera test that has no use for it."""

    instances = []
    #: Set by a test to hold `start()` open, so the mode can enter while a
    #: join is still in flight.
    gate = None

    def __init__(self, *, holder, width, height, fps, bitrate_kbps, on_lost=None):
        self.bitrate_kbps = bitrate_kbps
        self.on_lost = on_lost
        self.start_calls = []
        self.stop_calls = 0
        self.bitrate_calls = []
        _LivePublisher.instances.append(self)

    async def start(self, url, room, token):
        self.start_calls.append((url, room, token))
        if _LivePublisher.gate is not None:
            await _LivePublisher.gate.wait()

    async def stop(self):
        self.stop_calls += 1

    def set_bitrate_kbps(self, kbps):
        self.bitrate_calls.append(kbps)
        self.bitrate_kbps = kbps


def _live_factory():
    def factory(**kwargs):
        return _LivePublisher(**kwargs)

    return factory


async def _drain_samples(rt):
    await asyncio.sleep(0.3)
    out = []
    while True:
        sample = rt.samples.try_get()
        if sample is None:
            return out
        out.append(sample)


@pytest.fixture(autouse=True)
def _reset_publisher_instances():
    _LivePublisher.instances.clear()
    yield
    _LivePublisher.instances.clear()


# --- the parameters ---------------------------------------------------------


def test_parameters_are_declared_with_the_defaults():
    async def body(rt):
        return {
            key: rt._node.get_parameter("low_bandwidth." + key).value
            for key in LOW_BANDWIDTH_DEFAULTS
        }

    values = run(body)
    assert values["mode"] == "auto"
    assert values["enter_lag_ms"] == 2000
    assert values["camera"] == "reduce"
    # Declared as a double, not an integer: the vendored default is `1` and
    # ROS would take that literally, refusing `1.5` for the rest of the run.
    assert isinstance(values["datapoint_max_hz"], float)
    assert values["datapoint_max_hz"] == 1.0


def test_low_bandwidth_params_reads_all_eight_back():
    async def body(rt):
        return rt.low_bandwidth_params()

    values = run(body)
    assert set(values) == set(LOW_BANDWIDTH_DEFAULTS)
    assert LowBandwidthSettings.resolve(values, {}).mode == "auto"


def test_a_parameter_set_reaches_the_callback_with_the_whole_section():
    seen = []

    async def body(rt):
        rt.on_low_bandwidth_params(seen.append)
        result = rt._node.set_parameters(
            [Parameter("low_bandwidth.mode", Parameter.Type.STRING, "on")]
        )
        await asyncio.sleep(0.1)
        return result

    result = run(body)
    assert result[0].successful is True
    # The whole section, not only the key that moved: the client resolves
    # all eight against the YAML on top of it.
    assert len(seen) == 1
    assert seen[0]["mode"] == "on"
    assert seen[0]["enter_lag_ms"] == 2000


def test_a_value_the_settings_refuse_is_refused_with_the_same_sentence():
    seen = []

    async def body(rt):
        rt.on_low_bandwidth_params(seen.append)
        result = rt._node.set_parameters(
            [Parameter("low_bandwidth.enter_lag_ms", Parameter.Type.INTEGER, 50)]
        )
        await asyncio.sleep(0.1)
        return result, rt.low_bandwidth_params()["enter_lag_ms"]

    result, stored = run(body)
    assert result[0].successful is False
    assert result[0].reason == "low_bandwidth.enter_lag_ms must be an integer >= 100"
    assert stored == 2000
    assert seen == []


def test_a_parameter_of_someone_elses_never_reaches_the_callback():
    """`use_sim_time` is declared by rclpy itself and set by ordinary ROS
    tooling. A notification per unrelated parameter would re-resolve and
    re-apply the levers for a change that is none of this mode's business."""
    seen = []

    async def body(rt):
        rt.on_low_bandwidth_params(seen.append)
        rt._node.set_parameters([Parameter("use_sim_time", Parameter.Type.BOOL, False)])
        await asyncio.sleep(0.1)

    run(body)
    assert seen == []


# --- the datapoint cap ------------------------------------------------------


def test_the_cap_applies_unless_the_datapoint_says_keep():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped"), _dp("kept", keep=True)]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        node = _publish(6)
        try:
            return await _drain_samples(rt)
        finally:
            node.destroy_node()

    samples = run(body)
    per_slug = {}
    for sample in samples:
        per_slug.setdefault(sample.slug, []).append(sample)
    # Six publishes inside a second: the 1 Hz cap lets the first through and
    # nothing after it.
    assert len(per_slug.get("capped", [])) == 1
    assert len(per_slug.get("kept", [])) == 6


def _publish_for(seconds, interval=0.02):
    """Publish as steadily as the loop allows for `seconds`, and return how
    long it actually took — a rate asserted against the nominal duration
    would be asserting the machine's timer."""
    node = rclpy.create_node("test_lb_rate_publisher")
    try:
        pub = node.create_publisher(BatteryState, "/battery", 10)
        started = time.monotonic()
        i = 0
        while time.monotonic() - started < seconds:
            pub.publish(_battery(0.001 * (i % 100)))
            i += 1
            time.sleep(interval)
        return time.monotonic() - started
    finally:
        node.destroy_node()


def test_the_cap_is_an_average_and_not_a_minimum_gap():
    """The one decision that keeps the live rate on the ceiling.

    The configured rate gates the record, so the cap is applied to a stream
    that is already thinned — and a minimum gap asked about such a grid is
    never quite due: a 0.2 s interval against arrivals every 0.18 s takes
    every second one and settles at half the rate it was given. An average
    holds the rate whatever the spacing.

    Pinned here, where the choice is made, because the difference is
    invisible to any check of the form "at most the ceiling". What the two
    kinds actually produce is compared in
    `test_a_minimum_gap_cap_would_settle_at_the_beat_frequency`, and the
    policy's own arithmetic is `test_sampling.py`'s."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped", rate_throttle_hz=6)]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=5))
        return rt._subscriptions["capped"].cap

    assert isinstance(run(body), AverageHzPolicy)


def test_the_effective_live_rate_lands_on_the_ceiling_not_below_it():
    """The measured half of the claim above, at the ratio that beats worst:
    a datapoint configured just above the ceiling.

    6 Hz configured under a 5 Hz ceiling, fed at 50 Hz. Chained, the two
    policies settle at roughly 2.8 Hz — a datapoint losing far more than the
    mode asked for, in a direction no assertion of the form `<= 5 Hz` can
    see. The floor below is what separates the two implementations; the
    ceiling is there so a cap that stopped applying at all is caught too."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped", rate_throttle_hz=6)]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=5))
        elapsed = _publish_for(2.0)
        return len(await _drain_samples(rt)), elapsed

    count, elapsed = run(body)
    rate = count / elapsed
    # The floor sits well clear of the chain's 2.8 Hz and leaves a stalled
    # executor room to lose several admissions without a false red.
    assert 3.5 <= rate <= 6.0, "effective live rate was {:.2f} Hz".format(rate)


def test_the_record_stays_within_the_configured_rate_while_the_mode_holds():
    """What the cloud ends up holding — the live sends plus the backfill that
    follows them — is still `rate_throttle_hz`, not more.

    A live send is part of the record, so the configured rate is the one
    gate deciding whether a sample is recorded at all; the mode only decides
    whether a recorded sample goes now or waits. Two independent gates would
    make the two sets disjoint instead of nested, and their union would
    exceed the ceiling the configuration states — in the expensive
    direction, since a bounded `max_buffer_values` ring would then evict
    real history sooner and the backfill after recovery would be larger than
    the link the mode exists to spare can afford."""

    async def body(rt):
        await rt.apply_config(by_slug([
            _dp("capped", rate_throttle_hz=6,
                retention=RetentionConfig(enabled=True, max_buffer_values=500)),
        ]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=5))
        elapsed = _publish_for(3.0)
        live = await _drain_samples(rt)
        held = 0
        while rt.backlog.pop_any() is not None:
            held += 1
        return len(live), held, elapsed

    live, held, elapsed = run(body)
    record = (live + held) / elapsed
    # `MaxHzPolicy` carries its own 2 % tolerance, so the budget it enforces
    # is a shade over the nominal 6 Hz; anything near 10 is the two-gate
    # union this test exists to rule out.
    assert record <= 7.0, "the record ran at {:.2f} Hz".format(record)
    # And the live half still sits on the ceiling rather than under it.
    assert 3.5 <= live / elapsed <= 6.0


def test_a_datapoint_already_slower_than_the_cap_is_left_alone():
    """The cap is a ceiling, not a rate. A datapoint configured at 0.5 Hz
    must not be re-policed by a 1 Hz cap — that would reset its own timer on
    every transition and could only ever send it less often, never more."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("slow", rate_throttle_hz=0.5)]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        return rt._subscriptions["slow"].cap

    assert run(body) is None


def test_leaving_the_mode_clears_every_cap():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped")]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        in_mode = rt._subscriptions["capped"].cap
        await rt.set_low_bandwidth(False, _settings(datapoint_max_hz=1))
        return in_mode, rt._subscriptions["capped"].cap

    in_mode, after = run(body)
    assert in_mode is not None
    assert after is None


def test_a_datapoint_configured_while_the_mode_is_on_is_capped_too():
    async def body(rt):
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        await rt.apply_config(by_slug([_dp("late")]))
        await asyncio.sleep(0.3)
        node = _publish(6)
        try:
            return await _drain_samples(rt)
        finally:
            node.destroy_node()

    samples = run(body)
    assert len([s for s in samples if s.slug == "late"]) == 1


def test_capped_samples_go_to_the_backlog_when_retention_is_enabled():
    """The cap spares the link, not the history: a buffered datapoint keeps
    every sample the cap held back, and the cloud fills the gap in once the
    link recovers."""

    async def body(rt):
        await rt.apply_config(by_slug([
            _dp("buffered", retention=RetentionConfig(enabled=True, max_buffer_values=50)),
        ]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        node = _publish(6)
        try:
            live = await _drain_samples(rt)
        finally:
            node.destroy_node()
        held = []
        while True:
            sample = rt.backlog.pop_any()
            if sample is None:
                break
            held.append(sample)
        return live, held

    live, held = run(body)
    assert len(live) == 1
    assert len(held) == 5


def test_an_unbuffered_capped_sample_is_dropped_and_not_queued_anywhere():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("plain")]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        node = _publish(6)
        try:
            live = await _drain_samples(rt)
        finally:
            node.destroy_node()
        return live, rt.backlog.has_pending()

    live, pending = run(body)
    assert len(live) == 1
    assert pending is False


# --- the camera lever -------------------------------------------------------


def test_reduce_lowers_a_running_stream_and_restores_it_on_exit():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.set_low_bandwidth(True, _settings(camera="reduce", camera_bitrate_kbps=300))
        await rt.set_low_bandwidth(False, _settings(camera="reduce", camera_bitrate_kbps=300))
        return _LivePublisher.instances[0].bitrate_calls

    assert run(body, live_publisher_factory=_live_factory()) == [300, 800]


def test_stop_ends_running_streams_and_says_why():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        await rt.set_low_bandwidth(True, _settings(camera="stop"))
        states = await _drain_camera_states(rt)
        return states, _LivePublisher.instances[0].stop_calls

    states, stop_calls = run(body, live_publisher_factory=_live_factory())
    assert stop_calls == 1
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error == (
        "low_bandwidth", "Live video stopped: the bridge entered low-bandwidth mode."
    )
    assert states[0].cause == "live_lost"


def test_a_settings_change_while_the_mode_holds_rebuilds_with_the_new_numbers():
    """The levers carry numbers, not only a state, and the client re-applies
    them after a settings change that flipped nothing. Both halves: the
    datapoint ceiling and a running stream's bitrate."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped")]))
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        await asyncio.sleep(0.3)
        # The stream joins before the mode: a `camera_start` arriving after
        # it is refused outright, which the tests below cover.
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1, camera_bitrate_kbps=300))
        first = rt._subscriptions["capped"].cap.hz
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=5, camera_bitrate_kbps=200))
        return first, rt._subscriptions["capped"].cap.hz, _LivePublisher.instances[0].bitrate_calls

    first, second, bitrates = run(body, live_publisher_factory=_live_factory())
    assert (first, second) == (1.0, 5.0)
    # The lever on entry, then the lever again with the number that moved.
    assert bitrates == [300, 200]


def test_a_rebuild_that_changes_no_number_keeps_the_credit_it_has_spent():
    """The client rebuilds at every `hello_ok` and after every settings
    change, including ones that change nothing. A fresh policy starts with a
    full credit, so a reconnecting robot on a degraded link would buy one
    extra sample per datapoint per reconnect."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("capped")]))
        await asyncio.sleep(0.3)
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        before = rt._subscriptions["capped"].cap
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=1))
        same = rt._subscriptions["capped"].cap
        await rt.set_low_bandwidth(True, _settings(datapoint_max_hz=5))
        return before is same, rt._subscriptions["capped"].cap is same

    kept, replaced_on_change = run(body)
    assert kept is True
    assert replaced_on_change is False


@pytest.mark.parametrize("camera", ["reduce", "stop"])
def test_start_live_is_refused_under_either_lever(camera):
    """A new stream is new uplink whichever lever the mode is set to; the
    code does not branch on it, and this says so rather than leaving it to
    be read off the source."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.set_low_bandwidth(True, _settings(camera=camera))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-9")
        return await _drain_camera_states(rt), list(rt._live_publishers)

    states, publishers = run(body, live_publisher_factory=_live_factory())
    assert publishers == []
    assert states[0].error[0] == "low_bandwidth"


def test_start_live_is_refused_while_the_mode_holds():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.set_low_bandwidth(True, _settings(camera="reduce"))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-9")
        return await _drain_camera_states(rt), list(rt._live_publishers)

    states, publishers = run(body, live_publisher_factory=_live_factory())
    assert publishers == []
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].request_id == "req-9"
    assert states[0].cause == "command"
    assert states[0].error == (
        "low_bandwidth",
        "The bridge is in low-bandwidth mode; live video waits until the link recovers.",
    )


def test_a_slug_with_no_camera_is_still_answered_as_unavailable_not_as_the_mode():
    """The mode is not the reason a slug nobody configured cannot go live.
    Answering `low_bandwidth` there would send a developer chasing the link
    instead of the document."""

    async def body(rt):
        await rt.set_low_bandwidth(True, _settings())
        await rt.start_live("ghost", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_live_factory())
    assert states[0].error[0] == "live_unavailable"


def test_leaving_the_mode_lets_a_stream_start_again():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.set_low_bandwidth(True, _settings())
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        await rt.set_low_bandwidth(False, _settings())
        await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "req-2")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_live_factory())
    assert len(states) == 1
    assert states[0].publishing is True
    assert states[0].request_id == "req-2"


def test_the_lever_is_idempotent():
    """client.py pulls it on every settings change as well as on every
    transition, so the second pull with the same numbers has to be free."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        for _ in range(3):
            await rt.set_low_bandwidth(True, _settings(camera="stop"))
        return await _drain_camera_states(rt), _LivePublisher.instances[0].stop_calls

    states, stop_calls = run(body, live_publisher_factory=_live_factory())
    assert stop_calls == 1
    assert len(states) == 1


def test_setting_the_mode_before_start_is_not_an_error():
    """`main.py` constructs the runtime before it starts the node. Nothing
    reads the parameters that early, but a client that applied a forced
    section from a cached config would reach this with no node at all."""
    from fleetless_bridge.ros_runtime import RosRuntime

    rt = RosRuntime(node_name="test_lb_unstarted")
    assert rt.low_bandwidth_params() == {}
    asyncio.run(rt.set_low_bandwidth(True, _settings()))
    rt.stop()  # must not raise


# --- the composition, simulated ---------------------------------------------


def _simulate(source_hz, configured_hz, ceiling_hz, seconds=60.0,
              cap_policy=AverageHzPolicy):
    """`_on_message`'s rate decision, driven by a float clock instead of a
    ROS graph: the real policy objects, the real order, no timing.

    Returns the live rate, the backlog rate and their union — the record,
    which is what the cloud ends up holding once the backfill has drained."""
    rate = rate_policy(configured_hz)
    cap = cap_policy(ceiling_hz)
    live = held = 0
    for i in range(int(seconds * source_hz)):
        now = i / source_hz
        if not rate.should_send(i, now):
            continue
        if cap.should_send(i, now):
            live += 1
        else:
            held += 1
    return live / seconds, held / seconds, (live + held) / seconds


@pytest.mark.parametrize(
    "source_hz,configured_hz,ceiling_hz",
    [(50, 6, 5), (50, 3, 1), (10, 1.5, 1)],
)
def test_the_record_is_within_the_configured_rate_and_live_lands_on_the_min(
    source_hz, configured_hz, ceiling_hz
):
    """Three ratios, against the arithmetic the bridge actually runs.

    Both halves of the promise at once: the record never exceeds
    `rate_throttle_hz` — a live send counts against it, because the cloud
    holds it — and the live rate is `min(rate_throttle_hz,
    datapoint_max_hz)` rather than the beat frequency two chained minimum
    gaps settle at."""
    live, _held, record = _simulate(source_hz, configured_hz, ceiling_hz)
    # `MaxHzPolicy`'s own 2 % tolerance is the only thing above nominal here.
    assert record <= configured_hz * 1.03, "record {:.2f} Hz".format(record)
    expected = min(configured_hz, ceiling_hz)
    assert abs(live - expected) <= 0.1, "live {:.2f} Hz, wanted {}".format(live, expected)


def test_a_minimum_gap_cap_would_settle_at_the_beat_frequency():
    """Why the cap is not a `MaxHzPolicy`, stated as the number it produces.

    Same source, same configured rate, same ceiling — only the cap's kind
    differs. A minimum gap lands near 2.8 Hz against a ceiling of 5, and no
    assertion of the form "at most the ceiling" can tell that apart from
    working correctly."""
    average, _, _ = _simulate(50, 6, 5)
    gapped, _, _ = _simulate(50, 6, 5, cap_policy=MaxHzPolicy)
    assert abs(average - 5.0) <= 0.1
    assert gapped < 3.5


def test_a_join_still_in_flight_when_the_mode_enters_does_not_commit_a_stream():
    """The one window the pre-join refusal cannot close.

    A `camera_start` arriving during the `enter_after_s` window passes the
    refusal, and its LiveKit join — ICE and TURN, on the very link the mode
    is about to call narrow — can still be awaiting when the lever runs. The
    lever iterates the committed publishers and finds none; the join then
    returns and commits a full-rate stream that nothing will touch until the
    next transition. So the mode is read again after the join, and this one
    publisher gets the lever it missed."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        gate = asyncio.Event()
        _LivePublisher.gate = gate
        joining = asyncio.ensure_future(
            rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        )
        await asyncio.sleep(0.05)  # the join is now awaiting the gate
        await rt.set_low_bandwidth(True, _settings(camera="stop"))
        gate.set()
        await joining
        states = await _drain_camera_states(rt)
        return states, list(rt._live_publishers), _LivePublisher.instances[0].stop_calls

    try:
        states, publishers, stop_calls = run(body, live_publisher_factory=_live_factory())
    finally:
        _LivePublisher.gate = None
    assert publishers == []
    assert stop_calls == 1
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].request_id == "req-1"
    assert states[0].error == (
        "low_bandwidth",
        "The bridge is in low-bandwidth mode; live video waits until the link recovers.",
    )


def test_a_join_that_lands_under_reduce_is_re_targeted_before_it_commits():
    """The same window under the other lever: the stream is kept, at the
    mode's bitrate rather than the camera's."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", bitrate_kbps=800)]))
        gate = asyncio.Event()
        _LivePublisher.gate = gate
        joining = asyncio.ensure_future(
            rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        )
        await asyncio.sleep(0.05)
        await rt.set_low_bandwidth(True, _settings(camera="reduce", camera_bitrate_kbps=300))
        gate.set()
        await joining
        await _drain_camera_states(rt)
        return _LivePublisher.instances[0].bitrate_calls, list(rt._live_publishers)

    try:
        calls, publishers = run(body, live_publisher_factory=_live_factory())
    finally:
        _LivePublisher.gate = None
    assert publishers == ["front"]
    assert calls == [300]


# --- the asset refusal paths the low-bandwidth work sits beside -------------


class _FakeHTTPError:
    """Just the half `_store_refusal_details` reads: a bounded `read`."""

    def __init__(self, body):
        self._body = body if isinstance(body, bytes) else json.dumps(body).encode()

    def read(self, limit):
        return self._body[:limit]


@pytest.mark.parametrize(
    "details",
    [
        {"store_bytes": 1000, "used_bytes": 900, "size_bytes": 200.5},
        {"store_bytes": 1000, "used_bytes": -1, "size_bytes": 200},
        {"store_bytes": True, "used_bytes": 900, "size_bytes": 200},
        {"store_bytes": 1000, "used_bytes": 900},
        {"store_bytes": "1000", "used_bytes": 900, "size_bytes": 200},
    ],
)
def test_refusal_details_are_all_three_numbers_or_none(details):
    """Two thirds of an answer is worse than admitting there is none: the
    cloud's three numbers go on a `refused` entry a developer reads, and a
    float, a negative, a bool, a string or a missing key all mean the body
    did not say."""
    from fleetless_bridge.ros_runtime import RosRuntime

    assert RosRuntime._store_refusal_details(_FakeHTTPError({"details": details})) is None


def test_refusal_details_read_the_three_numbers_when_they_are_all_there():
    from fleetless_bridge.ros_runtime import RosRuntime

    body = {"details": {"store_bytes": 1000, "used_bytes": 900, "size_bytes": 200}}
    assert RosRuntime._store_refusal_details(_FakeHTTPError(body)) == {
        "store_bytes": 1000, "used_bytes": 900, "size_bytes": 200,
    }


def test_a_body_that_is_not_json_at_all_reads_as_no_details():
    from fleetless_bridge.ros_runtime import RosRuntime

    assert RosRuntime._store_refusal_details(_FakeHTTPError(b"<html>502</html>")) is None


def _refusing_server(status, body):
    """A one-shot HTTP server that answers every POST with `status`."""
    import http.server
    import threading

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            payload = json.dumps(body).encode() if body is not None else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = "http://127.0.0.1:{}/upload".format(server.server_port)

    def stop():
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)

    return url, stop


def test_the_urdf_upload_path_classifies_a_refusal_the_same_way():
    """Both refusal tests elsewhere drive the streaming path with a mesh.
    The URDF goes through the in-memory one, and the cloud never refuses a
    URDF today — which is exactly why this branch would otherwise be the one
    a future cloud change hits unseen."""
    from fleetless_bridge.ros_runtime import RosRuntime

    url, stop = _refusing_server(
        409, {"details": {"store_bytes": 1000, "used_bytes": 999, "size_bytes": 200}}
    )
    try:
        result = RosRuntime._upload_asset_bytes(
            url, "tok", "sync-1", "urdf", "robot_description", "text/xml", b"<robot/>",
        )
    finally:
        stop()
    assert result.ok is False
    assert result.refused is True
    assert result.details == {"store_bytes": 1000, "used_bytes": 999, "size_bytes": 200}


def test_a_bare_refusal_on_the_urdf_path_carries_no_invented_numbers():
    from fleetless_bridge.ros_runtime import RosRuntime

    url, stop = _refusing_server(413, None)
    try:
        result = RosRuntime._upload_asset_bytes(
            url, "tok", "sync-1", "urdf", "robot_description", "text/xml", b"<robot/>",
        )
    finally:
        stop()
    assert (result.ok, result.refused, result.details) == (False, True, None)
