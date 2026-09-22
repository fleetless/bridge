# SPDX-License-Identifier: Apache-2.0
"""Low-bandwidth mode as the session sees it: the report after hello, the
ping feeding the controller, the YAML section on top of the parameters, and
the backfill tier going quiet while the mode is on.

The controller itself is `test_link_mode.py`'s subject — thresholds, timers
and hysteresis are tested against a float clock there, not through a
WebSocket here. What this file covers is the wiring: which numbers reach
`observe_cloud`, which frame leaves, which lever is pulled, and what a
config that does not resolve costs (nothing but an entry in the ack).

`enter_after_s` counts in whole seconds and the shortest one the contract
allows is 1, so every test here injects `now` — a clock the test advances
itself — and a fast `tick`. Real time would buy the same assertions at ten
seconds apiece.
"""
import asyncio
import contextlib
import logging

from fake_cloud import FakeCloud, accepts, sequence
from helpers import make_client, run_until
from test_client_datapoints import FakeRos

from fleetless_bridge.sampling import Sample, capture_timestamp_ms


class Clock:
    """A monotonic clock a test moves by hand.

    Advanced from the cloud's side of the connection, between the frames it
    sends — so a test says "ten seconds of this lag" in one line instead of
    waiting them out."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    async def advance(self, seconds: float, ticks: float = 0.05) -> None:
        """Move the clock, then yield long enough for the client's tick task
        to call `evaluate` at least once against the new time."""
        self.t += seconds
        await asyncio.sleep(ticks)


def _client(cloud, clock, ros, **overrides):
    return make_client(cloud, ros=ros, now=clock, tick=0.005, **overrides)


def test_after_hello_the_bridge_reports_its_mode_once_and_pings_feed_the_controller():
    """Two frames, in order: the state at `hello_ok` — so the cloud's
    `bridge_state` is right from the first second rather than from the first
    transition — and the transition the lag then causes."""
    clock = Clock()
    ros = FakeRos()
    ros.low_bandwidth_params_value["enter_after_s"] = 1

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            box["first"] = await session.recv_link_mode()
            # One reading holds until the next ping or until it ages out, so
            # a single lag number plus the passage of time is enough to enter.
            await session.ping(1, latency_ms=50, lag_ms=5000)
            # The pong proves the ping was read, so the next tick sees the
            # lag — moving the clock before that would start the enter timer
            # from the later time and the mode would never arrive.
            await session.recv_pong()
            # Half of `enter_after_s`, so the tick has run a dozen times
            # against a lag over the threshold and must still say nothing.
            await clock.advance(0.5)
            box["early"] = list(cloud.link_modes)
            await clock.advance(5.0)
            box["second"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "second" in box)
            box["all"] = list(cloud.link_modes)
        return box

    box = asyncio.run(scenario())
    first, second = box["first"], box["second"]
    assert first["low_bandwidth"] is False and first["reason"] == "recovered"
    assert second["low_bandwidth"] is True and second["reason"] == "lag"
    # Nothing before the threshold: the crossing tick is pinned in
    # test_link_mode.py, and this is the wired path saying the same.
    assert len(box["early"]) == 1
    # Two frames for the whole session and no more. The tick runs tens of
    # times here, so a frame per evaluation — the heartbeat
    # `link_mode_message` exists not to be — would be unmissable.
    assert len(box["all"]) == 2
    # The lever was pulled with the same answer the wire carries.
    assert ros.low_bandwidth_calls[-1][0] is True


def test_a_ping_hands_over_both_numbers_before_the_pong_goes_out():
    """`latency_ms` is deliberately not part of the decision (a long round
    trip is a far cloud), but the ping handler hands both numbers over
    unchanged — the controller's signature is where that ruling lives."""
    clock = Clock()
    ros = FakeRos()
    seen = []

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.ping(7, latency_ms=120, lag_ms=None)
            box["pong"] = await session.recv_pong()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            original = client._link_mode.observe_cloud

            def record(lag_ms, latency_ms, now):
                seen.append((lag_ms, latency_ms))
                return original(lag_ms, latency_ms, now)

            client._link_mode.observe_cloud = record
            await run_until(client, lambda: "pong" in box)
        return box["pong"]

    pong = asyncio.run(scenario())
    assert pong["ts_ms"] == 7
    assert seen == [(None, 120)]


def test_a_forced_yaml_section_turns_the_mode_on_and_the_cloud_is_told():
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.send_config(1, {}, low_bandwidth={"mode": "on"})
            box["link_mode"] = await session.recv_link_mode()
            box["applied"] = await session.recv_config_applied()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "applied" in box)
        return box

    box = asyncio.run(scenario())
    assert box["link_mode"]["low_bandwidth"] is True
    assert box["link_mode"]["reason"] == "forced"
    assert box["applied"]["ok"] is True and box["applied"]["errors"] == []


def test_a_section_that_does_not_resolve_is_reported_and_the_mode_stays_put():
    """`exit_lag_ms: 3000` alone is a valid published document — contracts
    compares the pair only when the section carries both keys — and crosses
    the default `enter_lag_ms` once it reaches the robot. Refusing it must
    cost the ack an entry and nothing else: a bridge that treated it as
    fatal would be taken down by a document the console accepted."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.send_config(1, {}, low_bandwidth={"mode": "on"})
            await session.recv_link_mode()
            await session.recv_config_applied()
            await session.send_config(2, {}, low_bandwidth={"exit_lag_ms": 3000})
            box["applied"] = await session.recv_config_applied()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "applied" in box)
            box["active"] = client._link_mode.active
        return box

    box = asyncio.run(scenario())
    assert box["applied"]["ok"] is False
    assert box["applied"]["errors"] == [{
        "slug": "low_bandwidth",
        "kind": "low_bandwidth",
        "code": "low_bandwidth_invalid",
        "message": "low_bandwidth.exit_lag_ms must be at or below enter_lag_ms",
    }]
    # The previous section — `mode: on` — is still what the bridge runs.
    assert box["active"] is True


def test_backfill_pauses_in_the_mode_and_resumes_after():
    """Tier 4 is the one tier the mode silences outright: buffered history
    has no deadline, and sending it over the link the mode exists to spare
    is the one thing that can wait."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.send_config(1, {}, low_bandwidth={"mode": "on"})
            await session.recv_link_mode()
            await session.recv_config_applied()
            ros.backlog.configure("old", True, 10)
            ros.backlog.push("old", Sample(slug="old", value=1, timestamp_ms=1))
            # Nothing may arrive while the mode holds. A live sample is the
            # proof the writer is awake and simply not asking tier 4.
            loop = asyncio.get_event_loop()
            ros.samples.put_threadsafe(loop, Sample(slug="live", value=2, timestamp_ms=2))
            box["live"] = await session.recv_datapoint()
            await session.send_config(2, {}, low_bandwidth={"mode": "off"})
            # Tier order, on the way back out: the transition (tier 0), the
            # ack (tier 1), and only then the buffered sample (tier 4).
            await session.recv_link_mode()
            await session.recv_config_applied()
            box["replayed"] = await session.recv_datapoint()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "replayed" in box)
        return box

    box = asyncio.run(scenario())
    assert box["live"]["slug"] == "live" and "backfill" not in box["live"]
    assert box["replayed"]["slug"] == "old" and box["replayed"]["backfill"] is True


def test_a_sent_sample_feeds_the_dwell_tracker():
    """The mode's second input, and the only one that still works when the
    cloud has stopped answering: how long a sample waited between capture
    and the wire."""
    clock = Clock()
    ros = FakeRos()
    seen = []

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            loop = asyncio.get_event_loop()
            # Stamped now: a sample older than the session is deliberately
            # sent but not measured, which the reconnect test below covers.
            ros.samples.put_threadsafe(
                loop, Sample(slug="speed", value=1, timestamp_ms=capture_timestamp_ms())
            )
            box["sample"] = await session.recv_datapoint()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            original = client._link_mode.observe_dwell

            def record(dwell_ms, now):
                seen.append(dwell_ms)
                return original(dwell_ms, now)

            client._link_mode.observe_dwell = record
            await run_until(client, lambda: "sample" in box)
        return box

    asyncio.run(scenario())
    # The wire is the only thing between capture and the measurement here,
    # so the number is small; how small is the machine's business, not this
    # test's.
    assert len(seen) == 1 and 0 <= seen[0] < 60_000


def test_a_parameter_set_re_resolves_and_pulls_the_levers_again():
    """`ros2 param set low_bandwidth.mode on` at runtime. The runtime
    validates and then calls back on the event loop; this is what the client
    makes of that call."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            box["greeted"] = True
            box["link_mode"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            task = asyncio.ensure_future(client.run())
            while "greeted" not in box:
                await asyncio.sleep(0.005)
            values = dict(ros.low_bandwidth_params_value)
            values["mode"] = "on"
            ros.low_bandwidth_params_value = values
            ros.low_bandwidth_callback(values)
            while "link_mode" not in box:
                await asyncio.sleep(0.005)
            client.stop()
            await asyncio.wait_for(task, timeout=10.0)
        return box

    box = asyncio.run(scenario())
    assert box["link_mode"]["low_bandwidth"] is True
    assert box["link_mode"]["reason"] == "forced"
    assert ros.low_bandwidth_calls[-1][0] is True


def test_the_settings_reach_the_runtime_even_when_the_mode_does_not_flip():
    """`datapoint_max_hz` can move while the mode stays on. `update_settings`
    returns no transition then — there was none — so the levers have to be
    re-applied on the settings change itself, not only on a crossing."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.send_config(1, {}, low_bandwidth={"mode": "on"})
            await session.recv_link_mode()
            await session.recv_config_applied()
            await session.send_config(
                2, {}, low_bandwidth={"mode": "on", "datapoint_max_hz": 5}
            )
            box["applied"] = await session.recv_config_applied()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "applied" in box)
        return box

    asyncio.run(scenario())
    active, settings = ros.low_bandwidth_calls[-1]
    assert active is True and settings.datapoint_max_hz == 5


def test_a_client_with_no_runtime_still_resolves_and_reports():
    """`ros=None` is how most of the suite builds a client. The mode must
    still resolve from its defaults and report once, or every one of those
    tests would be greeting a cloud that never hears which mode it is in."""
    clock = Clock()

    async def scenario():
        box = {}

        async def then(session):
            box["link_mode"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(accepts(then=then)) as cloud:
            client = make_client(cloud, now=clock, tick=0.005)
            await run_until(client, lambda: "link_mode" in box)
        return box["link_mode"]

    frame = asyncio.run(scenario())
    assert frame["low_bandwidth"] is False and frame["reason"] == "recovered"


def test_a_reconnect_does_not_rewind_the_enter_timer():
    """The link this mode exists for is the one that drops the session.

    The cloud closes a socket after three unanswered pings, about six
    seconds; a bridge that restarted `enter_after_s` on every `hello_ok`
    would need ten seconds inside one session and so would never engage on
    a link that cannot hold one for ten. The controller therefore lives for
    the client, not the session, and its timers are only reset by settings
    that actually resolved differently.

    Six seconds of lag, a dropped session, six more: twelve against a
    threshold of ten, so the mode enters in the second session."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def first(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.ping(1, latency_ms=50, lag_ms=5000)
            await session.recv_pong()
            await clock.advance(6.0)
            box["first_done"] = True
            await session.close()

        async def second(session):
            await session.recv_hello()
            await session.accept()
            box["greeting"] = await session.recv_link_mode()
            await session.ping(2, latency_ms=50, lag_ms=5000)
            await session.recv_pong()
            await clock.advance(6.0)
            box["entered"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(sequence(first, second)) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "entered" in box)
        return box

    box = asyncio.run(scenario())
    # The second session opens by restating the mode it is still in: off.
    assert box["greeting"]["low_bandwidth"] is False
    assert box["entered"]["low_bandwidth"] is True
    assert box["entered"]["reason"] == "lag"


def test_a_forced_parameter_layer_states_the_mode_once_after_hello():
    """A parameter layer that forces the mode makes `update_settings` return
    a `forced` transition at the very moment the session opens, and the
    greeting states the mode too. One frame, not both."""
    clock = Clock()
    ros = FakeRos()
    ros.low_bandwidth_params_value["mode"] = "on"

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            box["frame"] = await session.recv_link_mode()
            await session.ping(1, latency_ms=10, lag_ms=0)
            await session.recv_pong()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "frame" in box)
            box["all"] = list(cloud.link_modes)
        return box

    box = asyncio.run(scenario())
    assert box["frame"]["low_bandwidth"] is True
    assert box["frame"]["reason"] == "forced"
    assert len(box["all"]) == 1


def test_a_sample_captured_before_this_session_is_not_measured_as_dwell():
    """A sample queued just before a session died keeps its capture stamp and
    is sent by the next one, so its dwell is the length of the outage rather
    than anything about the link that is up now. At the default
    `enter_after_s` of 10 s the five-second window ages it out first; at 1 s
    it would put every reconnect straight into the mode."""
    clock = Clock()
    ros = FakeRos()
    seen = []

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            loop = asyncio.get_event_loop()
            # An hour old: captured long before this session opened.
            ros.samples.put_threadsafe(
                loop, Sample(slug="stale", value=1, timestamp_ms=capture_timestamp_ms() - 3_600_000)
            )
            ros.samples.put_threadsafe(
                loop, Sample(slug="fresh", value=2, timestamp_ms=capture_timestamp_ms())
            )
            box["one"] = await session.recv_datapoint()
            box["two"] = await session.recv_datapoint()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            original = client._link_mode.observe_dwell

            def record(dwell_ms, now):
                seen.append(dwell_ms)
                return original(dwell_ms, now)

            client._link_mode.observe_dwell = record
            await run_until(client, lambda: "two" in box)
        return box

    box = asyncio.run(scenario())
    # Both samples went out — the guard drops the measurement, not the frame.
    assert {box["one"]["slug"], box["two"]["slug"]} == {"stale", "fresh"}
    assert len(seen) == 1 and seen[0] < 60_000


def test_a_parameter_set_the_published_section_crosses_is_survived():
    """The parameter callback validates the parameters alone, so a set that
    only crosses a threshold once the published section is laid over it
    reaches the client as a `ValueError` on a path the config apply does not
    cover. It has to keep the settings it had and keep running."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            # Valid on its own and valid over the defaults.
            await session.send_config(1, {}, low_bandwidth={"enter_lag_ms": 600})
            box["applied"] = await session.recv_config_applied()
            box["greeted"] = True
            # `exit_lag_ms: 1000` clears the default `enter_lag_ms` of 2000,
            # so the runtime's own callback accepts it; laid under the
            # published `enter_lag_ms: 600` it is crossed.
            await session.ping(1, latency_ms=10, lag_ms=0)
            await session.recv_pong()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            task = asyncio.ensure_future(client.run())
            while "greeted" not in box:
                await asyncio.sleep(0.005)
            before = client._lb_settings
            values = dict(ros.low_bandwidth_params_value)
            values["exit_lag_ms"] = 1000
            ros.low_bandwidth_callback(values)
            await asyncio.sleep(0.05)
            box["before"], box["after"] = before, client._lb_settings
            box["still_running"] = not task.done()
            client.stop()
            await asyncio.wait_for(task, timeout=10.0)
        return box

    box = asyncio.run(scenario())
    assert box["applied"]["ok"] is True
    assert box["after"] == box["before"]
    assert box["after"].enter_lag_ms == 600
    assert box["still_running"] is True


class _CapturingHandler(logging.Handler):
    """Everything client.py logs, at any level.

    Not `caplog`: this suite has established repeatedly (`test_main.py`,
    `test_live.py`, `test_livekit_signal.py`) that pytest disables
    propagation for these loggers, so a `caplog` assertion would pass
    whether or not the line was written — the worst state for a check whose
    job is noticing a missing one."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@contextlib.contextmanager
def _capturing_client_log():
    """Collect client.py's log lines for the duration of a `with`.

    A context manager rather than a pair to unwind by hand: the level has to
    go back too, and a test that left this logger at DEBUG would leave it
    there for every test after it in the same process."""
    handler = _CapturingHandler()
    watched = logging.getLogger("fleetless_bridge.client")
    previous = watched.level
    watched.addHandler(handler)
    watched.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        watched.removeHandler(handler)
        watched.setLevel(previous)


def test_the_transition_log_line_carries_the_numbers_that_caused_it():
    """An operator reading the robot's journal has the threshold in the
    configuration and needs the reading that crossed it. The controller does
    not keep either number, so the client remembers what it last fed in."""
    clock = Clock()
    ros = FakeRos()
    ros.low_bandwidth_params_value["enter_after_s"] = 1

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.ping(1, latency_ms=50, lag_ms=5000)
            await session.recv_pong()
            await clock.advance(1.0)
            await clock.advance(5.0)
            box["entered"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "entered" in box)
        return box

    with _capturing_client_log() as handler:
        asyncio.run(scenario())

    on = [m for m in handler.messages if m.startswith("Low-bandwidth mode on")]
    assert len(on) == 1
    # The reading the cloud sent, and the threshold it crossed.
    assert "5000 ms" in on[0] and "2000 ms" in on[0]
    # Not the raw dwell sample. The decision is made on a p95 over five
    # seconds, and one sample beside a threshold reads as the number that
    # crossed it.
    assert "dwell" not in on[0]


def test_the_greeting_after_a_reconnect_names_the_measure_that_entered_the_mode():
    """`lag` was hard-coded here, which is a guess: a mode entered on the
    local queue dwell — the case where the cloud had stopped answering
    altogether — would have been reported to that same cloud as a lag
    problem."""
    clock = Clock()
    ros = FakeRos()
    ros.low_bandwidth_params_value["enter_after_s"] = 1
    # A dwell a test can produce without waiting two real seconds out, and an
    # exit threshold that stays under it — crossed thresholds are refused.
    ros.low_bandwidth_params_value["enter_lag_ms"] = 100
    ros.low_bandwidth_params_value["exit_lag_ms"] = 50

    async def scenario():
        box = {}

        async def first(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            # No ping at all: the cloud is silent and the dwell carries the
            # decision, which is the whole point of the second measure.
            # The session has to be demonstrably older than the sample's
            # capture stamp, or the guard against measuring an outage drops
            # the reading — which is the behaviour tested just above.
            await asyncio.sleep(0.5)
            loop = asyncio.get_event_loop()
            ros.samples.put_threadsafe(
                loop,
                Sample(slug="speed", value=1, timestamp_ms=capture_timestamp_ms() - 200),
            )
            await session.recv_datapoint()
            # A small step first, so a tick evaluates while the reading is
            # already in the tracker and opens the enter window at this time
            # rather than at the far end of the jump. Then the crossing, all
            # inside the tracker's five-second window so the reading is still
            # there when the timer comes due.
            await clock.advance(0.2)
            await clock.advance(1.5)
            box["entered"] = await session.recv_link_mode()
            await session.close()

        async def second(session):
            await session.recv_hello()
            await session.accept()
            box["greeting"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(sequence(first, second)) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "greeting" in box)
        return box

    box = asyncio.run(scenario())
    assert box["entered"]["low_bandwidth"] is True
    assert box["entered"]["reason"] == "dwell"
    # The second session restates the mode it is still in, with the reason it
    # actually has.
    assert box["greeting"]["low_bandwidth"] is True
    assert box["greeting"]["reason"] == "dwell"


def test_a_forced_transition_logs_no_reading():
    """The line reports what was consulted. A forced mode consulted nothing —
    `mode: on` holds whatever the link is doing — so naming a lag and a
    threshold beside it would invite the reader to connect the two."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            # A reading arrives first, so the line has one to print if it
            # is going to.
            await session.ping(1, latency_ms=50, lag_ms=4000)
            await session.recv_pong()
            await session.send_config(1, {}, low_bandwidth={"mode": "on"})
            box["link_mode"] = await session.recv_link_mode()
            await session.recv_config_applied()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "link_mode" in box)
        return box

    with _capturing_client_log() as handler:
        box = asyncio.run(scenario())

    assert box["link_mode"]["reason"] == "forced"
    on = [m for m in handler.messages if m.startswith("Low-bandwidth mode on")]
    assert len(on) == 1
    assert "(forced)" in on[0]
    assert "lag" not in on[0] and "4000" not in on[0]


def test_a_refused_parameter_set_never_reaches_the_settings_in_force():
    """A `ros2 param set` the settings cannot use must leave nothing behind.

    The runtime validates the parameters against the published section, so a
    set that only crosses a threshold once that section is laid over it is
    refused there and never becomes the client's parameter layer. Keeping it
    would refuse every later config apply — including the re-send after
    every reconnect — for a value the bridge is not using, and blame a
    document the console accepted."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.send_config(1, {}, low_bandwidth={"enter_lag_ms": 600})
            box["first"] = await session.recv_config_applied()
            box["greeted"] = True
            while "set_done" not in box:
                await asyncio.sleep(0.005)
            # The same document again, the way a reconnect re-sends it.
            await session.send_config(2, {}, low_bandwidth={"enter_lag_ms": 600})
            box["second"] = await session.recv_config_applied()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            task = asyncio.ensure_future(client.run())
            while "greeted" not in box:
                await asyncio.sleep(0.005)
            # `exit_lag_ms: 1000` clears the default `enter_lag_ms` of 2000
            # but crosses the published 600. The runtime refuses it.
            box["refused"] = ros.refuse_or_accept_param("exit_lag_ms", 1000)
            box["set_done"] = True
            while "second" not in box:
                await asyncio.sleep(0.005)
            box["settings"] = client._lb_settings
            client.stop()
            await asyncio.wait_for(task, timeout=10.0)
        return box

    box = asyncio.run(scenario())
    assert box["first"]["ok"] is True
    # The set was refused where it is validated, against the section on top.
    assert box["refused"] is False
    # And the document the console published still applies.
    assert box["second"]["ok"] is True and box["second"]["errors"] == []
    assert box["settings"].enter_lag_ms == 600
    assert box["settings"].exit_lag_ms == 500


def test_a_parameter_set_the_published_section_makes_valid_is_accepted():
    """The other direction of the same handover. `exit_lag_ms: 3000` crosses
    the default `enter_lag_ms` of 2000 and would be refused on the
    parameters alone — but under a published `enter_lag_ms: 5000` the pair
    is fine, and the README promises a set is refused only for a rule it
    broke."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.send_config(1, {}, low_bandwidth={"enter_lag_ms": 5000})
            await session.recv_config_applied()
            box["greeted"] = True
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            task = asyncio.ensure_future(client.run())
            while "greeted" not in box:
                await asyncio.sleep(0.005)
            box["accepted"] = ros.refuse_or_accept_param("exit_lag_ms", 3000)
            await asyncio.sleep(0.05)
            box["settings"] = client._lb_settings
            client.stop()
            await asyncio.wait_for(task, timeout=10.0)
        return box

    box = asyncio.run(scenario())
    assert box["accepted"] is True
    assert box["settings"].enter_lag_ms == 5000
    assert box["settings"].exit_lag_ms == 3000


def test_the_lever_pull_does_not_hold_up_the_receive_loop_at_hello():
    """`set_low_bandwidth` waits on the ROS work queue, behind whatever the
    previous session left there. Awaited on the receive loop it would stop
    the bridge answering pings, and the cloud closes a socket after three
    unanswered ones — so a stalled executor would cost the session it is
    greeting. The tick pulls the levers instead."""
    clock = Clock()
    ros = FakeRos()

    async def scenario():
        box = {}
        gate = asyncio.Event()

        async def blocking(active, settings):
            ros.low_bandwidth_calls.append((active, settings))
            await gate.wait()

        ros.set_low_bandwidth = blocking

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.ping(1, latency_ms=10, lag_ms=0)
            # The pong has to come back while the lever is still blocked —
            # and it does so before the pull has even been attempted, which
            # is the ordering this test is about.
            box["pong"] = await session.recv_pong()
            while not ros.low_bandwidth_calls:
                await asyncio.sleep(0.005)
            box["blocked_pull"] = len(ros.low_bandwidth_calls)
            gate.set()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "pong" in box)
        return box

    box = asyncio.run(scenario())
    assert box["pong"]["ts_ms"] == 1
    # The pull happens, and it happens off the receive path: it was still
    # sitting on the gate when the pong had long since gone out.
    assert box["blocked_pull"] >= 1


def test_a_lever_pull_that_failed_is_retried_on_the_next_tick():
    """Swallowed, a failed pull leaves the cloud told the mode is on and the
    backfill gate shut while the cap and the camera are untouched, until a
    settings change or a reconnect — neither of which a narrow link
    promises."""
    clock = Clock()
    ros = FakeRos()
    attempts = []

    async def scenario():
        box = {}

        async def flaky(active, settings):
            attempts.append((active, settings))
            if len(attempts) == 1:
                raise RuntimeError("the executor was busy")

        ros.set_low_bandwidth = flaky

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            box["greeted"] = True
            await asyncio.sleep(0.1)  # tens of ticks
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: len(attempts) >= 2)
            box["pending"] = client._levers_pending
        return box

    box = asyncio.run(scenario())
    assert len(attempts) >= 2
    assert box["pending"] is False


def test_the_mode_leaves_on_its_own_over_the_wire_and_backfill_resumes():
    """Every other test leaves the mode by `mode: off`. This one drives the
    controller's own exit: calm pings for `exit_after_s`, the `recovered`
    frame on the socket, and the backfill tier opening again behind it."""
    clock = Clock()
    ros = FakeRos()
    ros.low_bandwidth_params_value["enter_after_s"] = 1
    ros.low_bandwidth_params_value["exit_after_s"] = 1

    async def scenario():
        box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await session.ping(1, latency_ms=50, lag_ms=5000)
            await session.recv_pong()
            await clock.advance(0.5)
            await clock.advance(2.0)
            box["entered"] = await session.recv_link_mode()
            # Buffered history, held for as long as the mode is on.
            ros.backlog.configure("old", True, 10)
            ros.backlog.push("old", Sample(slug="old", value=1, timestamp_ms=1))
            await session.ping(2, latency_ms=20, lag_ms=100)
            await session.recv_pong()
            await clock.advance(0.5)
            await clock.advance(2.0)
            box["left"] = await session.recv_link_mode()
            box["replayed"] = await session.recv_datapoint()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "replayed" in box)
        return box

    box = asyncio.run(scenario())
    assert box["entered"]["low_bandwidth"] is True
    assert box["left"]["low_bandwidth"] is False
    assert box["left"]["reason"] == "recovered"
    assert box["replayed"]["slug"] == "old" and box["replayed"]["backfill"] is True
