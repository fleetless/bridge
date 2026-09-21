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

from fake_cloud import FakeCloud, accepts
from helpers import make_client, run_until
from test_client_datapoints import FakeRos

from fleetless_bridge.sampling import Sample


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
            await clock.advance(1.0)
            await clock.advance(5.0)
            box["second"] = await session.recv_link_mode()
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            client = _client(cloud, clock, ros)
            await run_until(client, lambda: "second" in box)
        return box["first"], box["second"]

    first, second = asyncio.run(scenario())
    assert first["low_bandwidth"] is False and first["reason"] == "recovered"
    assert second["low_bandwidth"] is True and second["reason"] == "lag"
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
    assert box["link_mode"] == {
        **box["link_mode"], "low_bandwidth": True, "reason": "forced",
    }
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
        "slug": "*",
        "kind": "datapoint",
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
            ros.samples.put_threadsafe(loop, Sample(slug="speed", value=1, timestamp_ms=0))
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
    # `timestamp_ms=0` is the epoch, so the dwell is "now" in milliseconds —
    # a number this test can only bound, not pin.
    assert len(seen) == 1 and seen[0] > 1_600_000_000_000


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
