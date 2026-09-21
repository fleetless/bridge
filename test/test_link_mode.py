# SPDX-License-Identifier: Apache-2.0
"""The low-bandwidth decision, driven by a fake clock.

`LinkMode` holds no asyncio and no ROS on purpose, so every sustained
condition here is a plain loop over `float` seconds rather than a sleep.

**The loops call `evaluate` on every tick**, because the controller only
advances its timers when it is asked: it starts counting on the first
`evaluate` that sees the condition, not on the observation. A test that
observes twelve seconds of lag and then evaluates once sees a timer that
started at that single call and asserts the opposite of the behaviour.
"""
import pytest

from fleetless_bridge.link_mode import DwellTracker, LinkMode, LowBandwidthSettings, Transition

DEFAULTS = LowBandwidthSettings.resolve({}, {})


# --- settings -----------------------------------------------------------------


def test_settings_resolve_yaml_over_params_over_defaults():
    s = LowBandwidthSettings.resolve({"enter_lag_ms": 1500, "camera": "stop"}, {"enter_lag_ms": 900})
    assert s.enter_lag_ms == 900 and s.camera == "stop" and s.exit_lag_ms == 500 and s.mode == "auto"


def test_settings_defaults_are_the_vendored_constants():
    assert (DEFAULTS.mode, DEFAULTS.enter_lag_ms, DEFAULTS.enter_after_s) == ("auto", 2000, 10)
    assert (DEFAULTS.exit_lag_ms, DEFAULTS.exit_after_s) == (500, 60)
    assert (DEFAULTS.datapoint_max_hz, DEFAULTS.camera, DEFAULTS.camera_bitrate_kbps) == (1, "reduce", 300)


def test_settings_refuse_a_bad_value_and_name_it():
    with pytest.raises(ValueError, match="datapoint_max_hz"):
        LowBandwidthSettings.resolve({"datapoint_max_hz": 0}, {})
    with pytest.raises(ValueError, match="mode"):
        LowBandwidthSettings.resolve({}, {"mode": "sometimes"})


def test_settings_refuse_an_exit_threshold_above_the_enter_threshold():
    # Crossed, the two thresholds enter and exit on the same reading forever.
    # Contracts compares the pair only when the YAML carries both keys, so the
    # lone-key YAML below is published and valid and is caught here or nowhere.
    with pytest.raises(ValueError, match="exit_lag_ms must be at or below enter_lag_ms"):
        LowBandwidthSettings.resolve({}, {"exit_lag_ms": 3000})
    with pytest.raises(ValueError, match="exit_lag_ms must be at or below enter_lag_ms"):
        LowBandwidthSettings.resolve({"exit_lag_ms": 3000}, {})
    assert LowBandwidthSettings.resolve({"exit_lag_ms": 2000}, {}).exit_lag_ms == 2000


def test_settings_refuse_a_bool_where_a_number_belongs():
    # `True` is an `int` in Python and would sail through a naive check as
    # 1 ms, which is a threshold nothing ever clears.
    with pytest.raises(ValueError, match="enter_lag_ms"):
        LowBandwidthSettings.resolve({"enter_lag_ms": True}, {})


def test_settings_none_in_a_layer_falls_through_rather_than_erasing():
    # An undeclared ROS parameter reads as None; it must not beat the default.
    s = LowBandwidthSettings.resolve({"camera": None}, {"enter_after_s": None})
    assert s.camera == "reduce" and s.enter_after_s == 10


# --- the dwell tracker --------------------------------------------------------


def test_dwell_tracker_p95_over_the_window():
    t = DwellTracker(window_s=5.0)
    assert t.p95_ms(0.0) is None
    for i in range(20):
        t.observe(100.0 if i < 19 else 5000.0, 1.0 + i * 0.1)
    assert t.p95_ms(3.0) == pytest.approx(5000.0)
    assert t.p95_ms(20.0) is None  # window emptied


def test_dwell_tracker_ignores_one_slow_send_among_many():
    t = DwellTracker(window_s=5.0)
    for i in range(100):
        t.observe(9000.0 if i == 0 else 10.0, 1.0 + i * 0.01)
    assert t.p95_ms(2.0) == pytest.approx(10.0)


# --- entering -----------------------------------------------------------------


def test_enters_on_sustained_lag_not_on_a_spike():
    m = LinkMode(DEFAULTS, now=0.0)
    m.observe_cloud(lag_ms=3000, latency_ms=100, now=1.0)
    assert m.evaluate(2.0) is None  # 0 s of 10
    m.observe_cloud(lag_ms=100, latency_ms=100, now=3.0)
    assert m.evaluate(3.0) is None  # the spike ended; the timer reset
    for t in range(4, 14):
        m.observe_cloud(lag_ms=3000, latency_ms=100, now=float(t))
        assert m.evaluate(float(t)) is None
    m.observe_cloud(lag_ms=3000, latency_ms=100, now=14.0)
    assert m.evaluate(14.0) == Transition(True, "lag")
    assert m.active
    assert m.evaluate(15.0) is None  # entered once, not on every later tick


def test_enters_on_local_dwell_when_the_cloud_is_silent():
    m = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 11):
        m.observe_dwell(2500.0, now=float(t))
        assert m.evaluate(float(t)) is None
    m.observe_dwell(2500.0, now=11.0)
    assert m.evaluate(11.0) == Transition(True, "dwell")


def test_a_lag_reading_holds_until_the_next_ping():
    # The ping carries a level, not an event: one bad reading and then silence
    # is a link that stopped answering while it was slow.
    m = LinkMode(DEFAULTS, now=0.0)
    m.observe_cloud(lag_ms=9000, latency_ms=100, now=1.0)
    for t in range(2, 12):
        assert m.evaluate(float(t)) is None
    assert m.evaluate(12.0) == Transition(True, "lag")


# --- exiting ------------------------------------------------------------------


def test_exits_only_after_exit_after_s_of_calm():
    m = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 11):
        m.observe_cloud(3000, 100, float(t))
        assert m.evaluate(float(t)) is None
    m.observe_cloud(3000, 100, 11.0)
    assert m.evaluate(11.0) == Transition(True, "lag")
    for t in range(12, 72):
        m.observe_cloud(100, 100, float(t))
        m.observe_dwell(50.0, float(t))
        assert m.evaluate(float(t)) is None  # 59 s of 60 by the last one
    m.observe_cloud(100, 100, 72.0)
    m.observe_dwell(50.0, 72.0)
    assert m.evaluate(72.0) == Transition(False, "recovered")
    assert not m.active


def test_a_single_slow_reading_restarts_the_exit_timer():
    m = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 12):
        m.observe_cloud(3000, 100, float(t))
        m.evaluate(float(t))
    assert m.active
    for t in range(12, 70):
        m.observe_cloud(100, 100, float(t))
        assert m.evaluate(float(t)) is None
    m.observe_cloud(3000, 100, 70.0)
    assert m.evaluate(70.0) is None  # calm broken one tick before the exit
    for t in range(71, 131):
        m.observe_cloud(100, 100, float(t))
        assert m.evaluate(float(t)) is None
    m.observe_cloud(100, 100, 131.0)
    assert m.evaluate(131.0) == Transition(False, "recovered")


def test_null_lag_counts_as_calm_for_exit_and_never_enters():
    m = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 30):
        m.observe_cloud(None, None, float(t))
        assert m.evaluate(float(t)) is None
    assert not m.active


def test_null_lag_alone_lets_a_mode_entered_on_dwell_recover():
    m = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 12):
        m.observe_dwell(2500.0, now=float(t))
        m.evaluate(float(t))
    assert m.active
    # The exit timer starts at 17.0, not at 12.0: the 2500 ms samples are in
    # the tracker's five-second window until then, and the dwell is the half
    # of the pair still carrying a number.
    for t in range(12, 77):
        m.observe_cloud(None, None, float(t))  # the ping carries no baseline yet
        m.observe_dwell(50.0, float(t))
        assert m.evaluate(float(t)) is None
    m.observe_dwell(50.0, 77.0)
    assert m.evaluate(77.0) == Transition(False, "recovered")


def test_a_ping_that_stops_while_slow_ages_out_and_lets_the_mode_recover():
    m = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 12):
        m.observe_cloud(3000, 100, float(t))
        m.evaluate(float(t))
    assert m.active
    # No further ping. The last reading stays current for 30 s, so the exit
    # timer cannot start before 42.0, and the exit lands 60 s after that.
    for t in range(12, 102):
        m.observe_dwell(50.0, float(t))
        assert m.evaluate(float(t)) is None
    m.observe_dwell(50.0, 102.0)
    assert m.evaluate(102.0) == Transition(False, "recovered")


# --- forced -------------------------------------------------------------------


def test_forced_on_and_off_win_and_report_forced():
    m = LinkMode(DEFAULTS, now=0.0)
    on = LowBandwidthSettings.resolve({"mode": "on"}, {})
    assert m.update_settings(on, now=1.0) == Transition(True, "forced")
    for t in range(2, 200):
        m.observe_cloud(0, 10, float(t))
        assert m.evaluate(float(t)) is None  # stays on
    assert m.active
    off = LowBandwidthSettings.resolve({"mode": "off"}, {})
    assert m.update_settings(off, now=200.0) == Transition(False, "forced")
    for t in range(201, 300):
        m.observe_cloud(9000, 10, float(t))
        assert m.evaluate(float(t)) is None  # never enters
    auto = LowBandwidthSettings.resolve({}, {})
    assert m.update_settings(auto, now=300.0) is None  # back to auto, still off


def test_back_to_auto_starts_the_enter_timer_from_the_switch():
    m = LinkMode(LowBandwidthSettings.resolve({"mode": "off"}, {}), now=0.0)
    for t in range(1, 100):
        m.observe_cloud(9000, 10, float(t))
        assert m.evaluate(float(t)) is None
    assert m.update_settings(DEFAULTS, now=100.0) is None
    for t in range(101, 111):
        m.observe_cloud(9000, 10, float(t))
        assert m.evaluate(float(t)) is None  # the 99 s already elapsed do not count
    m.observe_cloud(9000, 10, 111.0)
    assert m.evaluate(111.0) == Transition(True, "lag")


def test_forced_on_is_active_from_construction():
    m = LinkMode(LowBandwidthSettings.resolve({"mode": "on"}, {}), now=0.0)
    assert m.active
    assert m.evaluate(1.0) is None


def test_reason_names_the_measure_that_entered_the_mode():
    """A caller that has to state the mode with no transition in hand — the
    greeting after a reconnect into a mode that was already on — needs the
    reason the mode actually has, not a guess."""
    lagging = LinkMode(DEFAULTS, now=0.0)
    lagging.observe_cloud(lag_ms=9000, latency_ms=100, now=1.0)
    for t in range(2, 12):
        lagging.evaluate(float(t))
    assert lagging.evaluate(12.0) == Transition(True, "lag")
    assert lagging.reason == "lag"

    dwelling = LinkMode(DEFAULTS, now=0.0)
    for t in range(1, 11):
        dwelling.observe_dwell(2500.0, now=float(t))
        dwelling.evaluate(float(t))
    dwelling.observe_dwell(2500.0, now=11.0)
    assert dwelling.evaluate(11.0) == Transition(True, "dwell")
    assert dwelling.reason == "dwell"


def test_reason_starts_from_no_transition_and_a_forced_mode_names_itself():
    """`lag` as the starting value is a claim nothing has made yet, and it
    survives `on` -> `auto`: the mode is then on because somebody forced it,
    not because the link was slow, and the greeting after a reconnect would
    have told the cloud otherwise."""
    assert LinkMode(DEFAULTS, now=0.0).reason == "recovered"
    forced = LinkMode(LowBandwidthSettings.resolve({"mode": "on"}, {}), now=0.0)
    assert forced.reason == "forced"
    forced.update_settings(DEFAULTS, now=1.0)
    assert forced.active and forced.reason == "forced"


def test_a_settings_change_within_auto_reports_nothing():
    m = LinkMode(DEFAULTS, now=0.0)
    tighter = LowBandwidthSettings.resolve({"enter_lag_ms": 300, "exit_lag_ms": 100}, {})
    assert m.update_settings(tighter, now=1.0) is None
    assert not m.active
