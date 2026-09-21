# SPDX-License-Identifier: Apache-2.0
"""Low-bandwidth mode: the settings, the two inputs and the decision.

Nothing here touches ROS or asyncio, so every threshold and every timer is
testable against a `float` clock instead of a sleep. `client.py` feeds it the
cloud's ping numbers and the local queue dwell and asks `evaluate` on a tick;
`ros_runtime.py` pulls the levers when a `Transition` comes back.
"""
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Mapping, Optional, Tuple

from fleetless_bridge.protocol import LOW_BANDWIDTH_DEFAULTS

_MODES = ("auto", "on", "off")
_CAMERA = ("reduce", "stop")

#: How long a ping number stays current. Pings arrive every 2 s, so this is
#: fifteen missed ones: past it the cloud has told us nothing recently, and no
#: number is not the same as a good one.
_PING_MAX_AGE_S = 30.0


@dataclass(frozen=True)
class LowBandwidthSettings:
    mode: str
    enter_lag_ms: int
    enter_after_s: int
    exit_lag_ms: int
    exit_after_s: int
    datapoint_max_hz: float
    camera: str
    camera_bitrate_kbps: int

    @classmethod
    def resolve(cls, params: Mapping[str, Any], yaml: Mapping[str, Any]) -> "LowBandwidthSettings":
        """Defaults <- ROS parameters <- `fleetless.yaml`, one layer at a time.

        A key present in the YAML wins over the parameter; a key absent in both
        is the vendored default. `None` in a layer is an undeclared or unset
        value and falls through rather than erasing the layer below it.

        Raises `ValueError` naming the key and the rule, so the parameter
        callback and the config apply refuse a bad value with the same words.
        Unknown keys are ignored on purpose: contracts refuses them in the YAML
        and ROS refuses an undeclared parameter, so nothing that arrives here
        is a typo this layer could be the first to catch.
        """
        merged: Dict[str, Any] = dict(LOW_BANDWIDTH_DEFAULTS)
        merged.update({k: v for k, v in params.items() if v is not None})
        merged.update({k: v for k, v in yaml.items() if v is not None})
        _check(merged["mode"] in _MODES, "mode", "one of auto, on, off")
        _check(_is_int(merged["enter_lag_ms"]) and merged["enter_lag_ms"] >= 100, "enter_lag_ms", "an integer >= 100")
        _check(_is_int(merged["enter_after_s"]) and merged["enter_after_s"] >= 1, "enter_after_s", "an integer >= 1")
        _check(_is_int(merged["exit_lag_ms"]) and merged["exit_lag_ms"] >= 0, "exit_lag_ms", "an integer >= 0")
        _check(_is_int(merged["exit_after_s"]) and merged["exit_after_s"] >= 1, "exit_after_s", "an integer >= 1")
        # Crossed thresholds leave no hysteresis: one reading satisfies the
        # enter rule and the exit rule, and the mode flips on every tick.
        # Contracts compares the pair only when the YAML carries both keys, so
        # a lone `exit_lag_ms` above the default is published and valid and
        # arrives here crossed. This check is the only one that sees that, from
        # the YAML apply as much as from `ros2 param set`.
        _check(merged["exit_lag_ms"] <= merged["enter_lag_ms"], "exit_lag_ms", "at or below enter_lag_ms")
        _check(
            _is_num(merged["datapoint_max_hz"]) and 0 < merged["datapoint_max_hz"] <= 20,
            "datapoint_max_hz",
            "a number in (0, 20]",
        )
        _check(merged["camera"] in _CAMERA, "camera", "reduce or stop")
        _check(
            _is_int(merged["camera_bitrate_kbps"]) and 50 <= merged["camera_bitrate_kbps"] <= 20000,
            "camera_bitrate_kbps",
            "an integer in [50, 20000]",
        )
        return cls(**{k: merged[k] for k in cls.__dataclass_fields__})


def _check(ok: bool, key: str, rule: str) -> None:
    if not ok:
        raise ValueError("low_bandwidth.{} must be {}".format(key, rule))


def _is_int(v: Any) -> bool:
    # `bool` is an `int` in Python: `True` would pass every range check below
    # as 1, which is a threshold nothing ever clears.
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


class DwellTracker:
    """p95 of the dwell samples seen in the last `window_s` seconds.

    The p95 rather than the mean or the maximum: one slow send is a scheduler
    hiccup, and a link that is actually narrow makes most sends slow.
    """

    def __init__(self, window_s: float = 5.0) -> None:
        self._window_s = window_s
        self._samples: Deque[Tuple[float, float]] = deque()

    def observe(self, dwell_ms: float, now: float) -> None:
        self._samples.append((now, dwell_ms))
        self._trim(now)

    def p95_ms(self, now: float) -> Optional[float]:
        """`None` when nothing was sent in the window — no traffic is no signal."""
        self._trim(now)
        if not self._samples:
            return None
        values = sorted(v for _, v in self._samples)
        return values[min(len(values) - 1, int(0.95 * len(values)))]

    def _trim(self, now: float) -> None:
        while self._samples and self._samples[0][0] < now - self._window_s:
            self._samples.popleft()


@dataclass(frozen=True)
class Transition:
    low_bandwidth: bool
    #: One of the contracts reasons: `lag`, `dwell`, `forced`, `recovered`.
    reason: str


class LinkMode:
    """Hysteresis over two measures of the same narrow link.

    Enter when either the cloud's lag or the local dwell exceeds
    `enter_lag_ms` for `enter_after_s`; exit when both are at or below
    `exit_lag_ms` for `exit_after_s`. `mode: on|off` overrides both and
    reports `forced`.

    A missing number counts as calm on its own side: no ping is not a slow
    link, and the dwell carries the decision while the pings are gone. A ping
    number that arrived is a level, not an event — it holds until the next one
    or until it ages out.

    The timers advance only inside `evaluate`, so the caller's tick is the
    resolution of every `_after_s` here.
    """

    def __init__(self, settings: LowBandwidthSettings, now: float) -> None:
        self._settings = settings
        self.active = settings.mode == "on"
        self._lag_ms: Optional[int] = None
        self._lag_at: float = now
        self._dwell = DwellTracker()
        self._over_since: Optional[float] = None
        self._calm_since: Optional[float] = None
        self._last_reason = "lag"

    @property
    def reason(self) -> str:
        """Which measure the mode is on for, for a caller holding no
        transition — the report after a reconnect into a mode that was
        already on. `lag` or `dwell`; it says nothing while `active` is
        False, and a forced mode names itself."""
        return self._last_reason

    def update_settings(self, settings: LowBandwidthSettings, now: float) -> Optional[Transition]:
        """New thresholds restart both timers: seconds counted against the old
        numbers say nothing about the new ones."""
        self._settings = settings
        self._over_since = self._calm_since = None
        if settings.mode == "on" and not self.active:
            self.active = True
            return Transition(True, "forced")
        if settings.mode == "off" and self.active:
            self.active = False
            return Transition(False, "forced")
        return None

    def observe_cloud(self, lag_ms: Optional[int], latency_ms: Optional[int], now: float) -> None:
        """The ping's two numbers. `latency_ms` is kept out of the decision on
        purpose: a long round trip is a far cloud, not a narrow uplink."""
        self._lag_ms = lag_ms
        self._lag_at = now

    def observe_dwell(self, dwell_ms: float, now: float) -> None:
        self._dwell.observe(dwell_ms, now)

    def evaluate(self, now: float) -> Optional[Transition]:
        """Returns the transition on the tick that crosses, `None` otherwise."""
        s = self._settings
        if s.mode != "auto":
            return None
        lag = self._lag_ms if now - self._lag_at <= _PING_MAX_AGE_S else None
        dwell = self._dwell.p95_ms(now)
        lag_over = lag is not None and lag > s.enter_lag_ms
        dwell_over = dwell is not None and dwell > s.enter_lag_ms
        calm = (lag is None or lag <= s.exit_lag_ms) and (dwell is None or dwell <= s.exit_lag_ms)
        if not self.active:
            if lag_over or dwell_over:
                if self._over_since is None:
                    self._over_since = now
                    # Whichever measure opened the window names the reason, so
                    # a mode entered with the pings gone does not report `lag`.
                    self._last_reason = "lag" if lag_over else "dwell"
                if now - self._over_since >= s.enter_after_s:
                    self.active = True
                    self._over_since = None
                    return Transition(True, self._last_reason)
            else:
                self._over_since = None
            return None
        if calm:
            if self._calm_since is None:
                self._calm_since = now
            if now - self._calm_since >= s.exit_after_s:
                self.active = False
                self._calm_since = None
                return Transition(False, "recovered")
        else:
            self._calm_since = None
        return None
