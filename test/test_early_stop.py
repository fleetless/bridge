# SPDX-License-Identifier: Apache-2.0
"""A stop in the bridge's first seconds: heard at once, and quiet.

A container is often stopped right after it started, and a supervisor reads
nothing but the exit code. So a SIGINT or a SIGTERM ends the bridge with
exit 0, within a second and without a stack trace, whenever it comes: during
the imports, while the ROS node is being created, before the cloud has
answered.

Every test here runs the bridge the way a robot does, as a process of its
own (`python3 -m fleetless_bridge.main`), against the suite's fake cloud. A
signal sent to the test process itself would say nothing about the seconds
before the bridge's own code runs.

The seam tests are the proof: each sends its signal at one exact step of the
start. The sweep is the net under them: twenty moments for each way of being
stopped, placed by what the bridge has done so far. For a run by hand,
`FLEETLESS_EARLY_STOP_MOMENTS=40` asks for forty instead, evenly over the
bridge's first five seconds; `./run-tests.sh` passes the variable on.
"""
import asyncio
import logging
import os
import pathlib
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

import pytest
from fake_cloud import FakeCloud, accepts

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _cores() -> float:
    """The cores this process can use, which on a small runner are few: the
    ones it may run on, and no more than a container's CPU limit gives it.
    Not the cores the machine has."""
    try:
        cores = float(len(os.sched_getaffinity(0)))
    except AttributeError:  # not Linux
        cores = float(os.cpu_count() or 1)
    limit = _cpu_limit()
    return cores if limit is None else min(cores, limit)


def _cpu_limit() -> Optional[float]:
    """A container's CPU limit in cores, where one is set and can be read."""
    try:  # cgroup v2: "<quota> <period>", or "max <period>" for no limit
        quota, period = pathlib.Path("/sys/fs/cgroup/cpu.max").read_text().split()
        return None if quota == "max" else int(quota) / int(period)
    except (OSError, ValueError, ZeroDivisionError):
        pass
    try:  # cgroup v1: a quota of -1 is no limit
        quota = int(pathlib.Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text())
        period = int(pathlib.Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text())
        return quota / period if quota > 0 and period > 0 else None
    except (OSError, ValueError):
        return None


#: How long a stop may take, from the signal to the process being gone: a
#: second, where the bridge has a core to itself.
#:
#: A handler in Python runs between two steps of the interpreter, never
#: inside one, so a stop that lands inside one long C call of the imports
#: waits for that call; `import cv2` alone is one call of about half a second
#: of an idle core. Below two usable cores the bridge shares its core with
#: this test and the fake cloud, and now and then the half second becomes
#: more than one. That says nothing about the bridge, so there a stop gets
#: three seconds. Everything else it has to be stays what it is.
STOP_WITHIN_S = 1.0 if _cores() >= 2 else 3.0

#: How many bridges start at once. Each one is a second of imports on a core
#: of its own, so a few run side by side without slowing each other into a
#: different test: four, where there are cores for them. Where there are not,
#: fewer, because a bridge that waits for a core cannot stop within a second
#: either, and that says nothing about the bridge. Half the cores at most:
#: the test and its fake cloud need theirs.
SIDE_BY_SIDE = max(1, min(4, int(_cores() // 2)))

#: Where the clock starts: the first moment the bridge is seen to catch both
#: signals (`SigCgt` in `/proc/<pid>/status`). Until a Python program's first
#: statement runs it cannot listen: the interpreter is starting, and the
#: kernel answers a signal, not the program. Nothing in the bridge can change
#: that, so no stop is sent there, and no stopwatch decides when "there" is
#: over: a loaded machine takes as long as it takes.
#:
#: What the bridge can change is how much it does before it listens, and that
#: is asked without a stopwatch too. At the moment it is first seen to
#: listen, it must not have logged its start yet (`_ITS_START`), which it
#: does once the imports are done. A bridge that listens late fails every
#: stop for it.
#:
#: This long is only how long a bridge that never listens is waited for.
LISTENS_WITHIN_S = 20.0

#: The bridge's first log line, written after the imports and before ROS.
_ITS_START = "Starting fleetless_bridge"

#: How long a bridge is waited for to log that line, for a moment counted
#: from it. On a shared core the imports alone take several seconds.
STARTS_WITHIN_S = 60.0

#: What a moment is counted from.
_IT_LISTENS = "it listens"
_ITS_FIRST_LINE = "its first log line"


@dataclass(frozen=True)
class _Moment:
    """When a stop is sent: so long after the bridge was seen to do something."""

    after: str
    seconds: float

    def __str__(self) -> str:
        return "{:.3f} s after {}".format(self.seconds, self.after)


#: The sweep in the suite: twenty moments for each way of being stopped.
#:
#: Placed by what the bridge has done and not by a clock alone, because where
#: its first seconds go depends on the machine: the imports take a second on
#: a free core and several on a shared one, and everything after them moves
#: with that.
#:
#:   six     in the imports, counted from the moment the bridge listens
#:   twelve  from its first log line on, close together: before ROS, inside
#:           ROS initialisation and node creation, before the loop listens,
#:           around the first connection
#:   two     later, with the connection up
_TWENTY = (
    [_Moment(_IT_LISTENS, s) for s in (0.0, 0.05, 0.15, 0.3, 0.5, 0.75)]
    + [
        _Moment(_ITS_FIRST_LINE, s)
        for s in (0.0, 0.01, 0.02, 0.035, 0.05, 0.065, 0.08, 0.1, 0.13, 0.2, 0.35, 0.7)
    ]
    + [_Moment(_ITS_FIRST_LINE, s) for s in (1.5, 3.0)]
)

#: The sweep by hand: forty moments, evenly over the five seconds after the
#: bridge listens.
_FORTY = [_Moment(_IT_LISTENS, 5.0 * step / 40) for step in range(40)]

_ASKED_FOR = os.environ.get("FLEETLESS_EARLY_STOP_MOMENTS", "").strip()
if _ASKED_FOR not in ("", "20", "40"):
    raise ValueError(
        "FLEETLESS_EARLY_STOP_MOMENTS is {!r}; it is 40 for the sweep by hand, "
        "and unset or 20 for the suite's".format(_ASKED_FOR)
    )

#: The moments a stop is sent at.
MOMENTS = _FORTY if _ASKED_FOR == "40" else _TWENTY

_SIGNALS = {"SIGINT": signal.SIGINT, "SIGTERM": signal.SIGTERM}


@dataclass
class _Stop:
    """One bridge, stopped once: what was done to it and how it ended."""

    moment: _Moment
    signal_name: str
    listening_after_s: Optional[float]
    started_before_it_listened: bool
    signal_sent: bool
    exit_code: Optional[int]
    stopped_after_s: Optional[float]
    output: str

    def problems(self) -> List[str]:
        if self.listening_after_s is None:
            # No signal was sent: there was nobody to hear it.
            return [
                "never caught both signals (exit code {})".format(self.exit_code)
                if self.exit_code is not None
                else "not catching both signals {:.0f} s after its birth".format(LISTENS_WITHIN_S)
            ]
        if not self.signal_sent:
            # Nor here: the moment is counted from a line that never came.
            return [
                "never logged its start (exit code {})".format(self.exit_code)
                if self.exit_code is not None
                else "had not logged its start {:.0f} s after its birth".format(STARTS_WITHIN_S)
            ]
        found = []
        if self.started_before_it_listened:
            found.append("had logged its start before it listened")
        if self.stopped_after_s is None:
            found.append("still running {:.0f} s after the signal".format(STOP_WITHIN_S))
        elif self.exit_code != 0:
            found.append("exit code {}".format(self.exit_code))
        if "Traceback" in self.output:
            found.append("a stack trace")
        if "unexpected error" in self.output:
            found.append("`unexpected error` in the log")
        if _error_lines(self.output):
            found.append("a log line at error level")
        if self.stopped_after_s is not None and self.output.count("Bridge stopped") != 1:
            found.append(
                "`Bridge stopped` logged {} times".format(self.output.count("Bridge stopped"))
            )
        return found

    def describe(self) -> str:
        listening = (
            "listening {:.3f} s after its birth".format(self.listening_after_s)
            if self.listening_after_s is not None
            else "no signal sent"
        )
        return "{} {} ({}): {}\n{}".format(
            self.signal_name,
            self.moment,
            listening,
            ", ".join(self.problems()),
            _indented(_tail(self.output)),
        )


def _error_lines(output: str) -> List[str]:
    # The bridge's own format: `<date> <time> <LEVEL> <logger>: <message>`.
    # A line counts from the level of an error upwards.
    return [
        line
        for line in output.splitlines()
        if len(line.split()) > 2 and _level(line.split()[2]) >= logging.ERROR
    ]


def _level(name: str) -> int:
    """The number of a log level's name, and 0 for a word that is none."""
    number = logging.getLevelName(name)
    return number if isinstance(number, int) else 0


def _tail(output: str, lines: int = 12) -> str:
    kept = output.splitlines()[-lines:]
    return "\n".join(kept) if kept else "(no output)"


def _indented(text: str) -> str:
    return "\n".join("    " + line for line in text.splitlines())


def _bridge_env(cloud_url: str, state_dir: pathlib.Path) -> dict:
    env = dict(os.environ)
    env["FLEETLESS_TOKEN"] = "frt_abc"
    env["FLEETLESS_CLOUD_URL"] = cloud_url
    # The goal mapping of one bridge is not another's.
    env["FLEETLESS_STATE_DIR"] = str(state_dir)
    return env


def _spawn(argv, env, log_file, *, sigint_ignored: bool) -> subprocess.Popen:
    """Start a process, with SIGINT ignored at its birth if asked.

    An ignored signal is inherited through `fork` and `exec`, which is how a
    shell's background job and a container's entrypoint hand it down. So the
    test ignores it itself for the length of the `Popen` call, which returns
    once the child has called `exec`. Not `preexec_fn`: that runs Python
    between `fork` and `exec`, in a process that may hold another thread's
    lock.
    """
    previous = signal.signal(signal.SIGINT, signal.SIG_IGN) if sigint_ignored else None
    try:
        return subprocess.Popen(
            argv,
            cwd=str(ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    finally:
        if sigint_ignored:
            signal.signal(signal.SIGINT, previous)


def _catches_both(pid: int) -> bool:
    """Whether the process has a handler of its own for SIGINT and SIGTERM."""
    try:
        with open("/proc/{}/status".format(pid)) as status:
            for line in status:
                if line.startswith("SigCgt:"):
                    caught = int(line.split()[1], 16)
                    return all(caught >> (number - 1) & 1 for number in _SIGNALS.values())
    except OSError:
        pass
    return False


async def _stop_at(
    moment: _Moment,
    signal_name: str,
    *,
    sigint_ignored: bool,
    cloud_url: str,
    workdir: pathlib.Path,
) -> _Stop:
    workdir.mkdir()
    log_path = workdir / "bridge.log"
    with open(log_path, "wb") as log_file:
        bridge = _spawn(
            [sys.executable, "-m", "fleetless_bridge.main"],
            _bridge_env(cloud_url, workdir),
            log_file,
            sigint_ignored=sigint_ignored,
        )
    born = time.monotonic()
    listening_after_s = None
    started_before_it_listened = False
    signal_sent = False
    stopped_after_s = None
    ended_by_itself = False

    def has_logged_its_start() -> bool:
        return _ITS_START in log_path.read_text(errors="replace")

    try:
        while bridge.poll() is None and time.monotonic() - born < LISTENS_WITHIN_S:
            if _catches_both(bridge.pid):
                listening_after_s = time.monotonic() - born
                # Read after it was seen to listen, never before: a line
                # that stands in the log now may have been written first.
                started_before_it_listened = has_logged_its_start()
                break
            await asyncio.sleep(0.002)

        # What the moment is counted from has to have happened.
        reached = listening_after_s is not None
        if reached and moment.after == _ITS_FIRST_LINE:
            reached = False
            while bridge.poll() is None and time.monotonic() - born < STARTS_WITHIN_S:
                if has_logged_its_start():
                    reached = True
                    break
                await asyncio.sleep(0.002)
        ended_by_itself = bridge.poll() is not None

        if reached:
            await asyncio.sleep(moment.seconds)
            bridge.send_signal(_SIGNALS[signal_name])
            signal_sent = True
            sent = time.monotonic()
            while time.monotonic() - sent < STOP_WITHIN_S:
                if bridge.poll() is not None:
                    stopped_after_s = time.monotonic() - sent
                    break
                await asyncio.sleep(0.005)
    finally:
        if bridge.poll() is None:
            bridge.kill()
        bridge.wait()
    return _Stop(
        moment=moment,
        signal_name=signal_name,
        listening_after_s=listening_after_s,
        started_before_it_listened=started_before_it_listened,
        signal_sent=signal_sent,
        exit_code=bridge.returncode if stopped_after_s is not None or ended_by_itself else None,
        stopped_after_s=stopped_after_s,
        output=log_path.read_text(errors="replace"),
    )


async def _stop_at_every_moment(
    signal_name: str, *, sigint_ignored: bool, workdir: pathlib.Path
) -> List[_Stop]:
    async with FakeCloud(accepts()) as cloud:
        turn = asyncio.Semaphore(SIDE_BY_SIDE)

        async def one(index: int, moment: _Moment) -> _Stop:
            async with turn:
                return await _stop_at(
                    moment,
                    signal_name,
                    sigint_ignored=sigint_ignored,
                    cloud_url=cloud.url,
                    workdir=workdir / "stop-{:02d}".format(index),
                )

        return await asyncio.gather(*(one(index, moment) for index, moment in enumerate(MOMENTS)))


@pytest.mark.parametrize(
    "signal_name, sigint_ignored",
    [
        pytest.param("SIGINT", False, id="SIGINT"),
        pytest.param("SIGTERM", False, id="SIGTERM"),
        pytest.param("SIGINT", True, id="SIGINT-ignored-at-birth"),
    ],
)
def test_a_stop_at_any_moment_of_its_first_seconds_ends_the_bridge_quietly(
    signal_name, sigint_ignored, tmp_path
):
    stops = asyncio.run(
        _stop_at_every_moment(signal_name, sigint_ignored=sigint_ignored, workdir=tmp_path)
    )
    assert len(stops) == len(MOMENTS)
    failed = [stop for stop in stops if stop.problems()]
    assert not failed, "{} of {} stops did not end the bridge quietly:\n\n{}".format(
        len(failed), len(stops), "\n\n".join(stop.describe() for stop in failed)
    )


# --- a stop at a seam of the start ------------------------------------------

#: The bridge, with one thing arranged before it starts: at the seam `argv[1]`
#: names, the process does what `argv[2]` says, and then carries on. A signal
#: sent from there lands exactly between two steps of the start, which no
#: sleep can promise.
#:
#:   during-the-imports the first slow import has begun, nothing is started
#:   before-ros         the imports are done, ROS has not been initialised
#:   before-the-node    `rclpy.init` has returned, the node does not exist yet
#:   before-the-loop    the node exists, the event loop does not listen yet
_AT_A_SEAM = """
import os, signal, sys

where, what = sys.argv[1], sys.argv[2]

def first(original):
    def then(*args, **kwargs):
        for word in what.split(","):
            if word == "fail":
                raise RuntimeError("the node could not be created")
            os.kill(os.getpid(), getattr(signal, word))
        return original(*args, **kwargs)
    return then

def saying_so(sentence, original):
    def then(*args, **kwargs):
        result = original(*args, **kwargs)
        sys.stderr.write(sentence + "\\n")
        return result
    return then

class AtTheFirstSlowImport:
    # Asked for every module before it is looked for. It finds none; it is
    # only there when the bridge's client is asked for, which is the first
    # import of the bridge that takes time.
    def __init__(self):
        self.then = first(lambda: None)

    def find_spec(self, name, path=None, target=None):
        if name == "fleetless_bridge.client" and self.then is not None:
            then, self.then = self.then, None
            then()
        return None

if where == "during-the-imports":
    # Nothing slow is imported here: this seam is in the bridge's own
    # imports, wherever the bridge does them.
    sys.meta_path.insert(0, AtTheFirstSlowImport())
    from fleetless_bridge import main
else:
    import rclpy
    from fleetless_bridge import main, ros_runtime

    rclpy.init = saying_so("ROS was initialised", rclpy.init)
    rclpy.shutdown = saying_so("ROS was shut down", rclpy.shutdown)
    if where == "before-ros":
        ros_runtime.RosRuntime.__init__ = first(ros_runtime.RosRuntime.__init__)
    elif where == "before-the-node":
        rclpy.create_node = first(rclpy.create_node)
    elif where == "before-the-loop":
        main._install_signal_handlers = first(main._install_signal_handlers)
    else:
        sys.exit("no such seam: " + where)

main.main()
"""


def _run_with(what: str, *, at: str, tmp_path, sigint_ignored: bool = False):
    """Run the bridge to its end; returns `(exit code, output)`."""
    log_path = tmp_path / "bridge.log"
    with open(log_path, "wb") as log_file:
        bridge = _spawn(
            [sys.executable, "-c", _AT_A_SEAM, at, what],
            # Nothing listens there. A bridge that stops never gets as far
            # as the cloud, and one that does not is ended by the timeout.
            _bridge_env("ws://127.0.0.1:9/bridge", tmp_path),
            log_file,
            sigint_ignored=sigint_ignored,
        )
    try:
        bridge.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass
    finally:
        if bridge.poll() is None:
            bridge.kill()
            bridge.wait()
    return bridge.returncode, log_path.read_text(errors="replace")


@pytest.mark.parametrize(
    "seam", ["during-the-imports", "before-ros", "before-the-node", "before-the-loop"]
)
@pytest.mark.parametrize(
    "signal_name, sigint_ignored",
    [
        pytest.param("SIGINT", False, id="SIGINT"),
        pytest.param("SIGTERM", False, id="SIGTERM"),
        pytest.param("SIGINT", True, id="SIGINT-ignored-at-birth"),
    ],
)
def test_a_stop_at_a_seam_of_the_start_ends_the_bridge_quietly(
    seam, signal_name, sigint_ignored, tmp_path
):
    exit_code, output = _run_with(
        signal_name, at=seam, tmp_path=tmp_path, sigint_ignored=sigint_ignored
    )
    assert "Traceback" not in output, output
    assert "unexpected error" not in output, output
    assert not _error_lines(output), output
    assert exit_code == 0, output
    assert output.count("Received {} — shutting down".format(signal_name)) == 1, output
    assert output.count("Bridge stopped") == 1, output
    # What was not started needs no undoing: a stop that came before ROS
    # does not start it on the way out. What was started is undone: the
    # bridge leaves through the way out that shuts ROS down, not past it.
    ros_was_started = seam in ("before-the-node", "before-the-loop")
    assert ("ROS was initialised" in output) == ros_was_started, output
    assert ("ROS was shut down" in output) == ros_was_started, output
    # A stop during the imports ends the bridge before it has logged its
    # start; from then on the start is in the log.
    assert (_ITS_START in output) == (seam != "during-the-imports"), output


def test_a_second_signal_while_the_bridge_is_stopping_changes_nothing(tmp_path):
    # The first signal is the one being acted on. The second is the other
    # one, so that a bridge which lets the later signal win shows it.
    exit_code, output = _run_with("SIGTERM,SIGINT,SIGINT", at="before-the-node", tmp_path=tmp_path)
    assert "Traceback" not in output, output
    assert exit_code == 0, output
    assert output.count("Received SIGTERM — shutting down") == 1, output
    assert "Received SIGINT" not in output, output
    assert "ROS was shut down" in output, output
    assert output.count("Bridge stopped") == 1, output


def test_an_error_while_starting_is_still_an_error(tmp_path):
    # Hearing a stop early must not turn into hearing one that was never
    # sent: a start that fails says so and exits 1, as it always did.
    exit_code, output = _run_with("fail", at="before-the-node", tmp_path=tmp_path)
    assert exit_code == 1, output
    assert "The bridge stopped with an unexpected error" in output, output
    assert "the node could not be created" in output, output
    assert "Bridge stopped" not in output, output
