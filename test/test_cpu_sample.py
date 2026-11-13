# SPDX-License-Identifier: Apache-2.0
"""`tools/cpu-sample/`: the share of one core a process used over a window,
read from `/proc`, and the stalled-queue lines its log gained meanwhile."""
import importlib.util
import io
import os
import pathlib
import re
import subprocess
import sys
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "cpu_sample", ROOT / "tools" / "cpu-sample" / "cpu_sample.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cpu_sample = _load()


def _share(said):
    return float(re.search(r"RESULT[^:]*: ([0-9.]+) % of one core", said).group(1))


# --- the arithmetic --------------------------------------------------------------


def test_cpu_time_is_read_from_the_fields_after_the_command():
    stat = "4242 (python3) S 1 4242 4242 0 -1 4194304 100 0 0 0 250 50 7 3 20 0 9 0 123456 1000 200"
    assert cpu_sample.cpu_ticks(stat) == 300


def test_a_command_with_spaces_and_parentheses_does_not_shift_the_fields():
    stat = "4242 (my (odd) name) x) R 1 4242 4242 0 -1 4194304 100 0 0 0 11 22 7 3 20 0 9 0 1 1 1"
    assert cpu_sample.cpu_ticks(stat) == 33


@pytest.mark.parametrize(
    "before, after, seconds, hz, share",
    [(0, 30000, 300.0, 100, 100.0), (1000, 14200, 300.0, 100, 44.0), (0, 0, 300.0, 100, 0.0),
     (500, 2000, 5.0, 100, 300.0), (0, 250, 10.0, 250, 10.0)],
)
def test_the_share_is_cpu_seconds_over_seconds(before, after, seconds, hz, share):
    assert cpu_sample.share_of_one_core(before, after, seconds, hz) == pytest.approx(share)


# --- real processes ----------------------------------------------------------------


def test_a_busy_process_is_about_one_core_and_a_sleeping_one_is_about_none():
    busy = subprocess.Popen([sys.executable, "-c", "while True: pass"])
    idle = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        time.sleep(0.3)  # past the interpreter's own start
        shares = {}
        for name, process in (("busy", busy), ("idle", idle)):
            out = io.StringIO()
            assert cpu_sample.main(
                ["--pid", str(process.pid), "--seconds", "1.5", "--interval", "0", "--label", name],
                out=out,
            ) == 0
            assert "RESULT {}: ".format(name) in out.getvalue()
            shares[name] = _share(out.getvalue())
    finally:
        busy.kill()
        idle.kill()
        busy.wait()
        idle.wait()
    # A machine that is doing other things gives a busy loop less than a
    # whole core; it never gives a sleeper one.
    assert 40.0 < shares["busy"] < 115.0
    assert shares["idle"] < 10.0


def test_the_intervals_are_printed_on_the_way():
    out = io.StringIO()
    code = cpu_sample.main(
        ["--pid", str(os.getpid()), "--seconds", "0.6", "--interval", "0.2"], out=out
    )
    lines = out.getvalue().splitlines()
    assert code == 0
    assert lines[0].startswith("cpu_sample: pid {}, 0.6 s".format(os.getpid()))
    assert lines[-1].startswith("RESULT: ")
    assert 1 <= len([line for line in lines if line.endswith(" %") and line.startswith("  ")]) <= 3


def test_a_process_that_ends_during_the_window_is_no_result():
    short = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.2)"])

    def sleep(seconds):
        short.wait()  # gone, and reaped: its /proc entry with it

    out = io.StringIO()
    code = cpu_sample.main(["--pid", str(short.pid), "--seconds", "5"], out=out, sleep=sleep)
    assert code == 1
    assert "ended" in out.getvalue() and "RESULT" not in out.getvalue()


def test_a_pid_that_is_not_there_is_exit_2():
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait()
    out = io.StringIO()
    assert cpu_sample.main(["--pid", str(gone.pid), "--seconds", "1"], out=out) == 2


# --- the log -------------------------------------------------------------------------


def test_only_the_stalled_queue_lines_the_log_gained_during_the_window_are_counted(tmp_path):
    log = tmp_path / "bridge.log"
    stall = "[ERROR] The snapshot pull timed out — the ROS work queue is stalled\n"
    log.write_text(stall * 5 + "something else\n")  # before the window: not counted

    def sleep(seconds):
        with log.open("a") as handle:
            handle.write("a frame arrived\n" + stall + "The snapshot was sent\n" + stall + stall)
        time.sleep(0.05)

    out = io.StringIO()
    code = cpu_sample.main(
        ["--pid", str(os.getpid()), "--seconds", "0.05", "--interval", "0", "--log", str(log)],
        out=out, sleep=sleep,
    )
    assert code == 0
    assert out.getvalue().splitlines()[-1].endswith(", 3 stalled-queue line(s)")


def test_the_line_counted_is_the_one_the_bridge_logs():
    source = (ROOT / "fleetless_bridge" / "client.py").read_text() + (
        ROOT / "fleetless_bridge" / "ros_runtime.py"
    ).read_text()
    assert cpu_sample.STALL_LINE in source


# --- finding the process ----------------------------------------------------------------


def _fake_proc(tmp_path, processes):
    for pid, argv in processes.items():
        (tmp_path / str(pid)).mkdir()
        (tmp_path / str(pid) / "cmdline").write_bytes(b"\0".join(a.encode() for a in argv) + b"\0")
    (tmp_path / "self").mkdir()
    (tmp_path / "meminfo").write_text("not a process")
    return str(tmp_path)


def test_the_one_process_that_matches_is_found(tmp_path):
    proc = _fake_proc(tmp_path, {
        101: ["/usr/bin/python3", "/opt/ros/humble/lib/fleetless_bridge/bridge", "--ros-args"],
        102: ["/usr/bin/python3", "-m", "something_else"],
        103: ["python3", "cpu_sample.py", "--match", "fleetless_bridge"],
    })
    # 103 is this tool's own command line, which always contains the text.
    assert cpu_sample.find_pid("fleetless_bridge", proc=proc, own_pid=103) == 101


def test_none_or_several_matches_are_refused_and_named(tmp_path):
    proc = _fake_proc(tmp_path, {
        101: ["python3", "/a/fleetless_bridge/bridge"],
        102: ["python3", "/b/fleetless_bridge/bridge"],
    })
    with pytest.raises(LookupError) as several:
        cpu_sample.find_pid("fleetless_bridge", proc=proc, own_pid=1)
    assert "2 processes match" in str(several.value)
    assert "101 python3 /a/fleetless_bridge/bridge" in str(several.value)
    assert "102 python3 /b/fleetless_bridge/bridge" in str(several.value)
    with pytest.raises(LookupError) as none:
        cpu_sample.find_pid("no-such-program", proc=proc, own_pid=1)
    assert "0 processes match" in str(none.value)


def test_a_match_that_finds_nothing_is_exit_2():
    out = io.StringIO()
    assert cpu_sample.main(["--match", "no-such-program-894", "--seconds", "1"], out=out) == 2
    assert "0 processes match" in out.getvalue()
