# SPDX-License-Identifier: Apache-2.0
"""`tools/camera_pattern.py`: the fake robot's opt-in test picture, and the
switch that chooses it. Off unless asked for; a typo is refused, not read as
the webcam."""
import importlib.util
import os
import pathlib
import signal
import subprocess
import sys
import time

import numpy as np
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "camera_pattern", ROOT / "tools" / "camera_pattern.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


camera_pattern = _load()


def test_the_source_is_the_device_unless_the_pattern_is_asked_for():
    assert camera_pattern.camera_source(None) == "device"
    assert camera_pattern.camera_source("") == "device"
    assert camera_pattern.camera_source("device") == "device"
    assert camera_pattern.camera_source("pattern") == "pattern"


def test_an_unknown_source_is_refused_naming_the_two_it_takes():
    with pytest.raises(ValueError, match=r"'patern'.*device, pattern"):
        camera_pattern.camera_source("patern")


def test_a_frame_is_bgr8_in_the_size_the_seed_configures():
    frame = camera_pattern.pattern_frame(0)
    assert frame.shape == (480, 640, 3)
    assert frame.dtype == np.uint8
    assert frame.flags["C_CONTIGUOUS"]


def test_consecutive_frames_differ_so_a_frozen_stream_shows_as_one():
    frames = [camera_pattern.pattern_frame(i) for i in range(40)]
    for before, after in zip(frames, frames[1:]):
        assert not np.array_equal(before, after)


def test_a_frame_of_another_size_keeps_that_size():
    assert camera_pattern.pattern_frame(7, width=320, height=240).shape == (240, 320, 3)


def test_run_fake_robot_passes_the_source_only_when_it_is_set():
    script = (ROOT / "run-fake-robot.sh").read_text()
    assert 'if [ -n "${FAKE_CAMERA_SOURCE:-}" ]; then' in script
    assert '-e "FAKE_CAMERA_SOURCE=${FAKE_CAMERA_SOURCE}"' in script
    assert '"${camera_flags[@]}"' in script
    subprocess.run(["bash", "-n", str(ROOT / "run-fake-robot.sh")], check=True)


def test_fake_robot_reads_the_source_from_the_pattern_module():
    source = (ROOT / "tools" / "fake_robot.py").read_text()
    assert "from camera_pattern import camera_source, pattern_frame" in source
    assert 'CAMERA_SOURCE = camera_source(os.environ.get("FAKE_CAMERA_SOURCE"))' in source


def test_fake_robot_shuts_ros_down_only_when_rclpy_has_not_already():
    # `docker stop` sends SIGTERM; rclpy's own handler shuts the context down,
    # and an unconditional second shutdown ended every stop in a traceback.
    source = (ROOT / "tools" / "fake_robot.py").read_text()
    assert "        if rclpy.ok():\n            rclpy.shutdown()\n" in source
    assert "        node.destroy_node()\n        rclpy.shutdown()\n" not in source


# --- the stop: rclpy's SIGTERM handler, then spin ---------------------------------


def _load_fake_robot():
    # fake_robot.py imports its sibling `camera_pattern` the way
    # `python3 tools/fake_robot.py` finds it: from its own directory.
    sys.path.insert(0, str(ROOT / "tools"))
    try:
        spec = importlib.util.spec_from_file_location(
            "fake_robot", ROOT / "tools" / "fake_robot.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(ROOT / "tools"))


def test_a_stop_ends_quietly_and_a_real_failure_still_raises():
    from rclpy.executors import ExternalShutdownException
    from rclpy.impl.implementation_singleton import rclpy_implementation as rclpy_impl

    fake_robot = _load_fake_robot()
    rcl_error = rclpy_impl.RCLError("context is not valid")
    # The three shapes a stop takes.
    assert fake_robot.stops_quietly(KeyboardInterrupt(), ros_ok=True)
    assert fake_robot.stops_quietly(ExternalShutdownException(), ros_ok=False)
    assert fake_robot.stops_quietly(rcl_error, ros_ok=False)
    # An RCLError while ROS is still up is not a stop.
    assert not fake_robot.stops_quietly(rcl_error, ros_ok=True)
    # Anything else raises, whatever rclpy says.
    assert not fake_robot.stops_quietly(RuntimeError("boom"), ros_ok=False)
    assert not fake_robot.stops_quietly(SystemExit(3), ros_ok=False)


def test_main_lets_only_what_stops_quietly_names_go():
    source = (ROOT / "tools" / "fake_robot.py").read_text()
    assert (
        "    except BaseException as error:\n"
        "        if not stops_quietly(error, rclpy.ok()):\n"
        "            raise\n"
    ) in source
    assert "    except KeyboardInterrupt:\n        pass\n" not in source


def test_sigterm_stops_the_fake_robot_without_a_traceback():
    # What `docker stop` does, three times. The race the decision above
    # covers is rare (one stop in sixteen), so this pins the plain path and
    # the exit status; the cases themselves are pinned one test up.
    env = dict(os.environ, FAKE_CAMERA_SOURCE="pattern")
    for _ in range(3):
        proc = subprocess.Popen(
            [sys.executable, str(ROOT / "tools" / "fake_robot.py")],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
        )
        said = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            line = proc.stdout.readline()
            if not line:
                break
            said.append(line)
            if "fake_robot publishing" in line:
                break
        assert any("fake_robot publishing" in line for line in said), "".join(said)
        time.sleep(1.0)
        proc.send_signal(signal.SIGTERM)
        rest, _ = proc.communicate(timeout=30)
        output = "".join(said) + rest
        assert proc.returncode == 0, output
        assert "Traceback" not in output, output
