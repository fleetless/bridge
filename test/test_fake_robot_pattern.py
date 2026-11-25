# SPDX-License-Identifier: Apache-2.0
"""`tools/camera_pattern.py`: the fake robot's opt-in test picture, and the
switch that chooses it. Off unless asked for; a typo is refused, not read as
the webcam."""
import importlib.util
import pathlib
import subprocess

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
