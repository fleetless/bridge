# SPDX-License-Identifier: Apache-2.0
"""goal_state.py in isolation — plain file I/O, no ROS dependency."""
import json

from fleetless_bridge.goal_state import load_mapping, save_mapping


def test_round_trips_a_mapping(tmp_path):
    path = tmp_path / "goal_state.json"
    mapping = {"job-1": ("drive_to", "goal-a"), "job-2": ("dock", "goal-b")}
    save_mapping(path, mapping)
    assert load_mapping(path) == mapping


def test_a_missing_file_loads_as_empty(tmp_path):
    assert load_mapping(tmp_path / "does-not-exist.json") == {}


def test_a_corrupt_file_loads_as_empty_rather_than_raising(tmp_path):
    path = tmp_path / "goal_state.json"
    path.write_text("{not valid json", encoding="utf-8")
    assert load_mapping(path) == {}


def test_a_truncated_entry_is_dropped_not_fatal(tmp_path):
    path = tmp_path / "goal_state.json"
    path.write_text(json.dumps({"job-1": ["drive_to"], "job-2": ["dock", "goal-b"]}), encoding="utf-8")
    assert load_mapping(path) == {"job-2": ("dock", "goal-b")}


def test_save_is_atomic_no_temp_file_left_behind(tmp_path):
    path = tmp_path / "goal_state.json"
    save_mapping(path, {"job-1": ("drive_to", "goal-a")})
    assert sorted(p.name for p in tmp_path.iterdir()) == ["goal_state.json"]


def test_save_creates_the_parent_directory(tmp_path):
    path = tmp_path / "nested" / "goal_state.json"
    save_mapping(path, {})
    assert path.exists()


def test_an_empty_mapping_round_trips(tmp_path):
    path = tmp_path / "goal_state.json"
    save_mapping(path, {})
    assert load_mapping(path) == {}


def test_save_fsyncs_the_file_and_its_directory(tmp_path, monkeypatch):
    """`os.replace` alone is atomic against a crash of this process, not
    against a power loss: without an fsync of the data before the rename
    and of the directory after it, the rename can reach the disk before
    the bytes it points at, and the next boot reads an empty or truncated
    mapping — every own job's restart survival gone at once."""
    import os

    from fleetless_bridge import goal_state

    synced = []
    real_fsync = os.fsync

    def recording_fsync(fd):
        synced.append(os.path.realpath("/proc/self/fd/{}".format(fd)))
        real_fsync(fd)

    monkeypatch.setattr(goal_state.os, "fsync", recording_fsync)
    path = tmp_path / "goal_state.json"
    save_mapping(path, {"job-1": ("drive_to", "goal-a")})
    assert synced == [str(path) + ".tmp", str(tmp_path)]
