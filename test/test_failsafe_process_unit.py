# SPDX-License-Identifier: Apache-2.0
"""The failsafe process's loop and its supervisor, in this process, on a
fake clock and a fake node: the paths no real process reaches on cue
(fleetless/fleetless#1124)."""
import multiprocessing
import os
import queue
import threading

import pytest
from geometry_msgs.msg import Twist

from fleetless_bridge.failsafe import FailsafeWatchdog
from fleetless_bridge.failsafe_process import (
    OWED_MATCH_WAIT_S,
    OWED_RESEND_WINDOW_S,
    FailsafeProcess,
)

TYPE = "geometry_msgs/msg/Twist"
TOPIC = "/unit/cmd_vel"


class _Publisher:
    def __init__(self):
        self.readers = 0
        self.sent = []

    def get_subscription_count(self):
        return self.readers

    def publish(self, msg):
        self.sent.append(msg)


class _Node:
    def __init__(self):
        self.publishers = []

    def create_publisher(self, message_class, topic, depth):
        publisher = _Publisher()
        self.publishers.append(publisher)
        return publisher

    def destroy_publisher(self, publisher):
        pass


class _Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


@pytest.fixture
def process():
    ours, theirs = multiprocessing.Pipe()
    clock = _Clock()
    proc = FailsafeProcess(_Node(), theirs, clock=clock)
    proc.clock = clock
    yield proc
    proc._load_requests.put(None)
    os.close(proc._loaded_r)
    os.close(proc._loaded_w)
    ours.close()
    theirs.close()


def _reports(proc):
    out = []
    while True:
        try:
            out.append(proc._outbox.get_nowait())
        except queue.Empty:
            return out


def _armed_owed(proc):
    """A writer for `drive` that owes a previous process's failsafe."""
    proc._loaded[TYPE] = Twist
    proc._arm("drive", TOPIC, TYPE, {}, 100, True)
    writer = proc._writers["drive"]
    assert writer.publisher is not None and writer.owed_since is not None
    return writer.publisher


def _at(proc, readers, publisher, after_s):
    proc.clock.now += after_s
    publisher.readers = readers
    proc._fire_due(proc.clock.now)


def test_an_owed_failsafe_goes_out_again_each_time_another_reader_matches(process):
    """The first reader may be a recorder: every reader that joins within
    the window gets the failsafe again, the robot's controller included."""
    publisher = _armed_owed(process)
    _at(process, 0, publisher, OWED_MATCH_WAIT_S)  # sent to nobody
    assert len(publisher.sent) == 1
    _at(process, 1, publisher, 1.0)  # a `ros2 topic echo`
    _at(process, 1, publisher, 0.05)
    assert len(publisher.sent) == 2
    _at(process, 2, publisher, 1.0)  # the controller
    assert len(publisher.sent) == 3
    _at(process, 1, publisher, 1.0)  # the echo leaves
    _at(process, 2, publisher, 1.0)  # the controller restarts
    assert len(publisher.sent) == 4
    owed = [r for r in _reports(process) if r[0] == "owed"]
    assert owed == [("owed", "drive", False)] + [("owed", "drive", True)] * 3


def test_a_matched_owed_failsafe_still_reaches_a_reader_that_matches_later(process):
    publisher = _armed_owed(process)
    _at(process, 1, publisher, 0.05)  # the bystander matched first
    _at(process, 2, publisher, 3.0)
    assert len(publisher.sent) == 2


def test_no_resend_after_a_newer_command_or_after_the_window(process):
    publisher = _armed_owed(process)
    _at(process, 0, publisher, OWED_MATCH_WAIT_S)
    process._handle(("publish", "drive", Twist()))
    _at(process, 1, publisher, 0.05)
    assert len(publisher.sent) == 2  # the owed failsafe, then the command
    assert process._writers["drive"].resend_until is None

    process._writers.clear()
    publisher = _armed_owed(process)
    _at(process, 0, publisher, OWED_MATCH_WAIT_S)
    _at(process, 1, publisher, OWED_RESEND_WINDOW_S)
    assert len(publisher.sent) == 1


def test_a_dead_loader_s_waiting_arms_are_armed_by_its_replacement(process):
    """The loader dies with a type in hand: the type goes to a new loader,
    and the arm that waited for it gets its writer."""
    process._loader = threading.Thread(target=lambda: None)
    process._loader.start()
    process._loader.join()
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    assert "drive" in process._pending_arms
    process._dispatch_loads(process.clock())  # handed to the dead loader
    assert process._dispatched == TYPE
    process._dispatch_loads(process.clock())  # found dead, replaced
    assert [r for r in _reports(process) if r[0] == "error"]
    assert "drive" in process._pending_arms
    process._dispatch_loads(process.clock())  # handed to the new one
    assert process._dispatched == TYPE
    result = process._load_results.get(timeout=30.0)
    process._load_results.put(result)
    process._take_loaded()
    assert "drive" not in process._pending_arms
    assert process._writers["drive"].publisher is not None


def _watchdog():
    watchdog = FailsafeWatchdog("unit")
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = True
    return watchdog


def test_the_bridge_keeps_a_failsafe_owed_until_a_matched_reader_got_it():
    """Sent to nobody, it is still owed: a process that replaces this one
    before any reader matched sends it too."""
    watchdog = _watchdog()
    watchdog._handle(("owed", "drive", False))
    assert watchdog._owed["drive"] is True
    watchdog._handle(("owed", "drive", True))
    assert watchdog._owed["drive"] is False
