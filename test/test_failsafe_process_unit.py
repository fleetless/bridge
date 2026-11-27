# SPDX-License-Identifier: Apache-2.0
"""The failsafe process's loop and its supervisor, in this process, on a
fake clock and a fake node: the paths no real process reaches on cue
(fleetless/fleetless#1124)."""
import multiprocessing
import os
import queue
import threading
import time

import pytest
from geometry_msgs.msg import Twist

from fleetless_bridge.failsafe import STATE_OK, FailsafeWatchdog
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
        self.destroyed = []
        # Readers a new writer finds already matched.
        self.readers = 0

    def create_publisher(self, message_class, topic, depth):
        publisher = _Publisher()
        publisher.readers = self.readers
        self.publishers.append(publisher)
        return publisher

    def destroy_publisher(self, publisher):
        self.destroyed.append(publisher)



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


def _pending_owed(proc, slug="drive", type_name=TYPE, topic=TOPIC):
    """An arm that owes a previous process's failsafe, its type not loaded."""
    proc._arm(slug, topic, type_name, {}, 100, True)
    assert slug in proc._pending_arms and slug not in proc._writers


def _loader_delivers(proc, type_name=TYPE, message_class=Twist):
    proc._load_results.put((type_name, message_class, None))
    proc._take_loaded()


def _sent(proc):
    return [msg for p in proc._node.publishers for msg in p.sent]


def test_an_owed_arm_disarmed_while_its_type_loads_still_sends_its_failsafe(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    assert "drive" not in process._pending_arms
    _loader_delivers(process)
    (retiring,) = process._retiring
    publisher = retiring.publisher
    process._reap_retiring(process.clock())  # no reader yet: waits for one
    assert publisher.sent == [] and process._retiring
    publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert len(publisher.sent) == 1
    assert ("parting", "drive") in _reports(process)
    assert not process._writers and not process._pending_arms


def test_an_owed_failsafe_with_no_reader_goes_out_after_the_match_wait(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    _loader_delivers(process)
    process._reap_retiring(process.clock.now + OWED_MATCH_WAIT_S)
    assert len(_sent(process)) == 1
    assert not process._retiring  # nobody to acknowledge it


def test_a_stop_sends_the_failsafe_of_an_owed_arm_still_waiting_for_its_type(process):
    """Loaded on the loop's thread at a stop: nothing is timed any more."""
    process._node.readers = 1
    _pending_owed(process)
    assert process._handle(("stop",)) is False
    assert len(_sent(process)) == 1
    assert not process._retiring


def test_the_bridge_gone_sends_the_failsafe_of_an_owed_arm_still_waiting_for_its_type(process):
    process._node.readers = 1
    _pending_owed(process)
    process._bridge_gone()
    assert len(_sent(process)) == 1


def test_a_writer_whose_owed_failsafe_went_to_nobody_sends_it_again_when_disarmed(process):
    publisher = _armed_owed(process)
    _at(process, 0, publisher, OWED_MATCH_WAIT_S)  # sent with no reader matched
    assert len(publisher.sent) == 1
    process._handle(("disarm", "drive"))
    assert publisher not in process._node.destroyed  # retiring, not gone
    publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.5)
    assert len(publisher.sent) == 2


def test_a_writer_whose_owed_failsafe_reached_a_reader_sends_it_again_only_for_a_new_one(process):
    """Removed after its owed failsafe went to a matched reader (perhaps a
    recorder): not again to that reader, but to one that matches before the
    writer is gone, as the resend does."""
    publisher = _armed_owed(process)
    _at(process, 1, publisher, 0.05)
    assert len(publisher.sent) == 1
    process._handle(("disarm", "drive"))
    process._reap_retiring(process.clock.now + 0.5)
    assert len(publisher.sent) == 1  # the same reader
    publisher.readers = 2
    process._reap_retiring(process.clock.now + 1.0)
    assert len(publisher.sent) == 2


def test_a_writer_whose_owed_failsafe_reached_its_readers_goes_without_a_second(process):
    publisher = _armed_owed(process)
    _at(process, 1, publisher, 0.05)
    process._handle(("disarm", "drive"))
    process._reap_retiring(process.clock.now + OWED_MATCH_WAIT_S)
    assert len(publisher.sent) == 1
    assert not process._retiring
    assert ("parting", "drive") in _reports(process)


def test_a_newer_arm_keeps_the_failsafe_a_waiting_arm_owed(process):
    _pending_owed(process)
    process._arm("drive", TOPIC, TYPE, {"linear": {"x": 0.5}}, 200, False)
    assert process._pending_arms["drive"][4] is True
    process._arm("drive", "/unit/other", TYPE, {}, 100, False)
    assert process._pending_arms["drive"][4] is False
    assert [slug for slug, _ in process._parting_pending] == ["drive"]


def test_a_newer_command_on_the_topic_cancels_a_removed_writer_s_owed_failsafe(process):
    """Removed and added back while its owed failsafe waits: a command from
    the new writer comes after it, and the old failsafe must not follow
    the command from the other writer."""
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    _loader_delivers(process)
    (retiring,) = process._retiring
    process._handle(("publish", "drive", "command"))
    for publisher in process._node.publishers:
        publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    process._reap_retiring(process.clock.now + OWED_MATCH_WAIT_S)
    assert retiring.publisher.sent == []
    assert process._writers["drive"].publisher.sent == ["command"]
    assert ("superseded", "drive") in _reports(process)


def test_a_command_by_another_slug_on_the_topic_cancels_the_owed_failsafe(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("drive2", TOPIC, TYPE, {}, 100, False)
    _loader_delivers(process)
    (retiring,) = process._retiring
    process._handle(("publish", "drive2", "command"))
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert retiring.publisher.sent == []
    assert ("superseded", "drive") in _reports(process)


def test_a_command_on_another_topic_leaves_the_owed_failsafe(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("other", "/unit/other", TYPE, {}, 100, False)
    _loader_delivers(process)
    process._handle(("publish", "other", "command"))
    (retiring,) = process._retiring
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert len(retiring.publisher.sent) == 1


def test_a_type_that_fails_to_load_names_each_publisher_whose_failsafe_is_lost(process):
    _pending_owed(process, slug="drive")
    _pending_owed(process, slug="steer", topic="/unit/steer")
    process._handle(("disarm", "drive"))
    process._load_results.put((TYPE, None, "Traceback: no such type"))
    process._take_loaded()
    reports = _reports(process)
    assert ("fire_failed", "drive", "Traceback: no such type") in reports
    assert ("fire_failed", "steer", "Traceback: no such type") in reports


def test_a_stop_takes_the_loader_s_result_for_a_type_it_is_importing(process, monkeypatch):
    """Not imported a second time on the loop: two threads importing one
    package at once can deadlock in Python's import locks."""
    import rosidl_runtime_py.utilities

    def not_on_the_loop(name):
        raise AssertionError("imported on the loop: " + name)

    monkeypatch.setattr(rosidl_runtime_py.utilities, "get_message", not_on_the_loop)
    stop = threading.Event()
    process._loader = threading.Thread(target=stop.wait, daemon=True)
    process._loader.start()
    process._node.readers = 1
    _pending_owed(process)
    process._dispatched = TYPE
    threading.Timer(0.2, process._load_results.put, args=((TYPE, Twist, None),)).start()
    try:
        process._retire_owed_pending()
    finally:
        stop.set()
    process._reap_retiring(process.clock())
    assert len(_sent(process)) == 1


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


def test_the_bridge_keeps_a_disarmed_owed_failsafe_until_a_process_sent_it():
    """A replacement gets the arm and the disarm again, so it sends it."""
    watchdog = _watchdog()
    watchdog.disarm("drive")  # no process: nothing sent, nothing lost
    assert "drive" not in watchdog._armed
    assert watchdog._parting["drive"] == (TOPIC, TYPE, {}, 100)
    watchdog._handle(("parting", "drive"))
    assert watchdog._parting == {}


@pytest.mark.parametrize("report", [("fire_failed", "drive", "Traceback"), ("superseded", "drive")])
def test_the_bridge_forgets_a_removed_owed_failsafe_that_failed_or_was_superseded(report):
    watchdog = _watchdog()
    watchdog.disarm("drive")
    watchdog._handle(report)
    assert watchdog._parting == {}


def test_a_newer_command_of_the_slug_forgets_its_removed_owed_failsafe():
    watchdog = _watchdog()
    watchdog.disarm("drive")
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())
    assert watchdog._parting == {}


def test_a_stop_does_not_wait_when_nothing_is_owed():
    watchdog = FailsafeWatchdog("unit")
    watchdog._supervisor = threading.Thread(target=threading.Event().wait, args=(5.0,), daemon=True)
    watchdog._supervisor.start()
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = False
    started = time.monotonic()
    watchdog._await_replacement_for_owed()
    assert time.monotonic() - started < 0.5
