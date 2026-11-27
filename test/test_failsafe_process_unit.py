# SPDX-License-Identifier: Apache-2.0
"""The failsafe process's loop and its supervisor, in this process, on a
fake clock and a fake node: the paths no real process reaches on cue
(fleetless/fleetless#1124)."""
import logging
import multiprocessing
import os
import queue
import threading
import time

import pytest
from geometry_msgs.msg import Twist
from std_msgs.msg import String

from fleetless_bridge.failsafe import STATE_OK, FailsafeWatchdog
from fleetless_bridge.failsafe_process import (
    OWED_MATCH_WAIT_S,
    OWED_RESEND_WINDOW_S,
    STOP_ACK_RESERVE_S,
    STOP_WAIT_S,
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
def process(monkeypatch):
    ours, theirs = multiprocessing.Pipe()
    clock = _Clock()
    proc = FailsafeProcess(_Node(), theirs, clock=clock)
    proc.clock = clock
    proc.monkeypatch = monkeypatch
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
    assert owed == [("owed", "drive", False, 0)] + [("owed", "drive", True, 0)] * 3


def test_a_matched_owed_failsafe_still_reaches_a_reader_that_matches_later(process):
    publisher = _armed_owed(process)
    _at(process, 1, publisher, 0.05)  # the bystander matched first
    _at(process, 2, publisher, 3.0)
    assert len(publisher.sent) == 2


def test_no_resend_after_a_newer_command_or_after_the_window(process):
    publisher = _armed_owed(process)
    _at(process, 0, publisher, OWED_MATCH_WAIT_S)
    process._handle(("publish", "drive", Twist(), 1))
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
    assert ("parting", "drive", TOPIC, 0) in _reports(process)
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
    assert ("parting", "drive", TOPIC, 0) in _reports(process)


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
    process._writers["drive"].publisher.readers = 1
    process._handle(("publish", "drive", "command", 1))
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    process._reap_retiring(process.clock.now + OWED_MATCH_WAIT_S)
    assert retiring.publisher.sent == []
    assert process._writers["drive"].publisher.sent == ["command"]
    assert ("superseded", "drive", TOPIC, 1) in _reports(process)


def test_a_command_by_another_slug_on_the_topic_cancels_the_owed_failsafe(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("drive2", TOPIC, TYPE, {}, 100, False)
    _loader_delivers(process)
    (retiring,) = process._retiring
    process._writers["drive2"].publisher.readers = 1
    process._handle(("publish", "drive2", "command", 1))
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert retiring.publisher.sent == []
    assert ("superseded", "drive", TOPIC, 1) in _reports(process)


def test_a_command_on_another_topic_leaves_the_owed_failsafe(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("other", "/unit/other", TYPE, {}, 100, False)
    _loader_delivers(process)
    process._writers["other"].publisher.readers = 1
    process._handle(("publish", "other", "command", 1))
    (retiring,) = process._retiring
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert len(retiring.publisher.sent) == 1


def test_a_command_no_reader_can_see_yet_leaves_the_owed_failsafe(process):
    """Lost under volatile durability, so it cannot overtake the failsafe."""
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    _loader_delivers(process)
    process._handle(("publish", "drive", "command", 1))  # its writer has no reader
    (retiring,) = process._retiring
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert len(retiring.publisher.sent) == 1


def test_a_command_that_raised_leaves_the_owed_failsafe(process):
    _pending_owed(process)
    process._handle(("disarm", "drive"))
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    _loader_delivers(process)
    writer = process._writers["drive"].publisher
    writer.readers = 1

    def raises(msg):
        raise RuntimeError("publish failed")

    writer.publish = raises
    with pytest.raises(RuntimeError):
        process._handle(("publish", "drive", "command", 1))
    (retiring,) = process._retiring
    retiring.publisher.readers = 1
    process._reap_retiring(process.clock.now + 0.1)
    assert len(retiring.publisher.sent) == 1


def test_a_command_cancels_another_publisher_s_inherited_failsafe_on_its_topic(process):
    """Two publishers on one topic after a crash: `drive`'s writer owes the
    dead process's failsafe, `steer`'s owed arm waits for its type. A
    command by `teleop` there reaches a reader: neither failsafe follows it."""
    publisher = _armed_owed(process)  # `drive`, owed_since set
    _pending_owed(process, slug="steer", type_name="geometry_msgs/msg/TwistStamped")
    process._loaded[TYPE] = Twist
    process._arm("teleop", TOPIC, TYPE, {}, 100, False)
    process._writers["teleop"].publisher.readers = 1
    process._handle(("publish", "teleop", "command", 7))
    publisher.readers = 1
    process._fire_due(process.clock.now + OWED_MATCH_WAIT_S)
    assert publisher.sent == []
    assert process._pending_arms["steer"][4] is False
    reports = _reports(process)
    assert ("superseded_owed", "drive", TOPIC, 7) in reports
    assert ("superseded_owed", "steer", TOPIC, 7) in reports


def test_a_type_that_fails_to_load_names_each_publisher_whose_failsafe_is_lost(process):
    _pending_owed(process, slug="drive")
    _pending_owed(process, slug="steer", topic="/unit/steer")
    process._handle(("disarm", "drive"))
    process._load_results.put((TYPE, None, "Traceback: no such type"))
    process._take_loaded()
    reports = _reports(process)
    assert ("owed_failed", "drive", TOPIC, 0, "Traceback: no such type") in reports
    assert ("owed_failed", "steer", "/unit/steer", 0, "Traceback: no such type") in reports


def test_a_stop_skips_a_type_the_loader_is_stuck_importing_and_sends_the_others(process, monkeypatch):
    """A loader stuck inside an import holds its module lock: the loop would
    wait on it too. Said for each of its publishers; the other types go on."""
    import fleetless_bridge.failsafe_process as fp

    monkeypatch.setattr(fp, "IMPORT_CAP_S", 0.2)
    stuck = threading.Event()
    process._loader = threading.Thread(target=stuck.wait, daemon=True)
    process._loader.start()
    process._node.readers = 1
    _pending_owed(process, slug="drive", type_name="geometry_msgs/msg/TwistStamped")
    _pending_owed(process, slug="other", topic="/unit/other")
    process._dispatched = "geometry_msgs/msg/TwistStamped"
    try:
        process._retire_owed_pending()
    finally:
        stuck.set()
    process._reap_retiring(process.clock())
    assert len(_sent(process)) == 1  # `other`'s
    failed = [r[:3] for r in _reports(process) if r[0] == "owed_failed"]
    assert failed == [("owed_failed", "drive", TOPIC)]


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
    watchdog._handle(("owed", "drive", False, 0))
    assert watchdog._owed["drive"] is True
    watchdog._handle(("owed", "drive", True, 0))
    assert watchdog._owed["drive"] is False


def test_the_bridge_keeps_a_disarmed_owed_failsafe_until_a_process_sent_it():
    """A replacement gets the arm and the disarm again, so it sends it."""
    watchdog = _watchdog()
    watchdog.disarm("drive")  # no process: nothing sent, nothing lost
    assert "drive" not in watchdog._armed
    assert watchdog._parting["drive"] == (TOPIC, TYPE, {}, 100, 0)
    watchdog._handle(("parting", "drive", TOPIC, 0))
    assert watchdog._parting == {}


@pytest.mark.parametrize("report", [
    ("owed_failed", "drive", TOPIC, 0, "Traceback"),
    ("superseded", "drive", TOPIC, 1),
    ("parting", "drive", TOPIC, 0),
    ("fired", "drive", 100, 0.5, TOPIC, 0),
])
def test_the_bridge_forgets_a_removed_owed_failsafe_dealt_with_on_its_topic(report):
    watchdog = _watchdog()
    watchdog.disarm("drive")
    watchdog._handle(report)
    assert watchdog._parting == {}


@pytest.mark.parametrize("report", [
    ("owed_failed", "drive", "/unit/other", 0, "Traceback"),
    ("superseded", "drive", "/unit/other", 1),
    ("parting", "drive", "/unit/other", 0),
    ("fired", "drive", 100, 0.5, "/unit/other", 0),
    ("fire_failed", "drive", "Traceback"),
])
def test_the_bridge_keeps_a_removed_owed_failsafe_for_a_report_about_another_topic(report):
    """The slug's writer on another topic, or its live writer's own fire."""
    watchdog = _watchdog()
    watchdog.disarm("drive")
    watchdog._handle(report)
    assert "drive" in watchdog._parting


def test_a_command_of_the_slug_on_another_topic_keeps_its_removed_owed_failsafe():
    """Removed while owed on one topic, added back on another, then a
    command: the first topic's failsafe is still owed."""
    watchdog = _watchdog()
    watchdog.disarm("drive")
    watchdog._armed["drive"] = ("/unit/other", TYPE, {}, 100)
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())
    assert watchdog._parting["drive"][0] == TOPIC


@pytest.mark.parametrize("answer", [
    lambda seq: ("fired", "drive", 100, 0.5, TOPIC, seq),
    lambda seq: ("parting", "drive", TOPIC, seq),
    lambda seq: ("owed", "drive", True, seq),
])
def test_a_newer_publish_in_flight_when_the_failsafe_is_reported_keeps_the_owed_mark(answer):
    """The failsafe answers publish 1; publish 2 is already on its way to
    the process when the report arrives. A crash now must still find
    publish 2's failsafe owed."""
    watchdog = _watchdog()
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())  # 1
    watchdog.publish("drive", Twist())  # 2, sent before the report came
    watchdog._handle(answer(1))
    assert watchdog._owed["drive"] is True
    watchdog._handle(answer(2))
    assert watchdog._owed["drive"] is False


def test_the_process_reports_which_publish_a_failsafe_answers(process):
    process._loaded[TYPE] = Twist
    process._arm("drive", TOPIC, TYPE, {}, 100, False, 4)
    publisher = process._writers["drive"].publisher
    process._fire_due(process.clock.now)
    process._handle(("publish", "drive", "command", 5))
    process._fire_due(process.clock.now + 0.2)
    fired = [r for r in _reports(process) if r[0] == "fired"]
    assert fired and fired[-1][-1] == 5
    assert publisher.sent == ["command", process._writers["drive"].failsafe]


def test_an_inherited_failsafe_answers_the_publish_the_bridge_had_made_when_it_armed_it(process):
    process._loaded[TYPE] = Twist
    process._arm("drive", TOPIC, TYPE, {}, 100, True, 9)
    publisher = process._writers["drive"].publisher
    _at(process, 1, publisher, 0.05)
    assert ("owed", "drive", True, 9) in _reports(process)


def test_the_bridge_clears_an_owed_mark_superseded_only_by_a_newer_command():
    """Another publisher's command superseded `drive`'s inherited failsafe;
    a command of `drive` sent after that one keeps `drive` owed."""
    watchdog = _watchdog()
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())  # publish 1
    watchdog._handle(("superseded_owed", "drive", TOPIC, 2))  # by publish 2
    assert watchdog._owed["drive"] is False
    watchdog.publish("drive", Twist())  # publish 3, sent before the report came
    watchdog._handle(("superseded_owed", "drive", TOPIC, 2))
    assert watchdog._owed["drive"] is True


def test_a_removed_writer_s_replayed_failsafe_leaves_the_slug_owed_for_its_command_on_a_new_topic():
    """Owed on one topic, removed, added back on another and driven there,
    then a restart: the replayed removal answers the slug's publish at its
    removal, so its report leaves the newer command owed."""
    watchdog = _watchdog()
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())  # 1, on TOPIC
    watchdog.disarm("drive")
    assert watchdog._parting["drive"][4] == 1  # what the replay sends
    watchdog._armed["drive"] = ("/unit/new", TYPE, {}, 100)
    watchdog._owed["drive"] = False
    watchdog.publish("drive", Twist())  # 2, on the new topic
    watchdog._handle(("parting", "drive", TOPIC, 1))  # the replacement sent the old one
    assert watchdog._owed["drive"] is True
    assert watchdog._parting == {}


def _replacement(proc):
    """The process as a replacement of a failed one, in its first seconds."""
    proc._replacement_until = proc.clock() + OWED_RESEND_WINDOW_S


def test_a_replacement_resends_a_fire_to_the_robot_s_reader_matching_late(process):
    """After a crash only a recorder had matched: it got the owed failsafe,
    the command and its failsafe. The robot's reader matches after the
    deadline, and still gets the failsafe."""
    _replacement(process)
    publisher = _armed_owed(process)
    _at(process, 1, publisher, 0.05)  # the recorder: the owed failsafe
    process._handle(("publish", "drive", "command", 1))
    _at(process, 1, publisher, 0.2)  # the command's failsafe fires
    assert len(publisher.sent) == 3
    _at(process, 2, publisher, 1.0)  # the robot's reader
    assert len(publisher.sent) == 4
    assert publisher.sent[-1] is process._writers["drive"].failsafe
    assert ("resent", "drive", TOPIC) in _reports(process)
    _at(process, 2, publisher, 1.0)
    assert len(publisher.sent) == 4  # not again to the same readers


def test_a_replacement_resends_the_superseding_publisher_s_fire_to_a_late_reader(process):
    """`teleop`'s command reached only a recorder and superseded `drive`'s
    inherited failsafe: `teleop`'s own failsafe reaches the robot's reader
    that matches later."""
    _replacement(process)
    drive = _armed_owed(process)
    process._arm("teleop", TOPIC, TYPE, {}, 100, False)
    teleop = process._writers["teleop"].publisher
    teleop.readers = 1
    process._handle(("publish", "teleop", "command", 1))
    process._fire_due(process.clock.now + 0.2)
    assert teleop.sent == ["command", process._writers["teleop"].failsafe]
    drive.readers = 2
    teleop.readers = 2
    process._fire_due(process.clock.now + 1.5)
    assert len(teleop.sent) == 3
    assert drive.sent == []


def _reap(proc, after_s):
    proc.clock.now += after_s
    proc._reap_retiring(proc.clock.now)


def test_a_removed_publisher_s_owed_failsafe_reaches_the_robot_s_reader_matching_late(process):
    """In a replacement's window, removed while it owes the dead process's
    failsafe, with only a recorder matched: the writer is kept, and the
    robot's reader that matches later gets the failsafe too, once."""
    _replacement(process)
    publisher = _armed_owed(process)
    publisher.readers = 1  # the recorder, before the owed failsafe went out
    process._handle(("disarm", "drive"))
    _reap(process, 0.05)
    assert len(publisher.sent) == 1 and process._retiring  # sent, kept
    _reap(process, 5.0)
    assert process._retiring  # acknowledged, still kept
    publisher.readers = 2  # the robot
    _reap(process, 0.05)
    assert len(publisher.sent) == 2
    _reap(process, 1.0)
    assert len(publisher.sent) == 2
    assert ("resent", "drive", TOPIC) in _reports(process)


def test_a_publisher_removed_in_the_window_sends_its_parting_failsafe_to_a_late_reader(process):
    """`teleop`'s command reached only a recorder, superseding `drive`'s
    inherited failsafe, and `teleop` is removed before its deadline: its
    parting failsafe reaches the robot's reader that matches afterwards."""
    _replacement(process)
    drive = _armed_owed(process)
    _at(process, 1, drive, 0.05)  # `drive`'s owed failsafe, to the recorder
    process._arm("teleop", TOPIC, TYPE, {}, 100, False)
    teleop = process._writers["teleop"].publisher
    teleop.readers = 1
    process._handle(("publish", "teleop", "command", 1))
    process._handle(("disarm", "teleop"))
    assert teleop.sent == ["command", process._retiring[0].failsafe]
    for publisher in (drive, teleop):
        publisher.readers = 2
    for _ in range(10):
        process.clock.now += 0.1
        process._fire_due(process.clock.now)
        process._reap_retiring(process.clock.now)
    assert len(teleop.sent) == 3  # once more, for the robot


def test_a_command_on_the_topic_ends_a_kept_retiring_writer(process):
    _replacement(process)
    publisher = _armed_owed(process)
    publisher.readers = 1
    process._handle(("disarm", "drive"))
    _reap(process, 0.05)
    assert len(publisher.sent) == 1
    process._arm("teleop", TOPIC, TYPE, {}, 100, False)
    process._writers["teleop"].publisher.readers = 1
    process._handle(("publish", "teleop", "command", 1))
    publisher.readers = 2
    _reap(process, 0.05)
    assert len(publisher.sent) == 1  # the command's own failsafe covers it
    assert not process._retiring


def test_outside_a_replacement_a_retiring_writer_is_not_kept(process):
    publisher = _armed_owed(process)
    publisher.readers = 1
    process._handle(("disarm", "drive"))
    _reap(process, 0.05)
    assert len(publisher.sent) == 1
    assert not process._retiring


def test_a_stop_does_not_wait_out_the_replacement_window(process):
    _replacement(process)
    publisher = _armed_owed(process)
    publisher.readers = 1
    process._handle(("disarm", "drive"))
    _reap(process, 0.05)
    assert process._retiring
    assert process._handle(("stop",)) is False
    assert not process._retiring


def test_a_kept_writer_does_not_hold_back_a_new_writer_of_its_type(process):
    """Another type waits for the keep (below)."""
    _replacement(process)
    publisher = _armed_owed(process)
    publisher.readers = 1
    process._handle(("disarm", "drive"))
    _reap(process, 0.05)
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    assert process._writers["drive"].publisher is not None


def _retargeted_in_the_window(proc):
    """`drive` driven in a replacement's window, then armed again on its
    topic with another type: its old writer is kept, the new one waits."""
    _replacement(proc)
    proc._loaded[TYPE] = Twist
    proc._loaded["std_msgs/msg/String"] = String
    proc._arm("drive", TOPIC, TYPE, {}, 100, False)
    old = proc._writers["drive"].publisher
    old.readers = 1  # a recorder
    proc._handle(("publish", "drive", "command", 1))
    proc._arm("drive", TOPIC, "std_msgs/msg/String", {}, 100, False)
    assert old.sent == ["command", proc._retiring[0].failsafe]
    return old


def test_a_new_type_on_the_topic_waits_while_the_old_type_s_writer_is_kept(process):
    """A late reader of the old type still gets the parting failsafe."""
    old = _retargeted_in_the_window(process)
    process.clock.now += 1.0
    process._create_pending(process.clock.now)
    assert process._writers["drive"].publisher is None
    process._handle(("publish", "drive", "dropped meanwhile", 2))
    assert ("dropped", "drive") in _reports(process)
    old.readers = 2  # the robot, still on the old type
    _reap(process, 0.05)
    assert len(old.sent) == 3
    assert old not in process._node.destroyed


def test_the_new_type_s_writer_is_created_once_the_window_ends(process):
    """The keep ends with the window; acknowledged, the old writer goes, and
    the new type's writer follows."""
    old = _retargeted_in_the_window(process)
    process.clock.now = process._replacement_until + 0.01
    process._reap_retiring(process.clock.now)
    assert old in process._node.destroyed
    process._create_pending(process.clock.now)
    assert process._writers["drive"].publisher is not None


def test_a_kept_writer_not_acknowledged_at_the_window_s_end_leaves_after_the_type_change_wait(
        process, monkeypatch):
    old = _retargeted_in_the_window(process)
    monkeypatch.setattr(process, "_acked", lambda publisher: False)
    process.clock.now = process._replacement_until + 0.01
    # A turn's order: pending writers first, then the retiring ones.
    process._create_pending(process.clock.now)
    process._reap_retiring(process.clock.now)  # the keep ends
    process._create_pending(process.clock.now)
    assert process._writers["drive"].publisher is None
    process.clock.now += 0.6
    process._create_pending(process.clock.now)
    assert process._writers["drive"].publisher is not None
    assert old in process._node.destroyed


def test_a_waiting_writer_that_owed_a_failsafe_is_reported_when_it_goes(process):
    _retargeted_in_the_window(process)
    process._writers["drive"].owed_since = process.clock()
    process._handle(("disarm", "drive"))
    assert ("unsent_owed", "drive", TOPIC) in _reports(process)


def _advancing(proc, step_s=0.05, at=None):
    """The process's clock moves on with each look, as a stop's waits need,
    and the waits do not sleep; `at` = (seconds, action) runs once that much
    time passed."""
    import fleetless_bridge.failsafe_process as fp

    proc.monkeypatch.setattr(fp.time, "sleep", lambda s: None)
    start = proc.clock.now

    def clock():
        proc.clock.now += step_s
        if at is not None and proc.clock.now - start >= at[0] and not getattr(clock, "done", False):
            clock.done = True
            at[1]()
        return proc.clock.now

    proc._clock = clock
    return start


def test_a_stop_waits_for_a_reader_as_long_as_its_shared_deadline_allows(process):
    """The robot's reader matches 3.5 s into the stop, past the old 2 s
    match wait: the owed failsafe still goes out to it, and is not said to
    have gone to no reader."""
    _pending_owed(process)
    publishers = process._node.publishers

    def robot_matches():
        for publisher in publishers:
            publisher.readers = 1

    sent_at = []
    _advancing(process, at=(3.5, robot_matches))
    real_retire = process._retire_owed

    def retire(slug, arm):
        real_retire(slug, arm)
        publisher = process._retiring[-1].publisher
        original = publisher.publish
        publisher.publish = lambda msg: (sent_at.append((process.clock.now, publisher.readers)),
                                         original(msg))

    process._retire_owed = retire
    assert process._handle(("stop",)) is False
    assert len(sent_at) == 1 and sent_at[0][1] == 1
    assert not [r for r in _reports(process) if r[0] == "unheard"]


def test_a_stop_s_owed_failsafe_that_found_no_reader_is_said(process):
    _pending_owed(process)
    start = _advancing(process)
    assert process._handle(("stop",)) is False
    assert len(_sent(process)) == 1
    assert ("unheard", "drive", TOPIC) in _reports(process)
    assert process.clock.now - start <= STOP_WAIT_S + 0.2
    assert process.clock.now - start >= STOP_WAIT_S - STOP_ACK_RESERVE_S


def test_a_stop_s_parting_failsafe_with_no_reader_is_said(process):
    process._loaded[TYPE] = Twist
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    process._handle(("publish", "drive", "command", 1))
    _advancing(process)
    assert process._handle(("stop",)) is False
    assert ("unheard", "drive", TOPIC) in _reports(process)


def _recording_sends(proc):
    """Each send of a writer created from here on, with the clock and the
    readers matched then."""
    sent = []
    create = proc._node.create_publisher

    def created(message_class, topic, depth):
        publisher = create(message_class, topic, depth)
        original = publisher.publish
        publisher.publish = lambda msg: (sent.append((proc.clock.now, publisher.readers)),
                                         original(msg))
        return publisher

    proc._node.create_publisher = created
    return sent


@pytest.mark.parametrize("load_s", [2.5, 3.5])
def test_a_stop_s_deadline_holds_through_a_slow_type_load(process, load_s):
    """The type of an owed arm takes seconds to load at the stop: its send,
    and the word that it reached nobody, still come inside the stop's
    shared deadline, so inside the bridge's wait for the process."""
    sent = _recording_sends(process)
    _pending_owed(process)
    start = _advancing(process)
    process._await_loader = lambda type_name: setattr(process.clock, "now",
                                                      process.clock.now + load_s)
    assert process._handle(("stop",)) is False
    assert len(sent) == 1
    assert sent[0][0] - start <= STOP_WAIT_S + 0.2
    assert ("unheard", "drive", TOPIC) in _reports(process)


def _kept_after_sending_to_nobody(proc):
    """In a replacement's window, `drive` removed while it owed the dead
    process's failsafe; that failsafe went out with no reader, and the
    writer is kept."""
    _replacement(proc)
    publisher = _armed_owed(proc)
    proc._handle(("disarm", "drive"))
    _reap(proc, OWED_MATCH_WAIT_S + 0.1)
    assert len(publisher.sent) == 1 and proc._retiring[0].sent_to == 0
    return publisher


def test_a_stop_waits_for_a_reader_for_a_kept_failsafe_that_reached_nobody(process):
    publisher = _kept_after_sending_to_nobody(process)
    _advancing(process, at=(1.0, lambda: setattr(publisher, "readers", 1)))
    assert process._handle(("stop",)) is False
    assert len(publisher.sent) == 2
    assert not [r for r in _reports(process) if r[0] == "unheard"]


def test_a_stop_says_a_kept_failsafe_reached_nobody_when_no_reader_came(process):
    publisher = _kept_after_sending_to_nobody(process)
    _advancing(process)
    assert process._handle(("stop",)) is False
    assert ("unheard", "drive", TOPIC) in _reports(process)
    assert len(publisher.sent) == 2


def test_a_stop_says_the_teardown_s_parting_failsafe_reached_nobody(process):
    """In a replacement's window, a command, then the runtime's teardown
    removes the publisher before the stop: its parting failsafe went to no
    reader. The stop waits for one, and says it when none came."""
    _replacement(process)
    process._loaded[TYPE] = Twist
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    publisher = process._writers["drive"].publisher
    process._handle(("publish", "drive", "command", 1))
    process._handle(("disarm", "drive"))
    _advancing(process)
    assert process._handle(("stop",)) is False
    assert ("unheard", "drive", TOPIC) in _reports(process)
    assert len(publisher.sent) == 3  # the command, its parting failsafe, again at the stop


def test_a_stop_gives_no_word_for_a_failsafe_a_bystander_had(process):
    """Reached a reader, perhaps only a recorder: no wait and no word, as
    an armed writer's."""
    _replacement(process)
    publisher = _armed_owed(process)
    publisher.readers = 1
    process._handle(("disarm", "drive"))
    _reap(process, 0.05)
    _advancing(process)
    assert process._handle(("stop",)) is False
    assert len(publisher.sent) == 1
    assert not [r for r in _reports(process) if r[0] == "unheard"]


def test_a_stop_s_send_that_raised_is_not_said_to_have_reached_nobody(process):
    sent = _recording_sends(process)
    _pending_owed(process)
    _advancing(process)
    create = process._node.create_publisher

    def failing(message_class, topic, depth):
        publisher = create(message_class, topic, depth)

        def raises(msg):
            raise RuntimeError("publish failed")

        publisher.publish = raises
        return publisher

    process._node.create_publisher = failing
    assert process._handle(("stop",)) is False
    reports = _reports(process)
    assert [r[0] for r in reports if r[0] in ("owed_failed", "unheard")] == ["owed_failed"]
    assert sent == []


def test_a_stop_is_not_held_by_a_new_type_waiting_for_a_kept_writer(process):
    _retargeted_in_the_window(process)
    assert process._handle(("stop",)) is False
    assert not process._retiring


def test_outside_a_replacement_a_fire_is_not_resent(process):
    process._loaded[TYPE] = Twist
    process._arm("drive", TOPIC, TYPE, {}, 100, False)
    publisher = process._writers["drive"].publisher
    publisher.readers = 1
    process._handle(("publish", "drive", "command", 1))
    _at(process, 1, publisher, 0.2)
    _at(process, 2, publisher, 1.0)
    assert len(publisher.sent) == 2


def _in_replacement_window(watchdog, clock):
    watchdog._replacement_until = clock() + OWED_RESEND_WINDOW_S


@pytest.mark.parametrize("report", [
    ("owed", "drive", True, 1),
    ("fired", "drive", 100, 0.5, TOPIC, 1),
])
def test_in_a_replacement_s_window_a_send_keeps_the_owed_mark_until_the_window_ends(report):
    """The send may have reached only a recorder; if this process fails
    too, the next one must send it again."""
    clock = _Clock()
    watchdog = FailsafeWatchdog("unit", clock=clock)
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())  # 1
    _in_replacement_window(watchdog, clock)
    watchdog._handle(report)
    assert watchdog._owed["drive"] is True
    clock.now += OWED_RESEND_WINDOW_S
    with watchdog._lock:
        watchdog._end_replacement_window(clock())
    assert watchdog._owed["drive"] is False


def test_in_a_replacement_s_window_a_removed_writer_s_record_stays_until_the_window_ends():
    clock = _Clock()
    watchdog = FailsafeWatchdog("unit", clock=clock)
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = True
    watchdog.disarm("drive")
    _in_replacement_window(watchdog, clock)
    watchdog._handle(("parting", "drive", TOPIC, 0))
    assert "drive" in watchdog._parting
    clock.now += OWED_RESEND_WINDOW_S
    with watchdog._lock:
        watchdog._end_replacement_window(clock())
    assert watchdog._parting == {}


def test_in_a_replacement_s_window_a_command_that_reached_a_reader_clears_at_once():
    clock = _Clock()
    watchdog = FailsafeWatchdog("unit", clock=clock)
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = True
    watchdog._armed["steer"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["steer"] = True
    watchdog.disarm("drive")
    _in_replacement_window(watchdog, clock)
    watchdog._handle(("superseded", "drive", TOPIC, 1))
    watchdog._handle(("superseded_owed", "steer", TOPIC, 5))
    assert watchdog._parting == {}
    assert watchdog._owed["steer"] is False


def test_a_stop_ends_the_bridge_s_replacement_window():
    clock = _Clock()
    watchdog = FailsafeWatchdog("unit", clock=clock)
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = True
    _in_replacement_window(watchdog, clock)
    watchdog._handle(("owed", "drive", True, 0))
    watchdog.stop()
    assert watchdog._owed["drive"] is False
    assert watchdog._replacement_until is None


def test_a_publisher_retargeted_while_owed_is_recorded_as_a_removal():
    """Armed again on another topic or type: the old arm is kept, with its
    publish number, for a replacement to send its failsafe."""
    watchdog = _watchdog()
    watchdog._state = STATE_OK
    watchdog.publish("drive", Twist())  # 1
    watchdog.arm("drive", "/unit/new", TYPE, {}, 100)
    assert watchdog._parting["drive"] == (TOPIC, TYPE, {}, 100, 1)
    assert watchdog._owed["drive"] is False
    watchdog.arm("drive", "/unit/new", TYPE, {"x": 1}, 100)  # same topic and type
    assert watchdog._parting["drive"][0] == TOPIC


def test_the_window_s_end_keeps_a_newer_removal_record_of_the_same_slug_and_topic():
    """Removed while owed, its parting failsafe sent (held); added back,
    driven and removed again on the same topic: the held send answers the
    first removal only, so the second record survives the window's end."""
    clock = _Clock()
    watchdog = FailsafeWatchdog("unit", clock=clock)
    watchdog._state = STATE_OK
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = False
    _in_replacement_window(watchdog, clock)
    watchdog.publish("drive", Twist())  # 1
    watchdog.disarm("drive")
    watchdog._handle(("parting", "drive", TOPIC, 1))
    watchdog.arm("drive", TOPIC, TYPE, {}, 100)
    watchdog.publish("drive", Twist())  # 2
    watchdog.disarm("drive")
    assert watchdog._parting["drive"][4] == 2
    clock.now += OWED_RESEND_WINDOW_S
    with watchdog._lock:
        watchdog._end_replacement_window(clock())
    assert watchdog._parting["drive"][4] == 2


class _Logged(logging.Handler):
    """The suite's `caplog` misses these; a handler on the module logger,
    as the runtime's tests do."""

    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


@pytest.fixture
def logged():
    handler = _Logged()
    logger = logging.getLogger("fleetless_bridge.failsafe")
    logger.addHandler(handler)
    yield handler.lines
    logger.removeHandler(handler)


def _two_removals(watchdog):
    """`drive` removed while owed (seq 1), added back, driven and removed
    again on the same topic (seq 2)."""
    watchdog._state = STATE_OK
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = False
    watchdog.publish("drive", Twist())  # 1
    watchdog.disarm("drive")
    watchdog.arm("drive", TOPIC, TYPE, {}, 100)
    watchdog.publish("drive", Twist())  # 2
    watchdog.disarm("drive")
    assert watchdog._parting["drive"][4] == 2


def test_a_failure_to_send_the_first_removal_s_failsafe_keeps_the_newer_record():
    watchdog = FailsafeWatchdog("unit")
    _two_removals(watchdog)
    watchdog._handle(("owed_failed", "drive", TOPIC, 1, "Traceback"))
    assert watchdog._parting["drive"][4] == 2
    watchdog._handle(("owed_failed", "drive", TOPIC, 2, "Traceback"))
    assert watchdog._parting == {}


def test_a_late_superseded_report_keeps_a_removal_made_since_the_command():
    """The first removal superseded by publish 2; the removal made after it
    answers that very publish and stays."""
    watchdog = FailsafeWatchdog("unit")
    _two_removals(watchdog)
    watchdog._handle(("superseded", "drive", TOPIC, 2))
    assert watchdog._parting["drive"][4] == 2
    watchdog._handle(("superseded", "drive", TOPIC, 3))
    assert watchdog._parting == {}


def test_the_bridge_logs_a_stop_s_failsafe_that_went_to_no_reader(logged):
    watchdog = FailsafeWatchdog("unit")
    watchdog._handle(("unheard", "drive", TOPIC))
    assert any("'drive'" in line and TOPIC in line and "no reader" in line for line in logged)


class _Stopping:
    """A process that ends once its pipe was read."""

    def __init__(self, ends_after):
        self.calls = 0
        self.ends_after = ends_after

    def is_alive(self):
        self.calls += 1
        return self.calls <= self.ends_after

    def join(self, timeout_s):
        pass


def test_a_stop_reads_what_the_process_said_before_it_exited(logged):
    """The process is gone already; what it said last is still in the pipe,
    up to its end."""
    watchdog = FailsafeWatchdog("unit")
    ours, theirs = multiprocessing.Pipe()
    try:
        theirs.send(("parting", "drive", TOPIC, 0))
        theirs.send(("unheard", "drive", TOPIC))
        theirs.send(("fired", "drive", 100, 50.0, TOPIC, 1))  # nothing that fails it at a stop
        theirs.close()
        started = time.monotonic()
        watchdog._join_reading(_Stopping(ends_after=0), ours, 5.0)
        assert time.monotonic() - started < 1.0  # the pipe's end, not the deadline
        assert any("no reader" in line for line in logged)
        assert watchdog.state != "failed"
    finally:
        ours.close()


def test_a_stop_does_not_wait_when_nothing_is_owed():
    watchdog = FailsafeWatchdog("unit")
    watchdog._supervisor = threading.Thread(target=threading.Event().wait, args=(5.0,), daemon=True)
    watchdog._supervisor.start()
    watchdog._armed["drive"] = (TOPIC, TYPE, {}, 100)
    watchdog._owed["drive"] = False
    started = time.monotonic()
    watchdog._await_replacement_for_owed()
    assert time.monotonic() - started < 0.5
