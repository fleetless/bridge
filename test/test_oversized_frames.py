# SPDX-License-Identifier: Apache-2.0
"""No text frame over `BRIDGE_FRAME_MAX_BYTES` (fleetless/fleetless#1222).

The cloud closes the socket at 2 MiB with 1009 and the bridge would
reconnect into the same frame, so every frame that can grow that large on
its own is shaped before it reaches the writer, and the writer refuses the
rest. One file for the four answers: datapoints are dropped and reported,
job frames shed feedback then result, the graph is cut to fit, and the
writer guard drops anything else without closing the connection.

The module-level `log` is monkeypatched rather than read through `caplog`,
for the reason `test_client_writer.py` records.
"""
import asyncio
import json
from collections import deque

import pytest
from schemas import validate_frame

import fleetless_bridge.client as client_module
from fleetless_bridge.client import (
    DatapointDropGuard,
    PrioritizedWriter,
    _BackfillSource,
    _DropReportSource,
    _JobFrameLog,
    _JobSource,
    _SampleSource,
    _referenced_names,
)
from fleetless_bridge.protocol import (
    BRIDGE_FRAME_MAX_BYTES,
    Config,
    JobStatusEntry,
    RosSource,
    datapoint_dropped_message,
    datapoint_message,
    fit_introspect_message,
    fit_job_status,
    fit_job_update,
    frame_bytes,
    introspect_message,
    job_update_message,
    type_definitions_message,
)
from fleetless_bridge.sampling import Sample

JOB = "22222222-2222-4222-8222-222222222222"
OTHER_JOB = "33333333-3333-4333-8333-333333333333"
# 3 MB of JSON once serialised: a whole map, an action's map as its result.
BIG = list(range(600_000))


class RecordingLog:
    def __init__(self):
        self.warnings = []
        self.errors = []

    def warning(self, fmt, *args):
        self.warnings.append(fmt % args if args else fmt)

    def error(self, fmt, *args):
        self.errors.append(fmt % args if args else fmt)

    def __getattr__(self, name):
        return lambda *a, **kw: None


@pytest.fixture
def recording_log(monkeypatch):
    recording = RecordingLog()
    monkeypatch.setattr(client_module, "log", recording)
    return recording


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeSamples:
    def __init__(self, samples):
        self._items = deque(samples)

    def try_get(self):
        return self._items.popleft() if self._items else None

    def pop_any(self):
        return self.try_get()


class FakeUpdates:
    """`JobUpdateQueue`'s pull half: `put` for the test, `try_get` and
    `requeue_front` for `_JobSource`."""

    def __init__(self):
        self._items = deque()

    def put(self, update):
        self._items.append(update)

    def try_get(self):
        return self._items.popleft() if self._items else None

    def requeue_front(self, update):
        self._items.appendleft(update)


class FakeRos:
    def __init__(self, samples=(), backlog=()):
        self.samples = FakeSamples(samples)
        self.backlog = FakeSamples(backlog)


def test_the_limit_is_the_vendored_contracts_constant():
    assert BRIDGE_FRAME_MAX_BYTES == 1_572_864


def test_frame_bytes_counts_utf8_bytes():
    assert frame_bytes("ab") == 2
    assert frame_bytes("é") == 2


# --- datapoints -----------------------------------------------------------


def _oversized(slug, ts):
    return Sample(slug=slug, value=BIG, timestamp_ms=ts)


def _small(slug, ts, value=1):
    return Sample(slug=slug, value=value, timestamp_ms=ts)


def test_a_live_oversized_value_is_not_sent_and_the_next_one_is_in_the_same_call(recording_log):
    clock = Clock()
    guard = DatapointDropGuard(now=clock)
    ros = FakeRos(samples=[_oversized("map", 1), _small("map", 2)])
    source = _SampleSource(ros, 2, drops=guard)

    payload = source.try_next()

    assert json.loads(payload) == {"type": "datapoint", "slug": "map", "value": 1, "timestamp_ms": 2}
    assert source.try_next() is None


def test_two_oversized_live_values_within_a_minute_log_once_and_report_once(recording_log):
    clock = Clock()
    guard = DatapointDropGuard(now=clock)
    ros = FakeRos(samples=[_oversized("map", 1), _oversized("map", 2)])
    source = _SampleSource(ros, 2, drops=guard)
    reports = _DropReportSource(guard)

    assert source.try_next() is None
    assert len(recording_log.warnings) == 1
    warning = recording_log.warnings[0]
    assert "datapoint 'map'" in warning
    assert str(BRIDGE_FRAME_MAX_BYTES) in warning
    assert "Expose one field instead of the whole message." in warning

    report = reports.try_next()
    assert reports.try_next() is None
    frame = json.loads(report)
    validate_frame("bridge-datapoint-dropped", frame)
    assert frame["slug"] == "map"
    assert frame["reason"] == "too_large"
    assert frame["limit_bytes"] == BRIDGE_FRAME_MAX_BYTES
    assert frame["timestamp_ms"] == 1  # the first dropped sample's capture time
    assert frame["bytes"] == frame_bytes(datapoint_message("map", BIG, 1))


def test_a_minute_later_the_same_slug_logs_and_reports_again(recording_log):
    clock = Clock()
    guard = DatapointDropGuard(now=clock)
    reports = _DropReportSource(guard)
    guard.dropped("map", 3_000_000, 1)
    clock.now += 59.0
    guard.dropped("map", 3_000_000, 2)
    clock.now += 1.0
    guard.dropped("map", 3_000_000, 3)
    assert len(recording_log.warnings) == 2
    stamps = [json.loads(reports.try_next())["timestamp_ms"], json.loads(reports.try_next())["timestamp_ms"]]
    assert stamps == [1, 3]
    assert reports.try_next() is None


def test_each_slug_has_its_own_window(recording_log):
    guard = DatapointDropGuard(now=Clock())
    guard.dropped("map", 3_000_000, 1)
    guard.dropped("cloud", 3_000_000, 1)
    assert len(recording_log.warnings) == 2


def test_a_config_apply_that_removes_a_slug_clears_its_windows(recording_log):
    guard = DatapointDropGuard(now=Clock())
    reports = _DropReportSource(guard)
    guard.dropped("map", 3_000_000, 1)
    guard.dropped("gone", 3_000_000, 1)
    guard.keep_only({"map"})
    # The removed slug's pending report goes with it.
    assert json.loads(reports.try_next())["slug"] == "map"
    assert reports.try_next() is None
    guard.dropped("gone", 3_000_000, 2)
    guard.dropped("map", 3_000_000, 2)
    assert len(recording_log.warnings) == 3  # gone again, map still inside its window


def test_a_backfilled_oversized_value_is_dropped_and_reported_the_same_way(recording_log):
    guard = DatapointDropGuard(now=Clock())
    ros = FakeRos(backlog=[_oversized("map", 1), _oversized("map", 2), _small("map", 3)])
    source = _BackfillSource(ros, guard)
    reports = _DropReportSource(guard)

    payload = source.try_next()

    assert json.loads(payload)["timestamp_ms"] == 3
    assert json.loads(payload)["backfill"] is True
    assert len(recording_log.warnings) == 1
    assert json.loads(reports.try_next())["timestamp_ms"] == 1
    assert reports.try_next() is None


def test_a_value_whose_frame_is_exactly_the_limit_is_sent(recording_log):
    # The frame of a string value is the string plus a fixed envelope; pad
    # the string so the whole frame lands on the limit.
    envelope = frame_bytes(datapoint_message("map", "", 1))
    value = "x" * (BRIDGE_FRAME_MAX_BYTES - envelope)
    assert frame_bytes(datapoint_message("map", value, 1)) == BRIDGE_FRAME_MAX_BYTES
    ros = FakeRos(samples=[_small("map", 1, value), _small("map", 2, value + "x")])
    source = _SampleSource(ros, 2, drops=DatapointDropGuard(now=Clock()))

    assert frame_bytes(source.try_next()) == BRIDGE_FRAME_MAX_BYTES
    assert source.try_next() is None  # one byte more is dropped
    assert len(recording_log.warnings) == 1


def test_datapoint_dropped_matches_the_outgoing_schema():
    validate_frame("bridge-datapoint-dropped", json.loads(datapoint_dropped_message("map", 3_000_000, BRIDGE_FRAME_MAX_BYTES, 7)))


# --- job frames -----------------------------------------------------------


def _update(**overrides):
    fields = dict(timestamp_ms=1, origin="fleetless", goal_id="g1")
    fields.update(overrides)
    return fit_job_update(JOB, "build_map", fields.pop("state", "running"), **fields)


def test_a_job_update_with_a_3_mb_feedback_goes_out_without_it():
    payload, shrink = _update(feedback={"cells": BIG}, progress=0.5)
    frame = json.loads(payload)
    validate_frame("bridge-job-update", frame)
    assert frame_bytes(payload) <= BRIDGE_FRAME_MAX_BYTES
    assert frame["feedback"] is None
    assert frame["progress"] == 0.5
    assert "result_omitted" not in frame
    assert shrink.feedback_dropped and shrink.result_omitted is None


@pytest.mark.parametrize("state, error", [("succeeded", None), ("failed", ("goal_aborted", "Aborted."))])
def test_a_job_update_with_a_3_mb_result_keeps_its_state_and_says_what_it_left_out(state, error):
    result = {"map": BIG}
    payload, shrink = _update(state=state, result=result, error=error)
    frame = json.loads(payload)
    validate_frame("bridge-job-update", frame)
    assert frame_bytes(payload) <= BRIDGE_FRAME_MAX_BYTES
    assert frame["state"] == state
    assert frame["result"] is None
    assert frame["result_omitted"] == {
        "reason": "too_large",
        "bytes": frame_bytes(json.dumps(result)),
        "limit_bytes": BRIDGE_FRAME_MAX_BYTES,
    }
    if error is not None:
        assert frame["error"] == {"code": error[0], "message": error[1]}
    assert shrink.result_omitted == frame["result_omitted"]


def test_a_job_update_that_fits_is_unchanged():
    payload, shrink = _update(state="succeeded", result={"ok": True}, feedback={"step": 1})
    assert payload == job_update_message(
        JOB, "build_map", "succeeded", timestamp_ms=1, origin="fleetless", goal_id="g1",
        result={"ok": True}, feedback={"step": 1},
    )
    assert "result_omitted" not in json.loads(payload)
    assert not shrink.shrunk


def test_job_status_shrinks_only_the_entry_that_is_too_large():
    jobs = [
        JobStatusEntry(job_id=JOB, state="succeeded", result={"map": BIG}),
        JobStatusEntry(job_id=OTHER_JOB, state="succeeded", result={"ok": True}, feedback={"step": 3}),
    ]
    payload, shrinks = fit_job_status("q1", jobs, [])
    frame = json.loads(payload)
    validate_frame("bridge-job-status", frame)
    assert frame_bytes(payload) <= BRIDGE_FRAME_MAX_BYTES
    big, small = frame["jobs"]
    assert big["result"] is None and big["result_omitted"]["reason"] == "too_large"
    assert big["state"] == "succeeded"
    assert small == {
        "job_id": OTHER_JOB, "state": "succeeded", "feedback": {"step": 3},
        "progress": None, "result": {"ok": True}, "error": None,
    }
    assert [job_id for job_id, _ in shrinks] == [JOB]


def test_job_status_that_fits_carries_no_omission():
    payload, shrinks = fit_job_status("q1", [JobStatusEntry(job_id=JOB, state="running")], [OTHER_JOB])
    assert shrinks == []
    assert "result_omitted" not in json.loads(payload)["jobs"][0]


def test_the_job_source_logs_an_omitted_result_once_per_job(recording_log):
    class Jobs:
        def __init__(self):
            self.updates = FakeUpdates()
            self.delivered = []

        def mark_delivered(self, update):
            self.delivered.append(update)

    class Update:
        job_id, slug, state, timestamp_ms, origin, goal_id = JOB, "build_map", "succeeded", 1, "fleetless", "g1"
        feedback, progress, result, error, details = None, None, {"map": BIG}, None, None

    ros = type("Ros", (), {})()
    ros.jobs = Jobs()
    frame_log = _JobFrameLog(now=Clock())
    source = _JobSource(ros, frame_log)
    for _ in range(2):
        ros.jobs.updates.put(Update())
        payload = source.try_next()
        assert frame_bytes(payload) <= BRIDGE_FRAME_MAX_BYTES
        source.on_sent(payload)
    omitted = [w for w in recording_log.warnings if "without its" in w and "result" in w]
    assert len(omitted) == 1
    assert JOB in omitted[0]


# --- introspection ----------------------------------------------------------


def _graph(topic_count):
    return {
        "topics": [{"name": "/t{:06d}/with/a/rather/long/name".format(i), "types": ["std_msgs/msg/String"]} for i in range(topic_count)],
        "services": [{"name": "/svc", "types": ["std_srvs/srv/Trigger"]}],
        "actions": [{"name": "/act", "types": ["nav2_msgs/action/NavigateToPose"]}],
        "captured_at_ms": 5,
    }


def test_a_graph_of_60000_topics_is_cut_to_fit_and_keeps_every_referenced_name():
    graph = _graph(60_000)
    assert frame_bytes(introspect_message("r1", graph)) > BRIDGE_FRAME_MAX_BYTES
    referenced = {
        "topics": {"/t059999/with/a/rather/long/name", "/t030000/with/a/rather/long/name"},
        "services": {"/svc"},
        "actions": {"/act"},
    }

    payload, omitted = fit_introspect_message("r1", graph, referenced)

    assert frame_bytes(payload) <= BRIDGE_FRAME_MAX_BYTES
    frame = json.loads(payload)
    validate_frame("bridge-introspect", frame)
    names = [entry["name"] for entry in frame["graph"]["topics"]]
    assert referenced["topics"] <= set(names)
    assert names == sorted(names)
    assert omitted == frame["graph"]["omitted"]
    assert omitted["topics"] == 60_000 - len(names)
    assert omitted["topics"] > 0
    # The rest was filled in name order: everything kept that is not
    # referenced comes before everything left out.
    filler = [n for n in names if n not in referenced["topics"]]
    assert filler == ["/t{:06d}/with/a/rather/long/name".format(i) for i in range(len(filler))]
    # Topics overflowed, so the fill stopped there; the referenced service
    # and action are still in.
    assert [e["name"] for e in frame["graph"]["services"]] == ["/svc"]
    assert [e["name"] for e in frame["graph"]["actions"]] == ["/act"]
    assert omitted["services"] == 0 and omitted["actions"] == 0
    assert frame["graph"]["captured_at_ms"] == 5


def test_a_graph_that_fits_goes_out_whole_without_omitted():
    graph = _graph(10)
    payload, omitted = fit_introspect_message("r1", graph, {})
    assert omitted is None
    assert payload == introspect_message("r1", graph)
    assert "omitted" not in json.loads(payload)["graph"]


def test_the_referenced_names_are_the_configs_topics_and_ros_names():
    class Entry:
        def __init__(self, **fields):
            self.__dict__.update(fields)

    message = Config(
        version=3,
        datapoints={"map": Entry(topic="/map")},
        publishers={"cmd": Entry(topic="/cmd_vel")},
        cameras={
            "front": Entry(source=RosSource(topic="/camera/image", type="sensor_msgs/msg/Image")),
            "door": Entry(source=object()),
        },
        services={"reset": Entry(ros_name="/reset")},
        actions={"go": Entry(ros_name="/navigate_to_pose")},
    )
    assert _referenced_names(message) == {
        "topics": {"/map", "/cmd_vel", "/camera/image"},
        "services": {"/reset"},
        "actions": {"/navigate_to_pose"},
    }


# --- the writer guard ---------------------------------------------------------


async def _run_briefly(writer, seconds=0.3):
    task = asyncio.ensure_future(writer.run())
    await asyncio.sleep(seconds)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class RecordingWs:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(payload)


def test_the_writer_drops_a_2_mb_type_definitions_frame_and_keeps_the_connection(recording_log):
    oversized = type_definitions_message("r1", [{"name": "x/msg/Y", "kind": "msg", "fields": []}] * 40_000, [])
    assert frame_bytes(oversized) > 2_000_000

    async def scenario():
        ws = RecordingWs()
        writer = PrioritizedWriter(ws, sources=[])
        writer.enqueue(3, oversized)
        writer.enqueue(3, '{"type": "pong", "ts_ms": 1}')
        task = asyncio.ensure_future(writer.run())
        await asyncio.sleep(0.2)
        alive = not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return ws.sent, alive

    sent, alive = asyncio.run(scenario())

    assert sent == ['{"type": "pong", "ts_ms": 1}']
    assert alive
    assert len(recording_log.errors) == 1
    assert "type_definitions" in recording_log.errors[0]
    assert str(frame_bytes(oversized)) in recording_log.errors[0]


def test_the_writer_sends_a_text_frame_of_exactly_the_limit(recording_log):
    head = '{"type": "pong", "pad": "'
    exact = head + "x" * (BRIDGE_FRAME_MAX_BYTES - len(head) - 2) + '"}'
    assert frame_bytes(exact) == BRIDGE_FRAME_MAX_BYTES

    async def scenario():
        ws = RecordingWs()
        writer = PrioritizedWriter(ws, sources=[])
        writer.enqueue(0, exact)
        writer.enqueue(0, exact[:-2] + 'x"}')
        await _run_briefly(writer, 0.2)
        return ws.sent

    assert asyncio.run(scenario()) == [exact]
    assert len(recording_log.errors) == 1


def test_a_dropped_frame_retires_its_job_instead_of_putting_it_back(recording_log):
    class Source:
        def __init__(self):
            self.items = ['{"type": "job_update", "pad": "' + "x" * BRIDGE_FRAME_MAX_BYTES + '"}']
            self.dropped, self.sent, self.failed = [], [], []

        def try_next(self):
            return self.items.pop() if self.items else None

        def on_sent(self, item):
            self.sent.append(item)

        def on_send_failure(self, item):
            self.failed.append(item)

        def on_dropped(self, item):
            self.dropped.append(item)

    source = Source()

    async def scenario():
        writer = PrioritizedWriter(RecordingWs(), sources=[(1, source)])
        await _run_briefly(writer, 0.2)

    asyncio.run(scenario())
    assert len(source.dropped) == 1
    assert source.sent == [] and source.failed == []
    assert "job_update" in recording_log.errors[0]


def test_the_guard_never_fires_for_the_three_shaped_frames(recording_log):
    """Datapoints, job frames and the graph are shaped before the writer
    sees them, so a session that produces each of them oversized sends what
    fits and logs no refusal."""

    class Jobs:
        def __init__(self):
            self.updates = FakeUpdates()

        def mark_delivered(self, update):
            pass

    class Update:
        job_id, slug, state, timestamp_ms, origin, goal_id = JOB, "build_map", "succeeded", 1, "fleetless", "g1"
        feedback, progress, result, error, details = {"cells": BIG}, None, {"map": BIG}, None, None

    ros = FakeRos(samples=[_oversized("map", 1), _small("map", 2)])
    ros.jobs = Jobs()
    ros.jobs.updates.put(Update())
    guard = DatapointDropGuard(now=Clock())
    introspect, _ = fit_introspect_message("r1", _graph(60_000), {})
    status, _ = fit_job_status("q1", [JobStatusEntry(job_id=JOB, state="succeeded", result={"map": BIG})], [])

    async def scenario():
        ws = RecordingWs()
        writer = PrioritizedWriter(
            ws,
            sources=[
                (1, _JobSource(ros, _JobFrameLog(now=Clock()))),
                (2, _DropReportSource(guard)),
                (2, _SampleSource(ros, 2, drops=guard)),
            ],
        )
        writer.enqueue(1, status)
        writer.enqueue(3, introspect)
        await _run_briefly(writer, 0.3)
        return ws.sent

    sent = asyncio.run(scenario())
    types = sorted(json.loads(payload)["type"] for payload in sent)
    assert types == ["datapoint", "datapoint_dropped", "introspect", "job_status", "job_update"]
    assert recording_log.errors == []
    assert all(frame_bytes(payload) <= BRIDGE_FRAME_MAX_BYTES for payload in sent)
