# SPDX-License-Identifier: Apache-2.0
"""Config/introspect/type request handling and the datapoint sample pump,
driven against the fake cloud with a lightweight duck-typed `FakeRos`
standing in for RosRuntime. Real ROS behaviour is ros_runtime.py's own
suite; this file is wire protocol and session plumbing."""
import asyncio
from collections import deque

from fake_cloud import FakeCloud
from helpers import make_client, run_until

import fleetless_bridge.client as client_module
from fleetless_bridge.jobs import JobUpdateQueue
from fleetless_bridge.link_mode import LowBandwidthSettings
from fleetless_bridge.protocol import (
    APPLY_ERROR_CODE_UNKNOWN,
    APPLY_ERROR_KIND_DATAPOINT,
    LOW_BANDWIDTH_DEFAULTS,
    ApplyError,
)
from fleetless_bridge.ros_runtime import BacklogStore, CameraStateQueue, SampleQueue
from fleetless_bridge.sampling import Sample


class _FakeSnapshots:
    """Stands in for RosRuntime's on-demand encoding: a test queues
    wire-ready snapshot bytes, `FakeRos.next_snapshot` returns the next
    one. Replaces the retired `ros_runtime.SnapshotQueue` — same
    `put_threadsafe` shape, so tests that queue a snapshot needed no
    change when snapshots became pulled rather than pushed."""

    def __init__(self) -> None:
        self._items = deque()

    def put_threadsafe(self, loop, frame_bytes) -> None:
        loop.call_soon_threadsafe(self._items.append, frame_bytes)

    def try_get(self):
        return self._items.popleft() if self._items else None


class _FakeJobs:
    """Stands in for `JobManager` — enough for the client's hello
    `active_jobs()` and `_pump_jobs`'s `updates` drain. Uses the real
    `JobUpdateQueue` (rclpy-free) rather than another fake, so it also
    has the real `requeue_front` a dropped connection needs."""

    def __init__(self) -> None:
        self.updates = JobUpdateQueue()
        self._active = []  # [(job_id, slug, state), ...] — set directly by tests
        self.delivered_calls = []

    def active_jobs(self):
        return list(self._active)

    def mark_delivered(self, update):
        self.delivered_calls.append(update)


class FakeRos:
    """Answers apply_config/apply_actions/apply_services/apply_publishers/
    graph_snapshot/resolve_types from fixed results set up by the test, and
    records what it was asked to do."""

    def __init__(
        self,
        *,
        apply_errors=None,
        apply_action_errors=None,
        apply_service_errors=None,
        apply_publisher_errors=None,
        apply_camera_errors=None,
        apply_config_raises=None,
        apply_action_raises=None,
        apply_service_raises=None,
        apply_publisher_raises=None,
        apply_camera_raises=None,
        graph=None,
        resolve=None,
    ) -> None:
        self.samples = SampleQueue(maxsize=10)
        self.snapshots = _FakeSnapshots()
        self.snapshot_max_bytes_asked = []
        self.camera_states = CameraStateQueue()
        self.assets = asyncio.Queue()
        self.asset_progress = asyncio.Queue()
        self.sync_assets_calls = []
        self.backlog = BacklogStore()  # rclpy-free, same reasoning as JobUpdateQueue above
        self.jobs = _FakeJobs()
        self.applied_calls = []
        self.applied_action_calls = []
        self.applied_service_calls = []
        self.applied_publisher_calls = []
        self.applied_camera_calls = []
        self.invoke_calls = []
        self.cancel_calls = []
        self.publish_calls = []
        self.start_live_calls = []
        self.stop_live_calls = []
        self.stop_all_live_calls = 0
        self.report_current_camera_health_calls = 0
        self.check_urdf_availability_now_calls = 0
        self.report_current_urdf_availability_calls = 0
        self.introspect_calls = 0
        self.resolve_calls = []
        self.connected_calls = []
        # Low-bandwidth mode, as the client sees the runtime: the parameter
        # layer a test can move, the callback the runtime would fire after a
        # `ros2 param set`, and a record of every lever pull.
        self.low_bandwidth_params_value = dict(LOW_BANDWIDTH_DEFAULTS)
        self.low_bandwidth_section = {}
        self.low_bandwidth_calls = []
        self.low_bandwidth_callback = None
        self._apply_errors = apply_errors or []
        self._apply_action_errors = apply_action_errors or []
        self._apply_service_errors = apply_service_errors or []
        self._apply_publisher_errors = apply_publisher_errors or []
        self._apply_camera_errors = apply_camera_errors or []
        self._apply_config_raises = apply_config_raises
        self._apply_action_raises = apply_action_raises
        self._apply_service_raises = apply_service_raises
        self._apply_publisher_raises = apply_publisher_raises
        self._apply_camera_raises = apply_camera_raises
        self._graph = graph or {
            "topics": [],
            "services": [],
            "actions": [],
            "captured_at_ms": 1754800000000,
        }
        self._resolve = resolve or ([], [])

    async def apply_config(self, datapoints):
        self.applied_calls.append(datapoints)
        if self._apply_config_raises is not None:
            raise self._apply_config_raises
        return self._apply_errors

    async def apply_actions(self, actions, messages=None):
        self.applied_action_calls.append(actions)
        if self._apply_action_raises is not None:
            raise self._apply_action_raises
        return self._apply_action_errors

    async def apply_services(self, services, messages=None):
        self.applied_service_calls.append(services)
        if self._apply_service_raises is not None:
            raise self._apply_service_raises
        return self._apply_service_errors

    async def apply_publishers(self, publishers, messages=None):
        self.applied_publisher_calls.append(publishers)
        if self._apply_publisher_raises is not None:
            raise self._apply_publisher_raises
        return self._apply_publisher_errors

    async def apply_cameras(self, cameras):
        self.applied_camera_calls.append(cameras)
        if self._apply_camera_raises is not None:
            raise self._apply_camera_raises
        return self._apply_camera_errors

    async def invoke(self, job_id, slug, params, patience_ms):
        self.invoke_calls.append((job_id, slug, params, patience_ms))

    async def cancel_job(self, slug, job_id):
        self.cancel_calls.append((slug, job_id))

    async def publish(self, slug, message):
        self.publish_calls.append((slug, message))

    async def start_live(self, slug, url, room, token, request_id):
        self.start_live_calls.append((slug, url, room, token, request_id))

    async def stop_live(self, slug, *, request_id=None):
        self.stop_live_calls.append((slug, request_id))

    async def stop_all_live(self):
        self.stop_all_live_calls += 1

    async def report_current_camera_health(self):
        self.report_current_camera_health_calls += 1

    async def check_urdf_availability_now(self):
        self.check_urdf_availability_now_calls += 1

    async def report_current_urdf_availability(self):
        self.report_current_urdf_availability_calls += 1

    async def sync_assets(self, sync_id, upload_url, token, meshes):
        self.sync_assets_calls.append((sync_id, upload_url, token, meshes))

    def set_connected(self, connected):
        self.connected_calls.append(connected)

    def low_bandwidth_params(self):
        return dict(self.low_bandwidth_params_value)

    def on_low_bandwidth_params(self, callback):
        self.low_bandwidth_callback = callback

    def set_low_bandwidth_section(self, section):
        self.low_bandwidth_section = dict(section)

    def refuse_or_accept_param(self, key, value):
        """Stand in for the runtime's parameter callback: validate the whole
        proposed section against the published one, and hand it over only on
        success. Returns what `ros2 param set` would report."""
        proposed = dict(self.low_bandwidth_params_value)
        proposed[key] = value
        try:
            LowBandwidthSettings.resolve(proposed, self.low_bandwidth_section)
        except ValueError:
            return False
        self.low_bandwidth_params_value = proposed
        if self.low_bandwidth_callback is not None:
            self.low_bandwidth_callback(dict(proposed))
        return True

    async def set_low_bandwidth(self, active, settings):
        self.low_bandwidth_calls.append((active, settings))

    async def graph_snapshot(self):
        self.introspect_calls += 1
        return self._graph

    async def resolve_types(self, type_names):
        self.resolve_calls.append(type_names)
        return self._resolve

    async def next_snapshot(self, max_bytes):
        """The pulled tier-5 source. Records the byte budget asked for —
        asserted in test_client_writer_wiring.py to follow the measured
        rate — and returns whatever a test queued on `self.snapshots`,
        or `None` when nothing is due."""
        self.snapshot_max_bytes_asked.append(max_bytes)
        return self.snapshots.try_get()


def _run_one_exchange(send_and_recv, *, ros=None):
    """Connects, runs `send_and_recv(session)` (sends a request, awaits
    the reply) on the fake cloud's side, then stops the client. Returns
    what `send_and_recv` returned."""
    result_box = {}

    async def behavior(session):
        await session.recv_hello()
        await session.accept()
        result_box["result"] = await send_and_recv(session)
        await session.drain()

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=ros)
            await run_until(client, lambda: "result" in result_box)
        return result_box["result"]

    return asyncio.run(scenario())


# --- config -------------------------------------------------------------------


def test_config_is_applied_and_config_applied_is_sent_back():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {"type": "config_applied", "version": 1, "ok": True, "errors": []}
    assert fake_ros.applied_calls == [{}]


def test_a_slug_error_from_apply_is_reported_as_ok_false():
    fake_ros = FakeRos(apply_errors=[
        ApplyError(
            slug="bad_slug", kind=APPLY_ERROR_KIND_DATAPOINT,
            code=APPLY_ERROR_CODE_UNKNOWN, message="unknown type",
        )
    ])

    async def send_and_recv(session):
        await session.send_config(2, {})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["ok"] is False
    assert payload["errors"] == [
        {"slug": "bad_slug", "kind": "datapoint", "code": "unknown", "message": "unknown type"}
    ]


def test_a_whole_kind_failure_reports_the_star_slug_and_its_own_code():
    """`_apply_or_report`'s catch: an apply_* call that raises outright
    (as opposed to returning per-slug errors) must not crash the
    session, and must report slug '*' with code 'whole_kind_failed' for
    the kind whose pass actually raised, not a neighbour's. Exercised
    once per call site, in isolation, because a kind label swapped
    between two of the five `_apply_or_report` call sites in client.py
    would leave every other assertion in this file green — only the
    named kind would change. Literal strings throughout, not
    `APPLY_ERROR_*` constants: a constant asserted against itself
    cannot fail when its value drifts."""
    cases = [
        ("apply_config_raises", "datapoint"),
        ("apply_action_raises", "action"),
        ("apply_service_raises", "service"),
        ("apply_publisher_raises", "publisher"),
        ("apply_camera_raises", "camera"),
    ]
    for raises_kwarg, expected_kind in cases:
        fake_ros = FakeRos(**{raises_kwarg: RuntimeError("boom")})

        async def send_and_recv(session):
            await session.send_config(4, {})
            return await session.recv_config_applied()

        payload = _run_one_exchange(send_and_recv, ros=fake_ros)
        assert payload["ok"] is False, raises_kwarg
        assert payload["errors"] == [
            {
                "slug": "*",
                "kind": expected_kind,
                "code": "whole_kind_failed",
                "message": "internal error applying {}".format(expected_kind),
            }
        ], raises_kwarg


def test_config_without_a_ros_runtime_is_acked_as_a_noop():
    async def send_and_recv(session):
        await session.send_config(0, {})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=None)
    assert payload == {"type": "config_applied", "version": 0, "ok": True, "errors": []}


# --- introspection / types -----------------------------------------------------


def test_introspect_request_answers_with_the_graph_snapshot():
    graph = {
        "topics": [{"name": "/battery", "types": ["sensor_msgs/msg/BatteryState"]}],
        "services": [],
        "actions": [],
        "captured_at_ms": 1754800000123,
    }
    fake_ros = FakeRos(graph=graph)

    async def send_and_recv(session):
        await session.send_introspect_request("req-1")
        return await session.recv_introspect()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {"type": "introspect", "request_id": "req-1", "graph": graph}
    assert fake_ros.introspect_calls == 1


def test_type_request_answers_with_definitions_and_unresolved():
    definitions = [
        {
            "name": "sensor_msgs/msg/BatteryState",
            "kind": "msg",
            "fields": [
                {"name": "percentage", "type": "float", "array": False, "fields": None}
            ],
        }
    ]
    fake_ros = FakeRos(resolve=(definitions, ["unknown_pkg/msg/Ghost"]))

    async def send_and_recv(session):
        await session.send_type_request(
            "req-2", ["sensor_msgs/msg/BatteryState", "unknown_pkg/msg/Ghost"]
        )
        return await session.recv_type_definitions()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["definitions"] == definitions
    assert payload["unresolved"] == ["unknown_pkg/msg/Ghost"]
    assert fake_ros.resolve_calls == [("sensor_msgs/msg/BatteryState", "unknown_pkg/msg/Ghost")]


# --- the datapoint pump ------------------------------------------------------------


def test_datapoint_samples_are_pumped_out_once_connected():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        fake_ros.samples.put_threadsafe(
            loop, Sample(slug="battery_percentage", value=87.5, timestamp_ms=1754800000123)
        )
        return await session.recv_datapoint()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {
        "type": "datapoint",
        "slug": "battery_percentage",
        "value": 87.5,
        "timestamp_ms": 1754800000123,
    }


def test_the_pump_and_the_receive_loop_can_send_concurrently_without_corrupting_frames():
    """A config_applied (receive loop) and a datapoint (pump) sent
    back-to-back must both arrive intact — one frame is one write, so
    this tests that nothing here fights that guarantee. Also the one
    place this pair is checked against a real socket rather than the
    writer's own bookkeeping."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        fake_ros.samples.put_threadsafe(
            loop, Sample(slug="x", value=1, timestamp_ms=1)
        )
        await session.send_config(1, {})
        first = await session.recv()
        second = await session.recv()
        return {first["type"], second["type"]}

    frame_types = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert frame_types == {"config_applied", "datapoint"}


# --- dropped-sample logging ---------------------------------------------------------


class _RecordingLog:
    """Stands in for client.py's module-level `log`: caplog does not
    reliably see records from inside `asyncio.run()` in this suite —
    checking what the code called is simpler and just as faithful as
    fighting log-capture plumbing."""

    def __init__(self) -> None:
        self.warnings = []

    def warning(self, fmt, *args):
        self.warnings.append(fmt % args if args else fmt)

    def __getattr__(self, name):
        return lambda *args, **kwargs: None  # info/error/debug/exception: ignored


def test_dropped_samples_while_disconnected_are_logged_once_on_reconnect(monkeypatch):
    fake_ros = FakeRos()
    # Simulate drops that happened while nobody was connected to drain them.
    fake_ros.samples._dropped = 3
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def send_and_recv(session):
        return True  # nothing to send — connecting itself triggers the log

    _run_one_exchange(send_and_recv, ros=fake_ros)

    assert any("3 datapoint sample" in message for message in recording_log.warnings)
    assert fake_ros.samples.drain_drop_count() == 0  # read-and-reset already consumed it


def test_no_log_line_when_nothing_was_dropped(monkeypatch):
    fake_ros = FakeRos()
    recording_log = _RecordingLog()
    monkeypatch.setattr(client_module, "log", recording_log)

    async def send_and_recv(session):
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)

    assert not any("dropped" in message for message in recording_log.warnings)
