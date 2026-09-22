# SPDX-License-Identifier: Apache-2.0
"""RosRuntime against a real, spinning ROS graph: config-diff subscription
management, sample delivery, and the bounded sample queue.

`RosRuntime.start()` owns the whole rclpy lifecycle (init through shutdown)
for the duration of one test, so these tests do not use the `ros`/`spun_node`
fixtures from conftest.py — a second `rclpy.init()` on the same default
context would collide with the runtime's own. A plain `rclpy.create_node()`
after `rt.start()` is enough for a helper publisher: publishing does not
need the helper node to be spinning, only subscribing does.
"""
import asyncio
import concurrent.futures
import http.server
import json
import logging
import os
import posixpath
import socket
import struct
import tempfile
import threading
import time
import urllib.error
import urllib.parse
from unittest import mock

import cv2
import numpy as np
import pytest
import rclpy
from example_interfaces.action import Fibonacci
from ament_index_python.packages import PackageNotFoundError, get_package_share_directory
from conftest import wait_until
from helpers import by_slug
from cv_bridge import CvBridge
from geometry_msgs.msg import Twist
from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.executors import MultiThreadedExecutor
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import BatteryState, Image
from std_msgs.msg import String as StringMsg
from std_srvs.srv import Trigger

from fleetless_bridge.protocol import (
    ActionConfig,
    CameraConfig,
    DatapointConfig,
    DatapointNumeric,
    FailsafeConfig,
    MjpegSource,
    ParameterSpec,
    PublisherConfig,
    RetentionConfig,
    RosSource,
    RtspSource,
    ServiceConfig,
    bridge_asset_progress_message,
)
from fleetless_bridge.ros_runtime import (
    ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES,
    DAE_MAX_INTERNAL_REFERENCES_PER_SYNC,
    DEFAULT_URDF_TOPIC,
    MESH_MAX_FAILED_PER_SYNC,
    PARTING_FAILSAFE_ACK_TIMEOUT_S,
    URDF_ASSET_NAME,
    AssetProgress,
    CameraStateQueue,
    CameraStateUpdate,
    DAE_SCAN_MAX_BYTES,
    RosRuntime,
    UploadResult,
)
from schemas import validate_frame

FIBONACCI_TYPE = "example_interfaces/action/Fibonacci"
TRIGGER_TYPE = "std_srvs/srv/Trigger"
TWIST_TYPE = "geometry_msgs/msg/Twist"
IMAGE_TYPE = "sensor_msgs/msg/Image"


def _dp(
    slug,
    topic="/battery",
    type_="sensor_msgs/msg/BatteryState",
    field="percentage",
    rate_throttle_hz=None,
    numeric=None,
    retention=None,
):
    return DatapointConfig(
        slug=slug,
        topic=topic,
        type=type_,
        field=field,
        rate_throttle_hz=rate_throttle_hz,
        numeric=numeric or DatapointNumeric(),
        retention=retention or RetentionConfig(enabled=False),
    )


def _battery(percentage):
    msg = BatteryState()
    msg.percentage = percentage
    return msg


# The Fibonacci Goal as a developer writes it in 3.0: the one field
# spelled out, with the one placeholder a caller may fill. `{"order": 5}` on
# an invoke is a value for the declared parameter `order`, not a field path —
# where it lands is decided here, by the template, not by its name.
_FIBONACCI_MESSAGE = {"order": "${order}"}
_FIBONACCI_PARAMETERS = {"order": ParameterSpec(type="int32")}

# The same for a Twist publisher: two fillable positions, everything else
# fixed at what is written.
_TWIST_MESSAGE = {"linear": {"x": "${speed}"}, "angular": {"z": "${turn}"}}
_TWIST_PARAMETERS = {
    "speed": ParameterSpec(type="float64"),
    "turn": ParameterSpec(type="float64", default=0.0),
}


def _action_cfg(
    slug, ros_name="/count", type_=FIBONACCI_TYPE, parameters=None, message=None
):
    return ActionConfig(
        slug=slug,
        ros_name=ros_name,
        type=type_,
        message=_FIBONACCI_MESSAGE if message is None else message,
        parameters=_FIBONACCI_PARAMETERS if parameters is None else parameters,
    )


def _start_fibonacci_server(
    *,
    action_name="/count",
    steps=5,
    step_delay=0.02,
    accept=True,
    accept_delay=0.0,
    honor_cancel=True,
):
    """A real Fibonacci action server on its own spinning node, so the
    bridge's real `ActionClient` drives a genuine goal lifecycle: feedback
    per step, cancellation via `is_cancel_requested`, a real accept/reject
    decision. Returns a `stop()` callable; `stop.cancel_requests` is a list
    cancel requests append to, so a test can witness one arriving instead of
    inferring it from the result.

    `accept_delay` holds the accept/reject decision open that long before
    answering — lets the goal-acceptance-timeout tests arrange a goal
    accepted *after* the bridge has already given up on it.

    `MultiThreadedExecutor`, not `SingleThreadedExecutor`: rclpy's
    `ActionServer` runs `execute_callback` synchronously on the executor's
    own thread. A single-threaded executor blocked in this callback's
    `time.sleep()` cannot also process an incoming cancel until the callback
    returns — by which point the goal has already finished. Same for a real
    robot's action server: this is rclpy, not a test shortcut."""
    node = rclpy.create_node("test_action_server_{}".format(id(object())))
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    cancel_requests = []

    def execute_callback(goal_handle):
        feedback = Fibonacci.Feedback()
        feedback.sequence = [0, 1]
        for _ in range(steps):
            if goal_handle.is_cancel_requested:
                goal_handle.canceled()
                result = Fibonacci.Result()
                result.sequence = feedback.sequence
                return result
            feedback.sequence.append(
                feedback.sequence[-1] + feedback.sequence[-2]
            )
            goal_handle.publish_feedback(feedback)
            time.sleep(step_delay)
        goal_handle.succeed()
        result = Fibonacci.Result()
        result.sequence = feedback.sequence
        return result

    def goal_callback(_goal_request):
        if accept_delay:
            time.sleep(accept_delay)
        return GoalResponse.ACCEPT if accept else GoalResponse.REJECT

    def cancel_callback(goal_handle):
        cancel_requests.append(goal_handle)
        return CancelResponse.ACCEPT if honor_cancel else CancelResponse.REJECT

    server = ActionServer(
        node,
        Fibonacci,
        action_name,
        execute_callback,
        goal_callback=goal_callback,
        # rclpy's default rejects every cancel request; cancel tests need a
        # server that honours one (unless `honor_cancel` says otherwise, for
        # goal-timeout tests that need a cancel to genuinely fail).
        cancel_callback=cancel_callback,
    )

    def stop():
        server.destroy()
        executor.shutdown()
        node.destroy_node()
        thread.join(timeout=5.0)

    stop.cancel_requests = cancel_requests

    return stop


def _service_cfg(slug, ros_name="/do_it", type_=TRIGGER_TYPE, parameters=None, message=None):
    return ServiceConfig(
        slug=slug,
        ros_name=ros_name,
        type=type_,
        message={} if message is None else message,
        parameters=parameters or {},
    )


def _publisher_cfg(
    slug,
    topic="/cmd_vel",
    type_=TWIST_TYPE,
    parameters=None,
    message=None,
    timeout_ms=300,
    failsafe=None,
    quiet_timeout_ms=0,
):
    if failsafe is None:
        failsafe = {"linear": {"x": 0.0}, "angular": {"z": 0.0}}
    return PublisherConfig(
        slug=slug,
        topic=topic,
        type=type_,
        message=_TWIST_MESSAGE if message is None else message,
        parameters=_TWIST_PARAMETERS if parameters is None else parameters,
        failsafe=FailsafeConfig(timeout_ms=timeout_ms, message=failsafe),
        quiet_timeout_ms=quiet_timeout_ms,
    )


def _start_trigger_server(*, service_name="/do_it", success=True, message="done", delay=0.0):
    """A real std_srvs/Trigger server on its own spinning node — request/
    response only, no lifecycle, so a plain node with a callback is enough.
    `MultiThreadedExecutor` so a `delay` (used to keep a call "running" long
    enough for a busy test to observe it) blocks only its own callback, not
    the whole node."""
    node = rclpy.create_node("test_service_server_{}".format(id(object())))
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()

    def callback(_request, response):
        if delay:
            time.sleep(delay)
        response.success = success
        response.message = message
        return response

    node.create_service(Trigger, service_name, callback)

    def stop():
        executor.shutdown()
        node.destroy_node()
        thread.join(timeout=5.0)

    return stop


#: How long a witness waits for a sample the bridge has already published on a
#: publisher that is still alive. Reliable DDS repairs a sample dropped on first
#: send at the writer's next heartbeat, which Fast DDS sends every 3 s by
#: default, so on a busy machine a failsafe can reach its witness two or three
#: seconds after it fired -- late, not lost, and not twice. A 2 s wait races that
#: repair. This bounds "at all", not latency: a failsafe that is never published
#: still fails the wait.
DELIVERY_TIMEOUT_S = 10.0


def _start_independent_subscriber(topic, msg_type, own_context=False):
    """A subscriber on its own node/executor/thread — not the bridge's, so it
    is a real independent witness to what lands on a topic. Returns
    `(received_list, stop_fn)`; `received_list` grows in place.

    `own_context=True` puts it on a separate `rclpy.Context` as well, which is
    closer to a robot's own node (still the same process). A witness to a
    message published immediately before its publisher is destroyed needs that:
    on the bridge's context the subscriber learns of the removal at once, and a
    sample it has acknowledged but not yet taken can go with it.
    `test_stop_fires_the_failsafe_for_an_armed_publisher_before_shutting_down`
    observes from its own context for the same reason. Whether such a sample
    arrives at all is the bridge's part: a removed publisher keeps its handle
    until its last failsafe is acknowledged (`PARTING_FAILSAFE_ACK_TIMEOUT_S`)."""
    context = None
    if own_context:
        context = rclpy.Context()
        rclpy.init(context=context, args=[])
    node = rclpy.create_node("test_subscriber_{}".format(id(object())), context=context)
    executor = MultiThreadedExecutor(context=context)
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    received = []
    subscription = node.create_subscription(msg_type, topic, received.append, 10)

    def stop():
        executor.shutdown()
        node.destroy_node()
        if context is not None:
            rclpy.shutdown(context=context)
        thread.join(timeout=5.0)

    return received, subscription, stop


async def _drain_until_terminal(rt, timeout=5.0):
    """Collects job updates until one with a terminal state arrives. `lost`
    is terminal too (jobs.py's `_TERMINAL_STATES`) — the point is a job
    settling as `lost` rather than never settling."""
    updates = []
    while True:
        update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=timeout)
        updates.append(update)
        if update.state in ("succeeded", "failed", "cancelled", "lost"):
            return updates


async def _with_runtime(body, **runtime_kwargs):
    rt = RosRuntime(node_name="test_runtime_{}".format(id(body)), **runtime_kwargs)
    rt.start(asyncio.get_event_loop())
    # Tests default to "a session is connected", matching what every test
    # here already assumed before samples had a connected/disconnected
    # distinction. Buffering tests below call rt.set_connected(False)
    # themselves to test the other side.
    rt.set_connected(True)
    try:
        return await body(rt)
    finally:
        rt.stop()


def run(body, **runtime_kwargs):
    return asyncio.run(_with_runtime(body, **runtime_kwargs))


# --- apply_config: creating and sampling -----------------------------------


def test_apply_config_creates_a_subscription_that_delivers_a_sample():
    async def body(rt):
        errors = await rt.apply_config(by_slug([_dp("battery_percentage")]))
        assert errors == []

        pub_node = rclpy.create_node("test_publisher")
        try:
            pub = pub_node.create_publisher(BatteryState, "/battery", 10)
            pub.publish(_battery(0.5))
            return await asyncio.wait_for(rt.samples.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    sample = run(body)
    assert sample.slug == "battery_percentage"
    assert sample.value == pytest.approx(0.5)


def test_an_unknown_type_is_a_per_slug_error_and_does_not_block_other_slugs():
    async def body(rt):
        return await rt.apply_config(by_slug(
            [
                _dp("bad", type_="nonexistent_pkg/msg/Ghost"),
                _dp("good"),
            ]
        ))

    errors = run(body)
    assert {e.slug for e in errors} == {"bad"}
    assert "bad" in {e.slug for e in errors}


def test_an_unresolvable_field_path_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_config(by_slug([_dp("bad-field", field="not_a_real_field")]))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "bad-field"


# --- apply errors: kind and code (contracts applyError) ---------------------


def test_apply_error_carries_the_kind_that_failed():
    """Each of the five passes reports its own kind, not a shared one — the
    label client.py used to pass here ("the configuration", "the actions",
    ...) was never the kind, and "the configuration" was the datapoint
    pass."""

    async def body(rt):
        return (
            await rt.apply_config(by_slug([_dp("bad", type_="nonexistent_pkg/msg/Ghost")])),
            await rt.apply_actions(by_slug([_action_cfg("bad", type_="nonexistent_pkg/action/Ghost")])),
            await rt.apply_services(by_slug([_service_cfg("bad", type_="nonexistent_pkg/srv/Ghost")])),
            await rt.apply_publishers(by_slug(
                [_publisher_cfg("bad", type_="nonexistent_pkg/msg/Ghost", failsafe={})]
            )),
            await rt.apply_cameras(by_slug([_camera_cfg("bad", type_="nonexistent_pkg/msg/Ghost")])),
        )

    datapoint_errors, action_errors, service_errors, publisher_errors, camera_errors = run(body)
    assert [e.kind for e in datapoint_errors] == ["datapoint"]
    assert [e.kind for e in action_errors] == ["action"]
    assert [e.kind for e in service_errors] == ["service"]
    assert [e.kind for e in publisher_errors] == ["publisher"]
    assert [e.kind for e in camera_errors] == ["camera"]


def test_a_field_path_error_is_classified_and_others_are_not():
    """`FieldPathError` -> `field_path_invalid`; a plain broad-catch exception
    (here: an unknown message type) -> `unknown`, carrying the same message
    the wire carried before this change."""

    async def body(rt):
        return (
            await rt.apply_config(by_slug([_dp("bad-field", field="not_a_real_field")])),
            await rt.apply_config(by_slug([_dp("bad-type", type_="nonexistent_pkg/msg/Ghost")])),
        )

    field_path_errors, unknown_errors = run(body)

    assert len(field_path_errors) == 1
    assert field_path_errors[0].kind == "datapoint"
    assert field_path_errors[0].code == "field_path_invalid"

    assert len(unknown_errors) == 1
    assert unknown_errors[0].code == "unknown"
    assert unknown_errors[0].message.startswith("unknown type 'nonexistent_pkg/msg/Ghost'")


# --- apply_config: the diff ---------------------------------------------------


def test_removing_a_slug_stops_delivering_its_samples():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("battery_percentage")]))
        await rt.apply_config(by_slug([]))  # removed
        assert "battery_percentage" not in rt._subscriptions

        pub_node = rclpy.create_node("test_publisher")
        try:
            pub = pub_node.create_publisher(BatteryState, "/battery", 10)
            pub.publish(_battery(0.9))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rt.samples.get(), timeout=0.5)
        finally:
            pub_node.destroy_node()

    run(body)


def test_retargeting_a_slug_to_a_new_topic_resubscribes():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x", topic="/battery")]))
        await rt.apply_config(by_slug([_dp("x", topic="/battery2")]))

        pub_node = rclpy.create_node("test_publisher")
        try:
            old_pub = pub_node.create_publisher(BatteryState, "/battery", 10)
            new_pub = pub_node.create_publisher(BatteryState, "/battery2", 10)
            old_pub.publish(_battery(0.1))
            new_pub.publish(_battery(0.2))
            sample = await asyncio.wait_for(rt.samples.get(), timeout=5.0)
            return sample
        finally:
            pub_node.destroy_node()

    sample = run(body)
    # Only the new topic's publish should have made it through — the old
    # subscription was torn down when the slug retargeted.
    assert sample.value == pytest.approx(0.2)


def test_an_unchanged_topic_and_type_keeps_the_same_subscription_object():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x")]))
        handle_before = rt._subscriptions["x"].handle
        # Same topic/type, only the rate mode changes.
        await rt.apply_config(by_slug([_dp("x", rate_throttle_hz=5)]))
        handle_after = rt._subscriptions["x"].handle
        return handle_before, handle_after

    before, after = run(body)
    assert before is after  # no resubscribe — no leak, no gap


# --- the bounded sample queue -------------------------------------------------


def test_the_sample_queue_drops_the_oldest_entry_and_counts_drops():
    async def body(rt):
        # No rate ceiling: every published value is a real attempt to enter
        # the queue. A ceiling would throttle most of the burst away before
        # it reached the queue — a different mechanism than the one under
        # test.
        await rt.apply_config(by_slug([_dp("x")]))
        pub_node = rclpy.create_node("test_publisher")
        try:
            pub = pub_node.create_publisher(BatteryState, "/battery", 20)
            # DDS discovery between the two nodes is asynchronous — publish
            # before it completes and the burst is mostly lost before it ever
            # reaches the queue, which would make this test pass for the
            # wrong reason (nothing arrived, so nothing had to be dropped).
            wait_until(lambda: pub.get_subscription_count() > 0)
            for i in range(20):
                pub.publish(_battery(i / 1000.0))
            # Give the executor thread a moment to drain the publishes into
            # the (deliberately tiny) queue.
            await asyncio.sleep(0.5)
        finally:
            pub_node.destroy_node()
        return rt.samples

    samples = run(body, sample_queue_maxsize=5)
    assert samples._queue.qsize() <= 5
    assert samples.drain_drop_count() > 0
    # Read-and-reset: a second read is zero until more samples are dropped.
    assert samples.drain_drop_count() == 0


# --- datapoint buffering while disconnected -------------------------


def _publish_battery_values(topic, values):
    """A helper publisher, run off the event loop thread (`run_in_executor`)
    since it blocks on DDS discovery — drives samples into a subscription
    from outside the runtime under test."""
    pub_node = rclpy.create_node("test_buffer_publisher_{}".format(id(object())))
    try:
        pub = pub_node.create_publisher(BatteryState, topic, 20)
        wait_until(lambda: pub.get_subscription_count() > 0)
        for value in values:
            pub.publish(_battery(value))
            time.sleep(0.01)  # each publish must land as its own sample
    finally:
        pub_node.destroy_node()


async def _publish_while_disconnected(rt, values):
    await asyncio.get_event_loop().run_in_executor(
        None, _publish_battery_values, "/battery", values
    )
    await asyncio.sleep(0.2)  # give the executor thread time to process them


def test_an_unbuffered_datapoint_has_a_gap_while_disconnected():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x")]))  # buffer disabled by _dp's default
        rt.set_connected(False)
        await _publish_while_disconnected(rt, [0.1, 0.2, 0.3])
        return rt.samples.try_get(), rt.backlog.has_pending()

    live, has_backlog = run(body)
    assert live is None  # nobody was connected to receive it live
    assert has_backlog is False  # and unbuffered, so nothing was kept either — an honest gap


def test_a_buffered_datapoint_keeps_values_while_disconnected():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x", retention=RetentionConfig(enabled=True, max_buffer_values=10))]))
        rt.set_connected(False)
        await _publish_while_disconnected(rt, [0.1, 0.2, 0.3])
        collected = []
        while rt.backlog.has_pending():
            collected.append(rt.backlog.pop_any())
        return rt.samples.try_get(), collected

    live, backlog_values = run(body)
    assert live is None  # buffered values do not also count as live
    assert [round(s.value, 1) for s in backlog_values] == [0.1, 0.2, 0.3]


def test_a_buffered_backlog_drops_the_oldest_beyond_max_values():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x", retention=RetentionConfig(enabled=True, max_buffer_values=2))]))
        rt.set_connected(False)
        await _publish_while_disconnected(rt, [0.1, 0.2, 0.3])
        collected = []
        while rt.backlog.has_pending():
            collected.append(rt.backlog.pop_any())
        return collected

    backlog_values = run(body)
    assert [round(s.value, 1) for s in backlog_values] == [0.2, 0.3]  # 0.1 was the oldest


def test_reconnecting_does_not_by_itself_flush_the_backlog():
    """RosRuntime only fills and holds the backlog — draining it into wire
    frames, at a limited rate, is the outgoing pump's job (client.py's
    `_next_sample`), not something `set_connected` triggers on its own."""

    async def body(rt):
        await rt.apply_config(by_slug([_dp("x", retention=RetentionConfig(enabled=True, max_buffer_values=10))]))
        rt.set_connected(False)
        await _publish_while_disconnected(rt, [0.1])
        rt.set_connected(True)
        await asyncio.sleep(0.1)
        return rt.backlog.has_pending()

    assert run(body) is True


def test_removing_a_buffered_slug_clears_its_backlog():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x", retention=RetentionConfig(enabled=True, max_buffer_values=10))]))
        rt.set_connected(False)
        await _publish_while_disconnected(rt, [0.1])
        await rt.apply_config(by_slug([]))  # removed
        return rt.backlog.has_pending()

    assert run(body) is False


def test_disabling_a_buffer_on_reapply_stops_new_values_being_kept():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x", retention=RetentionConfig(enabled=True, max_buffer_values=10))]))
        rt.set_connected(False)
        await _publish_while_disconnected(rt, [0.1])
        assert rt.backlog.has_pending()  # sanity: buffering was on

        # Re-published with buffering turned off, still disconnected.
        await rt.apply_config(by_slug([_dp("x", retention=RetentionConfig(enabled=False))]))
        # The already-buffered value is not retroactively discarded — only
        # new samples stop being kept from here on.
        while rt.backlog.has_pending():
            rt.backlog.pop_any()
        await _publish_while_disconnected(rt, [0.2])
        return rt.backlog.has_pending()

    assert run(body) is False


# --- actions: apply_actions, the diff ------------------------------------------


def test_applying_an_action_creates_a_client_and_reports_no_errors():
    async def body(rt):
        errors = await rt.apply_actions(by_slug([_action_cfg("count")]))
        assert errors == []
        assert "count" in rt._actions

    run(body)


def test_an_unresolvable_action_type_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_actions(by_slug(
            [_action_cfg("bad", type_="nonexistent_pkg/action/Ghost"), _action_cfg("good")]
        ))

    errors = run(body)
    assert {e.slug for e in errors} == {"bad"}


def test_a_goal_template_that_cannot_build_the_goal_is_a_per_slug_error():
    """The 3.0 format's successor to the dotted-path check. A parameter's name is no
    longer a path into the Goal, so there is nothing to resolve it against —
    what config-apply can still answer is whether the template the developer
    wrote could ever produce this Goal at all, and `warp_factor` is not a
    field of one."""

    async def body(rt):
        return await rt.apply_actions(by_slug(
            [_action_cfg("count", message={"warp_factor": 9}, parameters={})]
        ))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "count"
    assert errors[0].kind == "action"
    assert errors[0].code == "unknown"


def test_a_placeholder_naming_nothing_declared_is_a_per_slug_error():
    """The narrower half of contracts' `undeclared_parameter`: the cloud owns
    the whole-document version (it holds the joined index across `messages:`
    and the entry's own `parameters:`), but a template with a placeholder
    this entry cannot fill is unbuildable here too, and finding that out at
    apply time beats finding it out on the first invoke."""

    async def body(rt):
        return await rt.apply_actions(by_slug(
            [_action_cfg("count", message={"order": "${howfar}"}, parameters={})]
        ))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "count"
    assert "howfar" in errors[0].message


def test_an_action_with_no_declared_parameters_and_an_empty_goal_applies():
    """An absent `message` normalises to `{}` in the parser, and an empty
    Goal is the ordinary case, not an error."""

    async def body(rt):
        return await rt.apply_actions(by_slug(
            [_action_cfg("count", message={}, parameters={})]
        ))

    assert run(body) == []


def test_removing_an_action_slug_destroys_its_client():
    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("count")]))
        await rt.apply_actions(by_slug([]))
        assert "count" not in rt._actions

    run(body)


def test_retargeting_an_action_slugs_ros_name_replaces_the_client():
    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("x", ros_name="/count")]))
        first_client = rt._actions["x"].client
        await rt.apply_actions(by_slug([_action_cfg("x", ros_name="/count2")]))
        second_client = rt._actions["x"].client
        return first_client, second_client, rt._actions["x"].ros_name

    first, second, ros_name = run(body)
    assert first is not second
    assert ros_name == "/count2"


def test_an_unchanged_action_keeps_the_same_client_object():
    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("x")]))
        before = rt._actions["x"].client
        await rt.apply_actions(by_slug([_action_cfg("x")]))  # same slug, same shape
        after = rt._actions["x"].client
        return before, after

    before, after = run(body)
    assert before is after


# --- a config change must not orphan a job -------
#
# Destroying an ActionClient/service Client silently kills the pending
# result callback with it — before this fix, no terminal job_update was
# ever emitted, the slug answered `busy` forever, and the orphan
# permanently held a MAX_TRACKED_JOBS slot. Every test below checks three
# things: (a) a terminal update *is* emitted, (b) tracked_count() returns
# to its pre-job baseline, (c) the slug accepts a new invoke afterward —
# the one a user would actually notice.


def test_removing_an_actions_slug_while_a_job_is_running_settles_it_lost():
    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            baseline = rt.jobs.tracked_count()
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = [await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)]
            assert updates[-1].state == "running"  # sanity: the goal is actually in flight
            assert rt.jobs.tracked_count() == baseline + 1  # sanity

            # The config change that orphans it — the slug is removed
            # outright, taking its ActionClient with it.
            await rt.apply_actions(by_slug([]))
            updates += await _drain_until_terminal(rt)

            # (a) a terminal update was emitted, not silence.
            settled = updates[-1]
            assert settled.job_id == "job-1"
            assert settled.state == "lost"
            assert settled.error[0] == "config_changed"

            # Delivery is what actually frees the slot/slug (same rule
            # every other terminal update follows — jobs.py's own
            # mark_delivered docstring) — simulate what client.py's real
            # pump does once the frame is actually sent.
            rt.jobs.mark_delivered(settled)

            # (b) the tracked-job bound is not permanently spent.
            after_delivery = rt.jobs.tracked_count()

            # (c) the slug accepts a new invoke — it has to be reconfigured
            # first, since apply_actions([]) removed it entirely.
            stop_server_2 = _start_fibonacci_server(steps=3)
            try:
                await rt.apply_actions(by_slug([_action_cfg("count")]))
                await rt.invoke("job-2", "count", {"order": 3}, patience_ms=15000)
                retry_updates = await _drain_until_terminal(rt)
            finally:
                stop_server_2()

            return baseline, after_delivery, retry_updates
        finally:
            stop_server()

    baseline, after_delivery, retry_updates = run(body)
    assert after_delivery == baseline  # (b)
    assert retry_updates[-1].job_id == "job-2"  # (c)
    assert retry_updates[-1].state == "succeeded"  # a real goal actually ran


def test_retargeting_an_actions_slug_while_a_job_is_running_settles_it_lost():
    """The retarget path is a separate branch from removal in
    `_apply_actions` — the rule that already required this distinction for
    cameras ('any changed field... not only a source retarget') needs the
    same coverage for jobs."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05, action_name="/count")
        try:
            await rt.apply_actions(by_slug([_action_cfg("count", ros_name="/count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = [await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)]
            assert updates[-1].state == "running"  # sanity

            # Retargeted, not removed — still configured, but a new client.
            await rt.apply_actions(by_slug([_action_cfg("count", ros_name="/count2")]))
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].job_id == "job-1"
    assert updates[-1].state == "lost"
    assert updates[-1].error[0] == "config_changed"


def test_removing_a_services_slug_while_a_call_is_running_settles_it_lost():
    async def body(rt):
        stop_server = _start_trigger_server(delay=0.5)
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            baseline = rt.jobs.tracked_count()
            await rt.invoke("job-1", "do_it", {}, patience_ms=15000)
            running = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert running.state == "running"  # sanity
            assert rt.jobs.tracked_count() == baseline + 1  # sanity

            await rt.apply_services(by_slug([]))  # the config change that orphans it
            settled = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert settled.job_id == "job-1"
            assert settled.state == "lost"  # (a)
            assert settled.error[0] == "config_changed"

            rt.jobs.mark_delivered(settled)
            after_delivery = rt.jobs.tracked_count()

            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-2", "do_it", {}, patience_ms=15000)
            retry_updates = await _drain_until_terminal(rt)
            return baseline, after_delivery, retry_updates
        finally:
            stop_server()

    baseline, after_delivery, retry_updates = run(body)
    assert after_delivery == baseline  # (b)
    assert retry_updates[-1].job_id == "job-2"  # (c)
    assert retry_updates[-1].state == "succeeded"


def test_removing_an_idle_actions_slug_settles_nothing():
    """The common case — most config-apply destroys are of a slug with
    nothing running — must stay a silent no-op, not invent a job that was
    never there."""

    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("count")]))
        await rt.apply_actions(by_slug([]))  # nothing was ever invoked
        assert rt.jobs.updates.empty()
        return rt.jobs.tracked_count()

    assert run(body) == 0


def test_a_config_change_after_goal_timeout_does_not_re_settle_it():
    """Superseded: an earlier version of this test asserted that a
    job already reported `goal_timeout` got a *second*, corrective
    `lost`/`config_changed` update when a later config change touched its
    slug — reasoned about, at the time, as the same "honest second word"
    shape `_on_late_goal_cancel` already uses deliberately. The sharper,
    more important case in the same family (a *succeeded* job
    silently overwritten by `lost`) and the fix for that is a blanket
    rule, not a case-by-case one: `_settle_orphaned_job` never re-settles
    a job that has already reached *any* terminal state, `goal_timeout`
    included. One word, whichever one was true first, stands — nothing
    here may add a second."""

    async def body(rt):
        # No server at all — the goal is never accepted, so the watchdog's
        # own goal_timeout fires on schedule (patience_ms short so the
        # test doesn't wait out a realistic patience).
        await rt.apply_actions(by_slug([_action_cfg("count")]))
        await rt.invoke("job-1", "count", {"order": 5}, patience_ms=100)
        timeout_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)

        # Still held — undelivered — same window that used to make a
        # second update possible.
        assert rt.jobs.running_job_id("count") == "job-1"

        await rt.apply_actions(by_slug([]))  # must not find a "running" job to settle
        assert rt.jobs.updates.empty()
        return timeout_update

    timeout_update = run(body)
    assert timeout_update.job_id == "job-1"
    assert timeout_update.state == "failed"
    assert timeout_update.error[0] == "goal_timeout"


# --- a genuinely terminal job must never be re-settled -
#
# `_settle_orphaned_job` used `running_job_id(slug) is not None` as its
# trigger, which answers "not yet delivered", not "still running" (a job
# stays named until `mark_delivered`). A `succeeded`, undelivered job
# could have that true outcome silently overwritten by a false
# `lost`/`config_changed` if a config change touched its slug first — not
# merely vaguer than the true answer (the goal_timeout case above), but
# false.


def test_a_genuinely_succeeded_undelivered_job_survives_a_config_change():
    """The reproduction: a real action runs to genuine completion, its
    `succeeded` update sits undelivered (nothing here calls
    `mark_delivered`), and a config change touches its slug. Before this
    guard, the runtime queued a second update, `lost`/`config_changed`,
    silently replacing the true outcome. After it: nothing is queued — the
    slug is destroyed, but the job's already-true answer stays."""

    async def body(rt):
        # A short, fast-finishing goal — the point is that it actually
        # completes for real before the config change arrives.
        stop_server = _start_fibonacci_server(steps=1, step_delay=0.01)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            succeeded_update = await _drain_until_terminal(rt)
            assert succeeded_update[-1].state == "succeeded"  # sanity

            assert rt.jobs.running_job_id("count") == "job-1"  # undelivered
            assert rt.jobs.state_of("job-1") == "succeeded"

            await rt.apply_actions(by_slug([]))  # must not find a "running" job here
            assert rt.jobs.updates.empty()
            return rt.jobs.state_of("job-1")
        finally:
            stop_server()

    # The record itself must still say `succeeded` too — not just "no frame
    # was queued", but "the state nobody may silently overwrite is intact".
    assert run(body) == "succeeded"


def test_a_genuinely_running_job_still_settles_lost_on_a_config_change():
    """The guard must not close the door the `lost` settlement opens: a job
    that is *actually* still running (never reached a terminal state on
    its own) is exactly what `_settle_orphaned_job` exists for, and this
    must keep working."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = [await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)]
            assert updates[-1].state == "running"  # sanity: genuinely in flight
            assert rt.jobs.state_of("job-1") == "running"

            await rt.apply_actions(by_slug([]))  # the config change that orphans it
            settled = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            while settled.state == "running":  # skip any further feedback updates
                settled = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            return settled
        finally:
            stop_server()

    settled = run(body)
    assert settled.job_id == "job-1"
    assert settled.state == "lost"
    assert settled.error[0] == "config_changed"


# --- invoke / cancel: a real goal lifecycle -------------------------------------


def test_invoking_a_configured_action_runs_a_real_goal_with_feedback_and_result():
    async def body(rt):
        stop_server = _start_fibonacci_server(steps=3)
        try:
            assert await rt.apply_actions(by_slug([_action_cfg("count")])) == []
            await rt.invoke("job-1", "count", {"order": 3}, patience_ms=15000)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert [u.job_id for u in updates] == ["job-1"] * len(updates)
    assert any(u.state == "running" and u.feedback is not None for u in updates[:-1])
    # Fibonacci has no `progress` field — best-effort extraction stays None,
    # the full feedback body is still forwarded regardless.
    assert all(u.progress is None for u in updates)
    final = updates[-1]
    assert final.state == "succeeded"
    assert final.result["sequence"] == [0, 1, 1, 2, 3]


def test_a_rejected_goal_is_reported_failed_immediately():
    async def body(rt):
        stop_server = _start_fibonacci_server(accept=False)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 1}, patience_ms=15000)
            return await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
        finally:
            stop_server()

    update = run(body)
    assert update.state == "failed"
    assert update.error[0] == "goal_rejected"


def test_invoke_with_unbuildable_params_is_reported_failed_without_touching_ros():
    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("count")]))
        await rt.invoke("job-1", "count", {"not_a_real_field": 1}, patience_ms=15000)
        return await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)

    # No server running at all — a parameter_invalid must be caught before
    # any goal is ever sent.
    update = run(body)
    assert update.state == "failed"
    assert update.error[0] == "parameter_invalid"


def test_invoking_an_unconfigured_slug_reports_unknown_slug():
    async def body(rt):
        await rt.invoke("job-1", "no-such-slug", {}, patience_ms=15000)
        return await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)

    update = run(body)
    assert update.state == "failed"
    assert update.error[0] == "unknown_slug"


def test_a_goal_runs_to_completion_even_if_nobody_drains_its_updates():
    """The disconnect-survival guarantee, proven at the ROS level:
    `RosRuntime` never learns whether a websocket exists, so not draining
    `rt.jobs.updates` — what a disconnected bridge looks like from here —
    must not slow or stop the goal. Everything produced while
    "disconnected" sits in the queue, in order, until someone looks
    (`_pump_jobs` on reconnect, tested at the wire level in
    test_client_jobs.py)."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=8, step_delay=0.03)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            # Deliberately not reading rt.jobs.updates while the goal runs.
            await asyncio.sleep(0.5)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[0].state == "running"  # the goal was accepted
    assert updates[-1].state == "succeeded"
    assert all(u.job_id == "job-1" for u in updates)
    # In order — the queue is FIFO, nothing here reorders or drops.
    assert [u.timestamp_ms for u in updates] == sorted(u.timestamp_ms for u in updates)


def test_a_second_invoke_of_a_running_slug_is_refused_busy():
    async def body(rt):
        stop_server = _start_fibonacci_server(steps=20, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            await rt.invoke("job-2", "count", {"order": 5}, patience_ms=15000)
            updates = []
            while True:
                update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
                updates.append(update)
                if update.job_id == "job-2":
                    return updates
        finally:
            stop_server()

    updates = run(body)
    busy = next(u for u in updates if u.job_id == "job-2")
    assert busy.state == "failed"
    assert busy.error[0] == "busy"


def test_cancel_by_slug_issues_a_real_ros_goal_cancel():
    async def body(rt):
        # Long enough that the cancel unambiguously arrives mid-execution
        # rather than racing the goal to its own completion — but not so
        # many steps that the (int32) feedback sequence overflows first.
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = []
            # A couple of feedback updates first, so the cancel-request round
            # trip (client -> server accept -> is_cancel_requested) has no
            # chance of racing the goal's very first step.
            while len([u for u in updates if u.state == "running"]) < 2:
                updates.append(await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0))
            await rt.cancel_job("count", None)
            updates += await _drain_until_terminal(rt)
            return updates
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].state == "cancelled"


def test_cancelling_a_slug_with_nothing_running_is_a_silent_no_op():
    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("count")]))
        await rt.cancel_job("count", None)  # nothing running — must not raise

    run(body)  # must not raise


def test_cancel_by_the_matching_job_id_issues_a_real_ros_goal_cancel():
    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = []
            while len([u for u in updates if u.state == "running"]) < 2:
                updates.append(await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0))
            await rt.cancel_job("count", "job-1")
            updates += await _drain_until_terminal(rt)
            return updates
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].state == "cancelled"


def test_cancel_by_a_non_matching_job_id_cancels_nothing_and_never_falls_back_to_the_slug():
    """A caller who names an id has ruled out "whatever is
    running" as the answer. Falling back to the slug would stop a machine
    the caller did not name — the exact bug this field exists to close."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = []
            while len([u for u in updates if u.state == "running"]) < 2:
                updates.append(await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0))
            await rt.cancel_job("count", "some-other-job-id")
            # No cancel was issued — give the (non-existent) effect a real
            # chance to show up before concluding it did not.
            await asyncio.sleep(0.2)
            return rt.jobs.running_job_id("count")
        finally:
            stop_server()

    assert run(body) == "job-1"  # still running — untouched


def test_a_non_matching_job_id_and_an_idle_slug_are_logged_distinguishably():
    """The two silences must not read the same way: a caller
    who names a stale id and a caller who cancels an already-idle slug are
    different situations, and conflating them in the logs is how this
    whole addressing problem started. Bypasses the broken `caplog`
    fixture with a handler attached directly to the
    module logger, the same pattern `test_a_dropped_snapshot_is_logged_
    so_it_is_discoverable_not_silent` already uses."""
    import logging

    class _RecordingHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = _RecordingHandler()
    ros_runtime_log = logging.getLogger("fleetless_bridge.ros_runtime")
    ros_runtime_log.addHandler(handler)
    # The idle-slug case logs at INFO (routine, same reasoning as the
    # existing "nothing running" no-op) — the default root level (WARNING)
    # would otherwise swallow it before it reaches the handler above.
    original_level = ros_runtime_log.level
    ros_runtime_log.setLevel(logging.INFO)

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            updates = []
            while len([u for u in updates if u.state == "running"]) < 2:
                updates.append(await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0))
            await rt.cancel_job("count", "some-other-job-id")  # mismatch
            await rt.cancel_job("idle-slug-nothing-here", None)  # genuinely idle
        finally:
            stop_server()

    try:
        run(body)
    finally:
        ros_runtime_log.removeHandler(handler)
        ros_runtime_log.setLevel(original_level)

    messages = [r.getMessage() for r in handler.records]
    mismatch_lines = [m for m in messages if "some-other-job-id" in m]
    assert mismatch_lines, "expected a log line naming the mismatched job_id"
    idle_lines = [m for m in messages if "idle-slug-nothing-here" in m]
    assert idle_lines, "expected a log line for the idle-slug no-op"
    # Distinguishable, not merely both present: the mismatch line must say
    # something the idle-slug line does not (and vice versa).
    assert mismatch_lines[0] != idle_lines[0]


# --- a cancel before acceptance must not be dropped --
#
# `_active_goals[job_id]` populates only once the action server answers
# `send_goal_async` — but the cloud answers the invoke's REST call the
# instant the job is minted, so a caller can legitimately cancel inside
# that window. Before this fix that found no goal handle and silently
# returned: a cancel for the *right* job, discarded, while the caller was
# told it worked.


def test_a_cancel_arriving_before_acceptance_is_remembered_and_applied():
    async def body(rt):
        # accept_delay keeps the goal unaccepted long enough for the cancel
        # below to land inside the window — 300ms is generous against a
        # cancel dispatched essentially instantly.
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05, accept_delay=0.3)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            await rt.cancel_job("count", "job-1")  # the goal is not accepted yet
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].job_id == "job-1"
    assert updates[-1].state == "cancelled"


def test_a_cancel_by_slug_before_acceptance_is_also_remembered():
    """The remembered-cancel path has to work for a `job_id=None` cancel
    too — the slug-only form, not only the by-id form."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05, accept_delay=0.3)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            await rt.cancel_job("count", None)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].job_id == "job-1"
    assert updates[-1].state == "cancelled"


def test_a_pre_acceptance_cancel_for_a_goal_that_gets_rejected_does_nothing_odd():
    """The window can also close the other way — the server rejects the
    goal instead of accepting it. The remembered cancel must not survive
    that or try to act on a goal handle that was never actually granted;
    the ordinary `goal_rejected` failure is the whole story."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=5, accept=False)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            await rt.cancel_job("count", "job-1")
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].job_id == "job-1"
    assert updates[-1].state == "failed"
    assert updates[-1].error[0] == "goal_rejected"


def test_a_pre_acceptance_cancel_and_a_no_ros_cancel_service_are_both_logged():
    """The original proof of the bug was `docker logs ... | grep -ci cancel`
    returning 0 — every branch of `_cancel_job` must be discoverable in the
    logs now, not just two of the three."""
    import logging

    class _RecordingHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = _RecordingHandler()
    ros_runtime_log = logging.getLogger("fleetless_bridge.ros_runtime")
    ros_runtime_log.addHandler(handler)
    original_level = ros_runtime_log.level
    ros_runtime_log.setLevel(logging.INFO)

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05, accept_delay=0.3)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.apply_services(by_slug([_service_cfg("do_it")]))

            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000)
            await rt.cancel_job("count", "job-1")  # pending-acceptance path

            stop_trigger = _start_trigger_server(delay=0.3)
            try:
                await rt.invoke("job-2", "do_it", {}, patience_ms=15000)
                await rt.cancel_job("do_it", "job-2")  # no-ROS-cancel path
            finally:
                stop_trigger()

            await _drain_until_terminal(rt)  # let job-1 (cancelled) settle
        finally:
            stop_server()

    try:
        run(body)
    finally:
        ros_runtime_log.removeHandler(handler)
        ros_runtime_log.setLevel(original_level)

    messages = [r.getMessage() for r in handler.records]
    pending_lines = [m for m in messages if "job-1" in m and "not been accepted" in m]
    assert pending_lines, "expected a log line for the pre-acceptance cancel"
    applied_lines = [m for m in messages if "job-1" in m and "applying the cancel" in m]
    assert applied_lines, "expected a log line when the remembered cancel is applied"
    service_lines = [m for m in messages if "job-2" in m and "no ROS-level cancel" in m]
    assert service_lines, "expected a log line for the no-ROS-cancel case"


# --- command dispatch order must survive an adversarial thread pool --------


class _ReversingExecutor(concurrent.futures.ThreadPoolExecutor):
    """A stand-in for asyncio's default `ThreadPoolExecutor` that runs the
    first two callables submitted to it in *reverse* order — the worst case
    real scheduling could produce for two submissions issued moments apart
    (two idle worker threads racing an OS lock have no obligation to
    acquire it in submission order). This is the exact reordering that let
    a `cancel` overtake the `invoke` before it in a zero-gap burst — only
    reproduced there, since any real gap gives a real pool no chance to
    race.

    Good enough to prove the fix in `RosRuntime._enqueue`: `invoke`/
    `cancel_job` enqueue onto `_work_queue` synchronously, on the caller's
    own thread, before ever touching this executor — so nothing this
    executor does to the *wait* step can still reorder them."""

    #
    # It SUBCLASSES `ThreadPoolExecutor` rather than merely quacking like
    # one — not a preference, a distribution difference: `loop.
    # set_default_executor` deprecated a non-ThreadPoolExecutor argument in
    # Python 3.8 and REMOVED it in 3.12, so on jazzy (3.12) and lyrical
    # (3.14) a duck-typed stand-in raises `TypeError: executor must be
    # ThreadPoolExecutor instance` before either test below reaches its own
    # assertion. Nothing in the base class runs: `submit` is overridden
    # entirely, no worker thread starts — an isinstance check satisfied,
    # not a real pool used.

    def __init__(self):
        # max_workers=1: the pool is never used, and the default spawns a
        # worker count derived from the machine's CPUs at first submit.
        super().__init__(max_workers=1)
        self._pending = []

    def submit(self, fn, *args):
        future = concurrent.futures.Future()
        self._pending.append((fn, args, future))
        if len(self._pending) < 2:
            return future
        pending, self._pending = self._pending, []
        for fn_, args_, future_ in reversed(pending):
            if not future_.set_running_or_notify_cancel():
                continue
            try:
                future_.set_result(fn_(*args_))
            except Exception as exc:  # noqa: BLE001 - propagated via the future
                future_.set_exception(exc)
        return future

    def shutdown(self, wait=True, **kwargs):
        """Satisfies the `Executor` protocol asyncio's loop teardown expects
        of whatever `set_default_executor` was given. It does NOT call up:
        `submit` never reaches the real pool, so there is no worker thread to
        join, and the base class's own shutdown would be waiting on nothing.
        `**kwargs` absorbs `cancel_futures`, which asyncio's teardown passes
        from Python 3.9 on."""


def test_two_commands_dispatched_back_to_back_reach_ros_in_dispatch_order():
    """The frame-ordering bug, closed here and isolated from any ROS
    action's own accept/response lifecycle — see the sibling test below for
    why that isolation matters and what it does *not* cover. Two
    `publish`es (no ROS-side round trip — the work function is the entire
    effect) are dispatched via `ensure_future` back-to-back, exactly as
    `client.py`'s `_dispatch_invoke`/`_dispatch_cancel`/`_dispatch_publish`
    do for two frames arriving with no gap, while the event loop's default
    executor is replaced with one that completes two submissions in
    *reverse* order — the worst case a real pool's thread scheduling could
    produce. An independent subscriber (not the bridge's own state)
    witnesses which message actually reaches the topic first. Old, buggy
    `_submit`: the enqueue itself went through this executor, so the
    reversal reaches `_work_queue` and `linear.x=2.0` would be published
    before `linear.x=1.0`. Fixed `_enqueue`: it runs synchronously on the
    dispatching (event loop) thread before either `publish()` call ever
    touches the executor, so `_work_queue` — and therefore the topic — sees
    dispatch order regardless of what the executor does to the wait step
    afterwards."""

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("cmd", topic="/ordering_test")]))
        received, _sub, stop_sub = _start_independent_subscriber("/ordering_test", Twist)
        try:
            asyncio.get_event_loop().set_default_executor(_ReversingExecutor())
            first = asyncio.ensure_future(rt.publish("cmd", {"speed": 1.0}))
            second = asyncio.ensure_future(rt.publish("cmd", {"speed": 2.0}))
            await first
            await second
            wait_until(lambda: len(received) >= 2)
            return [msg.linear.x for msg in received]
        finally:
            stop_sub()

    xs = run(body)
    assert xs == [1.0, 2.0]


def test_a_cancel_dispatched_right_after_an_invoke_is_not_reordered_ahead_of_it():
    """The same mechanism as the sibling test above, through the actual
    invoke/cancel pair the bug report names ("a stop cannot be overtaken by
    the go before it"). A 10ms gap between the two `ensure_future` calls is
    real time, not application-level slack the bridge is promised — it only
    exists so the real ROS action server (a separate OS thread, unaffected
    by anything the test does to the event loop) has room to accept the
    goal before the cancel is dispatched, isolating this test to the one
    thing that changed: whether `cancel_job`'s work reaches `_work_queue`
    before or after `invoke`'s. Old, buggy `_submit`: the enqueue for
    *both* calls is gated behind this test's adversarial executor (which
    only ever fires once two callables are pending) — the goal is not even
    sent until the reversal happens, so 10ms of real time before that point
    buys the goal nothing, and cancel finds nothing running regardless. New
    `_enqueue`: dispatching `invoke` alone already queues `_invoke_action`
    and wakes the ROS thread immediately, no reversing executor involved
    yet — 10ms is enormous headroom for a loopback goal-accept round trip,
    so by the time `cancel` is dispatched the goal is already running, and
    the adversarial executor has nothing left to reorder that would matter."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            asyncio.get_event_loop().set_default_executor(_ReversingExecutor())
            invoke_task = asyncio.ensure_future(rt.invoke("job-1", "count", {"order": 5}, patience_ms=15000))
            await asyncio.sleep(0.01)
            cancel_task = asyncio.ensure_future(rt.cancel_job("count", None))
            await invoke_task
            await cancel_task
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].state == "cancelled"


# --- goal-acceptance timeout -------------------------------------------------


def test_a_goal_whose_server_never_responds_times_out_and_frees_the_slug():
    """An invoke whose action server is entirely absent left the slug wedged
    forever before this fix: `send_goal_async()`'s future never resolves, so
    `_on_goal_response` never runs, no job_update is ever emitted (not even
    `running`), and `_active_goals` never gets an entry — making
    `cancel_job` a silent no-op too. `patience_ms` is passed short on the
    invoke itself (it travels per-call now, there is no constructor
    default to inject) so the test doesn't wait out a realistic patience."""

    async def body(rt):
        # No server started at all — the action type resolves and the
        # client is created, but nothing is listening on "/count".
        assert await rt.apply_actions(by_slug([_action_cfg("count")])) == []
        await rt.invoke("job-1", "count", {"order": 5}, patience_ms=100)
        return await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)

    update = run(body)
    assert update.state == "failed"
    assert update.error[0] == "goal_timeout"


def test_a_slug_freed_by_goal_timeout_accepts_a_new_invoke():
    """The point of `goal_timeout` is that it frees the slug, not merely
    that it reports something — an operator retrying a slug like `dock`,
    `home` or `stop` against a server that has since come back must not
    find it still wedged."""

    async def body(rt):
        await rt.apply_actions(by_slug([_action_cfg("count")]))
        await rt.invoke("job-1", "count", {"order": 5}, patience_ms=100)
        timeout_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
        assert timeout_update.error[0] == "goal_timeout"  # sanity
        # `jobs.finish()` — what actually frees the slug for
        # `running_job_id` — only runs from `mark_delivered`, once
        # client.py has sent the frame (jobs.py: a terminal update must not
        # retire its job before its outcome genuinely reached the cloud).
        # This test drains `rt.jobs.updates` directly, bypassing client.py,
        # so it stands in for that acknowledgment itself — exactly what a
        # real session's `_pump_jobs` does right after `ws.send()`
        # succeeds.
        rt.jobs.mark_delivered(timeout_update)

        stop_server = _start_fibonacci_server(steps=2, step_delay=0.02)
        try:
            # DDS discovery of the brand-new server is real, uncontrolled
            # latency — waited out here rather than folded into the
            # retry's own `patience_ms`, which is what this test is proving
            # is generous enough once a server actually exists.
            wait_until(lambda: rt._actions["count"].client.server_is_ready())
            await rt.invoke("job-2", "count", {"order": 3}, patience_ms=100)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].job_id == "job-2"
    assert updates[-1].state == "succeeded"


def test_a_goal_accepted_after_its_timeout_was_declared_is_cancelled():
    """A goal can still be accepted after the bridge has already reported it
    `goal_timeout` — the server was merely slow, not absent. The platform
    already believes this job never started; the robot must not be left
    executing it regardless, so the bridge asks the server to stop it as
    soon as the late accept arrives."""

    async def body(rt):
        stop_server = _start_fibonacci_server(steps=40, step_delay=0.05, accept_delay=0.3)
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=100)
            timeout_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert timeout_update.state == "failed"
            assert timeout_update.error[0] == "goal_timeout"

            wait_until(lambda: len(stop_server.cancel_requests) >= 1, timeout=2.0)
            return stop_server.cancel_requests
        finally:
            stop_server()

    cancel_requests = run(body)
    assert len(cancel_requests) == 1


def test_a_cancel_refused_after_a_late_accept_is_reported_lost():
    """If the server refuses even the corrective cancel, the robot is now
    executing a goal nothing can stop, and the platform believes it never
    started — worse than `goal_timeout`, which at least implied nothing was
    moving. The honest correction is a second, `lost` job_update for the
    same job_id (`lost` is already a valid `job_update.state` on the wire —
    see contracts)."""

    async def body(rt):
        stop_server = _start_fibonacci_server(
            steps=40, step_delay=0.05, accept_delay=0.3, honor_cancel=False
        )
        try:
            await rt.apply_actions(by_slug([_action_cfg("count")]))
            await rt.invoke("job-1", "count", {"order": 5}, patience_ms=100)
            timeout_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert timeout_update.state == "failed"
            assert timeout_update.error[0] == "goal_timeout"

            return await asyncio.wait_for(rt.jobs.updates.get(), timeout=2.0)
        finally:
            stop_server()

    update = run(body)
    assert update.job_id == "job-1"
    assert update.state == "lost"


def test_a_shorter_patience_ms_times_out_before_a_longer_one_on_the_same_call_shape():
    """`patience_ms` travels with the call and is honoured per call — a
    short one gives up before a long one does, measurably, on two
    concurrent invokes against servers that never answer either. Before
    this, both shared one constant (`GOAL_ACCEPT_TIMEOUT_S`), and a
    caller's own patience could make no difference at all."""

    async def body(rt):
        # No servers started — both actions resolve their type but nothing
        # is listening, so neither goal is ever accepted (same shape as
        # test_a_goal_whose_server_never_responds_times_out_and_frees_the_slug).
        await rt.apply_actions(by_slug([
            _action_cfg("short-patience", ros_name="/no_such_server_short"),
            _action_cfg("long-patience", ros_name="/no_such_server_long"),
        ]))
        start = time.monotonic()
        await rt.invoke("job-short", "short-patience", {"order": 1}, patience_ms=100)
        await rt.invoke("job-long", "long-patience", {"order": 1}, patience_ms=2000)

        short_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
        short_elapsed = time.monotonic() - start
        assert short_update.job_id == "job-short"
        assert short_update.error[0] == "goal_timeout"

        long_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
        long_elapsed = time.monotonic() - start
        assert long_update.job_id == "job-long"
        assert long_update.error[0] == "goal_timeout"
        return short_elapsed, long_elapsed

    short_elapsed, long_elapsed = run(body)
    # Loose bounds — this only has to prove the two deadlines are actually
    # different and in the right order, not pin exact watchdog timing.
    assert short_elapsed < 1.0
    assert long_elapsed > 1.5
    assert long_elapsed > short_elapsed


# --- services: apply_services, the diff -----------------------------------------


def test_applying_a_service_creates_a_client_and_reports_no_errors():
    async def body(rt):
        errors = await rt.apply_services(by_slug([_service_cfg("do_it")]))
        assert errors == []
        assert "do_it" in rt._services

    run(body)


def test_an_unresolvable_service_type_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_services(by_slug(
            [_service_cfg("bad", type_="nonexistent_pkg/srv/Ghost"), _service_cfg("good")]
        ))

    errors = run(body)
    assert {e.slug for e in errors} == {"bad"}


def test_a_request_template_that_cannot_build_the_request_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_services(by_slug(
            [_service_cfg("do_it", message={"warp_factor": 9})]
        ))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "do_it"
    assert errors[0].kind == "service"
    assert errors[0].code == "unknown"


def test_a_service_with_an_empty_request_applies():
    """`std_srvs/srv/Trigger` has no request fields at all — the format's own
    reference example, and the reason an absent `message` is not an error."""

    async def body(rt):
        return await rt.apply_services(by_slug([_service_cfg("do_it", message={})]))

    assert run(body) == []


def test_removing_a_service_slug_destroys_its_client():
    async def body(rt):
        await rt.apply_services(by_slug([_service_cfg("do_it")]))
        await rt.apply_services(by_slug([]))
        assert "do_it" not in rt._services

    run(body)


# --- services: a real request/response call --------------------------------------


def test_invoking_a_configured_service_calls_it_and_reports_the_response():
    async def body(rt):
        stop_server = _start_trigger_server(success=True, message="all good")
        try:
            assert await rt.apply_services(by_slug([_service_cfg("do_it")])) == []
            await rt.invoke("job-1", "do_it", {}, patience_ms=15000)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    # running (dispatched) then succeeded (the response) — a service call has
    # no feedback/progress, just the two.
    assert [u.state for u in updates] == ["running", "succeeded"]
    assert updates[-1].result == {"success": True, "message": "all good"}


def test_a_service_with_no_server_is_reported_unavailable():
    async def body(rt):
        await rt.apply_services(by_slug([_service_cfg("do_it", ros_name="/nobody_is_serving_this")]))
        await rt.invoke("job-1", "do_it", {}, patience_ms=15000)
        return await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)

    update = run(body)
    assert update.state == "failed"
    assert update.error[0] == "service_unavailable"


def test_a_second_service_invoke_of_a_running_slug_is_refused_busy():
    async def body(rt):
        # A server slow enough that job-1 is still "running" when job-2 is
        # dispatched right behind it.
        stop_server = _start_trigger_server(delay=0.5)
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-1", "do_it", {}, patience_ms=15000)
            await rt.invoke("job-2", "do_it", {}, patience_ms=15000)
            updates = []
            while True:
                update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
                updates.append(update)
                if update.job_id == "job-2":
                    return updates
        finally:
            stop_server()

    updates = run(body)
    busy = next(u for u in updates if u.job_id == "job-2")
    assert busy.state == "failed"
    assert busy.error[0] == "busy"


def test_cancelling_a_running_service_call_is_a_silent_no_op():
    # Services have no ROS-level cancel — cancel_job must not raise, and must
    # not disturb the in-flight call.
    async def body(rt):
        stop_server = _start_trigger_server()
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-1", "do_it", {}, patience_ms=15000)
            await rt.cancel_job("do_it", None)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].state == "succeeded"


# --- 2n: service call patience ----------------------------------------------------


def test_a_service_that_never_responds_times_out_and_frees_the_slug():
    """A service whose server is ready but simply never answers used to wedge
    the slug forever — `call_async`'s future has no deadline of its own, and
    `_invoke_service`'s upfront `service_is_ready()` check only rules out the
    "nobody is listening" case (see `test_a_service_with_no_server_is_
    reported_unavailable`), not "someone is listening and never replies".
    `delay` is set far longer than `patience_ms` so the real response, if it
    ever arrived, would arrive well after this test has already asserted and
    torn the server down."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=3.0)
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-1", "do_it", {}, patience_ms=100)
            running = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert running.state == "running"  # sanity
            return await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
        finally:
            stop_server()

    update = run(body)
    assert update.state == "failed"
    assert update.error[0] == "service_timeout"


def test_a_slug_freed_by_service_timeout_accepts_a_new_invoke():
    """Mirrors `test_a_slug_freed_by_goal_timeout_accepts_a_new_invoke`: the
    point of `service_timeout` is that it frees the slug, not merely that it
    reports something."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=3.0)
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-1", "do_it", {}, patience_ms=100)
            await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)  # running, sanity
            timeout_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert timeout_update.error[0] == "service_timeout"  # sanity
            # `jobs.finish()` only runs from `mark_delivered` — stand in for
            # what client.py's `_pump_jobs` does right after a real `ws.send()`.
            rt.jobs.mark_delivered(timeout_update)
        finally:
            stop_server()

        stop_server = _start_trigger_server(success=True, message="second server")
        try:
            # DDS discovery of the brand-new server is real, uncontrolled
            # latency — same reasoning as the action-side equivalent test.
            wait_until(lambda: rt._services["do_it"].client.service_is_ready())
            await rt.invoke("job-2", "do_it", {}, patience_ms=15000)
            return await _drain_until_terminal(rt)
        finally:
            stop_server()

    updates = run(body)
    assert updates[-1].job_id == "job-2"
    assert updates[-1].state == "succeeded"


def test_a_late_service_response_after_timeout_never_arrives_as_a_second_update():
    """The asymmetry with the action path, made observable: a service
    timeout calls `remove_pending_request`, which rclpy documents as
    preventing the future from ever running its done callback — so unlike
    `_on_goal_response` (which can still fire "late" after `goal_timeout`,
    see `test_a_goal_accepted_after_its_timeout_was_declared_is_cancelled`),
    `_on_service_result` must never fire for this job_id at all, no matter
    how long the real server eventually takes. `delay` here is deliberately
    *longer than the whole test*, not just longer than `patience_ms` — the
    real response is still "in flight" (sleeping in the server's callback
    thread) when this test finishes and tears the server down, and must
    never have produced a second `job_update` up to that point."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=3.0)
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-1", "do_it", {}, patience_ms=100)
            await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)  # running, sanity
            timeout_update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert timeout_update.error[0] == "service_timeout"  # sanity
            # Long enough to have observed a late arrival if one were coming,
            # short enough to stay well inside the server's 3.0 s delay.
            await asyncio.sleep(0.5)
            return rt.jobs.updates.empty()
        finally:
            stop_server()

    assert run(body) is True


def test_removing_a_services_slug_after_it_times_out_does_not_double_settle():
    """The `_settle_orphaned_job` half of 2n: a config change arriving after
    `service_timeout` was already reported, but before the timed-out job_id's
    `_service_deadlines` entry existed for this to guard, used to be able to
    fire `_check_service_timeouts` a second time for a job already settled —
    exactly the class of bug already closed for the action path. Here
    the deadline is popped by `_settle_orphaned_job` itself, before the
    config-driven `lost` even happens, so there is nothing left in
    `_service_deadlines` for the watchdog to find."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=5.0)
        try:
            await rt.apply_services(by_slug([_service_cfg("do_it")]))
            await rt.invoke("job-1", "do_it", {}, patience_ms=50)
            await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)  # running, sanity

            await rt.apply_services(by_slug([]))  # orphans the call before its patience expires
            settled = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert settled.state == "lost"  # sanity
            assert settled.error[0] == "config_changed"  # sanity

            # Long enough to have crossed the original 50ms patience_ms and
            # observed a contradicting `service_timeout` if one were coming.
            await asyncio.sleep(0.3)
            return rt.jobs.updates.empty()
        finally:
            stop_server()

    assert run(body) is True


# --- the tracked-job bound ----------------------------------------------


def test_invoking_beyond_the_tracked_job_bound_is_refused_job_queue_full():
    """`max_tracked_jobs` is injected small (3) so the test doesn't have to
    actually create 200 jobs. Three slow calls fill every slot while still
    `running`; a fourth, on a slug that would otherwise be perfectly valid,
    is refused outright — refused, not queued: `JobUpdateQueue` stays
    unbounded and drop-nothing, but admission of *new* work is what this
    bound controls."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=0.5)
        try:
            await rt.apply_services(by_slug(
                [_service_cfg("svc-1"), _service_cfg("svc-2"), _service_cfg("svc-3"), _service_cfg("svc-4")]
            ))
            await rt.invoke("job-1", "svc-1", {}, patience_ms=15000)
            await rt.invoke("job-2", "svc-2", {}, patience_ms=15000)
            await rt.invoke("job-3", "svc-3", {}, patience_ms=15000)
            # The bound is already met by three still-running jobs — the
            # fourth must never reach the service at all.
            await rt.invoke("job-4", "svc-4", {}, patience_ms=15000)
            updates = []
            while len([u for u in updates if u.job_id == "job-4"]) == 0:
                updates.append(await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0))
            return updates
        finally:
            stop_server()

    updates = run(body, max_tracked_jobs=3)
    refusal = next(u for u in updates if u.job_id == "job-4")
    assert refusal.state == "failed"
    assert refusal.error[0] == "job_queue_full"
    # Structured, not just formatted into the message:
    # `jobQueueFullDetails` is the documented payload for
    # this code, so it must ride in `details`, not only in the sentence.
    assert refusal.details == {"limit": 3, "queued": 3}


def test_the_bound_is_on_admission_not_on_the_update_queue_itself():
    """The refusal must not cost anything already in flight: the three
    admitted jobs still complete and their updates still arrive — nothing
    about hitting the bound drops or truncates work that was already
    accepted, only the *next* one."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=0.2)
        try:
            await rt.apply_services(by_slug(
                [_service_cfg("svc-1"), _service_cfg("svc-2"), _service_cfg("svc-3"), _service_cfg("svc-4")]
            ))
            await rt.invoke("job-1", "svc-1", {}, patience_ms=15000)
            await rt.invoke("job-2", "svc-2", {}, patience_ms=15000)
            await rt.invoke("job-3", "svc-3", {}, patience_ms=15000)
            await rt.invoke("job-4", "svc-4", {}, patience_ms=15000)  # refused
            seen_terminal = set()
            while len(seen_terminal) < 4:
                update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
                if update.state in ("succeeded", "failed"):
                    seen_terminal.add(update.job_id)
            return None

        finally:
            stop_server()

    run(body, max_tracked_jobs=3)  # must not hang or raise — all four resolve


def test_delivering_a_tracked_job_frees_a_slot_for_a_new_invoke():
    """The bound counts what `JobManager.tracked_count()` counts — jobs not
    yet *delivered* — so freeing a slot means calling `mark_delivered`, the
    same acknowledgment client.py's `_pump_jobs` gives after a real
    `ws.send()` succeeds, not merely draining `rt.jobs.updates`."""

    async def body(rt):
        stop_server = _start_trigger_server(delay=0.05)
        try:
            await rt.apply_services(by_slug([_service_cfg("svc-1"), _service_cfg("svc-2")]))
            await rt.invoke("job-1", "svc-1", {}, patience_ms=15000)
            update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            while update.state != "succeeded":  # skip the "running" update first
                update = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert update.job_id == "job-1"  # sanity

            # Still refused — job-1 finished, but its update was only
            # dequeued above, never marked delivered.
            await rt.invoke("job-2", "svc-2", {}, patience_ms=15000)
            still_refused = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            assert still_refused.job_id == "job-2"
            assert still_refused.error[0] == "job_queue_full"  # sanity

            # Now deliver job-1's update for real — the slot it held frees.
            rt.jobs.mark_delivered(update)
            await rt.invoke("job-3", "svc-2", {}, patience_ms=15000)
            admitted = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            while admitted.state != "succeeded":  # skip its own "running" update
                admitted = await asyncio.wait_for(rt.jobs.updates.get(), timeout=5.0)
            return admitted
        finally:
            stop_server()

    admitted = run(body, max_tracked_jobs=1)
    assert admitted.job_id == "job-3"
    assert admitted.state == "succeeded"


# --- publishers: apply_publishers, the diff --------------------------------------


def test_applying_a_publisher_creates_a_handle_and_reports_no_errors():
    async def body(rt):
        errors = await rt.apply_publishers(by_slug([_publisher_cfg("drive")]))
        assert errors == []
        assert "drive" in rt._publishers

    run(body)


def test_an_unresolvable_publisher_type_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_publishers(by_slug(
            [_publisher_cfg("bad", type_="nonexistent_pkg/msg/Ghost", failsafe={}), _publisher_cfg("good")]
        ))

    errors = run(body)
    assert {e.slug for e in errors} == {"bad"}


def test_a_publish_template_that_cannot_build_the_message_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_publishers(by_slug(
            [_publisher_cfg("drive", message={"warp_factor": 9}, parameters={})]
        ))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "drive"
    assert errors[0].kind == "publisher"
    assert errors[0].code == "unknown"


# --- the failsafe's no-placeholder rule, which only the bridge enforces -------
#
# Contracts refuses a placeholder in an *inline* failsafe body and says
# plainly that it cannot answer the other half: a schema validating one
# publisher cannot see `messages:`, so a failsafe written `message: ${stop}`
# where `stop` holds a `${speed}` passes every schema there is.
#
# Both tests below use a **string** message on purpose. A leftover `${name}`
# in a numeric field is caught anyway by `set_message_fields`, which cannot
# put a string in a float64 — so a Twist would have proved nothing about this
# check, and deleting the check would have left those tests green. A `string`
# field accepts `"${speed}"` happily, builds a perfectly valid message, and
# publishes the literal text `${speed}` as the robot's emergency stop. That
# is the case the check exists for, and it is the case here.

_SAY_TYPE = "std_msgs/msg/String"


def _say_cfg(slug, failsafe, message=None):
    return _publisher_cfg(
        slug,
        topic="/chatter",
        type_=_SAY_TYPE,
        message={"data": "${text}"} if message is None else message,
        parameters={"text": ParameterSpec(type="string")},
        failsafe=failsafe,
    )


def test_a_placeholder_in_an_inline_failsafe_refuses_the_publisher():
    async def body(rt):
        return await rt.apply_publishers(by_slug([_say_cfg("say", {"data": "${text}"})]))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "say"
    assert errors[0].kind == "publisher"
    assert "placeholder" in errors[0].message and "text" in errors[0].message


def test_a_placeholder_behind_a_shared_failsafe_reference_refuses_the_publisher():
    """The case contracts states it cannot check."""

    async def body(rt):
        return await rt.apply_publishers(
            by_slug([_say_cfg("say", "${stop}")]),
            {"stop": {"data": "${text}"}},
        )

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "say"
    assert errors[0].kind == "publisher"
    assert "placeholder" in errors[0].message and "text" in errors[0].message


def test_a_placeholder_free_shared_failsafe_is_accepted_and_is_what_actually_fires():
    """The other side of the same gate, and the proof that the two tests
    above refuse the placeholder rather than the reference: a shared failsafe
    holding no placeholder is followed at apply time and is what the watchdog
    publishes."""

    async def body(rt):
        errors = await rt.apply_publishers(
            by_slug([_say_cfg("say", "${stop}")]),
            {"stop": {"data": "stop"}},
        )
        received, _, stop_sub = _start_independent_subscriber("/chatter", StringMsg)
        try:
            wait_until(lambda: rt._publishers["say"].handle.get_subscription_count() > 0)
            await rt.publish("say", {"text": "go"})
            wait_until(lambda: any(msg.data == "stop" for msg in received), timeout=5.0)
        finally:
            stop_sub()
        return errors

    assert run(body) == []


def test_a_republished_publisher_is_re_checked_for_a_placeholder_in_its_failsafe():
    """The same gate, on the path a running robot actually takes.

    A publisher is applied once and then applied again — the in-place update
    branch, not the create branch. Every other test in this file that calls
    apply_publishers twice either removes the slug or retargets it, so the
    branch that re-resolves a failsafe on republish was reachable by no test:
    deleting `resolve_failsafe_body` from it left the whole suite green.

    That is the wrong thing to leave undefended: a robot is configured once
    and republished many times, so republish is where a bad failsafe would
    actually arrive — silently, since a publisher that already exists keeps
    working while its failsafe quietly becomes unsendable.
    """

    async def body(rt):
        first = await rt.apply_publishers(
            by_slug([_say_cfg("say", "${stop}")]), {"stop": {"data": "stop"}}
        )
        assert first == [], "the publisher must exist before the republish is a republish"
        return await rt.apply_publishers(
            by_slug([_say_cfg("say", "${stop}")]), {"stop": {"data": "${text}"}}
        )

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "say"
    assert errors[0].kind == "publisher"
    assert "placeholder" in errors[0].message and "text" in errors[0].message


def test_a_shared_message_that_does_not_exist_refuses_the_publisher():
    async def body(rt):
        return await rt.apply_publishers(
            by_slug([_publisher_cfg("drive", failsafe="${stop}")]), {}
        )

    errors = run(body)
    assert len(errors) == 1
    assert "stop" in errors[0].message


def test_a_publish_template_may_reference_a_shared_message():
    async def body(rt):
        errors = await rt.apply_publishers(
            by_slug([_publisher_cfg("drive", message="${drive}")]),
            {"drive": {"linear": {"x": "${speed}"}, "angular": {"z": "${turn}"}}},
        )
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 2.5, "turn": 0.0})
            wait_until(lambda: len(received) > 0)
        finally:
            stop_sub()
        return errors, received[0]

    errors, msg = run(body)
    assert errors == []
    assert msg.linear.x == pytest.approx(2.5)


def test_an_unbuildable_failsafe_body_is_a_per_slug_error():
    """The most important structural check here: an unbuildable
    failsafe must be caught at config-apply time, not the first time the
    timer needs to fire it — mid-emergency is the worst moment to
    discover a typo."""

    async def body(rt):
        return await rt.apply_publishers(by_slug(
            [_publisher_cfg("drive", failsafe={"not_a_field": 1})]
        ))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "drive"


def test_removing_a_publisher_slug_destroys_its_handle():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive")]))
        await rt.apply_publishers(by_slug([]))
        assert "drive" not in rt._publishers

    run(body)


def test_retargeting_a_publishers_topic_replaces_the_handle():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("x", topic="/cmd_vel")]))
        before = rt._publishers["x"].handle
        await rt.apply_publishers(by_slug([_publisher_cfg("x", topic="/cmd_vel2")]))
        after = rt._publishers["x"].handle
        return before, after, rt._publishers["x"].topic

    before, after, topic = run(body)
    assert before is not after
    assert topic == "/cmd_vel2"


# --- publish: a real message lands on the topic -----------------------------------


def test_publish_sends_a_real_message_an_independent_subscriber_observes():
    async def body(rt):
        assert await rt.apply_publishers(by_slug([_publisher_cfg("drive")])) == []

        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.5, "turn": -0.3})
            wait_until(lambda: len(received) > 0)
        finally:
            stop_sub()

        return received[0]

    msg = run(body)
    assert msg.linear.x == pytest.approx(1.5)
    assert msg.angular.z == pytest.approx(-0.3)


def test_publish_marks_the_publisher_active_for_the_failsafe_timer():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive")]))
        assert rt._publishers["drive"].last_activity_at is None
        await rt.publish("drive", {"speed": 1.0})
        return rt._publishers["drive"].last_activity_at

    last_activity_at = run(body)
    assert last_activity_at is not None


def test_publish_to_an_unconfigured_slug_does_not_raise():
    async def body(rt):
        await rt.publish("no-such-publisher", {"speed": 1.0})  # must not raise

    run(body)  # must not raise


def test_publish_with_unbuildable_params_does_not_raise():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive")]))
        await rt.publish("drive", {"warp_factor": 9})  # must not raise

    run(body)  # must not raise


# --- the failsafe timer: the safety-critical path --------------


def test_a_publisher_with_no_publish_ever_stays_dormant():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=100)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            # Longer than timeout_ms, with no publish ever sent. The
            # failsafe is about publishes "staying away", which presumes
            # there were some — an unused publisher has nothing to fail safe
            # *from*.
            await asyncio.sleep(0.3)
        finally:
            stop_sub()
        return received

    assert run(body) == []


def test_the_bridge_itself_publishes_the_failsafe_after_silence():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=100)]))
        # An INDEPENDENT subscriber, not a read of the bridge's own
        # state — the thing that matters is what actually reaches the
        # topic.
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.5, "turn": -0.3})
            # The client stops publishing — by going quiet, which is all the
            # bridge can ever observe; it does not know or care whether that
            # is a polite pause or a dead connection.
            wait_until(lambda: len(received) >= 2, timeout=DELIVERY_TIMEOUT_S)
        finally:
            stop_sub()
        return received

    received = run(body)
    assert received[0].linear.x == pytest.approx(1.5)  # the real publish
    failsafe = received[1]
    assert failsafe.linear.x == pytest.approx(0.0)  # the configured failsafe
    assert failsafe.angular.z == pytest.approx(0.0)


def test_the_failsafe_fires_exactly_once_per_silence_not_repeatedly():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=50)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(received) >= 2, timeout=DELIVERY_TIMEOUT_S)
            # Several more timeout periods of continued silence — a second
            # failsafe would mean this is a heartbeat, not a one-time "I
            # noticed you stopped", which is not what was configured.
            await asyncio.sleep(0.3)
        finally:
            stop_sub()
        return received

    received = run(body)
    assert len(received) == 2  # the real publish, then exactly one failsafe


def test_the_failsafe_re_arms_on_the_next_publish():
    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=50)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(received) >= 2, timeout=DELIVERY_TIMEOUT_S)  # publish, first failsafe

            await rt.publish("drive", {"speed": 2.0})  # driving again
            wait_until(lambda: len(received) >= 4, timeout=DELIVERY_TIMEOUT_S)  # publish, second failsafe
        finally:
            stop_sub()
        return received

    received = run(body)
    assert len(received) == 4
    assert received[1].linear.x == pytest.approx(0.0)  # first failsafe
    assert received[2].linear.x == pytest.approx(2.0)  # driving again
    assert received[3].linear.x == pytest.approx(0.0)  # second failsafe


def test_the_failsafe_timer_needs_no_client_or_cloud_connection():
    """The whole point of the timer: it lives entirely on RosRuntime's own
    executor thread and never touches BridgeClient, a websocket, or the
    asyncio loop. Every test in this section already proves that by
    construction — none builds a BridgeClient — this one just says so
    explicitly."""

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=50)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(received) >= 2, timeout=DELIVERY_TIMEOUT_S)
        finally:
            stop_sub()
        return received

    assert len(run(body)) == 2


def test_removing_an_armed_publisher_fires_its_failsafe_before_teardown():
    """Destroying an armed publisher's ROS handle without
    firing first leaves the robot holding its last command forever — a
    config change is ordinary use, not an edge case, and there is no
    platform e-stop behind it. This is what `test_removing_a_
    publisher_disarms_its_failsafe` used to assert as *correct* before the
    fix; inverted here to pin the fixed behaviour instead of the hole."""

    async def body(rt):
        # A timeout the test cannot reach -- not even after the longest wait
        # below: the watchdog's own failsafe could stand in for a parting one
        # that never fired.
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=60_000)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist, own_context=True)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.0})
            # The bridge's side seeing a match does not mean this side has:
            # discovery across contexts is not symmetric in time.
            wait_until(lambda: len(received) == 1, timeout=DELIVERY_TIMEOUT_S)
            await rt.apply_publishers(by_slug([]))  # removed well before the timeout would elapse
            await asyncio.sleep(0.2)
        finally:
            stop_sub()
        return received

    received = run(body)
    assert len(received) == 2  # the real publish, then the parting failsafe
    assert received[0].linear.x == pytest.approx(1.0)
    assert received[1].linear.x == pytest.approx(0.0)  # the configured failsafe


def test_a_removed_armed_publisher_is_destroyed_once_its_failsafe_is_acknowledged():
    """The handle outlives the parting failsafe only until the witness has
    acknowledged it -- a publisher kept alive for good would hold a topic the
    configuration no longer names."""

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=60_000)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist, own_context=True)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(received) == 1, timeout=DELIVERY_TIMEOUT_S)
            await rt.apply_publishers(by_slug([]))
            # Well inside the bound: gone because it was acknowledged, not
            # because the bound ran out.
            wait_until(lambda: not rt._retiring_publishers, timeout=PARTING_FAILSAFE_ACK_TIMEOUT_S / 2)
            wait_until(lambda: rt._node.count_publishers("/cmd_vel") == 0)
            # Retirement means the RTPS reader *acknowledged* the sample, not
            # that this subscriber's callback has run: the witness has its own
            # context and its own executor. Asserting on `received` straight
            # after retirement reads one for the other, and loses the race
            # often enough to go red on a busy runner -- which is what it did.
            wait_until(lambda: len(received) == 2, timeout=DELIVERY_TIMEOUT_S)
        finally:
            stop_sub()
        return received

    received = run(body)
    assert [m.linear.x for m in received] == [pytest.approx(1.0), pytest.approx(0.0)]


def test_a_retiring_publisher_nobody_acknowledges_is_destroyed_at_its_bound():
    """A reader that went away without unmatching never acknowledges; the
    handle must still go, after `PARTING_FAILSAFE_ACK_TIMEOUT_S` and not
    before, and say so."""
    import logging

    class _RecordingHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = _RecordingHandler()
    ros_runtime_log = logging.getLogger("fleetless_bridge.ros_runtime")
    ros_runtime_log.addHandler(handler)

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=10_000)]))
        await rt.publish("drive", {"speed": 1.0})
        # Stands in for the reader that never answers: the RMW call is the
        # only thing asked, so it is the only thing replaced.
        rt._publishers["drive"].handle.wait_for_all_acked = lambda timeout: False
        removed_at = time.monotonic()
        await rt.apply_publishers(by_slug([]))
        await asyncio.sleep(PARTING_FAILSAFE_ACK_TIMEOUT_S / 2)
        still_retiring = len(rt._retiring_publishers)
        wait_until(lambda: not rt._retiring_publishers, timeout=PARTING_FAILSAFE_ACK_TIMEOUT_S + 1.0)
        return still_retiring, time.monotonic() - removed_at

    try:
        still_retiring, gone_after_s = run(body)
    finally:
        ros_runtime_log.removeHandler(handler)
    assert still_retiring == 1
    assert gone_after_s >= PARTING_FAILSAFE_ACK_TIMEOUT_S
    assert any("was not acknowledged" in r.getMessage() for r in handler.records)


def test_a_publisher_whose_watchdog_already_fired_still_retires():
    """Removal right after the watchdog's own failsafe: nothing new fires, but
    that failsafe can be as unacknowledged as a parting one, so the handle
    retires all the same instead of taking it down with it."""

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=50)]))
        await rt.publish("drive", {"speed": 1.0})
        wait_until(lambda: rt._publishers["drive"]._failsafe_sent)  # the watchdog fired
        # Holds the handle in retirement for the length of the check below.
        rt._publishers["drive"].handle.wait_for_all_acked = lambda timeout: False
        await rt.apply_publishers(by_slug([]))
        retiring = len(rt._retiring_publishers)
        wait_until(lambda: not rt._retiring_publishers, timeout=PARTING_FAILSAFE_ACK_TIMEOUT_S + 1.0)
        return retiring

    assert run(body) == 1


def test_a_driven_publisher_can_change_its_type_on_the_same_topic():
    """A retiring handle still holds its topic, and the middleware refuses a
    second type on it within one node. A type change of a driven publisher
    -- `Twist` to something else on `/cmd_vel` -- must still apply, with the
    old type's parting failsafe sent first."""

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=60_000)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist, own_context=True)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(received) == 1, timeout=DELIVERY_TIMEOUT_S)
            errors = await rt.apply_publishers(by_slug([_publisher_cfg(
                "drive",
                type_="std_msgs/msg/Float64",
                message={"data": "${speed}"},
                parameters={"speed": ParameterSpec(type="float64")},
                failsafe={"data": 0.0},
                timeout_ms=60_000,
            )]))
            wait_until(lambda: len(received) >= 2, timeout=DELIVERY_TIMEOUT_S)
            type_now = rt._publishers["drive"].type_name if "drive" in rt._publishers else None
        finally:
            stop_sub()
        return errors, received, type_now

    errors, received, type_now = run(body)
    assert errors == []
    assert type_now == "std_msgs/msg/Float64"
    assert received[1].linear.x == pytest.approx(0.0)  # the old type's parting failsafe


def test_removing_a_never_used_publisher_stays_silent():
    """The other half of the same rule: nothing was ever promised for a
    publisher nobody drove, so removing it must not manufacture a
    failsafe out of nowhere."""

    async def body(rt):
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=50)]))
        received, _, stop_sub = _start_independent_subscriber("/cmd_vel", Twist)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.apply_publishers(by_slug([]))  # removed without ever being published to
            await asyncio.sleep(0.2)
        finally:
            stop_sub()
        return received

    assert run(body) == []


def test_retargeting_an_armed_publisher_fires_the_failsafe_on_the_old_topic():
    """A retarget is a destroy-then-create under the hood — the old topic
    gets its parting failsafe (it is the one that was actually being
    driven); the new topic starts dormant, exactly like any fresh
    publisher, because nothing has been published there yet."""

    async def body(rt):
        # Unreachable timeout, as in the removal test above.
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", topic="/cmd_vel", timeout_ms=60_000)]))
        old_received, _, stop_old = _start_independent_subscriber("/cmd_vel", Twist, own_context=True)
        new_received, _, stop_new = _start_independent_subscriber("/cmd_vel2", Twist, own_context=True)
        try:
            wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
            await rt.publish("drive", {"speed": 1.0})
            wait_until(lambda: len(old_received) == 1, timeout=DELIVERY_TIMEOUT_S)  # this side matched too, not only the bridge's
            # A short timeout on the new topic, so a retarget that carried the
            # armed state across would fire the watchdog there within the
            # sleep -- and the bridge's own entry says it directly.
            await rt.apply_publishers(by_slug(
                [_publisher_cfg("drive", topic="/cmd_vel2", timeout_ms=50)]
            ))
            assert rt._publishers["drive"].last_activity_at is None  # the new topic starts dormant
            await asyncio.sleep(0.2)
        finally:
            stop_old()
            stop_new()
        return old_received, new_received

    old_received, new_received = run(body)
    assert len(old_received) == 2  # the real publish, then the old topic's parting failsafe
    assert old_received[1].linear.x == pytest.approx(0.0)
    assert new_received == []  # new topic never armed, never published to — silent


# --- cameras: apply_cameras, the diff ------------------------------


def _camera_cfg(
    slug,
    topic="/image_raw",
    type_=IMAGE_TYPE,
    width=64,
    height=48,
    fps=10,
    bitrate_kbps=500,
    snapshot_interval_seconds=1,
):
    return CameraConfig(
        slug=slug,
        source=RosSource(topic=topic, type=type_),
        width=width,
        height=height,
        fps=fps,
        bitrate_kbps=bitrate_kbps,
        snapshot_interval_seconds=snapshot_interval_seconds,
    )


class _InertCameraAdapter:
    """A stand-in for `camera_sources.MjpegSourceAdapter` (or any other
    `CameraSourceAdapter`) that never calls `on_frame`/`on_error` on its
    own — matches the real constructor's keyword shape (`url`,
    `credentials`, `on_frame`, `on_error`; V4L2's `device` too, unused
    here) so it can swap in via `mock.patch` wherever `_build_source_
    adapter` builds one, for a test whose claim is about `RosRuntime`'s own
    bookkeeping, not a real adapter's connect behaviour. Built to remove
    the one place in this file that raced a real, deliberately-unreachable
    network connection against the test's own manually-injected events
    (`test_a_recovery_survives_a_credential_fix_that_rebuilds_the_entry`,
    which reproduced 5 isolated failures in 150 runs before this
    existed)."""

    def __init__(self, *, url=None, device=None, credentials=None, on_frame=None, on_error=None):
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


def _unused_tcp_port() -> int:
    """A real, currently-free localhost port, so a real `socket.connect()`
    against it fails with a genuine `ConnectionRefusedError` — no server,
    no mock, just an ordinary closed port (bind then release, the standard
    idiom; a fixed high port number would flake against whatever else is
    listening in this container)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_image_publisher(
    topic, *, width=64, height=48, hz=30, color=(10, 20, 30), noisy=False
):
    """A real publisher on its own spinning node — synthetic pixels (a solid
    color, not the physical webcam, which keeps the pytest suite off real
    hardware), but a genuine `sensor_msgs/msg/Image`
    over a genuine subscription, which is what the bridge's camera pipeline
    actually has to convert. `color` changes between calls when a test wants
    to tell two frames apart. `noisy` publishes random pixels instead — a
    solid frame compresses to almost nothing regardless of quality, which
    proves nothing about the size-bounded encoding. Returns `stop()`."""
    node = rclpy.create_node("test_image_publisher_{}".format(id(object())))
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    pub = node.create_publisher(Image, topic, 10)
    bridge = CvBridge()
    if noisy:
        frame = np.random.default_rng(0).integers(0, 256, size=(height, width, 3), dtype=np.uint8)
    else:
        frame = np.zeros((height, width, 3), dtype=np.uint8)
        frame[:, :] = color

    def _tick():
        pub.publish(bridge.cv2_to_imgmsg(frame, encoding="bgr8"))

    timer = node.create_timer(1.0 / hz, _tick)

    def stop():
        timer.cancel()
        executor.shutdown()
        node.destroy_node()
        thread.join(timeout=5.0)

    return stop


def test_applying_a_camera_creates_a_subscription_and_reports_no_errors():
    async def body(rt):
        errors = await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        assert errors == []
        assert "front" in rt._cameras

    run(body)


def test_an_unresolvable_camera_type_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_cameras(by_slug(
            [_camera_cfg("bad", type_="nonexistent_pkg/msg/Ghost"), _camera_cfg("good")]
        ))

    errors = run(body)
    assert {e.slug for e in errors} == {"bad"}


def test_a_camera_type_that_is_not_image_or_compressed_image_is_a_per_slug_error():
    async def body(rt):
        return await rt.apply_cameras(by_slug(
            [_camera_cfg("bad", type_="sensor_msgs/msg/BatteryState")]
        ))

    errors = run(body)
    assert len(errors) == 1
    assert errors[0][0] == "bad"


def test_removing_a_camera_slug_destroys_its_subscription():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.apply_cameras(by_slug([]))
        assert "front" not in rt._cameras

    run(body)


def test_retargeting_a_camera_slugs_topic_replaces_the_subscription():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("x", topic="/image_raw")]))
        first_adapter = rt._cameras["x"].adapter
        await rt.apply_cameras(by_slug([_camera_cfg("x", topic="/image_raw_2")]))
        second_adapter = rt._cameras["x"].adapter
        return first_adapter, second_adapter, rt._cameras["x"].source.topic

    first, second, topic = run(body)
    assert first is not second
    assert topic == "/image_raw_2"


def test_an_unchanged_camera_keeps_the_same_subscription_object():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("x")]))
        before = rt._cameras["x"].adapter
        await rt.apply_cameras(by_slug([_camera_cfg("x")]))  # same slug, same shape
        after = rt._cameras["x"].adapter
        return before, after

    before, after = run(body)
    assert before is after


def test_reapplying_a_camera_updates_its_resolution_without_resubscribing():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("x", width=64, height=48)]))
        adapter_before = rt._cameras["x"].adapter
        await rt.apply_cameras(by_slug([_camera_cfg("x", width=320, height=240)]))
        entry = rt._cameras["x"]
        return adapter_before, entry

    adapter_before, entry = run(body)
    assert entry.adapter is adapter_before  # same source — updated in place
    assert (entry.width, entry.height) == (320, 240)


def test_a_stale_adapters_late_frame_does_not_refresh_a_newer_entry():
    """A real defect found against the demo robot, not reasoned about.
    `_close()` is a deliberate no-op for RTSP/V4L2 now (see
    camera_sources.py's `_CvCaptureAdapter` docstring), so an old adapter's
    thread alone is responsible for noticing `_stop_event` and exiting — on
    an established, healthy stream running for minutes, that took long
    enough that a credential rotation never actually stopped it: the old
    adapter kept delivering real frames on the *old* (meant-to-be-retired)
    credential indefinitely, because a lookup by slug alone could not tell
    its late frame apart from the current adapter's. Pinned directly at
    `_on_source_frame`: a frame tagged with a stale entry must land nowhere
    near the slug's current one."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("x", topic="/image_raw")]))
        stale_entry = rt._cameras["x"]

        # Retarget "x" -> a genuinely different entry, the same shape any
        # source or credentials change produces in _apply_cameras.
        await rt.apply_cameras(by_slug([_camera_cfg("x", topic="/image_raw_2")]))
        current_entry = rt._cameras["x"]
        assert current_entry is not stale_entry  # sanity: the retarget really happened

        # The stale adapter's own thread calling back in late, exactly as
        # its on_frame closure would (same call shape _create_camera wires
        # up) — simulated directly rather than waiting out a real orphaned
        # thread, which is the whole point of a unit test.
        fake_frame = np.zeros((4, 6, 3), dtype=np.uint8)
        rt._on_source_frame("x", stale_entry, fake_frame, 123456789)

        return current_entry, stale_entry

    current_entry, stale_entry = run(body)
    # A stale call is dropped outright, not redirected into the old
    # entry's own storage — neither one is touched by it.
    assert current_entry.latest.get() is None
    assert stale_entry.latest.get() is None


def test_a_stale_adapters_late_error_does_not_overwrite_a_newer_entry():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("x", topic="/image_raw")]))
        stale_entry = rt._cameras["x"]
        await rt.apply_cameras(by_slug([_camera_cfg("x", topic="/image_raw_2")]))
        current_entry = rt._cameras["x"]

        rt._on_source_error("x", stale_entry, "source_auth_failed", "stale")

        return current_entry, stale_entry

    current_entry, stale_entry = run(body)
    # Dropped outright, not redirected into the old entry's own storage.
    assert current_entry.last_source_error is None
    assert stale_entry.last_source_error is None


def test_a_background_source_error_is_reported_with_nobody_watching():
    """A source that fails at config-apply time, with no viewer
    and no `start_live` ever called, used to be invisible — `_on_source_error`
    only updated `entry.last_source_error` locally. This is the fix: an
    unsolicited `camera_state` with `cause: 'source'` reaches the wire even
    though no live session exists."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        entry = rt._cameras["front"]
        rt._on_source_error("front", entry, "source_auth_failed", "camera rejected the password")
        return await _drain_camera_states(rt)

    states = run(body)
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error == ("source_auth_failed", "camera rejected the password")
    assert states[0].cause == "source"


def test_a_real_config_apply_reports_a_source_failure_with_nobody_watching_end_to_end():
    """The bridge half, re-verified end to end: the test
    above (and everything else covering this row) calls `_on_source_error`
    directly, proving the reporting mechanism but not the actual path a
    real config apply takes to reach it. This one goes through
    `apply_cameras` -> `_apply_cameras` -> `_create_camera` ->
    `_build_source_adapter` -> a real `RtspSourceAdapter`, whose adapter
    thread does a real TCP connect against a `127.0.0.1` port nothing is
    listening on — a genuine, immediate `ConnectionRefusedError`, not an
    injected fake. No `start_live` is ever called, and no viewer is ever
    involved: this is exactly the reported scenario, an RTSP camera whose
    config-apply-time failure was invisible until someone pressed "Go
    live", sometimes days later.

    The bridge side has already been closed — `_on_source_error` reports
    unsolicited regardless of any live session,
    and `report_current_camera_health` restates it on every reconnect.
    What was still open is only the cloud side — the
    frame shape below (`cause='source'`, `error=(code, message)`,
    `request_id=None`) is exactly what that consumer will read."""
    unused_port = _unused_tcp_port()

    async def body(rt):
        cfg = CameraConfig(
            slug="front",
            source=RtspSource(
                url="rtsp://127.0.0.1:{}/stream".format(unused_port), transport="tcp", credentials=None
            ),
            width=320, height=240, fps=5, bitrate_kbps=500, snapshot_interval_seconds=1,
        )
        errors = await rt.apply_cameras(by_slug([cfg]))  # config apply itself succeeds — the connect happens in the background
        assert errors == []
        assert "front" in rt._cameras
        state = await _await_camera_state(rt)
        # Retire this camera's real, still-reconnecting adapter before
        # this test's own teardown — see _stop_camera_and_join's docstring
        # for the exact race this closes (reproduced once as a caught-but-
        # noisy InvalidHandle from a late cross-thread report, traced down
        # to this same gap in several other tests too).
        await _stop_camera_and_join(rt, "front")
        return state

    state = run(body)
    assert state.slug == "front"
    assert state.publishing is False
    assert state.cause == "source"
    assert state.request_id is None  # unsolicited — answers no camera_start/camera_stop
    assert state.error is not None
    code, message = state.error
    assert code == "source_unreachable"


def test_a_recovered_source_is_reported_unsolicited():
    """The mirror image of the test above: a background failure that gets
    fixed must not go on reading as broken forever — the same mistake in
    reverse. `_on_source_frame` already clears `last_source_error` on a
    good frame; this checks it also says so on the wire, once, at the
    transition."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=8, height=6)]))
        entry = rt._cameras["front"]
        rt._on_source_error("front", entry, "source_unreachable", "no route to host")
        await _drain_camera_states(rt)  # the failure report; not what this test is about

        fake_frame = np.zeros((6, 8, 3), dtype=np.uint8)
        rt._on_source_frame("front", entry, fake_frame, 123456789)
        return await _drain_camera_states(rt), entry.last_source_error

    states, last_source_error = run(body)
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error is None
    assert states[0].cause == "source"
    assert last_source_error is None  # cleared, same as before


def test_a_recovered_source_via_the_raw_path_is_reported_unsolicited():
    """The decode gate's raw-storage path — idle, no live publisher and
    nothing snapshot-due — shares the exact same recovery
    report as `_on_source_frame`, through `_clear_source_error`, rather
    than a second copy of the transition logic. A camera resuming delivery
    while idle must not stay reported broken forever just because its
    frames are staying raw instead of being converted — the bug this
    shared method exists to close (`_RosSourceAdapter`'s callback calls
    `_clear_source_error` for every raw frame it stores; this test drives
    that same method directly, the same way the tests above drive
    `_on_source_frame`/`_on_source_error` directly rather than through a
    real subscription)."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=8, height=6)]))
        entry = rt._cameras["front"]
        rt._on_source_error("front", entry, "source_unreachable", "no route to host")
        await _drain_camera_states(rt)  # the failure report; not what this test is about

        rt._clear_source_error("front", entry, 123456789)
        return await _drain_camera_states(rt), entry.last_source_error

    states, last_source_error = run(body)
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error is None
    assert states[0].cause == "source"
    assert last_source_error is None  # cleared by the raw path too


def test_a_good_frame_with_no_prior_error_reports_nothing_unsolicited():
    """The first frame after a config apply is not a "recovery" — there was
    nothing to recover from, and a report on every ordinary frame would be
    exactly the flood `_report_once` in camera_sources.py already exists to
    prevent one layer down."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=8, height=6)]))
        entry = rt._cameras["front"]
        fake_frame = np.zeros((6, 8, 3), dtype=np.uint8)
        rt._on_source_frame("front", entry, fake_frame, 123456789)
        return await _drain_camera_states(rt)

    assert run(body) == []


def test_a_recovery_survives_a_credential_fix_that_rebuilds_the_entry():
    """A bug found end to end: fixing a wrong password re-sends config, and for
    RTSP/MJPEG a changed *resolved*
    credential is `source_or_credentials_changed` in `_apply_cameras` —
    the entry is destroyed and a fresh one built, with no memory of the
    old error. A recovery report keyed off the entry's own field has
    nothing to transition from on the new entry, so it never fires — a
    fixed camera reads broken forever. Reproduces the exact shape: an
    error on the original entry, an `apply_cameras` call that changes
    only the resolved credential (same slug, same URL), then a good frame
    on whatever entry is current afterwards.

    This used to be the one camera test in the whole suite that wired a
    real, network-backed `MjpegSourceAdapter` against `192.0.2.1`
    (deliberately unreachable) instead of a fake, and it raced its own
    assertions because of it — 5/150 isolated runs, always the same extra
    `source_unreachable` event in the observation window, reproduced and
    diagnosed rather than assumed (a full-suite run found it twice first).
    What this test is actually about is `_apply_cameras`'s own rebuild
    bookkeeping across a credential fix, never the real adapter's connect
    behaviour — camera_sources.py's own suite already covers that on its
    fakes — so the real network attempt was never part of the claim, only
    a side effect of how the fixture was built. `MjpegSourceAdapter` is
    patched out for exactly the retarget call, same shape
    `test_destroying_a_camera_with_a_slow_stopping_adapter_does_not_block_
    the_executor` already uses for a different camera test a few hundred
    lines up."""

    def mjpeg_cfg(password):
        return CameraConfig(
            slug="cam",
            source=MjpegSource(
                url="http://192.0.2.1/video", credentials=("user", password)
            ),
            width=320, height=240, fps=5, bitrate_kbps=500, snapshot_interval_seconds=1,
        )

    async def body(rt):
        await rt.apply_cameras(by_slug([mjpeg_cfg("wrongpass")]))
        broken_entry = rt._cameras["cam"]
        rt._on_source_error("cam", broken_entry, "source_auth_failed", "camera rejected the password")
        await _drain_camera_states(rt)  # the failure report; not what this test is about

        # Retargets the real (still-reconnecting) adapter this credential
        # change replaces, and joins the resulting stop thread -- see
        # _retarget_cameras_and_join's own docstring for why this cannot
        # be a separate _stop_camera_and_join call ahead of a plain
        # apply_cameras (it would pop "cam" before _apply_cameras's own
        # same-kind-retarget bookkeeping ever saw it there). The rebuilt
        # entry's adapter is the inert fake below, not a real connection —
        # only _apply_cameras's own bookkeeping is under test here.
        with mock.patch("fleetless_bridge.camera_sources.MjpegSourceAdapter", _InertCameraAdapter):
            await _retarget_cameras_and_join(rt, by_slug([mjpeg_cfg("rightpass")]))
        fixed_entry = rt._cameras["cam"]
        assert fixed_entry is not broken_entry  # sanity: the entry really was rebuilt
        await _drain_camera_states(rt)  # config-change side effects, if any; not this test either

        fake_frame = np.zeros((240, 320, 3), dtype=np.uint8)
        rt._on_source_frame("cam", fixed_entry, fake_frame, 123456789)
        result = await _drain_camera_states(rt)
        await _stop_camera_and_join(rt, "cam")  # and this one, before rclpy tears down
        return result

    states = run(body)
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error is None
    assert states[0].cause == "source"


def test_a_source_kind_change_does_not_restate_the_old_kinds_error():
    """Keeping `_camera_last_reported_error` across a retarget is correct
    for a same-kind credential fix (see the test above) but wrong across a
    *kind* change — `auth_failed`
    restated for a ROS topic asserts a cause that cannot exist there, ROS
    has no credentials at all. Worse: if the cloud clears its own entry on
    a source-change publish, as it does, a stale kind-mismatched
    restatement from the bridge would immediately re-poison what the
    cloud just correctly cleared. Reproduces the exact shape: an MJPEG
    camera with a real auth failure, retargeted on the same slug to a ROS
    topic — the old error must not be reachable through the new entry at
    all, not even via a restatement."""

    async def body(rt):
        mjpeg_cfg = CameraConfig(
            slug="cam",
            source=MjpegSource(
                url="http://192.0.2.1/video", credentials=("user", "wrongpass")
            ),
            width=320, height=240, fps=5, bitrate_kbps=500, snapshot_interval_seconds=1,
        )
        await rt.apply_cameras(by_slug([mjpeg_cfg]))
        broken_entry = rt._cameras["cam"]
        rt._on_source_error("cam", broken_entry, "source_auth_failed", "camera rejected the password")
        await _drain_camera_states(rt)  # the failure report; not what this test is about

        # Retargeted to a completely different source kind, same slug —
        # not a credential fix, a structurally different camera. A ROS
        # source has no background thread of its own (a real rclpy
        # subscription, created synchronously), so nothing further needs
        # joining once this one lands -- only the OLD (real, still-
        # connecting) adapter this retarget destroys does, and its
        # _camera_last_reported_error cleanup (kind changed) must still
        # run inside _apply_cameras itself -- see
        # _retarget_cameras_and_join's own docstring.
        await _retarget_cameras_and_join(rt, by_slug([_camera_cfg("cam", topic="/image_raw")]))
        ros_entry = rt._cameras["cam"]
        assert ros_entry is not broken_entry  # sanity: the entry really was rebuilt
        await _drain_camera_states(rt)  # config-change side effects, if any; not this test either

        # Nothing has happened to the new ROS camera yet — no frame, no
        # error. A restatement now must be silent (skip), not a restated
        # `auth_failed` inherited from the old MJPEG source.
        await rt.report_current_camera_health()
        return await _drain_camera_states(rt), ros_entry.last_source_error

    states, last_source_error = run(body)
    assert states == []
    assert last_source_error is None


def test_a_reused_slug_does_not_inherit_a_stale_recovery():
    """The other half of the fix above: `_camera_last_reported_error` must
    survive a *retarget* but not survive the slug leaving the config
    entirely — otherwise an unrelated later camera that happens to reuse
    the same slug would open its very first frame as a spurious
    'recovered' report for a problem it never had."""

    async def body(rt):
        cfg = CameraConfig(
            slug="cam",
            source=MjpegSource(
                url="http://192.0.2.1/video", credentials=("user", "wrongpass")
            ),
            width=320, height=240, fps=5, bitrate_kbps=500, snapshot_interval_seconds=1,
        )
        await rt.apply_cameras(by_slug([cfg]))
        entry = rt._cameras["cam"]
        rt._on_source_error("cam", entry, "source_auth_failed", "camera rejected the password")
        await _drain_camera_states(rt)  # the failure report; not what this test is about

        # Removes "cam" entirely and joins the resulting stop thread for
        # its real adapter -- see _retarget_cameras_and_join's own
        # docstring for why this must wrap apply_cameras([]) rather than
        # pre-empt it (its own removal-loop is what clears
        # _camera_last_reported_error["cam"], which is the exact
        # behaviour this test is checking survives to the reused slug
        # below correctly *not* carrying it forward).
        await _retarget_cameras_and_join(rt, {})  # "cam" removed entirely, not retargeted
        assert "cam" not in rt._cameras
        await _drain_camera_states(rt)

        # An unrelated new camera reusing the same slug.
        await rt.apply_cameras(by_slug([_camera_cfg("cam")]))
        new_entry = rt._cameras["cam"]
        await _drain_camera_states(rt)  # its own first-config-apply side effects, if any

        fake_frame = np.zeros((48, 64, 3), dtype=np.uint8)
        rt._on_source_frame("cam", new_entry, fake_frame, 123456789)
        return await _drain_camera_states(rt)

    assert run(body) == []


# --- report_current_camera_health: the cloud-restart gap -------------


def test_report_current_camera_health_reports_a_known_error():
    """A ROS camera, and the error is injected: what this test is about is
    `report_current_camera_health` restating the last *known* state, never
    how a source discovers one.

    It used to wire a real, network-backed `MjpegSourceAdapter` against
    `192.0.2.1`, whose own background thread reports `source_unreachable`
    on its own schedule and overwrites the injected `source_auth_failed`
    before the walk reads it — the same defect already recorded on
    `test_a_credential_fix_reports_recovery_after_the_entry_is_rebuilt`.
    In 2 of 9 runs the pair that arrived was
    `('source_unreachable', 'could not reach the camera: OSError')` rather
    than the injected one. It reddened rarely on an ordinary network
    because the connect had to time out first, and often with no route at
    all, where the `OSError` returns immediately — so the real adapter was
    never part of the claim, only a second producer racing the one under
    test. A ROS source does nothing at all between frames, so the only
    report in play is the one this test injects.
    """

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("cam")]))
        entry = rt._cameras["cam"]
        rt._on_source_error("cam", entry, "source_auth_failed", "camera rejected the password")
        await _drain_camera_states(rt)  # the transition report itself; not this test

        await rt.report_current_camera_health()
        return await _drain_camera_states(rt)

    states = run(body)
    assert len(states) == 1
    assert states[0].slug == "cam"
    assert states[0].publishing is False
    assert states[0].error == ("source_auth_failed", "camera rejected the password")
    assert states[0].cause == "source"


def test_report_current_camera_health_reports_a_confirmed_ok():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=8, height=6)]))
        entry = rt._cameras["front"]
        fake_frame = np.zeros((6, 8, 3), dtype=np.uint8)
        rt._on_source_frame("front", entry, fake_frame, 123456789)
        await _drain_camera_states(rt)  # the ordinary first-frame side effects, if any

        await rt.report_current_camera_health()
        return await _drain_camera_states(rt)

    states = run(body)
    assert len(states) == 1
    assert states[0].slug == "front"
    assert states[0].publishing is False
    assert states[0].error is None
    assert states[0].cause == "source"


def test_report_current_camera_health_skips_a_camera_nothing_is_known_about_yet():
    """The third branch, deliberately not `unknown`: a camera that has
    neither delivered a frame nor reported an error has nothing true to
    say yet. Reporting `unknown` here would make it mean two different
    things (see the method's own docstring)."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        # No frame, no error — the adapter has not resolved anything.
        await rt.report_current_camera_health()
        return await _drain_camera_states(rt)

    assert run(body) == []


def test_report_current_camera_health_covers_every_configured_camera():
    """Both cameras are ROS sources, for the reason given on
    `test_report_current_camera_health_reports_a_known_error`: the walk is
    what is under test, and a real network-backed adapter is a second
    producer of exactly the state being asserted."""

    async def body(rt):
        healthy_cfg = _camera_cfg("healthy", topic="/image_raw")
        broken_cfg = _camera_cfg("broken", topic="/broken_image_raw")
        await rt.apply_cameras(by_slug([healthy_cfg, broken_cfg]))

        healthy_entry = rt._cameras["healthy"]
        fake_frame = np.zeros((48, 64, 3), dtype=np.uint8)
        rt._on_source_frame("healthy", healthy_entry, fake_frame, 123456789)

        broken_entry = rt._cameras["broken"]
        rt._on_source_error("broken", broken_entry, "source_unreachable", "no route to host")
        await _drain_camera_states(rt)  # both transition reports; not this test

        await rt.report_current_camera_health()
        return {s.slug: s for s in await _drain_camera_states(rt)}

    states = run(body)
    assert set(states) == {"healthy", "broken"}
    assert states["healthy"].error is None
    assert states["broken"].error == ("source_unreachable", "no route to host")


def test_report_current_camera_health_dates_a_restated_error_to_when_it_first_happened():
    """The cloud used to stamp `changed_at_ms` with
    its own receive time, so a restatement — an old state re-sent into a
    store that just lost it — dated a failure from long ago to the moment
    of the restatement. `observed_at_ms` is the bridge's half of the fix:
    a restatement must carry the time the state first became true, not the
    time this particular walk happens to run."""

    async def body(rt):
        # A ROS camera on purpose, not MJPEG/RTSP: those spin up a real
        # background adapter thread that keeps retrying on its own
        # backoff, which can race this test's own injected error/sleep
        # with a second, real report for an unrelated code — the ROS
        # adapter does nothing on its own between frames, so the only
        # report in play is the one this test injects.
        await rt.apply_cameras(by_slug([_camera_cfg("cam")]))
        entry = rt._cameras["cam"]
        rt._on_source_error("cam", entry, "source_auth_failed", "camera rejected the password")
        first_report = (await _drain_camera_states(rt))[0]
        original_observed_at_ms = first_report.observed_at_ms

        await asyncio.sleep(0.2)  # real time passes before the reconnect-restatement
        await rt.report_current_camera_health()
        restated = await _drain_camera_states(rt)

        assert len(restated) == 1
        return original_observed_at_ms, restated[0].observed_at_ms

    original_observed_at_ms, restated_observed_at_ms = run(body)
    assert restated_observed_at_ms == original_observed_at_ms


def test_report_current_camera_health_dates_a_restated_ok_to_the_latest_frame():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=8, height=6)]))
        entry = rt._cameras["front"]
        fake_frame = np.zeros((6, 8, 3), dtype=np.uint8)
        rt._on_source_frame("front", entry, fake_frame, 1786400000000)

        await rt.report_current_camera_health()
        return await _drain_camera_states(rt)

    states = run(body)
    assert len(states) == 1
    assert states[0].observed_at_ms == 1786400000000


def test_destroying_a_camera_with_a_slow_stopping_adapter_does_not_block_the_executor():
    """A credential rotation to a wrong password
    destroys and rebuilds the affected adapter(s) (_apply_cameras), and if
    the old adapter's stop() blocks — camera_sources.py's own documented
    limitation, a blocking connect call with nothing to cancel it — that
    must never stall the shared executor thread, which also serves every
    other camera, every datapoint, every action/service/publisher, and the
    safety-critical failsafe timer. Before the fix, `stop()`
    ran synchronously inside `_destroy_camera` on the executor thread, so a
    single slow adapter stalled all of that; this pins it by injecting a
    fake adapter whose `stop()` blocks for 30s (implausible for any test
    to wait out) and asserting both that removing it returns promptly and
    that the executor is still responsive to unrelated work immediately
    after."""
    from fleetless_bridge import camera, sampling
    from fleetless_bridge.camera_sources import CameraSourceAdapter
    from fleetless_bridge.ros_runtime import _CameraEntry

    class SlowStopAdapter(CameraSourceAdapter):
        def start(self) -> None:
            pass

        def stop(self) -> None:
            time.sleep(30)

    async def body(rt):
        # Injected directly — no real connect needed, this test is only
        # about _destroy_camera's own blocking behaviour on removal, not
        # about any adapter's connect logic.
        rt._cameras["slow"] = _CameraEntry(
            source=RosSource(topic="/whatever", type=IMAGE_TYPE),
            credentials=None,
            width=64, height=48, fps=10, bitrate_kbps=500, snapshot_interval_seconds=1,
            rate=sampling.MaxHzPolicy(10),
            latest=camera.LatestFrameHolder(),
            raw=camera.RawFrameHolder(),
            adapter=SlowStopAdapter(),
        )

        destroy_start = time.monotonic()
        await rt.apply_cameras(by_slug([]))  # removes "slow" -> _destroy_camera -> adapter.stop()
        destroy_elapsed = time.monotonic() - destroy_start

        # Not just that this one call returned — the executor must still be
        # promptly serving unrelated work right afterwards.
        liveness_start = time.monotonic()
        await rt.apply_config(by_slug([_dp("x")]))
        liveness_elapsed = time.monotonic() - liveness_start

        return destroy_elapsed, liveness_elapsed

    destroy_elapsed, liveness_elapsed = run(body)
    assert destroy_elapsed < 2.0
    assert liveness_elapsed < 2.0


def test_a_configured_camera_converts_and_stores_incoming_frames():
    """Conversion is lazy now: `apply_cameras` alone no longer converts
    anything while idle (see `test_ros_runtime_decode_gate.py` for that claim
    itself), so this
    drives a due snapshot pull — the mechanism that makes the ROS
    adapter's raw-stored message actually get decoded and resized — and
    checks the result landed in `entry.latest`, the same shared hand-off
    the live path also feeds."""

    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=64, height=48, hz=30)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=32, height=24, snapshot_interval_seconds=1)]
            ))
            await _pull_snapshot(rt)
            frame = rt._cameras["front"].latest.get()
            return frame.bgr.shape, frame.timestamp_ms
        finally:
            stop_pub()

    shape, timestamp_ms = run(body)
    assert shape == (24, 32, 3)  # resized to the configured width/height
    assert timestamp_ms > 0


def test_a_camera_throttles_incoming_frames_to_its_configured_fps():
    """Publishing far faster than the configured fps must not update the
    stored frame faster than `1/fps` — reuses `sampling.MaxHzPolicy`
    (already proven in test_sampling.py), wired in here rather than
    reimplemented. Conversion is lazy now: a snapshot
    pull is what drives it, and only the camera's own fps — applied inside
    the shared `_on_source_frame` hand-off both the lazy and eager paths go
    through — may be what rejects the second frame.

    The 3.0 format made the snapshot interval whole seconds (minimum 1), so it can
    no longer be dialled below the 0.3s window this test watches, and left
    alone it, not the fps throttle, would be what returns `None` on the
    second pull. The pacing is therefore cleared explicitly between the two
    pulls: the snapshot interval has its own tests a few hundred lines
    down, and this one must fail if and only if the fps throttle stops
    working."""

    async def body(rt):
        # 50 Hz source, 2 Hz configured — a 0.5s interval floor comfortably
        # wider than the 0.3s window this test actually watches.
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=50)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, fps=2, snapshot_interval_seconds=1)]
            ))
            await _pull_snapshot(rt)
            first_timestamp = rt._cameras["front"].latest.get().timestamp_ms
            await asyncio.sleep(0.3)
            # Snapshot pacing cleared on purpose — see the docstring. Without
            # this the pull below is refused by the interval and never
            # reaches the fps throttle this test is about.
            rt._cameras["front"].last_snapshot_at = None
            await rt.next_snapshot(_UNLIMITED_BUDGET)  # due again; fps must still reject it
            second_timestamp = rt._cameras["front"].latest.get().timestamp_ms
            return first_timestamp, second_timestamp
        finally:
            stop_pub()

    first_timestamp, second_timestamp = run(body)
    assert first_timestamp == second_timestamp  # throttled — no update within the interval


# --- snapshots: the pulled binary frame -------------------------
#
# Pulled, not queued, since the prioritized writer: snapshots are pulled
# and fitted rather than queued and dropped, so `SnapshotQueue` and the
# watchdog's encode-and-push are gone, and `next_snapshot(max_bytes)` runs
# the same dueness walk on demand, with the byte budget the caller can
# actually afford. Every claim below is the one the watchdog version made;
# only who asks has changed.


# Larger than anything `snapshot_max_bytes` is set to in this file, so a
# test that is not about the *link's* budget lets the camera's own
# configured ceiling be the one that decides (`next_snapshot` takes the
# smaller of the two).
_UNLIMITED_BUDGET = 10_000_000


def _split_snapshot_frame(wire: bytes):
    (header_len,) = struct.unpack(">I", wire[:4])
    header_bytes = wire[4 : 4 + header_len]
    image_bytes = wire[4 + header_len :]
    return json.loads(header_bytes.decode("utf-8")), image_bytes


async def _pull_snapshot(rt, max_bytes=_UNLIMITED_BUDGET, timeout=5.0):
    """Asks until something is due. A pull is not a wait — `next_snapshot`
    answers `None` immediately when nothing is due or nothing has been
    captured yet — so a test that expects a frame has to keep asking, the
    same way the writer does on its idle tick."""
    deadline = time.monotonic() + timeout
    while True:
        wire = await rt.next_snapshot(max_bytes)
        if wire is not None:
            return wire
        if time.monotonic() > deadline:
            raise AssertionError("no snapshot became available within {}s".format(timeout))
        await asyncio.sleep(0.02)


def test_no_snapshot_is_sent_before_any_frame_has_arrived():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", snapshot_interval_seconds=1)]))
        await asyncio.sleep(0.1)  # long past due, still no source frame
        return await rt.next_snapshot(_UNLIMITED_BUDGET)

    assert run(body) is None


def test_a_snapshot_is_sent_once_a_frame_is_available():
    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=64, height=48, hz=30)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=32, height=24, snapshot_interval_seconds=1)]
            ))
            return await _pull_snapshot(rt)
        finally:
            stop_pub()

    header, image_bytes = _split_snapshot_frame(run(body))
    assert header["type"] == "snapshot"
    assert header["slug"] == "front"
    assert header["mime"] == "image/jpeg"
    assert header["width"] == 32
    assert header["height"] == 24
    assert header["timestamp_ms"] > 0
    decoded = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
    assert decoded.shape == (24, 32, 3)


def test_snapshots_are_rate_limited_to_the_configured_interval():
    """A source publishing far faster than the interval must not produce a
    snapshot per pull — only one per `snapshot_interval_seconds`, the same
    "cheap by design" property that makes the cloud's one-frame cache work
    (a sub-second interval would defeat it, which is why the format's
    minimum is a whole second). The writer asks four times a
    second regardless of what any camera's interval is, so this is now the
    *only* thing enforcing that cadence."""

    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=50)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, snapshot_interval_seconds=1)]
            ))
            await _pull_snapshot(rt)  # the first one
            await asyncio.sleep(0.5)  # well under the 1s interval
            return await rt.next_snapshot(_UNLIMITED_BUDGET)
        finally:
            stop_pub()

    assert run(body) is None  # not due yet, however often it is asked


def test_a_second_snapshot_is_sent_once_the_interval_elapses():
    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=50)
        try:
            # fps well above 1/snapshot_interval_seconds: a fresh accepted frame
            # must be available between the two snapshot checks, or a
            # camera-throttle cadence that happened to align with the
            # snapshot cadence could serve the same stale frame twice.
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, fps=30, snapshot_interval_seconds=1)]
            ))
            first = await _pull_snapshot(rt)
            second = await _pull_snapshot(rt)
            first_header, _ = _split_snapshot_frame(first)
            second_header, _ = _split_snapshot_frame(second)
            return first_header["timestamp_ms"], second_header["timestamp_ms"]
        finally:
            stop_pub()

    first_ts, second_ts = run(body)
    assert second_ts > first_ts


def test_nothing_is_encoded_until_a_snapshot_is_asked_for():
    """Restated for the pulled design, and the successor to the two
    tests that used to prove "nothing is queued while disconnected".

    There is no longer a queue to fill, and `next_snapshot` deliberately
    does *not* re-check `_connected`: the only caller is the session's
    writer, which exists only while a session does, and a second copy of
    that decision here could later disagree with the first. What has to
    stay true is the honesty behind that rule — a snapshot gap must never become
    a delayed burst of stale images presented as current — and
    that is now the property proven here: with a camera configured, a real
    publisher running and the interval long past, the runtime has encoded
    nothing and dated nothing until somebody asks."""

    async def body(rt):
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=50)
        try:
            await rt.apply_cameras(by_slug([_camera_cfg("front", snapshot_interval_seconds=1)]))
            await asyncio.sleep(0.2)  # many intervals; a real frame exists by now
            never_asked = rt._cameras["front"].last_snapshot_at
            wire = await _pull_snapshot(rt)
            return never_asked, wire
        finally:
            stop_pub()

    never_asked, wire = run(body)
    assert never_asked is None  # nothing was produced in the background
    header, _ = _split_snapshot_frame(wire)
    assert header["slug"] == "front"  # and one pull produces one immediately


# --- a snapshot that cannot fit must never close the socket ----------------


def test_a_snapshot_that_needs_backoff_but_fits_is_still_sent():
    async def body(rt):
        stop_pub = _start_image_publisher(
            "/image_raw", width=200, height=200, hz=30, noisy=True
        )
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=200, height=200, snapshot_interval_seconds=1)]
            ))
            return await _pull_snapshot(rt)
        finally:
            stop_pub()

    # A small max_bytes forces quality/downscale backoff to actually run —
    # proof this reaches the wire despite needing it, not just when it
    # already fits.
    wire = run(body, snapshot_max_bytes=20_000)
    header, image_bytes = _split_snapshot_frame(wire)
    assert len(image_bytes) <= 20_000
    assert header["width"] > 0 and header["height"] > 0


def test_the_pulled_budget_binds_as_well_as_the_configured_ceiling():
    """Both limits are real and neither subsumes the other: the configured
    ceiling is what the *wire format* and the socket's `maxPayload` allow,
    the pulled budget is what the *link* can carry inside the
    occupancy budget. `next_snapshot` takes the smaller — here
    the caller's, which is well under the runtime's own."""

    async def body(rt):
        stop_pub = _start_image_publisher(
            "/image_raw", width=200, height=200, hz=30, noisy=True
        )
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=200, height=200, snapshot_interval_seconds=1)]
            ))
            return await _pull_snapshot(rt, max_bytes=6_000)
        finally:
            stop_pub()

    _, image_bytes = _split_snapshot_frame(run(body, snapshot_max_bytes=1_500_000))
    assert len(image_bytes) <= 6_000


def test_a_snapshot_that_cannot_fit_even_backed_off_is_dropped_not_sent():
    """The claim this is about: a frame nothing can shrink under the
    ceiling must never reach the wire — sending it is what closes the
    /bridge socket (ws enforces maxPayload before the frame reaches the
    application), taking datapoints, jobs, commands and config down with
    it. A dropped snapshot is a gap; a closed socket is not."""

    async def body(rt):
        stop_pub = _start_image_publisher(
            "/image_raw", width=200, height=200, hz=30, noisy=True
        )
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=200, height=200, snapshot_interval_seconds=1)]
            ))
            await asyncio.sleep(0.2)  # a real frame is captured and waiting
            return await rt.next_snapshot(_UNLIMITED_BUDGET)
        finally:
            stop_pub()

    # 1 byte: nothing, at any quality or resolution this module tries, ever
    # fits — the most aggressive case the ceiling has to survive.
    assert run(body, snapshot_max_bytes=1) is None


def test_a_dropped_snapshot_is_logged_so_it_is_discoverable_not_silent():
    async def body(rt):
        stop_pub = _start_image_publisher(
            "/image_raw", width=200, height=200, hz=30, noisy=True
        )
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=200, height=200, snapshot_interval_seconds=1)]
            ))
            await asyncio.sleep(0.2)
            await rt.next_snapshot(_UNLIMITED_BUDGET)
        finally:
            stop_pub()

    import logging

    class _RecordingHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.records = []

        def emit(self, record):
            self.records.append(record)

    handler = _RecordingHandler()
    ros_runtime_log = logging.getLogger("fleetless_bridge.ros_runtime")
    ros_runtime_log.addHandler(handler)
    try:
        run(body, snapshot_max_bytes=1)
    finally:
        ros_runtime_log.removeHandler(handler)

    assert any("front" in r.getMessage() and r.levelno >= logging.ERROR for r in handler.records)


def test_a_dropped_snapshot_still_paces_the_next_attempt():
    """Without updating last_snapshot_at, a frame that cannot fit would be
    re-encoded (at every quality/resolution combination) on every single
    pull — a real CPU cost, and a log line four times a second. A drop
    counts as "tried this interval" the same as a successful send."""

    async def body(rt):
        stop_pub = _start_image_publisher(
            "/image_raw", width=200, height=200, hz=30, noisy=True
        )
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=200, height=200, snapshot_interval_seconds=1)]
            ))
            await asyncio.sleep(0.15)
            assert await rt.next_snapshot(_UNLIMITED_BUDGET) is None
            entry = rt._cameras["front"]
            return entry.last_snapshot_at is not None
        finally:
            stop_pub()

    assert run(body, snapshot_max_bytes=1) is True


# --- camera_states: the cross-thread discipline ----------------------------


def test_camera_states_put_from_a_foreign_thread_are_applied_on_the_loop():
    """`CameraStateQueue.put` and `try_get` are unlocked multi-statement
    read-modify-writes over `_order`/`_pending`. A `put` racing a `try_get`
    can leave a marker with no entry behind it (`KeyError` out of the
    writer's `try_next`, which the writer answers by closing the socket) or
    an entry with no marker (that slug's health never reaches the cloud
    again). `on_put` is the other half: it ends in `asyncio.Event.set()`,
    which does not wake the selector when called off the loop thread.

    Both are closed the same way — `put_threadsafe` marshals through
    `call_soon_threadsafe`, so every mutation and every wake runs on the
    loop thread. Asserting the *thread* rather than trying to provoke the
    race is deliberate: a race test that only sometimes reddens is not an
    instrument, and the thread identity is the property the fix actually
    establishes."""

    async def scenario():
        loop = asyncio.get_event_loop()
        queue = CameraStateQueue()
        put_threads = []
        queue.on_put = lambda: put_threads.append(threading.current_thread().name)
        loop_thread = threading.current_thread().name
        finished = threading.Event()

        def producer():
            for i in range(500):
                queue.put_threadsafe(
                    loop,
                    CameraStateUpdate(
                        "front", i % 2 == 0, None, cause="source",
                        observed_at_ms=i, request_id=None,
                    ),
                )
            # The sentinel: a different slug, so coalescing cannot swallow
            # it, and last, so seeing it means the whole burst landed.
            queue.put_threadsafe(
                loop,
                CameraStateUpdate(
                    "back", True, None, cause="source",
                    observed_at_ms=999_999, request_id=None,
                ),
            )
            finished.set()

        thread = threading.Thread(target=producer)
        thread.start()
        seen = []
        deadline = time.monotonic() + 10.0
        while not any(state.slug == "back" for state in seen):
            state = queue.try_get()  # a KeyError here is the unlocked race
            if state is not None:
                seen.append(state)
            await asyncio.sleep(0)
            if time.monotonic() > deadline:
                raise AssertionError("the sentinel never came through")
        thread.join(timeout=5.0)
        assert finished.is_set()
        return seen, put_threads, loop_thread

    seen, put_threads, loop_thread = asyncio.run(scenario())
    assert put_threads, "on_put never fired"
    assert set(put_threads) == {loop_thread}, set(put_threads)
    assert [state.slug for state in seen if state.slug == "back"] == ["back"]


def test_camera_health_restatements_are_queued_from_the_loop_thread():
    """The wiring half of the same rule: `_report_current_camera_health`
    runs on the *executor* thread (via `_submit_async`), so its two
    `camera_states` writes are exactly the call sites that must go through
    `put_threadsafe`. Same for `_on_source_frame`/`_on_source_error`, which
    reach the queue by the same route (`_enqueue`).

    Conversion is lazy now: a real frame lands in
    `entry.latest` only once something pulls it, so this drives a due
    snapshot pull instead of just waiting on `apply_cameras` alone."""

    async def body(rt):
        loop_thread = threading.current_thread().name
        put_threads = []
        stop_pub = _start_image_publisher("/image_raw", width=16, height=12, hz=50)
        try:
            await rt.apply_cameras(by_slug(
                [_camera_cfg("front", width=16, height=12, snapshot_interval_seconds=1)]
            ))
            await _pull_snapshot(rt)
            await _drain_camera_states(rt)  # ordinary first-frame side effects, if any
            rt.camera_states.on_put = lambda: put_threads.append(
                threading.current_thread().name
            )
            await rt.report_current_camera_health()
            await asyncio.sleep(0.05)  # let the marshalled callbacks run
            return put_threads, loop_thread, await _drain_camera_states(rt)
        finally:
            stop_pub()

    put_threads, loop_thread, states = run(body)
    assert put_threads, "report_current_camera_health queued nothing to assert on"
    assert set(put_threads) == {loop_thread}, set(put_threads)
    assert [state.slug for state in states] == ["front"]


# --- live: start_live / stop_live orchestration -----------------


class _FakeLivePublisher:
    """Stands in for `live.LivePublisher` — records what it was asked to do
    rather than touching a real LiveKit server; ros_runtime.py's own
    start_live/stop_live orchestration (dispatch, idempotence, error
    reporting) is what these tests are for, not live.py's own mechanics
    (covered in test_live.py)."""

    instances = []

    def __init__(
        self, *, holder, width, height, fps, bitrate_kbps, on_lost=None,
        fail_with=None, stop_delay=0.0, start_delay=0.0,
    ):
        self.holder = holder
        self.width = width
        self.height = height
        self.fps = fps
        self.bitrate_kbps = bitrate_kbps
        self.on_lost = on_lost
        self.start_calls = []
        self.stop_calls = 0
        self._fail_with = fail_with
        self._stop_delay = stop_delay
        self._start_delay = start_delay
        _FakeLivePublisher.instances.append(self)

    async def start(self, url, room, token):
        self.start_calls.append((url, room, token))
        if self._start_delay:
            # Holds the join open past the point where a *different*
            # slug's own start_live has a
            # chance to run its admission check — without a real `await`
            # here, this coroutine never suspends before returning, and a
            # concurrently-dispatched sibling start_live never gets a turn
            # at all before this one commits.
            await asyncio.sleep(self._start_delay)
        if self._fail_with is not None:
            raise self._fail_with

    async def stop(self):
        if self._stop_delay:
            await asyncio.sleep(self._stop_delay)
        self.stop_calls += 1

    def lose(self, reason):
        """Test-only: simulates live.LivePublisher's own on_lost firing —
        an unexpected room disconnect, not something ros_runtime asked
        for."""
        if self.on_lost is not None:
            self.on_lost(reason)


def _fake_live_factory(fail_with=None, stop_delay=0.0, start_delay=0.0):
    def factory(**kwargs):
        return _FakeLivePublisher(
            fail_with=fail_with, stop_delay=stop_delay, start_delay=start_delay, **kwargs
        )

    return factory


@pytest.fixture(autouse=True)
def _reset_fake_live_publisher_instances():
    """`_FakeLivePublisher.instances` is class-level, shared mutable state
    across every test in this file — cleared before each one so a test
    asserting an exact count (or indexing `instances[-1]`) is never reading
    a previous test's leftovers. Autouse rather than a per-test `.clear()`
    call: relying on every test to remember it is exactly the kind of
    thing that quietly stops being true the next time someone adds one."""
    _FakeLivePublisher.instances.clear()
    yield


async def _drain_camera_states(rt):
    """Everything `CameraStateQueue` currently holds, after giving the loop
    one turn. `try_get` rather than `get_nowait`/`QueueEmpty`:
    `camera_states` stopped being a plain `asyncio.Queue` when it became
    bounded latest-per-slug (the unbounded version was a defect).

    **The turn is why this is `async`, and why there is no synchronous
    version left to reach for.** Tests here call `_on_source_frame` /
    `_on_source_error` / `_report_current_camera_health` inline, standing
    in for the executor thread that really calls them. Those call sites
    reach `camera_states` through `put_threadsafe`, which is one
    `call_soon_threadsafe` hop — invisible in the bridge, where the caller
    is on another thread and nobody is watching — but one loop turn that
    must be given when a test calls the private method and drains on the
    very next line.

    A synchronous drain that skipped the turn did not merely mis-time: it
    quietly turned `assert ... == []` into a check that could not fail, and
    a discard-drain into a line that discarded nothing. Four tests sat in
    that state after the first fix round, passing for the wrong reason,
    because only the ones that turned *red* were repaired. Making the turn
    part of the one and only drain stops that being forgotten again: an
    unawaited call now yields a coroutine object, which fails loudly
    against every assertion here instead of silently passing."""
    await asyncio.sleep(0)
    states = []
    while True:
        state = rt.camera_states.try_get()
        if state is None:
            return states
        states.append(state)


async def _await_camera_state(rt, timeout=10.0):
    """Waits for one camera state to be queued. `CameraStateQueue` has no
    awaitable `get()` on purpose — its only production consumer is the
    writer's tier scan, which must never block on one source — so a test
    that genuinely has to wait polls instead."""
    deadline = time.monotonic() + timeout
    while True:
        state = rt.camera_states.try_get()
        if state is not None:
            return state
        if time.monotonic() > deadline:
            raise AssertionError("no camera state arrived within {}s".format(timeout))
        await asyncio.sleep(0.01)


async def _stop_camera_and_join(rt, slug):
    """Retires `slug`'s adapter synchronously — the camera at `slug` is
    NOT expected to be replaced by anything else afterwards (the test is
    ending, or removing it outright). Unlike `apply_cameras`'s own
    mid-session removal/retarget, which dispatches the stop on a
    throwaway thread and does not wait (by design, so a stuck reconnect
    can never block the executor -- see `RosRuntime._destroy_camera`'s own
    docstring), several tests here give a camera a *real* network-backed
    source (MJPEG/RTSP against 192.0.2.1, deliberately never reachable)
    and then let the test end while it is still the current camera. Its
    adapter's own background thread is genuinely still alive at that
    point, and `RosRuntime.stop()`'s own teardown (`_destroy_all_cameras`)
    would normally catch it -- but only if it is *still* the thing this
    call retires cleanly and promptly, rather than being left to race
    rclpy's own shutdown (found via a full-suite run, reproduced down
    to the exact `InvalidHandle` traceback, not guessed at).

    Dispatched via `_submit_async`, not called directly: `_destroy_camera`
    mutates `self._cameras` and is only ever meant to run on the executor
    thread (every other reader of that dict follows this rule).

    Does NOT touch `_camera_last_reported_error` — that bookkeeping is
    `_apply_cameras`'s own responsibility (kept or cleared depending on
    whether a slug survived a same-kind retarget, per its own comments),
    and the RosRuntime this call retires from is about to be discarded
    entirely, so nothing will ever read it again. See
    `_retarget_cameras_and_join` for the case where the slug *is* about to
    be reused by something else within the same test — there, which
    dict entries survive matters, and only `_apply_cameras`'s own code is
    allowed to decide that."""
    stop_thread = await rt._submit_async(lambda: rt._destroy_camera(slug))
    if stop_thread is not None:
        stop_thread.join(timeout=5.0)
    await _drain_camera_states(rt)


async def _retarget_cameras_and_join(rt, cameras):
    """A drop-in replacement for `await rt.apply_cameras(cameras)` at a
    point where it retargets or removes a slug whose
    *current* adapter is a real, still-connecting network source (MJPEG/
    RTSP against 192.0.2.1) -- exactly `_stop_camera_and_join`'s problem,
    but here the slug goes on to be reused within the same test, so
    `_apply_cameras`'s own `_camera_last_reported_error` bookkeeping (kept
    across a same-kind credential fix, cleared across a kind change or a
    full removal -- see its own comments) has to run untouched, or a test
    checking either behaviour breaks (found the hard way: an earlier
    version of this fix called `_destroy_camera` directly *before*
    `apply_cameras`, which popped the slug early and let `_apply_cameras`
    walk right past it -- its own cleanup line never ran, because the
    slug it keys off was already gone).

    So instead of pre-empting `_destroy_camera`, this wraps it for the
    duration of exactly one `apply_cameras` call -- letting `_apply_
    cameras` dispatch it itself, at the same point and with the same
    bookkeeping production always has, and only adding a join for
    whichever stop thread(s) that produces."""
    stop_threads = []
    original_destroy_camera = rt._destroy_camera

    def capturing_destroy_camera(slug):
        stop_thread = original_destroy_camera(slug)
        if stop_thread is not None:
            stop_threads.append(stop_thread)
        return stop_thread

    rt._destroy_camera = capturing_destroy_camera
    try:
        result = await rt.apply_cameras(cameras)
    finally:
        rt._destroy_camera = original_destroy_camera
    for stop_thread in stop_threads:
        stop_thread.join(timeout=5.0)
    return result


def test_start_live_echoes_the_command_request_id_and_not_a_stale_one():
    """The `camera_state` answering a `camera_start` must carry
    *that* attempt's own `request_id`, not the previous attempt's — this is
    the bridge-side half of "a late answer to a superseded attempt cannot
    resolve the current one" (the correlation itself, matching a pending
    attempt by id, is the cloud's job; the bridge's only obligation is to
    tell the truth about which command it is answering)."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "attempt-A")
        first = await _drain_camera_states(rt)
        await rt.stop_live("front", request_id="stop-B")
        second = await _drain_camera_states(rt)
        await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "attempt-C")
        third = await _drain_camera_states(rt)
        return first, second, third

    first, second, third = run(body, live_publisher_factory=_fake_live_factory())
    assert [s.request_id for s in first] == ["attempt-A"]
    assert [s.request_id for s in second] == ["stop-B"]
    assert [s.request_id for s in third] == ["attempt-C"]


def test_an_unsolicited_camera_state_never_carries_a_request_id():
    """The other half: `report_current_camera_health`'s restatements
    (`cause='source'`) answer no request by definition — this is the case
    the pairing rule (enforced cloud-side, not by the schema) exists for."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=8, height=6)]))
        entry = rt._cameras["front"]
        fake_frame = np.zeros((6, 8, 3), dtype=np.uint8)
        rt._on_source_frame("front", entry, fake_frame, 123456789)
        await _drain_camera_states(rt)  # the ordinary first-frame side effects, if any

        await rt.report_current_camera_health()
        return await _drain_camera_states(rt)

    states = run(body)
    assert states  # sanity: something was actually reported
    assert all(s.cause == "source" for s in states)
    assert all(s.request_id is None for s in states)


def test_start_live_reports_publishing_true_on_success():
    _FakeLivePublisher.instances.clear()

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", width=64, height=48, fps=15)]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_fake_live_factory())
    assert len(states) == 1
    assert states[0].slug == "front"
    assert states[0].publishing is True
    assert states[0].error is None
    pub = _FakeLivePublisher.instances[0]
    assert pub.start_calls == [("wss://media.example", "room-1", "tok-1")]
    assert (pub.width, pub.height, pub.fps) == (64, 48, 15)


def test_start_live_reports_publishing_false_when_the_publisher_fails_to_start():
    from fleetless_bridge.live import LiveStartError

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    states = run(
        body,
        live_publisher_factory=_fake_live_factory(fail_with=LiveStartError("no route to host")),
    )
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error == ("live_unavailable", "no route to host")


def test_start_live_for_an_unconfigured_camera_is_reported_live_unavailable():
    _FakeLivePublisher.instances.clear()

    async def body(rt):
        # No apply_cameras call at all — the cloud enforces that a slug is a
        # granted camera before ever minting a token; this is the
        # defensive fallback for a race it lost, same shape as busy-per-slug
        # for actions.
        await rt.start_live("no-such-camera", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_fake_live_factory())
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error[0] == "live_unavailable"
    assert _FakeLivePublisher.instances == []  # never even tried


def test_a_redelivered_camera_start_for_an_already_live_slug_is_idempotent():
    _FakeLivePublisher.instances.clear()

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return len(_FakeLivePublisher.instances)

    count = run(body, live_publisher_factory=_fake_live_factory())
    assert count == 1  # only one LivePublisher ever created


def test_stop_live_stops_the_publisher_and_reports_publishing_false():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.stop_live("front")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_fake_live_factory())
    assert [s.publishing for s in states] == [True, False]
    assert states[-1].error is None
    assert states[-1].cause == "command"  # an explicit camera_stop, not a config change
    pub = _FakeLivePublisher.instances[-1]
    assert pub.stop_calls == 1


def test_a_config_change_that_stops_a_live_stream_says_so_on_the_wire():
    """The runtime already stops a live stream that a config change
    invalidated (a retarget or any other changed field) — this checks the
    reason travels with it. Before this, the wire frame was
    `{publishing: False, error: None}`, byte-identical to an explicit
    `camera_stop` answer; the console had to ship a message listing two
    possible causes and ranking neither."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front", topic="/image_raw")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)  # the start report; not what this test is about

        # Same shape any changed field produces — a retarget, not a
        # removal, so this exercises apply_cameras's changed_live_slugs path
        # rather than _destroy_camera's.
        await rt.apply_cameras(by_slug([_camera_cfg("front", topic="/image_raw_2")]))
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_fake_live_factory())
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error is None  # not a failure — nothing to report as one
    assert states[0].cause == "config_change"


def test_stop_live_for_a_slug_not_live_is_a_silent_no_op():
    async def body(rt):
        await rt.stop_live("never-started")  # must not raise
        return await _drain_camera_states(rt)

    assert run(body, live_publisher_factory=_fake_live_factory()) == []


def test_stop_all_live_stops_every_active_publisher():
    _FakeLivePublisher.instances.clear()

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front"), _camera_cfg("back", topic="/image_raw_2")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.start_live("back", "wss://media.example", "room-2", "tok-2", "req-1")
        await rt.stop_all_live()
        return [pub.stop_calls for pub in _FakeLivePublisher.instances]

    stop_calls = run(body, live_publisher_factory=_fake_live_factory())
    assert stop_calls == [1, 1]


def test_a_slug_can_start_live_again_after_stop_all_live():
    """Reconnecting starts with nothing live, and a slug the cloud asks
    for again (a fresh `camera_start`, modelled here as a direct
    `start_live` call — client.py's dispatch is what actually decides
    *whether* to ask, per the refcount) must not find itself blocked by
    stale idempotency bookkeeping from the session that just ended."""
    _FakeLivePublisher.instances.clear()

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await rt.stop_all_live()
        await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "req-1")
        return len(_FakeLivePublisher.instances)

    count = run(body, live_publisher_factory=_fake_live_factory())
    assert count == 2  # a genuinely new LivePublisher, not blocked as "already live"


# --- a publisher must never become unreachable -----------


def test_a_publisher_that_fails_after_connecting_is_still_cleaned_up_and_reported():
    """A live.LiveStartError is the documented failure shape, but the whole
    point is that *any* exception from `publisher.start()` — not only that
    one — must not leave a connected-but-untracked publisher. `stop_live`/
    `stop_all_live` can only ever reach what made it into
    `_live_publishers`; this proves a failing start() never gets there,
    while still being cleaned up."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        return await _drain_camera_states(rt), "front" in rt._live_publishers

    states, still_tracked = run(
        body,
        live_publisher_factory=_fake_live_factory(fail_with=RuntimeError("track_factory blew up")),
    )
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error[0] == "live_unavailable"
    pub = _FakeLivePublisher.instances[-1]
    assert pub.stop_calls == 1  # cleaned up, not leaked
    assert still_tracked is False  # not reachable via _live_publishers either


def test_a_failed_start_frees_the_slug_for_a_retry():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        # A slug left unreachable would still be "in" _live_publishers or
        # otherwise block a retry; it must not.
        await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "req-1")
        return len(_FakeLivePublisher.instances), await _drain_camera_states(rt)

    count, states = run(
        body, live_publisher_factory=_fake_live_factory(fail_with=RuntimeError("boom")),
    )
    assert count == 2  # the retry actually tried again, not blocked as "already live"
    assert states[0].publishing is False


# --- an unexpectedly lost publisher is removed and reported ----------------


def test_an_unexpectedly_lost_live_publisher_is_removed_from_tracking():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        pub = _FakeLivePublisher.instances[-1]
        pub.lose("room disconnected: SERVER_SHUTDOWN")
        return "front" in rt._live_publishers

    still_tracked = run(body, live_publisher_factory=_fake_live_factory())
    assert still_tracked is False


def test_an_unexpectedly_lost_live_publisher_is_reported_as_not_publishing():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)  # the initial publishing:true
        pub = _FakeLivePublisher.instances[-1]
        pub.lose("room disconnected: SERVER_SHUTDOWN")
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_fake_live_factory())
    assert len(states) == 1
    assert states[0].publishing is False
    assert states[0].error[0] == "live_unavailable"
    assert "SERVER_SHUTDOWN" in states[0].error[1]


def test_a_lost_slug_can_be_started_again():
    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        _FakeLivePublisher.instances[-1].lose("room disconnected: SERVER_SHUTDOWN")
        await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "req-1")
        return len(_FakeLivePublisher.instances)

    count = run(body, live_publisher_factory=_fake_live_factory())
    assert count == 2  # not blocked as "already live" by a stale entry


def test_stop_live_after_a_loss_is_a_silent_no_op():
    """The publisher already removed itself from tracking when it was lost
    (via on_lost) — a camera_stop that crosses it in flight must not raise
    or double-report."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        _FakeLivePublisher.instances[-1].lose("room disconnected: SERVER_SHUTDOWN")
        await _drain_camera_states(rt)
        await rt.stop_live("front")  # must not raise
        return await _drain_camera_states(rt)

    states = run(body, live_publisher_factory=_fake_live_factory())
    assert states == []  # no further report — nothing new happened


# --- start_live and stop_live for the same slug must not race --------------


def test_a_camera_start_arriving_mid_stop_waits_rather_than_racing():
    """Before this fix, stop_live popped the publisher from
    _live_publishers *before* awaiting publisher.stop() — a camera_start
    arriving in that window saw the slug as free and built a second
    publisher while the first was still disconnecting, both holding the
    same cloud-minted identity. Serialized per slug, the second call must
    wait for the first to fully finish, not run concurrently with it."""

    async def body(rt):
        await rt.apply_cameras(by_slug([_camera_cfg("front")]))
        events = []

        def factory(**kwargs):
            # Marks the moment start_live's body actually runs (past the
            # lock and the idempotency check) — not "about to call
            # start_live", which tells us nothing about whether it was
            # still blocked waiting for the lock.
            events.append("publisher-constructed")
            return _FakeLivePublisher(**kwargs)

        rt._live_publisher_factory = factory
        await rt.start_live("front", "wss://media.example", "room-1", "tok-1", "req-1")
        await _drain_camera_states(rt)
        events.clear()

        first_publisher = _FakeLivePublisher.instances[-1]

        async def slow_stop():
            events.append("stop-start")
            await asyncio.sleep(0.05)
            events.append("stop-end")

        first_publisher.stop = slow_stop

        async def start_again():
            await asyncio.sleep(0.01)  # let stop_live begin first
            await rt.start_live("front", "wss://media.example", "room-1", "tok-2", "req-1")

        await asyncio.gather(rt.stop_live("front"), start_again())
        return events

    events = run(body)
    # The second publisher must only be constructed once the first has
    # fully stopped — not while stop_live is still mid-disconnect.
    stop_end_index = events.index("stop-end")
    constructed_index = events.index("publisher-constructed")
    assert constructed_index > stop_end_index, events


# --- shutdown ------------------------------------------------------------------


def test_stop_fires_the_failsafe_for_an_armed_publisher_before_shutting_down():
    """§7.1 has no platform e-stop — a bridge that shuts down while a
    publisher is armed must not leave the robot holding its last command
    (same rule as removal/retarget, applied to `stop()`).

    The observer runs on a genuinely separate `rclpy.Context`, not the
    default one `rt` uses — production never shares a context between the
    bridge and the robot's own nodes either, and `rt.stop()` tears its own
    context down moments after firing the failsafe, which a same-context
    observer would race against for no reason relevant to what this test
    is actually about."""
    obs_context = rclpy.Context()
    rclpy.init(context=obs_context)
    obs_node = rclpy.create_node("test_stop_failsafe_observer", context=obs_context)
    executor = MultiThreadedExecutor(context=obs_context)
    executor.add_node(obs_node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    received = []
    obs_node.create_subscription(Twist, "/cmd_vel", received.append, 10)

    async def scenario():
        rt = RosRuntime(node_name="test_stop_failsafe")
        rt.start(asyncio.get_event_loop())
        rt.set_connected(True)
        await rt.apply_publishers(by_slug([_publisher_cfg("drive", timeout_ms=60_000)]))
        wait_until(lambda: rt._publishers["drive"].handle.get_subscription_count() > 0)
        await rt.publish("drive", {"speed": 1.0})
        rt.stop()

    try:
        asyncio.run(scenario())
        wait_until(lambda: len(received) >= 2, timeout=2.0)
    finally:
        executor.shutdown()
        obs_node.destroy_node()
        rclpy.shutdown(context=obs_context)
        thread.join(timeout=5.0)

    assert len(received) == 2  # the real publish, then the parting failsafe
    assert received[0].linear.x == pytest.approx(1.0)
    assert received[1].linear.x == pytest.approx(0.0)


def test_stop_destroys_subscriptions_and_joins_the_executor_thread():
    async def body(rt):
        await rt.apply_config(by_slug([_dp("x")]))
        return rt

    async def scenario():
        rt = RosRuntime(node_name="test_shutdown")
        rt.start(asyncio.get_event_loop())
        await rt.apply_config(by_slug([_dp("x")]))
        await rt.apply_actions(by_slug([_action_cfg("x")]))
        await rt.apply_services(by_slug([_service_cfg("x")]))
        await rt.apply_publishers(by_slug([_publisher_cfg("x")]))
        thread = rt._thread
        rt.stop()
        return thread

    thread = asyncio.run(scenario())
    assert not thread.is_alive()


def test_stop_is_safe_to_call_more_than_once():
    async def scenario():
        rt = RosRuntime(node_name="test_double_stop")
        rt.start(asyncio.get_event_loop())
        rt.stop()
        rt.stop()  # must not raise

    asyncio.run(scenario())


def test_stop_before_start_does_nothing():
    RosRuntime(node_name="test_never_started").stop()  # must not raise


# --- URDF availability detection ------------------------

# `robot_state_publisher`'s own QoS — a publisher on this domain must offer at
# least this durability for the bridge's TRANSIENT_LOCAL subscription to
# receive anything from it at all (DDS QoS compatibility, not merely a style
# choice).
_URDF_QOS = QoSProfile(
    depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL, reliability=ReliabilityPolicy.RELIABLE
)

# base.stl is referenced twice (visual and collision) — proves dedup. The
# wheel's mesh is a `file://` URI, not `package://` — proves it is excluded.
SIMPLE_URDF = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="package://test_pkg/meshes/base.stl"/></geometry></visual>
    <collision><geometry><mesh filename="package://test_pkg/meshes/base.stl"/></geometry></collision>
  </link>
  <link name="arm_link">
    <visual><geometry><mesh filename="package://test_pkg/meshes/arm.stl"/></geometry></visual>
  </link>
  <link name="wheel_link">
    <visual><geometry><mesh filename="file:///tmp/not_a_package_uri.stl"/></geometry></visual>
  </link>
</robot>"""

NO_MESH_URDF = """<?xml version="1.0"?>
<robot name="test_robot"><link name="base_link"/></robot>"""

# One texture at each of the two shapes a real URDF actually uses: a
# top-level `<material>` definition (referenced by name elsewhere) and an
# inline `<material>` inside a `<visual>` — `root.iter("texture")` must find
# both regardless of nesting depth, matching the cloud's own
# REWRITABLE_ELEMENTS walk.
TEXTURED_URDF = """<?xml version="1.0"?>
<robot name="test_robot">
  <material name="body_material">
    <texture filename="package://test_pkg/textures/body.png"/>
  </material>
  <link name="base_link">
    <visual>
      <geometry><mesh filename="package://test_pkg/meshes/base.stl"/></geometry>
      <material name="inline_material">
        <texture filename="package://test_pkg/textures/inline.png"/>
      </material>
    </visual>
  </link>
</robot>"""

MALFORMED_URDF = "<robot><link name=\"base_link\">"  # unclosed tags

# A crafted external-entity payload — must never resolve file:///etc/passwd
# or expand, only fail the same honest way malformed XML does.
XXE_URDF = """<?xml version="1.0"?>
<!DOCTYPE robot [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
<robot><link name="&xxe;"/></robot>"""


def _publish_urdf(node, text, topic=DEFAULT_URDF_TOPIC):
    pub = node.create_publisher(StringMsg, topic, _URDF_QOS)
    msg = StringMsg()
    msg.data = text
    pub.publish(msg)
    return pub


def test_a_urdf_is_detected_and_its_meshes_reported():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher")
        try:
            _publish_urdf(pub_node, SIMPLE_URDF)
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    update = run(body)
    assert update.urdf is True
    # base.stl deduplicated (visual + collision, same URI); the file:// mesh
    # excluded entirely; first-seen order kept.
    assert update.meshes == ("package://test_pkg/meshes/base.stl", "package://test_pkg/meshes/arm.stl")


def test_a_urdf_with_textures_reports_them_alongside_its_meshes():
    """A `<texture>` used to be invisible to this bridge entirely
    — the cloud's own extractor always saw it and listed it in `urdf.
    missing`, correctly named and permanently unfixable, because no sync
    would ever offer it. `meshes` (unrenamed on the wire) is deliberately
    unkinded; `_run_asset_sync` is what tells mesh and texture apart later."""

    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_textures")
        try:
            _publish_urdf(pub_node, TEXTURED_URDF)
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    update = run(body)
    assert update.urdf is True
    # Both the top-level <material> definition's texture and the inline
    # <visual><material>'s texture are found — any nesting depth, matching
    # the cloud's own walk — alongside the mesh, first-seen order.
    assert update.meshes == (
        "package://test_pkg/textures/body.png",
        "package://test_pkg/meshes/base.stl",
        "package://test_pkg/textures/inline.png",
    )


def test_a_urdf_with_no_meshes_reports_an_empty_tuple():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_no_mesh")
        try:
            _publish_urdf(pub_node, NO_MESH_URDF)
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    update = run(body)
    assert update.urdf is True
    assert update.meshes == ()


def test_a_urdf_published_before_the_bridge_started_still_arrives():
    """The TRANSIENT_LOCAL catch this whole detection depends on: the
    common case is a robot whose stack — and `robot_state_publisher` —
    started well before the bridge connects. A VOLATILE subscription would
    see nothing here and read as "this robot has no URDF" rather than as a
    bug.

    `RosRuntime.start()` owns `rclpy.init()` on the default context (this
    file's own module docstring: a second `rclpy.init()` on it would
    collide), so a genuine "publisher predates the bridge's own process
    context" cannot be staged there at all. A separate `Context()`
    publishes *before* `run(body)` ever calls `RosRuntime.start()`, and
    stays alive (TRANSIENT_LOCAL is retained by the *writer*, not a
    separate durability service — a publisher torn down before the late
    subscriber joins has nothing left to replay) until the bridge has
    actually received it — still the same DDS domain, so discovery and the
    retained-sample replay happen exactly as they would between two
    genuinely different processes started in this order."""
    from rclpy.context import Context

    late_context = Context()
    rclpy.init(context=late_context, args=[])
    pub_node = rclpy.create_node("test_urdf_publisher_late_join", context=late_context)
    try:
        _publish_urdf(pub_node, SIMPLE_URDF)

        async def body(rt):
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)

        update = run(body)
    finally:
        pub_node.destroy_node()
        rclpy.shutdown(context=late_context)

    assert update.urdf is True
    assert "package://test_pkg/meshes/base.stl" in update.meshes


def test_republishing_identical_urdf_content_does_not_queue_a_second_report():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_dup")
        try:
            pub = _publish_urdf(pub_node, SIMPLE_URDF)
            await asyncio.wait_for(rt.assets.get(), timeout=5.0)  # the first report
            msg = StringMsg()
            msg.data = SIMPLE_URDF  # byte-identical content, republished
            pub.publish(msg)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rt.assets.get(), timeout=0.5)
        finally:
            pub_node.destroy_node()

    run(body)  # must not raise for any reason other than the expected timeout above


def test_republishing_different_urdf_content_queues_a_second_report():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_change")
        try:
            pub = _publish_urdf(pub_node, SIMPLE_URDF)
            await asyncio.wait_for(rt.assets.get(), timeout=5.0)  # the first report
            msg = StringMsg()
            msg.data = NO_MESH_URDF  # genuinely different content
            pub.publish(msg)
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    update = run(body)
    assert update.meshes == ()


def test_a_blank_urdf_message_is_not_reported():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_blank")
        try:
            _publish_urdf(pub_node, "")
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rt.assets.get(), timeout=0.5)
        finally:
            pub_node.destroy_node()

    run(body)


def test_malformed_urdf_xml_is_reported_available_with_no_meshes_rather_than_crashing():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_malformed")
        try:
            _publish_urdf(pub_node, MALFORMED_URDF)
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    update = run(body)
    assert update.urdf is True
    assert update.meshes == ()


def test_an_xxe_attempt_fails_the_same_honest_way_as_malformed_xml():
    """Security review: `_extract_mesh_uris` parses with `defusedxml`, not
    stdlib `xml.etree` — `/robot_description` is graph input, not
    necessarily first-party. `DefusedXmlException` is a `ValueError`, not
    `ElementTree.ParseError`, so this also proves both are actually caught
    (a callback that raised here would take the executor thread's
    subscription callback down with it, silently ending all future URDF
    detection for the life of the process)."""

    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_xxe")
        try:
            _publish_urdf(pub_node, XXE_URDF)
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node.destroy_node()

    update = run(body)
    assert update.urdf is True
    assert update.meshes == ()


def test_report_current_urdf_availability_is_a_no_op_before_anything_is_seen():
    async def body(rt):
        await rt.report_current_urdf_availability()
        return rt.assets.empty()

    assert run(body) is True


def test_report_current_urdf_availability_restates_the_latest_cached_urdf():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_restate")
        try:
            _publish_urdf(pub_node, SIMPLE_URDF)
            first = await asyncio.wait_for(rt.assets.get(), timeout=5.0)
            await rt.report_current_urdf_availability()
            second = await asyncio.wait_for(rt.assets.get(), timeout=5.0)
            return first, second
        finally:
            pub_node.destroy_node()

    first, second = run(body)
    assert second == first  # a restatement of the same cached fact, not a new detection


# --- urdf_available stops being sticky ----------------------------
#
# The subscription callback above can only ever notice a publisher *sending*
# something — it can report `true` and it can restate `true`, but it has no
# way to notice a publisher going away, or one that was never there. These
# tests are all `_check_urdf_availability`, the active half: a real graph
# query (`count_publishers`), not the passive callback.


def test_urdf_availability_check_is_silent_before_it_has_ever_run():
    """Before any active check has happened at all, nothing is reported —
    that gap is the cloud's own `null` ("nobody has asked yet"), not this
    bridge's to fill in with a premature `false`."""

    async def body(rt):
        return rt.assets.empty()

    assert run(body) is True


def test_urdf_availability_check_reports_false_when_nothing_has_ever_published():
    """"Asked and none" — the case the passive callback could never reach
    on its own, because nothing ever published to trigger it."""

    async def body(rt):
        await rt.check_urdf_availability_now()
        return await asyncio.wait_for(rt.assets.get(), timeout=5.0)

    update = run(body)
    assert update.urdf is False
    assert update.meshes == ()


def test_urdf_availability_check_does_not_repeat_an_unchanged_false():
    async def body(rt):
        await rt.check_urdf_availability_now()
        await asyncio.wait_for(rt.assets.get(), timeout=5.0)  # consume the first `false`
        await rt.check_urdf_availability_now()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(rt.assets.get(), timeout=0.5)

    run(body)  # must not raise for any reason other than the expected timeout above


def test_urdf_availability_check_does_not_invent_true_from_a_publisher_alone():
    """A publisher existing is not the same as content having arrived — a
    late-joining subscriber (this bridge, TRANSIENT_LOCAL) sees nothing
    until something is actually published. The active check must not
    report `true` on presence alone; only `_on_robot_description` — on
    genuinely receiving content — may ever say `true`."""

    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_no_publish")
        try:
            pub_node.create_publisher(StringMsg, DEFAULT_URDF_TOPIC, _URDF_QOS)  # never published to
            wait_until(lambda: rt._node.count_publishers(DEFAULT_URDF_TOPIC) > 0)
            await rt.check_urdf_availability_now()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(rt.assets.get(), timeout=0.5)
        finally:
            pub_node.destroy_node()

    run(body)


def test_urdf_availability_check_transitions_true_to_false_and_clears_the_cache():
    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_disappears")
        _publish_urdf(pub_node, SIMPLE_URDF)
        first = await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        assert first.urdf is True

        pub_node.destroy_node()
        wait_until(lambda: rt._node.count_publishers(DEFAULT_URDF_TOPIC) == 0)
        await rt.check_urdf_availability_now()
        second = await asyncio.wait_for(rt.assets.get(), timeout=5.0)

        # The cache a sync would read from is genuinely cleared, not just
        # the wire report — a request arriving in this window must not
        # silently serve stale bytes as if the robot still had them.
        with rt._urdf_lock:
            text = rt._latest_urdf_text
            meshes = rt._latest_urdf_meshes
        return second, text, meshes

    second, text, meshes = run(body)
    assert second.urdf is False
    assert second.meshes == ()
    assert text is None
    assert meshes == ()


def test_urdf_availability_check_lets_identical_content_be_seen_again_after_a_gap():
    """The reset must clear `_last_urdf_hash` too — otherwise a robot that
    loses its URDF and later republishes the *exact same* text would be
    silently deduped away as "unchanged", staying `false` forever."""

    async def body(rt):
        pub_node = rclpy.create_node("test_urdf_publisher_gap_then_same")
        _publish_urdf(pub_node, SIMPLE_URDF)
        first = await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        assert first.urdf is True

        pub_node.destroy_node()
        wait_until(lambda: rt._node.count_publishers(DEFAULT_URDF_TOPIC) == 0)
        await rt.check_urdf_availability_now()
        second = await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        assert second.urdf is False

        pub_node_2 = rclpy.create_node("test_urdf_publisher_gap_then_same_2")
        try:
            _publish_urdf(pub_node_2, SIMPLE_URDF)  # identical content
            return await asyncio.wait_for(rt.assets.get(), timeout=5.0)
        finally:
            pub_node_2.destroy_node()

    third = run(body)
    assert third.urdf is True
    assert third.meshes == ("package://test_pkg/meshes/base.stl", "package://test_pkg/meshes/arm.stl")


# --- mesh resolution and the HTTP upload ----------------

# A real, always-installed package with real files in its share directory —
# resolution doesn't need a fixture package, just a genuine ament index entry.
#
# The prefix is read from $ROS_DISTRO rather than spelt "humble": this suite
# runs on every distribution the package is built for (./run-tests.sh --distro
# <name>), and a hardcoded prefix would make Jazzy and Lyrical fail at import
# time -- which is the one failure shape that reddens a whole run for a reason
# that has nothing to do with what it was measuring.
_ROS_SHARE = "/opt/ros/{}/share".format(os.environ["ROS_DISTRO"])

_RESOLVABLE_MESH_URI = "package://example_interfaces/package.xml"
with open(_ROS_SHARE + "/example_interfaces/package.xml", "rb") as _f:
    _RESOLVABLE_MESH_BYTES = _f.read()

# A second, distinct real file for texture tests — a different
# package so a test can request both a mesh and a texture URI in one sync
# and tell them apart by which bytes came back.
_RESOLVABLE_TEXTURE_URI = "package://sensor_msgs/package.xml"
with open(_ROS_SHARE + "/sensor_msgs/package.xml", "rb") as _f:
    _RESOLVABLE_TEXTURE_BYTES = _f.read()

_URDF_FOR_SYNC = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="package://example_interfaces/package.xml"/></geometry></visual>
  </link>
</robot>"""

_URDF_FOR_SYNC_WITH_TEXTURE = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual>
      <geometry><mesh filename="package://example_interfaces/package.xml"/></geometry>
      <material name="m"><texture filename="package://sensor_msgs/package.xml"/></material>
    </visual>
  </link>
</robot>"""


class _AssetUploadHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if self.server.keep_body:
            body = self.rfile.read(length)
            body_length = len(body)
        else:
            # Drained in chunks and counted, not kept: one test sends a
            # file larger than anything this process should hold, and
            # `received` outlives the request. The count is what that test
            # asserts on anyway.
            body = b""
            body_length = 0
            while body_length < length:
                chunk = self.rfile.read(min(1 << 20, length - body_length))
                if not chunk:
                    break
                body_length += len(chunk)
        # `self.headers` (an `email.message.Message`) is case-insensitive on
        # `.get()`, same as HTTP itself — `urllib.request.Request` sends
        # `X-fleetless-asset-name` (its own `Key.capitalize()`, not this
        # test's or ros_runtime.py's casing), so this reads it the same way
        # a real HTTP server would rather than assuming an exact case.
        #
        # TWO headers, not one reinterpreted — `X-Fleetless-Asset-Name`
        # keeps its original meaning (Latin-1-safe, lossy outside it), and
        # `X-Fleetless-Asset-Name-Encoded` carries the percent-encoded
        # truth. Mirrors the cloud's own preference rule: decode the
        # encoded header when present, fall back to the bare one — so
        # every existing assertion against the developer's own (unencoded)
        # name keeps working for the common ASCII case, and the raw fields
        # below let the one test that proves the wire encoding do so.
        raw_name = self.headers.get("X-Fleetless-Asset-Name")
        raw_name_encoded = self.headers.get("X-Fleetless-Asset-Name-Encoded")
        canonical_name = urllib.parse.unquote(raw_name_encoded) if raw_name_encoded is not None else raw_name
        record = {
            "path": self.path,
            "headers": {
                "Authorization": self.headers.get("Authorization"),
                "Content-Type": self.headers.get("Content-Type"),
                "X-Fleetless-Asset-Kind": self.headers.get("X-Fleetless-Asset-Kind"),
                "X-Fleetless-Asset-Name": canonical_name,
                "X-Fleetless-Asset-Name-Raw": raw_name,
                "X-Fleetless-Asset-Name-Encoded-Raw": raw_name_encoded,
                "X-Fleetless-Sync-Id": self.headers.get("X-Fleetless-Sync-Id"),
                "X-Fleetless-Asset-Size": self.headers.get("X-Fleetless-Asset-Size"),
                "Content-Length": self.headers.get("Content-Length"),
            },
            "body": body,
            "body_length": body_length,
        }
        self.server.received.append(record)
        answer = self.server.status_for(record)
        # A plain status, or `(status, body)` for a refusal that has
        # something to say — the cloud's store refusal carries its three
        # numbers in the body, and a test of that cannot state them in a
        # status code.
        status, body = answer if isinstance(answer, tuple) else (answer, None)
        self.send_response(status)
        if body is not None:
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body is not None:
            self.wfile.write(body)
        elif status < 300:
            self.wfile.write(b'{"id": "fake-asset-id"}')

    def log_message(self, format, *args):  # noqa: A002 - stdlib's own signature
        pass  # keep test output quiet


class _RateLimitedUploadHandler(http.server.BaseHTTPRequestHandler):
    """Refuses the first `retries_needed` attempts for any name in
    `rate_limit_names` with a real `429` carrying the exact
    `rateLimitDetails` shape (`{"code": "rate_limited", ..., "details":
    {"retry_after_ms": N}}`) the cloud actually sends, then succeeds — so
    the bridge's retry loop is proven against the real wire shape, not a
    stand-in for it."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        # The same preference rule the real cloud uses —
        # prefer decoding the encoded header, fall back to the bare one.
        # `rate_limit_names` is expressed in developer-facing names either way.
        raw_name = self.headers.get("X-Fleetless-Asset-Name")
        raw_name_encoded = self.headers.get("X-Fleetless-Asset-Name-Encoded")
        name = urllib.parse.unquote(raw_name_encoded) if raw_name_encoded is not None else raw_name
        attempts = self.server.attempts
        attempts[name] = attempts.get(name, 0) + 1
        self.server.attempt_log.append((name, attempts[name]))
        if name in self.server.rate_limit_names and attempts[name] <= self.server.retries_needed:
            self.send_response(429)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "code": "rate_limited",
                "message": "Too many uploads. Try again shortly.",
                "details": {"retry_after_ms": self.server.retry_after_ms},
            }).encode("utf-8"))
            return
        self.send_response(201)
        self.end_headers()
        self.wfile.write(b'{"id": "fake-asset-id"}')

    def log_message(self, format, *args):  # noqa: A002 - stdlib's own signature
        pass


def _start_rate_limited_upload_server(*, rate_limit_names, retries_needed=2, retry_after_ms=5):
    server = http.server.HTTPServer(("127.0.0.1", 0), _RateLimitedUploadHandler)
    server.rate_limit_names = set(rate_limit_names)
    server.retries_needed = retries_needed
    server.retry_after_ms = retry_after_ms
    server.attempts = {}
    server.attempt_log = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = "http://127.0.0.1:{}/api/bridge/assets".format(server.server_port)

    def stop():
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)

    return server, url, stop


def _start_asset_upload_server(*, status=201, fail_names=(), delay_s=0.0, keep_body=True):
    """A real HTTP server for the upload path (this package's own rule:
    verify the shape a real client sends, not the shape a test builds).
    `fail_names` makes uploads for specific `X-Fleetless-Asset-Name` values
    fail with a 500, everything else succeeds with `status`. `delay_s`
    stalls every response, for tests that need an upload still in flight.
    `keep_body=False` counts each body instead of retaining it, for the one
    test whose file is larger than this process has any business holding."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _AssetUploadHandler)
    server.received = []
    server.keep_body = keep_body

    def status_for(record):
        if delay_s:
            time.sleep(delay_s)
        name = record["headers"].get("X-Fleetless-Asset-Name")
        return 500 if name in fail_names else status

    server.status_for = status_for
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = "http://127.0.0.1:{}/api/bridge/assets".format(server.server_port)

    def stop():
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)

    return server, url, stop


async def _publish_urdf_and_wait(rt, text=_URDF_FOR_SYNC):
    pub_node = rclpy.create_node("test_urdf_publisher_for_sync_{}".format(id(rt)))
    try:
        _publish_urdf(pub_node, text)
        await asyncio.wait_for(rt.assets.get(), timeout=5.0)
    finally:
        pub_node.destroy_node()


async def _drain_asset_progress_until(rt, terminal_states, timeout=5.0):
    updates = []
    while True:
        update = await asyncio.wait_for(rt.asset_progress.get(), timeout=timeout)
        updates.append(update)
        if update.state in terminal_states:
            return updates


def test_sync_assets_uploads_the_urdf_and_a_resolved_mesh():
    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].state == "finished"
    assert updates[-1].done == updates[-1].total == 2
    assert updates[-1].failed == ()

    by_name = {r["headers"]["X-Fleetless-Asset-Name"]: r for r in received}
    assert set(by_name) == {URDF_ASSET_NAME, _RESOLVABLE_MESH_URI}

    urdf_record = by_name[URDF_ASSET_NAME]
    assert urdf_record["headers"]["Authorization"] == "Bearer upload-tok"
    assert urdf_record["headers"]["X-Fleetless-Asset-Kind"] == "urdf"
    assert urdf_record["headers"]["X-Fleetless-Sync-Id"] == "sync-1"
    assert urdf_record["headers"]["Content-Type"] == "application/xml"
    assert urdf_record["body"] == _URDF_FOR_SYNC.encode("utf-8")

    mesh_record = by_name[_RESOLVABLE_MESH_URI]
    assert mesh_record["headers"]["Authorization"] == "Bearer upload-tok"
    assert mesh_record["headers"]["X-Fleetless-Asset-Kind"] == "mesh"
    assert mesh_record["headers"]["X-Fleetless-Sync-Id"] == "sync-1"
    assert mesh_record["body"] == _RESOLVABLE_MESH_BYTES


def test_sync_assets_uploads_a_texture_with_kind_texture():
    """The same sync, resolution and upload path as a mesh — only
    `kind` on the wire differs, classified from which XML element the URI
    came from in the last-seen URDF, not from anything the request itself
    carries (`cloud_asset_request.meshes` is unkinded)."""

    async def body(rt):
        await _publish_urdf_and_wait(rt, text=_URDF_FOR_SYNC_WITH_TEXTURE)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets(
                "sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI, _RESOLVABLE_TEXTURE_URI)
            )
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].state == "finished"
    assert updates[-1].done == updates[-1].total == 3
    assert updates[-1].failed == ()

    by_name = {r["headers"]["X-Fleetless-Asset-Name"]: r for r in received}
    assert set(by_name) == {URDF_ASSET_NAME, _RESOLVABLE_MESH_URI, _RESOLVABLE_TEXTURE_URI}

    mesh_record = by_name[_RESOLVABLE_MESH_URI]
    assert mesh_record["headers"]["X-Fleetless-Asset-Kind"] == "mesh"
    assert mesh_record["body"] == _RESOLVABLE_MESH_BYTES

    texture_record = by_name[_RESOLVABLE_TEXTURE_URI]
    assert texture_record["headers"]["Authorization"] == "Bearer upload-tok"
    assert texture_record["headers"]["X-Fleetless-Asset-Kind"] == "texture"
    assert texture_record["headers"]["X-Fleetless-Sync-Id"] == "sync-1"
    assert texture_record["body"] == _RESOLVABLE_TEXTURE_BYTES


def test_sync_assets_classifies_an_unrecognised_uri_as_mesh_by_default():
    """A URI the cloud requests that no longer matches anything in the
    last-seen URDF (a race: the URDF changed between `assets_available` and
    this request landing) falls back to `mesh` — the only kind that existed
    before textures were offered, not a new failure mode."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)  # no texture in this URDF at all
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_TEXTURE_URI,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].failed == ()
    by_name = {r["headers"]["X-Fleetless-Asset-Name"]: r for r in received}
    assert by_name[_RESOLVABLE_TEXTURE_URI]["headers"]["X-Fleetless-Asset-Kind"] == "mesh"


def test_sync_assets_applies_the_same_containment_check_to_a_texture():
    """A texture goes through the *same* containment check as a mesh, not a
    parallel copy of it — proven the same way the mesh test proves it (a
    traversal that resolves to a real, readable file still fails, and fails
    identically to an ordinary unresolvable URI)."""
    traversal_uri = _traversal_uri_for("sensor_msgs", "/etc/passwd")
    urdf = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual>
      <geometry><mesh filename="package://example_interfaces/package.xml"/></geometry>
      <material name="m"><texture filename="{}"/></material>
    </visual>
  </link>
</robot>""".format(traversal_uri)

    async def body(rt):
        await _publish_urdf_and_wait(rt, text=urdf)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (traversal_uri,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].failed == ((traversal_uri, "unresolvable", None),)
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME}
    assert not any(b"root:" in r["body"] for r in received)


# --- no ceiling of the bridge's own ---
#
# There was one until the per-robot store replaced it: the bridge stated
# every file and refused anything over `ASSET_UPLOAD_MAX_BYTES` before
# `open()`. That constant is gone from the contracts, and a robot's assets
# are now charged against a 1 GB store the cloud alone can see — it knows
# what the other files already cost, and this process does not. So the
# bridge attempts every file and reports what comes back.


_OVER_OLD_CEILING_BYTES = 64 * 1024 * 1024 + 1


def test_a_mesh_far_over_the_old_ceiling_is_uploaded_and_states_its_size():
    """The inverse of the check that used to live here: one byte over the
    old 67,108,864-byte ceiling, which would have been refused before a
    single byte was read, and is now simply uploaded.

    Nothing in the upload path is mocked, so this also proves the number
    reaches the wire — the announced size, the `Content-Length` and the
    bytes the server actually received are asserted to be the same one. A
    mocked `_upload_mesh_file` could show "it was attempted" and nothing
    about what was sent, which is the half that decides whether the cloud
    can weigh it at all.

    The file is sparse (`truncate`, no bytes written), so a fixture this
    size costs nothing: measured at 0 blocks on disk, 0.05 s on the wire
    and no change in peak RSS. The server counts the body instead of
    keeping it, for the same reason.

    `.stl`, not `.dae`: a file this size is deliberately *not* scanned for
    internal textures — see `DAE_SCAN_MAX_BYTES` below. Uploading and
    reading are two different costs, and only one of them was ever the
    cloud's to bound."""
    mesh_uri = "package://robot_description_fixture/enormous.stl"
    with tempfile.NamedTemporaryFile(suffix=".stl") as mesh_file:
        mesh_file.truncate(_OVER_OLD_CEILING_BYTES)
        mesh_file.flush()

        async def body(rt):
            await _publish_urdf_and_wait(rt)
            server, url, stop = _start_asset_upload_server(keep_body=False)
            try:
                with mock.patch.object(
                    RosRuntime, "_resolve_package_uri", return_value=mesh_file.name,
                ):
                    await rt.sync_assets("sync-1", url, "upload-tok", (mesh_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert updates[-1].state == "finished"
    assert updates[-1].done == updates[-1].total == 2
    assert updates[-1].failed == ()

    sent = [r for r in received if r["headers"]["X-Fleetless-Asset-Name"] == mesh_uri]
    assert len(sent) == 1, [r["headers"]["X-Fleetless-Asset-Name"] for r in received]
    record = sent[0]
    assert record["headers"]["X-Fleetless-Asset-Size"] == str(_OVER_OLD_CEILING_BYTES)
    assert record["headers"]["Content-Length"] == str(_OVER_OLD_CEILING_BYTES)
    assert record["body_length"] == _OVER_OLD_CEILING_BYTES


# --- the cloud refuses: the store's three numbers survive the frame ---


_STORE_REFUSAL_BODY = json.dumps({
    "code": "quota_exceeded",
    "message": "the robot's asset store is full",
    "details": {
        "store_bytes": 1_000_000_000,
        "used_bytes": 999_999_000,
        "size_bytes": 193_886_766,
    },
}).encode("utf-8")


def test_a_store_refusal_becomes_a_refused_entry_carrying_the_three_numbers():
    """The bridge cannot compute any of these — it does not know what the
    robot's other assets cost — so it passes the cloud's own answer
    through untouched. A `refused` entry with no numbers beside it would
    leave a developer unable to tell a full store from a producer that
    declined, which is the distinction the details exist to draw.

    The URDF still uploads and the sync still reaches `finished`: one
    refused mesh is not a failed sync, and stopping would lose the meshes
    that do fit."""

    def status_for(record):
        if record["headers"]["X-Fleetless-Asset-Name"] == URDF_ASSET_NAME:
            return 201
        return 409, _STORE_REFUSAL_BODY

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        server.status_for = status_for
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].state == "finished"
    assert updates[-1].failed == (
        (_RESOLVABLE_MESH_URI, "refused", {
            "store_bytes": 1_000_000_000,
            "used_bytes": 999_999_000,
            "size_bytes": 193_886_766,
        }),
    )
    # It was attempted, unlike under the old ceiling: both names reached
    # the wire, and only the cloud decided which one it had room for.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {
        URDF_ASSET_NAME, _RESOLVABLE_MESH_URI,
    }


def test_a_refusal_the_bridge_cannot_read_is_still_a_refusal():
    """A 409 whose body is not the shape this bridge expects — an older
    cloud, a proxy's own error page — still means "never stored", which is
    `refused`. Inventing numbers to fill `details` would be worse than
    admitting there are none, and the contract allows a bare `refused`
    for exactly the case where no store number describes it."""

    def status_for(record):
        if record["headers"]["X-Fleetless-Asset-Name"] == URDF_ASSET_NAME:
            return 201
        return 409, b"<html>nope</html>"

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        server.status_for = status_for
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            return await _drain_asset_progress_until(rt, {"finished"})
        finally:
            stop()

    updates = run(body)
    assert updates[-1].failed == ((_RESOLVABLE_MESH_URI, "refused", None),)


# --- a mesh is streamed, not buffered whole ---
#
# A 60 MB file read whole into memory before upload cost 76.8 MB of peak
# RSS; handed to urllib as a file object with an explicit Content-Length,
# 19.5 MB. The test below proves the mechanism that measurement depended
# on — that the upload path never asks the file for all its bytes in one
# `read()` call — deterministically, rather than re-measuring RSS inside a
# container on every run.


def test_a_mesh_is_streamed_in_chunks_not_one_read():
    """Wraps the real file `_upload_asset_stream` opens (not a fake) so its
    `read()` calls are recorded, then asserts none of them asked for
    anywhere near the whole file — proving `http.client`'s own chunked
    send is what is happening, not `_upload_mesh_file` (or anything it
    calls) requesting all the bytes at once the way the older code did."""
    file_size = 200 * 1024  # small enough to keep the test fast, large
    # enough that a single default 8192-byte read chunk could not cover it

    read_sizes = []
    real_open = open

    def spy_open(path, *args, **kwargs):
        handle = real_open(path, *args, **kwargs)
        if not path.endswith("big_mesh_fixture.bin"):
            return handle
        real_read = handle.read

        def spying_read(n=-1, *a, **kw):
            read_sizes.append(n)
            return real_read(n, *a, **kw)

        handle.read = spying_read
        return handle

    with tempfile.TemporaryDirectory() as tmp_dir:
        mesh_path = os.path.join(tmp_dir, "big_mesh_fixture.bin")
        with open(mesh_path, "wb") as f:
            f.write(b"m" * file_size)

        dae_uri = "package://robot_description_fixture/big_mesh_fixture.bin"
        urdf = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)

        async def body(rt):
            await _publish_urdf_and_wait(rt, text=urdf)
            server, url, stop = _start_asset_upload_server()
            try:
                with mock.patch(
                    "fleetless_bridge.ros_runtime.open", side_effect=spy_open, create=True
                ), mock.patch.object(RosRuntime, "_resolve_package_uri", return_value=mesh_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert updates[-1].failed == ()
    by_name = {r["headers"]["X-Fleetless-Asset-Name"]: r for r in received}
    assert len(by_name[dae_uri]["body"]) == file_size
    # The actual claim: every individual read asked for a bounded chunk,
    # never the whole (or even half the) file in one call — the older
    # code's `handle.read()` with no argument would show up here as one
    # read of size -1 (or file_size), immediately falsifying this.
    assert read_sizes, "the spy never saw a read() call at all — test is broken, not proving anything"
    assert all(0 < n <= 65536 for n in read_sizes), (
        "at least one read() asked for more than a bounded chunk: {}".format(read_sizes)
    )
    assert len(read_sizes) > 1, "the whole file arrived in a single read() — not streamed"


# --- the upload rate limit is retried, not treated as a failure ---


def test_a_rate_limited_upload_is_retried_and_eventually_succeeds():
    """A mesh refused twice with a real `429 rate_limited` body still ends
    up uploaded, and the sync reports it as succeeded, not failed: nothing
    that resolves perfectly is reported missing because the bucket was
    momentarily empty."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_rate_limited_upload_server(
            rate_limit_names={_RESOLVABLE_MESH_URI}, retries_needed=2, retry_after_ms=5
        )
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server, updates
        finally:
            stop()

    server, updates = run(body)
    assert updates[-1].state == "finished"
    assert updates[-1].failed == ()
    # Two 429s, then a 201 — the identical request tried three times, not a
    # different item skipped and picked back up.
    assert server.attempts[_RESOLVABLE_MESH_URI] == 3


def test_a_rate_limited_upload_waits_the_retry_after_ms_it_was_told():
    """Not just "eventually succeeds" — waits *this* long, not a fixed
    guess, and not zero (which would make the retry indistinguishable from
    hammering the limiter)."""
    retry_after_ms = 250

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_rate_limited_upload_server(
            rate_limit_names={_RESOLVABLE_MESH_URI}, retries_needed=1, retry_after_ms=retry_after_ms
        )
        try:
            started = time.monotonic()
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            await _drain_asset_progress_until(rt, {"finished"})
            return time.monotonic() - started
        finally:
            stop()

    elapsed_s = run(body)
    # At least the told wait (loose lower bound: real scheduling jitter,
    # never less) and well under a second — proves this is reading the
    # number, not falling back to ASSET_UPLOAD_RATE_LIMIT_FALLBACK_WAIT_S
    # (0.5s) or some other fixed guess larger than what was actually asked.
    assert elapsed_s >= retry_after_ms / 1000.0
    assert elapsed_s < 2.0


def test_a_persistently_rate_limited_upload_gives_up_and_reports_failed():
    """The other half: "fails naming the limit rather than
    silently reporting meshes as missing that resolve perfectly" — a mesh
    that never clears the limiter still reaches a bounded, honest `failed`
    entry, not an unbounded retry loop (which is exactly the sync-that-
    never-ends shape the sync exists to rule out)."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_rate_limited_upload_server(
            rate_limit_names={_RESOLVABLE_MESH_URI},
            retries_needed=ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES + 5,  # never clears
            retry_after_ms=1,
        )
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server, updates
        finally:
            stop()

    server, updates = run(body)
    assert updates[-1].state == "finished"
    assert updates[-1].failed == ((_RESOLVABLE_MESH_URI, "upload_failed", None),)
    # Bounded: the first attempt plus exactly the retry ceiling, never more.
    assert server.attempts[_RESOLVABLE_MESH_URI] == ASSET_UPLOAD_RATE_LIMIT_MAX_RETRIES + 1


def test_a_connection_failure_is_not_retried_as_a_rate_limit():
    """The retry deliberately does not apply to a timeout or connection failure —
    only a `429` — so an unreachable cloud still fails on the first
    attempt, unchanged, rather than compounding a hung connection with a
    retry sleep on top of it."""
    with mock.patch(
        "fleetless_bridge.ros_runtime.urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ) as mocked:
        result = RosRuntime._upload_asset_bytes(
            "http://127.0.0.1:1/api/bridge/assets", "tok", "sync-1", "mesh",
            _RESOLVABLE_MESH_URI, "application/octet-stream", b"data",
        )
    assert result.ok is False
    assert mocked.call_count == 1  # exactly one attempt — no retry on this path


def test_upload_asset_bytes_sends_both_headers_and_a_cjk_name_now_succeeds():
    """Redesigned once already (reading the contract): TWO
    headers, not one reinterpreted. `name` keeps its original meaning — a
    Latin-1-safe, lossy best-effort when the real name doesn't fit —
    and `nameEncoded` carries the exact percent-encoded truth alongside
    it. Proves both, against a real HTTP server, not a mock of the
    encoding path: the actual claim is about what reaches the wire."""
    name = "textures/日本語.png"

    def body():
        server, url, stop = _start_asset_upload_server()
        try:
            result = RosRuntime._upload_asset_bytes(url, "tok", "sync-1", "texture", name, "image/png", b"data")
            return result, server.received
        finally:
            stop()

    result, received = body()
    assert result.ok is True
    assert len(received) == 1
    record = received[0]

    # `name`: Latin-1-safe, lossy — '?' for each character outside
    # Latin-1, never the original string and never an exception.
    raw_name = record["headers"]["X-Fleetless-Asset-Name-Raw"]
    raw_name.encode("latin-1")  # raises if this is not Latin-1-safe
    assert raw_name == "textures/???.png"

    # `nameEncoded`: the exact truth, percent-encoded — plain ASCII, and
    # byte-identical to what `encodeURIComponent` would produce.
    raw_encoded = record["headers"]["X-Fleetless-Asset-Name-Encoded-Raw"]
    raw_encoded.encode("ascii")  # raises if this is not pure ASCII
    assert urllib.parse.quote(name, safe="") == raw_encoded

    # A store that prefers `nameEncoded` when present (the cloud's own
    # rule, contracts fcdb317) recovers the developer's own string exactly.
    assert record["headers"]["X-Fleetless-Asset-Name"] == name


def test_upload_asset_bytes_sends_the_original_name_verbatim_when_it_is_latin1_safe():
    """The common case, proven separately from the lossy one: an ordinary
    ASCII name is not rewritten at all — `name` and the decoded
    `nameEncoded` are byte-identical to what the caller passed in, and
    `name` is not put through the lossy substitution unnecessarily."""
    name = "textures/skin.png"

    def body():
        server, url, stop = _start_asset_upload_server()
        try:
            result = RosRuntime._upload_asset_bytes(url, "tok", "sync-1", "texture", name, "image/png", b"data")
            return result, server.received
        finally:
            stop()

    result, received = body()
    assert result.ok is True
    record = received[0]
    assert record["headers"]["X-Fleetless-Asset-Name-Raw"] == name
    assert record["headers"]["X-Fleetless-Asset-Name"] == name


def test_upload_asset_bytes_unicode_error_guard_still_returns_false_not_raise():
    """The original guard, kept as a backstop (its own docstring says it
    should simply stop firing for a real caller, since every name this
    bridge sends is percent-encoded before it reaches `urllib`) — proven
    directly by forcing the path it exists for, the same way the
    connection-failure test above forces `URLError` rather than waiting
    for a real one. If `quote` were ever bypassed, this is what stops a
    single bad name from taking the rest of the sync down with it."""
    with mock.patch(
        "fleetless_bridge.ros_runtime.urllib.request.urlopen",
        side_effect=UnicodeEncodeError("latin-1", "x", 0, 1, "ordinal not in range(256)"),
    ) as mocked:
        result = RosRuntime._upload_asset_bytes(
            "http://127.0.0.1:1/api/bridge/assets", "tok", "sync-1", "texture",
            "textures/skin.png", "image/png", b"data",
        )
    assert result.ok is False
    assert mocked.call_count == 1  # not retried — the same name fails the same way every time


# --- the inside of a `.dae` -----------------------------------------
#
# A `.dae`'s own `<init_from>` image references have never been looked at by
# this platform before. A fake package (not a real installed one — the
# suite's own rule elsewhere is to prefer a real ament package with real
# files, but there is no real installed package with a `.dae` in it, so this
# is the one place that patches `get_package_share_directory` instead)
# backing a real temp directory, so real containment logic runs against
# real files on disk, not a mock of the filesystem itself.

_DAE_PACKAGE = "test_dae_pkg"


def _fake_share_dir_for(tmp_path):
    def fake_get_share_dir(name):
        if name == _DAE_PACKAGE:
            return tmp_path
        raise PackageNotFoundError(name)

    return mock.patch(
        "fleetless_bridge.ros_runtime.get_package_share_directory", side_effect=fake_get_share_dir
    )


def _dae_with_init_from(*refs):
    images = "".join(
        '<image id="img{}"><init_from>{}</init_from></image>'.format(i, ref)
        for i, ref in enumerate(refs)
    )
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<COLLADA xmlns="http://www.collada.org/2005/11/COLLADASchema" version="1.4.1">'
        "<library_images>{}</library_images>"
        "</COLLADA>"
    ).format(images)


def test_run_asset_sync_uploads_a_dae_internal_texture_and_totals_it_upfront():
    """The happy path: the *first* progress frame this sync ever sends already
    counts the internal texture — not a
    total that grows once the `.dae` has been opened."""
    urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="package://{pkg}/meshes/arm.dae"/></geometry></visual>
  </link>
</robot>""".format(pkg=_DAE_PACKAGE)
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    texture_name = "package://{}/meshes/textures/skin.png".format(_DAE_PACKAGE)
    texture_bytes = b"not a real PNG, but real bytes"

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "textures"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            # Two references to the same texture, within one .dae — proves
            # the within-file dedup as a side effect of the happy path.
            f.write(_dae_with_init_from("textures/skin.png", "textures/skin.png"))
        with open(os.path.join(tmp_path, "meshes", "textures", "skin.png"), "wb") as f:
            f.write(texture_bytes)

        async def body(rt):
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # total=3 (urdf, the .dae mesh, its one deduped texture) from the very
    # first frame — not 2 growing to 3.
    assert updates[0].total == 3
    assert all(u.total == 3 for u in updates)
    assert updates[-1].state == "finished"
    assert updates[-1].done == 3
    assert updates[-1].failed == ()

    by_name = {r["headers"]["X-Fleetless-Asset-Name"]: r for r in received}
    assert set(by_name) == {URDF_ASSET_NAME, dae_uri, texture_name}
    assert by_name[dae_uri]["headers"]["X-Fleetless-Asset-Kind"] == "mesh"
    texture_record = by_name[texture_name]
    assert texture_record["headers"]["X-Fleetless-Asset-Kind"] == "texture"
    assert texture_record["headers"]["X-Fleetless-Sync-Id"] == "sync-1"
    assert texture_record["body"] == texture_bytes
    # Exactly one upload for the texture, despite two <init_from>s naming it.
    assert sum(1 for r in received if r["headers"]["X-Fleetless-Asset-Name"] == texture_name) == 1


# --- the contained-but-not-normal case -----------
#
# The happy-path test above and the browser fixture both hardcoded
# `textures/skin.png` — the one shape that is already normal,
# so nothing anywhere exercised the naming rule's actual job: normalizing a
# reference that *isn't*. `./`, `../` from a subdirectory back to a sibling,
# and a double `a/../b/` are all ordinary exporter output (Blender writes
# `./` routinely), and each of the three asserts the produced *name*, not
# merely that something uploaded — the name is the contract, and it is what
# three.js's URL modifier matches on.


def test_run_asset_sync_normalizes_a_dot_slash_dae_internal_reference():
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    expected_name = "package://{}/meshes/textures/skin.png".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "textures"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            f.write(_dae_with_init_from("./textures/skin.png"))
        with open(os.path.join(tmp_path, "meshes", "textures", "skin.png"), "wb") as f:
            f.write(b"texture bytes")

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert updates[-1].failed == ()
    assert expected_name in {r["headers"]["X-Fleetless-Asset-Name"] for r in received}


def test_run_asset_sync_normalizes_a_dae_internal_reference_from_a_subdirectory():
    """A `.dae` one level down (`meshes/sub/part.dae`) referencing a
    texture *sibling to its parent directory* via `../textures/skin.png` —
    the standard ROS layout (a `.dae` nested under `meshes/`, a shared
    `textures/` beside it) is exactly this shape, not a contrived one."""
    dae_uri = "package://{}/meshes/sub/part.dae".format(_DAE_PACKAGE)
    expected_name = "package://{}/meshes/textures/skin.png".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "sub"))
        os.makedirs(os.path.join(tmp_path, "meshes", "textures"))
        with open(os.path.join(tmp_path, "meshes", "sub", "part.dae"), "w") as f:
            f.write(_dae_with_init_from("../textures/skin.png"))
        with open(os.path.join(tmp_path, "meshes", "textures", "skin.png"), "wb") as f:
            f.write(b"texture bytes")

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert updates[-1].failed == ()
    assert expected_name in {r["headers"]["X-Fleetless-Asset-Name"] for r in received}


def test_run_asset_sync_normalizes_a_double_indirection_dae_internal_reference():
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    expected_name = "package://{}/meshes/b/skin.png".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "b"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            f.write(_dae_with_init_from("a/../b/skin.png"))
        with open(os.path.join(tmp_path, "meshes", "b", "skin.png"), "wb") as f:
            f.write(b"texture bytes")

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert updates[-1].failed == ()
    assert expected_name in {r["headers"]["X-Fleetless-Asset-Name"] for r in received}


def test_run_asset_sync_refuses_a_dae_internal_traversal():
    """The same containment check as `package://` resolution, applied to a
    `.dae`'s own internal references. The mesh itself is unaffected — the
    escaping reference is refused, never uploaded or read, but no longer
    *silent*: named in `failed` the same way any other unresolvable
    reference is, so a developer whose `.dae` has a typo'd internal
    reference has something to act on. The name in `failed` says nothing
    about *why* it failed — the same value would appear for a reference
    that stayed inside the package but pointed at a file that simply does
    not exist."""
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    internal_ref = "../" * 20 + "etc/passwd"
    # The exact name the implementation computes — same join+normalize it
    # uses, not a hand-typed guess at the resulting string.
    expected_failed_name = "package://{}/{}".format(
        _DAE_PACKAGE, posixpath.normpath(posixpath.join("meshes", internal_ref))
    )

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            # Enough `../` to reach the filesystem root from any real temp
            # directory depth — realpath clamps at `/`, so exact depth
            # doesn't matter, unlike the URI-level traversal tests above
            # which compute it precisely for a *package://* URI's own
            # containment check (this is the same check, reached from an
            # internal reference instead).
            f.write(_dae_with_init_from(internal_ref))

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # Only the URDF and the .dae mesh itself were ever uploaded — the
    # escaping reference is named in `failed` below, never requested.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME, dae_uri}
    assert not any(b"root:" in r["body"] for r in received)
    assert updates[-1].total == 3  # urdf + mesh + the (failed) reference — no longer silently dropped
    assert updates[-1].done == 3
    assert updates[-1].failed == ((expected_failed_name, "unresolvable", None),)


def test_run_asset_sync_reports_a_dae_internal_reference_to_a_missing_file_as_failed():
    """The honest-mistake case, distinct from the traversal above: a
    reference that stays *inside* the package (no escape) but names a file
    that genuinely is not there — a typo, or a
    texture that simply was never added to the workspace. Before this
    fix, this reference could not appear in `urdfCompleteness.missing`
    (nothing in the URDF itself names it) and was dropped here too — a
    developer got a *successful* sync with a texture silently absent and
    no way to discover why. It must produce the identical `failed` shape
    the traversal case does, not a different one — that sameness is the
    whole point: `failed` cannot be used to tell an attack from a typo."""
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    expected_failed_name = "package://{}/meshes/textures/does_not_exist.png".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))  # no textures/ subdirectory created at all
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            f.write(_dae_with_init_from("textures/does_not_exist.png"))

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME, dae_uri}
    assert updates[-1].total == 3
    assert updates[-1].done == 3
    # Same shape the traversal case produces — the `kind` field makes that sameness
    # explicit: both are `unresolvable`, and `failed` cannot be used to
    # tell an attack from a typo.
    assert updates[-1].failed == ((expected_failed_name, "unresolvable", None),)


def test_run_asset_sync_uploads_a_dae_with_no_internal_textures_normally():
    dae_uri = "package://{}/meshes/plain.dae".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))
        with open(os.path.join(tmp_path, "meshes", "plain.dae"), "w") as f:
            f.write(_dae_with_init_from())  # no <image> elements at all

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    return await _drain_asset_progress_until(rt, {"finished"})
            finally:
                stop()

        updates = run(body)

    assert updates[-1].total == 2  # urdf + the mesh, no textures
    assert updates[-1].failed == ()


def test_run_asset_sync_still_uploads_the_mesh_when_its_dae_is_malformed():
    dae_uri = "package://{}/meshes/broken.dae".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))
        with open(os.path.join(tmp_path, "meshes", "broken.dae"), "w") as f:
            f.write("<COLLADA><library_images>")  # unclosed tags

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    assert updates[-1].total == 2
    assert updates[-1].failed == ()  # the mesh itself still uploaded fine
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME, dae_uri}


def test_run_asset_sync_skips_a_dae_internal_reference_carrying_its_own_scheme():
    """`<init_from>http://...</init_from>` (or `data:`, `file://`, ...) is
    not a package-relative reference — skipped, not resolved, and not a
    reason for the mesh itself to fail."""
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            f.write(_dae_with_init_from("http://example.com/tex.png", "data:image/png;base64,AAAA"))

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    return await _drain_asset_progress_until(rt, {"finished"})
            finally:
                stop()

        updates = run(body)

    assert updates[-1].total == 2  # neither reference was ever counted
    assert updates[-1].failed == ()


def test_run_asset_sync_dedupes_a_shared_texture_across_two_daes():
    """Two sibling `.dae`s in the same directory, both referencing the
    same relative texture — a real shape (a shared atlas across an arm and
    forearm mesh, say) — normalise to the identical name and must cost one
    upload and one `total` slot, not two."""
    dae_uri_1 = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    dae_uri_2 = "package://{}/meshes/forearm.dae".format(_DAE_PACKAGE)
    texture_name = "package://{}/meshes/textures/shared.png".format(_DAE_PACKAGE)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "textures"))
        for filename in ("arm.dae", "forearm.dae"):
            with open(os.path.join(tmp_path, "meshes", filename), "w") as f:
                f.write(_dae_with_init_from("textures/shared.png"))
        with open(os.path.join(tmp_path, "meshes", "textures", "shared.png"), "wb") as f:
            f.write(b"shared atlas bytes")

        async def body(rt):
            urdf_with_two_daes = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="l1"><visual><geometry><mesh filename="{}"/></geometry></visual></link>
  <link name="l2"><visual><geometry><mesh filename="{}"/></geometry></visual></link>
</robot>""".format(dae_uri_1, dae_uri_2)
            await _publish_urdf_and_wait(rt, text=urdf_with_two_daes)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri_1, dae_uri_2))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # urdf + 2 meshes + 1 (deduped) shared texture = 4, not 5.
    assert updates[-1].total == 4
    assert updates[-1].done == 4
    assert updates[-1].failed == ()
    assert sum(1 for r in received if r["headers"]["X-Fleetless-Asset-Name"] == texture_name) == 1


def test_run_asset_sync_uploads_a_cjk_dae_texture_name_through_the_real_encoding_path():
    """This is the test that would have caught the encoding defect
    originally — a real `.dae` with a CJK-named internal
    texture, through the real encoding path, not a mock of it.

    Before the redesign, this exact scenario (reproduced through the real sync path
    rather than `_upload_asset_bytes` in isolation) took the *whole sync*
    down — the CJK name's `UnicodeEncodeError` propagated past `_upload_
    mesh_file` into `_run_asset_sync`'s exception guard, marking the
    mesh and the other, perfectly-ordinary texture failed too, not just
    the one unrepresentable name. Now: nothing in the sync is affected,
    and the CJK texture lands under the developer's own name, not a
    percent-escaped one — `asset.name` (what the cloud stores) must never
    be the wire encoding."""
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    ok_texture_name = "package://{}/meshes/textures/skin.png".format(_DAE_PACKAGE)
    cjk_texture_name = "package://{}/meshes/textures/日本語.png".format(_DAE_PACKAGE)
    cjk_texture_bytes = b"once unreachable by this name; now ordinary"

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "textures"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            f.write(_dae_with_init_from("textures/skin.png", "textures/日本語.png"))
        with open(os.path.join(tmp_path, "meshes", "textures", "skin.png"), "wb") as f:
            f.write(b"ok texture bytes")
        with open(os.path.join(tmp_path, "meshes", "textures", "日本語.png"), "wb") as f:
            f.write(cjk_texture_bytes)

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # urdf + mesh + 2 textures = 4, and nothing failed — the CJK name did
    # not cost anything else in the sync, and didn't fail itself either.
    assert updates[-1].total == 4
    assert updates[-1].done == 4
    assert updates[-1].failed == ()

    by_name = {r["headers"]["X-Fleetless-Asset-Name"]: r for r in received}
    assert set(by_name) == {URDF_ASSET_NAME, dae_uri, ok_texture_name, cjk_texture_name}
    cjk_record = by_name[cjk_texture_name]
    # Two headers, not one reinterpreted: `name` itself
    # carries only a lossy Latin-1 substitute, never the real string —
    # `nameEncoded` is what actually recovers it, and did.
    assert cjk_record["headers"]["X-Fleetless-Asset-Name-Raw"] != cjk_texture_name
    assert urllib.parse.unquote(cjk_record["headers"]["X-Fleetless-Asset-Name-Encoded-Raw"]) == cjk_texture_name
    assert cjk_record["body"] == cjk_texture_bytes


# --- bounding failed against a hostile `.dae` -----------------------


def test_run_asset_sync_refuses_a_dae_with_too_many_internal_references():
    """An unresolvable `.dae`-internal reference turned `failed` from a
    list bounded by the workspace's real files into one bounded only by a
    `.dae`'s own text — a 2 MB `.dae` with 17,331 references produced a
    terminal frame 32 bytes over `MAX_WS_
    PAYLOAD_BYTES`, closing the robot's own socket mid-sync. Patches the
    per-file ceiling down to 3 rather than actually constructing thousands
    of `<image>` elements — the mechanism is the same at any threshold.
    Every one of the five references stays unresolvable (missing files) so
    a passing test can't be mistaken for "they all happened to resolve"."""
    dae_uri = "package://{}/meshes/arm.dae".format(_DAE_PACKAGE)
    refs = ["textures/img{}.png".format(i) for i in range(5)]
    expected_sentinel = (
        "{} references too many internal images (>3) — refused, not enumerated"
    ).format(dae_uri)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))
        with open(os.path.join(tmp_path, "meshes", "arm.dae"), "w") as f:
            f.write(_dae_with_init_from(*refs))

        async def body(rt):
            urdf_with_dae = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual><geometry><mesh filename="{}"/></geometry></visual>
  </link>
</robot>""".format(dae_uri)
            await _publish_urdf_and_wait(rt, text=urdf_with_dae)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path), mock.patch(
                    "fleetless_bridge.ros_runtime.DAE_MAX_INTERNAL_REFERENCES_PER_FILE", 3
                ):
                    await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # urdf + mesh + exactly ONE sentinel — not 2 + 5 individual references.
    assert updates[-1].total == 3
    assert updates[-1].done == 3
    # `refused`: never attempted, not confirmed missing — the ceiling
    # fired before any of the five references was individually examined.
    assert updates[-1].failed == ((expected_sentinel, "refused", None),)
    # The mesh itself is unaffected — still requested and uploaded, only its
    # internal references were refused as a block.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME, dae_uri}


def test_run_asset_sync_caps_dae_references_across_the_whole_sync():
    """The second ceiling: several `.dae`s, each individually under the
    per-file limit, whose *combined* reference count would still be
    unbounded if only the per-file cap existed. The first `.dae`'s
    references are processed normally (its own budget isn't exceeded); the
    sync-wide budget then runs out partway through the second `.dae`, which
    gets one sentinel for whatever was left unenumerated — and a *third*
    `.dae` after that is never even parsed for internal references, though
    its own mesh still uploads."""
    dae_uri_1 = "package://{}/meshes/one.dae".format(_DAE_PACKAGE)
    dae_uri_2 = "package://{}/meshes/two.dae".format(_DAE_PACKAGE)
    dae_uri_3 = "package://{}/meshes/three.dae".format(_DAE_PACKAGE)
    texture_1 = "package://{}/meshes/textures/a.png".format(_DAE_PACKAGE)
    texture_2 = "package://{}/meshes/textures/b.png".format(_DAE_PACKAGE)
    expected_sentinel = (
        "this sync's .dae-internal image references exceeded the "
        "2-entry sync limit at {} — the rest were not enumerated"
    ).format(dae_uri_2)

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes", "textures"))
        with open(os.path.join(tmp_path, "meshes", "one.dae"), "w") as f:
            f.write(_dae_with_init_from("textures/a.png"))  # 1 ref: fits the budget
        with open(os.path.join(tmp_path, "meshes", "textures", "a.png"), "wb") as f:
            f.write(b"texture a bytes")
        with open(os.path.join(tmp_path, "meshes", "two.dae"), "w") as f:
            # 2 refs: the first pushes the running total to the 2-entry
            # budget exactly, the second is where the cutoff fires.
            f.write(_dae_with_init_from("textures/b.png", "textures/c.png"))
        with open(os.path.join(tmp_path, "meshes", "textures", "b.png"), "wb") as f:
            f.write(b"texture b bytes")
        with open(os.path.join(tmp_path, "meshes", "three.dae"), "w") as f:
            f.write(_dae_with_init_from("textures/d.png"))  # never even parsed

        async def body(rt):
            urdf_with_three_daes = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="l1"><visual><geometry><mesh filename="{}"/></geometry></visual></link>
  <link name="l2"><visual><geometry><mesh filename="{}"/></geometry></visual></link>
  <link name="l3"><visual><geometry><mesh filename="{}"/></geometry></visual></link>
</robot>""".format(dae_uri_1, dae_uri_2, dae_uri_3)
            await _publish_urdf_and_wait(rt, text=urdf_with_three_daes)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path), mock.patch(
                    "fleetless_bridge.ros_runtime.DAE_MAX_INTERNAL_REFERENCES_PER_SYNC", 2
                ), mock.patch(
                    "fleetless_bridge.ros_runtime.DAE_MAX_INTERNAL_REFERENCES_PER_FILE", 10
                ):
                    await rt.sync_assets(
                        "sync-1", url, "upload-tok", (dae_uri_1, dae_uri_2, dae_uri_3)
                    )
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # urdf + 3 meshes + texture_1 (from dae 1, under budget) + texture_2
    # (from dae 2, the item that reaches the budget exactly) + one sentinel
    # (dae 2's second reference, where the cutoff fires) = 7. dae 3's own
    # internal reference is never enumerated at all — not counted, not
    # failed, simply never looked at.
    assert updates[-1].total == 7
    assert updates[-1].done == 7
    assert updates[-1].failed == ((expected_sentinel, "refused", None),)

    uploaded_names = {r["headers"]["X-Fleetless-Asset-Name"] for r in received}
    assert uploaded_names == {
        URDF_ASSET_NAME, dae_uri_1, dae_uri_2, dae_uri_3, texture_1, texture_2,
    }


# --- bounding failed's mesh-level half too --------------------
#
# The `.dae`-internal ceiling bounded one half of `failed` and left the
# mesh-level half — a `<mesh>`/`<texture>` URI straight off the URDF —
# uncapped. A URDF with more than 1000 unresolvable `package://` references
# produced a terminal frame the
# contract's own `failed.max(1000)` then refused outright: `parseBridgeFrame`
# silently drops an oversized/invalid frame, the sync sits untouched, and
# the idle timer ends it 120s later reporting "no response from the
# robot" — a false cause, in the exact field built so causes
# would stop being false. The robot answered; the cloud's own schema
# refused the answer.


def test_run_asset_sync_caps_mesh_level_failures_across_the_whole_sync():
    """The mesh-level mirror of `test_run_asset_sync_caps_dae_references_
    across_the_whole_sync` above: five mesh-level references that all fail
    to resolve, a ceiling patched down to 3 — the first three land
    individually, the rest collapse into one `refused` sentinel naming the
    ceiling, exactly the shape used one layer out."""
    unresolvable_uris = [
        "package://example_interfaces/does/not/exist-{}.stl".format(i) for i in range(5)
    ]
    expected_sentinel = (
        "more than 3 mesh-level references failed to resolve or upload in "
        "this sync — additional failures are not listed individually"
    )

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            with mock.patch("fleetless_bridge.ros_runtime.MESH_MAX_FAILED_PER_SYNC", 3):
                await rt.sync_assets("sync-1", url, "upload-tok", tuple(unresolvable_uris))
                updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)

    # urdf + 5 requested meshes = 6, all "done" even though most failed —
    # done counts attempts, not successes.
    assert updates[-1].total == 6
    assert updates[-1].done == 6
    assert updates[-1].failed == (
        (unresolvable_uris[0], "unresolvable", None),
        (unresolvable_uris[1], "unresolvable", None),
        (unresolvable_uris[2], "unresolvable", None),
        (expected_sentinel, "refused", None),
    )
    # Nothing but the URDF itself ever uploaded — every mesh reference was
    # unresolvable from the start.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME}


def test_run_asset_sync_exception_guard_shares_the_mesh_failure_budget():
    """The ordinary per-mesh loop and the exception guard's catch-all are
    two different code paths that can each add to `failed` — proves they draw
    on *one* shared counter rather than each getting its own, which would let
    the two together
    still exceed the cap the ordinary path alone respects. Two mesh
    references fail to resolve normally (2 of the budget's 3 spent before
    any exception fires); a third, resolvable, mesh then blows up inside
    `_upload_mesh_file`, and the exception guard's catch-all has to account
    for it *and* a fourth mesh that was never even attempted — using only
    the one slot the ordinary path left behind."""
    unresolvable_uris = [
        "package://example_interfaces/does/not/exist-{}.stl".format(i) for i in range(2)
    ]
    resolvable_uris = [_RESOLVABLE_MESH_URI, _RESOLVABLE_TEXTURE_URI]
    expected_sentinel = (
        "more than 3 mesh-level references failed to resolve or upload in "
        "this sync — additional failures are not listed individually"
    )

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            with mock.patch(
                "fleetless_bridge.ros_runtime.MESH_MAX_FAILED_PER_SYNC", 3
            ), mock.patch.object(
                RosRuntime, "_upload_mesh_file", side_effect=RuntimeError("boom")
            ):
                await rt.sync_assets(
                    "sync-1", url, "upload-tok", tuple(unresolvable_uris + resolvable_uris)
                )
                updates = await _drain_asset_progress_until(rt, {"finished", "refused_busy"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)

    # urdf + 4 requested meshes = 5; the exception guard sets done = total
    # for everything it never got a definite answer for.
    assert updates[-1].state == "finished"
    assert updates[-1].total == 5
    assert updates[-1].done == 5
    assert updates[-1].failed == (
        (unresolvable_uris[0], "unresolvable", None),
        (unresolvable_uris[1], "unresolvable", None),
        (resolvable_uris[0], "refused", None),
        (expected_sentinel, "refused", None),
    )
    # Only the URDF ever uploaded — the exception fired on the very first
    # resolvable mesh, before its own upload could be counted as sent, and
    # the second resolvable mesh was never even attempted.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME}


def test_dae_per_file_sentinels_share_the_sync_wide_reference_ceiling():
    """A re-derivation of the arithmetic, measured rather than assumed.
    `DAE_MAX_INTERNAL_REFERENCES_PER_SYNC` reads as bounding the
    `.dae`-internal half at 500, but the per-file branch (`if len(pairs) >
    DAE_MAX_INTERNAL_REFERENCES_PER_FILE`) grew `sync_reference_count`
    without ever checking it against that ceiling, and never set
    `sync_budget_exhausted` either — so a sync's budget spent entirely on
    over-large `.dae` files, rather than on many references inside one, was
    never noticed. Reproduced before
    the fix: 5 `.dae` files each over a per-file limit of 3, with the
    sync-wide cap patched to 2, produced 5 independent sentinels — the
    exact shape `record_failure` was built to close one layer out, left
    open one layer in. The fix routes both the per-file branch and a
    fresh `.dae`'s own pre-check through the same `sync_overflow_sentinel`
    the per-reference loop already used, so the third `.dae` here (also
    over its own per-file limit) gets the sync-wide overflow sentinel
    instead of its own, and the fourth and fifth are never even parsed."""
    n_daes = 5
    dae_uris = ["package://{}/meshes/d{}.dae".format(_DAE_PACKAGE, i) for i in range(n_daes)]
    refs_per_file = ["textures/img{}.png".format(i) for i in range(4)]  # 4 > per-file limit of 3
    expected_per_file_sentinel = (
        "{} references too many internal images (>3) — refused, not enumerated"
    )
    expected_overflow_sentinel = (
        "this sync's .dae-internal image references exceeded the 2-entry "
        "sync limit at {} — the rest were not enumerated"
    ).format(dae_uris[2])

    with tempfile.TemporaryDirectory() as tmp_path:
        os.makedirs(os.path.join(tmp_path, "meshes"))
        for i, uri in enumerate(dae_uris):
            with open(os.path.join(tmp_path, "meshes", "d{}.dae".format(i)), "w") as f:
                f.write(_dae_with_init_from(*refs_per_file))

        links = "".join(
            '<link name="l{}"><visual><geometry><mesh filename="{}"/></geometry></visual></link>'
            .format(i, uri)
            for i, uri in enumerate(dae_uris)
        )
        urdf = '<?xml version="1.0"?><robot name="test_robot">{}</robot>'.format(links)

        async def body(rt):
            await _publish_urdf_and_wait(rt, text=urdf)
            server, url, stop = _start_asset_upload_server()
            try:
                with _fake_share_dir_for(tmp_path), mock.patch(
                    "fleetless_bridge.ros_runtime.DAE_MAX_INTERNAL_REFERENCES_PER_FILE", 3
                ), mock.patch(
                    "fleetless_bridge.ros_runtime.DAE_MAX_INTERNAL_REFERENCES_PER_SYNC", 2
                ):
                    await rt.sync_assets("sync-1", url, "upload-tok", tuple(dae_uris))
                    updates = await _drain_asset_progress_until(rt, {"finished"})
                return server.received, updates
            finally:
                stop()

        received, updates = run(body)

    # urdf + 5 meshes + 3 sentinels (d0's own, d1's own, d2's shared
    # overflow one) = 9; d3 and d4 contribute no sentinel of their own,
    # since they are never even parsed.
    assert updates[-1].total == 9
    assert updates[-1].done == 9
    # Exactly 3 entries, not 5 -- d0 and d1 spend the 2-entry budget one
    # sentinel each, d2 hits the ceiling before it is even parsed and gets
    # the shared overflow sentinel instead of its own, d3 and d4 are never
    # examined at all (sync_budget_exhausted short-circuits them).
    assert updates[-1].failed == (
        (expected_per_file_sentinel.format(dae_uris[0]), "refused", None),
        (expected_per_file_sentinel.format(dae_uris[1]), "refused", None),
        (expected_overflow_sentinel, "refused", None),
    )
    # Every mesh itself still uploaded -- only the internal-reference scan
    # was refused, never the mesh file the scan runs against.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {
        URDF_ASSET_NAME, *dae_uris,
    }


def test_terminal_frame_stays_schema_valid_at_the_combined_ceiling():
    """The half nobody checked the first time: the `.dae`-internal ceiling
    was proven to produce a schema-valid `failed` list, but the two ceilings
    were never proven
    *together* stay under the contract's `failed.max(1000)` — a bound that
    can be exceeded by adding the other bound to it is not a bound. Builds
    `failed` at exactly the documented worst case, using the *production*
    constants rather than ones patched down for speed — 1 (the URDF's own
    entry) + `MESH_MAX_FAILED_PER_SYNC` individual mesh-level entries + 1
    mesh-level overflow sentinel + `DAE_MAX_INTERNAL_REFERENCES_PER_SYNC`
    dae-internal entries + 1 dae-level overflow sentinel (the true worst
    case — see `sync_overflow_sentinel`: the per-file
    branch used to grow past this ceiling unchecked, so the true worst
    case was one entry short of what this test now builds) — and validates
    the actual wire JSON `bridge_asset_progress_message` produces against
    the vendored schema. Not that the count is right: that the thing
    actually crosses the wire."""
    failed = [(URDF_ASSET_NAME, "upload_failed", None)]
    failed.extend(
        ("package://pkg/mesh-{}.stl".format(i), "unresolvable", None)
        for i in range(MESH_MAX_FAILED_PER_SYNC)
    )
    failed.append((
        "more than {} mesh-level references failed to resolve or upload "
        "in this sync — additional failures are not listed individually"
        .format(MESH_MAX_FAILED_PER_SYNC),
        "refused",
        None,
    ))
    failed.extend(
        ("package://pkg/part-{}.dae#texture-{}.png".format(i // 10, i), "unresolvable", None)
        for i in range(DAE_MAX_INTERNAL_REFERENCES_PER_SYNC)
    )
    failed.append((
        "this sync's .dae-internal image references exceeded the {}-entry "
        "sync limit at package://pkg/part-last.dae — the rest were not "
        "enumerated".format(DAE_MAX_INTERNAL_REFERENCES_PER_SYNC),
        "refused",
        None,
    ))
    total = len(failed)

    # The arithmetic the constant's own comment states, checked here
    # rather than only in prose: comfortably under the contract's ceiling.
    assert total == 1 + MESH_MAX_FAILED_PER_SYNC + 1 + DAE_MAX_INTERNAL_REFERENCES_PER_SYNC + 1
    assert total < 1000

    payload = json.loads(
        bridge_asset_progress_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", total, total, failed, "finished"
        )
    )
    validate_frame("bridge-asset-progress", payload)
    assert len(payload["failed"]) == total


def test_sync_assets_reports_progress_frames_with_increasing_done():
    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            return await _drain_asset_progress_until(rt, {"finished"})
        finally:
            stop()

    updates = run(body)
    # One frame per item completed (URDF, then the mesh) — the last one is
    # `state='finished'` directly, not a "running" frame followed by a
    # redundant, identical "finished" one.
    assert [(u.done, u.total, u.state) for u in updates] == [
        (1, 2, "running"),
        (2, 2, "finished"),
    ]
    assert all(u.sync_id == "sync-1" for u in updates)


def test_sync_assets_reports_an_unresolvable_mesh_as_failed_without_uploading_it():
    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets(
                "sync-1", url, "upload-tok",
                ("package://this_package_does_not_exist_xyz/foo.stl",),
            )
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].failed == (
        ("package://this_package_does_not_exist_xyz/foo.stl", "unresolvable", None),
    )
    assert updates[-1].done == updates[-1].total == 2
    # Only the URDF was actually uploaded — no attempt for a URI that never resolved.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME}


def test_sync_assets_reports_a_resolved_but_missing_file_as_failed():
    """The package exists; the file inside it does not — a different failure
    than an unknown package, same honest outcome."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets(
                "sync-1", url, "upload-tok",
                ("package://example_interfaces/no_such_file.stl",),
            )
            return await _drain_asset_progress_until(rt, {"finished"})
        finally:
            stop()

    updates = run(body)
    assert updates[-1].failed == (("package://example_interfaces/no_such_file.stl", "unresolvable", None),)


# --- Security: package:// path traversal ---------------------
#
# `_resolve_package_uri` used to join `relative_path` onto the package's
# share directory with no check that the result stayed inside it.
# `package://ament_index_python/../../../../../etc/passwd` climbed straight
# out and got read and uploaded into the org's asset store — reproduced
# directly, inside this same container image, before the fix landed.
# `/robot_description` is ROS graph input (the same reasoning `_extract_
# mesh_uris`' `defusedxml` choice already states): anything with publish
# access to this domain can put a hostile URI on it, so this is not a
# hypothetical caller.


def _traversal_uri_for(package_name, victim):
    """Builds a `package://` URI that climbs from `package_name`'s real
    share directory to `victim` via `..` segments — the exact shape of the
    original reproduction, computed from the actual installed depth rather than
    a hardcoded number of `../`s, so this stays correct regardless of where
    a given ROS image happens to install the package."""
    share = get_package_share_directory(package_name)
    depth = share.count("/")
    return "package://{}/{}{}".format(package_name, "../" * depth, victim.lstrip("/"))


def test_resolve_package_uri_rejects_a_traversal_that_escapes_the_share_directory():
    uri = _traversal_uri_for("ament_index_python", "/etc/passwd")
    assert RosRuntime._resolve_package_uri(uri) is None


def test_resolve_package_uri_rejects_a_traversal_that_lands_exactly_on_a_real_file():
    """Not just "no crash" — the escaped path genuinely resolves to a real,
    readable file (`/etc/passwd` exists in every image this suite runs in),
    so a resolver that merely tolerated a missing target would still pass
    the test above for the wrong reason. This confirms containment is what
    rejects it, not `os.path.isfile` failing to find anything."""
    victim = "/etc/passwd"
    assert os.path.isfile(victim)  # sanity: the attack target is real
    uri = _traversal_uri_for("ament_index_python", victim)
    assert RosRuntime._resolve_package_uri(uri) is None


def test_a_traversal_uri_fails_the_same_way_as_an_unresolvable_package():
    """A resolver that fails differently for hostile input tells an
    attacker they found something — both must return exactly `None`, the
    same as any other unresolvable URI, with nothing on the wire to tell
    them apart."""
    traversal = RosRuntime._resolve_package_uri(
        _traversal_uri_for("ament_index_python", "/etc/passwd")
    )
    typo = RosRuntime._resolve_package_uri("package://this_package_does_not_exist_xyz/foo.stl")
    assert traversal is None
    assert typo is None


def _symlink_install_layout(tmp_path):
    """Reproduces what `colcon build --symlink-install` actually puts on a
    robot: the share directory is a real directory, and each installed file
    inside it is a **symlink back into the source tree**. Returns the share
    directory to hand to a patched `get_package_share_directory`."""
    source = tmp_path / "src" / "demo_pkg" / "urdf" / "meshes"
    source.mkdir(parents=True)
    real_mesh = source / "part.stl"
    real_mesh.write_bytes(b"solid part\nendsolid part\n")
    share = tmp_path / "install" / "demo_pkg" / "share" / "demo_pkg"
    (share / "urdf" / "meshes").mkdir(parents=True)
    (share / "urdf" / "meshes" / "part.stl").symlink_to(real_mesh)
    return share


def test_resolve_package_uri_accepts_a_colcon_symlink_install(tmp_path):
    """**The symlink-install regression.** A containment check written with
    `os.path.realpath` rejects every file in a `--symlink-install`
    workspace, because each one physically lives in `<ws>/src` while the
    share directory is `<ws>/install`. On a real robot that meant ten meshes that
    were present and readable being reported as *"not found in the robot's
    workspace"* — a false cause, with a remedy the operator cannot act on.
    """
    share = _symlink_install_layout(tmp_path)
    with mock.patch(
        "fleetless_bridge.ros_runtime.get_package_share_directory",
        return_value=str(share),
    ):
        resolved = RosRuntime._resolve_package_uri("package://demo_pkg/urdf/meshes/part.stl")
    assert resolved is not None, "a symlink-installed mesh must resolve"
    assert os.path.isfile(resolved)
    assert open(resolved, "rb").read().startswith(b"solid part")


def test_a_symlink_install_layout_still_refuses_a_traversal(tmp_path):
    """The half that must NOT have been traded away: the same workspace
    shape, and a URI that climbs out of it with `..` is still refused. The
    lexical check collapses `..` without following symlinks, so the attack
    fails closed here exactly as it does on a copied install."""
    share = _symlink_install_layout(tmp_path)
    victim = tmp_path / "secret.txt"
    victim.write_text("do not upload me")
    depth = str(share).count("/")
    uri = "package://demo_pkg/{}{}".format("../" * depth, str(victim).lstrip("/"))
    with mock.patch(
        "fleetless_bridge.ros_runtime.get_package_share_directory",
        return_value=str(share),
    ):
        assert RosRuntime._resolve_package_uri(uri) is None
    assert victim.is_file()  # sanity: the target really exists, so containment is what rejected it


def test_an_absolute_path_cannot_replace_the_share_directory(tmp_path):
    """`os.path.join(base, "/etc/passwd")` discards `base` entirely. The
    lexical check catches it for the same reason it catches `..`: the
    result cannot start with `base`."""
    share = _symlink_install_layout(tmp_path)
    with mock.patch(
        "fleetless_bridge.ros_runtime.get_package_share_directory",
        return_value=str(share),
    ):
        assert RosRuntime._resolve_package_uri("package://demo_pkg//etc/passwd") is None


def test_sync_assets_never_reads_or_uploads_a_traversal_target():
    """The integration proof: not just that `_resolve_package_uri` returns
    `None` in isolation, but that a full `sync_assets` run — the actual path
    a hostile `/robot_description` would reach — never uploads the escaped
    file's bytes. `_upload_mesh_file` reads and uploads in one call, only
    ever invoked when `_resolve_package_uri` returned a path, so "the
    upload server received nothing for this URI" is exactly "the bytes were
    never read", not merely "the upload didn't happen for some other
    reason" — `resolve` and `read+upload` are the only two steps between a
    requested URI and the wire, and this proves neither leaked past
    resolution failing."""
    traversal_uri = _traversal_uri_for("ament_index_python", "/etc/passwd")

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (traversal_uri,))
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].failed == ((traversal_uri, "unresolvable", None),)
    # Only the URDF was uploaded — no request was ever made for the
    # traversal target, under its requested name or any other.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME}
    assert not any(b"root:" in r["body"] for r in received)  # /etc/passwd never left the process


def test_sync_assets_reports_a_server_error_as_failed():
    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server(fail_names={_RESOLVABLE_MESH_URI})
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            return await _drain_asset_progress_until(rt, {"finished"})
        finally:
            stop()

    updates = run(body)
    # `upload_failed`: the mesh resolved; the server refused it.
    assert updates[-1].failed == ((_RESOLVABLE_MESH_URI, "upload_failed", None),)
    assert updates[-1].done == updates[-1].total == 2  # attempted and counted, not skipped


def test_sync_assets_with_no_meshes_requested_uploads_only_the_urdf():
    """Contracts: 'Which URIs to send. Empty means the URDF only' — not
    'nothing was specified, send everything last reported'."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", ())
            updates = await _drain_asset_progress_until(rt, {"finished"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    assert updates[-1].total == 1
    assert len(received) == 1
    assert received[0]["headers"]["X-Fleetless-Asset-Name"] == URDF_ASSET_NAME


def test_sync_assets_with_no_urdf_ever_seen_reports_it_failed():
    async def body(rt):
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", ())
            return await _drain_asset_progress_until(rt, {"finished"})
        finally:
            stop()

    updates = run(body)
    # `upload_failed`: nothing was affirmatively determined absent —
    # a URDF simply hasn't arrived yet, not the same claim `unresolvable`
    # makes about a workspace file.
    assert updates[-1].failed == ((URDF_ASSET_NAME, "upload_failed", None),)
    assert updates[-1].total == 1


def test_a_second_sync_while_one_is_running_is_refused_busy():
    """The bridge's own backstop (contracts: the cloud owns single-flight
    primarily and refuses a concurrent sync first) — this proves the
    backstop itself, for the request that slips past the primary guard."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server(delay_s=1.0)
        try:
            first = asyncio.ensure_future(
                rt.sync_assets("sync-a", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            )
            await asyncio.sleep(0.1)  # let sync-a claim _active_sync_id
            await rt.sync_assets("sync-b", url, "upload-tok", ())
            refusal = await asyncio.wait_for(rt.asset_progress.get(), timeout=5.0)
            await first  # let sync-a finish so the server can be torn down cleanly
            return refusal
        finally:
            stop()

    refusal = run(body)
    assert refusal.sync_id == "sync-b"
    assert refusal.state == "refused_busy"
    assert refusal.done == 0
    assert refusal.total == 1  # sync-b's own request: URDF only, no meshes


def test_a_sync_frees_the_slot_for_the_next_one():
    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-a", url, "upload-tok", ())
            await _drain_asset_progress_until(rt, {"finished"})
            await rt.sync_assets("sync-b", url, "upload-tok", ())
            return await _drain_asset_progress_until(rt, {"finished", "refused_busy"})
        finally:
            stop()

    updates = run(body)
    assert updates[-1].sync_id == "sync-b"
    assert updates[-1].state == "finished"  # not refused — the first sync's slot was freed


# --- a sync that goes wrong still ends -----------------------------
#
# Before this, an exception anywhere inside `_run_asset_sync` propagated out
# as an unretrieved-task exception and nothing else — no terminal `asset_progress` frame, so the
# cloud's own sync-progress state waited on one that would never arrive
# (bounded only by the cloud-side 120s idle timeout, not reported promptly).
# `_active_sync_id` already cleared either way (the `finally` in
# `sync_assets` isn't new), so this is specifically about the frame, not the
# slot — proven separately below.


def test_an_unexpected_exception_mid_sync_still_reports_a_terminal_frame():
    """`_upload_mesh_file` is documented as never raising — this forces the
    genuinely unexpected case the `try` in `_run_asset_sync` exists for, not
    a known failure mode that already degrades to an ordinary `failed`
    entry."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            with mock.patch.object(
                RosRuntime, "_upload_mesh_file", side_effect=RuntimeError("boom")
            ):
                await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
                updates = await _drain_asset_progress_until(rt, {"finished", "refused_busy"})
            return server.received, updates
        finally:
            stop()

    received, updates = run(body)
    # The URDF (attempted before the exception) genuinely uploaded; the mesh
    # never got a definite answer and is reported failed rather than silence.
    assert {r["headers"]["X-Fleetless-Asset-Name"] for r in received} == {URDF_ASSET_NAME}
    assert updates[-1].state == "finished"
    assert updates[-1].done == updates[-1].total == 2
    # `refused`: the exception guard doesn't know whether this item
    # would have resolved, only that it never got a definite answer —
    # "never attempted, nothing known missing" is exactly the meaning
    # this kind already carries, reused deliberately rather than guessing
    # between the other two.
    assert updates[-1].failed == ((_RESOLVABLE_MESH_URI, "refused", None),)


def test_an_exception_before_any_item_is_attempted_still_reports_every_item_failed():
    """The exception guard covers the very first item too — `remaining`
    still names both the URDF and the mesh when nothing was ever attempted."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            with mock.patch.object(
                RosRuntime, "_upload_asset_bytes", side_effect=RuntimeError("boom")
            ):
                await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
                return await _drain_asset_progress_until(rt, {"finished", "refused_busy"})
        finally:
            stop()

    updates = run(body)
    assert updates[-1].state == "finished"
    assert updates[-1].done == updates[-1].total == 2
    assert set(updates[-1].failed) == {
        (URDF_ASSET_NAME, "refused", None), (_RESOLVABLE_MESH_URI, "refused", None),
    }


def test_a_sync_that_raised_still_frees_the_slot_for_the_next_one():
    """The half that was never actually broken (the `finally` in
    `sync_assets` already cleared `_active_sync_id` on any exit), proven
    explicitly alongside the frame guarantee above so the two are not
    conflated: a later sync must not be refused busy just because the
    previous one blew up."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            with mock.patch.object(
                RosRuntime, "_upload_mesh_file", side_effect=RuntimeError("boom")
            ):
                await rt.sync_assets("sync-a", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
                await _drain_asset_progress_until(rt, {"finished", "refused_busy"})
            await rt.sync_assets("sync-b", url, "upload-tok", ())
            return await _drain_asset_progress_until(rt, {"finished", "refused_busy"})
        finally:
            stop()

    updates = run(body)
    assert updates[-1].sync_id == "sync-b"
    assert updates[-1].state == "finished"  # not refused — the failed sync still freed the slot


# --- the announced size, and the two refusals it decides between ---------


def test_every_upload_announces_its_size_before_the_body():
    """The cloud weighs `x-fleetless-asset-size` against the robot's store
    *before* it accepts a body. Without the header nothing is weighed and
    an over-large file comes back as a bare `413` instead of a `409` naming
    the store — so this asserts the header on every upload of a sync, the
    URDF included, and that it agrees with the `Content-Length` beside it.

    Against a real HTTP server, so what is asserted is what a real client
    sent rather than what this test built."""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            await _drain_asset_progress_until(rt, {"finished"})
            return server.received
        finally:
            stop()

    received = run(body)
    assert len(received) == 2, [r["headers"]["X-Fleetless-Asset-Name"] for r in received]
    for record in received:
        announced = record["headers"]["X-Fleetless-Asset-Size"]
        assert announced is not None, record["headers"]["X-Fleetless-Asset-Name"]
        # The same number twice, deliberately: one is read before the body
        # and one describes it, and a gate that disagreed with the body it
        # let through would be worse than no gate.
        assert announced == record["headers"]["Content-Length"]
        assert int(announced) == len(record["body"])


def test_a_bare_413_is_a_refusal_not_a_transfer_failure():
    """A file over the server's own body limit never reaches the store
    gate, so the refusal names nothing. It is still a refusal: `upload_
    failed` would tell a reconciliation the asset is still worth retrying,
    and this one can never fit however many times it is sent."""

    def status_for(record):
        if record["headers"]["X-Fleetless-Asset-Name"] == URDF_ASSET_NAME:
            return 201
        return 413, b""

    async def body(rt):
        await _publish_urdf_and_wait(rt)
        server, url, stop = _start_asset_upload_server()
        server.status_for = status_for
        try:
            await rt.sync_assets("sync-1", url, "upload-tok", (_RESOLVABLE_MESH_URI,))
            return await _drain_asset_progress_until(rt, {"finished"})
        finally:
            stop()

    updates = run(body)
    assert updates[-1].failed == ((_RESOLVABLE_MESH_URI, "refused", None),)


# --- the .dae scan cap ---------------------------------------------------


def test_a_dae_over_the_scan_cap_is_uploaded_without_being_read():
    """`_extract_dae_texture_references` reads the whole file and parses
    it into an ElementTree — a multiple of the file size in memory, and
    nothing the cloud's store has an opinion about. The upload ceiling
    that used to bound it is gone, so `DAE_SCAN_MAX_BYTES` does.

    The claim is that the scan never happens: the mock raises if it is
    called at all, which no amount of "the sync still finished" could
    satisfy by other means. The mesh must still upload — an untextured
    render beats a bridge that died reading the file."""
    dae_uri = "package://robot_description_fixture/enormous.dae"
    with tempfile.NamedTemporaryFile(suffix=".dae") as dae_file:
        dae_file.write(b"x" * 200)
        dae_file.flush()

        with mock.patch.object(
            RosRuntime, "_file_size_or_none", return_value=DAE_SCAN_MAX_BYTES + 1
        ):
            with mock.patch.object(
                RosRuntime, "_extract_dae_texture_references", side_effect=AssertionError(
                    "a .dae over the scan cap must not be read"
                )
            ) as extract_mock:
                with mock.patch.object(
                    RosRuntime, "_upload_mesh_file", return_value=UploadResult(True)
                ) as upload_mock:

                    async def body(rt):
                        await _publish_urdf_and_wait(rt)
                        server, url, stop = _start_asset_upload_server()
                        try:
                            with mock.patch.object(
                                RosRuntime, "_resolve_package_uri", return_value=dae_file.name,
                            ):
                                await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                                return await _drain_asset_progress_until(rt, {"finished"})
                        finally:
                            stop()

                    class _ScanCapHandler(logging.Handler):
                        def __init__(self):
                            super().__init__(level=logging.DEBUG)
                            self.messages = []

                        def emit(self, record):
                            self.messages.append(record.getMessage())

                    watched = logging.getLogger("fleetless_bridge.ros_runtime")
                    handler = _ScanCapHandler()
                    previous = watched.level
                    watched.addHandler(handler)
                    watched.setLevel(logging.DEBUG)
                    try:
                        updates = run(body)
                    finally:
                        watched.removeHandler(handler)
                        watched.setLevel(previous)
                    extract_mock.assert_not_called()
                    upload_mock.assert_called_once()
                    # The operator's only trace of a texture set nobody
                    # looked for: once for the file, naming it and the cap.
                    skipped = [
                        m for m in handler.messages
                        if m.startswith("Not scanning") and dae_uri in m
                    ]
                    assert len(skipped) == 1
                    assert str(DAE_SCAN_MAX_BYTES) in skipped[0]

    assert updates[-1].state == "finished"
    # Not a failure of any reference: this sync determined nothing about
    # what is inside that file, and saying otherwise would name something
    # it never looked at.
    assert updates[-1].failed == ()


def test_a_dae_just_under_the_scan_cap_is_still_scanned():
    """The other side of the boundary, so the cap is a comparison and not
    a switch somebody left off."""
    dae_uri = "package://robot_description_fixture/large.dae"
    with tempfile.NamedTemporaryFile(suffix=".dae") as dae_file:
        dae_file.write(b"x" * 200)
        dae_file.flush()

        with mock.patch.object(
            RosRuntime, "_file_size_or_none", return_value=DAE_SCAN_MAX_BYTES
        ):
            with mock.patch.object(
                RosRuntime, "_extract_dae_texture_references", return_value=[]
            ) as extract_mock:
                with mock.patch.object(
                    RosRuntime, "_upload_mesh_file", return_value=UploadResult(True)
                ):

                    async def body(rt):
                        await _publish_urdf_and_wait(rt)
                        server, url, stop = _start_asset_upload_server()
                        try:
                            with mock.patch.object(
                                RosRuntime, "_resolve_package_uri", return_value=dae_file.name,
                            ):
                                await rt.sync_assets("sync-1", url, "upload-tok", (dae_uri,))
                                return await _drain_asset_progress_until(rt, {"finished"})
                        finally:
                            stop()

                    updates = run(body)
                    extract_mock.assert_called_once()

    assert updates[-1].failed == ()
