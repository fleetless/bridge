# SPDX-License-Identifier: Apache-2.0
"""The camera half of the client: config apply and the snapshot pump, run
against the fake cloud with the same duck-typed `FakeRos` double as
test_client_datapoints.py. Real ROS camera behaviour is ros_runtime.py's own
suite; this file is wire protocol and session plumbing."""
import asyncio
import json

from fake_cloud import FakeCloud, accepts, sequence
from helpers import by_slug, make_client, run_until
from test_client_datapoints import FakeRos, _run_one_exchange

from fleetless_bridge.protocol import snapshot_frame


def test_config_applies_cameras_too():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {}, cameras={})
        return await session.recv_config_applied()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {"type": "config_applied", "version": 1, "ok": True, "errors": []}
    assert fake_ros.applied_camera_calls == [{}]


def test_a_snapshot_is_pumped_out_once_connected():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        wire = snapshot_frame(
            slug="front_cam",
            mime="image/jpeg",
            width=64,
            height=48,
            timestamp_ms=1786400000000,
            image_bytes=b"\xff\xd8fake jpeg bytes",
        )
        fake_ros.snapshots.put_threadsafe(loop, wire)
        return await session.recv_snapshot()

    header, image_bytes = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert header == {
        "type": "snapshot",
        "slug": "front_cam",
        "mime": "image/jpeg",
        "width": 64,
        "height": 48,
        "timestamp_ms": 1786400000000,
    }
    assert image_bytes == b"\xff\xd8fake jpeg bytes"


def test_the_snapshot_pump_and_the_receive_loop_can_send_concurrently_without_corrupting_frames():
    """A config_applied (text, from the receive loop) and a snapshot (binary,
    from the pump) sent back-to-back must both arrive intact — the same
    property test_client_datapoints.py proves for the sample pump, here
    extended to an actual binary frame."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        loop = asyncio.get_event_loop()
        wire = snapshot_frame(
            slug="x", mime="image/jpeg", width=1, height=1, timestamp_ms=1, image_bytes=b"\x00"
        )
        fake_ros.snapshots.put_threadsafe(loop, wire)
        await session.send_config(1, {})
        first = await session.recv_raw()
        second = await session.recv_raw()
        kinds = set()
        for raw in (first, second):
            if isinstance(raw, (bytes, bytearray)):
                kinds.add("snapshot")
            else:
                kinds.add(json.loads(raw)["type"])
        return kinds

    frame_kinds = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert frame_kinds == {"config_applied", "snapshot"}


# --- camera_start / camera_stop dispatch, and the camera_state pump ---------


def test_a_camera_start_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_camera_start(
            "front_cam", "wss://media.fleetless.dev", "room-1", "tok-1", "req-42"
        )
        await asyncio.sleep(0.05)  # give the dispatched coroutine a turn
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.start_live_calls == [
        ("front_cam", "wss://media.fleetless.dev", "room-1", "tok-1", "req-42")
    ]


def test_a_camera_stop_is_dispatched_to_the_ros_runtime():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_camera_stop("front_cam", "req-43")
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.stop_live_calls == [("front_cam", "req-43")]


def test_a_camera_state_is_pumped_out_once_connected():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        from fleetless_bridge.ros_runtime import CameraStateUpdate

        fake_ros.camera_states.put(
            CameraStateUpdate(
                "front_cam", True, None, cause="command", observed_at_ms=1786400000000,
                request_id="req-42",
            )
        )
        return await session.recv_camera_state()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload == {
        "type": "camera_state",
        "slug": "front_cam",
        "publishing": True,
        "error": None,
        "cause": "command",
        "observed_at_ms": 1786400000000,
        "request_id": "req-42",
    }


def test_a_camera_state_error_is_pumped_out_with_its_code_and_message():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        from fleetless_bridge.ros_runtime import CameraStateUpdate

        fake_ros.camera_states.put(
            CameraStateUpdate(
                "front_cam", False, ("live_unavailable", "no route to host"),
                cause="live_lost", observed_at_ms=1786400000000, request_id=None,
            )
        )
        return await session.recv_camera_state()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["publishing"] is False
    assert payload["error"] == {"code": "live_unavailable", "message": "no route to host"}
    assert payload["cause"] == "live_lost"
    # cause: 'live_lost' is unsolicited — it answers no request by
    # definition (the pairing rule the cloud enforces).
    assert payload["request_id"] is None


def test_a_camera_state_answering_a_command_echoes_its_request_id():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        from fleetless_bridge.ros_runtime import CameraStateUpdate

        fake_ros.camera_states.put(
            CameraStateUpdate(
                "front_cam", True, None, cause="command", observed_at_ms=1786400000000,
                request_id="the-attempt-that-answered",
            )
        )
        return await session.recv_camera_state()

    payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert payload["cause"] == "command"
    assert payload["request_id"] == "the-attempt-that-answered"


def test_disconnecting_stops_every_live_camera():
    """A dropped connection must not leave the robot publishing into a
    room nobody can tell it to stop."""
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_camera_start(
            "front_cam", "wss://media.fleetless.dev", "room-1", "tok-1"
        )
        await asyncio.sleep(0.05)
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.stop_all_live_calls == 1


# --- a snapshot that would exceed the socket ceiling must never close it ----


def test_a_snapshot_that_would_exceed_the_socket_ceiling_never_closes_it():
    """Proven through the real pipeline, not one layer in isolation:
    RosRuntime's size-bounded encoding (test_camera.py, test_ros_runtime.py)
    is what keeps a snapshot under the /bridge socket's payload ceiling —
    this test proves the *consequence*, that the connection survives to
    carry a second frame, using a real RosRuntime and a real constrained
    websocket server rather than FakeRos for either. `ws` enforces its
    `max_size` before an oversized frame reaches the application
    (`ws/bridge.ts:443` on the real cloud) and closes with 1009 — if that
    happened here, the second `recv_snapshot()` below would time out or
    raise `ConnectionClosed`, not merely receive a large frame.

    `RosRuntime.start()` owns the whole rclpy lifecycle itself (init through
    shutdown, in `rt.start()`/`rt.stop()`) — no separate `rclpy.init()` here,
    the same rule test_ros_runtime.py's own suite follows, for the same
    reason: a second `rclpy.init()` on the default context would collide
    with the runtime's own."""
    import threading

    import numpy as np
    import rclpy
    from cv_bridge import CvBridge
    from rclpy.executors import MultiThreadedExecutor
    from sensor_msgs.msg import Image

    from fleetless_bridge.protocol import CameraConfig, RosSource
    from fleetless_bridge.ros_runtime import RosRuntime

    # Small enough to run fast and force the encoder's backoff ladder to
    # engage against noisy content — the real constant (contracts
    # d678bd9's SNAPSHOT_MAX_BYTES, 1.5 MiB) is a property of the
    # socket's real 2 MiB ceiling, not of this test.
    max_bytes = 30_000

    async def scenario():
        rt = RosRuntime(node_name="test_socket_survival_rt", snapshot_max_bytes=max_bytes)
        loop = asyncio.get_event_loop()
        rt.start(loop)  # owns rclpy.init() from here on

        pub_node = rclpy.create_node("test_socket_survival_pub")
        pub_executor = MultiThreadedExecutor()
        pub_executor.add_node(pub_node)
        pub_thread = threading.Thread(target=pub_executor.spin, daemon=True)
        pub_thread.start()
        image_pub = pub_node.create_publisher(Image, "/socket_survival_image_raw", 10)
        cv_bridge = CvBridge()
        rng = np.random.default_rng(0)
        noisy = rng.integers(0, 256, size=(200, 200, 3), dtype=np.uint8)

        def publish_tick():
            image_pub.publish(cv_bridge.cv2_to_imgmsg(noisy, encoding="bgr8"))

        pub_timer = pub_node.create_timer(1.0 / 30.0, publish_tick)

        result_box = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            first_header, first_bytes = await session.recv_snapshot()
            assert len(first_bytes) <= max_bytes
            # The claim: a second frame still gets through on the *same*
            # connection — proof it was never closed by the first.
            second_header, second_bytes = await session.recv_snapshot()
            assert len(second_bytes) <= max_bytes
            result_box["done"] = (first_header, second_header)
            await session.drain()

        try:
            async with FakeCloud(behavior, max_size=max_bytes) as cloud:
                client = make_client(cloud, ros=rt)
                client_task = asyncio.ensure_future(client.run())
                await rt.apply_cameras(by_slug(
                    [
                        CameraConfig(
                            slug="front",
                            source=RosSource(topic="/socket_survival_image_raw", type="sensor_msgs/msg/Image"),
                            width=200, height=200, fps=30, bitrate_kbps=500,
                            snapshot_interval_seconds=1,
                        )
                    ]
                ))
                deadline = loop.time() + 10.0
                while "done" not in result_box and loop.time() < deadline:
                    await asyncio.sleep(0.05)
                client.stop()
                await asyncio.wait_for(client_task, timeout=5.0)
            return result_box.get("done")
        finally:
            # Helper-node teardown before rt.stop(), which is what shuts
            # rclpy down — the same order test_ros_runtime.py's own tests
            # use (a `stop_pub()`-style helper always runs before
            # `_with_runtime`'s `rt.stop()`).
            pub_timer.cancel()
            pub_executor.shutdown()
            pub_node.destroy_node()
            pub_thread.join(timeout=5.0)
            rt.stop()

    first_header, second_header = asyncio.run(scenario())
    assert first_header["slug"] == "front"
    assert second_header["slug"] == "front"


# ---: current camera health is re-stated once per session, at hello ----


def test_current_camera_health_is_reported_after_the_first_config():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {}, cameras={})
        await session.recv_config_applied()
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    assert fake_ros.report_current_camera_health_calls == 1


def test_current_camera_health_is_not_re_reported_on_a_later_config_same_session():
    fake_ros = FakeRos()

    async def send_and_recv(session):
        await session.send_config(1, {}, cameras={})
        await session.recv_config_applied()
        await session.send_config(2, {}, cameras={})
        await session.recv_config_applied()
        return True

    _run_one_exchange(send_and_recv, ros=fake_ros)
    # Exactly once per session — steady-state changes stay on the
    # transition-only path (what stops a flood); only a fresh session
    # (a fresh hello_ok) re-states everything unconditionally.
    assert fake_ros.report_current_camera_health_calls == 1


def test_current_camera_health_is_reported_again_after_a_reconnect():
    """Closes the defect where a *cloud* restart — indistinguishable, from
    the bridge's side, from any other dropped connection, a fresh hello_ok
    either way — erases the cloud's own health store with nothing to hand
    it back. One `FakeRos`, reused across two sessions, so the call count
    accumulates across the reconnect the way a real robot's single
    long-lived RosRuntime would."""
    fake_ros = FakeRos()

    async def first_session(session):
        await session.send_config(1, {}, cameras={})
        await session.recv_config_applied()
        await session.close(1000, "")

    async def second_session(session):
        await session.send_config(1, {}, cameras={})
        await session.recv_config_applied()
        await session.drain()

    behavior = sequence(accepts(then=first_session), accepts(then=second_session))

    async def scenario():
        async with FakeCloud(behavior) as cloud:
            client = make_client(cloud, ros=fake_ros)
            await run_until(client, lambda: fake_ros.report_current_camera_health_calls >= 2)

    asyncio.run(scenario())
    assert fake_ros.report_current_camera_health_calls == 2
