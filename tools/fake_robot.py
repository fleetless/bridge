#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A stand-in robot for the end-to-end checks and for manual development: a plain
rclpy node, no fleetless_bridge involved, publishing real ROS traffic for the
bridge to introspect and subscribe to.

- `/battery` (`sensor_msgs/msg/BatteryState`) — `percentage` rides a slow
  triangle wave (0 -> 1 -> 0 over ~20s), so a datapoint on it visibly changes
  over the time it takes to look at a dashboard.
- `/joint_states` (`sensor_msgs/msg/JointState`) — two named joints, each a
  sine wave at a different phase, so a whole-topic datapoint has more than
  one thing moving in it.
- `/cmd_vel` (`geometry_msgs/msg/Twist`) — subscribed, not published: an
  INDEPENDENT observer of whatever lands on the topic a cmd_vel-shaped
  publisher would drive. A failsafe check needs a
  witness that is not the bridge itself and not the test harness reading the
  bridge's internal state — this is that witness, logging every message it
  receives so a human (or a test parsing this process's stdout) can see the
  failsafe actually reach the robot side.
- `count` (`example_interfaces/action/Fibonacci`) — an action server
  slow enough (30 steps, ~1s apart) to invoke, watch feedback arrive, cancel
  by hand, and disconnect the bridge mid-goal and watch it keep running.
- `/add_two_ints` (`example_interfaces/srv/AddTwoInts`) — deliberately slow
  (`FAKE_SERVICE_DELAY_S`, default 2.5s), not merely a trivial call target.
  A service only acknowledges once it completes, unlike an action (whose
  accept comes back in milliseconds) — so it is the one command shape with
  a wide, deterministic window between "sent" and "acknowledged" in which a
  disconnect can land. That window is what makes the SDK's
  `command_outcome_unknown` path testable at all: without it, a client
  disconnecting mid-command is a race against a near-instant reply, won or
  lost by scheduling luck, which proves nothing either way.
- `/image_raw` (`sensor_msgs/msg/Image`) — the real webcam (a Logitech Brio
  500 on `/dev/video0` via `run-fake-robot.sh`'s `--device`), opened with
  `cv2.VideoCapture` and converted with `cv_bridge`, not a synthetic
  gradient. Every frame the bridge sees is a real frame at a real rate,
  which is what actually exercises encoding
  and timing rather than hiding problems behind a fake source. Degrades to
  "no camera" (logged once, no crash) when the device cannot be opened —
  expected off the dev host.

Runs on a `MultiThreadedExecutor`, not a bare `rclpy.spin()`: a
synchronous `execute_callback` (the Fibonacci action's) runs on whichever
thread is spinning the node, and if that is the *only* thread, it cannot
also process an incoming cancel request while `time.sleep()`-ing inside the
callback — the cancel would only be noticed once the goal already finished
on its own, which defeats the entire point of a cancel test. Found the
hard way while testing the bridge's own action client against exactly this
shape of server (see ros_runtime.py's test suite).

Run via `./run-fake-robot.sh`, or directly in a sourced ROS2 Humble session:

    python3 tools/fake_robot.py
"""
from __future__ import annotations

import math
import os
import time

import rclpy
from example_interfaces.action import Fibonacci
from example_interfaces.srv import AddTwoInts
import cv2
from cv_bridge import CvBridge
from geometry_msgs.msg import Twist
from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from sensor_msgs.msg import BatteryState, Image, JointState

PUBLISH_HZ = 10.0
BATTERY_PERIOD_S = 20.0
JOINT_PERIOD_S = 4.0
JOINT_NAMES = ["shoulder_pan", "elbow"]

# Which V4L2 device to open for /image_raw — index
# 0 by default, matching --device /dev/video0 in run-fake-robot.sh.
# Overridable so a dev host with the webcam on a different node, or a human
# running this outside Docker, is not stuck editing code.
CAMERA_DEVICE = int(os.environ.get("FAKE_CAMERA_DEVICE", "0"))
# A modest, fixed capture rate — this is a stand-in robot's own publish rate
# (the source topic), not the bridge's configured
# snapshot/live rate, which is what actually governs what the cloud sees.
# Low enough to keep a dev laptop's CPU idle between frames; high enough
# that live video has more than one frame a second to work with.
CAMERA_HZ = float(os.environ.get("FAKE_CAMERA_HZ", "15"))

# Every cmd_vel is logged (not throttled): the whole point is to make each
# individual message — including the one failsafe fires — visible, and a
# real cmd_vel stream is sparse enough during manual testing that log
# volume is not a concern the way it would be for a live joystick session.
CMD_VEL_LOG_PREFIX = "cmd_vel received"

# Deliberately slow: a human, or a check driving this, needs time to invoke,
# observe feedback, and either cancel or disconnect the bridge mid-goal
# before the action would finish on its own. 30 steps stays comfortably
# under the int32 range of Fibonacci's own result/feedback sequence.
FIBONACCI_STEPS = 30
# The clamp on a goal's own `order`, and it is the int32 bound above that
# fixes it, not taste: the sequence's last element after n steps is
# fib(n + 1), and fib(47) = 2971215073 does not fit in the int32 array
# Fibonacci's result and feedback are declared as. 45 steps end at
# fib(46) = 1836311903, the largest value that does.
FIBONACCI_MAX_STEPS = 45
FIBONACCI_STEP_DELAY_S = 1.0

# Configurable, not just a literal, so a check (or a human) can widen or
# narrow the window without editing code — the value matters, not its
# exact number, and different checks may want different widths.
ADD_TWO_INTS_DELAY_S = float(os.environ.get("FAKE_SERVICE_DELAY_S", "2.5"))


class FakeRobot(Node):
    def __init__(self) -> None:
        super().__init__("fake_robot")
        self._battery_pub = self.create_publisher(BatteryState, "/battery", 10)
        self._joint_pub = self.create_publisher(JointState, "/joint_states", 10)
        self.create_subscription(Twist, "/cmd_vel", self._on_cmd_vel, 10)
        self._action_server = ActionServer(
            self,
            Fibonacci,
            "count",
            self._execute_fibonacci,
            goal_callback=self._accept_goal,
            # rclpy's own default rejects every cancel request; the
            # cancel-by-slug check needs a server that actually honours one.
            cancel_callback=self._accept_cancel,
        )
        self.create_service(AddTwoInts, "/add_two_ints", self._add_two_ints)
        self._start = time.monotonic()
        self.create_timer(1.0 / PUBLISH_HZ, self._tick)

        # /image_raw — the real webcam, not a synthetic gradient: every
        # frame the bridge ever sees is a real frame at a real rate, so
        # encoding and timing problems
        # show up here rather than being hidden by a fake source. Opening
        # the device can fail (no camera on this host, or run outside
        # --device /dev/video0) — logged once and degraded to "no camera",
        # not a crash, so the rest of the fake robot still comes up for
        # whatever else a session needs it for.
        self._image_pub = self.create_publisher(Image, "/image_raw", 10)
        self._cv_bridge = CvBridge()
        self._camera = cv2.VideoCapture(CAMERA_DEVICE)
        if not self._camera.isOpened():
            self.get_logger().warning(
                "Could not open camera device {} — /image_raw will not "
                "publish. Expected off the dev host; the camera checks "
                "need the real webcam.".format(CAMERA_DEVICE)
            )
            self._camera = None
        else:
            self.create_timer(1.0 / CAMERA_HZ, self._publish_image)

        self.get_logger().info(
            "fake_robot publishing /battery and /joint_states at {:.0f} Hz, "
            "{}, subscribed to /cmd_vel, serving the 'count' action and "
            "/add_two_ints ({:.1f}s deliberate delay)".format(
                PUBLISH_HZ,
                "/image_raw at {:.0f} Hz from camera device {}".format(
                    CAMERA_HZ, CAMERA_DEVICE
                )
                if self._camera is not None
                else "no camera",
                ADD_TWO_INTS_DELAY_S,
            )
        )

    @staticmethod
    def _accept_goal(_goal_request) -> GoalResponse:
        return GoalResponse.ACCEPT

    @staticmethod
    def _accept_cancel(_goal_handle) -> CancelResponse:
        return CancelResponse.ACCEPT

    def _execute_fibonacci(self, goal_handle):
        # Honour the goal's own `order`. This used to run FIBONACCI_STEPS
        # regardless, so every `count` invocation took the same ~30 s and a
        # check that wanted a short run had no way to ask for one — the
        # parameter was on the wire, accepted, and silently ignored, which
        # is the worst of the three options.
        #
        # `order` is a uint32 in the message, so it cannot be negative;
        # zero means "unspecified" and keeps the old default rather than
        # returning [0, 1] immediately. The upper clamp is what stops a
        # caller from parking this fake robot for an hour.
        requested = int(getattr(goal_handle.request, "order", 0) or 0)
        steps = min(requested or FIBONACCI_STEPS, FIBONACCI_MAX_STEPS)

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
            time.sleep(FIBONACCI_STEP_DELAY_S)
        goal_handle.succeed()
        result = Fibonacci.Result()
        result.sequence = feedback.sequence
        return result

    def _add_two_ints(self, request: AddTwoInts.Request, response: AddTwoInts.Response):
        # Deliberately slow — see the module docstring for why. Runs on the
        # MultiThreadedExecutor's own thread pool, so this sleep costs
        # nothing else here: the battery/joint_state timer and the cmd_vel
        # subscriber keep running on their own threads regardless.
        time.sleep(ADD_TWO_INTS_DELAY_S)
        response.sum = request.a + request.b
        return response

    def _on_cmd_vel(self, msg: Twist) -> None:
        # Zero-twist is what a failsafe typically configures, so it is
        # called out explicitly rather than left to blend into the rest of
        # a normal driving log.
        is_zero_twist = (
            msg.linear.x == 0.0
            and msg.linear.y == 0.0
            and msg.linear.z == 0.0
            and msg.angular.x == 0.0
            and msg.angular.y == 0.0
            and msg.angular.z == 0.0
        )
        self.get_logger().info(
            "{}: linear=({:.3f}, {:.3f}, {:.3f}) angular=({:.3f}, {:.3f}, {:.3f}){}".format(
                CMD_VEL_LOG_PREFIX,
                msg.linear.x,
                msg.linear.y,
                msg.linear.z,
                msg.angular.x,
                msg.angular.y,
                msg.angular.z,
                " [zero-twist]" if is_zero_twist else "",
            )
        )

    def _tick(self) -> None:
        elapsed = time.monotonic() - self._start
        self._publish_battery(elapsed)
        self._publish_joint_states(elapsed)

    def _publish_battery(self, elapsed: float) -> None:
        # A triangle, not a sine: it is easy to eyeball "definitely
        # still moving" on a live dashboard without doing trig in your head.
        phase = (elapsed % BATTERY_PERIOD_S) / BATTERY_PERIOD_S  # 0..1
        percentage = 1.0 - abs(2.0 * phase - 1.0)  # 0 -> 1 -> 0

        msg = BatteryState()
        msg.percentage = percentage
        msg.voltage = 11.1 + percentage  # a plausible-looking number, nothing more
        msg.present = True
        msg.power_supply_status = BatteryState.POWER_SUPPLY_STATUS_DISCHARGING
        msg.power_supply_health = BatteryState.POWER_SUPPLY_HEALTH_GOOD
        msg.power_supply_technology = BatteryState.POWER_SUPPLY_TECHNOLOGY_LIPO
        self._battery_pub.publish(msg)

    def _publish_joint_states(self, elapsed: float) -> None:
        msg = JointState()
        msg.name = list(JOINT_NAMES)
        msg.position = [
            math.sin(2 * math.pi * elapsed / JOINT_PERIOD_S + phase)
            for phase in (0.0, math.pi / 2)
        ]
        msg.velocity = [0.0 for _ in JOINT_NAMES]
        msg.effort = [0.0 for _ in JOINT_NAMES]
        self._joint_pub.publish(msg)

    def _publish_image(self) -> None:
        # A read failure (device unplugged mid-session, transient V4L2
        # hiccup) is logged and skipped, not fatal — the same "one bad
        # thing does not take the rest down" rule the other publishers
        # follow, here applied to a frame instead of a slug.
        ok, frame = self._camera.read()
        if not ok:
            self.get_logger().warning("Could not read a frame from the camera")
            return
        msg = self._cv_bridge.cv2_to_imgmsg(frame, encoding="bgr8")
        msg.header.frame_id = "camera"
        self._image_pub.publish(msg)

    def close_camera(self) -> None:
        """Releases the V4L2 device — called from `main`'s `finally`, so a
        second process (or a re-run of this one) is not left finding the
        device busy after a clean shutdown."""
        if self._camera is not None:
            self._camera.release()


def main() -> None:
    rclpy.init()
    node = FakeRobot()
    # MultiThreadedExecutor, not rclpy.spin(node)'s default single thread —
    # see the module docstring for why a synchronous action execute_callback
    # otherwise starves the cancel service request behind its own sleep.
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()
        node.close_camera()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
