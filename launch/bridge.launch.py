# SPDX-License-Identifier: Apache-2.0
"""Launch the Fleetless bridge. Token/cloud URL come from the environment or
can be set here via env parameters.

`respawn`/`respawn_delay`: a process that dies — an unhandled
exception, an OOM kill, anything short of `kill -9`, which is a known
limitation — previously stayed dead until something outside
ROS noticed and restarted it. `ros2 launch` restarts it itself now, on a
fixed delay rather than immediately: an immediate respawn against a cloud
that is down for a real reason (the same outage that likely killed the
process in the first place) just spins, and stacks with `client.py`'s own
reconnect jitter for the part after the process is back up — this
delay staggers the *process* restart, the jitter staggers the *socket*
reconnect that follows it, and a fleet needs both or a lockstep at one layer
just re-creates it at the other.
"""
from launch import LaunchDescription
from launch_ros.actions import Node

# Long enough that a crash loop backs off visibly in the log instead of
# flooding it; short enough that a one-off crash recovers well within the
# cloud's own idle-session expectations.
RESPAWN_DELAY_S = 5.0


def generate_launch_description() -> LaunchDescription:
    return LaunchDescription([
        Node(
            package="fleetless_bridge",
            executable="bridge",
            name="fleetless_bridge",
            output="screen",
            respawn=True,
            respawn_delay=RESPAWN_DELAY_S,
            # **An apt-installed numpy can still lose to a user one.**
            # `cv_bridge` is a C extension compiled against numpy 1.x, and
            # `python3-numpy` (an ordinary apt Depends now, no rosdep
            # constraint and no `/usr/local` install) puts it in system
            # dist-packages — but a `pip install --user` in the operating
            # account puts `~/.local` AHEAD of that on `sys.path`. On one
            # real robot that was numpy 2.2.6, and the result is the worst
            # available shape: `from cv_bridge import CvBridge` still succeeds,
            # the bridge starts, connects, and reports itself healthy — and the
            # first `imgmsg_to_cv2` of a real frame dies with
            # `AttributeError: _ARRAY_API not found`. Every camera on that
            # robot is broken and nothing says so until somebody opens one.
            #
            # Seen on a real robot. The alternative
            # fix — removing or downgrading the user's own numpy — reaches into
            # an environment this package does not own; this reaches only into
            # its own process.
            additional_env={"PYTHONNOUSERSITE": "1"},
        ),
    ])
