# SPDX-License-Identifier: Apache-2.0
"""Makes the package importable however the suite is started, and gives the
ROS-backed tests a spinning node to work against.

`python3 -m pytest` puts the working directory on sys.path; a plain `pytest`
does not.
"""
import itertools
import pathlib
import sys
import threading
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_node_names = itertools.count()


def pytest_configure(config):
    # `test_live_relay.py`'s three tests (TURN/relay, quality-under-a-real-
    # server, the 15-minute soak) drive docker containers from the host;
    # collected everywhere, runnable only where `docker` is on PATH -- see
    # that file's docstring. Registered here so `-m slow` works and
    # collection prints no "unknown marker" warning.
    config.addinivalue_line(
        "markers", "slow: drives real containers/network from the host; "
        "select with -m slow (see test_live_relay.py)")


@pytest.fixture
def ros():
    """One rclpy context per test. Sequential init/shutdown on the default
    context is the standard rclpy test pattern — this suite never runs ROS
    in parallel."""
    import rclpy

    rclpy.init()
    yield
    rclpy.shutdown()


@pytest.fixture
def spun_node(ros):
    """A real `rclpy.node.Node`, spinning on its own background thread for
    the test's duration — the same executor-on-a-thread shape `RosRuntime`
    uses in the bridge, so tests exercise real graph discovery and callback
    delivery, not mocks of either."""
    from rclpy.executors import SingleThreadedExecutor
    from rclpy.node import Node

    node = Node("fleetless_bridge_test_{}".format(next(_node_names)))
    executor = SingleThreadedExecutor()
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    try:
        yield node
    finally:
        executor.shutdown()
        node.destroy_node()
        thread.join(timeout=5.0)


def wait_until(condition, timeout: float = 5.0, interval: float = 0.02) -> None:
    """Poll `condition` until truthy — ROS graph discovery is asynchronous
    (DDS discovery between nodes), so a test that just created a
    publisher/subscription/action server must wait for the graph to catch up
    rather than assert immediately."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition never became true within {}s".format(timeout))
        time.sleep(interval)
