# SPDX-License-Identifier: Apache-2.0
"""A subscriber in a process of its own, started by `_start_witness_process`
in test_ros_runtime.py.

Fast DDS hands a sample to a reader in the writer's own process on a path of
its own, so a witness inside the test process cannot say whether a subscriber
on a robot -- always another process -- receives what the bridge publishes.
This one can.

Usage: python3 witness_process.py <topic>

It subscribes to `geometry_msgs/msg/Twist` on <topic> with the default QoS a
robot's node would use, and writes one JSON object per line to stdout:

  {"ev": "ready", "t": ...}            once the subscription exists
  {"ev": "matched", "t": ...}          when its side first sees a publisher
  {"ev": "publishers", "t": ..., "n": ..., "prev": ...}
                                       whenever the number of publishers it
                                       has matched changes
  {"ev": "rx", "t": ..., "x": ...}     for every sample; x is linear.x

`t` is `time.monotonic()`. Both processes run on one kernel, so the test's
own `time.monotonic()` is comparable with it. It runs until it is
terminated. The name does not start with `test_`, so pytest does not collect
it.
"""
import json
import os
import sys
import time

import rclpy
from geometry_msgs.msg import Twist
from rclpy.executors import ExternalShutdownException

#: How often this side asks whether it has matched the publisher. The test
#: reads "published before the witness matched" from the `matched` event, so
#: the poll has to be finer than the gap it is meant to resolve.
MATCH_POLL_S = 0.001

#: How often it counts its publishers afterwards, for `publishers`: a
#: replaced failsafe process brings a new writer to a witness that stays.
COUNT_POLL_S = 0.005


def emit(ev, **fields):
    fields["ev"] = ev
    fields["t"] = time.monotonic()
    sys.stdout.write(json.dumps(fields) + "\n")
    sys.stdout.flush()


def main(argv):
    if len(argv) != 2:
        sys.stderr.write("usage: witness_process.py <topic>\n")
        return 2
    topic = argv[1]
    rclpy.init(args=[])
    node = rclpy.create_node("test_witness_process_{}".format(os.getpid()))
    subscription = node.create_subscription(Twist, topic, lambda msg: emit("rx", x=msg.linear.x), 10)

    def publishers():
        # The same rule as `_wait_until_matched`: the subscription's own count
        # where rclpy has one (rclpy 7 and later), the node's graph on rclpy 3.
        count = getattr(subscription, "get_publisher_count", None)
        if count is not None:
            return count()
        return node.count_publishers(topic)

    last = [0]

    def count_poll():
        n = publishers()
        if n != last[0]:
            emit("publishers", n=n, prev=last[0])
            last[0] = n

    def poll():
        if publishers() > 0:
            emit("matched")
            timer.cancel()
            node.create_timer(COUNT_POLL_S, count_poll)

    timer = node.create_timer(MATCH_POLL_S, poll)
    emit("ready")
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, ExternalShutdownException):
        # SIGINT, or SIGTERM where rclpy's signal handler turns it into a
        # context shutdown: both are the ordinary end of this process.
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
