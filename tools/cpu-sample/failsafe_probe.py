#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""How late a publisher's failsafe arrives, seen from its topic.

A development fixture, never installed on a robot by the package. A ROS node
that subscribes to the topic of one publisher of a published fleetless.yaml
and tells two kinds of message apart by their content: the failsafe message
the file declares, and anything else, which is taken for a client's message.

    python3 failsafe_probe.py --config turtle.fleetless.yaml --publisher drive \\
        --silences 200 --out lateness.jsonl

For every silence (client messages, then a failsafe) it writes one JSON line:
the time from the last client message to the failsafe message, minus the
failsafe's `timeout_ms`. Three clocks, in milliseconds:

- `late_source_ms`: from the writer's source timestamps of the two messages,
  the moment each was handed to DDS. This is when the failsafe went out.
- `late_received_ms`: from the reception timestamps DDS gives this
  subscriber. The delivery is in it.
- `late_callback_ms`: from this node's own clock when each callback ran. The
  probe's own scheduling is in it as well.

A failsafe with no client message before it (a second one in the same
silence, or one at the start) is written as `"event": "failsafe_unmatched"`.
A client message that follows a gap longer than `timeout_ms` with no failsafe
in it is written as `"event": "silence_without_failsafe"`. Neither should
ever appear.

The last line is the summary: count, median, p99 and max of each clock.
"""
import argparse
import json
import math
import signal
import sys
import time

import yaml

import rclpy
from rclpy.node import Node
from rclpy.qos import HistoryPolicy, QoSProfile, ReliabilityPolicy
from rosidl_runtime_py.set_message import set_message_fields
from rosidl_runtime_py.utilities import get_message


def publisher_from_config(path, slug):
    """Topic, message type, `timeout_ms` and failsafe message body of one
    publisher of a fleetless.yaml."""
    with open(path) as source:
        doc = yaml.safe_load(source)
    entry = (doc.get("publishers") or {}).get(slug)
    if not entry:
        raise LookupError("{} has no publisher {!r}".format(path, slug))
    failsafe = entry.get("failsafe") or {}
    if "timeout_ms" not in failsafe or "message" not in failsafe:
        raise LookupError("publisher {!r} has no failsafe with timeout_ms and message".format(slug))
    return entry["topic"], entry["type"], int(failsafe["timeout_ms"]), failsafe["message"]


def percentile(values, share):
    """Nearest-rank percentile of a non-empty list."""
    ordered = sorted(values)
    rank = max(1, math.ceil(share * len(ordered)))
    return ordered[rank - 1]


def summary(values):
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "median": round(percentile(values, 0.5), 3),
        "p99": round(percentile(values, 0.99), 3),
        "max": round(max(values), 3),
        "min": round(min(values), 3),
    }


class Probe(Node):
    def __init__(self, topic, type_name, timeout_ms, failsafe_body, out, silences):
        super().__init__("fleetless_failsafe_probe")
        message_type = get_message(type_name)
        self._failsafe = message_type()
        set_message_fields(self._failsafe, failsafe_body)
        self._timeout_ns = timeout_ms * 1_000_000
        self._timeout_ms = timeout_ms
        self._out = out
        self._wanted = silences
        self._last_client = None  # (source_ns, received_ns, callback_ns)
        self._silence_matched = False
        self.late = {"source": [], "received": [], "callback": []}
        self.counts = {"client": 0, "failsafe": 0, "unmatched": 0, "missed": 0}
        self.done = False
        qos = QoSProfile(history=HistoryPolicy.KEEP_LAST, depth=100,
                         reliability=ReliabilityPolicy.RELIABLE)
        self.create_subscription(message_type, topic, self._on_message, qos)

    def _write(self, record):
        record["t"] = round(time.time(), 3)
        self._out.write(json.dumps(record) + "\n")
        self._out.flush()

    def _on_message(self, msg, info):
        callback_ns = time.time_ns()
        source_ns = int(info["source_timestamp"])
        received_ns = int(info["received_timestamp"])
        if msg == self._failsafe:
            self.counts["failsafe"] += 1
            if self._last_client is None or self._silence_matched:
                self.counts["unmatched"] += 1
                self._write({"event": "failsafe_unmatched"})
                return
            last_source, last_received, last_callback = self._last_client
            self._silence_matched = True
            record = {
                "event": "silence",
                "n": len(self.late["source"]) + 1,
                "late_source_ms": (source_ns - last_source - self._timeout_ns) / 1e6,
                "late_received_ms": (received_ns - last_received - self._timeout_ns) / 1e6,
                "late_callback_ms": (callback_ns - last_callback - self._timeout_ns) / 1e6,
            }
            for clock in ("source", "received", "callback"):
                self.late[clock].append(record["late_{}_ms".format(clock)])
            self._write(record)
            if len(self.late["source"]) >= self._wanted:
                self.done = True
            return
        self.counts["client"] += 1
        if self._last_client is not None and not self._silence_matched:
            gap_ns = source_ns - self._last_client[0]
            if gap_ns > self._timeout_ns:
                self.counts["missed"] += 1
                self._write({"event": "silence_without_failsafe", "gap_ms": gap_ns / 1e6})
        self._last_client = (source_ns, received_ns, callback_ns)
        self._silence_matched = False

    def result(self):
        return {
            "event": "summary",
            "timeout_ms": self._timeout_ms,
            "counts": self.counts,
            "late_source_ms": summary(self.late["source"]),
            "late_received_ms": summary(self.late["received"]),
            "late_callback_ms": summary(self.late["callback"]),
        }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--config", required=True, help="the published fleetless.yaml")
    parser.add_argument("--publisher", required=True, help="the publisher's slug")
    parser.add_argument("--silences", type=int, default=200, help="stop after this many (default: 200)")
    parser.add_argument("--out", help="the JSON lines file (default: stdout)")
    args = parser.parse_args(argv)

    topic, type_name, timeout_ms, body = publisher_from_config(args.config, args.publisher)
    out = open(args.out, "a") if args.out else sys.stdout
    rclpy.init()
    probe = Probe(topic, type_name, timeout_ms, body, out, args.silences)
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))
    try:
        while not probe.done and not stopping:
            rclpy.spin_once(probe, timeout_sec=0.5)
    finally:
        result = probe.result()
        out.write(json.dumps(result) + "\n")
        out.flush()
        probe.destroy_node()
        rclpy.try_shutdown()
    return 0 if probe.done else 1


if __name__ == "__main__":
    sys.exit(main())
