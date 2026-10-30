#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check the Description that dpkg exposes from each built Bridge package."""

import difflib
import pathlib
import subprocess
import sys


SHORT = (
    "Fleetless Bridge: connects a ROS 2 robot to the Fleetless cloud. "
    "Not a safety function — read the safety notice: "
    "https://docs.fleetless.dev/#safety-notice"
)
P32 = "https://docs.fleetless.dev/#safety-notice"
NOTICE = """Fleetless is not a safety function. It is not a safety component, a safety-rated controller or an emergency-stop system, and it does not replace the risk assessment of your robot and its surroundings.

- Commands sent through Fleetless can be delayed, lost or executed out of order, for example when a network connection drops.
- Every robot must reach, on its own, the safe state defined in its risk assessment when the connection to Fleetless is lost or the bridge stops working. Depending on the robot, stopping at once is not always the safe state.
- The failsafe message of a bridge publisher is an operating aid, not a safety function. It is sent by the bridge process and cannot be sent if that process or the robot's computer fails.
- Keep a physical emergency stop within reach whenever a robot can move.
- AI agents that control robots through Fleetless can make mistakes. Use an AI agent to move a robot only while a person supervises every motion and can stop the robot at once. Do not let AI agents move robots unattended. Where Fleetless allows it, for example through the roles of an app, limit what an AI agent may do.

Commands may also be executed more than once, and displayed status may be stale. Safety functions, including the response to communication loss, must be implemented independently of Fleetless and validated for the specific robot and its hazards. Human supervision and an emergency stop do not replace the required protective measures. Nothing in this notice excludes our liability under mandatory law.

You are responsible for the safe integration and operation of your robots, as described in section 8 of our [Terms of Service](https://fleetless.dev/terms)."""


def check(path: pathlib.Path) -> None:
    result = subprocess.run(
        ["dpkg-deb", "--field", str(path), "Description"],
        check=True,
        capture_output=True,
        text=True,
    )
    # Debian continuation lines start with one space. A line containing only
    # " ." is an empty paragraph; keep those boundaries in the comparison.
    expected_lines = [SHORT, " Version 1.0 — Effective from 1 November 2026", " ."]
    expected_lines.extend(" ." if not line else f" {line}" for line in NOTICE.splitlines())
    expected_lines.extend([" .", f" {P32}"])
    actual_lines = result.stdout.rstrip("\n").splitlines()
    expected = "\n".join(expected_lines)
    actual = "\n".join(actual_lines[: len(expected_lines)])
    if actual != expected or result.stdout.count(P32) != 2:
        diff = "\n".join(
            difflib.unified_diff(
                expected.splitlines(),
                actual.splitlines(),
                fromfile="approved Description prefix",
                tofile=str(path),
                lineterm="",
            )
        )
        raise AssertionError(f"{path}: built Debian Description differs:\n{diff}")
    print(f"{path}: approved Description and P32 present")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage: check-deb-description.py <built.deb> ...")
    for filename in sys.argv[1:]:
        check(pathlib.Path(filename))
