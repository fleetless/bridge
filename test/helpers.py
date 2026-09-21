# SPDX-License-Identifier: Apache-2.0
"""Small utilities the client tests share."""
import asyncio
import time

from fleetless_bridge.client import BridgeClient
from fleetless_bridge.config import BridgeConfig


class TimedWs:
    """Fixed time per send, so a pump's or writer's throughput estimate is a
    known number, not whatever the machine did. Shared by
    `test_client_snapshot_budget.py` (2.0.2's per-snapshot budget) and
    `test_client_writer.py` (the `PrioritizedWriter` that supersedes it) —
    one copy, not two drifting definitions.

    `elapsed` is what each send *actually* took, because "a known number"
    is only half true: `asyncio.sleep(0.1)` is a floor, not a duration, and
    on a loaded machine it overshoots by several percent. A test that
    compares a measured rate against the nominal one is then measuring the
    machine's timer, which it cannot assert anything about — read the
    matching `elapsed` entry instead and the claim stays about the
    arithmetic under test."""

    def __init__(self, seconds_per_send: float) -> None:
        self.sent = []
        self.elapsed = []
        self._seconds = seconds_per_send

    async def send(self, payload) -> None:
        started = time.monotonic()
        await asyncio.sleep(self._seconds)
        self.elapsed.append(time.monotonic() - started)
        self.sent.append(payload)


class RecordingSleep:
    """Stands in for asyncio.sleep in the backoff: records the delay, skips the
    wait. The real timing logic still runs — only the wait is skipped, so a
    test of the 30 s cap does not take 30 s.
    """

    def __init__(self) -> None:
        self.delays = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        await asyncio.sleep(0)  # yield, so a reconnect loop stays fair


def make_client(cloud, *, token="frt_test_token", **overrides) -> BridgeClient:
    """A client pointed at `cloud`, with timeouts short enough for a test."""
    overrides.setdefault("sleep", RecordingSleep())
    overrides.setdefault("handshake_timeout", 2.0)
    overrides.setdefault("idle_timeout", 2.0)
    return BridgeClient(
        BridgeConfig(token=token, cloud_url=cloud.url),
        **overrides,
    )


async def run_until(client, condition, timeout: float = 10.0):
    """Run the client until `condition()` holds, then stop it — returns the
    StopReason, or the client's own if it stops by itself first.
    """
    task = asyncio.ensure_future(client.run())
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not condition() and not task.done():
        if loop.time() > deadline:
            task.cancel()
            raise AssertionError("the condition never became true")
        await asyncio.sleep(0.01)
    client.stop()
    return await asyncio.wait_for(task, timeout=timeout)


def by_slug(entries):
    """Converts a list of config entries into the slug-keyed mapping
    `RosRuntime`'s five `apply_*` methods have taken since 3.0 — the wire
    itself is a mapping now (`doc.datapoints` keyed by slug, not an array).
    Entries already carry their slug, so a test can write
    `[_dp("battery"), _dp("speed")]` without repeating each slug as a dict
    key too."""
    return {entry.slug: entry for entry in entries}
