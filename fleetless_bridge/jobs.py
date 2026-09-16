# SPDX-License-Identifier: Apache-2.0
"""Job bookkeeping shared by actions and services.

Job state lives only here, only in memory — if this process restarts, it is
gone, and `hello.active_jobs` (protocol.py) is how a fresh process tells the
cloud so, rather than any frame this module sends (see `client.py`).

Two ROS-side execution paths — a goal lifecycle for actions, one call for
services — both funnel into the same `JobUpdateQueue`: from the cloud's and
the client's point of view a job is a job regardless of which ROS primitive
is running underneath it (`invoke` covers both).

This module is deliberately rclpy-free: `ros_runtime.py` owns every ROS
object (goal handles, service futures); this module only tracks *which* job
is running for which slug and queues what to say about it, so it is testable
without a ROS runtime and stays the single place that decides what "still
running" means.
"""
from __future__ import annotations

import asyncio
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Union

# Terminal states — once a job reaches one of these it is no longer "the
# running job" for its slug (see `_by_slug`); it keeps being named in
# `active_jobs()`, with this state, until its delivery is confirmed (see
# `finish`).
_TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled", "lost"})


@dataclass(frozen=True)
class JobUpdate:
    """One outgoing `job_update` — see `protocol.job_update_message` for the
    wire shape this becomes. `error` is `(code, message)` or `None`.

    `details` is the structured payload for an error code that documents one,
    `job_queue_full`'s `{limit, queued}` being the only one today. Kept
    separate from `error` rather than a third tuple element: almost every
    error has none, and `None` here must mean *no structured payload*, not
    *unset*. Only meaningful alongside a non-`None` `error`."""

    job_id: str
    slug: str
    state: str
    timestamp_ms: int
    feedback: Any = None
    progress: Optional[float] = None
    result: Any = None
    error: Optional[Tuple[str, str]] = None
    details: Any = None


class JobUpdateQueue:
    """The handoff from ROS callbacks (executor thread) to the client's send
    loop (asyncio) — unbounded and drop-nothing for a job's **outcome**,
    unlike `SampleQueue` (ros_runtime.py): a datapoint sample has a
    successor a moment later, a job result does not. A terminal update
    (`_TERMINAL_STATES`) is never dropped and never replaced; while
    disconnected these simply accumulate, and `_pump_jobs` delivers every
    one once reconnected.

    A non-terminal ("running") update — a plain state transition or a
    feedback frame — is different (2m): only the **latest per job** is ever
    queued for delivery. This is a real trade, not a free lunch: **when a
    job produces feedback faster than it can be delivered, the intermediate
    frames are genuinely lost**, not merely summarized — a navigation action
    reporting waypoints passed loses the ones between the last-delivered
    frame and the next one that makes it onto the wire, and they do not
    come back. `timestamp_ms` (carried by feedback the same as a
    datapoint) is what makes the frame that does arrive honest about how
    stale it is, not a way to reconstruct what was skipped.

    The alternative — the queue's previous behaviour, and the rationale this
    replaces — was drop-nothing for feedback too: correct for a slow
    reconnect, unbounded for a chatty action stuck behind a long outage. One
    accepted action publishing feedback at 10 Hz through an hours-long
    disconnect grew this queue by tens of thousands of frames, behind a
    bound (`MAX_TRACKED_JOBS`) that counts *jobs*, not updates per job, and
    so never sees it — a robot that looks bounded and healthy while one of
    its queues is not. A bounded queue that loses intermediate feedback is
    the smaller, honestly-stated cost.

    Built on a plain `deque`, not `asyncio.Queue`: `_pump_jobs` must be able
    to put an update it already dequeued *back*, at the front, when the send
    that was going to deliver it fails (the connection dropped between
    `get()` and `ws.send()` succeeding) — `asyncio.Queue` has no supported
    way to do that, and losing that update would be exactly the silent drop
    this queue exists to rule out for a *terminal* update. `_items` holds
    either a terminal `JobUpdate` directly (drop-nothing, as before) or a
    bare `job_id` string marking "the latest non-terminal update for this
    job is in `_pending_feedback`" — one marker per job at a time, which is
    what makes coalescing work: a second non-terminal update for a job
    already holding a marker overwrites `_pending_feedback[job_id]` in
    place rather than queuing a second position.

    `on_put` is the optional wake hook `ros_runtime.SampleQueue` documents,
    for the same reason and with the same rule: a settable attribute, never
    an import, so nothing here knows the writer exists."""

    def __init__(self, on_put: Optional[Callable[[], None]] = None) -> None:
        self._items: Deque[Union[JobUpdate, str]] = deque()
        self._pending_feedback: Dict[str, JobUpdate] = {}
        self._not_empty = asyncio.Event()
        self.on_put = on_put

    def put_threadsafe(self, loop: asyncio.AbstractEventLoop, update: JobUpdate) -> None:
        loop.call_soon_threadsafe(self._push, update)

    def _push(self, update: JobUpdate) -> None:
        if update.state in _TERMINAL_STATES:
            self._items.append(update)
        elif update.job_id not in self._pending_feedback:
            # First non-terminal update pending for this job — claim a
            # position in the deque; later ones for the same job just
            # overwrite the dict entry the marker resolves to.
            self._items.append(update.job_id)
            self._pending_feedback[update.job_id] = update
        else:
            self._pending_feedback[update.job_id] = update
        self._not_empty.set()
        if self.on_put is not None:
            self.on_put()

    async def get(self) -> JobUpdate:
        while not self._items:
            self._not_empty.clear()
            await self._not_empty.wait()
        return self._resolve(self._items.popleft())

    def try_get(self) -> Optional[JobUpdate]:
        """Non-blocking counterpart to `get()`: `None` when nothing is
        queued. What `client.py`'s tier-1 writer source uses — a tier scan
        must never block on one source while a higher tier waits."""
        if not self._items:
            return None
        return self._resolve(self._items.popleft())

    def _resolve(self, item: Union[JobUpdate, str]) -> JobUpdate:
        """A dequeued entry is either a terminal update directly, or a
        `job_id` marker standing for whatever the latest non-terminal
        update for that job currently is."""
        if isinstance(item, str):
            return self._pending_feedback.pop(item)
        return item

    def requeue_front(self, update: JobUpdate) -> None:
        """Puts `update` back where `get()` would hand it out next — for a
        send that was attempted and failed, not a new arrival, so it must
        not go to the back of updates that arrived after it.

        For a non-terminal update this can race a fresher one: `get()`
        already removed this job's marker and dict entry, so a feedback
        frame that arrived from the executor thread while the failed send
        was in flight (`_push`, via `call_soon_threadsafe`, running on this
        same loop between the `await` in `_pump_jobs` and this call) may
        already have installed a new marker and a newer `_pending_feedback`
        entry for the same `job_id`. Re-inserting the stale `update` in that
        case — as a second marker, or by overwriting the newer entry — would
        either double-deliver or silently drop the newer one on the next
        `get()`. The fresher update already **is** the correct thing to
        resend, so the stale one is dropped instead: exactly the coalescing
        this queue does for a first arrival, applied to a retry."""
        if update.state in _TERMINAL_STATES:
            self._items.appendleft(update)
            self._not_empty.set()
            return
        if update.job_id in self._pending_feedback:
            return  # superseded while the failed send was in flight — drop the stale copy
        self._items.appendleft(update.job_id)
        self._pending_feedback[update.job_id] = update
        self._not_empty.set()

    def empty(self) -> bool:
        return not self._items


@dataclass
class _JobRecord:
    job_id: str
    slug: str
    kind: str  # 'action' | 'service' — informational, nothing here branches on it
    state: str = "running"


class JobManager:
    """Which jobs this process currently believes are active, and the queue
    of what to say about them. Shared between `ros_runtime.py` (the only
    writer — always from the executor thread) and `client.py` (reads
    `active_jobs()` for `hello`, drains `updates` for the wire) — a `Lock`
    guards the registry since those are two different threads, unlike
    `RosRuntime`'s ROS-only state, which only the executor thread ever
    touches."""

    def __init__(self) -> None:
        self.updates = JobUpdateQueue()
        self._lock = threading.Lock()
        # job_id -> record, held until its terminal update is *delivered*
        # (`finish`, called from `mark_delivered`) — not kept for the life
        # of the process; see `finish`'s own docstring.
        self._jobs: Dict[str, _JobRecord] = {}
        self._by_slug: Dict[str, str] = {}  # slug -> job_id, only while that job is running

    def start(self, job_id: str, slug: str, kind: str) -> None:
        """Record a new job as running. Idempotent for the same `job_id` —
        a duplicate `invoke` (e.g. redelivered after a reconnect race) must
        not stomp on an already-tracked job."""
        with self._lock:
            if job_id in self._jobs:
                return
            self._jobs[job_id] = _JobRecord(job_id=job_id, slug=slug, kind=kind)
            self._by_slug[slug] = job_id

    def running_job_id(self, slug: str) -> Optional[str]:
        """The job currently running for `slug`, if any — cancel-by-slug and
        the defensive busy-guard both key off this."""
        with self._lock:
            return self._by_slug.get(slug)

    def tracked_count(self) -> int:
        """How many jobs `_jobs` currently holds — every job not yet
        *delivered* (see `finish`), whether still `running` or a terminal
        outcome the bridge is still holding for report. This is what
        `RosRuntime`'s admission guard bounds: refuse a new
        `invoke` once this is already at the limit, rather than admitting
        work whose eventual outcome may never be deliverable."""
        with self._lock:
            return len(self._jobs)

    def state_of(self, job_id: str) -> Optional[str]:
        """The state `active_jobs()` currently holds for `job_id`, or
        `None` if this manager is not holding it at all (already
        delivered, or never started).

        Exists because `running_job_id(slug)` answers a different
        question than it looks like: it stays non-`None` until
        *delivered*, not only while *running* — a job that already reached
        `succeeded` via `emit()` keeps being named by `running_job_id`
        until `mark_delivered()` runs (correct: don't free the slug before
        the outcome genuinely reached the cloud). See
        `RosRuntime._settle_orphaned_job` for the bug that distinction
        closes."""
        with self._lock:
            record = self._jobs.get(job_id)
            return record.state if record is not None else None

    def finish(self, job_id: str, state: str) -> None:
        """Only ever reached via `mark_delivered` — kept as a separate
        method because it is a distinct event (the *terminal outcome*
        landing, as opposed to `emit`'s *the bridge learned something*),
        not because anything else calls it. Retires the job entirely: once
        its terminal update has genuinely reached the cloud there is
        nothing left for `active_jobs()` to usefully say about it, and
        keeping the record around would only grow `_jobs` for the life of
        the process for no reason (an unbounded per-job record is exactly
        the kind of thing an OOM kill loses nothing by not having)."""
        assert state in _TERMINAL_STATES, "finish() needs a terminal state, got {!r}".format(state)
        with self._lock:
            record = self._jobs.pop(job_id, None)
            if record is None:
                return
            if self._by_slug.get(record.slug) == job_id:
                del self._by_slug[record.slug]

    def active_jobs(self) -> List[Tuple[str, str, str]]:
        """Every job this manager still holds, as `(job_id, slug, state)` —
        what `hello.active_jobs` sends (protocol.py), so the cloud can
        tell a reconnect (jobs named here) from a restart (nothing is,
        because a fresh process has nothing to name) without any other wire
        cooperation.

        `state` is whatever `emit()` last recorded for that job — the
        bridge's own current answer, not a history (contracts' `activeJob`
        doc comment). A job whose terminal update is still queued, not yet
        confirmed sent, is named here with its **actual** terminal state
        (`succeeded`/`failed`/`cancelled`), not `running`: the bridge
        already knows the outcome, and reporting `running` for a job that
        has in fact finished is exactly the kind of asserted-but-unverified
        state this settlement exists to remove. The window in which
        the queued `job_update` frame would have said the same thing a
        moment later does not make the earlier, wrong answer honest.

        A job stops being named here once its terminal update is
        *delivered* (`mark_delivered`, which also removes the record — see
        `finish`), not once it is merely known: a job that finishes while
        disconnected must keep appearing until its result genuinely reaches
        the cloud, or the cloud may mark it `lost` moments before the
        truthful outcome arrives for a job it had already given up on."""
        with self._lock:
            return [
                (job_id, record.slug, record.state) for job_id, record in self._jobs.items()
            ]

    def emit(self, loop: asyncio.AbstractEventLoop, update: JobUpdate) -> None:
        """Queues the frame, and records this job's state as `update.state`
        immediately — `active_jobs()` must be able to say what the
        bridge currently believes as soon as the bridge believes it, not
        only once the frame carrying it has actually reached the cloud.
        Removal from the registry is a separate, later event tied to actual
        delivery — see `mark_delivered`."""
        with self._lock:
            record = self._jobs.get(update.job_id)
            if record is not None:
                record.state = update.state
        self.updates.put_threadsafe(loop, update)

    def mark_delivered(self, update: JobUpdate) -> None:
        """Called once `update`'s frame has actually been sent over the
        wire (client.py's `_pump_jobs`, after a successful `ws.send()`, not
        after a failed one that got `requeue_front`-ed). Only now, for a
        terminal update, does the job stop being named in `active_jobs()`
        at all — see `finish`. A non-terminal update (feedback, a plain
        `running`) changes nothing here; `emit` already recorded whatever
        there was to record."""
        if update.state in _TERMINAL_STATES:
            self.finish(update.job_id, update.state)
