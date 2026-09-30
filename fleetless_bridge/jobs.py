# SPDX-License-Identifier: Apache-2.0
"""Job bookkeeping shared by actions and services.

Job state lives only here, only in memory — if this process restarts, it is
gone, and `hello.active_jobs` (protocol.py) is how a fresh process tells the
cloud so, rather than any frame this module sends (see `client.py`). The one
exception is an own action job still in the persisted goal mapping
(`goal_state.py`): `RosRuntime.start()` registers each of those here again
before the first `hello`, so the restarted process still names it.

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
    # Both required on the wire (contracts' bridgeJobUpdate, protocol 5) but
    # defaulted here so every existing call site — all of them a
    # 'fleetless' job with no ROS goal to report yet — need not be touched.
    # 'external' is only ever passed explicitly, by the goal tracker's own
    # discovery/heartbeat path (ros_runtime.py). `goal_id` is the ROS 2 goal
    # id string for an action job, or `None` for a service job (no ROS
    # goal exists) or a job that has none yet (e.g. `goal_send_failed`/
    # `goal_rejected`,
    # emitted before a goal ever existed).
    origin: str = "fleetless"
    goal_id: Optional[str] = None


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

    **A job's terminal update also retires its own pending marker, the
    moment it is queued** — not only later non-terminal arrivals collapse
    into one another, a *terminal* arrival collapses whatever non-terminal
    marker was already waiting for that job into itself. Without this, a
    job that produced a heartbeat or two while
    disconnected and then genuinely finished would queue *both*: the stale
    `running` marker and the real outcome, delivered in that order on
    reconnect — a caller watching the wire would see the job "still
    running" for one frame, immediately contradicted by the frame right
    behind it. `requeue_front` closes the same gap for a send that failed
    and is being retried: a stale non-terminal `update` handed back to it
    must not jump back in front of a terminal update for the same job that
    arrived while the failed send was in flight.

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
            # A job that just reached its outcome must deliver that outcome
            # alone — not a stale `running` marker immediately ahead of it,
            # which would say nothing the outcome does not already say
            # better. If this job still has a pending
            # non-terminal marker sitting in `_items`, its *position* is
            # retired here too, not merely its `_pending_feedback` entry —
            # leaving the bare job_id behind in the deque would let
            # `_resolve` pop from a `_pending_feedback` this line already
            # emptied, and (worse) would still hand out a `running` frame
            # ahead of the terminal one it was meant to collapse into.
            if self._pending_feedback.pop(update.job_id, None) is not None:
                self._items.remove(update.job_id)
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
        this queue does for a first arrival, applied to a retry.

        The same drop applies, for a different reason, when this job's
        *terminal* update landed while the failed send was in flight: `get()`
        already removed job_id's marker and `_pending_feedback` entry before
        `_push` ran for the terminal update, so the "superseded" check just
        below finds nothing there and would otherwise let this stale
        `running` copy jump back in *ahead of* the terminal update `_push`
        already appended — exactly the stale-frame-before-the-real-result
        case the dead-zone collapse (`_push`) exists to rule out, reopened
        here if this method did not also check for it."""
        if update.state in _TERMINAL_STATES:
            self._items.appendleft(update)
            self._not_empty.set()
            return
        if update.job_id in self._pending_feedback:
            return  # superseded while the failed send was in flight — drop the stale copy
        if any(isinstance(item, JobUpdate) and item.job_id == update.job_id for item in self._items):
            return  # this job's terminal update is already queued — see docstring above
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
    # 'fleetless' | 'external' (contracts' job.origin) — 'external' only for
    # a goal the tracker found active without this process having sent it
    # (ros_runtime.py's GoalTracker). Never written to job_runs by the
    # cloud, but tracked here the same as any other job: it occupies its
    # slug, gets heartbeated, and can be cancelled the same way.
    origin: str = "fleetless"


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
        # slug -> the ids of every job held on it, oldest first (a dict
        # used as an ordered set). Several at once on one action: an own
        # job and external goals, or several external goals — an action
        # server may run goals concurrently, and one ending must not make
        # the slug look free while another still runs.
        self._by_slug: Dict[str, Dict[str, None]] = {}
        # Called with the job id, outside the lock, once `finish` retired a
        # job — i.e. once its terminal update was *delivered*. Runs on
        # whichever thread called `mark_delivered` (client.py's asyncio
        # thread); a settable attribute, like `JobUpdateQueue.on_put`, so
        # nothing here knows who listens. `RosRuntime` uses it to drop the
        # job's persisted goal mapping only now (goal_state.py: "removed
        # once the job is terminal *and reported*").
        self.on_finished: Optional[Callable[[str], None]] = None

    def start(self, job_id: str, slug: str, kind: str) -> None:
        """Record a new job as running. Idempotent for the same `job_id` —
        a duplicate `invoke` (e.g. redelivered after a reconnect race) must
        not stomp on an already-tracked job."""
        with self._lock:
            if job_id in self._jobs:
                return
            self._jobs[job_id] = _JobRecord(job_id=job_id, slug=slug, kind=kind)
            self._by_slug.setdefault(slug, {})[job_id] = None

    def register_external(self, job_id: str, slug: str) -> None:
        """The other way a job starts: the goal tracker (ros_runtime.py)
        found an active goal on a published action that this process never
        sent. Same bookkeeping as `start()` — occupies `slug`, appears in
        `active_jobs()`, heartbeats and cancels the same way — except
        there is no cloud-issued `invoke` behind it, so none of `start()`'s
        own-job-only concerns (patience deadlines, goal-timeout bookkeeping)
        apply here; the caller sends no goal, it only noticed one.
        Idempotent for the same reasons `start()` is: the tracker's
        discovery loop may see the same still-active goal on more than one
        tick before this job is even reported once."""
        with self._lock:
            if job_id in self._jobs:
                return
            self._jobs[job_id] = _JobRecord(job_id=job_id, slug=slug, kind="action", origin="external")
            self._by_slug.setdefault(slug, {})[job_id] = None

    def origin_of(self, job_id: str) -> Optional[str]:
        """`'fleetless'` or `'external'` for a job this manager still holds,
        `None` if it does not (already delivered, or never started) — the
        same shape as `state_of`. What `RosRuntime._emit_job` reads so every
        call site can go on naming only a state, never an origin: a job's
        origin is decided once, at `start()`/`register_external()`, and
        every later word about it — including one this method itself does
        not know how to spell out, like `action_server_lost` — carries
        whichever origin the job already has."""
        with self._lock:
            record = self._jobs.get(job_id)
            return record.origin if record is not None else None

    def running_job_id(self, slug: str) -> Optional[str]:
        """The oldest job held for `slug`, if any — enough for a question
        with one answer ("is anything held here?", the service busy-guard).
        An action slug can hold several jobs at once; whoever must act on
        each of them uses `job_ids_on`."""
        with self._lock:
            held = self._by_slug.get(slug)
            return next(iter(held)) if held else None

    def job_ids_on(self, slug: str) -> List[str]:
        """Every job held for `slug`, oldest first — own and external,
        running or terminal-but-undelivered."""
        with self._lock:
            return list(self._by_slug.get(slug, ()))

    def slug_for(self, job_id: str) -> Optional[str]:
        """The slug `job_id` is running on, or `None` if this manager is not
        holding it (already delivered, or never started) — the inverse of
        `running_job_id`, for callers that only have a `job_id` to start
        from. `ros_runtime.py`'s heartbeat and action-server-liveness ticks
        both walk `_active_goals` (job_id -> goal handle) and need each
        job's slug to reach its `_ActionEntry`; `_active_goals` itself does
        not carry one, so this is where that lookup happens instead of a
        second, shadow copy of the same fact kept in step by hand."""
        with self._lock:
            record = self._jobs.get(job_id)
            return record.slug if record is not None else None

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
            held = self._by_slug.get(record.slug)
            if held is not None:
                held.pop(job_id, None)
                if not held:
                    del self._by_slug[record.slug]
        if self.on_finished is not None:
            self.on_finished(job_id)

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
