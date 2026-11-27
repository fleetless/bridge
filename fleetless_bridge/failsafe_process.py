# SPDX-License-Identifier: Apache-2.0
"""The failsafe process: the child side of `failsafe.FailsafeWatchdog`.

A process of its own, not a thread (fleetless/fleetless#1124). A thread in the
bridge's interpreter needs the bridge's GIL to publish. Under a busy executor
it was measured 12 ms late, and under four busy Python threads up to 327 ms.
This process has its own interpreter and its own GIL, and nothing else runs in
it, so a deadline is met within the 10 ms bound however busy the bridge is.

It owns the **only** writer of every publisher. DDS keeps order per writer,
not across writers: with the failsafe on a second writer, a client's last
command that was lost once and resent could arrive after the failsafe, and the
robot would end on the command. One writer keeps the failsafe after every
command before it. So the bridge builds a client's message, serializes it and
sends it here, and publishing it is what re-arms the deadline.

One loop thread, no executor. It waits in `select` until the earliest thing that
is due and fires what is due before it handles the next message. It never
waits for the rest of a message: it reads what has arrived into a buffer of
its own and handles a message once all of it is there. A large publish (a
`Path`, a `PointCloud2`) arrives in pieces, each written only once the
bridge's busy GIL lets its sender run again, and a blocking read of the rest
would hold every deadline that falls due meanwhile. Reports go back through a
sender thread, so a parent that stops reading never blocks a fire. A loader
thread imports a message type the first time an arm names it, and only while
no deadline is due within `IMPORT_MARGIN_S`: the import holds this
interpreter's GIL. The arm takes effect once the type is loaded.
Nothing here imports `ros_runtime`: the child loads rclpy, the message types
and `params`, nothing else.

If the pipe closes, the bridge is gone: it froze, crashed or was `kill -9`'d.
The process then fires every armed failsafe and exits once those are
acknowledged. SIGTERM does the same, after it said `terminating`, so the
supervisor knows it heard. The signal handler only sets a flag and wakes the
loop, which acts on it between messages: a handler that raised could land
between a client's publish and the line that makes its failsafe owed, and the
topic would end on the command. SIGINT is the bridge's to hear.
"""
from __future__ import annotations

import os
import pickle
import queue
import select
import signal
import struct
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

#: The bound: a failsafe goes out at most this late after its `timeout_ms`.
#: A fire later than this, or a self-test later than this, is a failed
#: watchdog, and the bridge fails closed.
LATENESS_BOUND_MS = 10.0

#: How far ahead the self-test's own deadline lies.
SELF_TEST_DELAY_S = 0.05

#: How often the process says it is alive. The parent declares it dead after
#: `failsafe.ALIVE_TIMEOUT_S` without a word.
ALIVE_INTERVAL_S = 0.25

#: How long a removed writer outlives its last failsafe. A reliable writer
#: resends a sample its readers have not acknowledged; a destroyed one resends
#: nothing, so a failsafe dropped on first send and destroyed a moment later
#: never arrives. Longer than one Fast DDS heartbeat period (3 s by default).
PARTING_FAILSAFE_ACK_TIMEOUT_S = 4.0

#: How long a new writer waits for a retiring writer of another type on the
#: same topic, which the middleware refuses within one node. A publish to the
#: new one before then is dropped.
TYPE_CHANGE_ACK_WAIT_S = 0.5

#: How often a retiring writer is asked whether its readers acknowledged.
RETIRING_CHECK_INTERVAL_S = 0.05

#: A failsafe owed after a restart (the old process may have died before it
#: fired) goes out on a fresh writer. A sample written before any reader
#: matched is lost, so it waits for a reader this long, at most.
OWED_MATCH_WAIT_S = 2.0

#: Once it went out, it goes out again each time another reader matches,
#: within this long: on a loaded machine discovery takes longer than
#: `OWED_MATCH_WAIT_S`, and the reader that matched first may be a recorder
#: or a `ros2 topic echo`, not the robot, which would keep its command.
OWED_RESEND_WINDOW_S = 30.0

QOS_DEPTH = 10

#: The most read from the pipe at once. Between two reads, what is due fires.
READ_CHUNK_BYTES = 64 * 1024

#: A message type is imported only while no deadline is due within this long.
#: The import runs on the loader thread, but loading its compiled typesupport
#: holds this interpreter's GIL for tens of ms (up to about 50 measured), and
#: the loop would wait for it.
IMPORT_MARGIN_S = 0.25

#: An import still not started after this long starts anyway, so a publisher
#: driven without pause at a `timeout_ms` below the margin cannot keep a new
#: one unarmed for ever. A fire it makes late fails the watchdog closed.
IMPORT_CAP_S = 5.0


class _Terminated(BaseException):
    """SIGTERM: handled like a closed pipe. A `BaseException`, so that no
    `except Exception` around a publish can swallow it."""


@dataclass
class _Writer:
    slug: str
    topic: str
    type_name: str
    message_class: type
    failsafe: Any
    timeout_s: float
    publisher: Any = None
    # `None` means dormant: nobody published, so no failsafe is owed.
    touched_at: Optional[float] = None
    fired: bool = True
    # A failsafe owed from a previous process, fired once a reader matched.
    owed_since: Optional[float] = None
    # It went out: until then it goes out again whenever more readers
    # matched than at its last send (`resend_matched`).
    resend_until: Optional[float] = None
    resend_matched: int = 0
    # Created only once no retiring writer of another type holds the topic.
    create_after: float = 0.0


@dataclass
class _Retiring:
    slug: str
    topic: str
    type_name: str
    publisher: Any
    deadline: float
    # A failsafe still owed when the writer went, from a previous process:
    # sent once more readers matched than `matched_floor` (those that got
    # it already), then acknowledged. At `send_by` without them: sent
    # anyway when none had it, given up otherwise.
    failsafe: Any = None
    send_by: Optional[float] = None
    matched_floor: int = 0


class FailsafeProcess:
    """The loop. Constructed with a node and a connection, so a test can run
    it in-process; `main` is what the spawned process runs."""

    def __init__(self, node, conn, options: Optional[Dict[str, Any]] = None,
                 clock=time.monotonic, wakeup_fd: Optional[int] = None) -> None:
        self._node = node
        self._conn = conn
        self._fd = conn.fileno()
        # What arrived of the messages not handled yet: `Connection` frames.
        self._inbox = bytearray()
        # Readable once a signal came (`signal.set_wakeup_fd`).
        self._wakeup_fd = wakeup_fd
        #: Set by the SIGTERM handler; the loop acts on it between messages.
        self.terminated = False
        self._options = dict(options or {})
        self._clock = clock
        self._writers: Dict[str, _Writer] = {}
        self._retiring: List[_Retiring] = []
        self._self_test_at: Optional[float] = None
        self._next_alive = 0.0
        self._outbox: "queue.SimpleQueue" = queue.SimpleQueue()
        self._sender = threading.Thread(
            target=self._send_loop, name="fleetless-failsafe-sender", daemon=True
        )
        # Message types, loaded off the loop: the first `get_message` of a
        # package imports it, measured at 7-42 ms, and every deadline would
        # wait that long. An arm waits in `_pending_arms` until its type is
        # here; the loader says so through `_loaded_r`.
        self._loaded: Dict[str, type] = {}
        self._loading: set = set()
        self._pending_arms: Dict[str, tuple] = {}
        # Types waiting for a moment with nothing due (`_dispatch_loads`),
        # with when they were asked for; and the one the loader has now.
        self._load_queue: List[tuple] = []
        self._dispatched: Optional[str] = None
        # Owed arms disarmed (or replaced by another topic or type) while
        # their type was loading, as `(slug, arm)`: their failsafe still
        # goes out, from a writer that only retires.
        self._parting_pending: List[tuple] = []
        self._load_requests: "queue.SimpleQueue" = queue.SimpleQueue()
        self._load_results: "queue.SimpleQueue" = queue.SimpleQueue()
        self._loaded_r, self._loaded_w = os.pipe()
        os.set_blocking(self._loaded_r, False)
        os.set_blocking(self._loaded_w, False)
        self._loader = threading.Thread(
            target=self._load_loop, name="fleetless-failsafe-loader", daemon=True
        )

    # --- reporting, off the loop's thread ------------------------------------

    def _report(self, *message) -> None:
        self._outbox.put(message)

    def _send_loop(self) -> None:
        while True:
            message = self._outbox.get()
            if message is None:
                return
            try:
                self._conn.send(message)
            except (OSError, EOFError, ValueError):
                return  # the parent is gone; the loop finds out by its own read

    # --- loading message types, off the loop's thread --------------------------

    def _load_loop(self) -> None:
        from rosidl_runtime_py.utilities import get_message

        while True:
            type_name = self._load_requests.get()
            if type_name is None:
                return
            try:
                message_class = get_message(type_name)
                # The compiled typesupport loads lazily, on the first
                # publisher; here instead, so that load is off the loop too.
                import_type_support = getattr(type(message_class), "__import_type_support__", None)
                if import_type_support is not None:
                    import_type_support()
                self._load_results.put((type_name, message_class, None))
            except Exception:  # noqa: BLE001 - reported by the loop, like any arm that failed
                self._load_results.put((type_name, None, traceback.format_exc()))
            try:
                os.write(self._loaded_w, b"l")
            except BlockingIOError:
                pass  # the pipe is full, so the loop has a wake-up waiting anyway

    def _take_loaded(self) -> None:
        """Arms whose message type the loader has loaded since, on the loop."""
        try:
            while os.read(self._loaded_r, 512):
                pass
        except (BlockingIOError, InterruptedError):
            pass
        while True:
            try:
                type_name, message_class, error = self._load_results.get_nowait()
            except queue.Empty:
                return
            self._loading.discard(type_name)
            if self._dispatched == type_name:
                self._dispatched = None
            waiting = [slug for slug, arm in self._pending_arms.items() if arm[1] == type_name]
            parting = [entry for entry in self._parting_pending if entry[1][1] == type_name]
            self._parting_pending = [e for e in self._parting_pending if e[1][1] != type_name]
            if message_class is None:
                # Every publisher whose owed failsafe is lost with the type,
                # by name.
                lost = [(slug, self._pending_arms[slug][0]) for slug in waiting
                        if self._pending_arms[slug][4]]
                lost += [(slug, arm[0]) for slug, arm in parting]
                for slug in waiting:
                    del self._pending_arms[slug]
                for slug, topic in lost:
                    self._report("owed_failed", slug, topic, error)
                self._report("error", error)
                continue
            self._loaded[type_name] = message_class
            for slug, arm in parting:
                self._retire_owed(slug, arm)
            for slug in waiting:
                self._arm(slug, *self._pending_arms.pop(slug))

    def _dispatch_loads(self, now: float) -> None:
        """Hands the loader the next type, one at a time, once no deadline
        (a fire or the self-test) is due within `IMPORT_MARGIN_S`, or once
        it has waited `IMPORT_CAP_S`."""
        if self._dispatched is not None and not self._loader.is_alive():
            # The loader died (it never should): its type, and every arm
            # waiting for one, would wait for ever. Said, and the type it
            # had goes to a new loader first; the arms keep waiting. A
            # loader that dies again is a second `error`, and two end
            # this process (`failsafe.ERROR_WINDOW_S`).
            self._report("error", "the failsafe process's loader thread ended")
            self._load_requests = queue.SimpleQueue()
            self._load_queue.insert(0, (self._dispatched, self._clock()))
            self._dispatched = None
            self._loader = threading.Thread(
                target=self._load_loop, name="fleetless-failsafe-loader", daemon=True
            )
            self._loader.start()
            return
        if self._dispatched is not None or not self._load_queue:
            return
        type_name, asked_at = self._load_queue[0]
        due = self._next_deadline()
        if due is not None and due - now < IMPORT_MARGIN_S and now - asked_at < IMPORT_CAP_S:
            return
        self._load_queue.pop(0)
        self._dispatched = type_name
        self._load_requests.put(type_name)

    def _next_deadline(self) -> Optional[float]:
        deadlines = [w.touched_at + w.timeout_s for w in self._writers.values()
                     if w.touched_at is not None and not w.fired and w.publisher is not None]
        if self._self_test_at is not None:
            deadlines.append(self._self_test_at)
        return min(deadlines) if deadlines else None

    # --- the loop -------------------------------------------------------------

    def run(self) -> int:
        # What the loader and `_arm` import, imported here, before the loader
        # runs: two threads importing the same package for the first time
        # can deadlock in Python's import locks, and the loser raises
        # `_DeadlockError`.
        from fleetless_bridge import params  # noqa: F401
        from rosidl_runtime_py.utilities import get_message  # noqa: F401

        self._sender.start()
        self._loader.start()
        self._report("ready")
        try:
            while True:
                try:
                    if not self._turn():
                        return 0
                except (_Terminated, EOFError, OSError):
                    raise
                except Exception:  # noqa: BLE001 - reported (the parent counts); the loop goes on
                    self._report("error", traceback.format_exc())
        except (_Terminated, EOFError, OSError):
            self._bridge_gone()
            return 0
        finally:
            self._outbox.put(None)
            self._load_requests.put(None)

    def _turn(self) -> bool:
        """One pass: fire what is due, then handle one message, or wait for
        more of one or for the next thing due. `False` once `stop` was
        handled."""
        if self.terminated:
            raise _Terminated()
        now = self._clock()
        self._fire_due(now)
        if self._self_test_at is not None and now >= self._self_test_at:
            lateness_ms = (self._clock() - self._self_test_at) * 1000.0
            self._self_test_at = None
            self._report("self_test", lateness_ms)
        if now >= self._next_alive:
            self._report("alive")
            self._next_alive = now + ALIVE_INTERVAL_S
        self._take_loaded()
        self._dispatch_loads(now)
        self._create_pending(now)
        self._reap_retiring(now)
        message = self._take_message()
        if message is not None:
            # Due first, handled second: a publish that arrives at the
            # deadline comes after the failsafe it was too late for.
            self._fire_due(self._clock())
            return self._handle(message)
        wake = self._next_wake(now)
        fds = [self._fd, self._loaded_r]
        if self._wakeup_fd is not None:
            fds.append(self._wakeup_fd)
        readable, _, _ = select.select(fds, [], [], max(0.0, wake - self._clock()))
        if self._wakeup_fd is not None and self._wakeup_fd in readable:
            self._drain_wakeup()
        if self._loaded_r in readable:
            self._fire_due(self._clock())
            self._take_loaded()
        if self._fd in readable:
            self._fire_due(self._clock())
            # Readable, so this returns what has arrived and does not wait.
            data = os.read(self._fd, READ_CHUNK_BYTES)
            if not data:
                raise EOFError("the bridge closed the pipe")
            self._inbox += data
        return True

    def _take_message(self):
        """The next whole message in the inbox, or `None` while it is still
        arriving. The framing is `multiprocessing.connection.Connection`'s: a
        signed 4-byte big-endian length, -1 followed by an 8-byte one for a
        message of 2 GiB and more, then the pickle."""
        inbox = self._inbox
        if len(inbox) < 4:
            return None
        (size,) = struct.unpack_from("!i", inbox, 0)
        start = 4
        if size == -1:
            if len(inbox) < 12:
                return None
            (size,) = struct.unpack_from("!Q", inbox, 4)
            start = 12
        end = start + size
        if len(inbox) < end:
            return None
        payload = bytes(inbox[start:end])
        del inbox[:end]
        return pickle.loads(payload)

    def _drain_wakeup(self) -> None:
        try:
            while os.read(self._wakeup_fd, 512):
                pass
        except (BlockingIOError, InterruptedError):
            pass

    def _next_wake(self, now: float) -> float:
        wake = self._next_alive
        if self._self_test_at is not None:
            wake = min(wake, self._self_test_at)
        for writer in self._writers.values():
            if writer.touched_at is not None and not writer.fired:
                wake = min(wake, writer.touched_at + writer.timeout_s)
            if (writer.owed_since is not None or writer.resend_until is not None
                    or writer.publisher is None):
                wake = min(wake, now + RETIRING_CHECK_INTERVAL_S)
        if self._retiring or self._load_queue:
            wake = min(wake, now + RETIRING_CHECK_INTERVAL_S)
        return wake

    def _fire_due(self, now: float) -> None:
        for writer in self._writers.values():
            if writer.touched_at is None or writer.fired or writer.publisher is None:
                continue
            deadline = writer.touched_at + writer.timeout_s
            if now < deadline:
                continue
            try:
                writer.publisher.publish(writer.failsafe)
            except Exception:  # noqa: BLE001 - counted as fired; said loudly
                writer.fired = True
                self._report("fire_failed", writer.slug, traceback.format_exc())
                continue
            writer.fired = True
            lateness_ms = (self._clock() - deadline) * 1000.0
            self._report("fired", writer.slug, int(writer.timeout_s * 1000), lateness_ms, writer.topic)
        for writer in self._writers.values():
            if writer.owed_since is None or writer.publisher is None:
                continue
            matched = writer.publisher.get_subscription_count()
            if matched > 0 or now - writer.owed_since >= OWED_MATCH_WAIT_S:
                writer.owed_since = None
                writer.resend_until = now + OWED_RESEND_WINDOW_S
                writer.resend_matched = matched
                self._publish_parting(writer, report="owed", matched=matched > 0)
        for writer in self._writers.values():
            if writer.resend_until is None or writer.publisher is None:
                continue
            if now >= writer.resend_until:
                writer.resend_until = None
                continue
            # Looked at every `RETIRING_CHECK_INTERVAL_S`: only a reader that
            # leaves while another joins within those 50 ms hides the join.
            matched = writer.publisher.get_subscription_count()
            if matched > writer.resend_matched:
                self._publish_parting(writer, report="owed", matched=True)
            writer.resend_matched = matched

    # --- messages from the bridge ---------------------------------------------

    def _handle(self, message) -> bool:
        kind = message[0]
        if kind == "publish":
            # `seq` numbers the bridge's publishes, for its bookkeeping.
            _, slug, data, seq = message
            writer = self._writers.get(slug)
            if writer is None or writer.publisher is None:
                self._report("dropped", slug)
                return True
            # Owed before it leaves: whatever stops this process from here
            # on finds the command's failsafe owed.
            writer.touched_at = self._clock()
            writer.fired = False
            writer.owed_since = None
            writer.resend_until = None  # a newer command: its own failsafe follows
            writer.publisher.publish(data)
            # Only a command that went out to a reader supersedes what
            # others owed on the topic: one that raised, or that no reader
            # can see yet, cannot overtake their failsafe either.
            if writer.publisher.get_subscription_count() > 0:
                self._supersede_owed(writer.topic, slug, seq)
        elif kind == "arm":
            _, slug, topic, type_name, body, timeout_ms, owed = message
            self._arm(slug, topic, type_name, body, timeout_ms, owed)
        elif kind == "disarm":
            pending = self._pending_arms.pop(message[1], None)
            if pending is not None and pending[4]:
                # Owed and its type still loading: its failsafe goes out
                # once the loader has the type, which keeps the import off
                # this loop while other publishers are still timed.
                self._parting_pending.append((message[1], pending))
            self._disarm(message[1], parting=True)
        elif kind == "self_test":
            block_s = self._options.get("block_before_self_test_s", 0.0)
            if block_s:
                time.sleep(block_s)  # a test's seam: a self-test made late on purpose
                self._options["block_before_self_test_s"] = 0.0
                self._self_test_at = self._clock() - block_s + SELF_TEST_DELAY_S
            else:
                self._self_test_at = self._clock() + SELF_TEST_DELAY_S
        elif kind == "count":
            # How many readers a slug's writer matched: for the bridge's tests
            # and diagnostics, never on a fire's path.
            _, request_id, slug = message
            writer = self._writers.get(slug)
            count = writer.publisher.get_subscription_count() if writer and writer.publisher else -1
            self._report("count", request_id, count)
        elif kind == "stop":
            # The runtime's teardown disarmed every publisher first, so
            # nothing is owed here, unless that teardown did not run or a
            # failsafe owed from a previous process still waits for its
            # type or a reader.
            for slug in list(self._writers):
                self._disarm(slug, parting=True)
            self._retire_owed_pending()
            self._wait_retiring()
            return False
        return True

    def _arm(self, slug, topic, type_name, body, timeout_ms, owed) -> None:
        from fleetless_bridge import params

        existing = self._writers.get(slug)
        if existing is not None and (existing.topic != topic or existing.type_name != type_name):
            self._disarm(slug, parting=True)
            existing = None
        if existing is not None:
            existing.failsafe = params.build_message(existing.message_class, body)
            existing.timeout_s = timeout_ms / 1000.0
            return
        previous = self._pending_arms.get(slug)
        if previous is not None and previous[4]:
            # A newer arm replaces a waiting one, but not the failsafe that
            # one owed: kept for the same topic and type, parted otherwise.
            if previous[0] == topic and previous[1] == type_name:
                owed = True
            else:
                del self._pending_arms[slug]
                self._parting_pending.append((slug, previous))
        message_class = self._loaded.get(type_name)
        if message_class is None:
            # Not loaded yet: the loader imports it, and this arm waits (a
            # newer arm of the slug replaces it). Until then the slug has no
            # writer, and a publish to it is reported dropped.
            self._pending_arms[slug] = (topic, type_name, body, timeout_ms, owed)
            if type_name not in self._loading:
                self._loading.add(type_name)
                self._load_queue.append((type_name, self._clock()))
            return
        self._pending_arms.pop(slug, None)
        writer = _Writer(
            slug=slug, topic=topic, type_name=type_name, message_class=message_class,
            failsafe=params.build_message(message_class, body), timeout_s=timeout_ms / 1000.0,
        )
        now = self._clock()
        if any(r.topic == topic and r.type_name != type_name for r in self._retiring):
            writer.create_after = now + TYPE_CHANGE_ACK_WAIT_S
        if owed:
            writer.owed_since = now
        self._writers[slug] = writer
        self._create_pending(now)

    def _create_pending(self, now: float) -> None:
        for writer in self._writers.values():
            if writer.publisher is not None:
                continue
            blocking = [r for r in self._retiring
                        if r.topic == writer.topic and r.type_name != writer.type_name]
            if blocking and now < writer.create_after:
                continue
            for retiring in blocking:
                self._destroy_retiring(retiring, acked=self._acked(retiring.publisher))
                self._retiring.remove(retiring)
            writer.publisher = self._node.create_publisher(
                writer.message_class, writer.topic, QOS_DEPTH
            )
            if writer.owed_since is not None:
                writer.owed_since = now

    def _disarm(self, slug: str, parting: bool) -> None:
        writer = self._writers.pop(slug, None)
        if writer is None or writer.publisher is None:
            return
        touched = writer.touched_at is not None and not writer.fired
        # A previous process's failsafe, not sent yet, or sent with perhaps
        # no reader of the robot matched (`resend_until`).
        inherited = writer.owed_since is not None or writer.resend_until is not None
        now = self._clock()
        if parting and touched:
            self._publish_parting(writer)
        elif parting and inherited:
            retiring = self._owed_retiring(
                slug, writer.topic, writer.type_name, writer.publisher, writer.failsafe, now)
            if writer.owed_since is None:
                # Sent already, to the readers matched then: again only for
                # one that matched since, as the resend does.
                retiring.matched_floor = writer.resend_matched
            self._retiring.append(retiring)
            return
        if writer.touched_at is None and not inherited:
            self._node.destroy_publisher(writer.publisher)
            return
        self._retiring.append(_Retiring(
            slug=slug, topic=writer.topic, type_name=writer.type_name,
            publisher=writer.publisher, deadline=now + PARTING_FAILSAFE_ACK_TIMEOUT_S,
        ))

    @staticmethod
    def _owed_retiring(slug, topic, type_name, publisher, failsafe, now) -> "_Retiring":
        """A retiring writer that still owes its failsafe: sent once a reader
        matched or after `OWED_MATCH_WAIT_S`, then acknowledged."""
        send_by = now + OWED_MATCH_WAIT_S
        return _Retiring(
            slug=slug, topic=topic, type_name=type_name, publisher=publisher,
            deadline=send_by + PARTING_FAILSAFE_ACK_TIMEOUT_S, failsafe=failsafe, send_by=send_by,
        )

    def _retire_owed(self, slug: str, arm: tuple) -> None:
        """An owed arm that went before it had a writer: a writer only to
        send that failsafe, retiring from the start. Its type is loaded."""
        from fleetless_bridge import params

        topic, type_name, body, _, _ = arm
        try:
            message_class = self._loaded[type_name]
            publisher = self._node.create_publisher(message_class, topic, QOS_DEPTH)
            failsafe = params.build_message(message_class, body)
        except Exception:  # noqa: BLE001 - said loudly; the next one still has to go
            self._report("owed_failed", slug, topic, traceback.format_exc())
            return
        self._retiring.append(self._owed_retiring(
            slug, topic, type_name, publisher, failsafe, self._clock()))

    def _await_loader(self, type_name: str) -> None:
        """When the loader is importing `type_name` right now, its result,
        `IMPORT_CAP_S` at most: this thread importing the same package at
        the same time could deadlock in Python's import locks."""
        deadline = time.monotonic() + IMPORT_CAP_S
        while self._dispatched == type_name and self._loader.is_alive():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                loaded, message_class, _ = self._load_results.get(timeout=remaining)
            except queue.Empty:
                return
            self._loading.discard(loaded)
            if self._dispatched == loaded:
                self._dispatched = None
            if message_class is not None:
                self._loaded[loaded] = message_class

    def _retire_owed_pending(self) -> None:
        """Every failsafe still waiting for its type, at a stop or with the
        bridge gone: the types loaded here, on this thread, since nothing
        is timed any more."""
        from rosidl_runtime_py.utilities import get_message

        owed = [(slug, arm) for slug, arm in self._pending_arms.items() if arm[4]]
        owed += self._parting_pending
        self._pending_arms.clear()
        self._parting_pending = []
        hung = set()
        for slug, arm in owed:
            if arm[1] not in hung:
                self._await_loader(arm[1])
            if self._dispatched == arm[1] and self._loader.is_alive():
                hung.add(arm[1])
                # The loader is stuck inside this type's import and holds
                # its module lock: an import here would wait on it too. Said,
                # and the other types go on.
                self._report("owed_failed", slug, arm[0],
                             "the loader thread still imports {} after {:.0f} s".format(
                                 arm[1], IMPORT_CAP_S))
                continue
            try:
                if arm[1] not in self._loaded:
                    self._loaded[arm[1]] = get_message(arm[1])
            except Exception:  # noqa: BLE001 - said loudly; the next one still has to go
                self._report("owed_failed", slug, arm[0], traceback.format_exc())
                continue
            self._retire_owed(slug, arm)

    def _publish_parting(self, writer: _Writer, report: str = "parting",
                         matched: bool = True) -> None:
        """A failsafe now, outside its deadline: `parting` for a writer that
        goes (removed, or the bridge gone), `owed` for one a previous
        process owed, with whether a reader was matched to receive it."""
        try:
            writer.publisher.publish(writer.failsafe)
        except Exception:  # noqa: BLE001 - said loudly; nothing else to do
            writer.fired = True
            if report == "owed":
                self._report("fire_failed", writer.slug, traceback.format_exc())
            else:
                self._report("owed_failed", writer.slug, writer.topic, traceback.format_exc())
            return
        writer.fired = True
        if report == "owed":
            self._report(report, writer.slug, matched)
        else:
            self._report(report, writer.slug, writer.topic)

    # --- retiring writers -------------------------------------------------------

    def _acked(self, publisher) -> bool:
        from rclpy.duration import Duration

        if self._options.get("never_acked"):
            return False  # a test's seam: the reader that went away without unmatching
        try:
            return bool(publisher.wait_for_all_acked(Duration(nanoseconds=0)))
        except Exception:  # noqa: BLE001 - an RMW without the call: nothing to wait for
            return True

    def _supersede_owed(self, topic: str, by_slug: str, seq: int) -> None:
        """A command on `topic` that reached a reader: every failsafe a
        previous process owed there, by any other slug, is not sent any
        more. Sent first would not do: DDS keeps no order across writers,
        and the robot could end on the old failsafe. The command's own
        failsafe follows it. A failsafe already sent can still arrive
        after it by a reliable resend; that is the safe direction."""
        # Removed writers' failsafes, of any slug.
        removed = []
        for retiring in self._retiring:
            if retiring.topic == topic and retiring.send_by is not None:
                retiring.send_by = None
                removed.append(retiring.slug)
        removed += [slug for slug, arm in self._parting_pending if arm[0] == topic]
        self._parting_pending = [(s, a) for s, a in self._parting_pending if a[0] != topic]
        # Other publishers' inherited failsafes on the topic, armed or
        # waiting for their type; their own commands' failsafes stand.
        active = []
        for writer in self._writers.values():
            if writer.slug == by_slug or writer.topic != topic:
                continue
            if writer.owed_since is not None or writer.resend_until is not None:
                writer.owed_since = None
                writer.resend_until = None
                active.append(writer.slug)
        for slug, arm in list(self._pending_arms.items()):
            if slug != by_slug and arm[0] == topic and arm[4]:
                self._pending_arms[slug] = arm[:4] + (False,)
                active.append(slug)
        for slug in removed:
            self._report("superseded", slug, topic)
        for slug in active:
            # With the superseding publish's number: the bridge keeps the
            # slug owed if it published since.
            self._report("superseded_owed", slug, topic, seq)

    def _send_owed(self, retiring: _Retiring) -> None:
        try:
            retiring.publisher.publish(retiring.failsafe)
        except Exception:  # noqa: BLE001 - said loudly; nothing else to do
            self._report("owed_failed", retiring.slug, retiring.topic, traceback.format_exc())
        else:
            self._report("parting", retiring.slug, retiring.topic)
        retiring.send_by = None

    def _reap_retiring(self, now: float) -> None:
        keep: List[_Retiring] = []
        for retiring in self._retiring:
            if retiring.send_by is not None:
                matched = retiring.publisher.get_subscription_count()
                if matched <= retiring.matched_floor and now < retiring.send_by:
                    keep.append(retiring)
                    continue
                if matched > retiring.matched_floor or retiring.matched_floor == 0:
                    self._send_owed(retiring)
                else:
                    # Every reader still there had it; nobody new came.
                    self._report("parting", retiring.slug, retiring.topic)
                    retiring.send_by = None
            acked = self._acked(retiring.publisher)
            if not acked and now < retiring.deadline:
                keep.append(retiring)
                continue
            self._destroy_retiring(retiring, acked)
        self._retiring = keep

    def _destroy_retiring(self, retiring: _Retiring, acked: bool) -> None:
        if retiring.send_by is not None:
            # A new writer of another type needs the topic: whatever was
            # still owed goes out now, matched or not.
            self._send_owed(retiring)
        if not acked:
            self._report("unacked", retiring.slug, PARTING_FAILSAFE_ACK_TIMEOUT_S)
        try:
            self._node.destroy_publisher(retiring.publisher)
        except Exception:  # noqa: BLE001 - the next one still has to go
            self._report("error", traceback.format_exc())

    def _wait_retiring(self) -> None:
        while self._retiring:
            now = self._clock()
            self._reap_retiring(now)
            if self._retiring:
                time.sleep(RETIRING_CHECK_INTERVAL_S)

    def _bridge_gone(self) -> None:
        """The pipe closed or SIGTERM came: every armed failsafe goes out
        now, and the process waits for its acknowledgement before it exits.
        `terminating` first: the supervisor waits for the acknowledgements
        only for a process that answered its SIGTERM. The writers' own
        failsafes first; then those owed from a previous process that still
        wait for their type, loaded here, on this thread: nothing is timed
        any more. Those go out once a reader matched, or after
        `OWED_MATCH_WAIT_S`."""
        self._report("terminating")
        for slug in list(self._writers):
            self._disarm(slug, parting=True)
        self._retire_owed_pending()
        self._wait_retiring()


def main(conn, node_name: str, options: Optional[Dict[str, Any]] = None) -> None:
    """The process's loop, on `conn` (a `multiprocessing.connection.Connection`)."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Until the loop runs nothing is armed, so a SIGTERM owes nothing: out at
    # once. Raising here instead would land inside rclpy's own C import, which
    # turns any exception into an ImportError with a stack trace.
    def _quiet_exit(signum, frame):
        os._exit(0)

    signal.signal(signal.SIGTERM, _quiet_exit)
    import rclpy
    from rclpy.signals import SignalHandlerOptions

    rclpy.init(args=[], signal_handler_options=SignalHandlerOptions.NO)
    node = rclpy.create_node(node_name)
    wakeup_r, wakeup_w = os.pipe()
    os.set_blocking(wakeup_r, False)
    os.set_blocking(wakeup_w, False)
    process = FailsafeProcess(node, conn, options, wakeup_fd=wakeup_r)

    def _on_term(signum, frame):
        # From here on a SIGTERM fires every armed failsafe, like a closed
        # pipe. Only a flag: the loop acts on it between messages. A second
        # SIGTERM sets it again and cuts nothing short.
        process.terminated = True

    signal.signal(signal.SIGTERM, _on_term)
    signal.set_wakeup_fd(wakeup_w)
    # The loader and the sender share this interpreter's GIL with the loop;
    # a shorter switch interval bounds how long the loop waits for it.
    sys.setswitchinterval(0.001)
    try:
        process.run()
    finally:
        try:
            node.destroy_node()
            rclpy.shutdown()
        except Exception:  # noqa: BLE001 - exiting anyway
            pass

if __name__ == "__main__":
    # `python3 -m fleetless_bridge.failsafe_process <fd> <node name> <options>`,
    # as `failsafe.FailsafeWatchdog` starts it: the fd is its end of a socket
    # pair, the options are JSON. Started this way, and not by
    # `multiprocessing`'s spawn, because spawn re-imports the bridge's own
    # `__main__`, and with it every camera library the bridge loads.
    import json
    import sys
    from multiprocessing.connection import Connection

    main(Connection(int(sys.argv[1])), sys.argv[2], json.loads(sys.argv[3]))
