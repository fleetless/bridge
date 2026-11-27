# SPDX-License-Identifier: Apache-2.0
"""The failsafe watchdog: the bridge's side of the failsafe process
(fleetless/fleetless#1124; the child is `failsafe_process`).

What the bridge sees:

- `start()` starts the supervisor, which spawns the process and self-tests it:
  the process arms an internal deadline 50 ms ahead and says how late it
  fired. At most `LATENESS_BOUND_MS` late is `ok`; anything else is a failed
  watchdog. Until then the state is `starting`.
- `publish(slug, msg)` serializes a client's message and sends it to the
  process, which owns the publisher's only writer. It returns `False` and
  sends nothing while the watchdog is not `ok`, `starting` included. That is fail closed:
  without a watchdog that meets its bound, no publisher with a failsafe takes
  a publish.
- `arm` and `disarm` follow config apply and removal. The arms are remembered
  here, so a new process gets them all back.

The **supervisor thread** reads the process's reports and watches three things.
Any of them makes the watchdog `failed`:

- a fire later than the bound (`late_fire`);
- a second without an `alive` (`not_alive`; one is sent every 0.25 s);
- the process ending, or two loop errors within 60 s (`crashed`).

It does not run on the executor's heartbeat timer, for one reason: the
executor's own send to a stuck process is what could block it. On a failure
the supervisor stops taking publishes, logs an ERROR and tells the runtime
(`on_state`, which becomes the `failsafe_watchdog` frame). It terminates the
old process by its PID. A process that answers the SIGTERM within
`TERM_ANSWER_TIMEOUT_S` fires every armed failsafe and gets the time its
readers need to acknowledge them; one that does not answer (stopped, or stuck
in C) is SIGKILLed, and the new process fires what it owed. Then the
supervisor starts a new one, re-arms it and self-tests it, and retries every
second until a self-test passes.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from multiprocessing.connection import Connection
from typing import Any, Callable, Dict, List, Optional, Tuple

from fleetless_bridge.failsafe_process import LATENESS_BOUND_MS, PARTING_FAILSAFE_ACK_TIMEOUT_S

log = logging.getLogger(__name__)

#: Where `fleetless_bridge` was loaded from: the failsafe process starts there.
_PACKAGE_PARENT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: No word from the process for this long is a dead watchdog.
ALIVE_TIMEOUT_S = 1.0

#: Spawning a Python with rclpy on a slow robot; a fast machine takes about a second.
START_TIMEOUT_S = 20.0

#: How long a SIGTERM'd process has to say `terminating`. One that did not
#: is SIGKILLed: a frozen process runs no handler, and every second it is
#: waited for is a second the robot stays on its last command.
TERM_ANSWER_TIMEOUT_S = 1.0

#: How long the self-test's answer may take, beyond its own 50 ms.
SELF_TEST_TIMEOUT_S = 5.0

#: Two loop errors within this window are a crash.
ERROR_WINDOW_S = 60.0

#: How often a failed watchdog tries a new process.
RETRY_INTERVAL_S = 1.0

#: How often the supervisor wakes when nothing arrives.
SUPERVISE_INTERVAL_S = 0.1

STATE_STARTING = "starting"
STATE_OK = "ok"
STATE_FAILED = "failed"

REASON_SELF_TEST = "self_test"
REASON_LATE_FIRE = "late_fire"
REASON_NOT_ALIVE = "not_alive"
REASON_CRASHED = "crashed"

OnState = Callable[[str, Optional[str], Optional[float]], None]


class _Process:
    """The failsafe process, by its PID: what the supervisor asks of it."""

    def __init__(self, popen: subprocess.Popen) -> None:
        self._popen = popen

    @property
    def pid(self) -> int:
        return self._popen.pid

    @property
    def exitcode(self) -> Optional[int]:
        return self._popen.poll()

    def is_alive(self) -> bool:
        return self._popen.poll() is None

    def terminate(self) -> None:
        if self.is_alive():
            self._popen.terminate()

    def kill(self) -> None:
        if self.is_alive():
            self._popen.kill()

    def join(self, timeout_s: float) -> None:
        try:
            self._popen.wait(timeout_s)
        except subprocess.TimeoutExpired:
            pass


def _writable(conn, timeout_s: float) -> bool:
    import select

    try:
        _, writable, _ = select.select([], [conn.fileno()], [], timeout_s)
    except (OSError, ValueError):
        return False
    return bool(writable)


#: Fast DDS's choice of built-in transports, read from the environment.
TRANSPORTS_VARIABLE = "FASTDDS_BUILTIN_TRANSPORTS"

#: The failsafe process's transports. Fast DDS 2.x shared memory sleeps up
#: to 333 ms in `Port::healthy_check` the first time a process sends to a
#: port more than about a second old whose listener looks idle, and it does
#: so under a lock every send of that participant waits on. A new failsafe
#: process opens the bridge's own long-lived port that way, so a fire could
#: wait a third of a second (fleetless/fleetless#1124). Over UDP the process
#: opens no shared-memory port at all.
CHILD_TRANSPORTS = "UDPv4"


def child_environment(environ) -> Dict[str, str]:
    """The failsafe process's environment: the bridge's own, so it finds
    the same ROS and the same domain, with three changes.

    - `PYTHONPATH` keeps the bridge's order, so an overlay workspace still
      wins over the base install and the child has the bridge's message
      types. The directory `fleetless_bridge` was loaded from is appended
      only when it is not on it already (a sourced install has it).
    - `-m` would put the working directory first on `sys.path`, where a
      `queue.py` or a `fleetless_bridge/` checkout would shadow what the
      child imports. The child starts in `/`, with `PYTHONSAFEPATH` (Python
      3.11 and later) keeping even that off the path.
    - `FASTDDS_BUILTIN_TRANSPORTS=UDPv4` (`CHILD_TRANSPORTS`), unless the
      robot set the variable to something other than `DEFAULT`: an explicit
      choice is kept, and the bridge says what it costs. A Fast DDS profile
      that defines its own transports overrides the variable in either case.
    """
    env = dict(environ)
    entries = [e for e in env.get("PYTHONPATH", "").split(os.pathsep) if e]
    if not any(os.path.abspath(e) == _PACKAGE_PARENT for e in entries):
        entries.append(_PACKAGE_PARENT)
    env["PYTHONSAFEPATH"] = "1"
    env["PYTHONPATH"] = os.pathsep.join(entries)
    if env.get(TRANSPORTS_VARIABLE, "").strip().upper() in ("", "DEFAULT"):
        env[TRANSPORTS_VARIABLE] = CHILD_TRANSPORTS
    return env


class _Gone(Exception):
    """The process ended or did not answer in time."""


class FailsafeWatchdog:
    def __init__(
        self,
        node_name: str,
        on_state: Optional[OnState] = None,
        options: Optional[Dict[str, Any]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._node_name = node_name
        self._on_state = on_state
        self._options = dict(options or {})
        self._clock = clock
        # Guards state and the arms; never held across a blocking call, so
        # the supervisor can always declare a failure.
        self._lock = threading.RLock()
        # Held around every send: the executor and the supervisor both send,
        # and a `Connection` is not safe for two senders. A send to a stuck
        # process can block under it until the supervisor ends that process.
        self._send_lock = threading.Lock()
        self._state = STATE_STARTING
        self._reason: Optional[str] = None
        self._lateness_ms: Optional[float] = None
        self._conn = None
        self._process = None
        self._armed: Dict[str, Tuple[str, str, Any, int]] = {}
        # Published since its last failsafe: owed if the process dies.
        self._owed: Dict[str, bool] = {}
        self._last_word_at = 0.0
        self._errors: List[float] = []
        self._stopping = False
        # The current process said `ready`.
        self._ready = False
        # Whether any publish went to a process: only then can a parting
        # failsafe be owed, and only then does `stop()` wait for one.
        self._ever_published = False
        self._supervisor: Optional[threading.Thread] = None
        self._log_limit: Dict[Tuple[str, str], float] = {}
        self._request_seq = 0
        self._requests: Dict[int, list] = {}
        self._transports_warned = False

    # --- what the runtime asks -------------------------------------------------

    @property
    def state(self) -> str:
        return self._state

    def snapshot(self) -> Tuple[str, Optional[str], Optional[float]]:
        with self._lock:
            return self._state, self._reason, self._lateness_ms

    @property
    def pid(self) -> Optional[int]:
        process = self._process
        return process.pid if process is not None else None

    def start(self) -> None:
        """Starts the supervisor, which spawns the process and self-tests it,
        and returns at once. Until the self-test passed the state is
        `starting`, and no publisher takes a publish. Not waited for here:
        the bridge's start must stay as quick to stop as it was, and a
        stop during that first second ends the process at once."""
        self._supervisor = threading.Thread(
            target=self._supervise, name="fleetless-failsafe-supervisor", daemon=True
        )
        self._supervisor.start()

    def _first_start(self) -> None:
        try:
            lateness_ms = self._spawn_and_test()
        except Exception as exc:  # noqa: BLE001 - becomes the failed state, retried
            if self._stopping:
                return
            log.error("Failsafe watchdog: did not start (%s); publishers take no publishes", exc)
            self._set_failed(REASON_SELF_TEST, None)
        else:
            self._judge_self_test(lateness_ms)

    def arm(self, slug: str, topic: str, type_name: str, body: Any, timeout_ms: int) -> None:
        with self._lock:
            self._armed[slug] = (topic, type_name, body, timeout_ms)
            self._owed.setdefault(slug, False)
        self._send(("arm", slug, topic, type_name, body, timeout_ms, False))

    def disarm(self, slug: str) -> None:
        """The process sends the parting failsafe if one is owed and keeps the
        writer until its readers acknowledged it."""
        with self._lock:
            self._armed.pop(slug, None)
            self._owed.pop(slug, None)
        self._send(("disarm", slug))

    def publish(self, slug: str, msg: Any) -> bool:
        from rclpy.serialization import serialize_message

        data = serialize_message(msg)
        with self._lock:
            if self._state != STATE_OK or slug not in self._armed:
                return False
            self._owed[slug] = True
            self._ever_published = True
        return self._send(("publish", slug, data))

    def subscription_count(self, slug: str, timeout_s: float = 2.0) -> int:
        """How many readers the slug's writer in the process matched; -1 when
        it has no writer, or no answer came within `timeout_s`."""
        with self._lock:
            self._request_seq += 1
            request_id = self._request_seq
            answer = [threading.Event(), -1]
            self._requests[request_id] = answer
        if self._send(("count", request_id, slug)):
            answer[0].wait(timeout_s)
        with self._lock:
            self._requests.pop(request_id, None)
        return answer[1]

    def stop(self) -> None:
        """Tells the process to stop and waits for it. The process disarms
        everything with the parting failsafe of any writer still owed one:
        none, when the runtime's teardown disarmed its publishers first, as
        it does unless that teardown timed out."""
        with self._lock:
            # Under the lock the supervisor spawns under: no process starts
            # after this line, and the one that did is in `_process`.
            self._stopping = True
            process = self._process
            ready = self._ready
            owed_possible = self._ever_published
        conn = self._conn
        if process is not None and not ready:
            # Still starting: it took no publish (they wait for its
            # self-test), so nothing is owed. SIGKILLed, not asked: a
            # process still importing runs no signal handler until the
            # import returns, and the bridge's stop would wait for that.
            process.kill()
            process.join(0.1)
        elif process is not None and not owed_possible:
            # Nothing was ever published, so no failsafe can be owed and no
            # writer is retiring: told to stop, so its writers leave their
            # readers at once, and not waited for. It ends on its own, and
            # on the closed pipe at the latest.
            if conn is not None and _writable(conn, 0.1):
                self._send(("stop",))
        else:
            # Only to a process that is reading: a stuck one would block the
            # send, and the bridge's own shutdown with it. Ended below.
            if conn is not None and _writable(conn, 1.0):
                self._send(("stop",))
            if process is not None:
                process.join(PARTING_FAILSAFE_ACK_TIMEOUT_S + 1.0)
                # Not read here: the supervisor may still be reading. A
                # process still there after the stop's own wait is stuck.
                self._end(process, None)
        if self._supervisor is not None:
            self._supervisor.join(2.0)

    # --- the process -------------------------------------------------------------

    def _send(self, message) -> bool:
        with self._send_lock:
            return self._send_locked(message)

    def _send_locked(self, message) -> bool:
        """`_send` for a caller that holds `_send_lock`."""
        conn = self._conn
        if conn is None:
            return False
        try:
            conn.send(message)
            return True
        except (OSError, EOFError, ValueError):
            return False  # the supervisor sees the process gone on its own read

    def _spawn_and_test(self) -> float:
        """A new process, re-armed and self-tested; returns the self-test's
        lateness. Raises `_Gone` when the process does not answer."""
        parent_sock, child_sock = socket.socketpair()
        parent_conn = Connection(parent_sock.detach())
        child_fd = child_sock.detach()
        options = dict(self._options)
        # A test's seam for a late self-test applies to the first process
        # only, so that a test can watch the watchdog recover.
        self._options.pop("block_before_self_test_s", None)
        self._lock.acquire()
        try:
            if self._stopping:
                parent_conn.close()
                raise _Gone("the bridge is stopping")
            self._ready = False
            # An argument list, no shell: this interpreter, the module, its
            # end of the socket pair, and `child_environment`.
            env = child_environment(os.environ)
            if env.get(TRANSPORTS_VARIABLE) != CHILD_TRANSPORTS:
                self._warn_transports_once(env.get(TRANSPORTS_VARIABLE))
            process = _Process(subprocess.Popen(
                [sys.executable, "-m", "fleetless_bridge.failsafe_process",
                 str(child_fd), self._node_name + "_failsafe", json.dumps(options)],
                pass_fds=(child_fd,),
                stdin=subprocess.DEVNULL,
                cwd="/",
                env=env,
            ))
            self._process = process
        finally:
            self._lock.release()
            os.close(child_fd)
        with self._send_lock:
            self._conn = parent_conn
        self._await(parent_conn, "ready", START_TIMEOUT_S)
        self._ready = True
        # The snapshot and its resend under one hold of the send lock: an
        # `arm` or `disarm` from the executor goes out before the snapshot
        # (and is in it) or after the resend, never between them, where the
        # older arm would overwrite it.
        with self._send_lock:
            with self._lock:
                arms = [(slug, arm, self._owed.get(slug, False)) for slug, arm in self._armed.items()]
            for slug, (topic, type_name, body, timeout_ms), owed in arms:
                self._send_locked(("arm", slug, topic, type_name, body, timeout_ms, owed))
            self._send_locked(("self_test",))
        reply = self._await(parent_conn, "self_test", SELF_TEST_TIMEOUT_S)
        return float(reply[1])

    def _warn_transports_once(self, value: Optional[str]) -> None:
        if self._transports_warned:
            return
        self._transports_warned = True
        log.warning(
            "Failsafe watchdog: %s=%s is kept for the failsafe process, which otherwise uses "
            "%s. With shared memory, a failsafe right after the process is restarted can go "
            "out up to 333 ms late (Fast DDS port health check); the watchdog then reports "
            "late_fire.", TRANSPORTS_VARIABLE, value, CHILD_TRANSPORTS,
        )

    def _await(self, conn, kind: str, timeout_s: float):
        deadline = self._clock() + timeout_s
        while True:
            remaining = min(deadline - self._clock(), SUPERVISE_INTERVAL_S)
            if self._stopping:
                raise _Gone("the bridge is stopping")
            if deadline - self._clock() <= 0:
                raise _Gone("no {!r} from the failsafe process within {:.0f} s".format(kind, timeout_s))
            try:
                if not conn.poll(remaining):
                    continue
                message = conn.recv()
            except (OSError, EOFError) as exc:
                raise _Gone("the failsafe process ended before {!r}".format(kind)) from exc
            self._last_word_at = self._clock()
            if message[0] == kind:
                return message
            self._handle(message)

    def _end(self, process, conn) -> None:
        """Ends one process of ours by its PID. SIGTERM first: a process that
        answers fires every armed failsafe and is waited for until its
        readers acknowledged them. One that does not answer within
        `TERM_ANSWER_TIMEOUT_S` is SIGKILLed. `conn` is read for the answer,
        so only its one reader passes it; without it the process gets only
        `TERM_ANSWER_TIMEOUT_S`."""
        if process.is_alive():
            process.terminate()
            if self._answered_term(process, conn):
                process.join(PARTING_FAILSAFE_ACK_TIMEOUT_S + 1.0)
        if process.is_alive():
            process.kill()
            process.join(1.0)

    def _answered_term(self, process, conn) -> bool:
        """Whether the process said `terminating`, or ended, within
        `TERM_ANSWER_TIMEOUT_S`. Its other reports on the way are handled."""
        deadline = self._clock() + TERM_ANSWER_TIMEOUT_S
        if conn is None:
            process.join(TERM_ANSWER_TIMEOUT_S)
            return False
        while True:
            remaining = deadline - self._clock()
            if remaining <= 0:
                # Once more without waiting, as `_word_waiting` does: this
                # thread can wake long after its deadline under GIL load, and
                # an answer already waiting is an answer.
                remaining = 0.0
                try:
                    if not conn.poll(0):
                        return False
                except (OSError, EOFError):
                    return True
            try:
                if remaining > 0 and not conn.poll(min(remaining, SUPERVISE_INTERVAL_S)):
                    if not process.is_alive():
                        return True
                    continue
                message = conn.recv()
            except (OSError, EOFError):
                return True  # its end closed: it is exiting
            if message[0] == "terminating":
                return True
            self._handle(message)

    # --- the supervisor ------------------------------------------------------------

    def _supervise(self) -> None:
        self._first_start()
        while not self._stopping:
            if self._state == STATE_FAILED:
                self._restart()
                continue
            conn = self._conn
            try:
                if conn is not None and conn.poll(SUPERVISE_INTERVAL_S):
                    message = conn.recv()
                    self._last_word_at = self._clock()
                    self._handle(message)
            except (OSError, EOFError):
                if self._stopping:
                    return
                log.error("Failsafe watchdog: the failsafe process ended (exit code %s)",
                          self._process.exitcode if self._process is not None else None)
                self._fail(REASON_CRASHED, None)
                continue
            if (self._state == STATE_OK
                    and self._clock() - self._last_word_at > ALIVE_TIMEOUT_S
                    and not self._word_waiting(conn)):
                log.error("Failsafe watchdog: no word from the failsafe process for %.1f s",
                          self._clock() - self._last_word_at)
                self._fail(REASON_NOT_ALIVE, None)

    def _word_waiting(self, conn) -> bool:
        """Whether the process has said something this thread has not read
        yet. This thread lives in the bridge's interpreter: under GIL load it
        can wake long after its poll returned, and its own lateness is not
        the process's silence."""
        try:
            return conn is not None and conn.poll(0)
        except (OSError, EOFError):
            return False

    def _handle(self, message) -> None:
        kind = message[0]
        if kind == "fired":
            _, slug, timeout_ms, lateness_ms = message
            with self._lock:
                if slug in self._owed:
                    self._owed[slug] = False
            log.warning(
                "Publisher slug %r: no publish for %d ms — the bridge published its failsafe "
                "(%.1f ms after timeout)", slug, timeout_ms, lateness_ms,
            )
            if lateness_ms > LATENESS_BOUND_MS:
                log.error(
                    "Publisher slug %r: the failsafe went out %.1f ms after its timeout, over "
                    "the %.0f ms bound", slug, lateness_ms, LATENESS_BOUND_MS,
                )
                self._fail(REASON_LATE_FIRE, lateness_ms)
        elif kind == "parting":
            with self._lock:
                if message[1] in self._owed:
                    self._owed[message[1]] = False
            log.warning("Publisher slug %r: removed while driven — the bridge published its failsafe",
                        message[1])
        elif kind == "owed":
            _, slug, matched = message
            if not matched:
                # Sent to nobody: still owed, so a process that replaces
                # this one before a reader matched sends it too.
                log.warning("Publisher slug %r: its failsafe was owed when the failsafe process was "
                            "replaced — the bridge published it with no reader matched yet, and "
                            "publishes it again as readers match", slug)
                return
            with self._lock:
                if slug in self._owed:
                    self._owed[slug] = False
            log.warning("Publisher slug %r: its failsafe was owed when the failsafe process was "
                        "replaced — the bridge published it", slug)
        elif kind == "fire_failed":
            log.error(
                "Publisher slug %r: its failsafe could not be published — nothing was sent, "
                "which is worse than a stale value, so this is logged loudly:\n%s",
                message[1], message[2],
            )
        elif kind == "dropped":
            self._limited(message[1], "dropped", logging.WARNING,
                          "Publisher slug %r: publish dropped — its writer is not created yet", message[1])
        elif kind == "unacked":
            log.warning(
                "Publisher slug %r: its last failsafe was not acknowledged within %.1f s; "
                "destroying the publisher anyway", message[1], message[2],
            )
        elif kind == "error":
            now = self._clock()
            self._errors = [t for t in self._errors if now - t < ERROR_WINDOW_S] + [now]
            log.error("Failsafe watchdog: error in the failsafe process:\n%s", message[1])
            if len(self._errors) >= 2:
                self._errors = []
                self._fail(REASON_CRASHED, None)
        elif kind == "count":
            with self._lock:
                answer = self._requests.get(message[1])
            if answer is not None:
                answer[1] = message[2]
                answer[0].set()
        elif kind == "self_test":
            pass  # a late answer to a test already judged
        elif kind == "terminating":
            pass  # a SIGTERM this process did not send, or one already judged
        # "alive" and "ready" only refresh `_last_word_at`.

    def _limited(self, slug: str, what: str, level: int, fmt: str, *args) -> None:
        now = self._clock()
        if now - self._log_limit.get((slug, what), -1e9) < 1.0:
            return
        self._log_limit[(slug, what)] = now
        log.log(level, fmt, *args)

    def _judge_self_test(self, lateness_ms: float) -> None:
        log.info("failsafe watchdog: self-test late by %.1f ms", lateness_ms)
        if lateness_ms > LATENESS_BOUND_MS:
            log.error(
                "Failsafe watchdog: the self-test was %.1f ms late, over the %.0f ms bound; "
                "publishers take no publishes", lateness_ms, LATENESS_BOUND_MS,
            )
            self._set_failed(REASON_SELF_TEST, lateness_ms)
            return
        with self._lock:
            self._state, self._reason, self._lateness_ms = STATE_OK, None, lateness_ms
        self._notify()

    def _set_failed(self, reason: str, lateness_ms: Optional[float]) -> None:
        with self._lock:
            was = self._state
            self._state, self._reason, self._lateness_ms = STATE_FAILED, reason, lateness_ms
        if was != STATE_FAILED:
            self._notify()

    def _fail(self, reason: str, lateness_ms: Optional[float]) -> None:
        """Fail closed: stop taking publishes (the state), say it, and leave the
        restart to the supervisor's next turn."""
        if self._state == STATE_FAILED:
            return
        log.error("Failsafe watchdog failed (%s); publishers take no publishes until it is back",
                  reason)
        self._set_failed(reason, lateness_ms)

    def _restart(self) -> None:
        # The process first: ending it is what unblocks a send stuck on it,
        # and only then can the send lock be taken.
        process, self._process = self._process, None
        if process is not None:
            self._end(process, self._conn)
        with self._send_lock:
            conn, self._conn = self._conn, None
        if conn is not None:
            conn.close()
        if self._stopping:
            return
        try:
            lateness_ms = self._spawn_and_test()
        except Exception as exc:  # noqa: BLE001 - retried after a pause
            if self._stopping:
                return
            log.error("Failsafe watchdog: a new failsafe process did not start (%s); retrying", exc)
            time.sleep(RETRY_INTERVAL_S)
            return
        if lateness_ms > LATENESS_BOUND_MS:
            log.error("Failsafe watchdog: the new process's self-test was %.1f ms late; retrying",
                      lateness_ms)
            time.sleep(RETRY_INTERVAL_S)
            return
        log.info("Failsafe watchdog: back after a restart; self-test late by %.1f ms", lateness_ms)
        with self._lock:
            self._state, self._reason, self._lateness_ms = STATE_OK, None, lateness_ms
        self._notify()

    def _notify(self) -> None:
        if self._on_state is None:
            return
        state, reason, lateness_ms = self.snapshot()
        try:
            self._on_state(state, reason, lateness_ms)
        except Exception:  # noqa: BLE001 - a report must not take the watchdog down
            log.exception("Failsafe watchdog: reporting its state failed")
