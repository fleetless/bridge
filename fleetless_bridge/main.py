# SPDX-License-Identifier: Apache-2.0
"""Entry point: `python3 -m fleetless_bridge.main`, and the `bridge` console
script the launch file starts.

Exit codes, because a supervisor cannot read log messages:
  0  stopped on request (SIGINT/SIGTERM) — nothing is wrong
  1  stopped by an unexpected error
  2  stopped because it will not heal by itself: no token, an unusable cloud
     URL, a token the cloud rejects, or another bridge that took over the robot

A stop is heard from the first statement of `main()` on, and it is exit 0
whenever it comes: during the imports, while the ROS node is being created,
before the cloud has answered. `_EarlyStop` says how.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import signal
import site
import sys

# Nothing slow up here. Whatever this file imports at its top runs before
# `main()` does, and until `main()` has run nobody listens for a stop: the
# client, ROS and asyncio are imported in `run()`.
log = logging.getLogger("fleetless_bridge")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_WONT_RETRY = 2

# For the reader and for a type checker. At run time `run()` imports them.
TYPE_CHECKING = False
if TYPE_CHECKING:
    from fleetless_bridge.client import BridgeClient, StopReason
    from fleetless_bridge.ros_runtime import RosRuntime


def shadowed_numpy_warning() -> "str | None":
    """A `pip install --user` in the operating account's own home
    puts `~/.local` ahead of the system site-packages `cv_bridge`/`cv2` and
    `numpy` (`python3-numpy`, an ordinary apt package now -- no rosdep
    constraint, no `/usr/local` install; see `rosdep/README.md`) all live
    in. If the resulting numpy is 2.x, the *documented* failure is
    `CvBridge.imgmsg_to_cv2` (a C extension built against numpy's 1.x ABI)
    dying on the first real frame with `AttributeError: _ARRAY_API not
    found` — seen on a real robot, with the bridge otherwise already
    started, connected and reporting healthy.

    Reproducing it live found something worse: with numpy 2.x installed in
    a user site-packages directory, `import cv2` itself raises the same
    `AttributeError` — meaning `camera.py`'s own
    module-level `import cv2` would crash the *whole process* on startup,
    with a bare traceback and no exit code from this file's own table,
    before `run()` is past its imports. So this check cannot live in
    `camera.py` and cannot `import numpy`: it uses
    `importlib.util.find_spec`, which locates a module without executing
    it, and it runs here, at this file's own import time, before the
    `from fleetless_bridge.ros_runtime import RosRuntime` line in `run()`
    ever gets a chance to drag `camera.py` (and `cv2`) in.

    Returns a message naming the real path if numpy resolves to a user
    site-packages directory, `None` otherwise. Never raises and never a
    reason to refuse to start — see this module's own exit-code table: a
    robot that will not start because a library might be shadowed is worse
    than one whose cameras fail loudly and say why. The launch file's
    `PYTHONNOUSERSITE=1` (`ff50b36`, shipped in 1.0.1) prevents `~/.local`
    from ever reaching `sys.path` for a bridge started that way; this is
    the backstop for every other way to start one."""
    try:
        spec = importlib.util.find_spec("numpy")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    user_site = site.getusersitepackages()
    if not spec.origin.startswith(user_site):
        return None
    return (
        "numpy resolves to {} (a user site-packages directory) instead of "
        "the system location cv_bridge/cv2 expect (apt's python3-numpy, an "
        "ordinary system dist-packages install -- see rosdep/README.md). Every "
        "camera will likely fail — anywhere from an immediate crash on "
        "`import cv2` to a first-real-frame AttributeError from cv_bridge, "
        "depending on the exact numpy 2.x build — while this process "
        "itself starts, connects and reports healthy if it starts at all. "
        "The launch file sets PYTHONNOUSERSITE=1 for exactly this reason; "
        "if this process was started a different way, that protection was "
        "not in effect."
    ).format(spec.origin)


_shadow_warning = shadowed_numpy_warning()
if _shadow_warning is not None:
    # No logging.basicConfig() yet (that is _configure_logging()'s job,
    # called from main() below) — Python's own "handler of last resort"
    # still puts this on stderr.
    log.warning("%s", _shadow_warning)


class _EarlyStop:
    """SIGINT and SIGTERM, from the first statement of `main()` until the
    event loop takes them over.

    A robot's container is often stopped right after it started, and the
    bridge needs a second or more to import ROS before it has a loop that
    could listen. In that time Python answers a SIGINT with a
    `KeyboardInterrupt` in the middle of an import, and the kernel answers a
    SIGTERM by killing the process. A bridge born with SIGINT ignored (a
    shell's background job, a container's entrypoint) does not hear a SIGINT
    at all: Python leaves an inherited "ignore" alone until a handler is
    installed over it. To a supervisor each of the three is a crash, or a
    bridge that will not stop.

    So `main()` installs these handlers before anything else. What a stop
    does then depends on how far the start has come:

    - **While the imports run** nothing has been started that needs undoing,
      and the handler itself ends the process with exit 0. It does not
      raise: an exception from a signal handler lands in whichever import is
      running, where a bare `except` swallows it.
    - **From `started()` on** the handler only records the request. The
      starting code asks `requested()` at each seam (`_run_client`,
      `_serve`) and leaves through the normal way out, which undoes what was
      started.

    A second signal changes nothing: the first is already being acted on.
    """

    def __init__(self) -> None:
        #: The signal that asked for a stop, or `None`.
        self._signal: int | None = None
        self._nothing_started = True
        self._said = False

    def listen(self) -> None:
        # `signal.signal`, whatever is installed now: an explicit handler is
        # the one thing that replaces an inherited "ignore".
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._on_signal)

    def started(self) -> None:
        """From here on a stop has something to undo, so the handler leaves
        it to the code that knows what."""
        self._nothing_started = False

    def requested(self) -> bool:
        """Whether a stop was asked for. The first yes says so in the log."""
        if self._signal is None:
            return False
        if not self._said:
            self._said = True
            _log_received(self._signal)
        return True

    def _on_signal(self, number: int, frame) -> None:
        if self._signal is not None:
            return
        self._signal = number
        if not self._nothing_started:
            return
        # The two lines every stop writes, then out. `os._exit` and not
        # `sys.exit`, which is an exception like any other. A line that
        # cannot be written is not worth a stack trace on the way out, and
        # the exit does not depend on it.
        logging.raiseExceptions = False
        try:
            _configure_logging()
            self.requested()
            log.info("Bridge stopped")
        finally:
            os._exit(EXIT_OK)


_early_stop = _EarlyStop()


def main() -> None:
    # Before anything else: see `_EarlyStop`.
    _early_stop.listen()
    _configure_logging()
    sys.exit(run())


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # The hello frame carries the robot's token, and an operator chasing a
    # connection problem turns on debug logging. A WebSocket library that logs
    # what it sends at DEBUG therefore writes a credential into the robot's
    # journal, which is exactly what the library this one replaced did — its
    # own frame log elided the middle of a long payload, so the token happened
    # to fall inside the elision, and reordering the hello's fields or
    # lengthening the token would have written it out in full.
    #
    # aiohttp does not log frame payloads at any level today. Kept anyway:
    # the guarantee has to hold for whichever version the robot's
    # distribution ships, not only the one this was read against — one line
    # removes the whole question.
    logging.getLogger("aiohttp").setLevel(logging.INFO)


def run() -> int:
    """Everything main() does, minus the process exit — so tests can call it."""
    # The slow part: the client, ROS and OpenCV. Here and not at the top of
    # this file, so that `main()` listens for a stop first; a stop during
    # these lines ends the process in the handler.
    import asyncio

    from fleetless_bridge import __version__
    from fleetless_bridge.client import BridgeClient, StopReason
    from fleetless_bridge.config import BridgeConfig
    from fleetless_bridge.protocol import PROTOCOL_VERSION
    from fleetless_bridge.ros_runtime import RosRuntime

    _early_stop.started()
    try:
        config = BridgeConfig.from_env()
    except ValueError as exc:
        log.error("%s", exc)
        return EXIT_WONT_RETRY

    log.info(
        "Starting fleetless_bridge %s (protocol v%d), cloud %s",
        __version__,
        PROTOCOL_VERSION,
        config.cloud_url,
    )
    # That check already ran, at this file's own import time (see
    # `shadowed_numpy_warning` above) — by the time this line runs,
    # camera.py's import either succeeded or already crashed the process.

    ros = RosRuntime()
    try:
        reason = asyncio.run(_run_client(BridgeClient(config, ros=ros), ros))
    except Exception:  # noqa: BLE001 - last resort, so the log says why
        log.exception("The bridge stopped with an unexpected error")
        return EXIT_ERROR

    if reason is StopReason.REJECTED:
        return EXIT_WONT_RETRY
    log.info("Bridge stopped")
    return EXIT_OK


async def _run_client(client: BridgeClient, ros: RosRuntime) -> StopReason:
    # Started before the client, so a config pushed right after hello_ok has
    # somewhere to land; stopped after the client is done, however it ended
    # (shutdown, rejection, or an unexpected error) — stop() destroys the
    # node and joins the executor thread, so SIGTERM never hangs.
    # RosRuntime's lifecycle is kept out of `_serve` itself so tests can go
    # on monkeypatching `_serve` with its original single-argument shape.
    import asyncio

    from fleetless_bridge.client import StopReason

    # A stop that came while the bridge was still starting is acted on at
    # the next seam. Here ROS has not been initialised: nothing to undo.
    if _early_stop.requested():
        return StopReason.SHUTDOWN
    ros.start(asyncio.get_event_loop())
    try:
        return await _serve(client)
    finally:
        ros.stop()


async def _serve(client: BridgeClient) -> StopReason:
    _install_signal_handlers(client)
    # The next seam: the node exists, the loop's handlers have just replaced
    # the early one, the client has not connected. A stop recorded until
    # here would wait for a second signal that nobody owes. It is passed on
    # now: `client.run()` returns before it connects, and `_run_client`
    # ends ROS again.
    if _early_stop.requested():
        client.stop()
    return await client.run()


def _install_signal_handlers(client: BridgeClient) -> None:
    import asyncio

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, client, sig)
        except NotImplementedError:
            # Platforms without loop signal handlers (not our target, but this
            # keeps the bridge startable there).
            signal.signal(sig, lambda number, frame: client.stop())


def _request_stop(client: BridgeClient, sig: int) -> None:
    _log_received(sig)
    client.stop()


def _log_received(sig: int) -> None:
    log.info("Received %s — shutting down", signal.Signals(sig).name)


if __name__ == "__main__":
    main()
