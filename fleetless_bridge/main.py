# SPDX-License-Identifier: Apache-2.0
"""Entry point: `python3 -m fleetless_bridge.main`, and the `bridge` console
script the launch file starts.

Exit codes, because a supervisor cannot read log messages:
  0  stopped on request (SIGINT/SIGTERM) — nothing is wrong
  1  stopped by an unexpected error
  2  stopped because it will not heal by itself: no token, an unusable cloud
     URL, a token the cloud rejects, or another bridge that took over the robot
"""
from __future__ import annotations

import importlib.util
import logging
import site
import sys

log = logging.getLogger("fleetless_bridge")


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
    before `main()` is ever reached. So this check cannot live in
    `camera.py`, cannot `import numpy`, and cannot run from inside `run()`:
    it uses `importlib.util.find_spec`, which locates a module without
    executing it, and it runs here, at this file's own import time, before
    the `from fleetless_bridge.ros_runtime import RosRuntime` line below
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
    # called from main() below, itself unreachable if camera.py's import
    # is about to crash the process) — Python's own "handler of last
    # resort" still puts this on stderr.
    log.warning("%s", _shadow_warning)

import asyncio  # noqa: E402 - the check above must run before any fleetless_bridge import
import signal  # noqa: E402

from fleetless_bridge import __version__  # noqa: E402
from fleetless_bridge.client import BridgeClient, StopReason  # noqa: E402
from fleetless_bridge.config import BridgeConfig  # noqa: E402
from fleetless_bridge.protocol import PROTOCOL_VERSION  # noqa: E402
from fleetless_bridge.ros_runtime import RosRuntime  # noqa: E402

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_WONT_RETRY = 2


def main() -> None:
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
    ros.start(asyncio.get_event_loop())
    try:
        return await _serve(client)
    finally:
        ros.stop()


async def _serve(client: BridgeClient) -> StopReason:
    _install_signal_handlers(client)
    return await client.run()


def _install_signal_handlers(client: BridgeClient) -> None:
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, client, sig)
        except NotImplementedError:
            # Platforms without loop signal handlers (not our target, but this
            # keeps the bridge startable there).
            signal.signal(sig, lambda number, frame: client.stop())


def _request_stop(client: BridgeClient, sig: int) -> None:
    log.info("Received %s — shutting down", signal.Signals(sig).name)
    client.stop()


if __name__ == "__main__":
    main()
