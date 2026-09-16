# SPDX-License-Identifier: Apache-2.0
"""Non-ROS camera sources: RTSP, MJPEG and V4L2 adapters that
fill the same `camera.LatestFrameHolder` a ROS subscription does. The fourth
kind, `ros`, is deliberately not here — it needs `self._node` to create a
subscription, so it lives in ros_runtime.py's `_RosSourceAdapter`, the one
adapter this rclpy-free module cannot host itself (same split camera.py and
sampling.py already keep, for the same reason: testable without any ROS
graph at all).

Every adapter (`RtspSourceAdapter`, `MjpegSourceAdapter`, `V4l2SourceAdapter`)
implements the same two-method interface — `start()`/`stop()` — and reports
through the same two callbacks, `on_frame(bgr, timestamp_ms)` and
`on_error(code, message)`. That is the actual seam here: nothing
above an adapter (ros_runtime.py's shared resize/rate-limit/holder-write,
or its camera_state report) ever branches on which kind produced a frame.

Credentials: resolved once per config-apply by `resolve_credentials`
and handed to an adapter as an already-decided `Credentials` value — an
adapter never re-derives ref-vs-URL-userinfo precedence itself. MJPEG
presents them as a real HTTP `Authorization` header and never touches the
URL. RTSP has no such channel available through `cv2.VideoCapture` (ffmpeg's
URI-based API takes one string, nothing else), so its *only* transport-level
option is `rtsp://user:pass@host/...` — built fresh at connect time, held
only as a local variable, and handed straight to the capture call. That is
different from writing a credential back into the stored configuration
document (the thing §5b actually forbids): the document never sees this
string, and neither does any log line, exception message or camera_state
report — see `_suppress_native_stderr` for the one leak channel that is not
under this module's own control at all (ffmpeg's native diagnostics, printed
directly to the process's real stderr on a failed open).

The RTSP preflight (`_rtsp_preflight`) exists only to *classify* a failure —
"nobody is listening" versus "somebody refused me" — not to
authenticate. It speaks RTSP's real `Authorization` header for Basic
challenges (the actual protocol mechanism, no URL involved at all here); a
Digest or unrecognised challenge is not classified further (a named
limitation, not a gap: implementing Digest just to answer a yes/no
classification question is more than this needs). ffmpeg
still gets to attempt the real handshake either way.
"""
from __future__ import annotations

import base64
import contextlib
import logging
import multiprocessing as mp
import os
import queue
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional, Tuple
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import cv2
import numpy as np

from fleetless_bridge import sampling

log = logging.getLogger(__name__)

OnFrame = Callable[["np.ndarray", int], None]
OnError = Callable[[str, str], None]

# ffmpeg's own diagnostics (via OpenCV's ffmpeg backend) go straight to the
# process's real stderr — outside Python's logging, outside every `except`
# block in this module — and on a failed RTSP open that includes the URL it
# tried. `_suppress_native_stderr` (wrapped around every real capture open
# below) is the guard actually verified by a test; this env var is OpenCV's
# own documented second line of defense for the same leak
# (`OPENCV_FFMPEG_LOGLEVEL`, an ffmpeg `av_log` level — -8 is AV_LOG_QUIET).
os.environ.setdefault("OPENCV_FFMPEG_LOGLEVEL", "-8")

_RTSP_DEFAULT_PORT = 554
_PREFLIGHT_TIMEOUT_S = 5.0
# Passed to urlopen() as `timeout=`, which — per Python's own socket
# semantics — bounds every subsequent `.read()` on the connection, not only
# the connect (verified directly against http.client, not assumed;
# `_close()`'s cross-thread `response.close()` turns out not to accelerate
# an in-progress read at all — measured, it can itself block for as long as
# the read does — so this constant is the *only* thing bounding a stalled
# MJPEG source, the same role `_RTSP_SOCKET_TIMEOUT_US` plays for RTSP).
# Kept under `_ThreadedSourceAdapter.stop()`'s own 5.0s join timeout with
# real margin, not equal to it: two independently-chosen constants that
# happened to match left zero margin, and a `stop()` bounded only by a
# cross-thread close that does not work is a `stop()` bounded only by
# this timeout racing that join deadline.
_MJPEG_CONNECT_TIMEOUT_S = 3.0
_MAX_CONSECUTIVE_READ_FAILURES = 30
_READ_RETRY_DELAY_S = 0.05


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str


class CameraSourceAdapter:
    """What every source kind implements. `start()`
    must return quickly (it hands the actual connect attempt to a
    background thread); `stop()` is idempotent and blocks until that
    thread has genuinely exited, the same "fully torn down before
    returning" contract `RosRuntime._destroy_camera` already holds for a
    subscription."""

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError


def validate_source_scheme(url: str, allowed_schemes: Tuple[str, ...]) -> None:
    """Raises `ValueError` unless `url`'s scheme is one of `allowed_schemes`
    (case-insensitive). The contract's own `url` field was
    drafted as `z.string().url()` and shipped as `z.string().min(1).max
    (2048)` with no scheme constraint at all — `file:///etc/hostname`
    validated as a legal `mjpeg` or `rtsp` source, and both
    `urllib.request.urlopen` (MJPEG) and `cv2.VideoCapture`'s ffmpeg
    backend (RTSP) honour `file:`/`ftp:` and would happily read a local
    file and publish whatever decodes as an image to the cloud — plus a
    clean file-existence oracle from `source_unreachable` vs. a real read.
    The contract now constrains this too (fixed at the sha this comment's
    neighbour vendors), but this adapter does not rely on that alone: the
    vendored schema is a copy of someone else's validator, and a robot
    must not become a file server because that copy drifts, is bypassed,
    or is wrong. Called from each adapter's `__init__` — fails the config
    apply immediately (same "caught now, not the first time a frame
    arrives" reasoning `_RosSourceAdapter.__init__` already uses for a bad
    ROS type), not silently retried forever in the background."""
    scheme = urlsplit(url).scheme.lower()
    if scheme not in allowed_schemes:
        raise ValueError(
            "source url must use {}, got {!r}".format(
                " or ".join("{}://".format(s) for s in allowed_schemes), scheme or "(no scheme)"
            )
        )


_DEVICE_PATH_RE = re.compile(r"^/dev/[A-Za-z0-9][A-Za-z0-9._/-]*$")


def validate_device_path(device: str) -> None:
    """Raises `ValueError` unless `device` is a legal V4L2 device path.

    `device` had no constraint at all — any string up to 128 chars — and
    reaches `cv2.VideoCapture(device)` directly. OpenCV does not restrict
    itself to actual devices: on cv2 4.5.4, an ordinary local video file
    opens (arbitrary local-file read) and so does an `http://` URL (an
    outbound fetch from inside the robot) — the same violation `validate_source_scheme` closes for
    `rtsp`/`mjpeg`, reachable through the branch that was skipped because
    "it is just a device path" read like a reason not to check.

    Mirrors the wire contracts' own three rules exactly (`cameraSource`'s
    `v4l2` variant) rather than trusting the wire alone,
    the same reasoning `validate_source_scheme` already gives: a
    `/dev/v4l/by-id/...` symlink must stay legal (the only way device
    naming survives a reboot), so this is a prefix + traversal check, not
    a fixed allowlist of paths.
    """
    if not _DEVICE_PATH_RE.match(device):
        raise ValueError("device must be a path under /dev/, got {!r}".format(device))
    if ".." in device.split("/"):
        raise ValueError("device must not contain a '..' path segment")
    if device.endswith("/"):
        raise ValueError("device must name a device, not a directory")


def redact_url(url: str) -> str:
    """`url` with any userinfo (`user:pass@`) stripped — safe for a log
    line, an exception message or a `camera_state.error.message`. This
    cannot protect the *stored* configuration document — userinfo in a `url`
    is legal and documented, not a warning (the 3.0 format dropped the two
    `credentials_in_url*` warnings that said otherwise) — so this
    function is for everything downstream of the document."""
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


def resolve_credentials(
    *, url: str, credentials: Optional[Tuple[str, str]]
) -> Optional[Credentials]:
    """The explicit block beats URL userinfo — a rotation must not appear to
    silently keep using the URL's old password. `None` means genuinely no
    credentials: nothing written on the source and no userinfo in the URL
    either.

    The 3.0 format replaced the named reference into a frame-level map with an
    explicit block on the source itself. The precedence is unchanged, and so
    is the reason for it; what is gone is the map, and with it the "a ref
    that does not resolve falls through to the URL" case, which has no way
    left to happen — a block that is written is a block that is here."""
    if credentials is not None:
        username, password = credentials
        return Credentials(username=username, password=password)

    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return None
    userinfo = parts.netloc.rsplit("@", 1)[0]
    if ":" not in userinfo:
        return None
    username, _, password = userinfo.partition(":")
    return Credentials(username=unquote(username), password=unquote(password))


def _credentialed_url(url: str, credentials: Optional[Credentials]) -> str:
    """A transient, in-process-only string — never logged, never stored,
    never returned to a caller outside this connect call. This is the one
    place `credentials` ever touches a URL, and it exists because
    `cv2.VideoCapture`'s ffmpeg backend has no other channel to receive
    them through (see the module docstring)."""
    if credentials is None:
        return url
    parts = urlsplit(url)
    host = parts.netloc.rsplit("@", 1)[-1]
    userinfo = "{}:{}".format(
        quote(credentials.username, safe=""), quote(credentials.password, safe="")
    )
    netloc = "{}@{}".format(userinfo, host)
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


_stderr_suppress_lock = threading.Lock()
_stderr_suppress_count = 0
_stderr_suppress_saved_fd: Optional[int] = None


@contextlib.contextmanager
def _suppress_native_stderr():
    """Redirects the process's real fd 2 to `/dev/null` for the duration of
    the block — the only reliable way to catch a native library (ffmpeg,
    via cv2.VideoCapture) writing directly to the OS-level stderr on a
    failed open, which bypasses Python's logging and every `except` clause
    in this module entirely. Scoped tightly around just the open call, not
    the read loop that follows — holding this for the life of a stream
    would swallow this process's own log lines too, since `sys.stderr`
    writes through the same fd.

    Reference-counted: concurrent RTSP opens sharing a transport now genuinely
    run their `cv2.VideoCapture(...)` calls in parallel (see
    `_ffmpeg_rtsp_transport`), and fd 2 is a single
    process-wide resource — two independent, unsynchronized dup2() dances
    racing on it would corrupt each other's saved fd, restoring the wrong
    target or restoring too early while a sibling call is still relying on
    the redirect. The first concurrent caller does the real redirect and
    saves the true original fd; later callers while it is already active
    just increment the count; the last one out restores it. A single
    caller (the ordinary case, and still true for V4L2,
    which only ever calls this from one process-isolated child at a time)
    behaves exactly as before."""
    global _stderr_suppress_count, _stderr_suppress_saved_fd
    stderr_fd = 2
    with _stderr_suppress_lock:
        _stderr_suppress_count += 1
        if _stderr_suppress_count == 1:
            _stderr_suppress_saved_fd = os.dup(stderr_fd)
            devnull_fd = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull_fd, stderr_fd)
            os.close(devnull_fd)
    try:
        yield
    finally:
        with _stderr_suppress_lock:
            _stderr_suppress_count -= 1
            if _stderr_suppress_count == 0:
                saved_fd, _stderr_suppress_saved_fd = _stderr_suppress_saved_fd, None
                os.dup2(saved_fd, stderr_fd)
                os.close(saved_fd)


class _SourceError(Exception):
    """Raised internally by an adapter's connect/read step to report a
    classified failure. `message` must already be redacted by whoever
    raises it — never built from a caught exception's raw `str()`, which is
    exactly how a credentialed URL would otherwise escape (see the
    module docstring)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Backoff:
    """Exponential backoff with a cap, for a reconnect loop. `next()`
    returns the delay to wait before the next attempt and advances;
    `reset()` on a successful connect. Deterministic and clock-free, so it
    is unit-testable without any real sleeping."""

    def __init__(self, *, initial_s: float = 1.0, max_s: float = 30.0, factor: float = 2.0) -> None:
        self._initial_s = initial_s
        self._max_s = max_s
        self._factor = factor
        self._current_s = initial_s

    def reset(self) -> None:
        self._current_s = self._initial_s

    def next(self) -> float:
        delay = self._current_s
        self._current_s = min(self._current_s * self._factor, self._max_s)
        return delay


class _ThreadedSourceAdapter(CameraSourceAdapter):
    """Shared thread lifecycle, backoff and error-transition tracking for
    every non-ROS adapter: connect, read frames until something goes
    wrong, report the failure exactly once per transition into it (not on
    every retry — a prolonged outage must not become a `camera_state` flood),
    back off, try again. Subclasses implement `_connect_and_read()`, which
    should loop internally (checking `self._stop_event`) until either
    `stop()` is called or something fails."""

    def __init__(
        self,
        *,
        on_frame: OnFrame,
        on_error: OnError,
        backoff: Optional[Backoff] = None,
    ) -> None:
        self._on_frame = on_frame
        self._on_error = on_error
        self._backoff = backoff or Backoff()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_reported_code: Optional[str] = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._close()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=5.0)
            if thread.is_alive():
                log.error(
                    "A camera source thread did not stop within 5s — a blocking "
                    "connect attempt in progress cannot be interrupted from here."
                )

    def _connect_and_read(self) -> None:
        raise NotImplementedError

    def _close(self) -> None:
        """Best-effort help for `stop()`: unblock a thread stuck in a
        blocking read, *if* whatever it is blocked on can be closed safely
        from a different thread — plain sockets can (MjpegSourceAdapter's
        override), an OpenCV/ffmpeg capture cannot (a real
        SIGSEGV in libavformat traced to exactly this; see
        `_CvCaptureAdapter`'s docstring). The default is a no-op, which is
        always correct, if not always fast: `_run`'s own loop notices
        `_stop_event` on its own once whatever it was blocked on returns —
        an in-flight call must have its own bound for that to happen in
        reasonable time, since nothing here forces it."""

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._connect_and_read()
                self._backoff.reset()
                self._last_reported_code = None
            except _SourceError as exc:
                self._report_once(exc.code, exc.message)
                self._stop_event.wait(self._backoff.next())
            except Exception as exc:  # noqa: BLE001 - never let a raw exception escape un-redacted
                # Deliberately `type(exc).__name__`, not `str(exc)`: a raw
                # library exception can embed the URL (and its credentials,
                # for RTSP or MJPEG) in its own message — exactly what
                # `log.exception(...)` would leak, since it logs `str()`
                # plus a traceback ending in that same message. This is the
                # last line of defense in this module against that leaking
                # into a log or a wire frame. `http.client.InvalidURL` on an
                # MJPEG URL with userinfo and no explicit port (`nonnumeric
                # port: 'campass@cam.lan'`) reached exactly this branch — an
                # `HTTPException`, not the `OSError`/`HTTPError`
                # `_connect_and_read` already classifies — and did leak the
                # password, via `log.exception`. `log.error` here,
                # deliberately without `exc_info`, so only the type name
                # reaches the log.
                self._report_once(
                    "source_error", "unexpected error ({})".format(type(exc).__name__)
                )
                log.error(
                    "Unexpected error in a camera source adapter: %s "
                    "(message redacted — see the module docstring on why)",
                    type(exc).__name__,
                )
                self._stop_event.wait(self._backoff.next())

    def _report_once(self, code: str, message: str) -> None:
        if code == self._last_reported_code:
            return
        self._last_reported_code = code
        try:
            self._on_error(code, message)
        except Exception as exc:  # noqa: BLE001 - a bad on_error must not kill the reconnect loop
            # Module-wide rule: no traceback from this module ever reaches
            # the log — not only the two `except` blocks that once leaked
            # one. `on_error` is caller-supplied (RosRuntime wires it to
            # `self._enqueue(...)`) and could raise anything, so the same
            # policy applies: type name only, never `log.exception`'s
            # traceback-with-message. `code` is already redacted
            # (`_SourceError` requires it), but what *this* call catches is
            # a second, unrelated exception with no such guarantee.
            #
            # This matters beyond this one call site: this method runs
            # inside `_run`'s own `except _SourceError as exc:` handler
            # above, so if `on_error` raises here, Python's implicit
            # chaining sets the new exception's `__context__` to the
            # password-bearing `_SourceError` already being handled.
            # `log.exception` (or any `exc_info=True` call) walks that
            # chain and prints both — a site holding no secret itself is
            # not automatically safe to log verbosely if what it's chained
            # to does. `log.error` with no `exc_info` never renders the
            # chain at all.
            log.error(
                "Error reporting a camera source error (code=%s): %s (message redacted)",
                code, type(exc).__name__,
            )

    def _emit_frame(self, bgr: "np.ndarray", timestamp_ms: int) -> None:
        try:
            self._on_frame(bgr, timestamp_ms)
        except Exception as exc:  # noqa: BLE001 - one bad frame must not kill the adapter
            # Same module-wide rule as `_report_once` above.
            log.error("Error handling a captured camera frame: %s (message redacted)", type(exc).__name__)


class _CvCaptureAdapter(_ThreadedSourceAdapter):
    """Shared `cv2.VideoCapture` read loop for the two adapters that pull
    frames through it (RTSP, V4L2) — only `_open()` differs between them.

    The adapter's own thread is the **sole owner** of `cap`'s whole
    lifecycle — it is the only thing that ever calls `.release()` on it,
    always from its own `finally` below, never from `stop()`'s caller.
    That was not always true: `_close()` used to reach across threads and
    release the capture out from under whichever native call the adapter's
    thread happened to be inside — the documented way to unblock a stuck
    `cap.read()`, and it did unblock it. It also produced a real,
    twice-reproduced SIGSEGV in `libavformat` (against a real
    RTSP camera): releasing an OpenCV/ffmpeg capture from a thread other
    than the one currently executing a blocking native call on it is not
    safe for every backend, and ffmpeg's turned out to be one where it
    is not. `_close()` is a no-op now; an in-flight native call is instead
    bounded by an explicit ffmpeg-level socket timeout for RTSP
    (`_RTSP_SOCKET_TIMEOUT_US`, `_ffmpeg_rtsp_transport`) — the adapter's
    own thread notices `_stop_event` and cleans up itself once whatever it
    was blocked on returns on its own, comfortably inside `stop()`'s own
    join timeout in the ordinary case, and never touched by any other
    thread in any case."""

    def _open(self):
        raise NotImplementedError

    def _connect_and_read(self) -> None:
        cap = self._open()
        try:
            consecutive_failures = 0
            while not self._stop_event.is_set():
                timestamp_ms = sampling.capture_timestamp_ms()
                ok, frame = cap.read()
                if not ok:
                    consecutive_failures += 1
                    if consecutive_failures >= _MAX_CONSECUTIVE_READ_FAILURES:
                        raise _SourceError(
                            "source_unavailable", "the camera stopped delivering frames"
                        )
                    self._stop_event.wait(_READ_RETRY_DELAY_S)
                    continue
                consecutive_failures = 0
                self._emit_frame(frame, timestamp_ms)
        finally:
            cap.release()  # always from this thread — see the class docstring


# --- RTSP --------------------------------------------------------------------


@dataclass(frozen=True)
class _PreflightResult:
    ok: bool
    # 'basic' | some other challenge scheme (lowercased) | None (no auth
    # needed at all). Carried through even on success so a *later* cv2-level
    # open failure can still say "authentication required, scheme not
    # classified" instead of a bare "could not open".
    scheme: Optional[str]
    error: Optional[_SourceError]


def _default_socket_factory(host: str, port: int, timeout_s: float) -> "socket.socket":
    return socket.create_connection((host, port), timeout=timeout_s)


def _rtsp_describe(
    host: str,
    port: int,
    request_uri: str,
    *,
    auth_header: Optional[str],
    timeout_s: float,
    socket_factory: Callable[[str, int, float], "socket.socket"],
) -> Tuple[int, Optional[str]]:
    """One DESCRIBE round trip over a fresh connection. Returns `(status,
    www_authenticate_or_None)`. Never raises anything but `_SourceError`
    (always `source_unreachable`) — a preflight's whole job is to tell that
    apart from a real answer, so every socket-level failure collapses to
    the one code that means it."""
    lines = [
        "DESCRIBE {} RTSP/1.0".format(request_uri),
        "CSeq: 1",
        "User-Agent: fleetless-bridge",
        "Accept: application/sdp",
    ]
    if auth_header:
        lines.append("Authorization: {}".format(auth_header))
    request = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")

    try:
        sock = socket_factory(host, port, timeout_s)
    except (OSError, socket.timeout) as exc:
        raise _SourceError(
            "source_unreachable", "could not reach {}:{}: {}".format(host, port, type(exc).__name__)
        ) from None

    try:
        sock.settimeout(timeout_s)
        sock.sendall(request)
        buf = b""
        while b"\r\n\r\n" not in buf and len(buf) < 65536:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
    except (OSError, socket.timeout) as exc:
        raise _SourceError(
            "source_unreachable", "connection dropped while probing: {}".format(type(exc).__name__)
        ) from None
    finally:
        try:
            sock.close()
        except OSError:
            pass

    header_part = buf.split(b"\r\n\r\n", 1)[0]
    text = header_part.decode("iso-8859-1", errors="replace")
    response_lines = text.split("\r\n")
    if not response_lines or not response_lines[0].startswith("RTSP/"):
        raise _SourceError("source_unreachable", "did not answer RTSP")
    try:
        status_code = int(response_lines[0].split()[1])
    except (IndexError, ValueError):
        raise _SourceError("source_unreachable", "did not answer RTSP")

    www_authenticate = None
    for line in response_lines[1:]:
        if line.lower().startswith("www-authenticate:"):
            www_authenticate = line.split(":", 1)[1].strip()
    return status_code, www_authenticate


def _rtsp_preflight(
    url: str,
    credentials: Optional[Credentials],
    *,
    timeout_s: float = _PREFLIGHT_TIMEOUT_S,
    socket_factory: Callable[[str, int, float], "socket.socket"] = _default_socket_factory,
) -> _PreflightResult:
    parts = urlsplit(url)
    if not parts.hostname:
        return _PreflightResult(False, None, _SourceError("source_unreachable", "camera url has no host"))
    host = parts.hostname
    port = parts.port or _RTSP_DEFAULT_PORT
    request_uri = redact_url(url)

    try:
        status, www_authenticate = _rtsp_describe(
            host, port, request_uri, auth_header=None, timeout_s=timeout_s, socket_factory=socket_factory
        )
    except _SourceError as exc:
        return _PreflightResult(False, None, exc)

    if 200 <= status < 300:
        return _PreflightResult(True, None, None)
    if status != 401:
        return _PreflightResult(
            False, None, _SourceError("source_error", "the camera answered RTSP {}".format(status))
        )

    scheme = (www_authenticate or "").split()[0].lower() if www_authenticate else None
    if scheme != "basic":
        # Digest or anything else: not classified further (see the
        # module docstring) — proceed, ffmpeg speaks Digest natively, and
        # only report if *it* also fails to open.
        return _PreflightResult(True, scheme, None)

    if credentials is None:
        return _PreflightResult(
            False,
            "basic",
            _SourceError(
                "source_auth_failed",
                "the camera requires authentication and no credentials are configured",
            ),
        )

    auth_header = "Basic " + base64.b64encode(
        "{}:{}".format(credentials.username, credentials.password).encode("utf-8")
    ).decode("ascii")
    try:
        status2, _ = _rtsp_describe(
            host, port, request_uri, auth_header=auth_header, timeout_s=timeout_s, socket_factory=socket_factory
        )
    except _SourceError as exc:
        return _PreflightResult(False, "basic", exc)

    if 200 <= status2 < 300:
        return _PreflightResult(True, "basic", None)
    return _PreflightResult(
        False, "basic", _SourceError("source_auth_failed", "the camera rejected the configured credentials")
    )


#  RTSP socket I/O timeout, in microseconds, applied to the demuxer's own
# `stimeout` AVOption — connect *and* every subsequent read on the
# underlying socket, not merely the open call. Existing to bound a
# `cv2.VideoCapture` open/read that would otherwise have no timeout at all
# (a real, twice-reproduced SIGSEGV in libavformat, traced to
# `_close()` releasing a capture from a different thread than the one
# blocked inside it — see `_CvCaptureAdapter._close()`'s docstring for the
# full mechanism and why bounding this is the actual fix, not a band-aid
# alongside it). Kept comfortably under `_ThreadedSourceAdapter.stop()`'s
# own 5s join timeout, so the ordinary case is a clean, on-time stop rather
# than a "did not stop within 5s" log — which is now harmless either way,
# but a log nobody sees is better than one everybody has to reason about.
_RTSP_SOCKET_TIMEOUT_US = 3_000_000


class _ValueGate:
    """Guards a shared value against outside mutation for as long as any
    caller is relying on it, while letting any number of callers who want
    the *current* value hold it at once. Replaces an earlier `_RWLock`
    (which measurement showed to be wrong): that design called anyone who
    *arrived* seeing a mismatch a "writer", and a writer had to wait for
    `readers == 0` — even readers already reading the exact value the writer
    was about to set. A mixed
    fleet (three `tcp` cameras, then two `udp` cameras arriving together)
    showed it directly: the second `udp` camera waited for the *first* one
    to finish its entire connect, because the first was still "a reader"
    (of `udp`, the value the second one also wanted) when the second's
    `acquire_write()` checked `readers == 0` and found it false.

    The actual shape here is not readers-and-writers at all — it is one
    shared value with a requested setting. `acquire()` checks both
    satisfying outcomes together, under the same lock, every time a
    caller (re)tries: "the value already matches" (join immediately, no
    matter how many others already hold it) or "nobody holds it right now"
    (safe to change, then join). Because both are checked in the same
    critical section on every attempt, a second caller wanting the value
    the first one just set sees the update on its own very next check —
    it never has to wait out the first caller's own connect.

    Not fair by construction: a continuous stream of callers wanting the
    *current* value could in principle keep a caller wanting to change it
    waiting indefinitely. Not a concern for `_ffmpeg_rtsp_transport`'s use
    (camera opens are not a firehose) — named rather than silently
    inherited, the same caveat `_RWLock` already carried and this design
    does not make any worse.

    Built on a `Condition`, matching `_RWLock`'s own reason for using one:
    a caller that must wait does so with the lock released for the
    duration (`Condition.wait()`'s own guarantee), never holding one lock
    while blocked on another — the exact shape that deadlocked
    `_RWLock`'s first, two-`Lock` version on its own first real test."""

    def __init__(self, initial=None) -> None:
        self._value = initial
        self._holders = 0
        self._cond = threading.Condition()

    def acquire(self, desired, set_value=None) -> None:
        """Blocks until `desired` is the shared value and registers the
        calling thread as a holder of it, then returns. If the value does
        not already match and nobody currently holds it, calls
        `set_value()` (if given — the caller's chance to also update
        whatever the shared value is a proxy for, e.g. an env var) and
        adopts `desired` as the new value, atomically with registering as
        the first holder."""
        with self._cond:
            while True:
                if self._value == desired:
                    self._holders += 1
                    return
                if self._holders == 0:
                    if set_value is not None:
                        set_value()
                    self._value = desired
                    self._holders += 1
                    self._cond.notify_all()  # others waiting on this same new value can join right away
                    return
                self._cond.wait()

    def release(self) -> None:
        with self._cond:
            self._holders -= 1
            if self._holders == 0:
                self._cond.notify_all()  # a caller wanting a different value can now proceed


# Guards `OPENCV_FFMPEG_CAPTURE_OPTIONS` — see _ValueGate's own docstring
# for why this is not a readers-writer lock. `None` before any RTSP
# camera has ever opened.
_ffmpeg_options_gate = _ValueGate()


@contextlib.contextmanager
def _ffmpeg_rtsp_transport(transport: str):
    """Sets ffmpeg's `rtsp_transport` and `stimeout` demuxer options for the
    duration of one `cv2.VideoCapture` open, via OpenCV's own documented
    escape hatch for passing backend options through an environment
    variable read once per open (verified — see `_ValueGate`'s own
    docstring), not at process start and not on every read.

    A redesign after the first fairness measurement: the process-global
    variable does not need protecting from
    *every* other camera's open, only from a concurrent open that would
    leave a *different* value set while this one reads it. A fleet's
    cameras overwhelmingly share one transport, so most opens can run
    fully in parallel once the value they want is already the one set.
    Only an open that needs to change the value pays for the change, and
    only for the instant the mutation itself takes — `_ffmpeg_options_gate`
    hands every caller straight into "holding the value" before this
    context manager's own `yield`, so even a transport *change* does not
    serialize the connect attempts behind it, only the handful of opens
    racing to be the one that makes the change.

    Deliberately does not restore a "previous" value on exit (the original
    single-lock version did): the whole point is for the value to persist
    between compatible opens, so the next one finds it already right.

    The env var's own grammar (OpenCV's, not ffmpeg's): `key;value` pairs
    joined by `|`, semicolon reserved for pairing within one option, never
    for separating two of them — `"a;1;b;2"` is one malformed option to
    ffmpeg, not two. An earlier version of this line joined every token
    with `;`, which parsed but disabled RTSP outright rather than raising
    anything a test could catch."""
    env_var = "OPENCV_FFMPEG_CAPTURE_OPTIONS"
    desired = "rtsp_transport;{}|stimeout;{}".format(transport, _RTSP_SOCKET_TIMEOUT_US)

    def set_env():
        os.environ[env_var] = desired

    _ffmpeg_options_gate.acquire(desired, set_value=set_env)
    try:
        yield
    finally:
        _ffmpeg_options_gate.release()


def _default_rtsp_capture_factory(url: str, transport: str):
    with _ffmpeg_rtsp_transport(transport), _suppress_native_stderr():
        return cv2.VideoCapture(url, cv2.CAP_FFMPEG)


class RtspSourceAdapter(_CvCaptureAdapter):
    """`kind: 'rtsp'`."""

    def __init__(
        self,
        *,
        url: str,
        transport: str,
        credentials: Optional[Credentials],
        on_frame: OnFrame,
        on_error: OnError,
        capture_factory: Optional[Callable[[str, str], object]] = None,
        socket_factory: Callable[[str, int, float], "socket.socket"] = _default_socket_factory,
        backoff: Optional[Backoff] = None,
    ) -> None:
        validate_source_scheme(url, ("rtsp", "rtsps"))  # see its own docstring
        super().__init__(on_frame=on_frame, on_error=on_error, backoff=backoff)
        self._url = url
        self._transport = transport
        self._credentials = credentials
        self._capture_factory = capture_factory or _default_rtsp_capture_factory
        self._socket_factory = socket_factory

    def _open(self):
        preflight = _rtsp_preflight(self._url, self._credentials, socket_factory=self._socket_factory)
        if not preflight.ok:
            raise preflight.error

        connect_url = _credentialed_url(self._url, self._credentials)
        cap = self._capture_factory(connect_url, self._transport)
        if not cap.isOpened():
            cap.release()
            if preflight.scheme:
                raise _SourceError(
                    "source_auth_unclassified",
                    "authentication required, scheme not classified ({})".format(preflight.scheme),
                )
            raise _SourceError("source_unavailable", "could not open the RTSP stream")
        return cap


# --- MJPEG ---------------------------------------------------------------------


def _default_mjpeg_opener(request: "urllib.request.Request"):
    return urllib.request.urlopen(request, timeout=_MJPEG_CONNECT_TIMEOUT_S)


class MjpegSourceAdapter(_ThreadedSourceAdapter):
    """`kind: 'mjpeg'` — a `multipart/x-mixed-replace` HTTP
    stream. Frames are found by scanning for JPEG SOI/EOI markers rather
    than trusting a per-part `Content-Length`, which is more robust across
    the variety of real MJPEG server implementations. Authentication is a
    real HTTP `Authorization` header, added to the request before it is
    ever sent — the one adapter that never has to touch the URL at all."""

    _JPEG_SOI = b"\xff\xd8"
    _JPEG_EOI = b"\xff\xd9"
    # A stream with no JPEG boundary across this much data is not producing
    # recoverable frames, not merely between frames — bound the buffer so a
    # misbehaving server can't grow it without limit.
    _MAX_BUFFERED_BYTES = 8 * 1024 * 1024

    def __init__(
        self,
        *,
        url: str,
        credentials: Optional[Credentials],
        on_frame: OnFrame,
        on_error: OnError,
        opener: Optional[Callable[["urllib.request.Request"], object]] = None,
        backoff: Optional[Backoff] = None,
    ) -> None:
        validate_source_scheme(url, ("http", "https"))  # see its own docstring
        super().__init__(on_frame=on_frame, on_error=on_error, backoff=backoff)
        self._url = url
        self._credentials = credentials
        self._opener = opener or _default_mjpeg_opener
        self._response = None
        self._response_lock = threading.Lock()

    def _connect_and_read(self) -> None:
        # `urllib.request` has no concept of `user:pass@host` userinfo — unlike
        # RTSP, where ffmpeg's URI parser genuinely understands it and
        # `_credentialed_url` exists
        # to *use* that. Passing the raw URL through here when it carries
        # userinfo does not merely fail to authenticate: `http.client`
        # either can't resolve the host (`user:pass@host` is not a
        # hostname) or, with no explicit port, misreads the userinfo as a
        # bogus port and raises `InvalidURL` — an `HTTPException`, not the
        # `OSError`/`HTTPError` this method already classifies, so it fell
        # through to `_run`'s last-resort handler and used to log a full
        # traceback whose last line was the password. `redact_url` is not
        # only for output here: the credentials this adapter needs are
        # already resolved into `self._credentials` (named ref or URL
        # userinfo, either way — see `resolve_credentials`) and presented
        # as a real `Authorization` header below, so the URL itself never
        # needs to carry them for this adapter to work; a userinfo-free
        # URL is the *only* one `urllib` can actually open.
        request = urllib.request.Request(redact_url(self._url))
        if self._credentials is not None:
            token = base64.b64encode(
                "{}:{}".format(self._credentials.username, self._credentials.password).encode("utf-8")
            ).decode("ascii")
            request.add_header("Authorization", "Basic {}".format(token))

        try:
            response = self._opener(request)
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise _SourceError(
                    "source_auth_failed", "the camera rejected the configured credentials"
                ) from None
            raise _SourceError("source_error", "the camera answered HTTP {}".format(exc.code)) from None
        except urllib.error.URLError as exc:
            raise _SourceError(
                "source_unreachable", "could not reach the camera: {}".format(type(exc.reason).__name__)
            ) from None

        with self._response_lock:
            self._response = response
        try:
            self._read_frames(response)
        finally:
            with self._response_lock:
                self._response = None
            response.close()

    def _read_frames(self, response) -> None:
        buf = b""
        while not self._stop_event.is_set():
            chunk = response.read(4096)
            if not chunk:
                raise _SourceError("source_unavailable", "the MJPEG stream ended")
            buf += chunk
            if len(buf) > self._MAX_BUFFERED_BYTES:
                raise _SourceError("source_unavailable", "no JPEG frame boundary found in the stream")

            # Drain every complete frame already sitting in `buf` before
            # asking for more bytes — one `read()` can easily return more
            # than one frame's worth (a small stream, a slow consumer), and
            # extracting only one per `read()` would silently fall behind
            # forever rather than actually keeping up.
            while not self._stop_event.is_set():
                start = buf.find(self._JPEG_SOI)
                if start == -1:
                    break
                end = buf.find(self._JPEG_EOI, start + 2)
                if end == -1:
                    if start > 0:
                        buf = buf[start:]  # drop leading multipart-boundary text, keep the SOI onward
                    break

                timestamp_ms = sampling.capture_timestamp_ms()
                frame_bytes = buf[start : end + 2]
                buf = buf[end + 2 :]
                frame = cv2.imdecode(np.frombuffer(frame_bytes, dtype=np.uint8), cv2.IMREAD_COLOR)
                if frame is None:
                    continue  # one corrupt part; the next SOI/EOI pair is the recovery
                self._emit_frame(frame, timestamp_ms)

    def _close(self) -> None:
        with self._response_lock:
            if self._response is not None:
                self._response.close()


# --- V4L2 ----------------------------------------------------------------------


# CAP_PROP_READ_TIMEOUT_MSEC — the property OpenCV exposes for
# exactly this: a local capture device that stops responding mid-read
# (unplugged, a frozen driver). **Verified against real hardware
# (tools/verify_v4l2.py, a Logitech Brio 500) — and the answer is negative.**
# `cap.set(CAP_PROP_READ_TIMEOUT_MSEC, ...)` returns `False` on Ubuntu
# 22.04's `python3-opencv` (cv2 4.5.4, confirmed not a missing `libv4l-0`),
# so a stalled read has no bound this property can give it. `_default_
# v4l2_capture_factory` below still asks for it (defence in depth, free on
# any build where it does work), but nothing here depends on the backend's
# cooperation for the actual bound — see `_V4l2Worker`.
_V4L2_READ_TIMEOUT_MS = 3000


def _default_v4l2_capture_factory(device: str):
    with _suppress_native_stderr():
        cap = cv2.VideoCapture(device, cv2.CAP_V4L2)
    if cap.isOpened():
        # Only meaningful once a backend is actually attached — verified
        # separately (not asserted here) that CAP_PROP_READ_TIMEOUT_MSEC
        # returns False on an empty/unopened VideoCapture regardless of
        # backend support, so setting it before this point would always
        # look unsupported even when it is not.
        if not cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, _V4L2_READ_TIMEOUT_MS):
            log.warning(
                "This V4L2 backend build did not accept a read timeout "
                "(CAP_PROP_READ_TIMEOUT_MSEC) — a stalled read on %r has no "
                "bound other than this process being told to stop.",
                device,
            )
    return cap


# A stalled read is bounded by this, however uncooperative the backend is —
# comfortably under `_ThreadedSourceAdapter.stop()`'s own 5.0s join budget,
# the same margin RTSP's `stimeout` already keeps. `_V4l2Worker.read()` is
# polled in slices of `_V4L2_STOP_POLL_S` so `stop()` notices `_stop_event`
# quickly rather than waiting out the full window on every teardown.
# `wait_ready()` gets the identical treatment:
# a single blocking `get(timeout=_V4L2_OPEN_TIMEOUT_S)` left `stop()` unable
# to interrupt a still-opening worker at all — `terminate()` killed the
# child in milliseconds, but the parent's own wait stayed blind to that and
# sat out the full open budget regardless, on any machine without CPU to
# spare for the spawned child's own re-import (measured: reproduces at
# every `--cpus` from 0.5 to 8 on a 32-core host, invisible only because
# development runs unconstrained).
_V4L2_READ_TIMEOUT_S = _V4L2_READ_TIMEOUT_MS / 1000.0
_V4L2_OPEN_TIMEOUT_S = 5.0  # process spawn + cv2 import + open, real margin
_V4L2_STOP_POLL_S = 0.2
_V4L2_PROCESS_JOIN_S = 1.0  # after terminate()/kill(), before giving up
_V4L2_DROP_LOG_INTERVAL_S = 30.0  # log again while still dropping, not per frame


def _v4l2_worker_main(
    device: str,
    frame_queue: "mp.Queue",
    ready_queue: "mp.Queue",
    capture_factory: Callable[[str], object] = _default_v4l2_capture_factory,
) -> None:
    """The body of the child process `_V4l2Worker` spawns — see that
    class's docstring for why this runs in its own OS process rather than
    the adapter's own thread. Everything genuinely blocking (`cv2.
    VideoCapture(...)`'s open, `cap.read()`) happens here and nowhere the
    parent can be stuck by it.

    Talks back over two mailbox queues, never anything richer: `ready_queue`
    gets exactly one message (open outcome), `frame_queue` gets the latest
    frame, overwriting a stale one nobody has read yet rather than
    buffering (same reasoning `camera.LatestFrameHolder` already uses —
    only the newest frame is ever wanted). Deliberately does not watch for
    `stop()`/a poison pill: this process is disposable by construction —
    `_V4l2Worker.stop()` just kills it, and that is safe here in a way it
    is not for `_CvCaptureAdapter._close()`'s in-thread `cap.release()`,
    because nothing in *this* process survives past its death for a signal
    to corrupt.

    Mailbox semantics mean a consumer that cannot keep up (a slow resize/
    encode downstream, a loaded host) silently loses frames — right for a
    live camera (a stale frame is worse than none), wrong to lose *sight
    of* (a new silent drop path would be exactly that). `frame_queue`'s payload
    therefore carries this process's own running discard count alongside every
    frame
    (`dropped_total`) rather than opening a second channel for it — the
    parent (`V4l2SourceAdapter._connect_and_read`) is what turns a rising
    count into `_dropped_frames` (queryable per adapter) and a rate-limited
    log line; this function only has to count honestly."""
    try:
        cap = capture_factory(device)
    except Exception as exc:  # noqa: BLE001 - report, do not crash silently out of view
        ready_queue.put(("error", "source_unavailable", "{}".format(type(exc).__name__)))
        return
    if not cap.isOpened():
        cap.release()
        ready_queue.put(("error", "source_unavailable", "could not open {}".format(device)))
        return
    ready_queue.put(("ok",))
    dropped_total = 0
    try:
        while True:
            timestamp_ms = sampling.capture_timestamp_ms()
            ok, frame = cap.read()
            if not ok:
                continue  # the parent's own read timeout is what bounds a stall now
            try:
                frame_queue.get_nowait()  # an unread frame sat here -- the consumer fell behind
                dropped_total += 1
            except queue.Empty:
                pass
            try:
                frame_queue.put_nowait((timestamp_ms, frame, dropped_total))
            except queue.Full:
                dropped_total += 1  # lost the race with the drain above; this frame itself is lost
    finally:
        cap.release()


class _V4l2Worker:
    """A V4L2 capture running in its own OS process.

    Why a process and not a thread: `CAP_PROP_READ_TIMEOUT_MSEC` does not
    work on this backend build (see `_V4L2_READ_TIMEOUT_MS`'s docstring),
    so nothing in-process can make a stuck `cap.read()` return — and
    Python cannot forcibly stop a thread blocked inside a native call.
    `_CvCaptureAdapter._close()` already documents why releasing such a
    capture from a *different thread* than the one blocked inside it
    reproduced a real SIGSEGV — that risk is specific to
    sharing one process's address space with the stuck native call. A
    genuinely separate OS process has neither problem: `terminate()`/
    `kill()` bounds the wait deterministically (a real measurement, not an
    OpenCV property that may or may not be honoured), and the kernel
    reclaims the device file descriptor when the process dies, cooperative
    or not — the known leak (one held handle and one stuck thread per
    stalled attempt, no ceiling on the count) cannot
    happen here, because there is never more than one live worker process
    per adapter instance.

    Frame transfer cost, checked before this design was committed to: a
    `multiprocessing.Queue(maxsize=1)` mailbox at a worst-case 1920x1080 BGR
    frame (6.22 MB), 30 fps, 40s sustained — 0 frames dropped, throughput
    matched the 186 MB/s target,
    p95 latency 19.9 ms, RSS flat after the initial ramp (no leak). Cheap
    enough not to trade one resource problem for another.

    `spawn`, not `fork` (`multiprocessing.get_context("spawn")` — see
    `start()`): this process embeds rclpy/DDS, which run their own internal
    threads. Forking a multi-threaded process can copy a lock held by a
    thread that does not exist in the child, and hang there forever — a
    real, well-documented class of bug, not a hypothetical one, and not
    worth risking for the sake of the (small) extra startup latency `spawn`
    costs over `fork`."""

    def __init__(self, device: str) -> None:
        self._device = device
        self._ctx = mp.get_context("spawn")
        self._frame_queue: "mp.Queue" = self._ctx.Queue(maxsize=1)
        self._ready_queue: "mp.Queue" = self._ctx.Queue(maxsize=1)
        self._process: Optional["mp.process.BaseProcess"] = None

    def start(self) -> None:
        self._process = self._ctx.Process(
            target=_v4l2_worker_main,
            args=(self._device, self._frame_queue, self._ready_queue),
            daemon=True,
        )
        self._process.start()

    def wait_ready(self, timeout_s: float) -> None:
        """Raises `queue.Empty` if nothing has arrived within `timeout_s` —
        the caller decides what a stall means, the same convention `read()`
        already uses, so a caller can poll this in short slices against
        `_stop_event` exactly as `_connect_and_read`'s read loop already
        does (this used to be one un-pollable block, see this file's
        `_V4L2_OPEN_TIMEOUT_S` comment). A real failure the child reported
        (`outcome[0] != "ok"`, e.g. the device would not open) still raises
        `_SourceError` immediately: that is a fact, not a stall, and there
        is nothing to poll past."""
        outcome = self._ready_queue.get(timeout=timeout_s)  # raises queue.Empty on its own
        if outcome[0] != "ok":
            _, code, message = outcome
            raise _SourceError(code, message)

    def read(self, timeout_s: float) -> Tuple[int, "np.ndarray", int]:
        """Raises `queue.Empty` (not `_SourceError` — the caller decides
        what a stall means) if no frame arrives within `timeout_s`. The
        third element is `_v4l2_worker_main`'s own running discard count —
        see that function's docstring for why it travels with the frame
        rather than over a second channel."""
        return self._frame_queue.get(timeout=timeout_s)

    def stop(self) -> None:
        """Idempotent, best-effort, bounded — unlike `_CvCaptureAdapter`'s
        in-thread release, killing a whole process is always safe to do
        from any thread, at any point in that process's execution."""
        process, self._process = self._process, None
        if process is None or not process.is_alive():
            return
        process.terminate()
        process.join(timeout=_V4L2_PROCESS_JOIN_S)
        if process.is_alive():
            process.kill()
            process.join(timeout=_V4L2_PROCESS_JOIN_S)


class V4l2SourceAdapter(_ThreadedSourceAdapter):
    """`kind: 'v4l2'` — a local capture device, no network,
    no credentials. Errors are local (`source_unavailable`): can't open,
    permission denied, device busy or unplugged mid-stream — cv2 does not
    reliably distinguish these across platforms, so this adapter does not
    pretend to either.

    Not a `_CvCaptureAdapter` (unlike RTSP): the actual `cv2.VideoCapture`
    now lives in a child process (`_V4l2Worker`), not in this
    adapter's own thread, so the shared cv2-in-this-thread read loop does
    not apply here. It still fits `_ThreadedSourceAdapter`'s shape exactly
    — `_connect_and_read()` is the one method that differs.

    `_dropped_frames`: the worker's `maxsize=1` mailbox drops a frame
    outright when this adapter's own consumer falls behind — right for a live
    camera (a stale frame is worse than none) but wrong to lose *sight of*
    silently.
    Read directly for tests/diagnostics; also logged, once on
    the first drop and then at most every `_V4L2_DROP_LOG_INTERVAL_S`
    while it keeps climbing — never per frame.

    What this counter has NOT been shown to do: fire under a real
    camera's own timing. It is proven against a deliberately unthrottled
    fake producer (`_FakeCapture`, no real hardware — none exists in this
    container) that produces far faster than any consumer here needs to
    drain it; the two tests exercising it prove the counting and
    rate-limiting logic, not that the mailbox drops in practice. Whether a
    real V4L2 device's own frame rate against this adapter's real
    downstream consumer (resize, snapshot, live publish) ever actually
    fills the mailbox — or never does, making this counter correct but
    permanently zero on real hardware — is open, and needs a robot to
    answer, not a fake."""

    def __init__(
        self,
        *,
        device: str,
        on_frame: OnFrame,
        on_error: OnError,
        worker_factory: Optional[Callable[[str], "_V4l2Worker"]] = None,
        backoff: Optional[Backoff] = None,
    ) -> None:
        validate_device_path(device)  # see its own docstring
        super().__init__(on_frame=on_frame, on_error=on_error, backoff=backoff)
        self._device = device
        self._worker_factory = worker_factory or _V4l2Worker
        self._worker: Optional["_V4l2Worker"] = None
        self._worker_lock = threading.Lock()
        self._dropped_frames = 0
        self._dropped_frames_logged_at: Optional[float] = None

    def _note_dropped_frames(self, dropped_total: int) -> None:
        if dropped_total == self._dropped_frames:
            return
        self._dropped_frames = dropped_total
        now = time.monotonic()
        if (
            self._dropped_frames_logged_at is None
            or now - self._dropped_frames_logged_at >= _V4L2_DROP_LOG_INTERVAL_S
        ):
            log.warning(
                "%r has dropped %d frame(s) so far -- the consumer is not "
                "keeping up with the camera's own rate, not a source "
                "failure (a stale frame is discarded in favour of a fresh "
                "one, by design).",
                self._device, dropped_total,
            )
            self._dropped_frames_logged_at = now

    def _connect_and_read(self) -> None:
        worker = self._worker_factory(self._device)
        with self._worker_lock:
            self._worker = worker
        try:
            worker.start()
            # Polled in the same _V4L2_STOP_POLL_S slices as the read loop
            # below, and for the identical reason: a single blocking
            # wait_ready(_V4L2_OPEN_TIMEOUT_S) could not notice
            # _stop_event at all, so a stop() called while the worker was
            # still opening sat out the full open budget regardless of how
            # fast terminate() itself killed the child (see this file's
            # _V4L2_OPEN_TIMEOUT_S comment).
            open_silence_s = 0.0
            while not self._stop_event.is_set():
                try:
                    worker.wait_ready(_V4L2_STOP_POLL_S)
                except queue.Empty:
                    open_silence_s += _V4L2_STOP_POLL_S
                    if open_silence_s >= _V4L2_OPEN_TIMEOUT_S:
                        raise _SourceError(
                            "source_unavailable",
                            "no answer opening {} within {:.0f}s".format(
                                self._device, _V4L2_OPEN_TIMEOUT_S
                            ),
                        )
                    continue
                break
            else:
                return  # stop() was called before the worker ever reported ready
            silence_s = 0.0
            while not self._stop_event.is_set():
                try:
                    timestamp_ms, frame, dropped_total = worker.read(_V4L2_STOP_POLL_S)
                except queue.Empty:
                    silence_s += _V4L2_STOP_POLL_S
                    if silence_s >= _V4L2_READ_TIMEOUT_S:
                        raise _SourceError(
                            "source_unavailable", "the camera stopped delivering frames"
                        )
                    continue
                silence_s = 0.0
                self._note_dropped_frames(dropped_total)
                self._emit_frame(frame, timestamp_ms)
        finally:
            with self._worker_lock:
                self._worker = None
            worker.stop()

    def _close(self) -> None:
        """Cross-thread nudge from `stop()` — safe
        here in a way it is not for `_CvCaptureAdapter`: killing a separate
        process from any thread cannot corrupt this one. Lets a `stop()`
        called mid-stall return promptly instead of waiting out
        `_V4L2_STOP_POLL_S`'s own slice."""
        with self._worker_lock:
            worker = self._worker
        if worker is not None:
            worker.stop()
