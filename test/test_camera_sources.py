# SPDX-License-Identifier: Apache-2.0
"""Non-ROS camera sources in isolation: the RTSP preflight's
failure classification, MJPEG's multipart parsing and header-based auth,
V4L2's local capture, and the redaction machinery — all
against fakes, no real network or hardware. Real-device behaviour belongs
to the end-to-end checks, which drive a real RTSP/MJPEG server and a real
V4L2 camera; this file proves the adapters against injected doubles, the same
role test_live.py plays for LiveKit."""
import base64
import multiprocessing as mp
import os
import queue
import socket
import threading
import time
from typing import Dict

import cv2
import numpy as np
import pytest

from fleetless_bridge import camera_sources
from fleetless_bridge.camera_sources import (
    Backoff,
    Credentials,
    MjpegSourceAdapter,
    RtspSourceAdapter,
    V4l2SourceAdapter,
    _SourceError,
    _suppress_native_stderr,
    redact_url,
    resolve_credentials,
    validate_device_path,
    validate_source_scheme,
)

_FAST_BACKOFF = Backoff(initial_s=0.01, max_s=0.02)


def _solid_frame(color=(10, 20, 30)):
    frame = np.zeros((4, 6, 3), dtype=np.uint8)
    frame[:, :] = color
    return frame


class _Recorder:
    """Thread-safe collector for `on_frame`/`on_error` callbacks — adapters
    call these from their own background thread."""

    def __init__(self):
        self._lock = threading.Lock()
        self.frames = []
        self.errors = []

    def on_frame(self, bgr, timestamp_ms):
        with self._lock:
            self.frames.append((bgr, timestamp_ms))

    def on_error(self, code, message):
        with self._lock:
            self.errors.append((code, message))

    def wait_for_frames(self, n, timeout_s=2.0):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.frames) >= n:
                    return
            time.sleep(0.01)
        raise AssertionError("timed out waiting for {} frames, got {}".format(n, len(self.frames)))

    def wait_for_errors(self, n, timeout_s=2.0):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.errors) >= n:
                    return
            time.sleep(0.01)
        raise AssertionError("timed out waiting for {} errors, got {}".format(n, len(self.errors)))


# --- redact_url / resolve_credentials -------------------------------------------


def test_validate_source_scheme_accepts_an_allowed_scheme():
    validate_source_scheme("rtsp://192.0.2.1:554/stream", ("rtsp", "rtsps"))  # must not raise


def test_validate_source_scheme_is_case_insensitive():
    validate_source_scheme("RTSP://192.0.2.1:554/stream", ("rtsp", "rtsps"))  # must not raise


def test_validate_source_scheme_rejects_a_disallowed_scheme():
    with pytest.raises(ValueError, match="rtsp"):
        validate_source_scheme("http://192.0.2.1/stream", ("rtsp", "rtsps"))


def test_validate_source_scheme_rejects_file_urls():
    """The `url` field had no scheme constraint until this sha, so
    `file:///etc/hostname` validated as a legal `mjpeg` or `rtsp` source —
    both `urllib.request.urlopen` and cv2's ffmpeg backend honour `file:`,
    turning a config document into an arbitrary local-file reader. Pinned
    directly, independent of whatever the wire schema currently enforces."""
    with pytest.raises(ValueError):
        validate_source_scheme("file:///etc/hostname", ("http", "https"))
    with pytest.raises(ValueError):
        validate_source_scheme("file:///etc/hostname", ("rtsp", "rtsps"))


def test_validate_source_scheme_rejects_ftp_urls():
    with pytest.raises(ValueError):
        validate_source_scheme("ftp://192.0.2.1/stream", ("http", "https"))


def test_rtsp_adapter_construction_rejects_a_file_url():
    """The construction-time half — see validate_source_scheme's own
    docstring. Rejected at __init__, the same "caught now, not at the
    first frame" timing _RosSourceAdapter uses for a bad ROS type: a
    clean per-slug config_applied error, not a background retry loop
    reading a local file forever."""
    with pytest.raises(ValueError):
        RtspSourceAdapter(
            url="file:///etc/hostname", transport="tcp", credentials=None,
            on_frame=lambda *a: None, on_error=lambda *a: None,
        )


def test_mjpeg_adapter_construction_rejects_a_file_url():
    with pytest.raises(ValueError):
        MjpegSourceAdapter(
            url="file:///etc/hostname", credentials=None,
            on_frame=lambda *a: None, on_error=lambda *a: None,
        )


def test_validate_device_path_accepts_an_ordinary_device():
    validate_device_path("/dev/video0")  # must not raise


def test_validate_device_path_accepts_a_stable_by_id_symlink():
    validate_device_path("/dev/v4l/by-id/usb-Logitech_BRIO-video-index0")  # must not raise


def test_validate_device_path_rejects_a_path_outside_dev():
    """The fourth source kind was untouched by the first fix. Against real
    cv2, an ordinary local video file opens and its pixels get published, and
    so does an http:// URL — the same violation validate_source_scheme
    closes for rtsp/mjpeg, reachable here because 'it is just a device
    path' read like a reason not to check."""
    with pytest.raises(ValueError):
        validate_device_path("/etc/hostname")
    with pytest.raises(ValueError):
        validate_device_path("http://127.0.0.1:8899/secret.jpg")


def test_validate_device_path_rejects_a_dotdot_segment():
    with pytest.raises(ValueError):
        validate_device_path("/dev/../etc/hostname")


def test_validate_device_path_rejects_a_trailing_slash():
    with pytest.raises(ValueError):
        validate_device_path("/dev/")


def test_v4l2_adapter_construction_rejects_a_path_outside_dev():
    with pytest.raises(ValueError):
        V4l2SourceAdapter(device="/etc/hostname", on_frame=lambda *a: None, on_error=lambda *a: None)


def test_redact_url_strips_userinfo():
    assert redact_url("rtsp://user:pass@192.0.2.1:554/stream") == "rtsp://192.0.2.1:554/stream"


def test_redact_url_leaves_a_url_with_no_userinfo_unchanged():
    assert redact_url("rtsp://192.0.2.1:554/stream") == "rtsp://192.0.2.1:554/stream"


def test_resolve_credentials_explicit_block_wins_over_url_userinfo():
    """The 3.0 precedence, kept from the retired `credentials_ref`: a
    rotation must not appear to silently keep using the URL's old
    password."""
    result = resolve_credentials(
        url="rtsp://urluser:urlpass@host/x",
        credentials=("realuser", "realpass"),
    )
    assert result == Credentials(username="realuser", password="realpass")


def test_resolve_credentials_falls_back_to_url_userinfo_when_none_is_written():
    result = resolve_credentials(url="rtsp://u:p@host/x", credentials=None)
    assert result == Credentials(username="u", password="p")


def test_resolve_credentials_is_none_with_nothing_written_and_no_url_userinfo():
    assert resolve_credentials(url="rtsp://host/x", credentials=None) is None


def test_resolve_credentials_url_decodes_percent_escaped_userinfo():
    result = resolve_credentials(url="rtsp://a%40b:p%40ss@host/x", credentials=None)
    assert result == Credentials(username="a@b", password="p@ss")


def test_a_half_written_credential_block_still_wins_over_url_userinfo():
    """Both `username` and `password` are optional, so the parser can hand
    over `("user", "")` — still an explicit answer, and must not silently
    fall through to the URL's own userinfo. A connection that fails with
    the password somebody actually wrote is diagnosable; one that quietly
    succeeds with a different password is not."""
    result = resolve_credentials(url="rtsp://urluser:urlpass@host/x", credentials=("user", ""))
    assert result == Credentials(username="user", password="")


# --- Backoff -----------------------------------------------------------------


def test_backoff_doubles_up_to_the_cap_then_holds():
    backoff = Backoff(initial_s=1.0, max_s=8.0, factor=2.0)
    assert [backoff.next() for _ in range(5)] == [1.0, 2.0, 4.0, 8.0, 8.0]


def test_backoff_reset_returns_to_the_initial_delay():
    backoff = Backoff(initial_s=1.0, max_s=8.0, factor=2.0)
    backoff.next()
    backoff.next()
    backoff.reset()
    assert backoff.next() == 1.0


# --- _suppress_native_stderr: the ffmpeg-diagnostics leak channel --------------


def test_suppress_native_stderr_swallows_a_direct_fd_write(capfd):
    with _suppress_native_stderr():
        os.write(2, b"a raw native write containing hunter2\n")
    captured = capfd.readouterr()
    assert "hunter2" not in captured.err
    assert "hunter2" not in captured.out


def test_suppress_native_stderr_restores_stderr_afterwards(capfd):
    with _suppress_native_stderr():
        pass
    os.write(2, b"back to normal\n")
    captured = capfd.readouterr()
    assert "back to normal" in captured.err


def test_suppress_native_stderr_is_safe_for_concurrent_callers(tmp_path):
    """The redesign made concurrent RTSP opens genuinely
    parallel, which means concurrent `_suppress_native_stderr()` callers
    too -- a real, newly-introduced risk this function's own reference
    counting exists to close: two unsynchronized dup2() dances on the same
    real fd 2 could corrupt the fd table (restore the wrong target, double
    -close, leak the real stderr permanently). Ten real threads, each
    writing its own marker to the real fd while inside the block.

    What this does NOT assert, and a first version of this test wrongly
    did (caught by running it, not by reasoning about it): that each
    thread's OWN post-exit write lands on real stderr immediately after
    ITS OWN `with` block ends. Reference counting shares one suppression
    window across every concurrent holder -- fd 2 only comes back once
    the LAST one releases it, so an early releaser's own subsequent write
    is *correctly* still suppressed if anyone else is still active. That
    is the intended semantics of counting a shared resource, not a bug;
    the actual contract is group-level: nothing written by ANYONE reaches
    real stderr while at least one caller is inside, and once every caller
    has genuinely finished (all threads joined), the fd is unconditionally
    restored for everyone after them.

    Deliberately not `capfd`: ten threads all dup2()-ing fd 2 back and
    forth concurrently is more fd churn than pytest's own capture fixture
    reliably tracks (measured -- `os.write()` succeeded, correct byte
    count, on every single call, but several landed nowhere `capfd` could
    later find; a test artifact, not evidence of the production code being
    wrong). A self-owned temp file, redirected onto fd 2 for the duration
    of this test and read back directly afterwards, removes that
    variable."""
    real_stderr = os.dup(2)
    log_path = tmp_path / "stderr.log"
    log_fd = os.open(str(log_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    os.dup2(log_fd, 2)
    os.close(log_fd)
    try:
        n = 10
        barrier = threading.Barrier(n)
        errors = []

        def worker(i):
            try:
                with _suppress_native_stderr():
                    barrier.wait(timeout=5.0)  # force real overlap across all n callers
                    os.write(2, "inside-{}\n".format(i).encode())
                    time.sleep(0.05)
                os.write(2, "maybe-suppressed-outside-{}\n".format(i).encode())
            except Exception as exc:  # noqa: BLE001 - surfaced via `errors`, not swallowed
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)  # every caller has now genuinely finished

        assert not errors, errors
        # Written strictly after every concurrent holder is done -- this
        # one, and only this one, is guaranteed unconditionally.
        os.write(2, b"stderr is restored\n")
        content = log_path.read_text()
    finally:
        os.dup2(real_stderr, 2)
        os.close(real_stderr)

    # Nothing written while suppression was active for anyone leaked.
    for i in range(n):
        assert "inside-{}".format(i) not in content
    # No corruption: not one of the ten dup2() dances lost track of the
    # real target or double-closed it -- if it had, this final write
    # (issued only after every thread's own release attempt has already
    # run) would land on a stale or already-closed descriptor instead.
    assert "stderr is restored" in content


# --- _ValueGate: the shared-value primitive behind the RTSP options redesign ---


def test_value_gate_multiple_holders_of_the_same_value_run_concurrently():
    gate = camera_sources._ValueGate(initial="a")
    n = 5
    barrier = threading.Barrier(n)
    reached = []

    def holder():
        gate.acquire("a")
        try:
            reached.append(True)
            barrier.wait(timeout=5.0)  # every holder must be inside at once
        finally:
            gate.release()

    threads = [threading.Thread(target=holder) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)
    assert len(reached) == n  # the barrier not timing out is the actual proof


def test_value_gate_a_different_value_waits_for_the_current_holder_to_release():
    gate = camera_sources._ValueGate(initial="a")
    events = []
    events_lock = threading.Lock()

    gate.acquire("a")

    def wants_b():
        gate.acquire("b")
        with events_lock:
            events.append("b")
        gate.release()

    t = threading.Thread(target=wants_b)
    t.start()
    time.sleep(0.2)  # a real chance for a broken gate to let it through early
    with events_lock:
        assert events == []  # "b" must not have gotten in while "a" is held
    gate.release()
    t.join(timeout=5.0)
    assert events == ["b"]


def test_value_gate_a_second_caller_wanting_the_same_new_value_joins_instead_of_waiting():
    """The exact defect `_ValueGate` replaces `_RWLock` to fix (found
    via a mixed-transport RTSP fleet test): `_RWLock` called anyone who
    arrived seeing a mismatch a "writer", and a writer had to wait for
    `readers == 0` -- even readers already reading the exact value the
    writer was about to set. Two `udp` cameras arriving together after a
    `tcp` fleet reproduced it directly: the second waited out the first's
    entire connect, because the first was still "a reader" (of `udp`, the
    value the second also wanted).

    Proven directly here: a second caller wanting the value the first
    just set must join within the time the first is still holding it --
    not only once the first releases."""
    gate = camera_sources._ValueGate(initial="a")
    first_holding = threading.Event()
    let_first_release = threading.Event()
    second_joined = threading.Event()

    def first_wants_b():
        gate.acquire("b")
        first_holding.set()
        let_first_release.wait(timeout=5.0)
        gate.release()

    def second_wants_b():
        first_holding.wait(timeout=5.0)  # only start once the first already holds "b"
        gate.acquire("b")
        second_joined.set()
        gate.release()

    t1 = threading.Thread(target=first_wants_b)
    t2 = threading.Thread(target=second_wants_b)
    t1.start()
    t2.start()
    assert first_holding.wait(timeout=5.0)
    # The second caller must join while the first is STILL holding "b" --
    # this is checked before let_first_release is ever set, so there is
    # no way for the assertion below to pass because the first happened
    # to finish first.
    assert second_joined.wait(timeout=2.0), (
        "the second caller waited for the first to release instead of joining its value"
    )
    let_first_release.set()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)


# --- V4L2 ----------------------------------------------------------------------


class _FakeCapture:
    def __init__(self, *, opened=True, frame=None):
        self._opened = opened
        self._frame = frame
        self.released = False

    def isOpened(self):
        return self._opened

    def read(self):
        if not self._opened or self._frame is None:
            return False, None
        return True, self._frame

    def release(self):
        self.released = True
        self._opened = False


class _FakeWorker:
    """Stands in for `_V4l2Worker` at `V4l2SourceAdapter`'s `worker_factory`
    seam — no real process, drives frames/errors in-memory. Same role
    `_FakeCapture` plays for RTSP/MJPEG's `capture_factory`/`opener` seam;
    the real-process tests below
    (`test_v4l2_adapter_bounds_a_genuinely_stuck_worker...` and its
    `stop()` sibling) exercise actual OS-level behaviour — this fake is
    for everything else."""

    def __init__(self, *, ready_error=None, frame=None):
        self._ready_error = ready_error
        self._frame = frame
        self.started = False
        self.stop_calls = []

    def start(self):
        self.started = True

    def wait_ready(self, timeout_s):
        if self._ready_error is not None:
            raise self._ready_error

    def read(self, timeout_s):
        if self._frame is None:
            raise queue.Empty()
        return self._frame

    def stop(self):
        self.stop_calls.append(threading.current_thread())


def test_v4l2_adapter_delivers_frames_from_the_worker():
    frame = _solid_frame()
    workers = []

    def factory(device):
        assert device == "/dev/video0"
        w = _FakeWorker(frame=(1_700_000_000_000, frame, 0))
        workers.append(w)
        return w

    rec = _Recorder()
    adapter = V4l2SourceAdapter(
        device="/dev/video0", on_frame=rec.on_frame, on_error=rec.on_error, worker_factory=factory
    )
    adapter.start()
    try:
        rec.wait_for_frames(3)
    finally:
        adapter.stop()
    assert np.array_equal(rec.frames[0][0], frame)
    assert isinstance(rec.frames[0][1], int)
    assert workers[0].started


def test_v4l2_adapter_reports_source_unavailable_when_the_device_will_not_open():
    def factory(device):
        return _FakeWorker(ready_error=_SourceError("source_unavailable", "could not open {}".format(device)))

    rec = _Recorder()
    adapter = V4l2SourceAdapter(
        device="/dev/video9",
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        worker_factory=factory,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_unavailable"
    assert "/dev/video9" in rec.errors[0][1]


def test_v4l2_adapter_does_not_spam_the_same_error_on_every_retry():
    def factory(device):
        return _FakeWorker(ready_error=_SourceError("source_unavailable", "could not open {}".format(device)))

    rec = _Recorder()
    adapter = V4l2SourceAdapter(
        device="/dev/video9",
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        worker_factory=factory,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
        time.sleep(0.15)  # several retry cycles at this backoff
    finally:
        adapter.stop()
    assert len(rec.errors) == 1


def test_v4l2_adapter_reports_source_unavailable_when_reads_stay_silent():
    """The read-timeout half, against the fake: after `wait_ready`
    succeeds, `read()` never delivers anything — the real OS-level version
    of this (a worker that genuinely never answers, killed for real) is
    `test_v4l2_adapter_bounds_a_genuinely_stuck_worker_and_kills_it` below;
    this one is the fast, no-process version of the same contract, and
    also proves `_V4L2_STOP_POLL_S` slicing adds up correctly instead of
    firing early or never."""

    def factory(device):
        return _FakeWorker()  # ready, but read() always raises queue.Empty

    rec = _Recorder()
    adapter = V4l2SourceAdapter(
        device="/dev/video0",
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        worker_factory=factory,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1, timeout_s=camera_sources._V4L2_READ_TIMEOUT_S + 3.0)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_unavailable"


class _FakeV4l2Capture:
    """Stands in for `cv2.VideoCapture` at the point
    `_default_v4l2_capture_factory` uses it — narrower than `_FakeCapture`
    above (which stands in at `V4l2SourceAdapter`'s own `capture_factory`
    seam, a level above where the timeout property gets set, so it cannot
    see this)."""

    def __init__(self, *, opened=True, set_succeeds=True):
        self._opened = opened
        self.set_succeeds = set_succeeds
        self.set_calls = []

    def isOpened(self):
        return self._opened

    def set(self, prop, value):
        self.set_calls.append((prop, value))
        return self.set_succeeds


def test_default_v4l2_capture_factory_opens_through_the_v4l2_backend_explicitly(monkeypatch):
    """F: a bare device path with no explicit backend can resolve to a
    *different* backend than V4L2 — reproduced directly against this
    build (cv2 4.5.4): `cv2.VideoCapture('/dev/video99')` with no backend
    argument tried GStreamer first, not the V4L2 API. `CAP_PROP_READ_
    TIMEOUT_MSEC` is documented against V4L2 specifically, so
    auto-detection would make the property's support silently
    backend-dependent and unpredictable."""
    calls = {}

    def fake_video_capture(device, backend):
        calls["device"] = device
        calls["backend"] = backend
        return _FakeV4l2Capture()

    monkeypatch.setattr(camera_sources.cv2, "VideoCapture", fake_video_capture)
    camera_sources._default_v4l2_capture_factory("/dev/video0")
    assert calls == {"device": "/dev/video0", "backend": cv2.CAP_V4L2}


def test_default_v4l2_capture_factory_asks_the_backend_for_a_read_timeout(monkeypatch):
    """F: proves only what can be proven without real hardware — the
    adapter asks. Whether a real V4L2 backend build honors the request is
    not verifiable in this environment (no /dev/video* device exists in
    this container); see `_V4L2_READ_TIMEOUT_MS`'s own docstring for what
    was and was not established."""
    fake = _FakeV4l2Capture(set_succeeds=True)
    monkeypatch.setattr(camera_sources.cv2, "VideoCapture", lambda device, backend: fake)
    camera_sources._default_v4l2_capture_factory("/dev/video0")
    assert fake.set_calls == [
        (cv2.CAP_PROP_READ_TIMEOUT_MSEC, camera_sources._V4L2_READ_TIMEOUT_MS)
    ]


def test_default_v4l2_capture_factory_only_sets_the_timeout_once_a_backend_is_attached(monkeypatch):
    """Verified separately (needs no fake — a real property of an unopened
    `cv2.VideoCapture`): `cap.set(...)` returns `False` with no backend
    attached yet, regardless of what that backend would otherwise
    support. Setting the property on a capture that failed to open would
    therefore always look unsupported — so this factory must only try
    once `isOpened()` is true."""
    fake = _FakeV4l2Capture(opened=False)
    monkeypatch.setattr(camera_sources.cv2, "VideoCapture", lambda device, backend: fake)
    camera_sources._default_v4l2_capture_factory("/dev/video0")
    assert fake.set_calls == []


def test_default_v4l2_capture_factory_warns_loudly_when_the_backend_refuses_the_timeout(
    monkeypatch, capfd
):
    """F: a backend that refuses the read timeout must not fail silently
    — a stalled read with no bound is invisible until it has already
    happened. `capfd`, not `caplog`, which passes unconditionally here
    and would test nothing."""
    fake = _FakeV4l2Capture(set_succeeds=False)
    monkeypatch.setattr(camera_sources.cv2, "VideoCapture", lambda device, backend: fake)
    camera_sources._default_v4l2_capture_factory("/dev/video7")
    captured = capfd.readouterr()
    assert "/dev/video7" in captured.err
    assert "read timeout" in captured.err.lower()


def test_default_v4l2_capture_factory_still_returns_the_capture_when_the_backend_refuses(
    monkeypatch,
):
    """Graceful degradation, the same shape every other adapter in this
    module already follows: a backend that cannot bound its own reads is
    still better than refusing to open the camera at all."""
    fake = _FakeV4l2Capture(set_succeeds=False)
    monkeypatch.setattr(camera_sources.cv2, "VideoCapture", lambda device, backend: fake)
    result = camera_sources._default_v4l2_capture_factory("/dev/video0")
    assert result is fake


def test_a_broken_on_frame_callback_never_logs_its_raw_message(capfd):
    """The module's rule — a raw exception must never reach the log —
    was applied only to the two `except` blocks once found leaking, not
    to every `except` that could leak. `on_frame` is caller-supplied
    (RosRuntime wires it to `self._enqueue(...)`) and could raise
    anything; `_emit_frame`'s handler must not let that message through
    either."""

    def on_frame(bgr, timestamp_ms):
        raise RuntimeError("leaked-secret-abc123")

    rec = _Recorder()
    adapter = V4l2SourceAdapter(
        device="/dev/video0",
        on_frame=on_frame,
        on_error=rec.on_error,
        worker_factory=lambda d: _FakeWorker(frame=(1_700_000_000_000, _solid_frame(), 0)),
    )
    adapter.start()
    time.sleep(0.2)
    adapter.stop()
    captured = capfd.readouterr()
    assert "leaked-secret-abc123" not in captured.err
    assert "leaked-secret-abc123" not in captured.out


def test_a_broken_on_error_callback_never_logs_its_raw_message(capfd):
    """Same rule, the other reporting path: `on_error` could also raise."""

    def on_error(code, message):
        raise RuntimeError("leaked-secret-xyz789")

    adapter = V4l2SourceAdapter(
        device="/dev/video9",
        on_frame=lambda *a: None,
        on_error=on_error,
        worker_factory=lambda d: _FakeWorker(
            ready_error=_SourceError("source_unavailable", "could not open {}".format(d))
        ),
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    time.sleep(0.2)
    adapter.stop()
    captured = capfd.readouterr()
    assert "leaked-secret-xyz789" not in captured.err
    assert "leaked-secret-xyz789" not in captured.out


def test_report_once_never_logs_a_secret_chained_through_the_exception_it_is_handling(capfd):
    """`_report_once` runs *inside* `_run`'s own `except _SourceError as exc:`
    handler, so if `on_error` itself raises, Python's own implicit chaining
    sets the new exception's
    `__context__` to whatever was already being handled — which can in
    turn chain further back. `log.exception`/any `exc_info=True` call
    would render the whole chain, so a call site holding no secret itself
    is not automatically safe to log verbosely; what it is chained to
    matters too. `log.error` with no `exc_info` renders none of it,
    regardless of how deep the chain goes — reproduced here with a
    three-level chain (on_error's own RuntimeError, chained to the
    _SourceError being handled, chained to the original secret-bearing
    exception that produced it)."""

    class _ChainedFailWorker(_FakeWorker):
        def wait_ready(self, timeout_s):
            try:
                raise ValueError("SECRET-deep-in-the-chain")
            except ValueError:
                # Deliberately not `from None` — this is the scenario
                # under test: an implicit chain, not a suppressed one.
                raise _SourceError("source_unavailable", "could not open device")

    def on_error(code, message):
        raise RuntimeError("on_error's own unrelated failure")

    adapter = V4l2SourceAdapter(
        device="/dev/video9",
        on_frame=lambda *a: None,
        on_error=on_error,
        worker_factory=lambda d: _ChainedFailWorker(),
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    time.sleep(0.2)
    adapter.stop()
    captured = capfd.readouterr()
    assert "SECRET-deep-in-the-chain" not in captured.err
    assert "SECRET-deep-in-the-chain" not in captured.out


def test_v4l2_adapter_stop_stops_the_worker_and_joins():
    rec = _Recorder()
    workers = []

    def factory(device):
        w = _FakeWorker(frame=(1_700_000_000_000, _solid_frame(), 0))
        workers.append(w)
        return w

    adapter = V4l2SourceAdapter(
        device="/dev/video0", on_frame=rec.on_frame, on_error=rec.on_error, worker_factory=factory
    )
    adapter.start()
    rec.wait_for_frames(1)
    adapter.stop()
    assert workers[0].stop_calls  # _close()'s cross-thread nudge and/or _connect_and_read's own finally


# --- V4L2: F, real OS-level behaviour ----------------------
#
# Everything above uses `_FakeWorker` — no real process, the same role
# fakes play elsewhere in this file. This row proves a bound that does
# not depend on anything cooperating, so it needs a real
# `multiprocessing.Process` whose body genuinely blocks forever
# (`threading.Event().wait()` with no `set()`), not a double that
# always answers — this container has no `/dev/video0`, but a real
# OS-level block does not need real hardware to be real.


def _real_block_target(ready_queue, frame_queue) -> None:
    """Module-level, not a closure — `spawn` pickles the target by
    reference, so a lambda or nested function would fail before the
    assertion under test is even reached. Reports ready immediately,
    then genuinely never returns."""
    ready_queue.put(("ok",))
    threading.Event().wait()  # never set — this is the "somebody unplugged it" case


class _RealBlockingWorker(camera_sources._V4l2Worker):
    """A real `_V4l2Worker` (real process, queues, terminate/kill) whose
    body is `_real_block_target` instead of the production
    `_v4l2_worker_main` — process lifecycle (spawn, terminate/join/kill)
    is the real, unmodified implementation; only the child's own body is
    swapped, the same seam `worker_factory` gives `V4l2SourceAdapter`
    one level up."""

    def start(self) -> None:
        self._process = self._ctx.Process(
            target=_real_block_target, args=(self._ready_queue, self._frame_queue), daemon=True
        )
        self._process.start()
        self.spawned_process = self._process  # kept even after stop() nulls self._process


def test_v4l2_adapter_bounds_a_genuinely_stuck_worker_and_kills_it():
    """`CAP_PROP_READ_TIMEOUT_MSEC` returning `True`
    only ever proved the backend agreed to store a number (see that
    constant's own docstring) — never that a real stall was bounded. This
    proves the replacement is: a worker that truly never answers still
    gets reported within the read timeout, and the process behind it is
    genuinely dead afterwards, not merely dereferenced."""
    rec = _Recorder()
    workers = []

    def factory(device):
        w = _RealBlockingWorker(device)
        workers.append(w)
        return w

    adapter = V4l2SourceAdapter(
        device="/dev/video0",
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        worker_factory=factory,
        backoff=_FAST_BACKOFF,
    )
    start = time.monotonic()
    adapter.start()
    try:
        rec.wait_for_errors(1, timeout_s=camera_sources._V4L2_READ_TIMEOUT_S + 10.0)
    finally:
        adapter.stop()
    elapsed = time.monotonic() - start

    assert rec.errors[0][0] == "source_unavailable"
    # Bounded by the read timeout with real margin for process scheduling —
    # not exact equality, this is a real process, not a mock.
    assert elapsed < camera_sources._V4L2_READ_TIMEOUT_S + 5.0
    assert workers  # the factory really did run
    assert not workers[0].spawned_process.is_alive()  # killed, not abandoned


def test_v4l2_adapter_stop_kills_a_stuck_worker_promptly_not_after_the_full_read_timeout():
    """The other half: `_close()`'s cross-thread nudge (safe here in a way
    it is not for `_CvCaptureAdapter`'s in-thread `cap.release()` — see
    `_V4l2Worker`'s own docstring) must make `stop()` return quickly even
    while the worker is still genuinely blocked, not wait out the full
    read-timeout window on every ordinary teardown."""
    rec = _Recorder()
    workers = []

    def factory(device):
        w = _RealBlockingWorker(device)
        workers.append(w)
        return w

    adapter = V4l2SourceAdapter(
        device="/dev/video0", on_frame=rec.on_frame, on_error=rec.on_error, worker_factory=factory
    )
    adapter.start()
    deadline = time.monotonic() + 5.0
    while not workers and time.monotonic() < deadline:
        time.sleep(0.01)
    assert workers, "worker was never constructed"
    time.sleep(0.3)  # let it actually settle into the blocked read loop

    start = time.monotonic()
    adapter.stop()
    elapsed = time.monotonic() - start

    assert elapsed < camera_sources._V4L2_READ_TIMEOUT_S  # nowhere near the read-timeout bound
    assert not workers[0].spawned_process.is_alive()


def _fake_opening_capture_factory(device: str):
    return _FakeCapture(frame=_solid_frame())


def test_v4l2_worker_main_runs_for_real_in_a_real_spawned_process():
    """The two tests above swap out the worker's *body* to prove process
    lifecycle. This one runs the actual production body,
    `_v4l2_worker_main`, in a real spawned process — only the cv2
    capture it opens is swapped (`_fake_opening_capture_factory`,
    module-level so `spawn` can pickle it by reference) — so the queue
    protocol (`ready_queue`'s one message, `frame_queue`'s mailbox
    semantics) is exercised for real, not assumed from the `_FakeWorker`
    fakes above."""
    ctx = mp.get_context("spawn")
    frame_q = ctx.Queue(maxsize=1)
    ready_q = ctx.Queue(maxsize=1)
    proc = ctx.Process(
        target=camera_sources._v4l2_worker_main,
        args=("/dev/video0", frame_q, ready_q),
        kwargs={"capture_factory": _fake_opening_capture_factory},
        daemon=True,
    )
    proc.start()
    try:
        outcome = ready_q.get(timeout=10.0)
        assert outcome == ("ok",)
        timestamp_ms, frame, dropped_total = frame_q.get(timeout=10.0)
        # Not asserted == 0: a real, unthrottled `_FakeCapture` spins fast
        # enough that the child can have already dropped several frames
        # against itself before this process's own first `get()` runs --
        # exactly the race `test_v4l2_worker_main_counts_real_drops_...`
        # below exists to measure on purpose. This test is about the
        # queue protocol shape, not the drop count.
        assert isinstance(dropped_total, int) and dropped_total >= 0
        assert isinstance(timestamp_ms, int)
        assert np.array_equal(frame, _solid_frame())
    finally:
        proc.terminate()
        proc.join(timeout=5.0)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5.0)


def test_v4l2_worker_main_counts_real_drops_when_the_consumer_falls_behind():
    """The discard counter itself, proven for real, not just plumbing:
    `frame_queue`'s `maxsize=1` mailbox drops a frame outright when the
    consumer does not keep up — right for a live camera, but it must not go
    silent. A real spawned
    process producing frames as fast as `_FakeCapture` allows, left
    undrained for a moment, must have counted every frame it overwrote
    before this test ever reads one."""
    ctx = mp.get_context("spawn")
    frame_q = ctx.Queue(maxsize=1)
    ready_q = ctx.Queue(maxsize=1)
    proc = ctx.Process(
        target=camera_sources._v4l2_worker_main,
        args=("/dev/video0", frame_q, ready_q),
        kwargs={"capture_factory": _fake_opening_capture_factory},
        daemon=True,
    )
    proc.start()
    try:
        outcome = ready_q.get(timeout=10.0)
        assert outcome == ("ok",)
        time.sleep(0.3)  # let the child race ahead, deliberately undrained
        timestamp_ms, frame, dropped_total = frame_q.get(timeout=10.0)
        assert dropped_total > 0
    finally:
        proc.terminate()
        proc.join(timeout=5.0)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5.0)


def test_v4l2_adapter_tracks_dropped_frames_and_rate_limits_the_log_line(capfd):
    """The adapter-facing half of the counter: `_dropped_frames` is always
    current (a caller/diagnostic can read it any time), but the log line
    fires once on the first drop and then not again within
    `_V4L2_DROP_LOG_INTERVAL_S` — never per frame, which a camera
    genuinely falling behind could otherwise flood the log with."""

    class _RisingDropWorker(_FakeWorker):
        def __init__(self):
            super().__init__()
            self._reads = [0, 5, 5, 12]  # two real increases, back to back
            self._i = 0

        def read(self, timeout_s):
            if self._i >= len(self._reads):
                raise queue.Empty()
            dropped = self._reads[self._i]
            self._i += 1
            return (1_700_000_000_000 + self._i, _solid_frame(), dropped)

    worker = _RisingDropWorker()
    rec = _Recorder()
    adapter = V4l2SourceAdapter(
        device="/dev/video0",
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        worker_factory=lambda d: worker,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_frames(len(worker._reads))
    finally:
        adapter.stop()

    assert adapter._dropped_frames == 12  # always current, even though...
    captured = capfd.readouterr()
    assert captured.err.count("has dropped") == 1  # ...the second jump (5->12) was rate-limited
    assert "5 frame" in captured.err  # the log line names the value at the *first* real drop


# --- MJPEG ---------------------------------------------------------------------


def _jpeg_bytes(frame):
    ok, buf = cv2.imencode(".jpg", frame)
    assert ok
    return buf.tobytes()


def _multipart(frames):
    boundary = b"--frame\r\nContent-Type: image/jpeg\r\n\r\n"
    body = b""
    for frame in frames:
        body += boundary + _jpeg_bytes(frame) + b"\r\n"
    return body


class _FakeMjpegResponse:
    def __init__(self, data: bytes):
        self._data = data
        self.closed = False

    def read(self, n):
        if not self._data:
            return b""
        chunk, self._data = self._data[:n], self._data[n:]
        return chunk

    def close(self):
        self.closed = True


def test_mjpeg_adapter_decodes_frames_from_a_multipart_stream():
    frame = _solid_frame(color=(1, 2, 3))
    response = _FakeMjpegResponse(_multipart([frame, frame]))
    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://192.0.2.1/video",
        credentials=None,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=lambda request: response,
    )
    adapter.start()
    try:
        rec.wait_for_frames(2)
    finally:
        adapter.stop()
    assert rec.frames[0][0].shape == frame.shape
    assert isinstance(rec.frames[0][1], int)


def test_mjpeg_adapter_sends_a_real_authorization_header_never_touching_the_url():
    captured_requests = []

    def opener(request):
        captured_requests.append(request)
        return _FakeMjpegResponse(_multipart([_solid_frame()]))

    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://192.0.2.1/video",
        credentials=Credentials(username="admin", password="hunter2"),
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=opener,
    )
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert captured_requests[0].full_url == "http://192.0.2.1/video"  # never rewritten
    token = base64.b64encode(b"admin:hunter2").decode("ascii")
    assert captured_requests[0].get_header("Authorization") == "Basic {}".format(token)


def test_mjpeg_adapter_strips_userinfo_from_the_request_url_and_still_authenticates():
    """`urllib.request` has no concept of `user:pass@host` userinfo —
    passed through raw, it either fails to resolve the host or (with no
    explicit port) makes `http.client` misread the userinfo as a bogus
    port and raise `InvalidURL`, which used to reach the generic handler
    and log the password (see the `_ThreadedSourceAdapter._run` fix and
    the caplog test below). Credentials reach here already resolved into
    `Credentials` (named ref or URL userinfo, either way —
    resolve_credentials) and go out as a real header, so the URL never
    needs to carry them — and now does not."""
    captured_requests = []

    def opener(request):
        captured_requests.append(request)
        return _FakeMjpegResponse(_multipart([_solid_frame()]))

    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://admin:hunter2@192.0.2.1/video",  # userinfo, no explicit port
        credentials=Credentials(username="admin", password="hunter2"),
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=opener,
    )
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert captured_requests[0].full_url == "http://192.0.2.1/video"  # userinfo stripped
    token = base64.b64encode(b"admin:hunter2").decode("ascii")
    assert captured_requests[0].get_header("Authorization") == "Basic {}".format(token)


def test_a_generic_error_in_the_adapter_loop_never_logs_its_raw_message(capfd):
    """The last-resort handler in `_ThreadedSourceAdapter._run` used to call
    `log.exception(...)`, which logs the caught exception's own `str()` and a
    traceback ending in it —
    defeating the same branch's own comment that a raw exception must
    never be allowed to leak a URL or credentials. Reproduced with the
    real shape that found it: MJPEG's opener raising
    `http.client.InvalidURL` (not `HTTPError`/`URLError`, so it reaches
    this branch) with a password embedded in the message, exactly what
    `nonnumeric port: 'campass@cam.lan'` looks like.

    `capfd`, not `caplog`: this module's logger reaches Python's real
    stderr in this environment (no handler this package configures
    attaches to the root logger the way `caplog` expects), the same
    reason `_suppress_native_stderr`'s own tests capture at the fd level
    rather than trusting `caplog` to see it. A `caplog`-only version of
    this test would pass whether or not the leak was fixed — checked by
    reverting just the code fix and rerunning with `caplog` still in
    place: it stayed green, which is exactly the vacuous-assertion shape
    this package has hit before."""
    import http.client

    def opener(request):
        raise http.client.InvalidURL("nonnumeric port: 'hunter2@cam.lan'")

    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://hunter2@cam.lan/video",
        credentials=Credentials(username="admin", password="hunter2"),
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=opener,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
        time.sleep(0.1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_error"
    assert "hunter2" not in rec.errors[0][1]
    captured = capfd.readouterr()
    assert "hunter2" not in captured.err
    assert "hunter2" not in captured.out


def test_mjpeg_adapter_omits_the_authorization_header_with_no_credentials():
    captured_requests = []

    def opener(request):
        captured_requests.append(request)
        return _FakeMjpegResponse(_multipart([_solid_frame()]))

    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://192.0.2.1/video",
        credentials=None,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=opener,
    )
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert captured_requests[0].get_header("Authorization") is None


def test_mjpeg_adapter_reports_source_auth_failed_on_401():
    import urllib.error

    def opener(request):
        raise urllib.error.HTTPError("http://x", 401, "Unauthorized", {}, None)

    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://192.0.2.1/video",
        credentials=Credentials(username="admin", password="wrong"),
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=opener,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_auth_failed"
    assert "wrong" not in rec.errors[0][1]


def test_mjpeg_adapter_reports_source_unreachable_on_connection_refused():
    import urllib.error

    def opener(request):
        raise urllib.error.URLError(ConnectionRefusedError())

    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url="http://192.0.2.1/video",
        credentials=None,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        opener=opener,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_unreachable"


# --- MJPEG: F — is the read actually bounded, and is stop() safe? -------------
#
# Every MJPEG test above drives the adapter through a fake `opener` that
# never touches a real socket, so none of them can answer F's own question:
# does `_close()`'s cross-thread `response.close()` matter, or is the read
# it exists to unblock already bounded some other way? These two go through
# a real TCP server instead — the only way to find out.


def _slow_drip_mjpeg_server(*, send_one_frame=False):
    """Accepts one real connection, sends valid MJPEG multipart headers
    (and, if `send_one_frame`, one complete frame after them), then sends
    nothing else, ever — the shape of a camera that answered and then
    stopped delivering mid-stream. Returns `(url, stop)`.

    When `send_one_frame`, the frame is padded past 4096 bytes
    (`_ThreadedSourceAdapter`'s own read size) — found while writing this
    test: `http.client.HTTPResponse.read(amt)` delegates to
    `io.BufferedReader.read(size)`, which tries to fill the *whole*
    requested size before returning rather than returning as soon as any
    data is available, unless the total already sent is at least `amt`
    bytes. An unpadded small frame (a handful of solid-color test-image
    bytes) therefore blocked on the very first read waiting for more data
    that was never coming — not what this test means by "genuinely
    blocked waiting for the *next* frame". Verified directly against
    `http.client` before writing this, not assumed."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    port = srv.getsockname()[1]
    srv.listen(1)
    srv.settimeout(5.0)
    stopped = threading.Event()

    def serve():
        try:
            conn, _ = srv.accept()
        except OSError:
            return  # srv.close() from stop() before a client ever connected
        try:
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: multipart/x-mixed-replace; boundary=frame\r\n"
                b"\r\n"
            )
            if send_one_frame:
                body = _multipart([_solid_frame()])
                body += b"\r\n" * (4096 - len(body) + 64)  # past the read(4096) floor
                conn.sendall(body)
            while not stopped.is_set():  # then genuinely nothing, ever
                time.sleep(0.05)
        except OSError:
            pass  # the client side closed on us — nothing left to send
        finally:
            try:
                conn.close()
            except OSError:
                pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    def stop():
        stopped.set()
        try:
            srv.close()
        except OSError:
            pass
        thread.join(timeout=2.0)

    return "http://127.0.0.1:{}/video".format(port), stop


def test_mjpeg_read_times_out_on_its_own_without_stop_ever_being_called():
    """F: `_default_mjpeg_opener` passes `timeout=_MJPEG_CONNECT_TIMEOUT_S`
    to `urllib.request.urlopen`, and per Python's own socket semantics that
    timeout applies to the whole socket (`socket.settimeout`), not only the
    connect call — verified directly against `http.client` before writing
    this test, not assumed. A `.read()` that never gets data therefore
    raises on its own, with `stop()` never called at all: the server sends
    valid headers and then nothing, forever, and the adapter must report a
    failure by itself within a bounded time."""
    url, stop_server = _slow_drip_mjpeg_server(send_one_frame=False)
    rec = _Recorder()
    adapter = MjpegSourceAdapter(
        url=url,
        credentials=None,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        backoff=_FAST_BACKOFF,
    )
    started_at = time.monotonic()
    adapter.start()
    try:
        # _MJPEG_CONNECT_TIMEOUT_S is 5.0s; generous headroom for the
        # connect + the read timeout to both elapse without flaking.
        rec.wait_for_errors(1, timeout_s=12.0)
    finally:
        adapter.stop()
        stop_server()
    elapsed = time.monotonic() - started_at
    # Bounded, and specifically by the socket timeout — not by stop() ever
    # having been called (it was not, until the finally block above, well
    # after the error already arrived).
    assert elapsed < 10.0
    assert rec.errors[0][0] in ("source_unavailable", "source_error")


def test_mjpeg_stop_while_blocked_in_read_does_not_crash_and_has_real_margin():
    """A second bound, and not the one this test set out to check.

    The read genuinely is bounded (test above) — but `_close()`'s
    cross-thread `response.close()` turns out **not to be what bounds it**:
    while a thread is blocked inside `response.read()`, calling
    `response.close()` from another thread does not unblock it either —
    `close()` itself blocks, for as long as the read does, until the read's
    *own* socket timeout fires naturally. So `_close()` here is not defense
    in depth on top of a bound; it is inert. `stop()`'s effective wait time
    is exactly the read's own timeout, not shortened by anything this class
    does.

    That would still be fine — no SIGSEGV, unlike RTSP — except
    `_MJPEG_CONNECT_TIMEOUT_S` (5.0s) was set to *exactly* `stop()`'s own
    join timeout (`thread.join(timeout=5.0)`, zero margin between two
    independently-chosen constants that happen to match, unlike RTSP's own
    `_RTSP_SOCKET_TIMEOUT_US` — 3s, deliberately kept under the 5s join
    timeout by that section's own docstring). Reproduced without the fix:
    `stop()` took 5.0007s here, past its own join deadline by a hair —
    harmless this one time, a live "did not stop within 5s" log the next.
    Fixed alongside this test: `_MJPEG_CONNECT_TIMEOUT_S` now leaves the
    same margin RTSP already established as the right shape.

    Still run several times — a crash that only sometimes reproduces is not
    ruled out by one pass, even though this one turned out to be about
    timing, not memory safety."""
    for _ in range(10):
        url, stop_server = _slow_drip_mjpeg_server(send_one_frame=True)
        rec = _Recorder()
        adapter = MjpegSourceAdapter(
            url=url,
            credentials=None,
            on_frame=rec.on_frame,
            on_error=rec.on_error,
            backoff=_FAST_BACKOFF,
        )
        adapter.start()
        try:
            rec.wait_for_frames(1, timeout_s=5.0)  # now genuinely blocked in the next read()
        finally:
            stop_thread_start = time.monotonic()
            adapter.stop()
            elapsed = time.monotonic() - stop_thread_start
            # Real margin under stop()'s own 5.0s join timeout — not merely
            # "did not hang forever". 4.5s leaves headroom for scheduling
            # jitter while still proving the margin is real, not a fluke of
            # this one run.
            assert elapsed < 4.5, "stop() took {:.3f}s — no margin under its own join timeout".format(
                elapsed
            )
            stop_server()


# --- RTSP: preflight classification, driven through the adapter ---------------


class _FakeRtspSocket:
    def __init__(self, response: bytes):
        self._response = response
        self.sent = b""

    def settimeout(self, timeout_s):
        pass

    def sendall(self, data):
        self.sent += data

    def recv(self, n):
        chunk, self._response = self._response[:n], self._response[n:]
        return chunk

    def close(self):
        pass


def _socket_factory(*responses):
    """One canned response (or a raised exception) per call, in order —
    matches how many DESCRIBE round trips a preflight makes (one, or two
    when a Basic challenge triggers an authenticated retry)."""
    remaining = list(responses)

    def factory(host, port, timeout_s):
        item = remaining.pop(0)
        if isinstance(item, Exception):
            raise item
        return _FakeRtspSocket(item)

    return factory


_RTSP_200 = b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nContent-Type: application/sdp\r\n\r\nv=0\r\n"
_RTSP_401_BASIC = b'RTSP/1.0 401 Unauthorized\r\nCSeq: 1\r\nWWW-Authenticate: Basic realm="cam"\r\n\r\n'
_RTSP_401_DIGEST = (
    b'RTSP/1.0 401 Unauthorized\r\nCSeq: 1\r\n'
    b'WWW-Authenticate: Digest realm="cam", nonce="abc123"\r\n\r\n'
)
_RTSP_404 = b"RTSP/1.0 404 Not Found\r\nCSeq: 1\r\n\r\n"


def _rtsp_adapter(*, credentials=None, socket_factory, capture_factory=None, backoff=None):
    rec = _Recorder()
    adapter = RtspSourceAdapter(
        url="rtsp://192.0.2.5:554/stream",
        transport="tcp",
        credentials=credentials,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        capture_factory=capture_factory or (lambda url, transport: _FakeCapture(frame=_solid_frame())),
        socket_factory=socket_factory,
        backoff=backoff or _FAST_BACKOFF,
    )
    return adapter, rec


def test_rtsp_adapter_proceeds_straight_through_when_no_auth_is_required():
    adapter, rec = _rtsp_adapter(socket_factory=_socket_factory(_RTSP_200))
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert rec.errors == []


def test_rtsp_adapter_authenticates_with_basic_and_succeeds():
    creds = Credentials(username="admin", password="hunter2")
    captured_urls = []

    def capture_factory(url, transport):
        captured_urls.append(url)
        return _FakeCapture(frame=_solid_frame())

    adapter, rec = _rtsp_adapter(
        credentials=creds,
        socket_factory=_socket_factory(_RTSP_401_BASIC, _RTSP_200),
        capture_factory=capture_factory,
    )
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert rec.errors == []
    # The credentials really did reach the connect call...
    assert "admin:hunter2@" in captured_urls[0]


def test_rtsp_adapter_reports_auth_failed_when_basic_credentials_are_wrong():
    creds = Credentials(username="admin", password="wrongpass")
    capture_calls = []

    def capture_factory(url, transport):
        capture_calls.append(url)
        return _FakeCapture(frame=_solid_frame())

    adapter, rec = _rtsp_adapter(
        credentials=creds,
        socket_factory=_socket_factory(_RTSP_401_BASIC, _RTSP_401_BASIC),
        capture_factory=capture_factory,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
        time.sleep(0.1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_auth_failed"
    # ...and never leaks into the one place a developer would actually see it.
    assert "wrongpass" not in rec.errors[0][1]
    # A camera that never authenticates is never handed to cv2 at all.
    assert capture_calls == []


def test_rtsp_adapter_reports_auth_failed_with_no_credentials_configured_and_skips_the_retry():
    call_count = [0]

    def socket_factory(host, port, timeout_s):
        call_count[0] += 1
        return _FakeRtspSocket(_RTSP_401_BASIC)

    # A slow backoff, not `_FAST_BACKOFF`: proves *one* attempt makes
    # exactly one DESCRIBE call, no same-attempt retry with nothing to
    # authenticate with. A fast backoff would let the outer reconnect
    # loop start a second attempt before `stop()` lands — a different,
    # flakier property to test.
    adapter, rec = _rtsp_adapter(
        credentials=None, socket_factory=socket_factory, backoff=Backoff(initial_s=5.0, max_s=5.0)
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_auth_failed"
    assert call_count[0] == 1


def test_rtsp_adapter_reports_source_unreachable_on_connection_refused():
    adapter, rec = _rtsp_adapter(socket_factory=_socket_factory(ConnectionRefusedError()))
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_unreachable"


def test_rtsp_adapter_reports_a_generic_error_for_an_unexpected_status():
    adapter, rec = _rtsp_adapter(socket_factory=_socket_factory(_RTSP_404))
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_error"


def test_rtsp_adapter_lets_ffmpeg_attempt_a_digest_challenge_and_succeeds_quietly():
    # Digest is not classified — the preflight hands
    # off to the real connect, which may still just work.
    adapter, rec = _rtsp_adapter(
        credentials=Credentials(username="admin", password="hunter2"),
        socket_factory=_socket_factory(_RTSP_401_DIGEST),
    )
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert rec.errors == []


def test_rtsp_adapter_reports_scheme_not_classified_when_ffmpeg_also_fails_after_a_digest_challenge():
    adapter, rec = _rtsp_adapter(
        credentials=Credentials(username="admin", password="hunter2"),
        socket_factory=_socket_factory(_RTSP_401_DIGEST),
        capture_factory=lambda url, transport: _FakeCapture(opened=False),
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
    finally:
        adapter.stop()
    assert rec.errors[0][0] == "source_auth_unclassified"
    assert "digest" in rec.errors[0][1].lower()


def test_rtsp_adapter_never_puts_the_password_in_a_reported_error_message():
    creds = Credentials(username="admin", password="s3cr3t-p4ss")
    adapter, rec = _rtsp_adapter(
        credentials=creds,
        socket_factory=_socket_factory(_RTSP_401_BASIC, _RTSP_401_BASIC),
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
        time.sleep(0.1)
    finally:
        adapter.stop()
    for _, message in rec.errors:
        assert "s3cr3t-p4ss" not in message


def test_rtsp_adapter_never_logs_the_password(capfd):
    """`caplog`, not `capfd`, was the original form of this test — and it
    was vacuous: this scenario's classified `_SourceError` never reaches any
    log call at all (only `_report_once` -> `on_error`, covered separately
    below), so
    `caplog.records` was always empty and the assertion loop never ran.
    Switched to `capfd` (this module's logger reaches real stderr in this
    environment, not a handler `caplog` sees — same reason
    `_suppress_native_stderr`'s own tests capture at the fd level) *and*
    to a scenario that actually logs something: an unclassified exception
    from `capture_factory` embedding the credentialed URL, the shape
    `_ThreadedSourceAdapter._run`'s generic handler exists for. Confirmed
    this version fails against the pre-fix `log.exception(...)` by
    reverting that one line and rerunning."""
    creds = Credentials(username="admin", password="s3cr3t-p4ss")

    def capture_factory(url, transport):
        raise RuntimeError("could not connect to rtsp://admin:s3cr3t-p4ss@192.0.2.5:554/stream")

    adapter, rec = _rtsp_adapter(
        credentials=creds,
        socket_factory=_socket_factory(_RTSP_200),  # preflight passes; the raise is in capture_factory
        capture_factory=capture_factory,
        backoff=_FAST_BACKOFF,
    )
    adapter.start()
    try:
        rec.wait_for_errors(1)
        time.sleep(0.1)
    finally:
        adapter.stop()
    assert "s3cr3t-p4ss" not in rec.errors[0][1]
    captured = capfd.readouterr()
    assert "s3cr3t-p4ss" not in captured.err
    assert "s3cr3t-p4ss" not in captured.out


def test_rtsp_adapter_passes_the_configured_transport_to_the_capture_factory():
    captured = []

    def capture_factory(url, transport):
        captured.append(transport)
        return _FakeCapture(frame=_solid_frame())

    rec = _Recorder()
    adapter = RtspSourceAdapter(
        url="rtsp://192.0.2.5/stream",
        transport="udp",
        credentials=None,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        capture_factory=capture_factory,
        socket_factory=_socket_factory(_RTSP_200),
    )
    adapter.start()
    try:
        rec.wait_for_frames(1)
    finally:
        adapter.stop()
    assert captured == ["udp"]


def test_a_capture_is_never_released_from_a_different_thread_than_the_one_reading_it():
    """Releasing an OpenCV/ffmpeg capture from a thread other
    than the one currently blocked inside a native call on it produced a
    real, twice-reproduced SIGSEGV in libavformat against an actual RTSP
    camera. Pinned at the shared `_CvCaptureAdapter` level — RTSP, not
    V4L2 (which moved off this shared class entirely: its
    capture now lives in a child process, killed from any thread with no
    such risk — see `_V4l2Worker`'s own docstring for why that is safe
    where this in-thread release is not). Proves the negative directly:
    while a `read()` is genuinely still in flight, `stop()` must not have
    released the capture from *any* thread — only once `read()` itself
    returns does the adapter's own thread do so, and it is provably a
    different thread than whichever one called `stop()`."""
    reading = threading.Event()
    let_read_return = threading.Event()
    release_calls = []

    class _BlockingCapture:
        def isOpened(self):
            return True

        def read(self):
            reading.set()
            let_read_return.wait(timeout=5.0)
            return True, _solid_frame()

        def release(self):
            release_calls.append(threading.current_thread())

    cap = _BlockingCapture()
    rec = _Recorder()
    adapter = RtspSourceAdapter(
        url="rtsp://192.0.2.5/stream",
        transport="tcp",
        credentials=None,
        on_frame=rec.on_frame,
        on_error=rec.on_error,
        capture_factory=lambda url, transport: cap,
        socket_factory=_socket_factory(_RTSP_200),
    )
    adapter.start()
    assert reading.wait(timeout=2.0)  # the adapter's own thread is now inside a blocking read()

    # stop() itself blocks (join) until the thread it is stopping exits, so
    # it runs on its own thread here — exactly the shape a real caller
    # (_destroy_camera's throwaway stop thread) has.
    stop_thread = threading.Thread(target=adapter.stop)
    stop_thread.start()
    time.sleep(0.2)  # a real chance for the old, buggy behaviour to fire
    assert release_calls == []  # not released by anyone while still in flight

    let_read_return.set()  # only now can the adapter's own thread notice _stop_event and exit
    stop_thread.join(timeout=5.0)
    assert not stop_thread.is_alive()
    assert len(release_calls) == 1
    assert release_calls[0] not in (threading.current_thread(), stop_thread)


# --- RTSP: the ffmpeg-options RWLock under real concurrency -


def _rtsp_capture_factory_with_timing(index, starts, finishes, bookkeeping_lock, delay_s):
    """Reproduces `_default_rtsp_capture_factory`'s real structure — the
    actual `_ffmpeg_rtsp_transport` context manager and the actual
    `_suppress_native_stderr`, not a stand-in for either — with a
    `time.sleep` in place of the real `cv2.VideoCapture(...)` call it
    wraps, timestamped so a test can check whether critical sections
    overlapped rather than only trusting a total wall-clock number."""

    def capture_factory(url, transport):
        with camera_sources._ffmpeg_rtsp_transport(transport), camera_sources._suppress_native_stderr():
            with bookkeeping_lock:
                starts[index] = time.monotonic()
            time.sleep(delay_s)
            with bookkeeping_lock:
                finishes[index] = time.monotonic()
        return _FakeCapture(frame=_solid_frame())

    return capture_factory


def test_same_transport_rtsp_opens_now_run_genuinely_concurrently():
    """The redesign: `OPENCV_FFMPEG_CAPTURE_ OPTIONS` is read exactly once per
    open (verified — see `_ValueGate`'s
    own docstring, an LD_PRELOAD probe on the real backend), so an open whose
    transport already matches what is currently set does not need to
    *change* the variable, only a guarantee nobody else changes it out
    from under it. Real fleets overwhelmingly share one transport, so this
    is the common case — and it must now run in *actual* parallel, not
    just look like it: an earlier starvation-test bug had a fake bypass the real
    lock entirely and 'measured' nothing). Eight real threads, the real
    RWLock, the real context manager -- checked directly by timestamp
    overlap, not inferred from a fast total."""
    n = 8
    delay_s = 0.3

    starts: Dict[int, float] = {}
    finishes: Dict[int, float] = {}
    bookkeeping_lock = threading.Lock()

    adapters = []
    recs = []
    for i in range(n):
        rec = _Recorder()
        recs.append(rec)
        adapters.append(
            RtspSourceAdapter(
                url="rtsp://192.0.2.{}/stream".format(i),
                transport="tcp",  # every one of the eight wants the same transport
                credentials=None,
                on_frame=rec.on_frame,
                on_error=rec.on_error,
                capture_factory=_rtsp_capture_factory_with_timing(i, starts, finishes, bookkeeping_lock, delay_s),
                socket_factory=_socket_factory(_RTSP_200),
            )
        )

    for adapter in adapters:
        adapter.start()
    try:
        for rec in recs:
            rec.wait_for_frames(1, timeout_s=n * delay_s + 10.0)
    finally:
        for adapter in adapters:
            adapter.stop()

    assert len(starts) == n and len(finishes) == n

    # The property this redesign exists to deliver, checked directly rather
    # than inferred from a total: at least one pair of critical sections
    # genuinely overlapped in wall-clock time. In a debug run of this
    # exact test all eight started within ~1ms of each
    # other and finished between 0.300s-0.409s -- true concurrent
    # execution, not a fast-looking illusion.
    #
    # **A total-time bound used to sit below this, and was removed because it
    # cannot separate a loaded parallel run from a serialized one.** Measured
    # parallel-under-load totals of 3.77s and 4.47s overlap the 3.94s-4.75s a
    # serialized run of this same n=8/delay=0.3s shape costs, so any bound
    # stable enough for a loaded runner no longer catches serialization --
    # it reddened on GitHub's runners while the property it guards held. The
    # overlap check is the precise, non-noisy signal: serialize the opens and
    # no pair overlaps, so this fails.
    intervals = sorted(((starts[i], finishes[i]) for i in range(n)), key=lambda pair: pair[0])
    any_overlap = any(
        finish_a > start_b for (start_a, finish_a), (start_b, _finish_b) in zip(intervals, intervals[1:])
    )
    assert any_overlap, "same-transport RTSP opens did not actually overlap -- still serialized"


def test_different_transport_rtsp_opens_still_exclude_each_other():
    """The correctness half of the same redesign: when the transport
    genuinely differs, nothing may run concurrently with the mutation that
    changes the shared env var, or a reader could observe a value meant
    for a sibling's connect attempt. Two adapters, deliberately different
    transports, real threads -- their critical sections must never
    overlap, the same check the original (now-removed) universal-
    serialization test used to make for every pair."""
    starts: Dict[int, float] = {}
    finishes: Dict[int, float] = {}
    bookkeeping_lock = threading.Lock()
    delay_s = 0.3

    tcp_rec = _Recorder()
    tcp_adapter = RtspSourceAdapter(
        url="rtsp://192.0.2.1/stream",
        transport="tcp",
        credentials=None,
        on_frame=tcp_rec.on_frame,
        on_error=tcp_rec.on_error,
        capture_factory=_rtsp_capture_factory_with_timing("tcp", starts, finishes, bookkeeping_lock, delay_s),
        socket_factory=_socket_factory(_RTSP_200),
    )
    udp_rec = _Recorder()
    udp_adapter = RtspSourceAdapter(
        url="rtsp://192.0.2.2/stream",
        transport="udp",
        credentials=None,
        on_frame=udp_rec.on_frame,
        on_error=udp_rec.on_error,
        capture_factory=_rtsp_capture_factory_with_timing("udp", starts, finishes, bookkeeping_lock, delay_s),
        socket_factory=_socket_factory(_RTSP_200),
    )

    try:
        tcp_adapter.start()
        udp_adapter.start()
        tcp_rec.wait_for_frames(1, timeout_s=10.0)
        udp_rec.wait_for_frames(1, timeout_s=10.0)
    finally:
        tcp_adapter.stop()
        udp_adapter.stop()

    assert set(starts) == {"tcp", "udp"}
    (first_start, first_finish), (second_start, _second_finish) = sorted(
        ((starts[k], finishes[k]) for k in starts), key=lambda pair: pair[0]
    )
    assert first_finish <= second_start, "mismatched-transport RTSP opens overlapped -- the env var race is back"


def test_a_mixed_transport_fleet_keeps_same_transport_parallel_and_cross_transport_excluded():
    """A third shape neither test above can produce: the
    first proves same-transport opens run in parallel, the second proves
    two *different* transports exclude each other -- both are two-adapter
    scenarios. A real fleet is neither: several `tcp` cameras and several
    `udp` cameras reconnecting around the same moment, which could in
    principle show a *third* pattern (repeated forced writer transitions
    as the two groups interleave, degrading toward the old linear
    behaviour) that a two-adapter test cannot expose either way. Three
    `tcp` and two `udp`, real threads, all started together."""
    starts: Dict[str, float] = {}
    finishes: Dict[str, float] = {}
    bookkeeping_lock = threading.Lock()
    delay_s = 0.3

    fleet = [("tcp", "a"), ("tcp", "b"), ("tcp", "c"), ("udp", "x"), ("udp", "y")]
    adapters = []
    recs = []
    for transport, key in fleet:
        rec = _Recorder()
        recs.append(rec)
        adapters.append(
            RtspSourceAdapter(
                url="rtsp://192.0.2.{}/stream".format(key),
                transport=transport,
                credentials=None,
                on_frame=rec.on_frame,
                on_error=rec.on_error,
                capture_factory=_rtsp_capture_factory_with_timing(key, starts, finishes, bookkeeping_lock, delay_s),
                socket_factory=_socket_factory(_RTSP_200),
            )
        )

    for adapter in adapters:
        adapter.start()
    try:
        for rec in recs:
            rec.wait_for_frames(1, timeout_s=len(fleet) * delay_s + 10.0)
    finally:
        for adapter in adapters:
            adapter.stop()

    assert set(starts) == {key for _t, key in fleet}

    by_transport = {"tcp": ["a", "b", "c"], "udp": ["x", "y"]}
    # Within one transport, at least one overlap -- the parallel case
    # still holds inside a mixed fleet, not only in an all-same fleet.
    for transport, keys in by_transport.items():
        intervals = sorted(((starts[k], finishes[k]) for k in keys), key=lambda pair: pair[0])
        assert any(
            finish_a > start_b for (start_a, finish_a), (start_b, _fb) in zip(intervals, intervals[1:])
        ), "{} cameras did not overlap with each other inside a mixed fleet".format(transport)

    # Across transports, no overlap at all -- correctness must survive
    # having more than one competing "other" group, not just one.
    for tcp_key in by_transport["tcp"]:
        for udp_key in by_transport["udp"]:
            a_start, a_finish = starts[tcp_key], finishes[tcp_key]
            b_start, b_finish = starts[udp_key], finishes[udp_key]
            overlap = a_start < b_finish and b_start < a_finish
            assert not overlap, "tcp={} overlapped udp={} in a mixed fleet".format(tcp_key, udp_key)

    # The old linear behaviour would show up as **no within-transport
    # overlap** -- five cameras running serially cannot overlap each other --
    # so the two checks above already catch a fleet degraded toward full
    # serialization. A total-time bound used to sit here and was removed for
    # the same reason as the one in the same-transport test: on a loaded
    # runner its parallel range overlaps the serialized range, so it
    # reddened without the property failing.


def test_a_healthy_rtsp_camera_can_wait_behind_several_broken_ones_at_the_full_timeout():
    """The regression boundary the fix is checked against. **Before the
    redesign, this same setup — seven RTSP cameras that each run a real ~3s
    open before failing, plus one healthy camera that would open immediately,
    all sharing one transport — measured 20.83s-20.84s across five real runs,
    essentially
    the theoretical `7 * 3s = 21s` ceiling every time.** That measurement
    is what refuted "no fairness mechanism needed" and started this
    redesign; it is preserved here as a comment, not as a live assertion,
    because asserting a number the fix is supposed to have made wrong
    would be testing the bug.

    The acceptance criterion: the healthy camera's wait must
    **not grow linearly** with the number of broken cameras ahead of it —
    checked directly by running the scenario twice, at n_broken=7 and
    n_broken=14, and asserting the second wait is not roughly double the
    first (the signature a still-linear/serialized wait would leave)."""
    delay_s = camera_sources._V4L2_READ_TIMEOUT_S  # ~3s, the same real-scale stand-in
    never_retry_backoff = Backoff(initial_s=60.0, max_s=60.0)

    def broken_capture_factory(url, transport):
        with camera_sources._ffmpeg_rtsp_transport(transport), camera_sources._suppress_native_stderr():
            time.sleep(delay_s)
        return _FakeCapture(opened=False)

    def healthy_capture_factory(url, transport):
        with camera_sources._ffmpeg_rtsp_transport(transport), camera_sources._suppress_native_stderr():
            result = _FakeCapture(frame=_solid_frame())
        return result

    def measure(n_broken):
        broken_adapters = []
        broken_recs = []
        for i in range(n_broken):
            rec = _Recorder()
            broken_recs.append(rec)
            broken_adapters.append(
                RtspSourceAdapter(
                    url="rtsp://192.0.2.{}/stream".format(i),
                    transport="tcp",
                    credentials=None,
                    on_frame=rec.on_frame,
                    on_error=rec.on_error,
                    capture_factory=broken_capture_factory,
                    socket_factory=_socket_factory(_RTSP_200),
                    backoff=never_retry_backoff,
                )
            )

        healthy_rec = _Recorder()
        healthy_adapter = RtspSourceAdapter(
            url="rtsp://192.0.2.100/stream",
            transport="tcp",
            credentials=None,
            on_frame=healthy_rec.on_frame,
            on_error=healthy_rec.on_error,
            capture_factory=healthy_capture_factory,
            socket_factory=_socket_factory(_RTSP_200),
        )

        try:
            for adapter in broken_adapters:
                adapter.start()
            time.sleep(0.2)  # a real chance for all of them to be contending before the healthy one joins
            start = time.monotonic()
            healthy_adapter.start()
            healthy_rec.wait_for_frames(1, timeout_s=n_broken * delay_s + 15.0)
            healthy_wait_s = time.monotonic() - start
        finally:
            healthy_adapter.stop()
            for adapter in broken_adapters:
                adapter.stop()

        for rec in broken_recs:
            assert rec.errors and rec.errors[0][0] == "source_unavailable"
        return healthy_wait_s

    wait_7 = measure(7)
    wait_14 = measure(14)

    # Both must be close to a single open's worth of time, not growing
    # with n_broken -- the direct rebuttal of the pre-fix 20.83s/~21s
    # measurement (which was for n_broken=7 alone; n_broken=14 would have
    # been ~42s under the old design).
    assert wait_7 <= delay_s * 3.0, "n_broken=7 healthy wait ({:.2f}s) looks linear again".format(wait_7)
    assert wait_14 <= delay_s * 3.0, "n_broken=14 healthy wait ({:.2f}s) looks linear again".format(wait_14)
    # The direct non-linearity check: doubling n_broken must not come
    # anywhere near doubling the wait -- generous margin (2.5x, not 2.0x)
    # for real scheduling noise between two separate real-thread runs.
    assert wait_14 <= max(wait_7 * 2.5, delay_s * 1.5), (
        "wait grew with n_broken ({:.2f}s at 7 -> {:.2f}s at 14) -- looks linear, not the fixed behaviour".format(
            wait_7, wait_14
        )
    )
