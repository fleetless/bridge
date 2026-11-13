# SPDX-License-Identifier: Apache-2.0
"""`RtspPassthroughSourceAdapter` against a real RTSP session.

The server is `rtsp_test_server.py`, ours and small; everything between its
socket and the adapter's `push()` is the PyAV and ffmpeg the distribution
ships. That is the subject: whether access units come out of the real
demuxer whole, in order and undecoded, and what the adapter makes of a
session that refuses, stalls, ends or carries something else than H.264.

No rclpy: the adapter lives in the rclpy-free half of the camera sources.
"""
import contextlib
import logging
import socket
import threading
import time

import pytest

import h264_streams as streams
from conftest import wait_until
from fleetless_bridge import camera_sources, passthrough
from fleetless_bridge.camera_sources import (
    Backoff,
    Credentials,
    RtspPassthroughSourceAdapter,
    RtspSourceAdapter,
)
from rtsp_test_server import RtspTestServer

PASSWORD = "not-a-real-secret"
WIDTH, HEIGHT = 320, 240


def _fast_backoff():
    return Backoff(initial_s=0.05, max_s=0.2)


@pytest.fixture(scope="module")
def units():
    return streams.encode_stream(width=WIDTH, height=HEIGHT, frames=30, gop=5)


class _Sink:
    """Stands where the camera's `PassthroughStream` stands and writes down
    what the adapter does with it."""

    def __init__(self, accept=True):
        self.lock = threading.Lock()
        self.resets = 0
        self.parameter_sets = []
        self.units = []
        self.accept = accept

    def reset(self):
        with self.lock:
            self.resets += 1

    def set_parameter_sets(self, data):
        with self.lock:
            self.parameter_sets.append(data)

    def push(self, data, timestamp_ms):
        with self.lock:
            self.units.append((data, timestamp_ms, time.monotonic()))
        return self.accept

    def count(self):
        with self.lock:
            return len(self.units)


class _Reports:
    def __init__(self):
        self.errors = []
        self.healthy = []

    def on_error(self, code, message):
        self.errors.append((code, message))

    def on_healthy(self, timestamp_ms):
        self.healthy.append(timestamp_ms)


@contextlib.contextmanager
def _running(server, sink, reports, *, credentials=None, userinfo="", **kwargs):
    adapter = RtspPassthroughSourceAdapter(
        url=server.url(userinfo),
        transport="tcp",
        credentials=credentials,
        stream=sink,
        on_healthy=reports.on_healthy,
        on_error=reports.on_error,
        backoff=_fast_backoff(),
        **kwargs,
    )
    adapter.start()
    try:
        yield adapter
    finally:
        adapter.stop()


# --- the stream arrives ------------------------------------------------------


def test_access_units_arrive_whole_in_order_and_undecoded(units):
    source = [streams.split(unit) for unit in units]
    sink, reports = _Sink(), _Reports()
    with RtspTestServer(units, fps=60.0, loop=True) as server, _running(server, sink, reports):
        wait_until(lambda: sink.count() >= 45, timeout=10.0)
    received = [streams.split(data) for data, _, _ in sink.units]

    # libavformat reads a little of the stream to describe it and does not
    # hand that part over, so the first unit received is somewhere in the
    # stream. From there on every unit is the source's next one, bit for bit.
    start = source.index(received[0])
    for offset, nals in enumerate(received):
        assert nals == source[(start + offset) % len(source)], "unit {} differs".format(offset)
    assert reports.errors == []


def test_units_arrive_as_the_camera_sends_them_not_in_a_burst(units):
    """Capture time is when the unit reached the bridge. A demuxer that
    buffered a second of stream and released it at once would stamp thirty
    units with one instant."""
    sink, reports = _Sink(), _Reports()
    with RtspTestServer(units, fps=30.0, loop=True) as server, _running(server, sink, reports):
        wait_until(lambda: sink.count() >= 20, timeout=10.0)
    stamps = [timestamp_ms for _, timestamp_ms, _ in sink.units[:20]]
    assert stamps == sorted(stamps)
    assert stamps[-1] - stamps[0] >= 400  # nineteen intervals of 33 ms, with slack for a loaded machine


def test_parameter_sets_from_the_session_description_reach_the_stream(units):
    """Many cameras send SPS and PPS only in the SDP. The adapter hands them
    to the stream before the first unit, and the stream puts them in front of
    each keyframe -- shown here on the real stream object, down to a decoded
    picture."""
    sps, pps = streams.parameter_sets(units[0])
    stream = passthrough.PassthroughStream(width=WIDTH, height=HEIGHT, slug="yard")
    reports = _Reports()
    with RtspTestServer(units, fps=60.0, loop=True, in_band_parameter_sets=False) as server:
        with _running(server, stream, reports):
            wait_until(lambda: stream.newest_keyframe() is not None, timeout=10.0)
            keyframe = stream.newest_keyframe()
    nals = streams.split(keyframe.data)
    assert nals[0] == sps and nals[1] == pps
    assert passthrough.decode_keyframe(keyframe.data).shape == (HEIGHT, WIDTH, 3)
    assert reports.errors == []


def test_health_is_reported_once_per_connection(units):
    stream = passthrough.PassthroughStream(width=WIDTH, height=HEIGHT, slug="yard")
    reports = _Reports()
    with RtspTestServer(units, fps=60.0, loop=True) as server, _running(server, stream, reports):
        wait_until(lambda: reports.healthy, timeout=10.0)
        time.sleep(0.3)  # more keyframes pass; none of them is news
    assert len(reports.healthy) == 1
    assert reports.errors == []


# --- construction ------------------------------------------------------------


@pytest.mark.parametrize("url", [
    "file:///etc/hostname",
    "http://cam.local/stream",
    "/dev/video0",
    "cam.local/stream",
])
def test_a_url_that_is_not_rtsp_is_refused_at_construction(url):
    """The same check, by the same function, as the decoding adapter: a
    configuration document must not turn the robot into a file reader."""
    with pytest.raises(ValueError):
        RtspPassthroughSourceAdapter(
            url=url, transport="tcp", credentials=None, stream=_Sink(),
            on_healthy=lambda timestamp_ms: None, on_error=lambda code, message: None,
        )


def test_both_adapters_refuse_the_same_urls():
    for url in ("rtsp://cam.local/s", "rtsps://cam.local/s", "RTSP://cam.local/s"):
        RtspSourceAdapter(url=url, transport="tcp", credentials=None,
                          on_frame=lambda *a: None, on_error=lambda *a: None)
        RtspPassthroughSourceAdapter(url=url, transport="tcp", credentials=None, stream=_Sink(),
                                     on_healthy=lambda *a: None, on_error=lambda *a: None)


# --- what goes wrong around the stream ---------------------------------------


def test_a_closed_port_is_source_unreachable():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    reports = _Reports()
    adapter = RtspPassthroughSourceAdapter(
        url="rtsp://127.0.0.1:{}/stream".format(port), transport="tcp", credentials=None,
        stream=_Sink(), on_healthy=reports.on_healthy, on_error=reports.on_error,
        backoff=_fast_backoff(),
    )
    adapter.start()
    try:
        wait_until(lambda: reports.errors, timeout=10.0)
        time.sleep(0.3)  # several more attempts
    finally:
        adapter.stop()
    assert [code for code, _ in reports.errors] == ["source_unreachable"]  # once, not per retry


def test_a_camera_that_wants_credentials_and_gets_none_is_source_auth_failed(units):
    sink, reports = _Sink(), _Reports()
    with RtspTestServer(units, auth="basic") as server, _running(server, sink, reports):
        wait_until(lambda: reports.errors, timeout=10.0)
    assert reports.errors[0][0] == "source_auth_failed"
    assert sink.count() == 0


def test_rejected_credentials_are_source_auth_failed(units):
    sink, reports = _Sink(), _Reports()
    wrong = Credentials(username="camuser", password="wrong-" + PASSWORD)
    with RtspTestServer(units, auth="basic") as server, _running(server, sink, reports, credentials=wrong):
        wait_until(lambda: reports.errors, timeout=10.0)
    assert reports.errors[0][0] == "source_auth_failed"
    assert sink.count() == 0


def test_accepted_credentials_play_the_stream(units):
    sink, reports = _Sink(), _Reports()
    right = Credentials(username="camuser", password=PASSWORD)
    with RtspTestServer(units, auth="basic", fps=60.0, loop=True) as server:
        with _running(server, sink, reports, credentials=right):
            wait_until(lambda: sink.count() >= 5, timeout=10.0)
        requests = list(server.requests)
    assert reports.errors == []
    # The password never travels in a request line.
    assert not any(PASSWORD in line for line in requests)


def test_a_challenge_the_preflight_does_not_classify_and_ffmpeg_fails_is_auth_unclassified(units):
    sink, reports = _Sink(), _Reports()
    credentials = Credentials(username="camuser", password=PASSWORD)
    with RtspTestServer(units, auth="digest") as server:
        with _running(server, sink, reports, credentials=credentials):
            wait_until(lambda: reports.errors, timeout=10.0)
    code, message = reports.errors[0]
    assert code == "source_auth_unclassified"
    assert "digest" in message
    assert PASSWORD not in message


def test_h265_is_refused_with_its_sentence():
    hevc = streams.encode_stream(width=WIDTH, height=HEIGHT, frames=10, gop=5, codec="libx265")
    sink, reports = _Sink(), _Reports()
    with RtspTestServer(hevc, codec="H265", loop=True) as server, _running(server, sink, reports):
        wait_until(lambda: reports.errors, timeout=10.0)
        time.sleep(0.3)
    assert reports.errors == [
        ("source_error", "the stream is H.265 and not H.264; passthrough forwards H.264 only")
    ]
    assert sink.count() == 0  # not one unit of it was handed on


def test_a_stream_that_ends_is_reported_and_the_adapter_connects_again(units):
    sink, reports = _Sink(), _Reports()
    with RtspTestServer(units, fps=120.0) as server, _running(server, sink, reports):
        wait_until(lambda: server.sessions >= 3, timeout=15.0)
        sessions = server.sessions
    assert sessions >= 3
    assert sink.resets >= 3  # every connection starts the stream over
    codes = [code for code, _ in reports.errors]
    assert codes and set(codes) == {"source_unavailable"}


def test_a_camera_that_falls_silent_is_source_unavailable(units):
    sink, reports = _Sink(), _Reports()

    def demux(url, transport):
        return camera_sources._default_rtsp_demux_factory(url, transport, timeout_s=0.5)

    # Forty-five units in: well past what libavformat reads to describe the
    # stream, so the silence falls into a read and not into the open.
    with RtspTestServer(units, fps=60.0, loop=True, stall_after=45) as server:
        with _running(server, sink, reports, demux_factory=demux):
            wait_until(lambda: reports.errors, timeout=10.0)
    code, message = reports.errors[0]
    assert code == "source_unavailable"
    assert "stopped delivering" in message
    assert sink.count() >= 5  # it did play before it fell silent


def test_stop_returns_in_time_while_the_camera_is_silent(units):
    """Nothing can interrupt a read inside libavformat from another thread
    safely, so the read's own timeout is what bounds `stop()`. It has to stay
    under the five seconds `stop()` waits for the thread."""
    sink, reports = _Sink(), _Reports()
    with RtspTestServer(units, fps=60.0, loop=True, stall_after=45) as server:
        adapter = RtspPassthroughSourceAdapter(
            url=server.url(), transport="tcp", credentials=None, stream=sink,
            on_healthy=reports.on_healthy, on_error=reports.on_error, backoff=_fast_backoff(),
        )
        adapter.start()
        wait_until(lambda: sink.count() >= 5, timeout=10.0)
        time.sleep(0.5)  # now it sits in a read that gets nothing
        thread = adapter._thread
        started = time.monotonic()
        adapter.stop()
        elapsed = time.monotonic() - started
    assert not thread.is_alive()
    assert elapsed < 4.5


# --- a stream that is refused ------------------------------------------------


def test_a_refused_stream_is_reported_once_and_stays_connected(units):
    """The camera sends 320x240 and is configured as 1280x720. The stream
    object refuses it; the adapter says so once and keeps the session, because
    a new one would deliver the same stream."""
    stream = passthrough.PassthroughStream(width=1280, height=720, slug="yard")
    reports = _Reports()
    with RtspTestServer(units, fps=60.0, loop=True) as server, _running(server, stream, reports):
        wait_until(lambda: reports.errors, timeout=10.0)
        time.sleep(0.5)  # thirty more units, each of them refused
        sessions = server.sessions
    assert len(reports.errors) == 1
    code, message = reports.errors[0]
    assert code == "source_error"
    assert "320x240" in message and "1280x720" in message
    assert sessions == 1
    assert reports.healthy == []
    assert stream.newest_keyframe() is None


def test_a_size_only_the_session_description_names_is_checked_too(units):
    """No unit of this stream carries an SPS; the size is in the SDP and
    nowhere else. The refusal naming it shows the adapter handed the sets
    over."""
    stream = passthrough.PassthroughStream(width=640, height=480, slug="yard")
    reports = _Reports()
    with RtspTestServer(units, fps=60.0, loop=True, in_band_parameter_sets=False) as server:
        with _running(server, stream, reports):
            wait_until(lambda: reports.errors, timeout=10.0)
    assert reports.errors[0][0] == "source_error"
    assert "320x240" in reports.errors[0][1] and "640x480" in reports.errors[0][1]


def test_b_frames_are_refused_through_the_real_demuxer():
    with_b = streams.encode_stream(width=WIDTH, height=HEIGHT, frames=24, gop=12, bframes=2)
    stream = passthrough.PassthroughStream(width=WIDTH, height=HEIGHT, slug="yard")
    reports = _Reports()
    with RtspTestServer(with_b, fps=60.0, loop=True) as server, _running(server, stream, reports):
        wait_until(lambda: reports.errors, timeout=10.0)
        time.sleep(0.5)
    assert [code for code, _ in reports.errors] == ["source_error"]
    assert "B-frames" in reports.errors[0][1]


# --- nothing leaks -----------------------------------------------------------


class _Everything(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.text = []

    def emit(self, record):
        try:
            self.text.append(record.getMessage())
        except Exception:  # noqa: BLE001 - a record that will not format is not the subject
            self.text.append(repr(record.msg))


@pytest.mark.parametrize("server_kwargs, expected", [
    ({"auth": "digest"}, "source_auth_unclassified"),  # ffmpeg's own attempt fails with the URL in hand
    ({"auth": "basic", "loop": True, "stall_after": 45}, "source_unavailable"),  # connected with the credentials, then silence
    ({"auth": "basic", "codec": "H265"}, "source_error"),
])
def test_no_log_line_error_message_or_stderr_byte_carries_the_password(
    units, capfd, server_kwargs, expected
):
    """The credentials are in the URL ffmpeg opens, and PyAV's exceptions
    quote that URL. Checked on every channel there is: what the adapter
    reports, what Python logs at any level under any logger (PyAV forwards
    libav's own lines there on two of the three versions), and the process's
    real stderr."""
    handler = _Everything()
    root = logging.getLogger()
    watched = [root, logging.getLogger("libav"), logging.getLogger("fleetless_bridge.camera_sources")]
    levels = [logger.level for logger in watched]
    for logger in watched:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

    stream_units = units
    if server_kwargs.get("codec") == "H265":
        stream_units = streams.encode_stream(width=WIDTH, height=HEIGHT, frames=10, gop=5, codec="libx265")
    capfd.readouterr()  # what the encoder said is not the subject
    sink, reports = _Sink(), _Reports()

    def demux(url, transport):
        return camera_sources._default_rtsp_demux_factory(url, transport, timeout_s=0.5)

    try:
        with RtspTestServer(stream_units, fps=60.0, **server_kwargs) as server:
            with _running(
                server, sink, reports,
                # Both places a password can be written: the explicit block
                # and the URL's own userinfo.
                credentials=Credentials(username="camuser", password=PASSWORD),
                userinfo="camuser:" + PASSWORD,
                demux_factory=demux,
            ):
                wait_until(lambda: reports.errors, timeout=10.0)
                time.sleep(0.3)
    finally:
        for logger, level in zip(watched, levels):
            logger.removeHandler(handler)
            logger.setLevel(level)

    assert reports.errors[0][0] == expected
    out, err = capfd.readouterr()
    for channel, text in (
        ("an error message", " ".join(message for _, message in reports.errors)),
        ("a log line", "\n".join(handler.text)),
        ("stdout", out),
        ("stderr", err),
    ):
        assert PASSWORD not in text, "the password reached {}".format(channel)
        assert "camuser:" not in text, "the userinfo reached {}".format(channel)
