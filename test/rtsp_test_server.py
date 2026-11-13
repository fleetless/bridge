# SPDX-License-Identifier: Apache-2.0
"""A small RTSP server for the passthrough adapter's tests.

The test images carry no RTSP server, and the adapter's subject is what the
real PyAV and ffmpeg RTSP demuxer hands over -- a fake container at the PyAV
seam would test the fake. So this serves access units the test encoded, over
RTSP with RTP interleaved in the TCP connection (RFC 2326 section 10.12,
RFC 6184 for the H.264 payload), and nothing more than a client needs to
play: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, and a 200 for a keep-alive.

What it can be told to do, because the adapter's tests need each:

- ask for Basic or Digest credentials;
- serve the parameter sets only in the session description (`sprop`), the way
  many cameras do;
- stall: keep the connection open and send nothing more;
- end: close the connection after the last unit;
- announce H.265.

One client plays at a time per connection; every connection gets its own
thread and the stream from its beginning. Bound to 127.0.0.1 on a port the
kernel picks.
"""
import base64
import socket
import threading
import time
from typing import List, Optional

RTP_PAYLOAD_TYPE = 96
MAX_RTP_PAYLOAD = 1200


def _split(unit: bytes) -> List[bytes]:
    nals = []
    position = unit.find(b"\x00\x00\x01")
    while position != -1:
        start = position + 3
        position = unit.find(b"\x00\x00\x01", start)
        nal = unit[start:len(unit) if position == -1 else position].rstrip(b"\x00")
        if nal:
            nals.append(nal)
    return nals


class RtspTestServer:
    def __init__(
        self,
        units: List[bytes],
        *,
        fps: float = 60.0,
        codec: str = "H264",
        auth: Optional[str] = None,  # None | "basic" | "digest"
        username: str = "camuser",
        password: str = "not-a-real-secret",
        in_band_parameter_sets: bool = True,
        stall_after: Optional[int] = None,
        loop: bool = False,
        hold_open_at_end: bool = False,
    ) -> None:
        self._units = [_split(unit) for unit in units]
        self._fps = fps
        self._codec = codec
        self._auth = auth
        self._username = username
        self._password = password
        self._in_band = in_band_parameter_sets
        self._stall_after = stall_after
        self._loop = loop
        self._hold_open_at_end = hold_open_at_end
        self._stop = threading.Event()
        self._listener: Optional[socket.socket] = None
        self._threads: List[threading.Thread] = []
        self._clients: List[socket.socket] = []
        self._lock = threading.Lock()
        #: How many connections reached PLAY. A reconnect shows here.
        self.sessions = 0
        #: Every request line received, e.g. `DESCRIBE rtsp://... RTSP/1.0`.
        self.requests: List[str] = []
        self.port = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> "RtspTestServer":
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", self.port))
        self._listener.listen(8)
        self._listener.settimeout(0.1)
        self.port = self._listener.getsockname()[1]
        thread = threading.Thread(target=self._accept, daemon=True)
        thread.start()
        self._threads.append(thread)
        return self

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            clients, self._clients = self._clients, []
        for client in clients:
            try:
                client.close()
            except OSError:
                pass
        if self._listener is not None:
            self._listener.close()
            self._listener = None
        for thread in self._threads:
            thread.join(timeout=5.0)
        self._threads = []

    def __enter__(self) -> "RtspTestServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def url(self, userinfo: str = "") -> str:
        return "rtsp://{}127.0.0.1:{}/stream".format(userinfo + "@" if userinfo else "", self.port)

    # -- the session description -------------------------------------------

    def _parameter_sets(self):
        sps = pps = vps = None
        for nals in self._units:
            for nal in nals:
                if self._codec == "H264":
                    kind = nal[0] & 0x1F
                    if kind == 7 and sps is None:
                        sps = nal
                    elif kind == 8 and pps is None:
                        pps = nal
                else:
                    kind = (nal[0] >> 1) & 0x3F
                    if kind == 32 and vps is None:
                        vps = nal
                    elif kind == 33 and sps is None:
                        sps = nal
                    elif kind == 34 and pps is None:
                        pps = nal
        return vps, sps, pps

    def _sdp(self) -> str:
        vps, sps, pps = self._parameter_sets()
        b64 = lambda nal: base64.b64encode(nal).decode("ascii")  # noqa: E731
        if self._codec == "H264":
            fmtp = "packetization-mode=1;profile-level-id={};sprop-parameter-sets={},{}".format(
                sps[1:4].hex(), b64(sps), b64(pps)
            )
        else:
            fmtp = "sprop-vps={};sprop-sps={};sprop-pps={}".format(b64(vps), b64(sps), b64(pps))
        return "\r\n".join([
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            "s=fleetless test stream",
            "c=IN IP4 0.0.0.0",
            "t=0 0",
            "m=video 0 RTP/AVP {}".format(RTP_PAYLOAD_TYPE),
            "a=rtpmap:{} {}/90000".format(RTP_PAYLOAD_TYPE, self._codec),
            "a=fmtp:{} {}".format(RTP_PAYLOAD_TYPE, fmtp),
            "a=control:trackID=0",
            "",
        ])

    # -- connections -------------------------------------------------------

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                self._clients.append(client)
            thread = threading.Thread(target=self._serve, args=(client,), daemon=True)
            thread.start()
            self._threads.append(thread)

    def _authorized(self, headers) -> bool:
        if self._auth is None:
            return True
        given = headers.get("authorization", "")
        if self._auth == "basic":
            expected = base64.b64encode(
                "{}:{}".format(self._username, self._password).encode()
            ).decode()
            return given == "Basic " + expected
        # Digest: this server never accepts. The tests that ask for it want
        # the client's own attempt to fail, after the preflight has declined
        # to classify the challenge.
        return False

    def _challenge(self) -> str:
        if self._auth == "basic":
            return 'WWW-Authenticate: Basic realm="test"'
        return 'WWW-Authenticate: Digest realm="test", nonce="0123456789abcdef"'

    def _serve(self, client: socket.socket) -> None:
        write_lock = threading.Lock()
        sender: Optional[threading.Thread] = None
        done = threading.Event()

        def reply(cseq: str, status: str, headers=(), body: str = "") -> None:
            lines = ["RTSP/1.0 " + status, "CSeq: " + cseq]
            lines.extend(headers)
            if body:
                lines.append("Content-Length: {}".format(len(body)))
            message = ("\r\n".join(lines) + "\r\n\r\n" + body).encode("ascii")
            with write_lock:
                client.sendall(message)

        buffer = b""
        try:
            client.settimeout(0.1)
            while not self._stop.is_set() and not done.is_set():
                try:
                    chunk = client.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    return
                buffer += chunk
                while buffer:
                    if buffer[:1] == b"$":  # interleaved RTCP from the client
                        if len(buffer) < 4:
                            break
                        size = int.from_bytes(buffer[2:4], "big")
                        if len(buffer) < 4 + size:
                            break
                        buffer = buffer[4 + size:]
                        continue
                    end = buffer.find(b"\r\n\r\n")
                    if end == -1:
                        break
                    head, buffer = buffer[:end].decode("iso-8859-1"), buffer[end + 4:]
                    request_line, *header_lines = head.split("\r\n")
                    headers = {}
                    for line in header_lines:
                        name, _, value = line.partition(":")
                        headers[name.strip().lower()] = value.strip()
                    self.requests.append(request_line)
                    method = request_line.split(" ", 1)[0]
                    cseq = headers.get("cseq", "0")

                    if method == "OPTIONS":
                        reply(cseq, "200 OK", ["Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, GET_PARAMETER"])
                    elif method == "DESCRIBE":
                        if not self._authorized(headers):
                            reply(cseq, "401 Unauthorized", [self._challenge()])
                            continue
                        reply(
                            cseq, "200 OK",
                            ["Content-Type: application/sdp", "Content-Base: {}/".format(self.url())],
                            self._sdp(),
                        )
                    elif method == "SETUP":
                        if not self._authorized(headers):
                            reply(cseq, "401 Unauthorized", [self._challenge()])
                            continue
                        reply(cseq, "200 OK", [
                            "Transport: RTP/AVP/TCP;unicast;interleaved=0-1",
                            "Session: 12345678;timeout=60",
                        ])
                    elif method == "PLAY":
                        reply(cseq, "200 OK", ["Session: 12345678", "Range: npt=0.000-"])
                        self.sessions += 1
                        sender = threading.Thread(
                            target=self._send_stream, args=(client, write_lock, done), daemon=True
                        )
                        sender.start()
                    elif method == "TEARDOWN":
                        reply(cseq, "200 OK", ["Session: 12345678"])
                        return
                    else:  # GET_PARAMETER and anything else a client keeps alive with
                        reply(cseq, "200 OK", ["Session: 12345678"])
        except OSError:
            return
        finally:
            done.set()
            if sender is not None:
                sender.join(timeout=5.0)
            try:
                client.close()
            except OSError:
                pass

    # -- RTP ---------------------------------------------------------------

    def _send_stream(self, client: socket.socket, write_lock, done: threading.Event) -> None:
        sequence = 0
        interval = 1.0 / self._fps
        ticks = int(90000 / self._fps)
        sent = 0

        def send(payload: bytes, timestamp: int, marker: bool) -> None:
            nonlocal sequence
            header = bytes([
                0x80, (0x80 if marker else 0) | RTP_PAYLOAD_TYPE,
                (sequence >> 8) & 0xFF, sequence & 0xFF,
            ]) + (timestamp & 0xFFFFFFFF).to_bytes(4, "big") + (0x1234ABCD).to_bytes(4, "big")
            sequence = (sequence + 1) & 0xFFFF
            packet = header + payload
            with write_lock:
                client.sendall(b"$\x00" + len(packet).to_bytes(2, "big") + packet)

        try:
            while not self._stop.is_set() and not done.is_set():
                for nals in self._units:
                    if self._stop.is_set() or done.is_set():
                        return
                    if self._stall_after is not None and sent >= self._stall_after:
                        # Connected and silent, until the test ends.
                        while not self._stop.is_set() and not done.is_set():
                            time.sleep(0.05)
                        return
                    timestamp = sent * ticks
                    if not self._in_band and self._codec == "H264":
                        nals = [nal for nal in nals if nal[0] & 0x1F not in (7, 8)]
                    for index, nal in enumerate(nals):
                        last = index == len(nals) - 1
                        for payload, final in self._payloads(nal):
                            send(payload, timestamp, marker=last and final)
                    sent += 1
                    time.sleep(interval)
                if not self._loop:
                    break
            if self._hold_open_at_end:
                while not self._stop.is_set() and not done.is_set():
                    time.sleep(0.05)
                return
        except OSError:
            return
        finally:
            if not self._hold_open_at_end:
                done.set()

    def _payloads(self, nal: bytes):
        """One NAL unit as RTP payloads: whole when it fits, fragmented
        otherwise (FU-A for H.264, FU for H.265)."""
        if len(nal) <= MAX_RTP_PAYLOAD:
            yield nal, True
            return
        if self._codec == "H264":
            indicator = bytes([(nal[0] & 0xE0) | 28])
            kind, body, header_size = nal[0] & 0x1F, nal[1:], 1
        else:
            kind = (nal[0] >> 1) & 0x3F
            indicator = bytes([(nal[0] & 0x81) | (49 << 1), nal[1]])
            body, header_size = nal[2:], 2
        del header_size
        chunk = MAX_RTP_PAYLOAD - len(indicator) - 1
        for offset in range(0, len(body), chunk):
            piece = body[offset:offset + chunk]
            start = 0x80 if offset == 0 else 0
            end = 0x40 if offset + chunk >= len(body) else 0
            yield indicator + bytes([start | end | kind]) + piece, bool(end)
