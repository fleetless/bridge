#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Drive `fleetless_bridge.live.LivePublisher` against a LiveKit server, so a
browser can be asked whether it decodes moving video from it.

A development fixture, never installed on a robot: it mints its own access
token, which the cloud does on a real robot, and it paints a synthetic moving
pattern, which a camera does. Everything between those two ends — the peer
connection, the track, the signalling, the frame feed — is the shipped code,
because a harness that reimplements the thing it is proving proves the
harness.

    python3 publish.py --room ROOM --seconds 60 --codec vp8

`--still` freezes the pattern. It exists as a break test for the viewer: the
viewer asserts two independent things (a frame arrived, and it changed), and
"no publisher at all" only breaks the first.

The token is minted with `hmac`/`hashlib` rather than a JWT library so that
nothing in the proof can be mistaken for a dependency of the package. The
publisher itself imports nothing this harness added.
"""
import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from aiortc import RTCPeerConnection, RTCRtpSender  # noqa: E402

from fleetless_bridge import live  # noqa: E402
from fleetless_bridge.camera import LatestFrameHolder  # noqa: E402


def log(*args):
    print(time.strftime("%H:%M:%S"), "HARNESS", *args, flush=True)


def _b64(raw):
    if not isinstance(raw, bytes):
        raw = json.dumps(raw, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def make_token(api_key, api_secret, room, identity):
    now = int(time.time())
    head = _b64({"alg": "HS256", "typ": "JWT"})
    body = _b64({
        "iss": api_key, "sub": identity, "name": identity,
        "nbf": now - 10, "exp": now + 3600,
        "video": {"room": room, "roomJoin": True, "canPublish": True,
                  "canSubscribe": False, "canPublishData": False},
    })
    signing_input = "{}.{}".format(head, body).encode()
    signature = hmac.new(api_secret.encode(), signing_input, hashlib.sha256).digest()
    return "{}.{}.{}".format(head, body, _b64(signature))


def pattern(width, height, n):
    """A frame with real spatial variance and something that moves in it.

    Flat colour compresses to nothing and hides encoder problems, and a
    picture that only changes in one corner can be lost to a crop, so the
    pattern carries a static ramp, a bar that sweeps, a block that flips every
    frame and a band that slides — the viewer's comparison then means the same
    thing wherever it samples.
    """
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    frame[:, :, 0] = np.linspace(0, 255, width, dtype=np.uint8)[None, :]
    frame[:, :, 1] = np.linspace(0, 255, height, dtype=np.uint8)[:, None]
    frame[:, :, 2] = 128
    x = (n * 13) % max(1, width - 40)
    frame[:, x:x + 40, :] = 255
    frame[20:120, 20:120, :] = 255 if n % 2 == 0 else 0
    y = (n * 7) % max(1, height - 30)
    frame[y:y + 30, :, 0] = 255 - (n % 256)
    # The holder and the publisher work in BGR; this is built in RGB order
    # above, so the channel swap the publisher performs is visible in the
    # result rather than cancelled out by building it backwards here.
    return frame[:, :, ::-1].copy()


def codec_pinning_factory(codec):
    """A `connection_factory` that offers exactly one video codec.

    aiortc offers VP8 and both H264 profiles and the server picks, which is
    the right behaviour for a robot and the wrong one for a measurement that
    has a per-codec column. `setCodecPreferences` is the framework's own way
    of saying it — the alternative, rewriting the `m=video` line by hand, is a
    second SDP implementation in a file whose job is to trust the first.
    """
    wanted = "video/" + {"vp8": "VP8", "h264": "H264"}[codec] if codec != "all" else None

    def factory(ice_servers):
        pc = live._default_connection_factory(ice_servers)
        if wanted is None:
            return pc
        create_offer = pc.createOffer

        async def pinned_create_offer():
            capabilities = RTCRtpSender.getCapabilities("video").codecs
            keep = [c for c in capabilities
                    if c.mimeType.lower() in (wanted.lower(), "video/rtx")]
            for transceiver in pc.getTransceivers():
                if transceiver.kind == "video":
                    transceiver.setCodecPreferences(keep)
            return await create_offer()

        pc.createOffer = pinned_create_offer
        return pc

    return factory


async def run(args):
    holder = LatestFrameHolder()
    publisher = live.LivePublisher(
        holder=holder,
        width=args.width,
        height=args.height,
        fps=args.fps,
        bitrate_kbps=args.bitrate_kbps,
        connection_factory=codec_pinning_factory(args.codec),
        on_lost=lambda reason: log("LOST", reason),
    )
    token = make_token(args.api_key, args.api_secret, args.room, args.identity)
    log("publishing into", args.url, "room", args.room, "codec", args.codec,
        "still", args.still)

    try:
        await publisher.start(args.url, args.room, token)
    except live.LiveStartError as exc:
        log("FAILED to start:", exc)
        return 2
    log("started; track sid =", publisher._track_sid)

    # A running peer connection is not a connected one. Report both, and let
    # the exit code say which: a viewer that sees nothing next to a publisher
    # that never reached `connected` is a different finding from one that sees
    # nothing next to a publisher that did.
    connected_at = None
    deadline = time.monotonic() + args.seconds
    n = 0
    interval = 1.0 / args.fps
    while time.monotonic() < deadline:
        holder.set(pattern(args.width, args.height, 0 if args.still else n),
                   timestamp_ms=int(time.time() * 1000))
        n += 1
        pc = publisher._pc
        if pc is not None:
            if connected_at is None and pc.connectionState == "connected":
                connected_at = time.monotonic()
                log("ICE/DTLS CONNECTED")
            if pc.connectionState in ("failed", "closed"):
                log("FAILED: peer connection state =", pc.connectionState)
                break
        else:
            log("FAILED: the session was torn down")
            break
        if n % (args.fps * 5) == 0:
            log("t+%ds state=%s frames handed to the encoder=%s"
                % (n // args.fps, pc.connectionState,
                   getattr(publisher._track, "frames_sent", "?")))
        await asyncio.sleep(interval)

    track = publisher._track
    log("frames handed to the encoder:",
        getattr(track, "frames_sent", "unknown"), "frames painted:", n)
    await publisher.stop()
    if connected_at is None:
        log("FAILED: never reached connected")
        return 3
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("LK_URL", "ws://localhost:7880"))
    parser.add_argument("--api-key", default=os.environ.get("LK_KEY", "devkey"))
    parser.add_argument("--api-secret", default=os.environ.get("LK_SECRET", "secret"))
    parser.add_argument("--room", default="fleetless-live-proof")
    parser.add_argument("--identity", default="fleetless-bridge-proof")
    parser.add_argument("--seconds", type=int, default=45)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--fps", type=int, default=15)
    parser.add_argument("--bitrate-kbps", type=int, default=800)
    parser.add_argument("--codec", default="vp8", choices=["vp8", "h264", "all"])
    parser.add_argument("--still", action="store_true",
                        help="freeze the pattern (a break test for the viewer's "
                             "movement assertion)")
    parser.add_argument("--quiet", action="store_true",
                        help="drop the package's own log output")
    args = parser.parse_args()
    # Everything the bridge logs, at every level. A proof that hides the
    # publisher's own account of what it did can only report that the picture
    # was missing, never why.
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.DEBUG,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    code = asyncio.run(run(args))
    log("EXIT", code)
    sys.exit(code)


if __name__ == "__main__":
    main()
