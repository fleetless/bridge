#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""3c.4's TURN/relay proof: with host and server-reflexive candidates hidden
from the offer this publisher sends, does the LiveKit server actually reach
it through a TURN relay -- not merely "does it connect"?

    python3 relay_probe.py --room ROOM --coturn-host 127.0.0.1 \
        --coturn-port 3478 --coturn-user U --coturn-pass P

A development fixture, run from `run-relay-proof.sh` against a `coturn/coturn`
container that script creates and removes. `LivePublisher` itself is
unmodified; this harness only replaces `connection_factory` (the seam
`live.py`'s own docstring calls out for exactly this) and, for the one thing
`connection_factory` cannot reach on its own -- which of THIS side's own
gathered candidates get offered to the far end -- patches
`RTCIceGatherer.getLocalCandidates` for the lifetime of this process.

**Why filtering the offer, and not something more surgical, forces the
path.** `live.py`'s own `_default_connection_factory` already notes aiortc
honours only the *first* STUN and the *first* TURN entry in
`RTCConfiguration.iceServers`; that alone does not force relay, because
aioice still gathers this host's own local (and, if a STUN server is
reachable, reflexive) addresses and would offer all of them alongside the
relay one. ICE candidate pairs are formed independently by each side from
candidates it has learned -- ours locally, LiveKit's from our SDP and vice
versa -- so a LiveKit that never learns about our host/reflexive addresses at
all has no pair to try for them, and can only ever reach us through the one
address it *was* told about: the relay allocation. That is what
`getLocalCandidates` filtering buys, at the one point (`RTCPeerConnection.
setLocalDescription`, per aiortc's own source) it is read to build the SDP
this publisher sends.

**Reading the selected pair.** aiortc 1.3.0/1.6.0/1.14.0 expose no public
`getStats()` entry for ICE candidate pairs (`aiortc.stats` carries only RTP
stream and transport stats) -- so this reads aioice's own bookkeeping instead,
`RTCIceTransport._connection` (a public attribute despite the leading
underscore -- assigned directly off `gatherer._connection` in aiortc's own
`__init__`) `._nominated`, the dict RFC 8445's nomination procedure fills in
per ICE component. `local_candidate.type` on the nominated pair is what
answers the question this proof exists to ask.
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

from aiortc import RTCConfiguration, RTCIceGatherer, RTCIceServer, RTCPeerConnection  # noqa: E402

from fleetless_bridge import live  # noqa: E402
from fleetless_bridge.camera import LatestFrameHolder  # noqa: E402


def log(*args):
    print(time.strftime("%H:%M:%S"), "RELAY-PROBE", *args, flush=True)


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


def relay_only_connection_factory(coturn_host, coturn_port, username, password):
    """A `connection_factory` (the seam `LivePublisher` already takes) that
    ignores `join.ice_servers` and points aiortc at our own coturn instead,
    then makes sure this side both OFFERS and actually TRIES only the relay
    candidate it allocates there.

    **Two filters, not one, and the second is the one that matters.**
    `RTCIceGatherer.getLocalCandidates` is what builds the SDP this side
    sends (aiortc's `RTCPeerConnection.setLocalDescription` calls it, per its
    own source), so patching only that hides our host/reflexive addresses
    from LiveKit -- but does not stop US from trying them: aioice's
    `Connection.add_remote_candidate` pairs every REMOTE candidate against
    every protocol in `self._protocols`, which is *every locally gathered
    socket*, independent of what was ever advertised: an SDP-only filter
    still connects on a `host` pair, sub-millisecond, because as the
    ICE-controlling side this publisher checks every local candidate it has,
    not only the ones it told the other side about. Pruning `_protocols` and
    `_local_candidates` down to the relay entry right after gathering closes
    that gap -- there is nothing left to check but the one the coturn
    container answers for.

    Both patches are process-global and left in place: this script builds
    exactly one peer connection and exits, so there is nothing else in the
    process for a global patch to affect.
    """
    real_get_local_candidates = RTCIceGatherer.getLocalCandidates
    real_gather = RTCIceGatherer.gather

    def relay_only(self):
        candidates = real_get_local_candidates(self)
        return [c for c in candidates if c.type == "relay"]

    async def gather_relay_only(self):
        await real_gather(self)
        connection = self._connection
        connection._local_candidates[:] = [
            c for c in connection._local_candidates if c.type == "relay"
        ]
        connection._protocols[:] = [
            p for p in connection._protocols
            if getattr(p.local_candidate, "type", None) == "relay"
        ]
        log("pruned to {} relay-only local candidate(s)".format(
            len(connection._local_candidates)))

    RTCIceGatherer.getLocalCandidates = relay_only
    RTCIceGatherer.gather = gather_relay_only

    def factory(ice_servers):
        log("ignoring join's {} ICE server(s); using our own coturn at {}:{}".format(
            len(ice_servers), coturn_host, coturn_port))
        turn = RTCIceServer(
            urls=["turn:{}:{}".format(coturn_host, coturn_port)],
            username=username,
            credential=password,
        )
        return RTCPeerConnection(configuration=RTCConfiguration(iceServers=[turn]))

    return factory


def nominated_local_candidate_type(pc):
    """The type (`"host"`/`"srflx"`/`"relay"`) of THIS side's local candidate
    in the nominated (selected) pair, or `None` if nothing is nominated yet --
    see the module docstring for why this reads aioice's own state rather
    than a public stats call."""
    for transceiver in pc.getTransceivers():
        sender = getattr(transceiver, "sender", None)
        transport = getattr(sender, "transport", None)  # RTCDtlsTransport
        ice_transport = getattr(transport, "transport", None)  # RTCIceTransport
        connection = getattr(ice_transport, "_connection", None)  # aioice Connection
        if connection is None:
            continue
        nominated = getattr(connection, "_nominated", None) or {}
        for pair in nominated.values():
            return pair.local_candidate.type
    return None


async def run(args):
    holder = LatestFrameHolder()
    publisher = live.LivePublisher(
        holder=holder,
        width=args.width,
        height=args.height,
        fps=args.fps,
        bitrate_kbps=args.bitrate_kbps,
        connection_factory=relay_only_connection_factory(
            args.coturn_host, args.coturn_port, args.coturn_user, args.coturn_pass,
        ),
        on_lost=lambda reason: log("LOST", reason),
    )
    token = make_token(args.api_key, args.api_secret, args.room, args.identity)
    log("publishing into", args.url, "room", args.room)

    try:
        await publisher.start(args.url, args.room, token)
    except live.LiveStartError as exc:
        log("FAILED to start:", exc)
        return 2
    log("started; track sid =", publisher._track_sid)

    frame = np.zeros((args.height, args.width, 3), dtype=np.uint8)
    frame[:, :, 1] = 200
    deadline = time.monotonic() + args.seconds
    connected_at = None
    pair_type = None
    while time.monotonic() < deadline:
        holder.set(frame, timestamp_ms=int(time.time() * 1000))
        pc = publisher._pc
        if pc is None:
            log("FAILED: the session was torn down")
            break
        if connected_at is None and pc.connectionState == "connected":
            connected_at = time.monotonic()
            log("ICE/DTLS CONNECTED")
        if pc.connectionState in ("failed", "closed"):
            log("FAILED: peer connection state =", pc.connectionState)
            break
        if connected_at is not None:
            pair_type = nominated_local_candidate_type(pc)
            if pair_type is not None:
                break
        await asyncio.sleep(0.25)

    await publisher.stop()
    if connected_at is None:
        log("FAILED: never reached connected")
        return 3
    if pair_type is None:
        log("FAILED: connected, but no nominated pair was ever found")
        return 4
    log("SELECTED PAIR LOCAL CANDIDATE TYPE:", pair_type)
    if pair_type != "relay":
        log("FAILED: the selected pair is not a relay pair")
        return 5
    log("PASS: the selected pair is a relay pair")
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("LK_URL", "ws://localhost:7880"))
    parser.add_argument("--api-key", default=os.environ.get("LK_KEY", "devkey"))
    parser.add_argument("--api-secret", default=os.environ.get("LK_SECRET", "secret"))
    parser.add_argument("--room", default="fleetless-relay-proof")
    parser.add_argument("--identity", default="fleetless-bridge-relay-proof")
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--width", type=int, default=320)
    parser.add_argument("--height", type=int, default=240)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--bitrate-kbps", type=int, default=400)
    parser.add_argument("--coturn-host", required=True)
    parser.add_argument("--coturn-port", type=int, default=3478)
    parser.add_argument("--coturn-user", required=True)
    parser.add_argument("--coturn-pass", required=True)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
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
