#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""3c.4's soak: `LivePublisher` against the dev LiveKit for a bounded run,
with open file descriptors (`/proc/self/fd`) and RSS sampled at a fixed
interval. The spike (3c.3's report) ran only 60s; this proves a longer,
unattended session instead.

    python3 soak.py --room ROOM --minutes 15 --sample-interval-s 30

Prints one `SOAK SAMPLE tick=<n> elapsed_s=<n> fds=<n> rss_kb=<n>` line per
sample and, at the end, `SOAK PASS`/`SOAK FAIL` plus the slope of each series
(least squares, in units per minute, over every sample after the first two
minutes of wall time -- the first two are warm-up: aiortc's encoder, the
socket buffers and the first several GC generations are all still settling
then, and including them in the fit would price normal startup as drift).
The thresholds are stated here, not implied: **flat** means the fitted
slope, extended over the fitted window, moves fewer than 5 descriptors and no
more than 20% of the run's own mean RSS.

**What a run at the default 15 minutes / 30s samples covers, and what it does
not, stated plainly rather than left for a flat line to imply.** This run
samples RSS and fd counts 30 times over the 900s window -- coarse compared
to the loops running underneath it, which is the point: a per-tick leak
still shows up as a slope over 30 points as long as it accumulates by a
fixed amount on every tick of one of those faster loops. In 15 minutes, the
1s bitrate-hold reassertion ticks ~900 times and the pingReq keepalive ticks
~180 times (5s default interval) -- either is well inside this window.
`run-soak.sh` keeps a viewer subscribed for the whole run, so the per-frame
media path (`cv2.cvtColor`, `VideoFrame.from_ndarray`, the encoder) also
runs continuously rather than pausing after the first ~10s, which is what a
run with no viewer measures instead: a process asleep for the other 99% of
it. It does **not** reach a leak whose growth is keyed to garbage-collector
cadence rather than to a per-tick cost: 3b's own `ros_runtime.py` finding
needed roughly 880 create/destroy cycles before `gc.collect()` fell behind
allocation, and `main.py` builds one `LivePublisher` per process and never
tears it down mid-run -- this soak samples ZERO create/destroy cycles, not
merely fewer than 880 of them. A flat line from this run is evidence against
the first kind of leak and says nothing about the second.
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

from fleetless_bridge import live  # noqa: E402
from fleetless_bridge.camera import LatestFrameHolder  # noqa: E402

# Without this, `live.py`'s own `log.info("Live publish paused/resumed", ...)`
# never reaches stdout: the root logger has no handler configured, so a
# 15-minute run where the publisher paused at t=10s and one that fed frames
# the whole time print byte-identical output apart from the SAMPLE lines
# below -- and those alone cannot tell the two apart either (see
# `frames_sent`).
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")


def log(*args):
    print(time.strftime("%H:%M:%S"), "SOAK", *args, flush=True)


def _b64(raw):
    if not isinstance(raw, bytes):
        raw = json.dumps(raw, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def make_token(api_key, api_secret, room, identity):
    now = int(time.time())
    head = _b64({"alg": "HS256", "typ": "JWT"})
    body = _b64({
        "iss": api_key, "sub": identity, "name": identity,
        "nbf": now - 10, "exp": now + 3600 * 3,
        "video": {"room": room, "roomJoin": True, "canPublish": True,
                  "canSubscribe": False, "canPublishData": False},
    })
    signing_input = "{}.{}".format(head, body).encode()
    signature = hmac.new(api_secret.encode(), signing_input, hashlib.sha256).digest()
    return "{}.{}.{}".format(head, body, _b64(signature))


def fd_count():
    return len(os.listdir("/proc/self/fd"))


def rss_kb():
    with open("/proc/self/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return -1


def _slope(xs, ys):
    """Least-squares slope of ys against xs. Pure stdlib/numpy arithmetic
    (numpy is already a runtime dependency of this package, so nothing new is
    pulled in for the soak alone)."""
    x = np.array(xs, dtype=float)
    y = np.array(ys, dtype=float)
    x_mean, y_mean = x.mean(), y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def pattern(width, height, n):
    frame = np.zeros((height, width, 3), dtype=np.uint8)
    frame[:, :, 0] = np.linspace(0, 255, width, dtype=np.uint8)[None, :]
    frame[:, :, 1] = np.linspace(0, 255, height, dtype=np.uint8)[:, None]
    frame[:, :, 2] = 128
    x = (n * 13) % max(1, width - 40)
    frame[:, x:x + 40, :] = 255
    return frame


async def run(args):
    holder = LatestFrameHolder()
    publisher = live.LivePublisher(
        holder=holder, width=args.width, height=args.height, fps=args.fps,
        bitrate_kbps=args.bitrate_kbps,
        on_lost=lambda reason: log("LOST", reason),
    )
    token = make_token(args.api_key, args.api_secret, args.room, args.identity)
    log("publishing into", args.url, "room", args.room, "for", args.minutes,
        "minute(s), sampling every", args.sample_interval_s, "s")
    try:
        await publisher.start(args.url, args.room, token)
    except live.LiveStartError as exc:
        log("FAILED to start:", exc)
        return 2
    log("started")

    # Elapsed minutes (float) is the x-axis for both series, independent of
    # the sample interval -- so `--sample-interval-s` only changes how many
    # points the fit gets, never what the slope means.
    elapsed_min, fds, rsses = [], [], []
    frame_n = 0
    start = time.monotonic()
    deadline = start + args.minutes * 60
    next_sample = start
    frame_interval = 1.0 / args.fps
    next_frame = start
    while time.monotonic() < deadline:
        now = time.monotonic()
        if now >= next_frame:
            holder.set(pattern(args.width, args.height, frame_n),
                       timestamp_ms=int(time.time() * 1000))
            frame_n += 1
            next_frame += frame_interval
        pc = publisher._pc
        if pc is None:
            log("FAILED: the session was torn down")
            await publisher.stop()
            return 3
        if now >= next_sample:
            tick = len(elapsed_min)
            elapsed_s = now - start
            fd, rss = fd_count(), rss_kb()
            elapsed_min.append(elapsed_s / 60.0)
            fds.append(fd)
            rsses.append(rss)
            # frames_sent is cumulative on the track itself (see live.py) --
            # reading it here, per sample, is what makes a paused run and a
            # frame-feeding one produce visibly different logs: a paused
            # publisher's count stops climbing while `connection_state` and
            # the fd/RSS samples keep looking exactly as healthy as a working
            # one.
            frames_sent = publisher._track.frames_sent if publisher._track is not None else -1
            log("SAMPLE tick={} elapsed_s={:.0f} fds={} rss_kb={} connection_state={} "
                "paused={} frames_sent={}".format(
                    tick, elapsed_s, fd, rss, pc.connectionState, publisher._paused, frames_sent))
            next_sample += args.sample_interval_s
        await asyncio.sleep(min(frame_interval, max(0.0, next_frame - time.monotonic()) or frame_interval))

    await publisher.stop()

    if len(elapsed_min) < 4:
        log("FAILED: too few samples ({}) to fit a slope over".format(len(elapsed_min)))
        return 4

    # Drop the first two minutes of wall time as warm-up -- see the module
    # docstring. At least one sample is always kept even on a very short run.
    warm = 0
    for i, t in enumerate(elapsed_min):
        if t >= 2.0:
            warm = i
            break
    else:
        warm = 0
    if len(elapsed_min) - warm < 4:
        warm = 0  # too short a run to spare a warm-up; fit everything instead

    fit_elapsed = elapsed_min[warm:]
    fit_fds = fds[warm:]
    fit_rss = rsses[warm:]

    fd_slope = _slope(fit_elapsed, fit_fds)
    rss_slope = _slope(fit_elapsed, fit_rss)
    fitted_minutes = fit_elapsed[-1] - fit_elapsed[0] if len(fit_elapsed) > 1 else 0
    fd_drift = fd_slope * fitted_minutes
    rss_drift = rss_slope * fitted_minutes
    mean_rss = sum(fit_rss) / len(fit_rss)

    log("{} samples fitted ({} dropped as warm-up), over {:.1f} minutes".format(
        len(fit_elapsed), warm, fitted_minutes))
    log("fd slope = {:.4f} fds/minute, drift over the fitted window = {:.2f} fds".format(
        fd_slope, fd_drift))
    log("rss slope = {:.2f} kB/minute, drift over the fitted window = {:.1f} kB "
        "({:.1f}% of mean {:.0f} kB)".format(
            rss_slope, rss_drift, 100.0 * abs(rss_drift) / mean_rss if mean_rss else 0.0, mean_rss))

    fd_flat = abs(fd_drift) < 5
    rss_flat = mean_rss == 0 or abs(rss_drift) < 0.20 * mean_rss
    if fd_flat and rss_flat:
        log("SOAK PASS: both fds and RSS flat within noise")
        return 0
    log("SOAK FAIL: {}{}".format(
        "" if fd_flat else "fd count drifted by {:.2f} over the run; ".format(fd_drift),
        "" if rss_flat else "RSS drifted by {:.1f}% of its mean over the run".format(
            100.0 * abs(rss_drift) / mean_rss if mean_rss else 0.0)))
    return 5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.environ.get("LK_URL", "ws://localhost:7880"))
    parser.add_argument("--api-key", default=os.environ.get("LK_KEY", "devkey"))
    parser.add_argument("--api-secret", default=os.environ.get("LK_SECRET", "secret"))
    parser.add_argument("--room", default="fleetless-live-soak")
    parser.add_argument("--identity", default="fleetless-bridge-soak")
    parser.add_argument("--minutes", type=int, default=15)
    parser.add_argument("--sample-interval-s", type=int, default=30)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--fps", type=int, default=15)
    parser.add_argument("--bitrate-kbps", type=int, default=800)
    args = parser.parse_args()
    code = asyncio.run(run(args))
    log("EXIT", code)
    sys.exit(code)


if __name__ == "__main__":
    main()
