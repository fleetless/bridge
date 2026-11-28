#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A viewer without a browser: joins live sessions and takes their frames.

A development fixture, never installed on a robot by the package. The bridge
encodes a camera only while somebody subscribes to its track, so a
measurement of the bridge under live load needs a viewer that stays. This
one joins each session the cloud handed out (`POST
/api/robots/:id/cameras/:slug/live`), subscribes, decodes every frame and
counts them.

The sessions come from the environment, never from the command line, since
a session token is a credential:

    LIVE_SESSIONS=2
    LIVE_URL_1=ws://...   LIVE_TOKEN_1=...
    LIVE_URL_2=ws://...   LIVE_TOKEN_2=...

Every `--every` seconds it prints one line with the frames each session
received since the last line. A session that ends is said so and not
joined again: the counts then show the gap.
"""
import argparse
import asyncio
import os
import sys
import time

from livekit import rtc


async def watch(index, url, token, counts, ended):
    room = rtc.Room()

    async def consume(track):
        async for _ in rtc.VideoStream(track):
            counts[index] += 1

    @room.on("track_subscribed")
    def on_subscribed(track, publication, participant):
        if track.kind == rtc.TrackKind.KIND_VIDEO:
            asyncio.ensure_future(consume(track))

    @room.on("disconnected")
    def on_disconnected(reason):
        ended[index] = str(reason)

    await room.connect(url, token)
    print("viewer: session {} joined room {}".format(index, room.name), flush=True)
    return room


async def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--every", type=float, default=30.0, help="seconds between two count lines")
    args = parser.parse_args(argv)
    sessions = int(os.environ.get("LIVE_SESSIONS", "0"))
    if sessions < 1:
        print("viewer: LIVE_SESSIONS names no session", flush=True)
        return 2
    counts = {index: 0 for index in range(1, sessions + 1)}
    ended = {}
    rooms = []
    for index in counts:
        url = os.environ["LIVE_URL_{}".format(index)]
        token = os.environ.pop("LIVE_TOKEN_{}".format(index))
        rooms.append(await watch(index, url, token, counts, ended))
    started = time.monotonic()
    last = dict(counts)
    while True:
        await asyncio.sleep(args.every)
        now = dict(counts)
        print("viewer: {:6.0f} s  frames {}{}".format(
            time.monotonic() - started,
            " ".join("{}:{}".format(index, now[index] - last[index]) for index in now),
            "".join("  session {} ended ({})".format(index, why) for index, why in sorted(ended.items())),
        ), flush=True)
        last = now


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
