#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Prove a hardware encoder chain on the machine that has the hardware.

A development fixture, never installed on a robot by the package. It runs the
shipped pipeline class, `fleetless_bridge.live_encoder.Pipeline`, over the
chains of one value of `live.encoder`, with a synthetic moving pattern, and
checks what every chain has to deliver:

- it can be built and started;
- one access unit comes back for one frame, within the time limit;
- the first frame and every frame a keyframe was asked for is an IDR picture
  with SPS and PPS, and no other frame is one;
- a decoder shows each access unit as one picture, the picture that went in,
  in the order it went in;
- the bitrate follows the target while running;
- the pipeline goes back to NULL.

Then it asks the encoder class itself what the bridge would do with this
setting on this machine.

    python3 encoder_proof.py --backend nvenc
    python3 encoder_proof.py --backend auto
    python3 encoder_proof.py --backend vaapi --width 1920 --height 1080 --fps 30

It prints timings and a verdict and exits 0 for PASS, 1 for a chain that
runs and does not do what it must, 2 when no chain of the backend can be
built here. The whole output is what to send along with a report.

`--backend stand-in` runs the software stand-in, which is what the suite
uses on machines without an encoder chip. It proves the tool, not a chip.

Decoding is done with PyAV, which the bridge has anyway. Nothing here needs
a camera, a cloud or a network.
"""
import argparse
import logging
import os
import sys
import time
from fractions import Fraction

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import av  # noqa: E402

from fleetless_bridge import live_encoder  # noqa: E402
from fleetless_bridge.live_encoder import (  # noqa: E402
    STAND_IN,
    ChainFailed,
    ChainMemory,
    GstUnavailable,
    HardwareH264Encoder,
    Pipeline,
    chains_for,
)

NAL_SLICE, NAL_IDR, NAL_SPS, NAL_PPS = 1, 5, 7, 8

#: How far a decoded picture may be from the one that went in, as the mean
#: difference of a channel on a scale of 255. An encoder at a sane bitrate is
#: far below it; a picture with swapped channels or planes is far above.
PICTURE_TOLERANCE = 20.0


def pattern(width, height, index):
    """A picture with gradients that shift and a bar that travels, as RGB.
    Each frame differs from its neighbours, and the three channels differ
    from each other, so a wrong frame and a swapped channel both show."""
    picture = np.zeros((height, width, 3), dtype=np.uint8)
    picture[:, :, 0] = (np.arange(width) * 255 // max(1, width - 1) + index * 9) % 256
    picture[:, :, 1] = ((np.arange(height) * 255 // max(1, height - 1) + index * 5) % 256)[:, None]
    picture[:, :, 2] = 64
    bar = max(8, height // 10)
    top = (index * max(2, height // 40)) % max(1, height - bar)
    picture[top:top + bar, :, 2] = 255
    return picture


def frame_of(picture, index, fps):
    frame = av.VideoFrame.from_ndarray(picture, format="rgb24")
    frame.pts = index * (90000 // fps)
    frame.time_base = Fraction(1, 90000)
    return frame


def nal_types(data):
    return [unit[0] & 0x1F for unit in HardwareH264Encoder._split_bitstream(data) if unit]


class Report:
    """Collects the lines of one chain's proof and whether any check failed."""

    def __init__(self, out):
        self._out = out
        self.failed = []

    def say(self, text):
        print(text, file=self._out, flush=True)

    def check(self, ok, what, detail=""):
        self.say("  {} {}{}".format("ok  " if ok else "FAIL", what, " ({})".format(detail) if detail else ""))
        if not ok:
            self.failed.append(what)
        return ok


def prove(chain, args, out):
    """Run one chain through every check. Returns `pass`, `fail`, or
    `absent` when the chain cannot be built on this machine at all."""
    report = Report(out)
    report.say("")
    report.say("chain {}: {} ! {}".format(chain.name, chain.convert, chain.encoder))
    started = time.monotonic()
    try:
        pipeline = Pipeline(
            chain, width=args.width, height=args.height, fps=args.fps,
            bitrate_bps=args.bitrate_kbps * 1000,
        )
    except GstUnavailable as unavailable:
        report.say("  not on this machine: {}".format(unavailable.reason))
        return "absent"
    except ChainFailed as failed:
        report.say("  not on this machine: {}".format(failed.reason))
        return "absent"
    report.say("  built and started in {:.0f} ms".format((time.monotonic() - started) * 1000))

    decoder = av.CodecContext.create("h264", "r")
    forced = set(range(args.keyframe_every, args.frames, args.keyframe_every))
    sizes, times, keyframes = [], [], set()
    try:
        try:
            for index in range(args.frames):
                picture = pattern(args.width, args.height, index)
                began = time.monotonic()
                data = pipeline.encode(frame_of(picture, index, args.fps), index in forced)
                times.append(time.monotonic() - began)
                sizes.append(len(data))
                types = nal_types(data)
                if NAL_IDR in types:
                    keyframes.add(index)
                    if not {NAL_SPS, NAL_PPS} <= set(types):
                        report.check(False, "frame {}: a keyframe without SPS and PPS".format(index),
                                     "NAL types {}".format(types))
                shown = decoder.decode(av.Packet(data))
                if len(shown) != 1:
                    report.check(
                        False, "frame {}: one access unit is one picture".format(index),
                        "the decoder made {} of it; B-frames or lookahead?".format(len(shown)),
                    )
                    break
                seen = shown[0].to_ndarray(format="rgb24").astype(np.int16)
                difference = float(np.abs(seen - picture).mean())
                if seen.shape != picture.shape or difference > PICTURE_TOLERANCE:
                    report.check(
                        False, "frame {}: the picture that comes out is the one that went in".format(index),
                        "mean difference {:.1f} of 255, size {}x{}".format(
                            difference, seen.shape[1], seen.shape[0]
                        ),
                    )
                    break
        except ChainFailed as failed:
            report.check(False, "frame {}: an access unit for the frame".format(len(sizes)), failed.reason)

        if len(sizes) == args.frames and not report.failed:
            report.check(True, "{} frames in, {} access units out, each decoded to its own picture".format(
                args.frames, len(sizes)))
        if sizes:
            ordered = sorted(times)
            report.say("  per frame: median {:.1f} ms, 95th percentile {:.1f} ms, slowest {:.1f} ms".format(
                ordered[len(ordered) // 2] * 1000, ordered[int(len(ordered) * 0.95) - 1] * 1000,
                ordered[-1] * 1000))
            report.check(0 in keyframes, "the first frame is a keyframe")
            report.check(
                forced <= keyframes or len(sizes) < args.frames,
                "every frame a keyframe was asked for is one",
                "asked at {}, got {}".format(sorted(forced), sorted(keyframes)),
            )
            unasked = keyframes - forced - {0}
            report.check(not unasked, "no keyframe nobody asked for",
                         "at {}".format(sorted(unasked)) if unasked else "")

        if len(sizes) == args.frames and not report.failed:
            _prove_bitrate(pipeline, args, report)
    finally:
        try:
            pipeline.close()
        except ChainFailed as failed:
            report.check(False, "the pipeline goes back to NULL", failed.reason)
        else:
            report.check(pipeline.final_state == "null", "the pipeline goes back to NULL")

    report.say("  => {}".format("FAIL: " + "; ".join(report.failed) if report.failed else "PASS"))
    return "fail" if report.failed else "pass"


def _prove_bitrate(pipeline, args, report):
    """Two seconds at the target, two at a quarter of it, two back: the
    stream has to shrink and grow with it."""
    settle = args.fps * 2
    index = args.frames

    def stretch(bitrate_bps):
        nonlocal index
        pipeline.set_bitrate(bitrate_bps)
        sizes = []
        for _ in range(settle):
            data = pipeline.encode(
                frame_of(pattern(args.width, args.height, index), index, args.fps), False
            )
            sizes.append(len(data))
            index += 1
        # The second half, once the rate control has followed.
        return sum(sizes[settle // 2:]) * 8 * args.fps / (settle - settle // 2)

    target = args.bitrate_kbps * 1000
    try:
        high = stretch(target)
        low = stretch(target // 4)
        back = stretch(target)
    except ChainFailed as failed:
        report.check(False, "the bitrate follows the target", failed.reason)
        return
    report.say("  measured: {:.0f} kbps at {} kbps, {:.0f} kbps at {} kbps, {:.0f} kbps back at {} kbps".format(
        high / 1000, target // 1000, low / 1000, target // 4000, back / 1000, target // 1000))
    report.check(low < 0.6 * high and low < 0.6 * back, "the bitrate follows the target",
                 "a quarter of the target should be well under it")


def what_the_bridge_would_do(backend, chains, args, out):
    """The encoder class over the same chains, with a memory of its own:
    the chain it picks, or the fallback and the warning it logs."""
    print("", file=out)
    print("what the bridge does with live.encoder: {} here".format(backend), file=out)
    encoder = HardwareH264Encoder(backend, chains, fps=args.fps, memory=ChainMemory())
    encoder.target_bitrate = args.bitrate_kbps * 1000
    try:
        payloads, _ = encoder.encode(
            frame_of(pattern(args.width, args.height, 0), 0, args.fps), False
        )
        chain = encoder.chain
        if chain is not None:
            print("  encodes with {} ({} RTP payloads for the first frame)".format(
                chain.name, len(payloads)), file=out)
        else:
            print("  falls back to software H.264, with the warning above", file=out)
        return chain
    finally:
        encoder.close()


def main(argv=None, out=None):
    out = out if out is not None else sys.stdout
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--backend", required=True,
                        choices=["auto", "nvenc", "vaapi", "v4l2", "stand-in"],
                        help="the value of live.encoder to prove, or stand-in for the software element")
    parser.add_argument("--chain", help="only this chain of the backend, by its name")
    parser.add_argument("--width", type=int, default=1024)
    parser.add_argument("--height", type=int, default=768)
    parser.add_argument("--fps", type=int, default=15)
    parser.add_argument("--frames", type=int, default=90)
    parser.add_argument("--bitrate-kbps", type=int, default=2000)
    parser.add_argument("--keyframe-every", type=int, default=30,
                        help="ask for a keyframe at every such frame")
    args = parser.parse_args(argv)
    if args.width % 2 or args.height % 2:
        parser.error("width and height have to be even")

    chains = (STAND_IN,) if args.backend == "stand-in" else chains_for(args.backend)
    if args.chain is not None:
        chains = tuple(chain for chain in chains if chain.name == args.chain)
        if not chains:
            parser.error("no chain {!r} in backend {}".format(args.chain, args.backend))

    # The encoder's own lines belong in the report: the chain it picks, the
    # one warning of a fallback. Its logger only; the codec libraries have
    # plenty to say that is not about this.
    handler = logging.StreamHandler(out)
    handler.setFormatter(logging.Formatter("  log %(levelname)s %(name)s: %(message)s"))
    package_log = logging.getLogger(live_encoder.__name__)
    previous_level = package_log.level
    package_log.addHandler(handler)
    package_log.setLevel(logging.INFO)
    try:
        return _run(args, chains, out)
    finally:
        package_log.removeHandler(handler)
        package_log.setLevel(previous_level)


def _run(args, chains, out):
    print("encoder proof: backend {}, {}x{} at {} fps, {} kbps, {} frames".format(
        args.backend, args.width, args.height, args.fps, args.bitrate_kbps, args.frames), file=out)
    try:
        Gst, _ = live_encoder._load_gst()
        print("GStreamer {}".format(".".join(str(part) for part in Gst.version()[:3])), file=out)
    except GstUnavailable as unavailable:
        print(unavailable.reason, file=out)
        print("\nVERDICT: no chain of {} can be built on this machine".format(args.backend), file=out)
        return 2

    results = {chain.name: prove(chain, args, out) for chain in chains}
    used = what_the_bridge_would_do(args.backend, chains, args, out)

    print("", file=out)
    if "fail" in results.values():
        failed = [name for name, result in results.items() if result == "fail"]
        print("VERDICT: FAIL ({})".format(", ".join(failed)), file=out)
        return 1
    if used is None or "pass" not in results.values():
        print("VERDICT: no chain of {} can be built on this machine".format(args.backend), file=out)
        return 2
    print("VERDICT: PASS ({})".format(used.name), file=out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
