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
- the colours are right when the picture is shown the way the stream says to
  show it: a viewer converts by the matrix the stream names, so a stream
  that names the wrong one decodes without a fault and looks wrong;
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
    StreamFailed,
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

#: The flat colours of the bars the colour check sends: the three primaries,
#: their mixes, and a light and a dark grey, which is where a wrong range
#: shows.
BARS = (
    (200, 30, 30), (30, 200, 30), (30, 30, 200), (200, 200, 30),
    (30, 200, 200), (200, 30, 200), (235, 235, 235), (20, 20, 20),
)

#: How far a bar's colour may be from the one that went in, per channel on a
#: scale of 255, once it is converted the way the stream says. Rounding and
#: the encoder cost two; a chain that converts to another matrix and says so
#: costs six. BT.601 samples under a BT.709 label put the worst bar 27 off,
#: and limited-range samples under a full-range label 17.
COLOUR_TOLERANCE = 10.0

#: `matrix_coefficients` of the stream, to the name and the two luma weights
#: (red, blue) a viewer converts with. 5 and 6 are the same matrix.
MATRICES = {
    1: ("BT.709", 0.2126, 0.0722),
    5: ("BT.601", 0.299, 0.114),
    6: ("BT.601", 0.299, 0.114),
    7: ("SMPTE 240M", 0.212, 0.087),
}

#: What the bridge pushes into every chain, and what an unlabelled stream is
#: therefore in.
PUSHED_MATRIX = 6

#: `profile_idc` of the stream, to its name.
PROFILES = {66: "baseline", 77: "main", 88: "extended", 100: "high"}

#: How far the stream's bitrate may be from the target it was encoded for,
#: as a factor either way. No measure of the rate control: at two frames a
#: second a good one lands at a third of its target. A chain that is told
#: kilobits where its encoder takes bits, or the other way round, is a
#: thousand off, and can still shrink and grow in step with the target.
BITRATE_UNIT_FACTOR = 8

_PROFILES_WITH_CHROMA_INFO = {100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135}


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


# --- what the stream says about itself -------------------------------------------


class _Bits:
    """The bits of a parameter set, read the way the H.264 syntax writes
    them. Reading past the end raises IndexError."""

    def __init__(self, data):
        self._data = data
        self._at = 0

    def u(self, count):
        value = 0
        for _ in range(count):
            value = (value << 1) | ((self._data[self._at >> 3] >> (7 - (self._at & 7))) & 1)
            self._at += 1
        return value

    def ue(self):
        zeros = 0
        while self.u(1) == 0:
            zeros += 1
            if zeros > 32:
                raise ValueError("not an Exp-Golomb code")
        return (1 << zeros) - 1 + (self.u(zeros) if zeros else 0)

    def se(self):
        code = self.ue()
        return (code + 1) // 2 if code % 2 else -(code // 2)


def _payload(unit):
    """A NAL unit without its header byte and without the bytes that keep
    start codes out of it."""
    payload = bytearray()
    zeros = 0
    for byte in unit[1:]:
        if zeros >= 2 and byte == 3:
            zeros = 0
            continue
        payload.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
    return bytes(payload)


def read_sps(unit):
    """What a sequence parameter set says that a viewer acts on: profile,
    constraint flags and level, and how the colours are to be converted.
    `colours` is `(primaries, transfer, matrix)` or `None` when the stream
    does not say. Raises IndexError or ValueError for one it cannot read."""
    bits = _Bits(_payload(unit))
    said = {"profile_idc": bits.u(8), "constraints": bits.u(8), "level_idc": bits.u(8),
            "colours": None, "full_range": False}
    bits.ue()  # seq_parameter_set_id
    if said["profile_idc"] in _PROFILES_WITH_CHROMA_INFO:
        chroma_format_idc = bits.ue()
        if chroma_format_idc == 3:
            bits.u(1)
        bits.ue()
        bits.ue()
        bits.u(1)
        if bits.u(1):  # scaling lists
            for index in range(8 if chroma_format_idc != 3 else 12):
                if bits.u(1):
                    last = following = 8
                    for _ in range(16 if index < 6 else 64):
                        if following:
                            following = (last + bits.se() + 256) % 256
                        last = following or last
    bits.ue()  # log2_max_frame_num_minus4
    order = bits.ue()
    if order == 0:
        bits.ue()
    elif order == 1:
        bits.u(1)
        bits.se()
        bits.se()
        for _ in range(bits.ue()):
            bits.se()
    bits.ue()  # max_num_ref_frames
    bits.u(1)
    bits.ue()  # width and height, in macroblocks
    bits.ue()
    if not bits.u(1):  # frame_mbs_only_flag
        bits.u(1)
    bits.u(1)
    if bits.u(1):  # cropping
        for _ in range(4):
            bits.ue()
    if bits.u(1):  # the VUI
        if bits.u(1) and bits.u(8) == 255:  # aspect ratio, spelled out
            bits.u(32)
        if bits.u(1):  # overscan
            bits.u(1)
        if bits.u(1):  # video signal type
            bits.u(3)
            said["full_range"] = bool(bits.u(1))
            if bits.u(1):
                said["colours"] = (bits.u(8), bits.u(8), bits.u(8))
    return said


def profile_of(sps):
    """`(name, fits the offer)`: what the stream calls its profile, and
    whether a decoder made for baseline can decode it. Every live session
    is offered as baseline."""
    name = PROFILES.get(sps["profile_idc"], "profile {}".format(sps["profile_idc"]))
    if sps["profile_idc"] == 66 and sps["constraints"] & 0x40:
        name = "constrained baseline"
    return name, sps["profile_idc"] == 66 or bool(sps["constraints"] & 0x80)


def sps_of(data):
    """The sequence parameter set of an access unit, read; `None` if it has
    none or it cannot be read."""
    for unit in HardwareH264Encoder._split_bitstream(data):
        if unit and unit[0] & 0x1F == NAL_SPS:
            try:
                return read_sps(unit)
            except (IndexError, ValueError):
                return None
    return None


# --- the colours ---------------------------------------------------------------------


def bars(width, height):
    """`(picture, edges)`: vertical bars of `BARS`, as RGB, and where each
    begins and the last ends."""
    picture = np.zeros((height, width, 3), dtype=np.uint8)
    edges = [width * index // len(BARS) for index in range(len(BARS) + 1)]
    for colour, left, right in zip(BARS, edges, edges[1:]):
        picture[:, left:right] = colour
    return picture, edges


def samples_of(frame):
    """The three planes of a decoded picture as the decoder left them:
    luma, and the two chroma planes at their own size."""
    return [
        np.frombuffer(plane, np.uint8).reshape(-1, abs(plane.line_size))[:plane.height, :plane.width]
        for plane in frame.planes[:3]
    ]


def as_a_viewer_shows_it(luma, blue, red, matrix, full_range):
    """One colour, from its three samples to RGB, by the weights of
    `matrix` and the range the stream names."""
    _, weight_red, weight_blue = MATRICES[matrix]
    if full_range:
        y, cb, cr = luma / 255.0, (blue - 128.0) / 255.0, (red - 128.0) / 255.0
    else:
        y, cb, cr = (luma - 16.0) / 219.0, (blue - 128.0) / 224.0, (red - 128.0) / 224.0
    r = y + 2 * (1 - weight_red) * cr
    b = y + 2 * (1 - weight_blue) * cb
    g = (y - weight_red * r - weight_blue * b) / (1 - weight_red - weight_blue)
    return tuple(255.0 * channel for channel in (r, g, b))


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
    except StreamFailed as failed:
        # It is here, it started, and then it refused the stream: a chain
        # that runs and does not do what it must, not one that is absent.
        report.check(False, "frame 0: an access unit for the frame", failed.reason)
        report.say("  => FAIL: the chain started and then refused the stream")
        return "fail"
    except ChainFailed as failed:
        report.say("  not on this machine: {}".format(failed.reason))
        return "absent"
    report.say("  built and started in {:.0f} ms".format((time.monotonic() - started) * 1000))

    decoder = av.CodecContext.create("h264", "r")
    forced = set(range(args.keyframe_every, args.frames, args.keyframe_every))
    sizes, times, keyframes = [], [], set()
    first_unit = b""
    try:
        try:
            for index in range(args.frames):
                picture = pattern(args.width, args.height, index)
                began = time.monotonic()
                data = pipeline.encode(frame_of(picture, index, args.fps), index in forced)
                times.append(time.monotonic() - began)
                sizes.append(len(data))
                first_unit = first_unit or data
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

        sps = sps_of(first_unit)
        if sps is not None:
            name, fits = profile_of(sps)
            report.say("  the stream is H.264 {} (profile_idc {}, constraint flags 0x{:02x}), "
                       "level {}.{}".format(name, sps["profile_idc"], sps["constraints"],
                                            sps["level_idc"] // 10, sps["level_idc"] % 10))
            if not fits:
                report.say("  note: a live session is offered as baseline. A viewer that holds "
                           "the stream to its offer may not show this one; say so in a report")
        index = args.frames
        if len(sizes) == args.frames and not report.failed:
            index = _prove_colours(pipeline, decoder, sps, args, report, index)
        if len(sizes) == args.frames and not report.failed:
            _prove_bitrate(pipeline, args, report, index)
    finally:
        try:
            pipeline.close()
        except ChainFailed as failed:
            report.check(False, "the pipeline goes back to NULL", failed.reason)
        else:
            report.check(pipeline.final_state == "null", "the pipeline goes back to NULL")

    report.say("  => {}".format("FAIL: " + "; ".join(report.failed) if report.failed else "PASS"))
    return "fail" if report.failed else "pass"


def _prove_colours(pipeline, decoder, sps, args, report, index):
    """Flat bars in, and each bar's colour out again when it is converted
    the way the stream says. Returns the index of the next frame.

    The picture check before this one converts with the decoder library's
    default, which is not what a viewer does: a browser reads the matrix
    from the stream. So here the samples are taken as they were decoded and
    converted by what the stream names."""
    what = "the colours are right when shown the way the stream says"
    if sps is None:
        report.check(False, what, "the first frame's parameter set could not be read")
        return index
    matrix = sps["colours"][2] if sps["colours"] is not None else 2
    if matrix == 2:  # unspecified, said or implied
        report.say("  the stream does not name its colours; a viewer assumes, as it does "
                   "for the software encoder. Converted here as what was pushed, BT.601")
        matrix = PUSHED_MATRIX
    elif matrix in MATRICES:
        report.say("  the stream names its colours: matrix {} ({}), primaries {}, transfer {}, "
                   "{} range".format(matrix, MATRICES[matrix][0], sps["colours"][0],
                                     sps["colours"][1], "full" if sps["full_range"] else "limited"))
    else:
        report.check(False, what, "the stream names matrix {}, which this tool cannot convert "
                                  "by".format(matrix))
        return index

    picture, edges = bars(args.width, args.height)
    shown = None
    try:
        for step in range(4):
            data = pipeline.encode(frame_of(picture, index, args.fps), step == 0)
            index += 1
            decoded = decoder.decode(av.Packet(data))
            shown = decoded[0] if len(decoded) == 1 else None
    except ChainFailed as failed:
        report.check(False, what, failed.reason)
        return index
    if shown is None or shown.format.name not in ("yuv420p", "yuvj420p"):
        report.check(False, what, "the decoder made {} of the bars".format(
            "no picture" if shown is None else shown.format.name))
        return index

    luma, blue, red = samples_of(shown)
    rows = slice(args.height // 4, max(args.height // 4 + 1, args.height * 3 // 4))
    half_rows = slice(rows.start // 2, max(rows.start // 2 + 1, rows.stop // 2))
    worst, worst_bar = 0.0, None
    for colour, left, right in zip(BARS, edges, edges[1:]):
        inset = (right - left) // 4
        columns = slice(left + inset, max(left + inset + 1, right - inset))
        half_columns = slice(columns.start // 2, max(columns.start // 2 + 1, columns.stop // 2))
        seen = as_a_viewer_shows_it(
            float(luma[rows, columns].mean()), float(blue[half_rows, half_columns].mean()),
            float(red[half_rows, half_columns].mean()), matrix, sps["full_range"],
        )
        off = max(abs(was - now) for was, now in zip(colour, seen))
        if off > worst:
            worst, worst_bar = off, (colour, tuple(int(round(channel)) for channel in seen))
    report.check(
        worst <= COLOUR_TOLERANCE, what,
        "the bar that went in as {} is shown as {}".format(*worst_bar) if worst > COLOUR_TOLERANCE
        else "worst channel of a bar {:.1f} of 255 off".format(worst),
    )
    return index


#: The fewest frames of one stretch of the bitrate check. A rate control
#: follows over frames, not over time: at a camera's two frames a second,
#: two seconds are four frames, and what is measured then is where the
#: encoder happened to be, not where it settles.
BITRATE_STRETCH_FRAMES = 30


def _prove_bitrate(pipeline, args, report, index):
    """A stretch at the target, one at a quarter of it, one back: the
    stream has to shrink and grow with it. Each stretch is two seconds of
    frames and never fewer than `BITRATE_STRETCH_FRAMES`; its second half
    is what is measured."""
    settle = max(BITRATE_STRETCH_FRAMES, args.fps * 2)

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
    # What a passing line says must not read like the failure: the detail
    # is printed either way.
    in_unit = target / BITRATE_UNIT_FACTOR <= high <= target * BITRATE_UNIT_FACTOR
    said = "{:.0f} kbps at a target of {} kbps".format(high / 1000, target // 1000)
    if not in_unit:
        said += (" is more than a factor of {} off; a chain that takes bits for kilobits "
                 "is a thousand off".format(BITRATE_UNIT_FACTOR))
    report.check(in_unit, "the bitrate is in the target's unit", said)


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
