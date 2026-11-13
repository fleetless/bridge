# SPDX-License-Identifier: Apache-2.0
"""H.264 (and one H.265) test streams, encoded in the test with PyAV's libx264.

Nothing is checked in: a stream committed as bytes says nothing about how it
was made, and the tests here need to choose the profile, the size, the keyframe
interval and whether there are B-frames.

Every access unit comes back as Annex-B bytes, in decode order -- the order a
camera sends them in and the order the bridge forwards them in.
"""
import fractions
from typing import List, Optional, Tuple

import av
import numpy as np


def source_pictures(width: int, height: int, frames: int, noisy: bool = False) -> List["np.ndarray"]:
    """`frames` BGR pictures that differ from one another: a bar that moves
    and a background that changes, so an encoder has something to predict and
    a comparison of two decoded pictures can tell them apart.

    `noisy` lays a random texture under each picture, a new one per frame.
    Flat colour compresses to a few hundred bytes whatever the size; noise
    makes access units of several kilobytes, which is what it takes to see a
    NAL unit split across RTP packets."""
    random = np.random.default_rng(1117)
    pictures = []
    for index in range(frames):
        if noisy:
            picture = random.integers(0, 256, size=(height, width, 3), dtype=np.uint8)
        else:
            picture = np.full((height, width, 3), (index * 9) % 200 + 20, dtype=np.uint8)
        left = (index * 5) % max(1, width - 8)
        picture[:, left:left + 8] = (255, 64, 0)
        pictures.append(picture)
    return pictures


def encode_stream(
    *,
    width: int = 64,
    height: int = 48,
    frames: int = 12,
    gop: int = 4,
    bframes: int = 0,
    profile: str = "high",
    codec: str = "libx264",
    noisy: bool = False,
) -> List[bytes]:
    """`frames` pictures as access units.

    `gop` is the keyframe interval, exact: scene-cut detection is off and the
    minimum equals the maximum, so unit `n * gop` (in display order) is an IDR
    and nothing else is. `bframes` above zero forces that many B-frames
    between references (`b-adapt=0`), because x264 left to itself drops them
    for pictures this small. SPS and PPS repeat with every keyframe; a test
    that wants them only once, or out of band, strips them with
    `strip_parameter_sets`.

    `profile` picks the SPS's profile: with the `veryfast` preset x264 uses
    CABAC and the 8x8 transform where the profile allows them, so `baseline`,
    `main` and `high` come out as 66, 77 and 100. (`ultrafast` uses neither
    and reports baseline whatever was asked for.)
    """
    context = av.CodecContext.create(codec, "w")
    context.width = width
    context.height = height
    context.pix_fmt = "yuv420p"
    context.time_base = fractions.Fraction(1, 30)
    if codec == "libx264":
        params = "keyint={g}:min-keyint={g}:scenecut=0:bframes={b}:b-adapt=0:repeat-headers=1".format(
            g=gop, b=bframes
        )
        options = {"preset": "veryfast", "profile": profile, "x264-params": params}
        if bframes == 0:
            options["tune"] = "zerolatency"
    else:
        options = {"preset": "ultrafast", "x265-params": "keyint={g}:min-keyint={g}:log-level=none".format(g=gop)}
    context.options = options

    units: List[bytes] = []
    for index, picture in enumerate(source_pictures(width, height, frames, noisy)):
        frame = av.VideoFrame.from_ndarray(picture, format="bgr24").reformat(format="yuv420p")
        frame.pts = index
        units.extend(bytes(packet) for packet in context.encode(frame))
    units.extend(bytes(packet) for packet in context.encode(None))
    return [unit for unit in units if unit]


def split(unit: bytes) -> List[bytes]:
    """The NAL units of one access unit, without start codes. The tests' own
    splitter, deliberately not the one under test: a helper that prepared its
    input with the code it then checks would agree with it about any mistake."""
    nals: List[bytes] = []
    position = unit.find(b"\x00\x00\x01")
    while position != -1:
        start = position + 3
        position = unit.find(b"\x00\x00\x01", start)
        end = len(unit) if position == -1 else position
        nal = unit[start:end].rstrip(b"\x00")
        if nal:
            nals.append(nal)
    return nals


def join(nals: List[bytes], start_code: bytes = b"\x00\x00\x00\x01") -> bytes:
    return b"".join(start_code + nal for nal in nals)


def parameter_sets(unit: bytes) -> Tuple[Optional[bytes], Optional[bytes]]:
    """`(sps, pps)` of an access unit, each a NAL unit without start code."""
    sps = pps = None
    for nal in split(unit):
        if nal[0] & 0x1F == 7:
            sps = nal
        elif nal[0] & 0x1F == 8:
            pps = nal
    return sps, pps


def strip_parameter_sets(unit: bytes) -> bytes:
    """The access unit without its SPS and PPS -- what a source sends that
    carries them out of band, or only at its beginning."""
    return join([nal for nal in split(unit) if nal[0] & 0x1F not in (7, 8)])


def strip_nal_types(unit: bytes, kinds) -> bytes:
    """The access unit without its NAL units of the given types: `{8}` for a
    source that repeats its SPS and sends its PPS once, `{7}` for the other
    way round."""
    return join([nal for nal in split(unit) if nal[0] & 0x1F not in kinds])


def nal_types(unit: bytes) -> List[int]:
    return [nal[0] & 0x1F for nal in split(unit)]


def is_idr(unit: bytes) -> bool:
    return 5 in nal_types(unit)


def decode_all(units: List[bytes]) -> List["np.ndarray"]:
    """Every picture of a stream, decoded with PyAV, in display order."""
    decoder = av.CodecContext.create("h264", "r")
    pictures = []
    for unit in units:
        for frame in decoder.decode(av.Packet(unit)):
            pictures.append(frame.to_ndarray(format="bgr24"))
    for frame in decoder.decode(None):
        pictures.append(frame.to_ndarray(format="bgr24"))
    return pictures
