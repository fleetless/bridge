# SPDX-License-Identifier: Apache-2.0
"""Reading an H.264 byte stream without decoding it.

Passthrough forwards the access units a source delivers, so the bridge never
decodes them. It still has to know four things about each one, and all four
are in the first bytes of its NAL units:

- where the NAL units are (Annex B start codes), because the sender packetizes
  NAL units, not access units;
- whether it is a keyframe (an IDR slice), because a viewer can only start
  there;
- the picture size and profile (the SPS), because `width` and `height` in the
  configuration must say what the bridge sends;
- whether it holds a B-slice, because browsers do not play a stream whose
  frames arrive out of display order.

Deliberately free of rclpy, aiortc and PyAV, like sampling.py: bytes in,
facts out, testable against a stream an encoder wrote.

Annex B only. The length-prefixed form an MP4 file carries (AVCC) has no
start codes and is refused as not parseable; RTSP through ffmpeg, Foxglove's
`CompressedVideo` and Isaac ROS all deliver Annex B.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

#: `nal_unit_type` values this module tells apart (ITU-T H.264, table 7-1).
NAL_SLICE = 1  # a slice of a picture that is not an IDR picture
NAL_IDR = 5  # a slice of an IDR picture: a keyframe
NAL_SEI = 6
NAL_SPS = 7
NAL_PPS = 8
NAL_AUD = 9

#: `slice_type % 5` (table 7-6). The values 5 to 9 say the same about every
#: slice of the picture and reduce to these.
SLICE_P = 0
SLICE_B = 1
SLICE_I = 2
SLICE_SP = 3
SLICE_SI = 4

#: The largest `seq_parameter_set_id` and `pic_parameter_set_id` the standard
#: has (7.4.2.1.1, 7.4.2.2). An id above them is not a parameter set to keep.
SPS_ID_MAX = 31
PPS_ID_MAX = 255

#: The longest SPS or PPS NAL unit this module reads. A real one is a few
#: dozen bytes, and a few hundred with every scaling list written out; this
#: is several times that. Anything longer is refused before a bit of it is
#: read: the reader below goes through a parameter set bit by bit, in Python.
PARAMETER_SET_MAX_BYTES = 4096

_START_CODE = b"\x00\x00\x01"
_ANNEXB_START = b"\x00\x00\x00\x01"

#: Profiles whose SPS carries the chroma format and the scaling lists before
#: the fields every profile has (7.3.2.1.1). Baseline, main and extended are
#: not among them and are always 4:2:0.
_PROFILES_WITH_CHROMA_INFO = frozenset(
    (100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135)
)


class H264ParseError(ValueError):
    """These bytes are not H.264 this module can read: no start code, a unit
    cut short, a field outside what the standard allows."""


class TooManyNalUnits(H264ParseError):
    """More NAL units in one access unit than the caller allows."""


def split_nal_units(data: bytes, max_units: Optional[int] = None) -> List[bytes]:
    """The NAL units of an Annex-B byte string, without their start codes.

    A unit runs from one start code (`00 00 01`, with any number of zero
    bytes before it) to the next. Zero bytes before a start code are padding
    and not payload: a NAL unit ends in its stop bit and never in a zero
    byte. The escape `00 00 03` inside a unit is left where it is.

    Raises `H264ParseError` when there is no unit to return, or when the
    bytes before the first start code are anything but padding -- that is
    what a JPEG, an AVCC sample or a truncated message looks like from here.

    With `max_units`, raises `TooManyNalUnits` at the start code that comes
    after that many. The count is taken while splitting and counts start
    codes, so megabytes of five-byte units, or of start codes with nothing
    between them, are turned away after `max_units` steps and not walked to
    the end: every step here is Python.
    """
    data = bytes(data)
    position = data.find(_START_CODE)
    if position == -1:
        raise H264ParseError("no Annex-B start code")
    if data[:position].strip(b"\x00"):
        raise H264ParseError("bytes before the first start code")
    nals: List[bytes] = []
    found = 0
    while position != -1:
        found += 1
        if max_units is not None and found > max_units:
            raise TooManyNalUnits(
                "more than {} NAL units in one access unit".format(max_units)
            )
        start = position + 3
        position = data.find(_START_CODE, start)
        nal = data[start:len(data) if position == -1 else position].rstrip(b"\x00")
        if nal:
            nals.append(nal)
    if not nals:
        raise H264ParseError("start codes with nothing between them")
    return nals


def join_annexb(nals: Sequence[bytes]) -> bytes:
    """NAL units as one Annex-B byte string, each behind a four-byte start
    code -- what a decoder or a file wants."""
    return b"".join(_ANNEXB_START + nal for nal in nals)


def nal_type(nal: bytes) -> int:
    """`nal_unit_type` of one unit. Raises when the forbidden bit is set: no
    H.264 unit has it, and an H.265 unit read as H.264 often does."""
    if not nal:
        raise H264ParseError("empty NAL unit")
    if nal[0] & 0x80:
        raise H264ParseError("forbidden_zero_bit is set")
    return nal[0] & 0x1F


def _rbsp(payload: bytes) -> bytes:
    """The payload with the emulation-prevention bytes taken out: every
    `00 00 03` becomes `00 00`."""
    if b"\x00\x00\x03" not in payload:
        return payload
    out = bytearray()
    zeros = 0
    for byte in payload:
        if zeros >= 2 and byte == 3:
            zeros = 0
            continue
        out.append(byte)
        zeros = zeros + 1 if byte == 0 else 0
    return bytes(out)


class _Bits:
    """An MSB-first bit reader over an RBSP, with the two Exp-Golomb codes
    the headers are written in."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._position = 0
        self._length = len(data) * 8

    def u(self, bits: int) -> int:
        if self._position + bits > self._length:
            raise H264ParseError("the unit ends inside a field")
        value = 0
        for _ in range(bits):
            byte = self._data[self._position >> 3]
            value = (value << 1) | ((byte >> (7 - (self._position & 7))) & 1)
            self._position += 1
        return value

    def ue(self) -> int:
        zeros = 0
        while self.u(1) == 0:
            zeros += 1
            if zeros > 32:
                raise H264ParseError("an Exp-Golomb code longer than the standard allows")
        return (1 << zeros) - 1 + (self.u(zeros) if zeros else 0)

    def se(self) -> int:
        value = self.ue()
        return (value + 1) // 2 if value & 1 else -(value // 2)


@dataclass(frozen=True)
class Sps:
    """What passthrough reads from a sequence parameter set. `width` and
    `height` are the picture as a decoder shows it, after cropping: 1080 and
    not the 1088 the macroblock grid has."""

    profile_idc: int
    level_idc: int
    sps_id: int
    width: int
    height: int


def _skip_scaling_list(bits: _Bits, size: int) -> None:
    last, following = 8, 8
    for _ in range(size):
        if following != 0:
            following = (last + bits.se() + 256) % 256
        last = last if following == 0 else following


def parse_sps(nal: bytes) -> Sps:
    """Profile, level and picture size of an SPS (7.3.2.1.1 and equation
    7-18 onward for the size). Everything else in it is skipped, field by
    field, because the size sits behind fields of variable length."""
    if nal_type(nal) != NAL_SPS:
        raise H264ParseError("not a sequence parameter set")
    if len(nal) > PARAMETER_SET_MAX_BYTES:
        raise H264ParseError("a sequence parameter set of {} bytes".format(len(nal)))
    bits = _Bits(_rbsp(nal[1:]))
    profile_idc = bits.u(8)
    bits.u(8)  # the constraint flags and two reserved bits
    level_idc = bits.u(8)
    sps_id = bits.ue()
    if sps_id > SPS_ID_MAX:
        raise H264ParseError("seq_parameter_set_id out of range")

    chroma_format_idc = 1
    separate_colour_plane = 0
    if profile_idc in _PROFILES_WITH_CHROMA_INFO:
        chroma_format_idc = bits.ue()
        if chroma_format_idc > 3:
            raise H264ParseError("chroma_format_idc out of range")
        if chroma_format_idc == 3:
            separate_colour_plane = bits.u(1)
        bits.ue()  # bit_depth_luma_minus8
        bits.ue()  # bit_depth_chroma_minus8
        bits.u(1)  # qpprime_y_zero_transform_bypass_flag
        if bits.u(1):  # seq_scaling_matrix_present_flag
            for index in range(8 if chroma_format_idc != 3 else 12):
                if bits.u(1):
                    _skip_scaling_list(bits, 16 if index < 6 else 64)

    bits.ue()  # log2_max_frame_num_minus4
    pic_order_cnt_type = bits.ue()
    if pic_order_cnt_type == 0:
        bits.ue()  # log2_max_pic_order_cnt_lsb_minus4
    elif pic_order_cnt_type == 1:
        bits.u(1)  # delta_pic_order_always_zero_flag
        bits.se()  # offset_for_non_ref_pic
        bits.se()  # offset_for_top_to_bottom_field
        cycle = bits.ue()
        if cycle > 255:
            raise H264ParseError("num_ref_frames_in_pic_order_cnt_cycle out of range")
        for _ in range(cycle):
            bits.se()
    elif pic_order_cnt_type != 2:
        raise H264ParseError("pic_order_cnt_type out of range")

    bits.ue()  # max_num_ref_frames
    bits.u(1)  # gaps_in_frame_num_value_allowed_flag
    width_in_mbs = bits.ue() + 1
    height_in_map_units = bits.ue() + 1
    frame_mbs_only = bits.u(1)
    if not frame_mbs_only:
        bits.u(1)  # mb_adaptive_frame_field_flag
    bits.u(1)  # direct_8x8_inference_flag

    crop_left = crop_right = crop_top = crop_bottom = 0
    if bits.u(1):  # frame_cropping_flag
        crop_left, crop_right = bits.ue(), bits.ue()
        crop_top, crop_bottom = bits.ue(), bits.ue()

    # The cropping fields count in chroma samples; how many luma samples one
    # is worth depends on the chroma format, and vertically on whether the
    # stream codes fields (table 6-1, equations 7-19 to 7-22).
    field_factor = 2 - frame_mbs_only
    if chroma_format_idc == 0 or separate_colour_plane:
        crop_unit_x, crop_unit_y = 1, field_factor
    else:
        crop_unit_x = 1 if chroma_format_idc == 3 else 2
        crop_unit_y = (2 if chroma_format_idc == 1 else 1) * field_factor

    width = width_in_mbs * 16 - crop_unit_x * (crop_left + crop_right)
    height = field_factor * height_in_map_units * 16 - crop_unit_y * (crop_top + crop_bottom)
    if width <= 0 or height <= 0:
        raise H264ParseError("the SPS crops away the whole picture")
    return Sps(profile_idc=profile_idc, level_idc=level_idc, sps_id=sps_id, width=width, height=height)


def pps_id(nal: bytes) -> int:
    """`pic_parameter_set_id` of a PPS -- the key a source's several picture
    parameter sets are told apart by."""
    if nal_type(nal) != NAL_PPS:
        raise H264ParseError("not a picture parameter set")
    if len(nal) > PARAMETER_SET_MAX_BYTES:
        raise H264ParseError("a picture parameter set of {} bytes".format(len(nal)))
    value = _Bits(_rbsp(nal[1:9])).ue()
    if value > PPS_ID_MAX:
        raise H264ParseError("pic_parameter_set_id out of range")
    return value


def slice_type(nal: bytes) -> int:
    """One of `SLICE_P`, `SLICE_B`, `SLICE_I`, `SLICE_SP`, `SLICE_SI` for a
    slice NAL unit (types 1 and 5). Only the first two fields of the slice
    header are read (7.3.3): `first_mb_in_slice` and `slice_type`."""
    if nal_type(nal) not in (NAL_SLICE, NAL_IDR):
        raise H264ParseError("not a slice")
    # Two Exp-Golomb codes fit into sixteen bytes with room to spare; the
    # escape bytes are taken out of that much and no more.
    bits = _Bits(_rbsp(nal[1:17]))
    bits.ue()  # first_mb_in_slice
    value = bits.ue()
    if value > 9:
        raise H264ParseError("slice_type out of range")
    return value % 5


@dataclass(frozen=True)
class AccessUnit:
    """One access unit, summed up from its NAL unit headers.

    `nals` are the units in the order they arrived, without start codes.
    `sps` is the last SPS among them and `pps` every PPS, or `None` and `()`:
    most access units carry neither. `has_slice` is false for parameter sets
    that travel alone."""

    nals: Tuple[bytes, ...]
    sps: Optional[bytes]
    pps: Tuple[bytes, ...]
    is_idr: bool
    has_slice: bool
    has_b_slice: bool


def inspect_access_unit(data: bytes, max_nal_units: Optional[int] = None) -> AccessUnit:
    """Splits one access unit and reads what passthrough needs to know about
    it. Raises `H264ParseError` for bytes that are not Annex B, a unit with
    the forbidden bit set, a slice header cut short, and data partitioning
    (NAL types 2 to 4): no browser decodes it, and it is what the slices of
    an H.265 stream look like when somebody labels them H.264. With
    `max_nal_units`, raises `TooManyNalUnits` for a unit that holds more."""
    nals = tuple(split_nal_units(data, max_nal_units))
    sps: Optional[bytes] = None
    pps: List[bytes] = []
    is_idr = has_slice = has_b_slice = False
    for nal in nals:
        kind = nal_type(nal)
        if kind == NAL_SPS:
            sps = nal
        elif kind == NAL_PPS:
            pps.append(nal)
        elif kind in (NAL_SLICE, NAL_IDR):
            has_slice = True
            if kind == NAL_IDR:
                is_idr = True
            if slice_type(nal) == SLICE_B:
                has_b_slice = True
        elif kind in (2, 3, 4):
            raise H264ParseError("data-partitioned slices are not supported")
    return AccessUnit(
        nals=nals, sps=sps, pps=tuple(pps),
        is_idr=is_idr, has_slice=has_slice, has_b_slice=has_b_slice,
    )
