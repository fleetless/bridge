# SPDX-License-Identifier: Apache-2.0
"""The bitstream reader: NAL units out of Annex B, their types, the SPS fields
passthrough needs, and the slice type. Streams are encoded here with libx264,
so every number asserted below is one an encoder produced, not one typed in.

No rclpy, no aiortc: `h264.py` imports neither.
"""
import pytest

import h264_streams as streams
from fleetless_bridge import h264


def _first_slice(unit, nal_type):
    for nal in h264.split_nal_units(unit):
        if h264.nal_type(nal) == nal_type:
            return nal
    raise AssertionError("no NAL unit of type {} in this access unit".format(nal_type))


# --- splitting ---------------------------------------------------------------


def test_nal_units_are_split_at_three_and_four_byte_start_codes():
    a, b, c = b"\x67\x42\x00\x1f", b"\x68\xce\x3c\x80", b"\x65\x88\x84\x21"
    data = b"\x00\x00\x00\x01" + a + b"\x00\x00\x01" + b + b"\x00\x00\x00\x01" + c
    assert h264.split_nal_units(data) == [a, b, c]


def test_zero_bytes_before_a_start_code_belong_to_it_not_to_the_unit():
    """Annex B allows any number of zero bytes before a start code
    (`leading_zero_8bits`, `trailing_zero_8bits`). A NAL unit never ends in a
    zero byte, so they are padding and not payload."""
    a, b = b"\x67\x42\x00\x1f", b"\x65\x88\x84\x21"
    data = b"\x00\x00\x00\x00\x00\x01" + a + b"\x00\x00\x00\x00\x00\x01" + b + b"\x00\x00"
    assert h264.split_nal_units(data) == [a, b]


def test_emulation_prevention_bytes_stay_in_the_unit():
    """`00 00 03` inside a NAL unit is the escape that keeps a start code from
    appearing in the payload. It is part of what is forwarded, so the splitter
    must leave it alone and must not mistake it for a boundary."""
    nal = b"\x65\x00\x00\x03\x01\x00\x00\x03\x00\x80"
    assert h264.split_nal_units(b"\x00\x00\x00\x01" + nal) == [nal]


def test_splitting_reproduces_what_an_encoder_wrote():
    for unit in streams.encode_stream(frames=6, gop=3):
        nals = h264.split_nal_units(unit)
        assert nals == streams.split(unit)
        # Joined again, the units decode to the same pictures: nothing was
        # lost at a boundary.
        assert h264.split_nal_units(h264.join_annexb(nals)) == nals


@pytest.mark.parametrize("data", [
    b"",
    b"\x00\x00\x00",
    b"not a byte stream at all",
    # AVCC, the length-prefixed form an MP4 carries: four length bytes, no
    # start code anywhere.
    b"\x00\x00\x00\x04\x67\x42\x00\x1f",
    # Bytes before the first start code that are not zero padding.
    b"\xff\xd8\xff\xe0\x00\x00\x01\x67\x42",
])
def test_bytes_that_are_not_annex_b_raise(data):
    with pytest.raises(h264.H264ParseError):
        h264.split_nal_units(data)


def test_a_unit_with_the_forbidden_bit_set_is_not_h264():
    with pytest.raises(h264.H264ParseError):
        h264.nal_type(b"\xe5\x88")
    with pytest.raises(h264.H264ParseError):
        h264.nal_type(b"")


# --- types -------------------------------------------------------------------


def test_nal_types_of_a_keyframe_and_of_a_frame_that_depends_on_one():
    units = streams.encode_stream(frames=4, gop=4)
    keyframe = [h264.nal_type(nal) for nal in h264.split_nal_units(units[0])]
    assert h264.NAL_SPS in keyframe and h264.NAL_PPS in keyframe and h264.NAL_IDR in keyframe
    assert keyframe.index(h264.NAL_SPS) < keyframe.index(h264.NAL_PPS) < keyframe.index(h264.NAL_IDR)
    assert [h264.nal_type(nal) for nal in h264.split_nal_units(units[1])] == [h264.NAL_SLICE]


# --- the SPS -----------------------------------------------------------------


@pytest.mark.parametrize("profile, profile_idc", [("baseline", 66), ("main", 77), ("high", 100)])
def test_the_sps_gives_profile_and_size(profile, profile_idc):
    units = streams.encode_stream(width=64, height=48, frames=1, profile=profile)
    sps = h264.parse_sps(_first_slice(units[0], h264.NAL_SPS))
    assert sps.profile_idc == profile_idc
    assert (sps.width, sps.height) == (64, 48)


@pytest.mark.parametrize("profile", ["baseline", "main", "high"])
@pytest.mark.parametrize("width, height", [(100, 50), (1280, 720), (1920, 1080), (322, 242)])
def test_the_size_is_the_cropped_one_not_the_macroblock_grid(profile, width, height):
    """H.264 codes whole 16x16 macroblocks and says in the SPS how much of the
    last row and column to cut off. 1920x1080 is coded as 1920x1088; a reader
    that skips the cropping fields refuses every Full HD camera as the wrong
    size."""
    units = streams.encode_stream(width=width, height=height, frames=1, profile=profile)
    sps = h264.parse_sps(_first_slice(units[0], h264.NAL_SPS))
    assert (sps.width, sps.height) == (width, height)


def test_an_sps_cut_short_raises():
    units = streams.encode_stream(frames=1)
    sps = _first_slice(units[0], h264.NAL_SPS)
    with pytest.raises(h264.H264ParseError):
        h264.parse_sps(sps[:4])
    with pytest.raises(h264.H264ParseError):
        h264.parse_sps(_first_slice(units[0], h264.NAL_PPS))  # not an SPS at all


# --- slice types and B-frames ------------------------------------------------


def test_slice_types_of_a_stream_without_b_frames():
    units = streams.encode_stream(frames=8, gop=4, bframes=0)
    kinds = []
    for unit in units:
        info = h264.inspect_access_unit(unit)
        assert not info.has_b_slice
        nal = _first_slice(unit, h264.NAL_IDR if info.is_idr else h264.NAL_SLICE)
        kinds.append(h264.slice_type(nal))
    assert kinds == [h264.SLICE_I, h264.SLICE_P, h264.SLICE_P, h264.SLICE_P] * 2


def test_b_frames_are_found_by_the_slice_type():
    units = streams.encode_stream(frames=12, gop=12, bframes=2, profile="high")
    infos = [h264.inspect_access_unit(unit) for unit in units]
    assert infos[0].is_idr and not infos[0].has_b_slice
    assert any(info.has_b_slice for info in infos), "the test stream carries no B-frame"
    # A B-frame is not a keyframe and a keyframe is never a B-frame.
    assert not any(info.is_idr and info.has_b_slice for info in infos)
    # The first B-slice is not the first unit after the keyframe: in decode
    # order the P-frame it refers to comes first. A reader that judged a
    # stream by its first few units would pass this one.
    first_b = next(index for index, info in enumerate(infos) if info.has_b_slice)
    assert first_b >= 2


# --- one access unit, summed up ----------------------------------------------


def test_an_access_unit_is_summed_up_without_decoding_it():
    units = streams.encode_stream(width=64, height=48, frames=3, gop=3)
    keyframe = h264.inspect_access_unit(units[0])
    assert keyframe.is_idr and keyframe.has_slice
    assert h264.parse_sps(keyframe.sps).width == 64
    assert len(keyframe.pps) == 1
    assert list(keyframe.nals) == streams.split(units[0])

    dependent = h264.inspect_access_unit(units[1])
    assert not dependent.is_idr and dependent.has_slice
    assert dependent.sps is None and dependent.pps == ()


def test_parameter_sets_alone_are_a_unit_without_a_picture():
    """What RTSP's SDP carries, and what some sources send once at the
    beginning: SPS and PPS with no slice."""
    sps, pps = streams.parameter_sets(streams.encode_stream(frames=1)[0])
    info = h264.inspect_access_unit(streams.join([sps, pps]))
    assert info.sps == sps and info.pps == (pps,)
    assert not info.has_slice and not info.is_idr


def test_h265_is_not_mistaken_for_h264():
    """An H.265 unit starts with the same start codes and a different
    two-byte header. Read as H.264 its units are of types no H.264 picture
    has, so it must come out as a unit without a picture or as an error --
    never as a keyframe."""
    for unit in streams.encode_stream(frames=3, gop=3, codec="libx265"):
        try:
            info = h264.inspect_access_unit(unit)
        except h264.H264ParseError:
            continue
        assert not info.is_idr
