# SPDX-License-Identifier: Apache-2.0
"""`tools/encoder-proof/`: the tool that proves an encoder chain on the
machine that has the hardware.

This machine has none, so what is tested is the instrument: that it passes
the software stand-in, that it says "not here" for the chains whose hardware
is missing, and — the part that matters — that it **fails** a chain that runs
and misses a requirement. A proof that cannot fail proves nothing; each
requirement that can be broken with the elements in the test image is broken
here once.
"""
import importlib.util
import io
import pathlib

import pytest

from fleetless_bridge.live_encoder import STAND_IN
from test_live_encoder_pipeline import _variant

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "encoder_proof", ROOT / "tools" / "encoder-proof" / "encoder_proof.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


proof = _load()

SMALL = ["--width", "320", "--height", "240", "--frames", "45", "--keyframe-every", "15",
         "--bitrate-kbps", "1500"]


def _run(*argv):
    out = io.StringIO()
    code = proof.main(list(argv) + SMALL, out=out)
    return code, out.getvalue()


def _prove(chain, **changes):
    out = io.StringIO()
    stream = dict(width=320, height=240, fps=15, frames=45, keyframe_every=15, bitrate_kbps=1500)
    stream.update(changes)
    return proof.prove(chain, proof.argparse.Namespace(**stream), out), out.getvalue()


def test_the_stand_in_passes_every_check():
    code, said = _run("--backend", "stand-in")
    assert code == 0, said
    assert "VERDICT: PASS (stand-in/x264)" in said
    assert "FAIL" not in said
    for line in [
        "ok   45 frames in, 45 access units out, each decoded to its own picture",
        "ok   the first frame is a keyframe",
        "ok   every frame a keyframe was asked for is one (asked at [15, 30], got [0, 15, 30])",
        "ok   no keyframe nobody asked for",
        "the stream names its colours: matrix 6 (BT.601), primaries 6, transfer 6, limited range",
        "ok   the colours are right when shown the way the stream says",
        "ok   the bitrate follows the target",
        "ok   the bitrate is in the target's unit",
        "ok   the pipeline goes back to NULL",
        "encodes with stand-in/x264",
    ]:
        assert line in said, line
    # Timings and measured bitrates are in the report, and what the stream
    # says it is.
    assert "built and started in" in said and "per frame: median" in said and "measured:" in said
    assert (
        "the stream is H.264 constrained baseline (profile_idc 66, constraint flags 0xc0), "
        "level 2.0"
    ) in said
    assert "note:" not in said


def test_a_backend_whose_hardware_is_missing_is_exit_2_and_says_what_is_missing():
    code, said = _run("--backend", "nvenc")
    assert code == 2
    assert "chain nvenc/jetson:" in said and "chain nvenc/desktop:" in said
    assert "not on this machine: GStreamer has no element 'nvvidconv'" in said
    assert "not on this machine: GStreamer has no element 'nvh264enc'" in said
    assert "VERDICT: no chain of nvenc can be built on this machine" in said
    # And what the bridge does about it, in the bridge's own words.
    assert "falls back to software H.264" in said
    assert "live.encoder=nvenc: no hardware encoder could be used" in said


def test_auto_goes_through_every_chain_in_the_bridge_s_order():
    code, said = _run("--backend", "auto")
    assert code == 2
    names = ["nvenc/jetson", "nvenc/desktop", "vaapi/va", "vaapi/vaapi", "v4l2"]
    positions = [said.index("chain {}:".format(name)) for name in names]
    assert positions == sorted(positions)


def test_one_chain_can_be_named_and_an_unknown_one_is_refused():
    code, said = _run("--backend", "nvenc", "--chain", "nvenc/desktop")
    assert code == 2
    assert "chain nvenc/desktop:" in said and "chain nvenc/jetson:" not in said
    with pytest.raises(SystemExit):
        proof.main(["--backend", "nvenc", "--chain", "no-such-chain"], out=io.StringIO())


def test_a_chain_that_holds_frames_back_fails_the_proof():
    """An encoder with lookahead: frames go in and nothing comes out in
    time. The stand-in without its low-latency settings is one."""
    result, said = _prove(_variant(name="stand-in/lookahead", settings=()))
    assert result == "fail"
    assert "FAIL frame 0: an access unit for the frame (no access unit within 1 s of a frame)" in said
    assert "=> FAIL" in said


def test_a_chain_that_draws_the_wrong_picture_fails_the_proof():
    """The failure nothing else catches: the stream is fine and the colours
    are not. Here the two chroma planes are read in the wrong order."""
    swapped = _variant(
        name="stand-in/swapped-planes",
        convert="rawvideoparse use-sink-caps=false format=yv12 width=320 height=240 "
        "framerate=15/1 ! videoconvert",
        elements=("rawvideoparse", "videoconvert", "x264enc"),
    )
    result, said = _prove(swapped)
    assert result == "fail"
    assert "FAIL frame 0: the picture that comes out is the one that went in" in said


def test_a_chain_that_names_the_wrong_colours_fails_the_proof():
    """The stream decodes, the pictures match by the decoder library's own
    conversion, and a viewer shows every colour wrong: BT.601 samples under
    a BT.709 label. The chain here forgets what it was told about the
    colours, and anything of more than 576 lines is then taken for BT.709."""
    relabelled = _variant(
        name="stand-in/relabelled",
        convert="rawvideoparse use-sink-caps=false format=i420 width=320 height=608 "
        "framerate=15/1 ! videoconvert",
        elements=("rawvideoparse", "videoconvert", "x264enc"),
    )
    result, said = _prove(relabelled, height=608)
    assert result == "fail"
    assert "the stream names its colours: matrix 1 (BT.709)" in said
    assert (
        "FAIL the colours are right when shown the way the stream says "
        "(the bar that went in as (200, 30, 200) is shown as (2"
    ) in said
    # Every check before it passed: the picture check cannot see this.
    assert "ok   45 frames in, 45 access units out, each decoded to its own picture" in said
    # The same size through the stand-in itself, which says what it carries.
    result, said = _prove(STAND_IN, height=608)
    assert result == "pass" and "matrix 6 (BT.601)" in said


def test_a_chain_that_converts_its_colours_and_says_so_passes_the_proof():
    """The check follows the label, it does not insist on BT.601: a chain
    that really converts to BT.709 names BT.709 and is right."""
    converted = _variant(
        name="stand-in/converted", convert="videoconvert ! video/x-raw,format=I420,colorimetry=bt709"
    )
    result, said = _prove(converted)
    assert result == "pass", said
    assert "the stream names its colours: matrix 1 (BT.709)" in said


def test_the_parameter_set_is_read_as_a_receiver_reads_it():
    """Against `h264parse`, for the stand-in's baseline stream, and for a
    high-profile stream with every colour field set, which takes the other
    branch through the syntax."""
    import av
    import numpy as np
    from fractions import Fraction

    from fleetless_bridge.live_encoder import Pipeline
    from test_live_encoder_pipeline import _what_the_stream_says, moving_frame

    pipeline = Pipeline(STAND_IN, width=1024, height=768, fps=15, bitrate_bps=2_000_000)
    try:
        unit = pipeline.encode(moving_frame(0, 1024, 768), False)
    finally:
        pipeline.close()
    said = proof.sps_of(unit)
    theirs = _what_the_stream_says(unit)
    assert said == {
        "profile_idc": 66, "constraints": 0xC0, "level_idc": 31,
        "colours": (6, 6, 6), "full_range": False,
    }
    assert (theirs.get_string("profile"), theirs.get_string("level")) == ("constrained-baseline", "3.1")
    assert theirs.get_string("colorimetry") == "bt601"

    encoder = av.CodecContext.create("libx264", "w")
    encoder.width, encoder.height, encoder.pix_fmt = 320, 240, "yuv420p"
    encoder.time_base = Fraction(1, 15)
    encoder.options = {
        "profile": "high",
        "x264-params": "colorprim=bt709:transfer=bt709:colormatrix=bt709:fullrange=on:cqm=jvt",
    }
    frame = av.VideoFrame.from_ndarray(np.zeros((240, 320, 3), np.uint8), format="rgb24").reformat(
        format="yuv420p"
    )
    frame.pts = 0
    packets = list(encoder.encode(frame)) + list(encoder.encode(None))
    high = proof.sps_of(b"".join(bytes(packet) for packet in packets))
    assert high is not None and high["profile_idc"] == 100
    assert high["colours"] == (1, 1, 1) and high["full_range"] is True

    # A stream without a parameter set, and one that ends early.
    assert proof.sps_of(b"\x00\x00\x00\x01\x65picture") is None
    assert proof.sps_of(b"\x00\x00\x00\x01\x67\x42") is None
    # The bytes that keep start codes out of a unit are not part of it.
    assert proof._payload(b"\x67\x00\x00\x03\x01\x00\x00\x03\x00") == b"\x00\x00\x01\x00\x00\x00"


def test_a_colour_is_converted_by_the_matrix_and_the_range_it_is_given():
    """The numbers of the finding this check exists for: (200, 30, 30) is
    85.42, 102.80, 202.67 in BT.601, and a viewer told BT.709 shows that
    as (215, 46, 28)."""
    as_bt601 = proof.as_a_viewer_shows_it(85.42, 102.80, 202.67, 6, False)
    as_bt709 = proof.as_a_viewer_shows_it(85.42, 102.80, 202.67, 1, False)
    assert [round(channel) for channel in as_bt601] == [200, 30, 30]
    assert [round(channel) for channel in as_bt709] == [215, 46, 28]
    # Limited-range grey read as full range is darker at the top and lighter
    # at the bottom.
    assert round(proof.as_a_viewer_shows_it(218.0, 128.0, 128.0, 6, False)[0]) == 235
    assert round(proof.as_a_viewer_shows_it(218.0, 128.0, 128.0, 6, True)[0]) == 218


def test_a_chain_that_does_not_follow_the_bitrate_fails_the_proof():
    """A chain whose bitrate property does nothing: it names a property that
    is not the encoder's, so every target is the element's own default."""
    deaf = _variant(name="stand-in/deaf", bitrate=("no-such-bitrate-property", "{kbps}"))
    result, said = _prove(deaf)
    assert result == "fail"
    assert "FAIL the bitrate follows the target" in said
    # Everything before that check passed: it is the bitrate and nothing else.
    assert "ok   45 frames in, 45 access units out" in said


@pytest.mark.parametrize("bitrate", [[], ["--bitrate-kbps", "500"], ["--bitrate-kbps", "150"]],
                         ids=["default-bitrate", "500-kbps", "150-kbps"])
def test_the_command_for_a_robot_s_cameras_passes_the_stand_in(bitrate):
    """The command the pull request gives for the first robot, with the
    stand-in for its chip: 1024x768 at two frames a second. Two seconds of
    that are four frames, which is where the encoder happens to be and not
    where it settles; the bitrate check once failed a working encoder on
    them."""
    out = io.StringIO()
    code = proof.main(
        ["--backend", "stand-in", "--width", "1024", "--height", "768", "--fps", "2"] + bitrate,
        out=out,
    )
    said = out.getvalue()
    assert code == 0, said
    assert "ok   the bitrate follows the target" in said
    assert "VERDICT: PASS (stand-in/x264)" in said


def test_a_stretch_of_the_bitrate_check_is_thirty_frames_at_least(monkeypatch):
    """Counted, at a frame rate where two seconds are far fewer."""
    pushed = []
    real = proof.Pipeline.encode

    def counting(self, frame, force_keyframe):
        pushed.append(self.bitrate_bps)
        return real(self, frame, force_keyframe)

    monkeypatch.setattr(proof.Pipeline, "encode", counting)
    result, said = _prove(STAND_IN, fps=2, frames=6, keyframe_every=3, bitrate_kbps=400)
    assert result == "pass", said
    # Six frames of the pattern and four of the bars at the target, then the
    # three stretches.
    assert pushed == [400_000] * (6 + 4 + 30) + [100_000] * 30 + [400_000] * 30
    # And two seconds of frames where that is more.
    pushed.clear()
    result, said = _prove(STAND_IN, fps=20, frames=6, keyframe_every=3, bitrate_kbps=400)
    assert result == "pass", said
    assert pushed == [400_000] * (6 + 4 + 40) + [100_000] * 40 + [400_000] * 40


def test_a_chain_that_takes_the_bitrate_in_another_unit_fails_the_proof():
    """It shrinks and grows in step with the target, a hundred times too
    large: the check that the bitrate follows cannot see that."""
    wrong_unit = _variant(name="stand-in/wrong-unit", bitrate=("bitrate", "{kbps}00"))
    result, said = _prove(wrong_unit, bitrate_kbps=30)
    assert result == "fail"
    assert "ok   the bitrate follows the target" in said
    assert "FAIL the bitrate is in the target's unit" in said
    assert "at a target of 30 kbps is more than a factor of 8 off" in said


def test_a_profile_the_offer_does_not_promise_is_said_and_fails_nothing():
    """The stand-in allowed the high profile and given a preset that uses
    what only that profile has."""
    assert proof.profile_of({"profile_idc": 66, "constraints": 0xC0}) == ("constrained baseline", True)
    assert proof.profile_of({"profile_idc": 66, "constraints": 0x00}) == ("baseline", True)
    assert proof.profile_of({"profile_idc": 77, "constraints": 0x80}) == ("main", True)
    assert proof.profile_of({"profile_idc": 100, "constraints": 0x00}) == ("high", False)
    assert proof.profile_of({"profile_idc": 244, "constraints": 0x00}) == ("profile 244", False)

    higher = _variant(
        name="stand-in/high",
        caps="video/x-h264,profile=(string)high",
        settings=(("tune", "zerolatency"), ("speed-preset", "veryfast"), ("key-int-max", "{gop}")),
    )
    result, said = _prove(higher)
    assert result == "pass", said
    assert "the stream is H.264 high (profile_idc 100, " in said
    assert "note: a live session is offered as baseline." in said


def test_a_failing_chain_makes_the_verdict_fail_whatever_else_passes(monkeypatch):
    monkeypatch.setattr(
        proof, "chains_for",
        lambda backend: (_variant(name="stand-in/lookahead", settings=()), STAND_IN),
    )
    code, said = _run("--backend", "v4l2")
    assert code == 1
    assert "VERDICT: FAIL (stand-in/lookahead)" in said
    assert "chain stand-in/x264:" in said and said.count("=> PASS") == 1


def test_the_proof_runs_the_shipped_pipeline_class():
    """A harness that reimplements what it proves, proves the harness."""
    from fleetless_bridge import live_encoder

    assert proof.Pipeline is live_encoder.Pipeline
    assert proof.HardwareH264Encoder is live_encoder.HardwareH264Encoder
    assert proof.chains_for is live_encoder.chains_for
