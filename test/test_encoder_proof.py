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


def _prove(chain):
    out = io.StringIO()
    args = proof.argparse.Namespace(
        width=320, height=240, fps=15, frames=45, keyframe_every=15, bitrate_kbps=1500
    )
    return proof.prove(chain, args, out), out.getvalue()


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
        "ok   the bitrate follows the target",
        "ok   the pipeline goes back to NULL",
        "encodes with stand-in/x264",
    ]:
        assert line in said, line
    # Timings and measured bitrates are in the report.
    assert "built and started in" in said and "per frame: median" in said and "measured:" in said


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


def test_a_chain_that_does_not_follow_the_bitrate_fails_the_proof():
    """A chain whose bitrate property does nothing: it names a property that
    is not the encoder's, so every target is the element's own default."""
    deaf = _variant(name="stand-in/deaf", bitrate=("no-such-bitrate-property", "{kbps}"))
    result, said = _prove(deaf)
    assert result == "fail"
    assert "FAIL the bitrate follows the target" in said
    # Everything before that check passed: it is the bitrate and nothing else.
    assert "ok   45 frames in, 45 access units out" in said


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
