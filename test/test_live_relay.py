# SPDX-License-Identifier: Apache-2.0
"""3c.4: TURN/relay forcing, quality-under-a-real-server, and a soak -- the
three things `test_live.py`'s fakes cannot prove, because all three are about
what a real LiveKit server, `coturn` relay and browser viewer actually do, not
what this package's own code decides given a payload it was handed.

**Why this file drives `docker` itself rather than running inside
`run-tests.sh`'s own container.** Every other test in this suite runs inside
the dev image `run-tests.sh` builds, which has no `docker` binary or socket --
adding one would bake a docker-in-docker dependency into `Dockerfile.dev` for
three tests nothing else needs, and that file is 3c.5's. Instead these tests
follow `tools/live-proof/run-proof.sh`'s shape, established in 3c.3: a
host-side script starts the publisher inside the dev image (`--network host`,
so it can reach the dev LiveKit) and drives any needed viewer or relay from
the host, using the `docker` and `node`/Playwright `run-proof.sh` already
needs. This file is the pytest wrapper around that pattern --
`python3 -m pytest test/test_live_relay.py -m slow`, run directly on a host
with `docker`, `node`/Playwright and the dev LiveKit reachable, not through
`run-tests.sh`.

**Collected everywhere, useful only where its preconditions hold.** Every
test here checks its own preconditions (`docker` on `PATH`, the dev image
built, the dev LiveKit answering on `:7880`) and skips with the reason if any
is missing -- the same shape as `test_contracts_sync.py`'s
`FLEETLESS_CONTRACTS_DIR` gate. Inside `run-tests.sh`'s container every test
here skips in well under a second (`docker` is simply not on `PATH` there),
which keeps `./run-tests.sh` fast without marker-based deselection. `slow` is
still registered as a marker (`conftest.py`) and applied here, so
`pytest -m slow` picks these out on a host that *can* run them.
"""
import os
import re
import shutil
import socket
import subprocess
import warnings

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
LIVE_PROOF = os.path.join(REPO, "tools", "live-proof")
DISTRO = os.environ.get("FLEETLESS_LIVE_DISTRO", "humble")
IMAGE = "fleetless-bridge-dev-{}".format(DISTRO)


def _docker_available():
    return shutil.which("docker") is not None


def _image_built():
    if not _docker_available():
        return False
    return subprocess.run(
        ["docker", "image", "inspect", IMAGE],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0


def _livekit_reachable(host="localhost", port=7880, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _skip_reason():
    """`None` if every precondition holds, else the reason to skip -- one
    function so every test answers the same missing precondition the same
    way, instead of each spelling its own subset and drifting apart."""
    if not _docker_available():
        return "docker is not on PATH -- these tests drive their own containers from the host"
    if not _image_built():
        return "{} is not built; run ./run-tests.sh --distro {} first".format(IMAGE, DISTRO)
    if not _livekit_reachable():
        return "the dev LiveKit is not reachable at localhost:7880"
    return None


def _run(script, *args, timeout_s):
    return subprocess.run(
        [os.path.join(LIVE_PROOF, script), *args],
        cwd=LIVE_PROOF, capture_output=True, text=True, timeout=timeout_s,
    )


#: aioice's own INFO line for one losing ICE candidate pair -- part of every
#: SUCCESSFUL connection, not a failure. No "HARNESS" prefix, unlike anything
#: publish.py's own `log()` prints. Recorded verbatim from a real publisher.log
#: (see run-proof.sh/run-quality-proof.sh's own comment).
_AIOICE_FROZEN_TO_FAILED = (
    "18:12:26.304 INFO aioice.ice: Connection(0) Check "
    "CandidatePair(('10.1.0.9', 40544) -> ('10.1.0.9', 7882)) "
    "State.FROZEN -> State.FAILED\n"
)
_HARNESS_CONNECTED = "18:16:00 HARNESS ICE/DTLS CONNECTED\n"
_HARNESS_FAILED = "18:16:00 HARNESS FAILED: never reached connected\n"


def _grep_q(pattern, path):
    return subprocess.run(["grep", "-q", pattern, path]).returncode == 0


def test_the_wait_loops_own_grep_does_not_break_on_aioices_ordinary_pruning(tmp_path):
    # No docker, no LiveKit: this drives the exact two grep conditions
    # run-proof.sh and run-quality-proof.sh poll with, against a log frozen
    # at the moment the review measured -- one aioice FROZEN->FAILED line,
    # no HARNESS line yet. Neither condition may fire, or the publisher gets
    # reported as failed while it is about to connect.
    log = tmp_path / "publisher.log"
    log.write_text(_AIOICE_FROZEN_TO_FAILED)
    assert not _grep_q("ICE/DTLS CONNECTED", str(log))
    assert not _grep_q("HARNESS FAILED", str(log)), (
        "the fixed pattern must not fire on aioice's own pruning line"
    )
    # The pattern this fix replaces would have: proves the break is real, not
    # a strawman.
    assert _grep_q("FAILED", str(log)), (
        "aioice's line really does contain the bare word 'FAILED' -- "
        "otherwise the defect this fixes could never have fired"
    )

    # The publisher connects a moment later: the loop's other condition must
    # now fire.
    with log.open("a") as f:
        f.write(_HARNESS_CONNECTED)
    assert _grep_q("ICE/DTLS CONNECTED", str(log))


def test_the_wait_loops_own_grep_still_catches_a_real_harness_failure(tmp_path):
    # The other direction: a genuine harness-reported failure (publish.py's
    # own `log("FAILED: ...")`, always prefixed "HARNESS") must still be
    # caught, so the fix does not trade a false failure for a missed one.
    log = tmp_path / "publisher.log"
    log.write_text(_AIOICE_FROZEN_TO_FAILED + _HARNESS_FAILED)
    assert not _grep_q("ICE/DTLS CONNECTED", str(log))
    assert _grep_q("HARNESS FAILED", str(log))


def test_every_run_proof_script_starts_docker_containers_through_dollar_docker():
    # run-relay-proof.sh sets DOCKER="sudo docker" when the daemon needs
    # sudo (lines 30-31) but started coturn and the probe container with a
    # bare `docker run`/`docker rm` in two of three places -- so on a
    # sudo-docker host coturn came up and the probe could not start. Every
    # actual container command must go through $DOCKER; `docker info` (the
    # one line that decides which $DOCKER to use) is the sole exception.
    for script in ("run-proof.sh", "run-quality-proof.sh", "run-soak.sh", "run-relay-proof.sh"):
        for lineno, line in enumerate(
            open(os.path.join(LIVE_PROOF, script)).read().splitlines(), start=1
        ):
            stripped = line.strip()
            if stripped.startswith("#") or "docker info" in stripped:
                continue
            for verb in ("docker run", "docker rm", "docker image"):
                assert not stripped.startswith(verb), (
                    "{}:{} starts a container command with a bare '{}', "
                    "bypassing $DOCKER: {}".format(script, lineno, verb, line)
                )


def _quality_correlation_script():
    """The embedded python3 heredoc in run-quality-proof.sh, extracted the
    way this suite already extracts a function from a shell script -- so the
    pause/resume bounds below can be tested against synthetic logs without a
    real LiveKit, browser, or the 45-70s a live run costs."""
    text = open(os.path.join(LIVE_PROOF, "run-quality-proof.sh")).read()
    body = re.search(r"^python3 - .*<<'PYEOF'\n(.*)\nPYEOF$", text, re.M | re.S)
    assert body, "run-quality-proof.sh's embedded python3 heredoc moved or was renamed"
    return body.group(1)


def _run_quality_correlation(tmp_path, log_lines, disconnect_ms, reconnect_ms, t0_host=0.0):
    script = tmp_path / "correlate.py"
    script.write_text(_quality_correlation_script())
    log = tmp_path / "publisher.log"
    log.write_text("\n".join(log_lines) + "\n")
    return subprocess.run(
        ["python3", str(script), str(log), str(t0_host), str(disconnect_ms), str(reconnect_ms)],
        capture_output=True, text=True,
    )


def test_quality_correlation_passes_on_the_reviews_own_recorded_numbers(tmp_path):
    # The measurement test_quality_pauses_when_the_viewer_disconnects_and_
    # resumes_when_it_returns's own docstring cites: paused 4.568s after
    # disconnect, resumed 2.675s before the second viewer's video -- both
    # well inside the bounds below. The control proving they don't reject a
    # real, healthy run.
    result = _run_quality_correlation(
        tmp_path,
        [
            "00:00:00 HARNESS start",
            "00:00:13.568 INFO Live publish paused (subscribed layers: none)",
            "00:00:23.325 INFO Live publish resumed",
        ],
        disconnect_ms=9000, reconnect_ms=26000,
    )
    assert "PASS: pause and resume both observed" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0


def test_quality_correlation_fails_a_pause_that_exceeds_its_bound(tmp_path):
    # The exact failure scenario the review measured -- a 70s run,
    # disconnect at t~9s, but the pause not logged until t=60s. The old
    # condition (offset >= disconnect_s, no upper bound) printed
    # "PASS" for this; the bound below must reject it.
    result = _run_quality_correlation(
        tmp_path,
        [
            "00:00:00 HARNESS start",
            "00:01:00.000 INFO Live publish paused (subscribed layers: none)",
        ],
        disconnect_ms=9000, reconnect_ms=70000,
    )
    assert "FAIL: pause took" in result.stdout, result.stdout + result.stderr
    assert "PASS" not in result.stdout
    assert result.returncode == 1


def test_quality_correlation_fails_a_resume_logged_after_the_bound_instead_of_printing_a_negative(tmp_path):
    # The old resume condition had no upper bound either, and its PASS
    # message printed "reconnect_s - resumed_at" unconditionally -- a resume
    # logged well after the second viewer's video produced a negative number
    # under a message claiming "before". This must FAIL instead, naming what
    # was actually measured.
    result = _run_quality_correlation(
        tmp_path,
        [
            "00:00:00 HARNESS start",
            "00:00:09.500 INFO Live publish paused (subscribed layers: none)",
            "00:00:46.000 INFO Live publish resumed",
        ],
        disconnect_ms=9000, reconnect_ms=26000,
    )
    assert "FAIL: resume logged" in result.stdout, result.stdout + result.stderr
    assert "before the second viewer's video" not in result.stdout
    assert result.returncode == 1


def test_the_scripts_themselves_poll_for_harness_failed_not_bare_failed():
    # The two tests above prove the grep pattern is right in isolation; this
    # proves run-proof.sh and run-quality-proof.sh actually use it, so a
    # future edit reverting to the bare pattern is caught here rather than
    # only in a 1-in-6 flaky live run.
    for script in ("run-proof.sh", "run-quality-proof.sh"):
        text = open(os.path.join(LIVE_PROOF, script)).read()
        assert 'grep -q "HARNESS FAILED"' in text, (
            "{} does not poll for HARNESS FAILED".format(script)
        )
        assert 'grep -q "FAILED"' not in text, (
            "{} still has a bare 'FAILED' poll, which aioice's own pruning "
            "log line matches on every successful connection".format(script)
        )


def _relay_attempt_verdict(result):
    """One of `"pass"`, `"hard_fail"`, or a retry cause string -- the
    classification `test_relay_path_is_selected_when_host_and_srflx_are_blocked`
    acts on, factored out to test against synthetic
    `subprocess.CompletedProcess`-shaped results without docker, coturn or a
    real LiveKit. See that test's own body for what each branch means."""
    if "SELECTED PAIR LOCAL CANDIDATE TYPE: relay" in result.stdout:
        return "pass"
    if "the selected pair is not a relay pair" in result.stdout:
        return "hard_fail"
    if "probe exit=" not in result.stdout:
        return ("run-relay-proof.sh never reached the probe (see stderr below), "
                "not a timing flake")
    if "FAILED: connected, but no nominated pair was ever found" in result.stdout:
        return ("connected, but the nominated-pair read itself failed -- the "
                "private aioice attribute read relay_probe.py's own docstring "
                "flags as fragile, not a DTLS timing issue")
    return "did not reach ICE/DTLS connected before the timeout"


def test_relay_attempt_verdict_tells_a_structural_failure_from_a_timing_one():
    # The catch-all this replaces treated "no image", "coturn never
    # started" and "the fragile nominated-pair read failed" identically to a
    # genuine DTLS timeout, and reported all of them as "timing out before
    # connecting" -- a false statement about the two structural cases. No
    # docker needed: these are the exact stdout shapes run-relay-proof.sh and
    # relay_probe.py produce for each case, read from their own source above.
    CompletedProcess = type("CompletedProcess", (), {})

    def result(stdout, stderr=""):
        r = CompletedProcess()
        r.stdout, r.stderr = stdout, stderr
        return r

    assert _relay_attempt_verdict(result(
        "  probe exit=0\nSELECTED PAIR LOCAL CANDIDATE TYPE: relay\n"
    )) == "pass"
    assert _relay_attempt_verdict(result(
        "  probe exit=5\nFAILED: the selected pair is not a relay pair\n"
    )) == "hard_fail"
    # No image: run-relay-proof.sh's own early exit, before it ever prints
    # "probe exit=" -- the message is on stderr, which stdout-only string
    # matching cannot see at all.
    never_reached = _relay_attempt_verdict(result(
        "=== relay / humble / room=fl-relay-proof-humble-1 ===\n",
        stderr="run-relay-proof.sh: no image fleetless-bridge-dev-humble.\n",
    ))
    assert "never reached the probe" in never_reached
    assert "timing out" not in never_reached
    # relay_probe.py exit 4: connected, but the private aioice read failed.
    fragile_read = _relay_attempt_verdict(result(
        "  probe exit=4\nFAILED: connected, but no nominated pair was ever found\n"
    ))
    assert "nominated-pair read itself failed" in fragile_read
    assert "timing out" not in fragile_read
    # relay_probe.py exit 3: the genuine, documented timing flake.
    assert _relay_attempt_verdict(result(
        "  probe exit=3\nFAILED: never reached connected\n"
    )) == "did not reach ICE/DTLS connected before the timeout"


@pytest.mark.slow
def test_relay_path_is_selected_when_host_and_srflx_are_blocked():
    """`run-relay-proof.sh` creates its own `coturn/coturn` container, points
    `relay_probe.py`'s `connection_factory` at it with host/reflexive
    candidates pruned from both what this side offers AND what it actually
    tries (see that file's own docstring -- offer-only filtering still let
    aiortc win the pair on a *host* candidate, because as the ICE-controlling
    side it pairs every locally gathered socket against the remote's
    candidates independent of what it advertised), and reads the SELECTED
    candidate pair from aioice's own state, not from "it connected".

    **A bounded retry, what it is and is not compensating for, and the honest
    gap in it.** Over roughly twenty runs of the final script against this
    dev topology, spread across sessions with nothing else connected to the
    dev LiveKit and sessions where a second one (the quality proof, the full
    test suite on another distribution, the soak) was also connected -- the
    pass rate has been under half either way, run in streaks as low as
    0-for-5 and as high as 5-for-5 in BOTH conditions. A second connection
    makes it worse on average, but "nothing else connected" is not a
    guarantee of a pass, only a better bet, and this is not a clean
    correlation this test can act on. What has been fully consistent: not one
    attempt, in either condition, has ever connected and selected something
    other than `relay`. The ICE-level forcing is what is reliable; DTLS
    completing inside LiveKit's fixed 10s `CONNECTION_TIMEOUT` over that path
    is what is not.

    **What this cannot rule out, said plainly rather than left for a retry to
    paper over:** a bounded retry that only distinguishes "timed out" from
    "connected on the wrong pair type" cannot tell today's host-load
    flakiness apart from a real regression that made the relay path
    intermittently slower without breaking it outright -- a relay that
    connects only on attempt 2 or 3 is a different state from one that
    connects on attempt 1, and this test currently reports both as the same
    PASS. `warnings.warn` below is the mitigation available now: it does not
    fix that gap, but it stops a first-attempt failure from disappearing
    silently behind a later PASS, so a shift from "occasionally needs a
    retry" to "always needs one" is at least visible in the test's own
    output across repeated runs, for a human to notice rather than for this
    test to catch on its own.
    """
    reason = _skip_reason()
    if reason:
        pytest.skip(reason)

    attempts = []
    causes = []
    for attempt in range(1, 4):
        result = _run("run-relay-proof.sh", DISTRO, timeout_s=90)
        attempts.append(result)
        verdict = _relay_attempt_verdict(result)
        if verdict == "pass":
            if attempt > 1:
                warnings.warn(
                    "relay path selected only on attempt {} of 3 -- see this "
                    "test's own docstring for what a retry here can and "
                    "cannot rule out".format(attempt))
            return  # PASS
        if verdict == "hard_fail":
            pytest.fail(
                "attempt {}: a pair WAS selected but it was not relay -- "
                "the forcing mechanism itself failed, not a timing flake:\n{}".format(
                    attempt, result.stdout[-4000:]))
        # Anything else retries, bounded -- see _relay_attempt_verdict for
        # what each remaining cause means and why they are told apart rather
        # than lumped into one "timing out" catch-all.
        causes.append(verdict)
    pytest.fail(
        "the relay path was not selected in {} attempts. Last attempt's cause: "
        "{}\nstdout:\n{}\nstderr:\n{}".format(
            len(attempts), causes[-1], attempts[-1].stdout[-4000:], attempts[-1].stderr[-2000:]))


@pytest.mark.slow
def test_quality_pauses_when_the_viewer_disconnects_and_resumes_when_it_returns():
    """`run-quality-proof.sh`: a real viewer (the official `livekit-client`,
    driven headlessly -- `tools/live-proof/quality-check.mjs`) connects, sees
    moving video, disconnects; a second viewer connects later and also sees
    moving video. The script correlates the viewer's own disconnect/reconnect
    timestamps against the publisher's `Live publish paused`/`resumed` log
    lines (both re-anchored onto one shared origin -- the container and the
    host do not agree on timezone, see that script's own comment) and prints
    the delay in each direction.

    This is the proof for `add_track`'s `layers` fix and the latch's removal
    (`fleetless_bridge/live.py`): against this dev LiveKit, the publisher
    paused 4.4s after the viewer disconnected -- inside one `pingInterval`
    (5s, read from `join.ping_interval_s` on this dev server) -- and had
    already resumed 2.6s before the second viewer's video decoded.
    """
    reason = _skip_reason()
    if reason:
        pytest.skip(reason)
    if shutil.which("node") is None:
        pytest.skip("node is not on PATH -- the viewer half needs it")

    result = _run("run-quality-proof.sh", DISTRO, timeout_s=150)
    assert "PASS: pause and resume both observed" in result.stdout, (
        "quality proof did not pass; output:\n" + result.stdout[-6000:] + "\n" + result.stderr[-2000:]
    )


@pytest.mark.slow
def test_soak_fds_and_rss_stay_flat():
    """15 minutes against the dev LiveKit, `/proc/self/fd` and RSS sampled
    every 30s inside the container the publisher runs in, with a real
    browser viewer held subscribed for the whole run -- a fourfold-shorter
    run than an unattended hour, weighed against the container, browser and
    dev-LiveKit time that hour costs. See `tools/live-proof/soak.py` for the
    sampling, the slope computation and what a viewer subscribed for the
    whole run changes; this test only runs the script and asserts what it
    reports.

    **What 15 minutes covers, and what it does not, stated rather than left
    for a flat line to imply.** 30 RSS/fd samples pass over that window at
    the default 30s cadence -- coarse next to the loops running underneath:
    the 1s bitrate-hold reassertion ticks ~900 times and the pingReq
    keepalive ~180 times (5s default), either well inside this window, so a
    leak sized to one of those ticks still shows up as a slope over 30
    points. It does **not** reach a leak keyed to garbage-collector cadence
    rather than to a per-tick cost: 3b's own `ros_runtime.py` finding needed
    roughly 880 create/destroy cycles before `gc.collect()` fell behind
    allocation, and this soak's one `LivePublisher` is never torn down
    mid-run -- it samples ZERO create/destroy cycles, not merely fewer than
    880. A pass here is evidence against the first kind of leak and says
    nothing about the second.

    Opt-in beyond `slow`: `FLEETLESS_LIVE_SOAK=1` must also be set, because
    this is still a different order of cost than the other two tests in this
    file and `pytest -m slow` alone should not silently commit to it.
    """
    if os.environ.get("FLEETLESS_LIVE_SOAK") != "1":
        pytest.skip("set FLEETLESS_LIVE_SOAK=1 to run the soak")
    reason = _skip_reason()
    if reason:
        pytest.skip(reason)

    result = _run("run-soak.sh", DISTRO, "15", "30", timeout_s=1200)
    assert "SOAK PASS" in result.stdout, (
        "soak did not pass; output:\n" + result.stdout[-6000:] + "\n" + result.stderr[-2000:]
    )
