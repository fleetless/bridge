# SPDX-License-Identifier: Apache-2.0
"""The workflows name the distributions the table names, and their scripts parse.

A workflow cannot source tools/distros.sh, so `.github/workflows/verify.yml`
spells its test matrix out. A distribution added to the table and not to the
matrix would build and publish a package whose suite never ran on it -- so
the matrix is held against the table here, in the same shape
test_apt_suites.py holds reprepro's stanzas against it.
"""
import pathlib
import re
import subprocess

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _table_distros():
    return subprocess.run(
        ["bash", "-c", '. tools/distros.sh; printf "%s" "$FLEETLESS_DISTROS"'],
        cwd=str(ROOT), capture_output=True, text=True, check=True,
    ).stdout.split()


def test_the_verify_matrix_is_the_distribution_table():
    text = (ROOT / ".github" / "workflows" / "verify.yml").read_text()
    match = re.search(r"^\s*distro: \[([^\]]*)\]\s*$", text, re.M)
    assert match, "verify.yml has no single-line `distro: [...]` matrix"
    assert [d.strip() for d in match.group(1).split(",")] == _table_distros()


def _run_blocks():
    """Every `run:` of every step of every workflow, with its `${{ }}`
    expressions replaced by a word -- GitHub substitutes them before bash
    reads the script, so bash never sees the braces."""
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        workflow = yaml.safe_load(path.read_text())
        for job_name, job in (workflow.get("jobs") or {}).items():
            for index, step in enumerate(job.get("steps") or []):
                if "run" in step:
                    script = re.sub(r"\$\{\{.*?\}\}", "x", step["run"], flags=re.S)
                    yield "{}:{}:{}".format(path.name, job_name, step.get("name", index)), script


RUN_BLOCKS = list(_run_blocks())


def test_the_workflows_have_run_blocks_to_check():
    assert len(RUN_BLOCKS) >= 6, "found {} run blocks; the workflows moved or the walk broke".format(len(RUN_BLOCKS))


@pytest.mark.parametrize("where, script", RUN_BLOCKS, ids=[w for w, _ in RUN_BLOCKS])
def test_every_workflow_run_block_is_valid_bash(where, script):
    # A YAML parse passes a step whose script was cut off mid-line; the job
    # then fails on the runner -- for release.yml, after the release exists.
    result = subprocess.run(["bash", "-n"], input=script, capture_output=True, text=True)
    assert result.returncode == 0, "{}: {}".format(where, result.stderr)


def _runs_on_by_job():
    """Every job's `runs-on:`, across every workflow file.

    A job that calls a reusable workflow (`uses: <workflow>`) sets no
    `runs-on` of its own, so it is skipped here rather than reported as a
    job with no runner. Nothing calls one that way today -- release.yml
    used to call verify.yml as a reusable workflow on a tag push; it no
    longer does, and verify.yml dropped the `workflow_call:` trigger that
    made that possible. The branch stays because a local call is a shape
    this walk must not misreport if one returns: a called file is globbed
    here too, so its own jobs are still checked directly, and skipping the
    caller is not a hole.
    """
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        workflow = yaml.safe_load(path.read_text())
        for job_name, job in (workflow.get("jobs") or {}).items():
            if "uses" in job:
                continue
            yield "{}:{}".format(path.name, job_name), job.get("runs-on")


RUNS_ON = list(_runs_on_by_job())


def test_the_workflows_have_runners_to_check():
    # Anti-vacuity: a glob matching nothing -- an emptied directory, a
    # renamed folder -- would otherwise pass every assertion below it by
    # never running one.
    assert len(RUNS_ON) >= 2, "found {} jobs with a runner of their own; the workflows moved or the walk broke".format(len(RUNS_ON))


@pytest.mark.parametrize("where, runner", RUNS_ON, ids=[w for w, _ in RUNS_ON])
def test_every_job_runs_on_ubuntu_latest(where, runner):
    # The bridge is about to be public. A pull request is a stranger's code;
    # `ubuntu-latest` is hosted and disposable, an office runner is neither.
    assert runner == "ubuntu-latest", "{}: runs-on is {!r}, not ubuntu-latest".format(where, runner)


def _self_hosted_offenders():
    """`self-hosted`, classified per line rather than as a flat substring
    over the whole file.

    A flat search is the obvious belt to the check above's braces -- it
    would catch `runs-on: ${{ matrix.os }}`, a `runs-on` passed as an input
    to a reusable-workflow call (which the parse above never inspects,
    since such a job carries `uses`, not `runs-on`), and a commented-out
    block somebody is about to uncomment. It is also wrong: `sdk`'s and
    `contracts`' `release.yml` both carry a load-bearing prose comment
    explaining that npm's trusted publishing needs a cloud-hosted runner
    ("self-hosted is not supported"), and a flat search fails on that
    sentence -- inviting its deletion instead of a better check. No
    release.yml among the three differs enough to make any of them immune,
    so the classification below is the one true fix rather than a
    workaround for a neighbour's file.

    - no `#` before the match on that line -> live configuration. Fails.
    - a `#` before the match, and the comment itself looks like a
      commented-out `runs-on:` line -> one keystroke from live. Fails --
      this is the case the flat search existed for.
    - a `#` before the match, anything else -> prose. Allowed.
    """
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            at = line.find("self-hosted")
            if at == -1:
                continue
            hash_at = line.find("#")
            is_comment = hash_at != -1 and hash_at < at
            if not is_comment:
                yield "{}:{}".format(path.name, lineno), "live configuration", line.strip()
                continue
            if re.match(r"\s*runs-on\b", line[hash_at + 1:]):
                yield "{}:{}".format(path.name, lineno), "commented-out runs-on, one keystroke from live", line.strip()


def test_no_workflow_has_self_hosted_config():
    offenders = list(_self_hosted_offenders())
    assert offenders == [], "self-hosted found: {}".format(
        "; ".join("{} ({}): {}".format(where, reason, text) for where, reason, text in offenders)
    )
