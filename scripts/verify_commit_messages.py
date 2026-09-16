#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The commit messages in a range carry nothing internal.

This history is treated as public from the moment it is pushed -- there is
no sweep afterwards, and the public repository created from it later
carries forward whatever this one enforced.

The patterns are `scripts/internal_markers.py`, the same ones the published
bytes are held to. A second copy would be a second policy, and the weaker one
always wins.

**What a commit message is allowed to say that a source file is not.** A
message describes the repository it lands in, so the stance classes are its own
voice and are left alone -- the set is named in `COMMIT_EXTRA_STANCE`. A path
into a sibling repository is not its own voice: it names a file the reader of a
public history cannot open.

Usage::

    python3 scripts/verify_commit_messages.py            # origin/main..HEAD
    python3 scripts/verify_commit_messages.py <range>    # any git range

**What makes this fail**: put a hostname, a wave label, a pipeline number or a
sibling-repository path into a commit message in the range. Verified by running
it over the history written before the licence change, which is full of all
four, and -- since this file had no test of any kind for its first release --
by `test/test_published_prose.py`, which drives every branch of
`default_range`, both exit codes, the empty-range answer, the anti-vacuity
floor, and one message per marker class built from that class's own fixture.
"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from internal_markers import (  # noqa: E402
    COMMIT_DETECTOR_FLOOR,
    COMMIT_EXTRA_STANCE,
    ROOT,
    detectors,
    hits_in,
)

ZERO = "0" * 40


def git(*args):
    return subprocess.run(
        ["git"] + list(args), cwd=str(ROOT), check=True, stdout=subprocess.PIPE
    ).stdout.decode().rstrip("\n")


def default_range():
    """The range this run is about, and the honest answer when there is no range.

    **A tag pipeline has no pushed range.** The TypeScript port of this check
    learned that the expensive way: with no before-sha it fell back to the
    runner's `origin/main`, which in a CI checkout is whatever that shallow
    clone happened to fetch -- twenty commits of history written long before
    the constraint existed. The job failed on all of them. It was right about
    every line it printed and wrong about what it had been asked.

    So each case is named rather than approximated:

    ========================================  =========================
    situation                                 range
    ========================================  =========================
    an argument was given                     that argument
    a branch push (a real before-sha)         exactly what was pushed
    a tag pipeline                            the tagged commit alone
    a first push (an all-zero before-sha)     the new commit alone
    locally                                   ``origin/main..HEAD``
    ========================================  =========================

    **The residual, stated rather than closed**: this governs messages written
    from here on. It says nothing about the history behind the range it is
    given, and it is not what makes that history safe to publish -- the public
    repository starting from one clean initial commit is.
    """
    if len(sys.argv) > 1:
        return sys.argv[1]
    before = os.environ.get("CI_COMMIT_BEFORE_SHA")
    head = os.environ.get("CI_COMMIT_SHA")
    if head and before and before.strip("0") != "":
        return "{}..{}".format(before, head)
    if head:
        return "{}~1..{}".format(head, head)
    return "origin/main..HEAD"


def main():
    rng = default_range()
    shas = [s for s in git("rev-list", rng).split("\n") if s]
    print("== commit messages in {} ({}) ==".format(rng, len(shas)))

    # **An empty range is not a clean range, and must not print like one.** On a
    # tag sitting at the tip of the default branch, `origin/main..HEAD` is empty
    # and every assertion below is free while the summary line says the messages
    # were checked. Say which of the two happened; the exit code is still 0,
    # because a range with no commits in it is not a failure.
    if not shas:
        print("  no commits in {} -- nothing was checked, and nothing is claimed "
              "about the history".format(rng))
        return 0

    active = [d for d in detectors() if not d.stance or d.name in COMMIT_EXTRA_STANCE]
    # Anti-vacuity for the detector set: a filter that matched nothing would
    # report zero hits over zero patterns and print the same line as a clean
    # range.
    # **The floor is derived, not typed.** It was 12 against 14 active
    # detectors, which tolerated losing two whole classes before it spoke --
    # and the number was maintained by hand, one file away from the list it is
    # about. `COMMIT_DETECTOR_FLOOR` is computed from the detector set and the
    # stance split, so a class added or moved keeps it exact by construction,
    # and `test/test_published_prose.py` asserts the resulting *names* rather
    # than the arithmetic.
    if len(active) < COMMIT_DETECTOR_FLOOR:
        print("x only {} detector(s) are active; the filter has stopped matching".format(len(active)),
              file=sys.stderr)
        return 1

    hits = []
    for sha in shas:
        message = git("log", "-1", "--format=%B", sha)
        for d in active:
            hits.extend(hits_in(d, "{} [{}]".format(sha[:9], d.name), message))

    if hits:
        print("x {} internal reference(s) in the commit messages of {}:".format(len(hits), rng),
              file=sys.stderr)
        for h in hits:
            print("    " + h, file=sys.stderr)
        print("", file=sys.stderr)
        print("  A commit message is treated as public the moment it is pushed.",
              file=sys.stderr)
        print("  Reword the message (git rebase -i / git commit --amend) before pushing.",
              file=sys.stderr)
        return 1

    print("  {} message(s) clean against {} detectors".format(len(shas), len(active)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
