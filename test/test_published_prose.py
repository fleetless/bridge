# SPDX-License-Identifier: Apache-2.0
"""Nothing internal reaches the bytes this package publishes, or the tree a
public mirror gets.

These files had never been swept. Their comments cited an internal design
document by section, named internal defect and feature ids, carried internal
wave labels, and named the reference robot and the people who reviewed them:
1190 references across 120 files when this guard first ran.
None of that is resolvable by anybody outside the company, and all of it
becomes permanent the moment the public mirror exists.

The patterns, the scopes and the file set live in
``scripts/internal_markers.py``, because the commit-message check needs the same
answers and a second copy of them would be a second policy.

**What makes this fail**: put one German sentence, one internal id, one
developer path, one wave label, one reference to the design document or one
internal hostname into any file this package installs or mirrors. Verified by
doing exactly that, per detector, through the fixture table in the module.

**What makes it fail the harder way**: scanning nothing. The three sets are
floored against their real sizes, a control string that must be present is
asserted, the total byte count is floored, and the detector list itself has a
floor -- deleting a detector is the cheapest way to make a sweep look finished.

**And the layer under all of that**: `hits_in`, the function both callers
actually run over the bytes. Every assertion here except one reads `hits ==
[]`, and the fixture tests below go straight to `detector.pattern.search`, so
nothing asserted that the scanner returns a hit when there is one. An early
`return out` inserted at the top of `hits_in` leaves the whole suite green and
the CLI printing `0 hit(s)` -- run, not reasoned about. There is a positive
control now.

The last section of this file covers `scripts/verify_commit_messages.py`, which
shipped with no test of any kind.
"""
import os
import pathlib
import re
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import verify_commit_messages  # noqa: E402
from internal_markers import (  # noqa: E402
    COMMIT_DETECTOR_FLOOR,
    COMMIT_EXTRA_STANCE,
    DETECTOR_FLOOR,
    DETECTOR_NAMES,
    SCOPED_CASE_OPT_OUT,
    STANCE,
    detectors,
    hits_in,
    normalise,
    public_file_set,
    scope_of,
    spliced,
    tracked_files,
    vendored_files,
    wrapped_blocks,
)

DETECTORS = detectors()

#: Files that must contain the strings the detectors look for.
#:
#: An exemption list is a hole, so each entry is bounded four ways below:
#: exact name, inside the scanned set, must exist, and **must still contain a
#: hit**. That last one is what matters: an exemption hiding nothing is one
#: more file this guard would have quietly stopped reading.
EXEMPT = {
    "scripts/internal_markers.py": "the detectors and their fixtures are spelt out here",
    # The shared release library's test suite: copied byte for byte from
    # every repository that carries a Release button, so a hit here is fixed
    # upstream, never in this checkout. Its recipe-gate fixture pair spells
    # an ordinal as a digit on the second half on purpose, to prove a diff
    # notices it -- which this detector's numbering shape mistakes for a
    # coordinate in a plan.
    ".github/release/release.test.mjs": "a fixture pair whose ordinal-as-digit rewrite reads as a numbered coordinate",
}

#: A string that MUST be in the scanned bytes; its absence means we read nothing.
CONTROL = "Fleetless"

#: The two classes a shipped document is exempt from, written out here rather
#: than read back from the module -- a real second statement, not the same
#: one twice.
#:
#: The first name used to be split across two lines by string concatenation,
#: because spelled whole it matched its own detector and turned the suite red
#: about itself. That split was an EVASION, noted as one in a comment -- until
#: ``hits_in`` grew its spliced view and the guard caught it anyway, which
#: turned the workaround into a rename instead. A detector whose name is an
#: instance of its own class forces every reference to it around the guard,
#: forever; the new name matches none.
#:
#: Nothing keeps it that way but the guard itself: this file is scanned, and
#: the spliced view leaves no way to spell the phrase here unseen. A second
#: test saying the same thing would be a second policy, and the weaker one
#: always wins.
DOCUMENT_EXEMPT_CLASSES = {
    "a reference to the writer's own project rather than to the reader's",
    "how the behaviour was found rather than what it is",
}

SET = public_file_set(ROOT)
SCANNED = [f for f in SET.readable if f not in EXEMPT]
CONTENTS = {f: (ROOT / f).read_text() for f in SCANNED}
TOTAL_BYTES = sum(len(t) for t in CONTENTS.values())


def test_the_scanned_set_is_asked_of_git_setup_py_and_the_vendor_trees():
    # Three questions, three tools, each answer floored against its real size.
    # A named list of directories is what lets a file sit outside every sweep:
    # `debian/postinst` and `Dockerfile.dev` are neither source nor test, and
    # both carry wave labels.
    assert len(SET.tracked) >= 110, "git ls-files returned almost nothing"
    assert len(SET.installed) >= 24, "setup.py named almost nothing -- did setup() run?"
    assert len(SET.vendored) >= 35, "the vendored contract trees are empty"

    # Each of the three is inside the union by construction, but say so: a
    # question whose answer the union does not contain is a silent gap.
    union = set(SET.union)
    for group in (SET.tracked, SET.installed, SET.vendored):
        for f in group:
            assert f in union, "{} was answered for but is not scanned".format(f)

    # Named files from each of the three, so a set that answered with the wrong
    # tree is visible. The last is what dpkg installs without anybody naming it.
    for f in [
        "fleetless_bridge/ros_runtime.py",
        "package.xml",
        "NOTICE",
        "test/contracts/schema/cloud-config.schema.json",
        "debian/copyright",
    ]:
        assert f in union, "{} is missing from the scanned set".format(f)

    # The `installed` set is floored separately so it cannot silently shrink,
    # but a floor on SIZE cannot say which files are in it. Named here because
    # dpkg installs them without anybody naming them anywhere, and `postinst`
    # is the one that runs as ROOT on every robot. Today `tracked` covers it
    # too, which makes the separate floor read stronger than it is: a
    # maintainer script generated at build time would land in no set at all
    # and the floor would still be green.
    for f in ["debian/postinst.in", "debian/changelog.in", "debian/copyright"]:
        assert f in set(SET.installed), (
            "{} lands on a robot but is not in the installed set".format(f)
        )

    # Nothing was silently dropped for being unreadable, and nothing the three
    # tools named has gone missing from disk.
    assert SET.binary == [], "a binary file is in the published set"
    assert SET.missing == [], "a listed file does not exist"


def test_each_of_the_three_questions_still_answers_for_itself():
    # **The union cannot tell a working set from an inert one.** `installed`
    # and `vendored` are both entirely inside `tracked` today, so the union
    # is `tracked` exactly and the other two contribute no file the first
    # doesn't already carry. The three set sizes are not frozen here as
    # numbers that go stale the moment a file is added anywhere in scope --
    # `internal_markers.py`'s own docstring answers them fresh; the floors
    # and named-member checks below are what is actually asserted.
    #
    # Correct for a repository that commits everything it ships, and not a
    # reason to drop either set: they exist so a *shrinking* `tracked` cannot
    # shrink the scan. But it means the union, the scanned file count and the
    # hit count are all **unchanged** by `installed_files()` returning [],
    # globbing the wrong directory, or answering with a stale list -- nothing
    # downstream would say a word.
    #
    # So each question is asserted for itself: its own floor (above) and its
    # own named members (here). An inert `installed` fails this test while
    # every other number in the suite stays exactly as it is.
    tracked, installed, vendored = set(SET.tracked), set(SET.installed), set(SET.vendored)

    # The invariant itself, computed rather than quoted as a number:
    # `installed` and `vendored` are each entirely inside `tracked`, so the
    # union is `tracked` exactly. If a file ever lands in one of the other two
    # but not in `tracked` -- a generated maintainer script, a build artifact
    # vendored_files() picked up -- this is what says so, rather than the
    # union quietly absorbing it unremarked.
    assert installed <= tracked, "installed has a file tracked does not: {}".format(installed - tracked)
    assert vendored <= tracked, "vendored has a file tracked does not: {}".format(vendored - tracked)
    assert set(SET.union) == tracked, "the union is not exactly tracked"

    # `installed` -- one file from each of the four sources it is derived from.
    # A bare size check, whatever `len(installed)` measures today (the floor
    # above asserts `>= 24`; the real number will keep moving), is satisfied
    # by any set of that size, right files or wrong -- which is why this
    # checks specific members instead of a count.
    for f, source in [
        ("fleetless_bridge/ros_runtime.py", "setup.py's own packages"),
        ("NOTICE", "debian/rules' explicit install line"),
        ("debian/changelog.in", "debhelper, named nowhere"),
        ("debian/postinst.in", "dpkg's control archive, named nowhere, runs as root"),
    ]:
        assert f in installed, "{} reaches a robot via {} and installed_files() does not name it".format(f, source)

    # `vendored` -- both roots, and the one that is a bare file rather than a
    # directory to walk. A walk that lost its second root keeps its floor.
    for f in [
        "test/contracts/schema/cloud-config.schema.json",
        "test/contracts/schema-outgoing/bridge-asset-progress.schema.json",
        "fleetless_bridge/contracts_constants.json",
    ]:
        assert f in vendored, "{} was copied out of another repository and vendored_files() misses it".format(f)

    # `tracked` -- the files that are in NEITHER of the other two and reach a
    # public mirror through git alone. This is the half that carries the scan,
    # and two of these four are exactly the kind that a directory-name list
    # would leave out.
    tracked_only = tracked - installed - vendored
    assert len(tracked_only) >= 50, "git is answering for almost nothing the other two do not"
    for f in [".gitignore", "CONTRIBUTING.md", "Dockerfile.dev", "run-tests.sh"]:
        assert f in tracked_only, "{} is mirrored and is in no set".format(f)


def test_a_set_that_cannot_be_computed_raises_rather_than_shrinking(tmp_path):
    # An empty answer from either walk is, downstream, indistinguishable from
    # a repository that tracks or vendors nothing -- and `git ls-files` exits
    # 0 with nothing in a directory that is a repository with no commit. The
    # command line printed
    # `scanned 62 files (tracked 0, installed 24, vendored 39); 23 hit(s)` over
    # such a tree: a real-looking summary, a real hit count, and two thirds of
    # the files never opened.
    #
    # Both callers share these functions and only one had a floor, so the
    # floor belongs on the function, not the suite.
    empty = tmp_path / "empty"
    empty.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=str(empty), check=True, stdout=subprocess.PIPE)
    with pytest.raises(AssertionError, match="answered with nothing"):
        tracked_files(empty)
    with pytest.raises(AssertionError, match="VENDORED_ROOTS"):
        vendored_files(empty)


def test_every_scanned_file_has_a_scope_and_its_bytes_were_read():
    # `scope_of` raises on an extension it does not know, so a new kind of file
    # is a decision rather than a file that stops being scanned. Asserted over
    # the real set rather than trusted.
    for f in SCANNED:
        assert scope_of(f) in ("code", "document")

    # The control. A walk returning paths whose contents are empty strings
    # would satisfy every "does not match" assertion below; this is the one
    # assertion that goes the other way.
    assert TOTAL_BYTES > 900_000, "the scanned bytes are far too few to be this tree"
    with_control = [f for f, text in CONTENTS.items() if CONTROL in text]
    assert with_control, "not one scanned file contains {}".format(CONTROL)

    # Both scopes are actually populated. A classifier that answered "document"
    # for everything would make every stance class free.
    scopes = {scope_of(f) for f in SCANNED}
    assert scopes == {"code", "document"}

    # Non-emptiness is not classification. `scopes == {"code", "document"}`
    # stays true after `.py` moves into DOC_EXT -- the `.sh`/`.json`/`.xml`
    # files keep the code half populated -- while every Python file silently
    # stops being held to the stance classes. So name which files land where,
    # on both sides of the split and both ways a file is classified (its
    # extension, and its bare name).
    for f in [
        "fleetless_bridge/config.py",
        "fleetless_bridge/ros_runtime.py",
        "test/test_published_prose.py",
        "run-tests.sh",
        "package.xml",
        "debian/rules",
        "debian/postinst",
    ]:
        assert scope_of(f) == "code", "{} is source and must be held to every class".format(f)
    for f in ["README.md", "debian/changelog", "debian/copyright", "rosdep/example.yaml"]:
        assert scope_of(f) == "document", "{} is a document and is not classified as one".format(f)

    # And every Python file the .deb installs is code, which is the assertion
    # the extension tables can actually be broken against.
    for f in SET.installed:
        if f.endswith(".py"):
            assert scope_of(f) == "code", "{} is installed source classified as a document".format(f)


def test_every_exemption_is_named_and_still_hides_something():
    assert sorted(EXEMPT) == [
        ".github/release/release.test.mjs",
        "scripts/internal_markers.py",
    ]
    for f in EXEMPT:
        assert f in set(SET.union), "{} is exempt but is outside the scanned set".format(f)
        text = (ROOT / f).read_text()
        hit = any(d.pattern.search(normalise(text, d.name)) for d in DETECTORS)
        assert hit, "{} is exempt but no longer contains anything the detectors catch".format(f)


@pytest.mark.parametrize("detector", DETECTORS, ids=lambda d: d.name)
def test_no_published_file_carries_it(detector):
    # Collected across every file and reported together: somebody who
    # reintroduced a paragraph needs to see all of it, not its first line.
    #
    # The stance classes are not run over documents. A README, a changelog or a
    # packaging control file legitimately describes the package it ships with,
    # and a guard that reddened on that would be demanding the document stop
    # addressing its own reader. The marker classes run everywhere, because an
    # internal hostname in a README link is exactly the leak this exists for.
    hits = []
    for f, text in CONTENTS.items():
        if detector.stance and scope_of(f) != "code":
            continue
        hits.extend(hits_in(detector, f, text))
    assert hits == [], "{} hit(s) for {}".format(len(hits), detector.name)


def test_the_detector_list_keeps_every_detector_and_its_fixtures():
    # The anti-vacuity floor. Deleting a detector, or adding one without the two
    # fixtures that prove it can fire and will not fire on its near-miss, is red
    # here.
    assert len(DETECTORS) >= DETECTOR_FLOOR
    assert len({d.name for d in DETECTORS}) == len(DETECTORS)
    for d in DETECTORS:
        assert d.must_match, "{} has no must_match fixture".format(d.name)
        assert d.must_not_match, "{} has no must_not_match fixture".format(d.name)
        assert isinstance(d.stance, bool), "{} does not say whether it is a stance class".format(d.name)

    # The floor above is a count, and a count cannot tell a deletion from a
    # deletion plus an addition. This is the same requirement as a set: rename
    # a class, drop one, or add one without saying so, and it is red.
    names = {d.name for d in DETECTORS}
    assert names == DETECTOR_NAMES, "the detector list and DETECTOR_NAMES disagree: {}".format(
        sorted(names ^ DETECTOR_NAMES)
    )

    # Stance classes are the ones documents are exempt from, so the split is a
    # named set cross-checked against each detector's own flag rather than an
    # ordering: taken as "the last two", appending one marker would silently
    # make a different two "the stance classes", with documents exempt from
    # the wrong ones.
    #
    # **Cross-checking is not the same claim as being right.** The previous
    # version asserted `len(STANCE) == 5` and `d.stance == (d.name in STANCE)`
    # -- the two representations agreeing with each other. Swap two classes
    # between the halves, swap the same two names in STANCE, and both
    # assertions hold while documents lose the wrong five detectors. So the
    # set is written out here, literally, and membership is what is asserted.
    assert STANCE == DOCUMENT_EXEMPT_CLASSES, (
        "the stance half changed; documents are now exempt from a different set of classes"
    )
    for n in STANCE:
        assert n in names, "{} is a stance class but not a detector".format(n)
    for d in DETECTORS:
        assert d.stance == (d.name in STANCE), "{} disagrees with STANCE".format(d.name)

    # The three classes that left the stance half. Named, because "STANCE has
    # two members" is satisfied by any two, and these three are the ones a
    # document is never legitimately about: an internal decision id, a path
    # into a repository the reader does not have, and a citation of a document
    # they cannot open.
    for n in [
        "an internal decision label",
        "a path into another repository",
        "a reference to a document the reader does not have",
    ]:
        detector = next(d for d in DETECTORS if d.name == n)
        assert not detector.stance, (
            "{} is exempt over documents again; debian/changelog ships in every .deb".format(n)
        )


def test_hits_in_reports_the_hits_its_detectors_find():
    # **The positive control for the scanner itself.** Every other assertion in
    # this file is `hits == []`, and the fixture tests below go straight to
    # `detector.pattern.search`, so `hits_in` -- the one function both callers
    # actually run over the tree -- was asserted by nothing. An early `return
    # out`, a changed split, a swapped argument: the guard goes silently inert,
    # the suite stays green and the CLI prints `0 hit(s)`.
    #
    # Proved by doing it: inserting `return out` as the first statement of
    # `hits_in` left all 145 tests green before this test existed.
    for detector in DETECTORS:
        for text in detector.must_match:
            assert hits_in(detector, "f.py", text), (
                "hits_in found nothing in a string {} is asserted to match: {!r}".format(
                    detector.name, text
                )
            )

    # And the shape of what it returns, so a stub that answers a constant
    # non-empty list is red too: the label, the 1-based line number of the
    # matching line, and the matched text itself.
    #
    # The probe line is taken from the detector's own fixture at runtime rather
    # than typed here, because a marker written into this file is a marker in
    # the scanned set -- this file is not the exempt one.
    detector = next(d for d in DETECTORS if d.name == "an internal fix-round or change label")
    marker = detector.must_match[0]
    hits = hits_in(detector, "some/file.py", "clean\nalso clean\n" + marker + "\nclean")
    assert len(hits) == 1, hits
    assert hits[0].startswith("some/file.py:3:"), hits[0]
    assert detector.pattern.search(marker).group(0) in hits[0], hits[0]

    # A text with nothing in it returns nothing -- the other direction, so this
    # test cannot be satisfied by a function that always answers.
    assert hits_in(detector, "some/file.py", "nothing internal here\nnor here") == []


def test_the_commit_message_check_runs_every_marker_class():
    # `verify_commit_messages.py` filters the detector list, and its own
    # anti-vacuity floor was 12 against 14 active classes -- room to lose two
    # whole classes before it spoke. The floor is derived now, so it cannot
    # drift; assert the SET it is derived from, because a floor that stays
    # arithmetically right while the wrong classes are in it is the failure
    # this module documents one level up.
    active = [d for d in DETECTORS if not d.stance or d.name in COMMIT_EXTRA_STANCE]
    assert {d.name for d in active} == DETECTOR_NAMES - STANCE | COMMIT_EXTRA_STANCE
    assert len(active) == COMMIT_DETECTOR_FLOOR

    # The two classes a commit message is allowed to take. A message describes
    # the repository it lands in, and how a defect turned up is its own voice;
    # naming a file in a repository the reader of a public history cannot open
    # is not, and that class is a marker now, so it is held here
    # unconditionally rather than by being listed as an exception.
    assert {d.name for d in DETECTORS if d.stance} == DOCUMENT_EXEMPT_CLASSES
    assert "a path into another repository" in {d.name for d in active}


def test_every_detector_says_whether_it_is_case_sensitive():
    # **Stated as a property of the set rather than of whichever detector
    # somebody happened to read.** Twenty of
    # twenty-one carried `re.IGNORECASE`; one did not. Nothing said so and
    # nothing could -- so a label was swept out of a docstring while its
    # capitalised twin four lines below survived, and so did every hyphenated
    # spelling of the same label, with the guard printing 0 over all of them.
    # (Written without an instance of the class on purpose: this file is in
    # the scanned set. The instances are the detector's own fixtures, in the
    # one file that is exempt.)
    #
    # A count of how many carry the flag would be the same mistake one level
    # up. This is the requirement itself: every detector is case-insensitive
    # unless it declares, in a sentence, why it is not.
    for d in DETECTORS:
        insensitive = bool(d.pattern.flags & re.IGNORECASE)
        scoped = SCOPED_CASE_OPT_OUT in d.pattern.pattern
        if d.case_sensitivity is None:
            assert insensitive and not scoped, (
                "{}: no case_sensitivity reason, so it must be case-insensitive throughout "
                "-- flag {}, inline (?-i:) {}".format(d.name, insensitive, scoped)
            )
        else:
            assert isinstance(d.case_sensitivity, str) and len(d.case_sensitivity) > 40, (
                "{}: case_sensitivity must be a sentence saying why, not {!r}".format(
                    d.name, d.case_sensitivity
                )
            )
            assert not insensitive or scoped, (
                "{}: declares a reason for case-sensitivity and is not "
                "case-sensitive".format(d.name)
            )


def test_a_detector_claiming_case_insensitivity_matches_its_fixtures_either_way():
    # The half a declaration cannot make true, and **a test of its own rather
    # than a second assertion in the one above**: the two speak about
    # different mechanisms, and pytest stops a test at its first failure, so
    # sharing a body would let the cheaper check mask this one.
    #
    # **Write the assertion over what the framework consumes.** A flag on the
    # pattern object is not the same claim as every branch of that pattern
    # honouring it: an inline `(?-i:...)` turns it off again for one branch
    # and reads like part of the regex. The check above greps the pattern
    # source for that spelling, which is a check shaped like the code we
    # wrote; this one runs every must_match fixture upper-cased and
    # lower-cased through the compiled object, which is what the scan runs.
    for d in DETECTORS:
        if d.case_sensitivity is not None:
            continue
        for text in d.must_match:
            for variant in (text.upper(), text.lower()):
                assert d.pattern.search(normalise(variant, d.name)), (
                    "{} claims to be case-insensitive but does not match {!r}".format(
                        d.name, variant
                    )
                )


def test_a_pattern_split_across_a_line_break_is_still_found():
    # **The other blocking defect: the scan ran one line at a time and this
    # tree wraps prose at ~72 columns.** Every multi-word pattern was evadable
    # by where a paragraph happened to break, and three internal citations
    # were live for exactly that reason, with the guard printing 0 over all
    # three.
    #
    # The probe is DERIVED from the detector's own must_match fixture, cut at
    # the first space inside the match, for two reasons: it cannot drift from
    # the pattern it is about, and -- as with the positive control above -- a
    # marker typed into this file is a marker in the scanned set, since this
    # file is not the exempt one.
    detector = next(d for d in DETECTORS if d.name == "an internal task or step id")
    fixture = next(t for t in detector.must_match if " " in detector.pattern.search(t).group(0))
    match = detector.pattern.search(fixture)
    cut = fixture.index(" ", match.start())
    head, tail = fixture[:cut], fixture[cut + 1:]

    # A wrapped comment in this tree: indentation and a comment marker in
    # front of the continuation.
    wrapped = "# {}\n    # {}".format(head, tail)
    assert not any(detector.pattern.search(line) for line in wrapped.split("\n")), (
        "the probe is not actually split across the break this test is about"
    )

    hits = hits_in(detector, "some/file.py", wrapped)
    assert len(hits) == 1, hits
    # Reported against the line the phrase STARTS on, which is where a person
    # has to go and look, and marked so a sweeper knows why the line alone
    # does not look like a hit.
    assert hits[0].startswith("some/file.py:1:"), hits[0]
    assert "(wrapped)" in hits[0], hits[0]

    # A blank line is a paragraph break and wrapping never crosses one, so
    # joining across it would invent adjacency the text does not have.
    assert hits_in(detector, "some/file.py", "# {}\n\n    # {}".format(head, tail)) == []


def test_wrapped_blocks_joins_paragraphs_and_maps_back_to_line_numbers():
    text = "alpha\n  # beta\n\ngamma\n"
    blocks = wrapped_blocks(text)
    # One block of two lines; the single-line paragraph is left out, because
    # the line scan has already seen it and a second view of the same hit is
    # not a second finding.
    assert [joined for joined, _ in blocks] == ["alpha beta"]
    assert blocks[0][1] == [(0, 1), (6, 2)]
    # The comment marker and the indentation go; nothing else does.
    assert wrapped_blocks("# - one\n#   two\n")[0][0] == "- one two"


def test_a_pattern_split_across_a_python_string_junction_is_still_found():
    # **The hole the rejoining did not close.** Splitting a phrase across
    # implicit string concatenation survives BOTH earlier views: neither raw
    # line carries the whole phrase, and joining the two leaves `" "` sitting
    # in the middle of it. 148 such line pairs exist in shipped `.py` files
    # here -- not a shape somebody has to go looking for.
    #
    # Derived from the detector's own fixture, cut at the first space inside
    # the match, exactly like the wrapped test above and for the same two
    # reasons: it cannot drift from the pattern, and a marker typed into this
    # file is a marker in the scanned set.
    detector = next(d for d in DETECTORS if d.name == "an internal task or step id")
    fixture = next(t for t in detector.must_match if " " in detector.pattern.search(t).group(0))
    match = detector.pattern.search(fixture)
    cut = fixture.index(" ", match.start())
    head, tail = fixture[:cut], fixture[cut + 1:]

    # What a `raise` in this package looks like when black or a human wraps it.
    concatenated = '    raise ValueError(\n        "{} "\n        "{}"\n    )'.format(head, tail)
    assert not any(detector.pattern.search(line) for line in concatenated.split("\n")), (
        "the probe is not actually split across the junction this test is about"
    )
    joined = wrapped_blocks(concatenated)
    assert joined and not any(detector.pattern.search(j) for j, _ in joined), (
        "the probe is already caught by the rejoining, so it proves nothing about splicing"
    )

    hits = hits_in(detector, "some/file.py", concatenated)
    assert len(hits) == 1, hits
    # Against the line the phrase STARTS on, and marked with the view that
    # found it, so a sweeper is not left staring at a line that looks clean.
    assert hits[0].startswith("some/file.py:2:"), hits[0]
    assert "(spliced)" in hits[0], hits[0]

    # The near miss: a blank line is a paragraph break, and splicing must not
    # reach across one any more than joining does.
    across = '    raise ValueError(\n        "{} "\n\n        "{}"\n    )'.format(head, tail)
    assert hits_in(detector, "some/file.py", across) == [], (
        "splicing crossed a paragraph break and invented adjacency the text does not have"
    )


def test_a_phrase_split_by_a_run_of_spaces_is_still_found():
    # The one property other than case that could be made general rather than
    # per-detector. A pattern that spells a literal space, or `\s`, matches
    # one character, so a phrase separated by two spaces matches nothing --
    # the view closes that for all twenty-one detectors at once, where the
    # alternative is twenty-one regex edits.
    #
    # The probe is SEARCHED FOR rather than named: any detector whose fixture
    # stops matching when one space inside it becomes three will do, and
    # naming one would pin the test to whichever branch somebody happened to
    # look at -- the shape this module exists to refuse. `[\s-]+` branches are
    # already immune, and most branches here are that.
    probes = []
    for d in DETECTORS:
        for t in d.must_match:
            m = d.pattern.search(t)
            if m is None or " " not in m.group(0):
                continue
            cut = t.index(" ", m.start())
            padded = t[:cut] + "   " + t[cut + 1:]
            if not d.pattern.search(padded):
                probes.append((d, padded))
    assert probes, (
        "no fixture stops matching when a space inside it is widened -- either every "
        "separator is a class now, in which case delete this test, or the search above "
        "has stopped selecting anything, which is the same test passing vacuously"
    )
    for detector, padded in probes:
        hits = hits_in(detector, "some/file.py", padded)
        assert len(hits) == 1 and "(spliced)" in hits[0], (detector.name, padded, hits)


def test_splicing_leaves_ordinary_quoted_prose_alone_and_maps_every_offset():
    # A quoted word is not a junction: two quotes with a word between them must
    # not collapse, or every `"foo"` in the tree would be joined to whatever
    # follows it and this view would be a false-positive machine.
    for text in ['he said "no" and left', "the 'topic' name and the 'action' name"]:
        assert spliced(text) == (text, list(range(len(text) + 1))), text

    # The junction closes the gap, and the map still points at real offsets --
    # which is the whole reason the map exists rather than a length-preserving
    # blanking: the view's job is to CLOSE a gap, and closing one moves every
    # offset after it.
    # Neutral tokens on purpose: this file is in the scanned set, so a probe
    # spelt with a real marker would be a marker in the published bytes. The
    # tests above take their probes from the detectors at runtime instead,
    # which is the only way to write one down here without shipping it.
    out, index = spliced('"alpha " "beta"')
    assert out == '"alpha beta"'
    assert len(index) == len(out) + 1
    assert out[out.index("beta")] == "b"
    assert index[out.index("beta")] == len('"alpha " "')

    # Explicit `+` concatenation is the same evasion written differently.
    assert spliced('"alpha " + "beta"')[0] == '"alpha beta"'
    # And a string prefix does not hide it either.
    assert spliced('"alpha " f"beta"')[0] == '"alpha beta"'


@pytest.mark.parametrize(
    "detector,text",
    [(d, t) for d in DETECTORS for t in d.must_match],
    ids=lambda x: x.name if hasattr(x, "name") else x[:40],
)
def test_the_detector_catches(detector, text):
    assert detector.pattern.search(normalise(text, detector.name))


@pytest.mark.parametrize(
    "detector,text",
    [(d, t) for d in DETECTORS for t in d.must_not_match],
    ids=lambda x: x.name if hasattr(x, "name") else x[:40],
)
def test_the_detector_leaves_alone(detector, text):
    assert not detector.pattern.search(normalise(text, detector.name))


# --------------------------------------------------------------------------
# The commit-message half of the guard
# --------------------------------------------------------------------------
#
# `scripts/verify_commit_messages.py` is shipped, documented in
# CONTRIBUTING.md, and was exercised by nothing: `grep -rn` for its name over
# the whole repository returned that document and the file's own docstring.
# Its `default_range()` documents five distinct situations and no test
# entered any of them; neither did the empty-range branch, the anti-vacuity
# floor or either exit code. No pipeline in this package ran it automatically
# either.
#
# It is the only thing standing between a contributor and a public history,
# and a history is public the moment it is pushed -- there is no sweep
# afterwards.


def _throwaway_repo(tmp_path, messages):
    """A repository of its own, one commit per message, built rather than
    faked: `default_range` and `main` both shell out to git, so a fake would
    test the fake."""
    root = tmp_path / "repo"
    root.mkdir(parents=True)

    def run(*args):
        subprocess.run(
            ["git"] + list(args), cwd=str(root), check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    run("init", "-q")
    run("config", "user.email", "test@example.invalid")
    run("config", "user.name", "A Test")
    run("config", "commit.gpgsign", "false")
    for i, message in enumerate(messages):
        (root / "f{}.txt".format(i)).write_text("x")
        run("add", "-A")
        run("commit", "-q", "-m", message)
    return root


def _run_main(monkeypatch, root, argv):
    monkeypatch.setattr(verify_commit_messages, "ROOT", root)
    monkeypatch.setattr(sys, "argv", ["verify_commit_messages.py"] + argv)
    for name in ("CI_COMMIT_BEFORE_SHA", "CI_COMMIT_SHA"):
        monkeypatch.delenv(name, raising=False)
    return verify_commit_messages.main()


def test_default_range_names_each_of_its_five_situations(monkeypatch):
    # Every row of the table in `default_range`'s own docstring. The tag row is
    # the one that matters: the TypeScript port of this check fell back to the
    # runner's `origin/main` there and failed a job on twenty commits written
    # long before the constraint existed.
    monkeypatch.delenv("CI_COMMIT_BEFORE_SHA", raising=False)
    monkeypatch.delenv("CI_COMMIT_SHA", raising=False)

    monkeypatch.setattr(sys, "argv", ["verify_commit_messages.py", "aaa..bbb"])
    assert verify_commit_messages.default_range() == "aaa..bbb"

    monkeypatch.setattr(sys, "argv", ["verify_commit_messages.py"])
    assert verify_commit_messages.default_range() == "origin/main..HEAD"

    monkeypatch.setenv("CI_COMMIT_SHA", "bbb")
    monkeypatch.setenv("CI_COMMIT_BEFORE_SHA", "aaa")
    assert verify_commit_messages.default_range() == "aaa..bbb"

    # A first push: an all-zero before-sha is not a commit, and must not be
    # used as one.
    monkeypatch.setenv("CI_COMMIT_BEFORE_SHA", verify_commit_messages.ZERO)
    assert verify_commit_messages.default_range() == "bbb~1..bbb"

    # A tag pipeline: no before-sha at all.
    monkeypatch.delenv("CI_COMMIT_BEFORE_SHA")
    assert verify_commit_messages.default_range() == "bbb~1..bbb"


def test_a_clean_range_passes_and_says_how_many_detectors_ran(monkeypatch, capsys, tmp_path):
    root = _throwaway_repo(tmp_path, ["first commit", "second commit, nothing internal in it"])
    assert _run_main(monkeypatch, root, ["HEAD~1..HEAD"]) == 0
    out = capsys.readouterr().out
    assert "1 message(s) clean against {} detectors".format(COMMIT_DETECTOR_FLOOR) in out


def test_an_internal_reference_in_a_message_fails_the_range(monkeypatch, capsys, tmp_path):
    # One message per marker class, each taken from that detector's own fixture
    # at runtime: a marker typed into this file would be a marker in the
    # scanned set, and "the check catches the one example I wrote" is a
    # requirement about a set guarded by an example.
    active = [d for d in DETECTORS if not d.stance or d.name in COMMIT_EXTRA_STANCE]
    for i, detector in enumerate(active):
        root = _throwaway_repo(
            tmp_path / "marker{}".format(i),
            ["first commit", "fix(x): a change\n\n" + detector.must_match[0]],
        )
        assert _run_main(monkeypatch, root, ["HEAD~1..HEAD"]) == 1, detector.name
        err = capsys.readouterr().err
        assert detector.name in err, detector.name


def test_a_stance_class_is_left_alone_in_a_message(monkeypatch, capsys, tmp_path):
    # The other direction, so the test above cannot be satisfied by a check
    # that fails every message: a message is about the repository it lands in.
    stance = [d for d in DETECTORS if d.stance and d.name not in COMMIT_EXTRA_STANCE]
    assert stance, "no stance class is left for a commit message to take"
    for i, detector in enumerate(stance):
        root = _throwaway_repo(
            tmp_path / "stance{}".format(i),
            ["first commit", "docs: a change\n\n" + detector.must_match[0]],
        )
        assert _run_main(monkeypatch, root, ["HEAD~1..HEAD"]) == 0, detector.name


def test_an_empty_range_says_so_rather_than_reporting_clean(monkeypatch, capsys, tmp_path):
    # On a tag at the tip of the default branch the range is empty and every
    # assertion is free. Passing is right; printing "clean" is not.
    root = _throwaway_repo(tmp_path, ["first commit"])
    assert _run_main(monkeypatch, root, ["HEAD..HEAD"]) == 0
    out = capsys.readouterr().out
    assert "nothing was checked" in out
    assert "clean against" not in out


def test_the_floor_trips_when_the_detector_filter_stops_matching(monkeypatch, capsys, tmp_path):
    # A filter that matched nothing would report zero hits over zero patterns
    # and print the same line as a clean range. The floor it is held to was 12
    # against 14 active classes -- two whole classes could go without a word.
    root = _throwaway_repo(tmp_path, ["first commit", "second commit"])
    monkeypatch.setattr(verify_commit_messages, "detectors", lambda: DETECTORS[:2])
    assert _run_main(monkeypatch, root, ["HEAD~1..HEAD"]) == 1
    assert "the filter has stopped matching" in capsys.readouterr().err
