# SPDX-License-Identifier: Apache-2.0
"""The detectors that decide whether a byte may be published, and the set of
bytes they run over.

**Why this is a module and not a test.** Two callers need the same answer and
must not be able to drift apart: ``test/test_published_prose.py`` (the working
tree) and ``scripts/verify_commit_messages.py`` (the messages of the commits
being pushed). A second copy of these patterns is a second policy, and the
weaker one always wins.

This is a port of the guard the TypeScript packages carry. It keeps that
guard's shape rather than its literal text, because the traps it was rebuilt
around are language-independent:

**The scanned set is computed from where the bytes go, never from a list of
directory names.** A named list is a requirement about a set guarded by
examples. So the set is the union of three questions asked of the tools that
answer them:

==============  ==============================================  ==========================================
set             asked of                                        what it is
==============  ==============================================  ==========================================
``tracked``     ``git ls-files``                                the bytes a public mirror gets
``installed``   ``setup.py``'s own ``setup()`` call, plus
                ``debian/rules``' explicit ``install`` lines,
                plus the documentation files debhelper
                installs and the maintainer scripts dpkg
                installs, from ``debian/``, without either
                being named anywhere                           the bytes that land on a robot
``vendored``    a walk of the trees copied out of another
                repository                                     bytes this repository did not write
==============  ==============================================  ==========================================

A file in none of the three is out of scope. A file in any of them is scanned.
Nothing chooses. The three are floored separately, because a total-only floor
stays green with one of them empty.

**Read the table as three questions, not as three contributions.** On a
repository that commits everything it ships -- this one, today -- ``installed``
and ``vendored`` are both entirely inside ``tracked``, and the union is
``tracked`` exactly: neither adds a file. The three set sizes themselves move
every time a file is added anywhere in the scanned scope, so they are not
frozen into this docstring as numbers to go stale the next time that happens
(``python3 -c "import internal_markers as m; fs = m.public_file_set();
print(len(fs.tracked), len(fs.installed), len(fs.vendored), len(fs.union))"``
answers it fresh); what IS asserted, by ``test/test_published_prose.py``, is
the *invariant* -- ``installed`` and ``vendored`` both subsets of ``tracked``,
the union equal to ``tracked`` -- which is the thing a drift in either set
would actually break. They are here because a *shrinking* ``tracked`` must
not shrink the scan -- a mirror filter, a sparse checkout, a ``git ls-files``
that answers less -- and because the question "what lands on a robot" is
worth asking of ``setup.py`` rather than inferred from a directory name.

The cost of that is worth saying plainly: **the union, the scanned file count
and the hit count are all unchanged if ``installed_files()`` goes inert.** So
each of the three is floored *and* has named members asserted in
``test/test_published_prose.py``. The union is not evidence that any of them
is working.

**Two scopes, decided by extension, and every file must land in one.** A source
file addresses a stranger reading the code; a README, a changelog or a
packaging control file legitimately addresses *this repository's own* reader,
and a guard that reddened on ``README.md`` saying "this repository" would be
demanding the document stop being about itself. So documents are scanned for
the MARKER classes only, and code for every class. ``scope_of`` raises on
anything it cannot classify, so a new kind of file is a decision somebody makes
rather than a file that silently stops being scanned.

The split is a hole in exactly the size of the STANCE set, so that set is kept
as small as the argument for it. Only two classes are in it, and both are
things a shipped document genuinely says about itself. A decision label, a
sibling-repository path and a citation of a document the reader does not have
were once in it, and none of the three is ever self-description: a README
citing an internal register is the same leak as a source file doing it, and one
such line shipped in a ``.yaml`` for exactly that reason while the guard
printed zero.

**Stripping happens for one detector, not for all of them.** The predecessor
this is ported from stripped URLs *and* inline code spans from every line
before every class, which made its documents blind to exactly the shapes an
internal host, a developer path or an internal id take in a README: a link and
a backticked citation. Only the German scan strips here, and it strips two
things: URL-ish runs (several German function words are also ordinary path
segments) and **single-token** code spans (a backticked lone token is a
citation; a multi-word span may be a German sentence).

**Every detector carries its own fixtures.** ``DETECTORS`` is a list of objects,
not two parallel lists, so a detector cannot be added without a ``must_match``
and a ``must_not_match``. ``DETECTOR_FLOOR`` is the anti-vacuity floor:
deleting a detector -- the cheapest way to make a sweep look finished -- is red,
and ``DETECTOR_NAMES`` is the same floor stated as a set, because a count
cannot tell a deletion from a deletion plus an addition.

**Why this one file is exempt from its own guard, and what that does not
license.** A pattern has to name what it blocks: a detector for an internal
hostname is a hostname written down, and there is no way to write it that is
not writing it. So this file is the single exemption, and the suite pins the
exemption to this name and asserts it still catches something -- an exemption
that hides nothing is a file the guard has quietly stopped reading.

The exemption covers the *patterns*. It does not cover the *fixtures*, which
are free text and were quoting real internal coordinates.

**The line between the two runs through the pattern, not through the
fixture.** Ask what the pattern is:

* **A literal.** ``the reference robot by name``, ``an internal review
  codename``, ``a person by first name``, ``a file only the maintainers have``
  and the repository names inside ``a path into another repository`` are spelt
  out in the regex itself. A ``must_match`` fixture for one of those
  **necessarily contains the real token**, because that token is the whole
  pattern: no invented name makes the pattern for a person fire. Those fixtures
  keep the token.
  **Do not "sanitise" them** -- replacing the name in the fixture with an
  invented one silently turns the detector's only proof that it can fire into
  a proof about something else, or deletes it outright. Generalising the
  *pattern* is the move that gets the real token out of both, and the paragraph
  below the next bullet is where one detector did exactly that.

  **Keeping the token is not keeping the sentence.** A fixture's whole job is
  to make the regex fire, so everything around the token is free text and must
  assert nothing: no operational fact, no location of a secret, no real
  repository or document path, no host suffix the pattern does not need. One
  of these fixtures used to say where a signing key lives and in whose keyring
  -- a sentence a commit in this same series removed 79 lines of runbook from
  this repository to avoid publishing, which the fixture then carried straight
  back in. Keep the token; drop the claim.
* **A shape.** The wave and train labels, the decision ids, the task, step,
  sub-project, fix-round and pipeline grammars, the hosts and working trees in
  ``an internal host or workspace name``, and the directory layout *inside* a
  sibling path are all grammars over digits and letters. A shape regex does
  not need a real coordinate to fire, so those fixtures are now
  shape-equivalent and synthetic -- a label that matches the grammar and never
  referred to anything (``R47``, ``W0``, ``ledger decision 9``, ``pipeline
  9001``, ``infra/example/compose.yml``). They prove the regex fires exactly
  as well, and they publish nothing.

So a fixture here carries a real token only where the pattern above it already
carries the same token, and it says nothing further about it.

**``an internal host or workspace name`` moved from the first list to the
second, and that is the direction to move a detector.** It spelt a forge host,
a documentation host, a VPN peer prefix and a renamed working tree -- four
literals, all four therefore in the published bytes, the pattern leaking
exactly what its fixtures were being careful about. It matches shapes now: any
GitLab host, any ``.cloud`` documentation host, any WireGuard peer name, any
tree renamed ``.old``. Each branch is wider than the literal it replaced, so
nothing that was caught before escapes now. The two that stay literal stay
because there is no shape behind a name: a person, and a robot.

**The explanatory comments are a third thing, and the rule above is not about
them.** Every detector says which instance it was written for -- ``fix round
2`` swept while ``Fix round 3`` four lines below survived, the numbered gate,
the severity grade -- because a pattern whose motivating instance is
paraphrased away cannot be checked against the defect it exists to catch, and
"widen this" then becomes a matter of taste. Those citations are inside the one
exempt file, and none of them is a host name, a person, a path or a document
the reader cannot open. This paragraph exists because the fixtures went
synthetic and the comments did not: a reader who meets one of these and takes
the rule above to cover it would conclude the rule is not followed.
"""
import ast
import fnmatch
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------
# The German detector
# --------------------------------------------------------------------------

#: German function words with no English homograph. Function words rather than a
#: topic vocabulary: prose switches language wholesale, so the cheap words are
#: the reliable detector and a noun list would miss a paragraph that happens to
#: avoid its nouns.
#:
#: ``was`` and ``hat`` are deliberately absent: both are ordinary English words,
#: and a detector that fires on "the reference was not resolved" is a detector
#: somebody deletes.
#:
#: **The definite articles are the cheapest words in the language and were
#: missing.** ``das``, ``der``, ``des`` and ``dem`` are here because the two
#: German fragments this package actually shipped were found by *reading* and
#: not by this detector, and both contained ``das``. ``die`` and ``den`` stay
#: out: both are ordinary English words ("die", "den"), and a detector that
#: reddens on "the process must not die" is one somebody deletes. Adding the
#: four costs nothing measurable -- they produce no hit anywhere in this tree.
GERMAN = [
    "das", "der", "des", "dem",
    "und", "oder", "nicht", "aber", "sondern", "weil", "dass", "damit", "wenn",
    "denn", "doch", "schon", "noch", "auch", "nur", "sehr", "hier", "dort",
    "jetzt", "dann", "immer", "nie", "alle", "alles", "viele", "jede", "jeder",
    "jedes", "eine", "einen", "einem", "einer", "eines", "kein", "keine",
    "keinen", "ist", "sind", "wurde", "wurden", "wird", "werden", "worden",
    "kann", "können", "muss", "müssen", "soll", "sollen", "darf", "dürfen",
    "haben", "hatte", "hatten", "für", "über", "unter", "zwischen",
    "gegen", "ohne", "durch", "nach", "vor", "bei", "beim", "zum", "zur", "vom",
    "aus", "mit", "von", "auf", "sich", "ihre", "ihrer", "seine", "seiner",
    "diese", "dieser", "dieses", "welche", "welcher", "wer", "wie",
    "warum", "deshalb", "daher", "gemessen", "gilt", "steht", "liegt",
]

#: Every character that is not a letter, in the Unicode sense. Not ``\b``, and
#: that is not a style choice: Python's ``\b`` is defined over ``\w``, which
#: includes digits and the underscore, so ``\bist\b`` would refuse to fire in
#: ``W3_ist_hier`` while firing between a digit and a word elsewhere. Written as
#: the complement of the letter class instead, the boundary means the one thing
#: it should: the neighbouring character is not a letter. Umlauts are letters to
#: Python's Unicode ``\w``, so ``ueberflieger`` written properly with an umlaut
#: is not split in the middle the way an ASCII boundary would split it.
NOT_LETTER = r"[\W\d_]"


def _german_word(word):
    """Each word is matched as ``[Ww]ort``: a sentence-initial capital still
    counts, an all-caps acronym does not.

    That is not tidiness. ``mit`` is on the list and ``MIT`` is a licence
    identifier, so under a case-insensitive flag ``MIT License`` is a German hit
    before a word of German is written.

    **Two residuals, stated rather than implied away.**

    *Block capitals.* A heading ``## WARUM DAS SO IST`` passes, because every
    word is matched as ``[Ww]ort`` and none of those is. The strictly stronger
    alternative -- keep the case-insensitive flag and exempt the standalone
    uppercase token ``MIT`` -- was not taken, so this is a known miss and not a
    class the detector covers.

    *The vocabulary itself.* This is a heuristic over a word list, not a
    language classifier: a German sentence that happens to use none of the
    words in ``GERMAN`` passes, and no amount of widening closes that. Saying
    "the residual is block capitals" would imply the rest of the class is
    covered, and it is not -- the two fragments this package shipped were
    caught by a person reading, after this detector reported nothing. Adding
    the definite articles narrows the gap; it does not close it.

    Nothing in this tree writes German either way today, which is a judgement
    about the corpus, not a guarantee about the pattern.
    """
    return "[{}{}]{}".format(word[0].upper(), word[0], word[1:])


GERMAN_PATTERN = re.compile(
    "(^|{nl})({words})({nl}|$)".format(nl=NOT_LETTER, words="|".join(_german_word(w) for w in GERMAN))
)

#: URL-ish runs. Removed before the German scan only, because several German
#: function words are also ordinary path segments (``/api/v1/von/``, ``/nach/``,
#: ``/bei/``).
URLISH = re.compile(
    r"""\b[a-z][a-z0-9+.-]*://\S+"""
    r"""|(?:^|[\s(`"'])/[A-Za-z0-9_.:${}*-]+(?:/[A-Za-z0-9_.:${}*-]+)+"""
)

#: Single-token inline code spans. Removed before the German scan only.
#:
#: The token limit is the point: ```von``` is a citation and cannot be prose,
#: while ```Das gilt nur hier``` is a German sentence that happens to be inside
#: backticks. The predecessor stripped every span, of any length, before every
#: class.
CODESPAN_TOKEN = re.compile(r"`[^`\s]*`")

# --------------------------------------------------------------------------
# Sibling repositories
# --------------------------------------------------------------------------

#: The repositories a path may point into. Computed against this repository's
#: own name so that ``bridge/test/conftest.py`` is a self-reference here and a
#: sibling reference elsewhere -- the TypeScript port of this list was narrowed
#: by hand once, which made it strictly weaker than the guard it came from and
#: hid nothing in return.
SIBLING_REPOS = ["cloud", "console", "bridge", "infra", "contracts", "docs", "sdk"]

#: The adjectives that make ``wave`` or ``train`` physics rather than process.
#: Written as fixed-width lookbehinds because that is what Python allows, one
#: per word. ``tools/fake_robot.py`` publishes a triangle wave and two sine
#: waves and describes them in its own module docstring; a guard that reddens
#: on a signal name is one somebody deletes.
NOT_A_SIGNAL = "".join(
    "(?<!{} )".format(word)
    for word in (
        "sine", "cosine", "triangle", "square", "saw", "sawtooth", "carrier",
        "radio", "sound", "shock", "gravity", "heat", "brain", "standing",
        "pulse", "gear", "power", "drive",
    )
)

SELF = "bridge"


def _repo_alternation(self_name):
    return "|".join(r for r in SIBLING_REPOS if r != self_name)


#: How a pattern turns its own case-insensitivity off again for one branch.
#: Named because two places have to look for it -- ``Detector.__init__`` and
#: the suite -- and a second spelling of it would be a second policy.
SCOPED_CASE_OPT_OUT = "(?-i:"


class Detector:
    """One class of text that may not be published, with the two fixtures that
    prove it can fire and will not fire on its near-miss.

    **Case is a declared property, not an absence.** Twenty of twenty-one
    detectors carried ``re.IGNORECASE`` and one did not. Nothing said so and
    nothing could: the flag is invisible at the call site, so the odd one out
    was found by reading, after ``fix round 2`` was swept out of a docstring
    and ``Fix round 3`` four lines below it was not. That is the shape this
    whole module exists to refuse -- a requirement about a set, guarded by
    whichever member somebody happened to look at.

    So the constructor asks. A detector is case-insensitive unless it passes
    ``case_sensitivity``, a sentence saying why, and passing that while *also*
    setting the flag is refused too: a stance stated and then contradicted is
    worse than none. ``test_published_prose.py`` asserts the same thing over
    the whole list, and -- for every detector that claims to be wholly
    case-insensitive -- re-runs its ``must_match`` fixtures upper-cased and
    lower-cased, which is the only way to see a flag that the pattern turns
    off again inside one branch with ``(?-i:...)``.

    **Case is the only property that is a declared property. Say what that
    leaves.** Two more were closed since, and neither of them here: whitespace
    runs and Python string junctions are handled once for the whole list by the
    spliced view in ``hits_in``, because a view over the text closes a class
    for twenty-one patterns where a flag would have to be repeated in
    twenty-one places. **Everything else is still per-detector, and the forms
    that escape are concrete rather than theoretical.** Run against the
    finished guard, ``FL 07``, ``DEF 12`` and ``NEW 1`` all score **zero**,
    while ``FL-07`` and ``NEW-1`` fire: the pattern each belongs to spells one
    separator, a hyphen, and a space is not it. (``this  task`` and ``task
    brief`` with two spaces used to be on that list and are not any more --
    that is the half the spliced view took.)

    That is a residual and not a leak: a rescan of the whole tree with every
    separator relaxed to ``[\\s-]`` found nothing live behind the assumption.
    So widening them all is *rejected* rather than postponed, and the reason is
    the measurement: the gain over this tree is known and it is zero, while the
    cost -- twenty-one patterns made looser against ordinary English, in the
    last round before a permanent public commit -- is not measured at all, and
    a detector that reddens on ordinary prose is one somebody deletes rather
    than obeys. If somebody later finds an instance that escapes this way, the
    instance is the evidence and the widening is the fix.
    """

    __slots__ = ("name", "stance", "pattern", "must_match", "must_not_match", "case_sensitivity")

    def __init__(self, name, stance, pattern, must_match, must_not_match, case_sensitivity=None):
        insensitive = bool(pattern.flags & re.IGNORECASE)
        scoped = SCOPED_CASE_OPT_OUT in pattern.pattern
        if case_sensitivity is None and not (insensitive and not scoped):
            raise AssertionError(
                "{}: case_sensitivity is None, which claims this pattern is case-insensitive "
                "throughout, and it is not ({}). Either make it so, or say in one sentence "
                "why -- a detector whose flags differ from every other one by accident is the "
                "defect this argument exists to make impossible.".format(
                    name, "an inline (?-i:...) turns the flag off again" if scoped
                    else "re.IGNORECASE is not set"
                )
            )
        if case_sensitivity is not None and insensitive and not scoped:
            raise AssertionError(
                "{}: declares a reason for being case-sensitive and is not. Delete the "
                "reason or the flag.".format(name)
            )
        self.name = name
        self.stance = stance
        self.pattern = pattern
        self.must_match = must_match
        self.must_not_match = must_not_match
        self.case_sensitivity = case_sensitivity

    def __repr__(self):
        return "Detector({!r})".format(self.name)


def detectors(self_name=SELF):
    """The detector list.

    :param self_name: this repository's own short name, excluded from the
        sibling-repository alternation.
    """
    repos = _repo_alternation(self_name)
    # Two fixtures below have to name a repository that is NOT this one, and
    # one has to name this one. Written against ``self_name`` rather than
    # hardcoded, because the same fixture table runs in every repository that
    # adopts this module and a hardcoded name silently becomes a self-reference
    # in one of them.
    sibling = next(r for r in SIBLING_REPOS if r != self_name)
    return [
        # ---- MARKERS: text that cannot be anything but internal. Scanned in
        # both scopes, because a document is as public as a source file.
        Detector(
            "German prose",
            False,
            GERMAN_PATTERN,
            [
                "Die Cloud wusste es die ganze Zeit und warf die Markierung weg.",
                # An umlaut-initial word, which an ASCII boundary silently misses.
                "Eine Grenze, über die niemand spricht.",
                # A single-token span is stripped; the prose beside it is not.
                "The `von` segment ist hier nicht gemeint.",
                # Sentence-initial capital, which the case-sensitivity must still catch.
                "Über die Grenze spricht niemand.",
                # A multi-word span is NOT a citation and is not stripped.
                "The comment read `Das gilt nur mit einer Grenze` and was rewritten.",
                # A digit next to a German word: the boundary is "not a letter",
                # so a wave-labelled German note is still German.
                "3 Punkte sind offen und werden nicht bearbeitet.",
                # The two shapes this package actually shipped, both of which a
                # list without the definite articles cannot see. Neither uses a
                # verb, a conjunction or a preposition from the rest of the
                # list; ``das`` is the only German word in either.
                "erkennt das URDF (Standardquelle /robot_description)",
                "Bridge erkennt das URDF automatisch.",
            ],
            [
                # The two definite articles that are also ordinary English, and
                # are deliberately NOT on the list.
                "The process must not die when the socket closes.",
                "A den of nested futures is never built here.",
                # The places a licence identifier appears in this package's bytes.
                "MIT License",
                'license="Apache-2.0"',
                "SPDX-License-Identifier: Apache-2.0",
                # A URL whose segments are also German function words.
                "See `https://example.test/api/v1/von/bei/nach/wie` for the shape.",
                "The path `/nach/bei` is reserved.",
                # German letters INSIDE an English word, which an ASCII boundary would claim.
                "The überflieger identifier is not a keyword here.",
                # Naming the words the detector looks for, which a changelog has to do.
                "It stopped tripping on `von`, `bei`, `nach` and `wie`.",
            ],
            case_sensitivity=(
                "each word is built as ``[Ww]ort`` by ``_german_word``, so the flag would "
                "have nothing to do; and under it ``mit`` claims ``MIT License``, which is "
                "in this package's own bytes. The residual -- a heading in block capitals -- "
                "is stated in ``_german_word``'s docstring rather than implied away."
            ),
        ),
        Detector(
            "an internal feature id (FL-0xx)",
            False,
            # The prefix is the shape: an id class cannot be detected without
            # the letters that name it, so ``FL-`` stays. The fixture's number
            # does not -- a three-digit one reads as a real item, and two digits
            # fire this pattern exactly as well.
            re.compile(r"FL-0\d", re.IGNORECASE),
            ["Closed under FL-07; the route answers 409 now."],
            ["The flag is FL_ENABLED and defaults to false.", "Serial FL-12 is printed on the chassis."],
        ),
        Detector(
            "an internal defect id (DEF-nnn)",
            False,
            # As above: ``DEF-`` is the class and stays; the number is invented
            # and deliberately short, so nothing here reads as a defect somebody
            # could look up.
            re.compile(r"DEF-\d", re.IGNORECASE),
            ["A crashed process must come back by itself (DEF-77)."],
            ["The default is DEF_TIMEOUT_MS.", "A `DEF-` prefix is not used by this API."],
        ),
        Detector(
            "an internal wave label (W1..W9x)",
            False,
            # **Case-sensitive, and that is why it is a detector of its own.**
            # The wave/train detector below needs the ignore-case flag for
            # ``Wave 4`` and ``The train``; this one must not have it, because
            # ``w2`` is an ordinary identifier and a fixture named
            # ``w2 = {}`` is not a leak. The boundary is "not an ASCII
            # alphanumeric" rather than ``\b``, so ``PREFIX_W5_SUFFIX`` is a hit
            # and ``W3C`` is not.
            re.compile(r"(^|[^0-9A-Za-z])W\d[a-e]?([^0-9A-Za-z]|$)"),
            ["Hand-rolled rather than generated (W0, R47).", "Closed in W0, reopened in W9e."],
            ["w2 = {'robots': {}}", "The W3C spelling is different.", "Bucket w9 holds the remainder."],
            case_sensitivity=(
                "``w2`` is an ordinary identifier and ``w9`` an ordinary bucket name; a "
                "case-insensitive ``W\\d`` would redden on both. The capitalised form is the "
                "label. The wave/train detector below carries the flag instead, which is why "
                "these are two detectors and not one."
            ),
        ),
        Detector(
            "an internal wave or train label",
            False,
            # Four shapes, because this codebase writes all four: the terse
            # ``W9b``, the numbered ``wave 4`` / ``train 6 review``, the
            # determined ``the wave`` / ``one train early`` / ``the per-app
            # train`` / ``spent two waves removing``, and ``the wave's gate``.
            #
            # **``a wave label`` is excluded on purpose.** Naming the class is
            # not using it, and a changelog and this module's own callers have
            # to be able to say what the detector looks for. A bare
            # ``\bwaves?\b`` would have made the guard's vocabulary unwritable,
            # which is how a detector gets deleted rather than obeyed.
            # **The separator is part of the shape.** ``wave 4`` and ``wave-4``
            # are one label written two ways, and a pattern that takes only the
            # space is a pattern somebody evades without meaning to. Same for
            # every numbered label below.
            #
            # ``NOT_A_SIGNAL`` is the other direction: a triangle wave and a
            # sine wave are physics, not process, and this package's own fake
            # robot publishes both. They were invisible while the scan was
            # line-at-a-time and appeared the moment it stopped being.
            re.compile(
                r"\b(?:wave|train)s?[\s-]+(?:\d|lead\b)"
                r"|\b(?:the|this|that|a|an|each|every|next|last|later|earlier|same|whole|entire"
                r"|following|previous|one|two|three|four|five|several|another)\s+"
                r"(?:[a-z][a-z-]*\s+){0,2}" + NOT_A_SIGNAL + r"(?:waves?|trains?)\b(?!\s+labels?\b)"
                r"|\bwave(?:'s)?[\s-]+(?:lead|gate|review|sweep|browser)\b",
                re.IGNORECASE,
            ),
            [
                "The flag is `attempted`, not `changed` (train 9 review, C97).",
                "It was renamed for a wave 9 deletion that did not happen.",
                "NOTE (from the wave lead): the shared cloud restarts.",
                "It sent the same frame for an entire wave of commits.",
                # An adjective between the determiner and the noun, and a
                # quantifier instead of a determiner.
                "It has one producer already, one train early.",
                "The per-app train adds the second one.",
                "This is the defect this file has spent two waves removing.",
                # The hyphenated spellings, which the space-only version
                # missed. Written WITHOUT a determiner in front, deliberately:
                # "the wave-2 sweep" is caught by the determiner branch
                # whatever the separator does, so it would prove nothing about
                # the separator at all.
                "Reproduced in wave-2 and closed in wave-3.",
                "Confirmed by wave-lead review before the merge.",
            ],
            [
                "The waveform is sampled at 40 Hz.",
                "Trained models are out of scope for this bridge.",
                # The guard's own vocabulary: a changelog has to be able to say
                # which class it swept, and this module's callers have to name it.
                "Put a hostname, a wave label or a pipeline number into a message.",
                "| internal wave labels | 134 | 0 |",
                # Physics. Both are published by this package's own fake robot,
                # and both were hits until the scan learnt to see a wrapped
                # line and the pattern learnt these are not labels.
                "`percentage` rides a slow triangle wave (0 -> 1 -> 0 over ~20s)",
                "two named joints, each a sine wave at a different phase",
                "A pulse train on the trigger line is not what this reads.",
            ],
        ),
        Detector(
            "an absolute path from a developer machine",
            False,
            # ``/Users/`` keeps its capital inline. Lower-cased it is
            # ``/api/users/me``, a route this package's own fixtures call, and
            # a detector that reddens on a REST path is one somebody deletes.
            # ``/home/`` has no such twin, so it takes the flag.
            re.compile(r"/home/|(?-i:/Users/)", re.IGNORECASE),
            [
                "Written to /home/someone/work/fleetless/bridge.",
                "See /Users/someone/work/notes.txt for the tally.",
                "cd /HOME/someone && ./run-tests.sh",
            ],
            [
                "The home page is served from `/`.",
                "POST /api/users/me returns the caller.",
            ],
            case_sensitivity=(
                "``/Users/`` is a macOS home directory and ``/users/`` is a REST collection; "
                "only the capital distinguishes them. ``/home/`` takes the flag."
            ),
        ),
        Detector(
            "the reference robot by name",
            False,
            # **The one literal kept on purpose.** The reference robot is named
            # in the public developer documentation, so this pattern discloses
            # nothing the docs do not; and there is no shape behind it --
            # generalised, two letters and a digit is every other identifier in
            # this package. What it keeps out of the published bytes is a test
            # rig nobody outside can reach, which is worth a detector even when
            # the name is not a secret.
            re.compile(r"rx1", re.IGNORECASE),
            ["Verified against rx1 rather than a container.", "ssh wg-RX1-example"],
            ["The register is RX_1 on that board.", "A `matrix1` field is not read."],
        ),
        Detector(
            "an internal host or workspace name",
            False,
            # **Four shapes, and not one literal.** A detector that spells the
            # host it hunts publishes it: the pattern is then the leak, one
            # file along, and ``git grep`` over the public mirror finds it on
            # day one. So the branches are, in order: any GitLab host, any
            # ``.cloud`` documentation host, any WireGuard peer name, any tree
            # renamed ``.old``.
            #
            # Each branch is strictly wider than the literal it replaces, which
            # is the safe direction -- ``gitlab.com`` and ``wg-quick`` redden
            # now too, and neither is written anywhere in this package.
            # Narrowing one back to a literal reopens the hole twice over: in
            # the published bytes, and in the pattern itself.
            #
            # **No leading ``\\b``, deliberately.** The literals had none, so
            # anchoring the front narrows instead of widening --
            # a host with a letter glued to its front matched it. The one
            # trailing guard, on ``.old``, is ``(?![a-zA-Z0-9_])`` and not
            # ``\\b``: Python's ``\\b`` is Unicode-aware and JavaScript's is
            # ASCII, so ``\\b`` here would make this port miss every
            # spelling whose neighbour is an accented letter, which the sdk
            # and contracts twins catch -- the port silently narrower than
            # the guard it came from,
            # which is the exact drift this file has been bitten by before. It
            # earns its place against one measured false positive,
            # ``config.older``, which ``must_not_match`` names.
            re.compile(
                r"gitlab\.(?:[a-z0-9-]+\.)*[a-z]{2,}"
                r"|(?:[a-z0-9-]+\.)+cloud(?![a-zA-Z0-9_])"
                r"|wg-[a-z0-9]+(?:-[a-z0-9]+)*"
                r"|[a-z0-9_]+\.old(?![a-zA-Z0-9_])",
                re.IGNORECASE,
            ),
            [
                "The tunnel peer is wg-example-app.",
                # The shape the predecessor could not see, because it stripped
                # links out of markdown before looking: a hostname inside one.
                "[source](https://gitlab.example.test/example/example)",
                "The note is at https://docs.example.cloud/example.",
                # The renamed working tree, which had no fixture at all while
                # the pattern was a list of literals.
                "The previous checkout is in tree.old-2026-01.",
            ],
            [
                "The console is at https://console.fleetless.dev.",
                "A `wg0` interface is not required.",
                # The ordinary words the widened branches must not claim: a bare
                # ``cloud`` with no host in front of it, and ``older``.
                "The shared cloud restarts on every save.",
                "It reads `config.older` from the cache directory.",
            ],
        ),
        Detector(
            "a person by first name",
            False,
            # The maintainer's full name is attribution and stays: it is in
            # ``debian/control``, ``package.xml`` and every changelog entry
            # because a Debian package and a ROS manifest are required to name a
            # maintainer. A bare first name is a colleague the reader has never
            # met, which is a different thing entirely.
            re.compile(r"Andr[eé]\b(?!\s+Dehne\b)"),
            ["Andre confirmed the shape on the call.", "Raised by André in review."],
            [
                "Maintainer: Andre Dehne <andre@dehne-robotik.de>",
                "The Andrews constant is unrelated.",
                "Copyright 2026 Dehne Robotik GmbH",
            ],
            case_sensitivity=(
                "the maintainer's own address, ``andre@dehne-robotik.de``, is attribution "
                "this package is required to carry, and it is lower-case. Under the flag the "
                "lookahead that lets ``Andre Dehne`` through does not save it: the address "
                "carries no surname after the name."
            ),
        ),
        Detector(
            "an internal review codename",
            False,
            # Bare, not only suffixed: the live instances were ``confirmed with
            # Nimbus`` and ``Argus-W7a``. ``Eve`` and ``Data`` are ordinary
            # enough words that they keep the suffix requirement.
            # **The dangling hyphen is the shape a half-done sweep leaves.**
            # ``Eve-W7a`` with its wave label cut out is ``Eve-``, which read
            # as ordinary prose to every detector here and shipped in
            # ``protocol.py``. A codename followed by a hyphen and then
            # anything that is not a letter or digit is not a word: it is a
            # label with its coordinate removed.
            re.compile(
                r"\b(?:Momus|Kassandra|Threepio|Argus|Nimbus|Rosie)\b"
                r"|\b(?:Eve|Data|Momus|Kassandra|Threepio|Argus|Nimbus|Rosie)-W\d*[a-e]?\b"
                r"|\b(?:Eve|Data|Momus|Kassandra|Threepio|Argus|Nimbus|Rosie)-(?![A-Za-z0-9])"
            ),
            [
                "sensor_msgs (R47, Argus-W0): imported at module load.",
                "Raised by Eve-W0 and closed the same day.",
                # The stripped form, live in this package until it was read.
                "A fourth, not a `refused` variant (Eve- against the",
            ],
            [
                "The datapoint payload is opaque data.",
                "Every frame carries a `data` member.",
                # The hyphenated ordinary words the dangling-hyphen branch must
                # not claim, which is also why this detector is case-sensitive.
                "A data-driven check is not what this is.",
                "the data-plane socket is closed first",
            ],
            case_sensitivity=(
                "``Eve`` and ``Data`` are ordinary English words. Under the flag the "
                "dangling-hyphen branch claims ``data-driven`` and ``data-plane``, which "
                "this package writes; capitalised, they are names."
            ),
        ),
        Detector(
            "a file only the maintainers have",
            False,
            re.compile(r"\bCLAUDE\.md\b|\.superpowers/|\bdocs/superpowers/|\bMEMORY\.md\b", re.IGNORECASE),
            [
                # No longer excused by backticks: a published comment that cites
                # a maintainer-only file cites it whether or not it is in a code
                # span.
                "a published comment that cites CLAUDE.md cites it either way",
                "The plan is under docs/superpowers/plans/.",
            ],
            ["See CONTRIBUTING.md for the test rules.", "The claude field is not part of the schema."],
        ),
        Detector(
            "an internal task or step id",
            False,
            # ``task 6.3`` is a coordinate in a plan the reader does not have. It
            # is not a wave label and not a decision label, and it survived both.
            #
            # **The decimal point was doing the whole job, and most of this
            # corpus does not write one.** The first version required
            # ``\d+\.\d+``, so ``task 6.3`` was a hit while ``Task 3``,
            # ``task 4``, ``Step 5`` and ``step 5's`` -- twelve live instances,
            # one of them citing ``the task brief`` by name -- walked through
            # all eighteen detectors. A bare number is the same coordinate; it
            # is not a weaker one.
            #
            # **``the task brief`` and ``this task`` are the same class without
            # a number.** They name the commissioning document and the piece of
            # work it commissions, neither of which a public reader can obtain.
            # ``this task`` carries a real risk in an asyncio package, where it
            # can mean an ``asyncio.Task``; the lookahead keeps the verbs that
            # give it that reading out, and the fixture below pins them.
            #
            # **What this detector cannot tell apart, stated rather than
            # implied away:** a step of an internal plan from a step of a
            # procedure the same file spells out for its own reader. Both are
            # ``step 2``. It reddens on both, and the second is reworded rather
            # than exempted -- an exemption keyed on "the file explains it
            # nearby" is not something a regex can hold.
            re.compile(
                r"\b(?:tasks?|steps?)[\s-]+\d"
                # **``the`` was doing work no part of the class needs.** The
                # live instance was ``# field (task brief).`` -- the same
                # citation, opening a parenthesis instead of a sentence.
                r"|\btask brief\b"
                r"|\bthis task\b(?!\s+(?:completes|finishes|returns|raises|ends|is\b|was\b|has\b))",
                re.IGNORECASE,
            ),
            [
                "They are the browser check's subject (task 9.7).",
                "Deferred to step 8.4.",
                # The bare forms, all four of which the decimal requirement missed.
                "a bare task (Task 9 does exactly that). Same shape as",
                "# must never decide scan order (Step 8 break #1 of the task brief).",
                "step 9's actual claim: nothing that resolves perfectly is reported",
                "Standalone in this task: nothing constructs it yet.",
                # The determiner-free citation and the hyphenated number.
                "# and the console consumer must never need a null check per field (task brief).",
                "Deferred to step-8 of the same sweep.",
            ],
            [
                "The task id is a uuid.",
                "Retry after 1.5 seconds.",
                "See section 6.3 of the RFC.",
                "It advances in steps of five degrees.",
                # The asyncio reading of ``this task``, which must stay ordinary.
                "The future resolves when this task completes.",
                "A `steps` array is not part of the schema.",
            ],
        ),
        Detector(
            "an internal pipeline or job number",
            False,
            # A CI pipeline id is a number only a maintainer can resolve, and it
            # is the class that survived into a commit message written under the
            # constraint that forbids it.
            re.compile(r"\b(?:pipelines?|MRs?|merge requests?)[\s-]+#?\d{3,}|\bjobs?[\s-]+#?\d{4,}", re.IGNORECASE),
            [
                "Measured on pipeline 9001: this runner uses bash.",
                "A retry is safe here (pipeline 9001, job 9002).",
                "Reproduced on pipeline-9001 of the same branch.",
            ],
            ["A job id is a uuid, never an integer.", "The 202 carries `job_id`.", "Cancel job 7 of the queue."],
        ),
        Detector(
            "an internal decision label",
            False,
            # ``[a-e]`` suffix included: ``W7a`` and ``D3a`` are the forms this
            # codebase writes, and a trailing ``(?![A-Za-z0-9])`` alone made
            # every suffixed label invisible.
            #
            # **``C``, ``G`` and ``M`` need a closing bracket; ``D``, ``R`` and
            # ``K`` do not.** A review finding is always written ``(review,
            # C26)`` or ``(G3)``, so the bracket is free for those two -- and
            # without it ``C0`` and ``C1`` are the control-character ranges.
            # ``M`` joins them because ``M`` is SVG's moveto command, and a
            # detector that reddens on every icon in a built page is one
            # somebody deletes rather than obeys.
            # **The trailing context is a set, and reading this corpus widened
            # it twice.** A label is written `R1's`, `R10/M6`, `R6-follow-up`
            # and `M6:` here as well as `(R1)` and `R1.`, and the first
            # version -- which allowed only `)`, whitespace, `.`, `,`, `;` and
            # `:` -- missed all four shapes: a possessive, a slash-joined
            # pair, a hyphenated suffix and a colon. Twelve live instances
            # survived the sweep because of it, found by reading rather than
            # by the guard.
            #
            # `F` and `N` join `D`, `R` and `K` in the permissive branch:
            # neither is ambiguous, and both name labels in this tree.
            #
            # **The residual, stated rather than closed:** a bare `C<n>` or
            # `G<n>` followed by a space -- `The C1 defect` -- is not caught,
            # because dropping the bracket requirement for those two would
            # claim the C0 and C1 control-character ranges and SVG's moveto
            # command, which the two fixtures below pin. Neither appears in
            # this tree today, which is a judgement about the corpus rather
            # than a guarantee about the pattern.
            re.compile(
                r"(?<![A-Za-z0-9])(?:spec |design )?"
                r"(?:[DRKFN]\d{1,2}[a-e]?(?:[–\-/,] ?[DRKFN]?\d{1,2}[a-e]?)*(?![A-Za-z0-9])(?=[)\s.,;:'\u2019/-]|$)"
                r"|[CGM]\d{1,2}[a-e]?(?=[):'\u2019]))"
            ),
            [
                "Parses `/robot_description` (R47, spec §4.6): ROS graph input.",
                "contracts_constants.json (R47, W0) is vendored.",
                "The bridge renders no page for an app user (D75).",
                "sensor_msgs (R47): imported at module load.",
                "The bearer is checked for life before it is used (review, C97).",
                "The claim was that the assertion was already green (G88).",
                # The four shapes the first version of this pattern missed.
                "# same class as R47/M99 in camera_sources.py:",
                "the WebSocket only ever carried the *conversation* (R47's",
                "# existing R47-follow-up handling below (an unresolved reference",
                '"""M99: the module\'s stated rule -- a raw exception',
            ],
            [
                "The bound is 1..10000 and the ceiling is 64 MiB.",
                "A `.dae` referencing `textures/skin.png` uploads under that name.",
                "The colour is #C1D2E3 in both themes.",
                # The control-character ranges.
                "`json.dumps` escapes the C0 controls and nothing else.",
                "A C1 control never reaches the parser.",
                # SVG path data. An icon in a built page is not a decision label.
                '<path stroke-linecap="round" d="M4 12h16M4 6h16M4 18h16"/>',
                # A camera model number, which the suffix rule must not claim.
                "Tested against a D435i depth camera.",
                # Lower case is an identifier, not a label, everywhere in this
                # package -- a letter and a digit is what a loop variable, a
                # node name and a fixture name look like.
                "node_name=\"n7_camera\", topic=\"/n7_image_raw\"",
                "for k1, r1 in sorted(rows):",
            ],
            case_sensitivity=(
                "``d3``, ``r1``, ``n2``, ``f1`` and ``k1`` are ordinary identifiers, loop "
                "variables and fixture names, and this package writes all of them. Only the "
                "capitalised form is a decision label. The cost is stated rather than "
                "hidden: a lower-cased label baked into a test's node names is invisible "
                "to this detector, and the one instance in this tree was found by reading."
            ),
        ),
        Detector(
            "a path into another repository",
            False,
            # Four shapes: a file (it ends in an extension), a directory of any
            # depth that ends in a slash, a bare two-segment ``cloud/src``, and
            # a **relative** ``../contracts/...``.
            # The third one stops at two segments on purpose -- ``the
            # cloud/bridge/job are entirely untouched`` is a slash-alternation in
            # a sentence, not a path, and a pattern that took any depth without a
            # trailing slash claimed it.
            #
            # **The relative shape is the one this tree actually writes**, and
            # the lookbehind that guards the other three -- ``(?<![\w./-])`` --
            # is exactly what made it invisible: a ``/`` sits immediately
            # before the repository name. A shell script mounting a sibling
            # checkout, a comment pointing at ``../cloud/src/...``: both walked
            # through. It is also the dominant shape in every repository that
            # adopts this module next, so the blind spot travels.
            re.compile(
                r"(?<![\w./-])(?:{repos})/(?:[A-Za-z0-9_.-]+/)*\.?[A-Za-z0-9_-][A-Za-z0-9_.-]*"
                r"\.[A-Za-z]{{1,6}}(?![A-Za-z0-9])"
                r"|(?<![\w./-])(?:{repos})(?:/[A-Za-z0-9_.-]+)+/(?![A-Za-z0-9])"
                r"|(?<![\w./-])(?:{repos})/[A-Za-z0-9_.-]+(?![A-Za-z0-9_./-])"
                r"|\.\.(?:/\.\.)*/(?:{repos})(?![A-Za-z0-9_-])".format(repos=repos)
            ),
            [
                "Vendored from contracts/artifacts/constants.json.",
                "The shape is defined in {}/src/rest.ts.".format(sibling),
                "Read from infra/example/compose.yml.",
                "A generated script lives under infra/example/.",
                "The value is read from docs/example/shell.mjs.",
                # A dotfile: the shape a commit message written under the
                # constraint forbidding it already carried once.
                "`{}/.gitlab-ci.yml` already installs git on the same line.".format(sibling),
                # A bare two-segment directory, with no trailing slash and no
                # extension.
                "It is produced nowhere in cloud/example.",
                # The relative shapes, both live in this tree.
                "if [ -d ../contracts/artifacts/schema ]; then",
                "see ../{}/src/example.ts for the other half".format(sibling),
                "..; the checkout is at ../{}".format(sibling),
            ],
            [
                "The package is built from `setup.py`.",
                # This repository's own name is not a sibling reference.
                # Asserted rather than assumed.
                "`{}/test/conftest.py` is this package's own fixture module.".format(self_name),
                "The route is `/api/robots/:id/config/draft`.",
                "the cloud/bridge/job are entirely untouched",
                "The window is 3/4 of the configured budget.",
                # A relative path that is not a sibling repository: an artefact
                # beside the checkout, this package's own tree, and the
                # traversal segment the mesh loader rejects.
                "cp ../ros-humble-fleetless-bridge_*.deb dist/",
                "`../../fleetless_bridge/contracts_constants.json` is a second copy.",
                "A `../` segment lands outside `base` lexically and is rejected.",
            ],
            case_sensitivity=(
                "the repository names are lower case by definition -- they are directory "
                "names in a git group -- while ``Docs/`` and ``SDK/`` opening a sentence or "
                "a heading are English. Under the flag every ``Contracts/`` in prose becomes "
                "a path."
            ),
        ),
        # ---- STANCE: ordinary text addressed to the wrong reader. Code scope
        # only, because the shipped documents are legitimately about themselves.
        #
        # **Three classes left this half and did not come back.** An internal
        # decision label, a path into another repository and a citation of a
        # document the reader does not have were all exempt from every
        # ``.md``, ``.txt``, ``.yaml`` and ``.yml`` file, and from
        # ``debian/changelog`` and ``debian/copyright`` -- the two files that
        # ship inside every binary package. None of the three is ever a
        # document legitimately addressing its own reader: a README citing an
        # internal register is the same leak as a source file doing it, and one
        # such line was live in ``rosdep/`` for exactly that reason. Measured
        # before the move: those three had 1 hit between them over the whole
        # document half, and it was the leak.
        #
        # The two that stayed are the two a document really does write about
        # itself. "This project follows the Contributor Covenant" is what a
        # CONTRIBUTING file is *for*, and a changelog entry saying a fix was
        # measured rather than assumed is its own honest voice. Six live hits
        # sit under the first of those today, all of them legitimate, which is
        # what makes this half worth keeping rather than folding in.
        Detector(
            "a reference to the writer's own project rather than to the reader's",
            True,
            re.compile(r"\bthis (?:project|repository|repo|train|wave|codebase|delta|playbook)(?:'s)?\b", re.IGNORECASE),
            ["the failure this project keeps paying for", "resolved via this repo's own rosdep source"],
            ["this bridge has no sessions", "this node publishes nothing"],
        ),
        Detector(
            "a reference to a document the reader does not have",
            False,
            # ``(?<![\w-])...(?![\w-])`` rather than ``\b``: a hyphenated or
            # suffixed word that merely contains ``spec`` is a domain term, and a
            # detector that reddens on one is a detector somebody deletes.
            #
            # **A bare ``spec`` is not enough, and this corpus is why.** The
            # first version of this pattern took ``(?:the )?spec`` with the
            # determiner optional, which is what the TypeScript original does.
            # Ported to Python it claimed twelve ordinary identifiers --
            # ``for name, spec in parameters.items()``, and the ``spec`` that
            # ``importlib.util.find_spec`` returns, which is the name the
            # standard library itself gives that object. The class is about
            # *citing* a document the reader does not have, and a citation
            # always carries a determiner, a section sign or a colon: ``the
            # spec``, ``spec §4.6``, ``Spec: always built``. So the citation
            # shapes are named, and a lone identifier is left alone.
            # **A design document is cited the same way and was missed
            # entirely by the first version**, which knew only `spec` and
            # `playbook`. This tree cites one thirty times over as `design
            # doc`, `design §4`, `(design, "...")` and `the pressure
            # design` -- a document no reader of the package has, cited by
            # section and by internal sub-project. The bare word `design` is
            # deliberately not matched: "by design" and "the design is" are
            # ordinary English, and a detector that reddens on those is one
            # somebody deletes rather than obeys. Only the citation shapes
            # are named.
            re.compile(
                r"(?<![\w-])(?:"
                r"(?:the|a|this|that|per|see|in|against|from|by)\s+spec(?![\w-])"
                # ``(`` joins ``§`` and ``:``: ``Spec (2026-08-25-...-design.md)``
                # opens the first line of a shipped module docstring and was a
                # citation nothing caught. The lookbehind at the top of the
                # group keeps ``find_spec(`` and ``parameter-spec`` out.
                r"|spec\s*[§:(]"
                # An internal document by **filename**. A dated markdown name
                # is how every design and plan in the private tree is spelt,
                # and the one that shipped was cited by name rather than by
                # section, so no determiner branch could see it.
                r"|\d{4}-\d{2}-\d{2}-[a-z0-9][a-z0-9-]*\.(?:md|txt)(?![\w-])"
                r"|designs?\s+docs?(?![\w-])"
                r"|design\s*§"
                # **The trailing set again.** `\(design[,)]` took a comma or a
                # closing paren; this tree also writes `(design "…"` and
                # `(design: "…"`, and both walked through the branch on the
                # commit that added it. A space, a quote and a colon are the
                # same citation.
                r"|\(design\s*[,)\"':\u201c]"
                r"|(?:the\s+)?(?:pressure|bridge-pressure)\s+design(?:'s)?(?![\w-])"
                r"|sub-projects?\s+\d"
                r"|(?:the\s+)?ledger(?:'s)?(?![\w-])"
                r"|(?:the\s+)?playbooks?(?![\w-])"
                r"|register rows?(?![\w-])"
                r"|deferral register(?![\w-])"
                r"|discipline gate(?![\w-])"
                r"|gate steps?(?![\w-])"
                r")",
                re.IGNORECASE,
            ),
            [
                "the spec calls this the capture time",
                "Deferred by the playbook to a later step.",
                "Turning a ROS image message into bytes (spec §10).",
                "# Spec: always built, even when the budget is unset.",
                "Three cases, matching the register row's own three states.",
                # The design-document citation shapes, all live in this tree.
                'One plain dict (design doc, "Pressure counters, collected',
                "Lazy half of design §4's decode gate: when nobody has a live",
                'Decode-on-demand (design, "Decode only what someone will see")',
                "out of scope for the pressure design by name; if",
                "(design, bridge-pressure sub-project 7 task 9)",
                "same job (ledger decision 9: only the REST layer tells them apart)",
                "the ledger's own decision keeps the suite off real hardware",
                # The two spellings the comma-or-paren version missed.
                'The session on the prioritized writer (design "One prioritized',
                '(design: "live traffic always sits in a higher tier")',
                # The two shapes that shipped inside the package: a spec cited
                # by filename, and the parenthesis spelling of the citation.
                "Spec (2026-08-25-example-design.md): a developer configures the",
                "See 2026-08-25-example-design.md for the admission rule.",
            ],
            [
                "RFC 6749 §5.2 permits additional members.",
                "The OpenAPI specification is generated.",
                "'parameter-spec': parameter_spec,",
                "The `parameterSpec` schema is exported from the barrel.",
                # Ordinary Python identifiers, which the first version claimed.
                "        spec = _parse_parameter_spec(entry)",
                "    for name, spec in parameters.items():",
                '        spec = importlib.util.find_spec("numpy")',
                # The bare word, which is ordinary English and must stay so.
                "The run itself is unbounded, by design.",
                "The design is a diff against the live subscriptions.",
                # A parenthesis that opens with the bare word: widening the
                # trailing set must not claim ordinary English.
                "(design of the queue is a diff against the live set)",
                # A date that is not a document name, and a version that is not
                # a date: neither may be claimed by the filename branch.
                "The sample is stamped 2026-08-25 and never re-stamped.",
                "It reads `1.2.3-rc1.md` out of the fixture directory.",
            ],
        ),
        Detector(
            "how the behaviour was found rather than what it is",
            True,
            # Widened past ``found by <Capital>``: the live instances were
            # ``found by re-reading``, ``discovered by looking at it``,
            # ``confirmed by the wave lead`` and ``has already been caught by
            # once``. Still narrow enough that ``the row is found by token hash``
            # and ``caught by the body schema`` -- both correct descriptions of
            # behaviour -- stay green.
            # ``^[ \t]*`` rather than ``^``: this scan runs line by line, and a
            # sentence that opens a wrapped paragraph is indented. With a bare
            # ``^`` the detector saw only column zero and missed two live
            # instances inside docstrings -- the shape it exists for.
            re.compile(
                # **The flag is the pattern's, and the two branches that need
                # a capital say so inline.** This used to be the other way
                # round -- case-sensitive throughout with ``(?i:)`` on four
                # branches -- and the same shape one detector along was the
                # blocking defect of this round: a flag that is present or
                # absent by habit, on a pattern nobody can read the flags of.
                # An opt-out that is written down is a decision; an opt-out
                # that is an absence is an accident waiting to be found by
                # somebody reading.
                r"(?-i:(?:^[ \t]*|[.!?]\s+|\*\*)Measured\b)"
                r"|\bmeasured (?:against|on|through|either|before|in a|at the)\b"
                r"|\bthe first version of this\b"
                r"|\bused to (?:say|read|claim)\b"
                # ``[A-Z]`` here means a capital -- a name -- and nothing else,
                # so it opts out too. Under the pattern's flag it would match
                # any letter at all and claim "caught by a schema".
                r"|\b(?:found|caught|discovered|confirmed|noticed|spotted) by (?:a |the )?"
                r"(?:review|reading|re-reading|looking|inspection|hand|eye|\w+ lead\b|(?-i:[A-Z]))"
                r"|\balready been caught by\b"
                # A role nobody outside the company can identify. `lead` on its
                # own is ordinary English ("lead time", "leads to"), so only
                # the citation shapes count: the hyphenated title, and `the
                # lead` immediately followed by the thing being cited.
                r"|\bteam-leads?\b"
                r"|\bthe lead(?:'s)?\s+"
                r"(?:review|gate|bench|decision|ask|call|condition|instruction|own|found|binding)"
                r"|\b(?:the\s+)?lead(?:'s)?\s+(?:review|gate fixture|bench|binding)\b",
                re.IGNORECASE,
            ),
            [
                "Measured against a real container, not assumed.",
                "    Measured directly (a standalone repro, not this adapter):",
                "This comment used to say the opposite.",
                "found by re-reading rather than by a failure",
                "renders with one surface missing, discovered by looking at it",
                "confirmed by the wave lead on both REST and realtime",
                "a defect class this file has already been caught by once",
                # The role citations, which no outside reader can resolve.
                "The redesign (team-lead): the capture option is process-global.",
                "per the lead's binding reading of the contract",
                "The end-to-end bug the lead found and diagnosed: fixing a wrong",
                "the second half (lead's review): the entry is rebuilt",
                # The same citations opening a sentence, which a case-sensitive
                # branch cannot see.
                "Team-lead's own re-derivation of the arithmetic, measured rather than",
                "Lead's review: the entry is rebuilt rather than patched.",
                "Used to say the opposite, before the ceiling moved.",
                # A capital opening the sentence, which the pattern-wide flag
                # is what makes visible.
                "Confirmed by the wave lead, not assumed.",
            ],
            [
                "A malformed uuid in a frame is caught by the schema first.",
                "The row is found by token hash and the previous hash is gone.",
                "A link measured below 0.5 B/s floors to 0 here.",
                "The robot is discovered by mDNS on the local segment.",
                # `lead` as ordinary English, which must stay ordinary.
                "A long lead time on the ceiling check is acceptable.",
                "Lead time on the ceiling check is not part of the budget.",
                "That leads to a second stat call, which this avoids.",
                # The two inline opt-outs, each pinned by the thing it lets
                # through. Lower-cased, the first is an ordinary description of
                # a unit; the second is an ordinary description of a mechanism.
                "    measured in milliseconds rather than in frames",
                "A malformed uuid is caught by a schema before the route sees it.",
            ],
            case_sensitivity=(
                "the pattern is case-insensitive apart from two inline ``(?-i:)`` branches: "
                "sentence-initial ``Measured`` (lower-cased it is an ordinary unit "
                "description) and the ``found by <Name>`` capital, which under the flag "
                "would match any letter and claim ``caught by a schema``."
            ),
        ),
        # ---- Back to MARKERS. These three name internal *coordinates*, not a
        # stance: a document citing them is as unreadable to an outsider as a
        # source file citing them, so they run over both scopes.
        Detector(
            "an internal review, gate or finding citation",
            False,
            # Three vocabularies from one internal process, none of which a
            # reader outside the company can resolve.
            #
            # ``(review,`` was already claimed by the decision-label detector,
            # because a finding id always followed it. ``(review;`` carries no
            # id and walked through every detector -- the trailing set again,
            # one detector over.
            #
            # **``the gate`` is the hard one and this pattern deliberately does
            # not take it whole.** ``_GatedSource`` and the decode gate are
            # real objects in this package, so "the gate moves from when a
            # source starts" and "the decode gate is closed" are correct
            # descriptions of behaviour. What no outside reader can resolve is
            # the gate as a *process that acts*: its finding, its bug, its
            # requirement, its fixture, its script, and being found by it. Those
            # are named. **The residual, stated rather than closed:** a bare
            # ``the gate`` used as that process -- "so the gate can run this" --
            # is not caught, and cannot be without claiming the objects.
            re.compile(
                r"\(review[,;:)]"
                # ``(contracts review)`` -- a qualifier in front of the word,
                # so the paren was no longer immediately before ``review``.
                # Only the *closing* form: ``(review the log before filing)``
                # is an instruction to a reader and stays ordinary.
                r"|\([a-z][a-z-]*\s+reviews?\)"
                # A numbered gate is a coordinate, and ``_GatedSource`` and the
                # decode gate are never numbered. A numbered one shipped in a
                # tracked file, in exactly the parenthesised form below.
                r"|\bgates?[\s-]+\d"
                # The people, not the role. ``a reviewer reading the diff``
                # is what a config field's own description says about the
                # package's *users*, and it is in the vendored contracts
                # schemas -- bytes this repository does not write. What no
                # outside reader can resolve is a reviewer of *this work*
                # acting in the past tense, which is what the verb set names.
                r"|\breviewers?\s+(?:happened|found|raised|caught|noticed|spotted|asked|"
                r"wanted|said|flagged|missed|disagreed|objected)\b"
                # The residual this detector documented and then had live in a
                # tracked file: ``the gate`` as a process that *executes*.
                # **The modal is not what separates the two** -- ``the gate
                # could not simply stay`` is ``_GatedSource``, and a pattern
                # keyed on ``can|could`` claims it. The *verb* is: a decode
                # gate moves, opens and closes; only a process runs, invokes
                # and asks. So the modal is optional and the verb is the test.
                r"|\bthe gate\s+(?:(?:can|could|will|would|must|should|cannot)\s+)?"
                r"(?:runs?|ran|invokes?|invoked|asks?|asked|reports?|reported)\b"
                r"|\b(?:see|per|from|against)\s+the\s+review\b"
                # ``(contracts, review of the plan)``: the citation names the
                # document the review was OF, which is a document the reader
                # does not have either.
                r"|\breviews?\s+of\s+the\b"
                r"|\bthe review\s+(?:raised|said|found|asked|closed|wanted|noticed)\b"
                r"|\b(?:the|a)\s+gate(?:'s)?\s+"
                r"(?:findings?|bug|report|requirement|own|fixture|script|run|steps?|review|check)\b"
                r"|\b(?:found|caught|raised|reported|confirmed|discovered)\s+by\s+(?:a|the)\s+gate\b"
                r"|\((?:[a-z-]+\s+)?finding\)"
                r"|\b(?:this|that|a parked|the (?:first|second|third|fourth|next|earlier|later|open|parked))"
                r"\s+finding\b",
                re.IGNORECASE,
            ),
            [
                "zero-gap burst (review; only reproduced there because any real gap",
                "# `texture_path is None` (silence closed, see the review",
                "# checks, per the review: (a) a terminal update *is* emitted",
                "The honest-mistake case the review raised, distinct from the",
                "`finish`), not once it is merely known -- the gate's finding",
                "The exact shape of the gate's bug, reproduced directly.",
                "independent witness (the gate's own requirement for the step),",
                "This script is the gate fixture the check runs against.",
                "yet retire its job from `active_jobs`. Found by the gate: a job",
                "# wrong for `RosRuntime.stop()` (second finding): the",
                "even though this finding turned out to be about something else",
                # The three shapes that were live in the tree after the sweep.
                "classify a failure -- \"nobody is listening\" (gate 88c) -- not to",
                "`observed_at_ms` (contracts review): when the **robot** observed",
                "# Left at the front of the file so the gate can run this by hand.",
                "not only the two `except` blocks two different reviewers happened to",
                "The second reviewer raised the same point about the ordering.",
                "`state='refused_busy'` (contracts, review of the plan) is the bridge's",
            ],
            [
                # The gate objects this package really has.
                "So the gate moves from *when a source starts* to *when*",
                "The decode gate is closed while nobody has a live subscriber.",
                "gate = camera_sources._ValueGate(initial=\"a\")",
                # Ordinary uses of the two nouns.
                "A design review is scheduled for the next release.",
                "(review the log before filing anything)",
                "a high-entropy literal here is a secret-scanner finding",
                "The `caplog` finding above is worked around in `conftest.py`.",
                # The gate objects again, now against the verb branch: neither
                # of these verbs is in it, and neither may become so.
                "The gate is closed while nobody has a live subscriber.",
                "the gate moves to the first frame that arrives",
                # The role, in the vendored contracts schemas, about this
                # package's own users rather than about its authors.
                "**visible** rather than plausible, so a reviewer reading the diff sees it",
                # The modal without the verb: `_GatedSource`, not the process.
                "see its docstring for why the gate could not simply stay \"start the",
            ],
        ),
        Detector(
            "the umbrella repository by name",
            False,
            # The private repository that holds the deployment, the checks and
            # the documentation. It is not a path, so the sibling-path detector
            # cannot see it: the shape is the bare word, in a mount argument, a
            # skip reason and a comment about where a second copy of a vendored
            # file lives. Ten instances survived every other detector.
            #
            # The lookahead keeps the ordinary English noun out. A guard that
            # reddens on "an umbrella term" is one somebody deletes.
            re.compile(r"\bumbrella\b(?!\s+(?:term|organi[sz]ation|group|clause))", re.IGNORECASE),
            [
                "# Mount the umbrella checkout when there is one, so the sync test",
                "compares this file byte-for-byte against the umbrella's copy",
                'reason="no umbrella checkout with contracts/ next to us"',
            ],
            [
                "A gateway is an umbrella term for both roles.",
                "The `umbrella_id` column is not read by this package.",
            ],
        ),
        Detector(
            "an internal fix-round or change label",
            False,
            # ``fix round 2`` and ``NEW-1`` are coordinates in a review cycle:
            # they say *which pass of an internal process* produced a line, and
            # there is no pass one, two or three that a public reader can look
            # up. Twenty-five instances, in two files, under no detector at all.
            # **This was the one detector in twenty-one with no
            # ``re.IGNORECASE``, and nothing said so.** ``fix round 2`` was
            # swept out of a docstring while ``Fix round 3`` four lines below
            # survived, and so did every ``fix-round-3``: the separator was a
            # space only. Both holes are the same hole -- a pattern shaped like
            # the instances somebody happened to have in front of them. The
            # flag is now a declared property of every detector (see
            # ``Detector``), and the separator is a class here as it is in
            # every numbered label above.
            #
            # ``NEW-\d`` keeps its capitals with an inline ``(?-i:)``: ``new-1``
            # and ``new-2`` are ordinary hyphenated English, and this package
            # writes ``new-style`` and ``new-3`` shaped version fragments.
            # **A severity is a coordinate too.** ``Important 1`` and a
            # docstring opening ``CRITICAL:`` are a review's own grading of a
            # finding, and neither means anything to a reader who cannot see
            # the finding. The all-caps branch keeps its capitals inline for
            # the same reason ``NEW-`` does: ``critical section``,
            # ``safety-critical`` and ``a critical failsafe timer`` are all in
            # this package and all ordinary.
            #
            # ``same round`` is the round label with its number left out. The
            # lookahead is the whole reason it is safe: this package writes
            # ``a second round trip`` about a real network round trip in eight
            # places.
            re.compile(
                r"\bfix[\s-]?round[\s-]?#?\d"
                r"|(?-i:\bNEW-\d)"
                r"|(?-i:\b(?:CRITICAL|IMPORTANT|BLOCKER|NIT)\b)"
                r"|\b(?:important|critical|blocker|major|minor)[\s-]+\d"
                r"|\b(?:same|next|previous|earlier|later|another|second|third)[\s-]+"
                r"rounds?\b(?![\s-]*trip)",
                re.IGNORECASE,
            ),
            [
                "# Restored (fix round 3): a concurrently running `apply_config`",
                "reason (fix round 3, NEW-2 below) -- an in-flight enforcement pass",
                "# NEW-1: check that *this* slug was actually removed",
                # The two spellings that survived the first sweep.
                "Fix round 7, IMPORTANT: the capitalised form, which is the one",
                "**Fix round 8**: the same label in bold, which is how it wrapped",
                "the CRITICAL fix-round-7 spelling, hyphenated and capitalised",
                "verified under fix-round-8, hyphenated and lower case",
                # The severity forms, both live in this package until they
                # were read out of it.
                "# too (Important 9, same round): a severity is a coordinate too",
                '"""CRITICAL: admission read `active_sum` from',
            ],
            [
                "The round-trip time is 12 ms on this link.",
                "A `NEW_STATE` constant is not exported.",
                "It rounds to the nearest 4 kbps.",
                # The lower-cased form the inline flag deliberately leaves
                # alone, so that turning the flag off inside one branch is a
                # thing a fixture states rather than a thing a reader notices.
                "A new-2 style suffix is not part of this grammar.",
                # Lower-case severity, which this package writes everywhere.
                "Blocking it on one camera's critical failsafe timer is wrong.",
                "their critical sections must never overlap",
                "a security-critical copy is not what this is",
                # A real network round trip, which the round label must not
                # claim. Both spellings, and both are in this package.
                "a second cross-thread round trip for every camera apply",
                "reachable within the second round trip of connecting",
            ],
            case_sensitivity=(
                "the pattern is case-insensitive apart from ``NEW-\\d``, which keeps its "
                "capitals inline: lower-cased, it is ordinary hyphenated English."
            ),
        ),
    ]


#: Anti-vacuity floor over the detector list itself. Deleting a detector -- the
#: cheapest way to make a sweep look finished -- is red here, and the count is
#: asserted rather than derived so that a rename cannot quietly take one out.
#:
#: The count is the weaker half of that guarantee: it cannot tell a deletion
#: from a deletion plus an addition. ``DETECTOR_NAMES`` below is the half that
#: can, and the suite asserts membership against it.
DETECTOR_FLOOR = 21

#: Every detector, by name.
#:
#: A count is a requirement about a set guarded by a number: delete one class
#: and add another and the floor is satisfied while the class you cared about
#: is gone. This is the set itself, asserted by the suite, so removing a class
#: is red even when the arithmetic still works. Adding one is red too, on
#: purpose -- a new class is a line in this list, which is one line of review.
DETECTOR_NAMES = {
    "German prose",
    "an internal feature id (FL-0xx)",
    "an internal defect id (DEF-nnn)",
    "an internal wave label (W1..W9x)",
    "an internal wave or train label",
    "an absolute path from a developer machine",
    "the reference robot by name",
    "an internal host or workspace name",
    "a person by first name",
    "an internal review codename",
    "a file only the maintainers have",
    "an internal task or step id",
    "an internal pipeline or job number",
    "an internal decision label",
    "a path into another repository",
    "a reference to a document the reader does not have",
    "an internal review, gate or finding citation",
    "the umbrella repository by name",
    "an internal fix-round or change label",
    "a reference to the writer's own project rather than to the reader's",
    "how the behaviour was found rather than what it is",
}

#: The stance half, named rather than counted.
#:
#: Selecting it as "the last two" would be a requirement about a set guarded by
#: an ordering: one marker appended makes a different two "the stance classes",
#: silently, with the documents then exempt from the wrong ones. Named here and
#: cross-checked against each detector's own ``stance`` flag by the suite, so
#: the two have to agree -- and the suite asserts this set *by membership*, not
#: by its size, because agreeing with itself is not the same claim as being
#: right. A detector added without a decision lands on the MARKER side, which is
#: the strict one.
#:
#: Only two classes are here, and both are things a shipped document really does
#: say about itself. See the banner above the first of them for what left and
#: why.
STANCE = {
    "a reference to the writer's own project rather than to the reader's",
    "how the behaviour was found rather than what it is",
}

#: The stance class a commit message is also held to.
#:
#: A commit message is *about* this repository, so "this repository" and a
#: sentence about how a defect was found are its own voice and are left alone.
#: A path into a sibling repository is not: it names a file the reader of a
#: public history cannot open -- and it is now a marker class, so it is held
#: unconditionally and this set is empty of anything the filter would have
#: dropped.
COMMIT_EXTRA_STANCE = set()

#: Anti-vacuity floor for the commit-message check's own detector set.
#:
#: The number it replaces was 12 against 14 active detectors, which tolerated
#: losing two whole classes before it spoke. Derived from the two sets it is
#: about rather than typed, so a marker added or a stance class moved keeps it
#: exact by construction -- and the suite asserts the *names*, because a floor
#: that stays right by arithmetic is the failure this file already documents
#: one level up.
COMMIT_DETECTOR_FLOOR = len(DETECTOR_NAMES) - len(STANCE) + len(COMMIT_EXTRA_STANCE)

# --------------------------------------------------------------------------
# Scopes
# --------------------------------------------------------------------------

#: ``.mjs`` and ``.html`` are the browser half of ``tools/live-proof``: a
#: driver script and the page it serves, both addressed to a stranger
#: reading the code, so they are held to every class exactly as the Python
#: beside them is.
CODE_EXT = {".py", ".sh", ".json", ".xml", ".cfg", ".mjs", ".html"}
DOC_EXT = {".md", ".txt", ".yaml", ".yml"}

#: Extensionless files, and files whose extension says nothing, classified by
#: name. Both sets are explicit and ``scope_of`` raises on anything in neither,
#: so adding a file of a new kind is a decision somebody makes rather than a
#: file that silently stops being scanned. ``debian/rules`` is a makefile and
#: ``debian/postinst`` is a shell script; the rest of ``debian/`` and all of
#: ``apt/reprepro/conf/`` are declarative control files that describe this
#: package to its own reader.
CODE_NAMES = {"rules", "postinst", "preinst", "prerm", "postrm", "Dockerfile.dev"}
DOC_NAMES = {"changelog", "control", "copyright", "format", "distributions", "options",
             "fleetless_bridge", ".gitignore", "LICENSE", "NOTICE"}


def scope_of(path):
    """``'code'`` or ``'document'``. Raises on a file it cannot classify."""
    base = path.rsplit("/", 1)[-1]
    # A packaging template is scanned as the file it renders to. The packaging
    # is generated per ROS distribution (``tools/distros.sh``), so
    # ``debian/changelog`` is ``debian/changelog.in`` here -- and ``.in`` is an
    # extension that says nothing about what is inside, which is exactly the
    # state this function refuses to guess at. Stripping it asks the question
    # about the real file: a rendered ``debian/rules`` is still a makefile and
    # a rendered ``debian/changelog`` is still a document.
    if base.endswith(".in") and len(base) > 3:
        base = base[:-3]
    if base in CODE_NAMES:
        return "code"
    if base in DOC_NAMES:
        return "document"
    dot = base.rfind(".")
    ext = base[dot:] if dot > 0 else ""
    if ext in CODE_EXT:
        return "code"
    if ext in DOC_EXT:
        return "document"
    raise AssertionError(
        "{}: no scope for this file. Add its extension to CODE_EXT or DOC_EXT, or its name to "
        "CODE_NAMES or DOC_NAMES, in scripts/internal_markers.py -- an unclassified file would "
        "otherwise stop being scanned without anybody deciding that.".format(path)
    )


def _blank(match):
    """Replace a run with the same number of spaces.

    Length-preserving on purpose. ``normalise`` used to collapse a stripped URL
    to a single space, which moved every offset after it -- harmless while the
    only consumer was an excerpt window, and wrong the moment an offset has to
    be mapped back to the line it came from.
    """
    return " " * len(match.group(0))


def normalise(line, detector_name):
    """The German scan is the only one that strips anything."""
    if detector_name != "German prose":
        return line
    return CODESPAN_TOKEN.sub(_blank, URLISH.sub(_blank, line))


#: What a wrapped line carries in front of its prose: indentation, and the
#: comment marker if it is a comment. Nothing else -- a bullet or a quote
#: marker is left alone, because stripping those would join a list of separate
#: items into one sentence, which is a different text from the one on the page.
CONTINUATION_PREFIX = re.compile(r"^[ \t]*#*[ \t]*")


def wrapped_blocks(text):
    """Paragraphs, rejoined, with a map back to real line numbers.

    **Why this exists.** The scan used to be one line at a time, and this tree
    wraps prose at about 72 columns. Every multi-word pattern was therefore
    evadable by a line break in the middle of the phrase -- not deliberately,
    just by where the paragraph happened to wrap. Three internal citations were
    live for exactly that reason and the guard printed zero over all three:
    ``the task`` / ``brief names``, and ``this`` / ``wave found the hard way``.
    A guard whose answer depends on the column a sentence broke at is a guard
    that cannot say what it means.

    So each run of consecutive non-blank lines is joined with a single space,
    the leading indentation and comment marker of each line removed, and the
    detectors run over that as well as over the raw lines. A blank line ends a
    block: wrapping never crosses one, and joining across one would invent
    adjacency that the text does not have.

    :returns: a list of ``(joined_text, [(offset, line_number), ...])``. Blocks
        of a single line are left out -- the line scan has already seen those,
        and returning them would only be a second way to find the same hit.
    """
    blocks = []
    current = []
    for i, line in enumerate(text.split("\n"), start=1):
        prose = CONTINUATION_PREFIX.sub("", line).rstrip()
        if prose:
            current.append((prose, i))
        elif current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)

    out = []
    for block in blocks:
        if len(block) < 2:
            continue
        pieces, starts, offset = [], [], 0
        for prose, line_number in block:
            starts.append((offset, line_number))
            pieces.append(prose)
            offset += len(prose) + 1
        out.append((" ".join(pieces), starts))
    return out


def _line_at(starts, offset):
    """The line a joined-block offset came from: the last piece that starts at
    or before it. A match is reported against the line it *starts* on, which is
    where a person has to go and look."""
    line_number = starts[0][1]
    for start, candidate in starts:
        if start > offset:
            break
        line_number = candidate
    return line_number


#: What a Python source file can put between two words of one sentence, other
#: than a single space.
#:
#: **The string junction is a hole the rejoining did not close.** A phrase split
#: across implicit string concatenation --
#: ``"...the task " / "brief names..."`` -- survives BOTH earlier views: the raw
#: lines never see the whole phrase, and joining them leaves ``" "`` sitting in
#: the middle of it. Measured on this tree: 148 such line pairs exist in shipped
#: ``.py`` files, and none of them hides a marker today. That is what makes this
#: a hole rather than a leak, and it is exactly why it is worth closing -- a
#: guard with a known way through it is a guard somebody walks through later,
#: and nothing about the way a `raise` happens to wrap is a decision anybody
#: makes on purpose. Explicit ``+`` concatenation is the same evasion written
#: differently, so it is here too.
#:
#: **The whitespace run is the one property other than case that could be made
#: general.** ``this  task`` and ``task  brief`` (two spaces) score zero against
#: every detector, because a pattern written ``\s`` or with a literal space
#: matches one character. Collapsing runs in a derived view fixes that for all
#: twenty-one at once, rather than editing twenty-one regexes -- the same move
#: as making case a declared property of the set instead of a flag per pattern.
SPLICE = re.compile(r"[\"'](?:[ \t]*\+)?[ \t]*[rRbBuUfF]{0,2}[\"']|\s{2,}")


def spliced(text):
    """``text`` with string junctions removed and whitespace runs collapsed,
    plus a map from every offset in the result back to its offset in ``text``.

    **This view can only ADD hits, never hide one**, which is the property that
    makes it safe to bolt on: it is a third view beside the raw lines and the
    rejoined paragraphs, and a line is reported if any view finds it. So a
    pattern anchored at the start of a line, or one that wants the real spacing,
    is unaffected by anything here.

    The cost is the one the rejoining already has (see ``wrapped_blocks``): a
    derived view can *invent* a hit that is in neither the bytes nor the intent
    -- ``"...a gate" "9 later..."`` becomes a numbered gate. It fails toward
    red, and a false positive is read by a person while a false negative is
    read by nobody.

    :returns: ``(text_without_the_junctions, [offset_in_text, ...])``, the list
        indexed by offset in the result and one entry longer than it, so that a
        match at the very end still maps. Returning the map rather than blanking
        in place is deliberate: the point of the view is to CLOSE the gap
        between two words, and a length-preserving substitution cannot.
    """
    pieces, index, pos = [], [], 0
    for m in SPLICE.finditer(text):
        pieces.append(text[pos:m.start()])
        index.extend(range(pos, m.start()))
        if m.group(0).isspace():
            pieces.append(" ")
            index.append(m.start())
        pos = m.end()
    pieces.append(text[pos:])
    index.extend(range(pos, len(text) + 1))
    return "".join(pieces), index


def hits_in(detector, label, text):
    """Run one detector over a text and return ``label:line: excerpt`` for every
    hit.

    Three views of the same bytes: the raw lines, which is what an anchored
    pattern (``^[ \t]*Measured``) needs; the rejoined paragraphs from
    ``wrapped_blocks``, which is what a multi-word pattern needs when the prose
    wrapped in the middle of it; and ``spliced``, which is what one needs when
    the two halves are separated by a Python string junction or by a run of
    spaces. A line is reported once however many views found it.

    What these three views do NOT close is stated once, in ``Detector``,
    beside the case rule it belongs with -- naming it in both places would be
    two statements of one policy, which is the failure this module is about.
    """
    found = {}
    blocks = wrapped_blocks(text)
    for i, line in enumerate(text.split("\n"), start=1):
        m = detector.pattern.search(normalise(line, detector.name))
        if m:
            start = max(0, m.start() - 40)
            found[i] = "{}:{}: ...{}...".format(label, i, line[start:m.start() + 80].strip())
    for joined, starts in blocks:
        for m in detector.pattern.finditer(normalise(joined, detector.name)):
            i = _line_at(starts, m.start())
            if i in found:
                continue
            start = max(0, m.start() - 40)
            found[i] = "{}:{}: ...{}... (wrapped)".format(
                label, i, joined[start:m.start() + 80].strip()
            )
    # The spliced view runs over the raw lines AND over the rejoined
    # paragraphs, because the two holes compose: a phrase can be split by a
    # string junction that is itself in the middle of a wrapped block.
    views = [([(0, i)], line) for i, line in enumerate(text.split("\n"), start=1)]
    views.extend((starts, joined) for joined, starts in blocks)
    for starts, view in views:
        text_out, index = spliced(view)
        if text_out == view:
            continue
        for m in detector.pattern.finditer(normalise(text_out, detector.name)):
            i = _line_at(starts, index[m.start()])
            if i in found:
                continue
            start = max(0, m.start() - 40)
            found[i] = "{}:{}: ...{}... (spliced)".format(
                label, i, text_out[start:m.start() + 80].strip()
            )
    return [found[i] for i in sorted(found)]


# --------------------------------------------------------------------------
# The three sets
# --------------------------------------------------------------------------


def tracked_files(root=ROOT):
    """The bytes a public mirror gets, asked of git.

    Raises rather than returning an empty list if git will not answer. The
    likely cause inside a container is ``detected dubious ownership``: the tree
    is mounted from the host, so its files belong to the host user while the
    process runs as root. ``run-tests.sh`` grants ``safe.directory`` for that
    reason. Falling back to a directory walk here would turn a broken
    precondition into a smaller scan, which is the one outcome a guard must
    never have.
    """
    proc = subprocess.run(
        ["git", "ls-files", "-z"], cwd=str(root), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        raise AssertionError(
            "git ls-files failed in {}: {}\nThe scanned set cannot be computed, and a "
            "smaller set is not an acceptable answer.".format(root, proc.stderr.decode().strip())
        )
    out = [f for f in proc.stdout.decode().split("\0") if f]
    # **git answering successfully with nothing is a third state**, and it was
    # slipping through: a directory that is a git repository with no commit and
    # no index answers exit 0 and an empty list. The suite's floor catches it;
    # the command line did not, and printed
    # ``scanned 62 files (tracked 0, installed 24, vendored 39); 23 hit(s)``
    # over an exported tree -- a real hit count, a real-looking summary, and
    # two thirds of the tree never opened. Anti-vacuity belongs on the function
    # both callers use, not only on the one that has a test.
    if not out:
        raise AssertionError(
            "git ls-files answered with nothing in {}. That is not an empty repository "
            "this guard may scan -- it is the scanned set failing to be computed, and a "
            "smaller set is not an acceptable answer.".format(root)
        )
    return out


def _setup_kwargs(root):
    """``setup.py``'s own ``setup()`` call, captured by running it.

    Asked of the file that decides rather than re-derived: a static reading
    would have to resolve ``packages=[package_name]`` and a ``package_data``
    dict keyed by the same variable, and a re-derivation that drifts from
    ``setup.py`` is a second answer to the question this exists to ask once.
    """
    import setuptools

    captured = {}

    def fake_setup(**kwargs):
        captured.update(kwargs)

    original = setuptools.setup
    original_argv = sys.argv
    original_cwd = os.getcwd()
    setuptools.setup = fake_setup
    sys.argv = ["setup.py", "--version"]
    try:
        os.chdir(str(root))
        source = (root / "setup.py").read_text()
        exec(compile(source, str(root / "setup.py"), "exec"), {"__name__": "__main__", "__file__": str(root / "setup.py")})
    finally:
        setuptools.setup = original
        sys.argv = original_argv
        os.chdir(original_cwd)
    if not captured:
        raise AssertionError("setup.py did not call setup(); the installed set would be empty")
    return captured


#: The maintainer scripts dpkg puts into a binary package's control archive.
#: The set is dpkg's own, not a list of this repository's directories.
MAINTAINER_SCRIPTS = ("preinst", "postinst", "prerm", "postrm", "config", "triggers")


def _packaging_source(root, path):
    """The file in THIS repository that becomes ``path`` in the built package.

    The packaging is rendered per ROS distribution (``tools/distros.sh``), so
    ``debian/control``, ``debian/rules``, ``debian/changelog`` and
    ``debian/postinst`` exist in the checkout only as ``<name>.in`` templates
    and are written out by ``./build-deb.sh``.

    This function is the fix for a gap ``installed_files``' own docstring
    predicted before the templates existed: "a maintainer script generated at
    build time (a ``.postinst.in``) would sit outside every set, and the floor
    that exists to catch exactly that would still be green." A set built from
    the rendered names alone would name four files that are not in the
    checkout -- and ``public_file_set`` would report them as *missing* rather
    than scan the templates that actually carry the text.

    A path that exists as neither is returned unchanged, so a genuinely absent
    file is still reported as absent rather than silently dropped.
    """
    if (root / path).is_file():
        return path
    if (root / (path + ".in")).is_file():
        return path + ".in"
    return path


def installed_files(root=ROOT):
    """The bytes that land on a robot.

    Three sources, each asked of the file that decides it:

    * ``setup.py``'s ``packages``, ``package_data`` and ``data_files``;
    * ``debian/rules``' explicit ``install`` lines, which is how ``NOTICE``
      reaches the package -- ``setup.py`` does not ship it;
    * ``debian/copyright``, ``debian/changelog`` and ``debian/control``
      itself, which land in the control archive every binary package carries
      without being named anywhere in ``debian/rules``. ``debian/control`` is
      exactly what ``apt-cache show`` and ``dpkg -s`` print on the robot, so a
      sentence in its ``Description`` naming an Ubuntu release reaches every
      reader of the installed package -- a distro-name sweep built from an
      earlier version of this set (which carried the other two but not this
      one) missed exactly that;
    * the **maintainer scripts** dpkg installs from ``debian/`` into the
      control archive of every binary package, equally without being named
      anywhere. ``debian/postinst`` is the one that matters: it runs as root on
      every robot that installs this package. It is git-tracked today, so
      ``tracked`` happens to cover it and the union saved the set -- but that
      makes the separate floor on ``installed`` weaker than it reads. A
      maintainer script generated at build time (a ``.postinst.in``, a
      ``debian/*.install`` emitted by ``rules``) would sit outside every set,
      and the floor that exists to catch exactly that would still be green.

    ``MAINTAINER_SCRIPTS`` is a named list and that is not the mistake it looks
    like: the names are **dpkg's**, fixed by the packaging format, not this
    repository's choice of directory layout. Only the ones that exist on disk
    are added, because dpkg installs only those.
    """
    kwargs = _setup_kwargs(root)
    out = set()
    for package in kwargs.get("packages", []):
        directory = root / package.replace(".", "/")
        for name in sorted(os.listdir(str(directory))):
            if name.endswith(".py"):
                out.add("{}/{}".format(package.replace(".", "/"), name))
    for package, patterns in (kwargs.get("package_data") or {}).items():
        directory = package.replace(".", "/")
        for pattern in patterns:
            for name in sorted(os.listdir(str(root / directory))):
                if fnmatch.fnmatch(name, pattern):
                    out.add("{}/{}".format(directory, name))
    for _destination, sources in kwargs.get("data_files", []):
        out.update(sources)
    rules = (root / _packaging_source(root, "debian/rules")).read_text()
    for match in re.finditer(r"^\t\s*install\s+(?:-\S+\s+|\d+\s+)*(\S+)\s+\S+\s*$", rules, re.MULTILINE):
        out.add(_packaging_source(root, match.group(1)))
    out.update(_packaging_source(root, f) for f in ("debian/copyright", "debian/changelog", "debian/control"))
    for name in MAINTAINER_SCRIPTS:
        for candidate in ("debian/{}".format(name), "debian/{}.in".format(name)):
            if (root / candidate).is_file():
                out.add(candidate)
    return sorted(out)


#: The trees copied out of another repository. A walk rather than a list of
#: filenames: the vendored set changes with every re-vendor, and a list would be
#: a requirement about a set guarded by examples.
VENDORED_ROOTS = ["test/contracts", "fleetless_bridge/contracts_constants.json"]


def vendored_files(root=ROOT):
    """Bytes this repository did not write, and therefore did not sweep.

    Raises on an empty answer for the same reason ``tracked_files`` does: a
    walk whose roots have been renamed away returns ``[]`` and looks exactly
    like a repository that vendors nothing.
    """
    out = []
    for entry in VENDORED_ROOTS:
        absolute = root / entry
        if absolute.is_file():
            out.append(entry)
        elif absolute.is_dir():
            for dirpath, _dirnames, filenames in os.walk(str(absolute)):
                for name in sorted(filenames):
                    out.append(os.path.relpath(os.path.join(dirpath, name), str(root)))
    if not out:
        raise AssertionError(
            "none of VENDORED_ROOTS {} exists under {}. The vendored set cannot be "
            "computed, and a smaller set is not an acceptable answer.".format(VENDORED_ROOTS, root)
        )
    return sorted(out)


class FileSet:
    __slots__ = ("tracked", "installed", "vendored", "union", "readable", "binary", "missing")


def public_file_set(root=ROOT):
    """The three sets and their union, plus the files skipped for being binary --
    counted rather than dropped silently, so "nothing was skipped" and "the skip
    list ate the tree" are different answers."""
    result = FileSet()
    result.tracked = tracked_files(root)
    result.installed = installed_files(root)
    result.vendored = vendored_files(root)
    result.union = sorted(set(result.tracked) | set(result.installed) | set(result.vendored))
    result.readable, result.binary, result.missing = [], [], []
    for f in result.union:
        absolute = root / f
        if not absolute.is_file():
            result.missing.append(f)
            continue
        if b"\0" in absolute.read_bytes()[:8192]:
            result.binary.append(f)
        else:
            result.readable.append(f)
    return result


def _cli():
    """``python3 scripts/internal_markers.py`` prints every hit in the working
    tree, grouped by class. The pytest check is the guard; this is the sweeping
    tool."""
    file_set = public_file_set()
    active = detectors()
    exempt = {"scripts/internal_markers.py"}
    contents = {}
    for f in file_set.readable:
        if f not in exempt:
            contents[f] = (ROOT / f).read_text(errors="replace")
    total = 0
    for d in active:
        hits = []
        for f, text in contents.items():
            if d.stance and scope_of(f) != "code":
                continue
            hits.extend(hits_in(d, f, text))
        total += len(hits)
        if hits:
            print("== {} ({}) ==".format(d.name, len(hits)))
            for h in hits:
                print("   " + h)
    print("scanned {} files (tracked {}, installed {}, vendored {}); {} hit(s)".format(
        len(contents), len(file_set.tracked), len(file_set.installed), len(file_set.vendored), total))
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(_cli())
