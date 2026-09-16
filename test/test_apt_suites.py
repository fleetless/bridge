# SPDX-License-Identifier: Apache-2.0
"""The apt repository's suites, and the one command that files a .deb into one.

`apt/reprepro/conf/distributions` is kept in git so the suite list,
architectures and signing key fingerprint are reviewable. Nothing here talks
to `apt.fleetless.dev`: what's checkable from a checkout is that the declared
suites match the distribution table, the three stanzas differ only where
they're supposed to, and `apt/publish-deb.sh` refuses to file a package into
the wrong one.

**The check that actually matters is `apt/verify-suites.sh`**: it builds a
throwaway reprepro repository from this config, serves it, and runs
`apt-get install` in each distribution's own container. Needs Docker and
three built `.deb`s, so it's not a pytest.
"""
import pathlib
import re
import shutil
import subprocess
import tempfile

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONF = ROOT / "apt" / "reprepro" / "conf" / "distributions"

#: Derived from tools/distros.sh, not retyped here: a fourth distribution
#: added to the table and forgotten here used to pass silently, because
#: SUPPORTED and CODENAMES were their own copies of the table.
def _table_distros():
    return subprocess.run(
        ["bash", "-c", '. tools/distros.sh; printf "%s" "$FLEETLESS_DISTROS"'],
        cwd=str(ROOT), capture_output=True, text=True, check=True,
    ).stdout.split()


def _table_codename(distro):
    return subprocess.run(
        ["bash", "-c", ". tools/distros.sh; fleetless_distro_field {} codename".format(distro)],
        cwd=str(ROOT), capture_output=True, text=True, check=True,
    ).stdout


SUPPORTED = _table_distros()
CODENAMES = {d: _table_codename(d) for d in SUPPORTED}

#: The upstream version this tree would publish for a given suite —
#: debian/changelog.in's top entry, codename substituted. The same derivation
#: apt/check-suites.sh uses, so a fixture and the script it drives never
#: silently drift onto two different "correct" versions.
_CHANGELOG_VERSION = re.search(
    r"^ros-@ROS_DISTRO@-fleetless-bridge \((.*)\) @DEB_CODENAME@;",
    (ROOT / "debian" / "changelog.in").read_text(), re.M,
).group(1)


def expected_version(distro):
    return _CHANGELOG_VERSION.replace("@DEB_CODENAME@", CODENAMES[distro])


#: Fields that must match across every stanza. `Architectures` is the
#: dangerous one: the package is `Architecture: all`, so reprepro files it
#: into every architecture the suite lists -- list only one and the other
#: gets nothing, silently. No publish-time failure; an arm64 robot just sees
#: `Unable to locate package`.
SHARED_FIELDS = ["Origin", "Label", "Architectures", "Components", "SignWith"]


def stanzas():
    """The config's stanzas, as dicts, comments dropped."""
    out = []
    current = {}
    for line in CONF.read_text().splitlines():
        if line.startswith("#"):
            continue
        if not line.strip():
            if current:
                out.append(current)
                current = {}
            continue
        key, _, value = line.partition(":")
        current[key.strip()] = value.strip()
    if current:
        out.append(current)
    return out


def test_the_declared_suites_are_exactly_the_distributions_the_table_supports():
    # SUPPORTED comes from the table itself (_table_distros above), so this
    # isn't "the table against a copy of itself" -- it's the config's stanza
    # order against the table, the pair that can actually drift: a suite with
    # no table entry can never be built for, and a table entry with no suite
    # builds a .deb with nowhere to go.
    assert SUPPORTED, "the table named no distributions at all"
    assert [s["Codename"] for s in stanzas()] == SUPPORTED


def test_every_stanza_is_complete_and_agrees_with_the_others():
    got = stanzas()
    assert len(got) == len(SUPPORTED), "expected one stanza per distribution"
    for field in SHARED_FIELDS:
        values = {s["Codename"]: s.get(field) for s in got}
        assert all(values.values()), "{} is missing from a stanza: {}".format(field, values)
        assert len(set(values.values())) == 1, (
            "{} differs between suites, which is a repository that looks correct and "
            "serves differently per suite: {}".format(field, values)
        )
    for stanza in got:
        assert stanza["Architectures"].split() == ["amd64", "arm64"]
        assert stanza["Components"] == "main"


def test_the_signing_key_is_named_by_fingerprint_and_not_by_uid():
    # A uid-shaped SignWith can match more than one key, because a retired key
    # left in the same keyring carries a byte-identical uid, and then signing
    # picks one silently. Forty hex digits is the only spelling that cannot.
    for stanza in stanzas():
        assert re.fullmatch(r"[0-9A-F]{40}", stanza["SignWith"]), stanza["SignWith"]


def test_each_description_names_its_own_distribution_and_nobody_else_s():
    for stanza in stanzas():
        mine = stanza["Codename"]
        description = stanza["Description"]
        assert mine.capitalize() in description, description
        assert CODENAMES[mine] in description, description
        for other in SUPPORTED:
            if other == mine:
                continue
            assert other.capitalize() not in description, (
                "the {} suite's description names {}: {}".format(mine, other, description)
            )
            assert CODENAMES[other] not in description, description


# ---------------------------------------------------------------------------
# publish-deb.sh: the suite is derived from the package, never typed
# ---------------------------------------------------------------------------

def _fake_deb(directory, package, version):
    """A .deb with nothing in it but a control file.

    `publish-deb.sh` reads `Package:` and `Version:` via `dpkg-deb`, so a
    real payload adds nothing here and would need the whole packaging build.
    """
    root = directory / (package + "_" + version)
    (root / "DEBIAN").mkdir(parents=True)
    (root / "DEBIAN" / "control").write_text(
        "Package: {}\nVersion: {}\nArchitecture: all\nMaintainer: t <t@example.com>\n"
        "Description: fixture\n".format(package, version)
    )
    out = directory / "{}_{}_all.deb".format(package, version)
    subprocess.run(["dpkg-deb", "--build", str(root), str(out)],
                   capture_output=True, check=True)
    return out


@pytest.fixture
def publish_env():
    """A basedir with a conf/, and a place to build fixture .debs.

    Temporary, not `apt/reprepro`: `publish-deb.sh` ends in a `reprepro`
    call, and pointing it at the checkout would write reprepro's database
    into the source tree on any machine that has reprepro installed. Here it
    doesn't, so every case below is decided by the script's own guards --
    asserted by reading the message, never by exit code alone.
    """
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="fleetless-publish-"))
    (tmp / "base" / "conf").mkdir(parents=True)
    shutil.copy(str(CONF), str(tmp / "base" / "conf" / "distributions"))
    (tmp / "debs").mkdir()
    try:
        yield tmp
    finally:
        shutil.rmtree(str(tmp), ignore_errors=True)


def run_publish(publish_env, deb):
    return subprocess.run(
        ["./apt/publish-deb.sh", str(publish_env / "base"), str(deb)],
        cwd=str(ROOT), capture_output=True, text=True,
    )


def test_publish_deb_passes_expected_version_to_check_suites():
    # The failure path this closes (publish-deb.sh -> check-suites.sh, inside
    # apt/verify-suites.sh's throwaway container) can't be driven end to end
    # here: reprepro isn't installed in this test image, so every
    # publish_env-based test below stops at the `reprepro includedeb` call,
    # before check-suites.sh is ever invoked.
    # test_expected_version_lets_check_suites_run_with_no_debian_directory_at_all
    # proves the flag itself works; this proves publish-deb.sh actually
    # passes it, by reading the source instead of the unreachable runtime
    # behaviour.
    text = (ROOT / "apt" / "publish-deb.sh").read_text()
    call = re.search(r"^if !.*?--require-published", text, re.M | re.S).group(0)
    assert "check-suites.sh" in call and "--expected-version" in call, call


def test_a_correct_package_reaches_its_own_suite_and_only_that_one(publish_env):
    # The control case. Without it, every refusal below is satisfied by a
    # script that refuses everything -- the shape a guard takes when its own
    # happy path stopped working.
    deb = _fake_deb(publish_env / "debs", "ros-jazzy-fleetless-bridge", "3.0.1-0noble")
    result = run_publish(publish_env, deb)
    assert "-> suite jazzy (and no other)" in result.stderr, result.stderr
    for other in ("humble", "lyrical"):
        assert "suite " + other not in result.stderr, result.stderr
    # Got past every guard and reached the reprepro call -- as far as a
    # machine without reprepro can go. Asserted so "the guards passed" isn't
    # confused with "the script did nothing".
    assert "reprepro" in result.stderr


def test_a_package_whose_version_names_another_distribution_is_refused(publish_env):
    # The half-rendered tree: the package name says jazzy and the debian
    # revision says jammy. reprepro would file it happily.
    deb = _fake_deb(publish_env / "debs", "ros-jazzy-fleetless-bridge", "3.0.1-0jammy")
    result = run_publish(publish_env, deb)
    assert result.returncode != 0
    assert "noble" in result.stderr and "jammy" in result.stderr, result.stderr
    assert "reprepro" not in result.stderr, "it reached the publish before refusing"


def test_a_package_for_an_unknown_distribution_is_refused_by_name(publish_env):
    deb = _fake_deb(publish_env / "debs", "ros-jazy-fleetless-bridge", "3.0.1-0jammy")
    result = run_publish(publish_env, deb)
    assert result.returncode != 0
    assert "jazy" in result.stderr
    for distro in SUPPORTED:
        assert distro in result.stderr
    assert "reprepro" not in result.stderr


def test_a_package_that_is_not_this_one_at_all_is_refused(publish_env):
    # A stray file in dist/. Guessing a suite from a prefix is how one gets
    # published.
    deb = _fake_deb(publish_env / "debs", "some-other-package", "1.0-1")
    result = run_publish(publish_env, deb)
    assert result.returncode != 0
    assert "some-other-package" in result.stderr
    assert "reprepro" not in result.stderr


# ---------------------------------------------------------------------------
# check-suites.sh: "nobody published it" and "somebody broke it" are two answers
# ---------------------------------------------------------------------------
#
# The distinction is the whole point of the script. Before it existed, one
# branch covered both, and the consequence was concrete: `deploy.sh` called
# that branch from `verify_app`, so from the moment the third suite was
# declared until it was published, every cloud deploy would have ended FATAL
# after deploying successfully. An instrument whose exit code is always 1
# stops being read.

import contextlib
import functools
import http.server
import socketserver
import threading


def _write_suite(root, suite, packages, *, arches=("amd64", "arm64"), release=True,
                  version=None, empty=False):
    """One suite's served tree. `packages` is the list of Package: names.

    `version` defaults to what THIS suite would correctly serve
    (`expected_version(suite)`), so a fixture using the right package name is
    right about the version by default -- a test proving the version check
    works passes a wrong one explicitly. `empty=True` writes a served,
    signed, zero-stanza index (what `reprepro export` produces before any
    .deb is filed), ignoring `packages`.
    """
    if version is None:
        version = expected_version(suite)
    for arch in arches:
        d = root / "dists" / suite / "main" / ("binary-" + arch)
        d.mkdir(parents=True, exist_ok=True)
        text = "" if empty else "".join(
            "Package: {}\nVersion: {}\nArchitecture: all\nFilename: pool/x.deb\n\n".format(p, version)
            for p in packages
        )
        (d / "Packages").write_text(text)
    if release:
        (root / "dists" / suite).mkdir(parents=True, exist_ok=True)
        (root / "dists" / suite / "InRelease").write_text("-----BEGIN PGP SIGNED MESSAGE-----\n")


@contextlib.contextmanager
def _served(root):
    """`root` on a loopback port, for as long as the block runs."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(root))
    handler.log_message = lambda *a, **k: None
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            yield "http://127.0.0.1:{}".format(httpd.server_address[1])
        finally:
            httpd.shutdown()
            thread.join(timeout=5)


def run_check(base, *args):
    return subprocess.run(
        ["./apt/check-suites.sh", base, *args],
        cwd=str(ROOT), capture_output=True, text=True,
    )


@pytest.fixture
def served_root():
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="fleetless-suites-"))
    try:
        yield tmp
    finally:
        shutil.rmtree(str(tmp), ignore_errors=True)


def _publish_all(root):
    for suite in SUPPORTED:
        _write_suite(root, suite, ["ros-{}-fleetless-bridge".format(suite)])


def test_every_suite_published_and_correct_exits_zero(served_root):
    # The control. Without it, every case below is satisfied by a script that
    # never returns 0.
    _publish_all(served_root)
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 0, result.stderr
    for suite in SUPPORTED:
        assert "{}/amd64: ros-{}-fleetless-bridge".format(suite, suite) in result.stderr


def test_a_suite_nobody_published_is_reported_and_is_not_an_error(served_root):
    # THE case this split exists for: it must not abort a caller that is
    # deploying something else.
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"])
    _write_suite(served_root, "jazzy", ["ros-jazzy-fleetless-bridge"])
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 2, result.stderr
    assert "lyrical: NOT PUBLISHED" in result.stderr
    assert "not published yet: lyrical" in result.stderr
    # and the suites that ARE published are still checked, so this is not a
    # branch that gives up on the first absence.
    assert "humble/amd64: ros-humble-fleetless-bridge" in result.stderr


def test_the_same_state_is_an_error_when_publishing_is_the_task(served_root):
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"])
    _write_suite(served_root, "jazzy", ["ros-jazzy-fleetless-bridge"])
    with _served(served_root) as base:
        result = run_check(base, "--require-published")
    assert result.returncode == 1, result.stderr
    assert "lyrical" in result.stderr


def test_a_suite_carrying_another_distributions_package_is_an_error(served_root):
    _publish_all(served_root)
    _write_suite(served_root, "humble",
                 ["ros-humble-fleetless-bridge", "ros-jazzy-fleetless-bridge"])
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 1, result.stderr
    assert "ros-jazzy-fleetless-bridge" in result.stderr
    assert "BROKEN" in result.stderr


def test_a_suite_served_without_its_signed_index_is_an_error_not_an_absence(served_root):
    # Served Packages, no InRelease: apt refuses the whole suite. That is a
    # publish that half worked, and reporting it as "not published yet" would
    # send somebody to run a publish that has already run.
    _publish_all(served_root)
    (served_root / "dists" / "jazzy" / "InRelease").unlink()
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 1, result.stderr
    assert "jazzy: BROKEN" in result.stderr
    assert "InRelease(404)" in result.stderr
    assert "NOT PUBLISHED" not in result.stderr


def test_a_suite_serving_one_architecture_only_is_an_error(served_root):
    # The package is Architecture: all and reprepro files it into every
    # architecture the suite lists, so this is a suite that answers "Unable to
    # locate package" to half the fleet while looking published.
    _publish_all(served_root)
    shutil.rmtree(str(served_root / "dists" / "lyrical" / "main" / "binary-arm64"))
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 1, result.stderr
    assert "lyrical: BROKEN" in result.stderr
    assert "arm64(404)" in result.stderr


def test_a_suite_serving_the_previous_version_is_not_published_yet(served_root):
    # The incident this file exists after: an earlier version's copyright
    # declared the software closed and not redistributable, a later one
    # relicensed to Apache-2.0, and a check comparing only Package: names
    # reported the stale one "(expected)". The check must SEE the version --
    # but the right package at an OLDER version isn't broken, it's the
    # previous release still being served before the next publish. This test
    # used to assert BROKEN for exactly that, and since deploy.sh turns
    # BROKEN into FATAL, every cloud deploy aborted from the moment the tree
    # said 3.0.1 while apt still served 3.0.0; a review caught it. The
    # publish step still runs this check with --require-published, where
    # "not yet" IS the failure, and a stale index after a publish reads as
    # exactly that.
    _publish_all(served_root)
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"], version="3.0.0-0jammy")
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 2, result.stderr
    assert "NOT PUBLISHED at this version" in result.stderr
    assert "BROKEN" not in result.stderr
    assert "3.0.0-0jammy" in result.stderr
    assert expected_version("humble") in result.stderr
    with _served(served_root) as base:
        strict = run_check(base, "--require-published")
    assert strict.returncode == 1, strict.stderr


def test_a_suite_serving_a_version_the_tree_never_had_is_broken(served_root):
    # A version NEWER than the tree, or one the tree never produced, is not a
    # state a pending publish can explain: somebody filed something by hand.
    # That stays BROKEN -- the older-than-tree carve-out above must not widen
    # into "any version is fine".
    _publish_all(served_root)
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"], version="9.9.9-0jammy")
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 1, result.stderr
    assert "humble" in result.stderr and "BROKEN" in result.stderr
    assert "9.9.9-0jammy" in result.stderr


def test_an_older_version_the_changelog_never_declared_is_broken_not_not_published(served_root):
    # The narrower gap the test above doesn't cover: OLDER than the tree,
    # which `dpkg --compare-versions … lt` alone can't tell apart from a real
    # past release. "2.0.3-0jammy" sits between two versions
    # debian/changelog.in actually lists (2.0.2 and 2.1.0) and was never one
    # of them -- a version this package never built, filed by hand, not the
    # ordinary gap before the next publish. Before this test's fix,
    # `classify_index` read only "lt" and reported this as NOT PUBLISHED,
    # exit 2 -- the same reassuring message a genuinely stale index gets.
    _publish_all(served_root)
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"], version="2.0.3-0jammy")
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 1, result.stderr
    assert "humble" in result.stderr and "BROKEN" in result.stderr
    assert "NOT PUBLISHED" not in result.stderr
    assert "2.0.3-0jammy" in result.stderr


def test_an_older_version_the_changelog_does_list_stays_not_published(served_root):
    # The other direction of the same fix, proven both ways: a version
    # further back than "one release ago" -- and one this package genuinely
    # shipped -- stays NOT PUBLISHED. Guards against narrowing the carve-out
    # to "exactly one version behind" instead of "any version this changelog
    # lists".
    _publish_all(served_root)
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"], version="1.0.0-0jammy")
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 2, result.stderr
    assert "NOT PUBLISHED at this version" in result.stderr
    assert "BROKEN" not in result.stderr
    assert "1.0.0-0jammy" in result.stderr


def test_an_exported_but_empty_suite_is_not_published_rather_than_broken(served_root):
    # `reprepro export` on a suite that was just declared in conf/distributions
    # writes a signed, empty index: every probe answers 200 and nothing is
    # indexed. That is "nobody has published yet", the same state a 404'd
    # suite reports -- not "somebody filed the wrong package", which a bare
    # comparison of an empty name list against the expected one would report.
    _write_suite(served_root, "humble", ["ros-humble-fleetless-bridge"])
    _write_suite(served_root, "jazzy", [], empty=True)
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 2, result.stderr
    assert "jazzy" in result.stderr and "NOT PUBLISHED" in result.stderr
    assert "jazzy" not in " ".join(
        line for line in result.stderr.splitlines() if "BROKEN" in line
    )


def test_a_single_redirect_is_followed(served_root):
    # apt itself follows redirects; a repository reached through one (the
    # http:// -> https:// spelling, or a fronting proxy) is not broken.
    _publish_all(served_root)
    handler = functools.partial(_RedirectOnceHandler, directory=str(served_root))
    handler.log_message = lambda *a, **k: None
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            # /straight/... is what gets redirected (once) to /...; asking
            # check-suites.sh for THAT base is what makes every one of its
            # probes take the hop this test exists to prove works.
            base = "http://127.0.0.1:{}/straight".format(httpd.server_address[1])
            result = run_check(base)
        finally:
            httpd.shutdown()
            thread.join(timeout=5)
    assert result.returncode == 0, result.stderr


class _RedirectOnceHandler(http.server.SimpleHTTPRequestHandler):
    """308s everything under /straight/ to itself without the prefix.

    One hop: `check-suites.sh` follows AT MOST one redirect, so the fixture
    only needs to prove that one works, not a chain.
    """

    def do_GET(self):
        if self.path.startswith("/straight/"):
            self.send_response(308)
            self.send_header("Location", self.path[len("/straight"):])
            self.end_headers()
            return
        return super().do_GET()

    def do_HEAD(self):
        if self.path.startswith("/straight/"):
            self.send_response(308)
            self.send_header("Location", self.path[len("/straight"):])
            self.end_headers()
            return
        return super().do_HEAD()


def test_broken_outranks_unpublished(served_root):
    # Both conditions at once must exit 1, not 2: an error that is reported as
    # an outstanding item is an error nobody acts on.
    _write_suite(served_root, "humble",
                 ["ros-humble-fleetless-bridge", "ros-lyrical-fleetless-bridge"])
    _write_suite(served_root, "jazzy", ["ros-jazzy-fleetless-bridge"])
    with _served(served_root) as base:
        result = run_check(base)
    assert result.returncode == 1, result.stderr
    assert "NOT PUBLISHED" in result.stderr
    assert "broken:" in result.stderr


def test_expected_version_lets_check_suites_run_with_no_debian_directory_at_all():
    # `apt/verify-suites.sh`'s throwaway container mounts only `apt/` and
    # `tools/` -- no `/debian` at all -- and without a flag to supply the
    # expected version, publish-deb.sh's `check_apt_suites` call would read
    # `debian/changelog.in` unconditionally: the documented pre-publish
    # rehearsal died on a missing file before serving or installing anything.
    # Reproduces that container's shape exactly: copy ONLY apt/ and tools/
    # into an empty root, run check-suites.sh with --expected-version -- the
    # flag a caller that already has a concrete version (as publish-deb.sh
    # does, from the .deb's own Package/Version) passes instead of letting
    # this script re-derive it.
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="fleetless-suites-nodebian-"))
    try:
        shutil.copytree(str(ROOT / "apt"), str(tmp / "apt"))
        shutil.copytree(str(ROOT / "tools"), str(tmp / "tools"))
        assert not (tmp / "debian").exists()
        basedir = tmp / "base"
        (basedir / "conf").mkdir(parents=True)
        shutil.copy(str(CONF), str(basedir / "conf" / "distributions"))
        result = subprocess.run(
            ["./apt/check-suites.sh", "--local", str(basedir), "--distro", "humble",
             "--expected-version", "3.1.0-0jammy", "--require-published"],
            cwd=str(tmp), capture_output=True, text=True,
        )
        # Nothing was actually filed into this basedir, so the real answer is
        # NOT PUBLISHED -- the point here is that it gets that far at all,
        # rather than dying on a missing debian/changelog.in first.
        assert "debian/changelog.in" not in result.stderr, result.stderr
        assert "NOT PUBLISHED" in result.stderr, result.stderr
        assert result.returncode == 1, result.stderr
    finally:
        shutil.rmtree(str(tmp), ignore_errors=True)


def test_without_expected_version_the_same_missing_debian_directory_is_the_old_failure():
    # The control for the test above: without --expected-version, the same
    # debian-less tree fails the way C11 described, so the fix above is
    # proven against the failure it replaces, not against a strawman.
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="fleetless-suites-nodebian-ctrl-"))
    try:
        shutil.copytree(str(ROOT / "apt"), str(tmp / "apt"))
        shutil.copytree(str(ROOT / "tools"), str(tmp / "tools"))
        basedir = tmp / "base"
        (basedir / "conf").mkdir(parents=True)
        shutil.copy(str(CONF), str(basedir / "conf" / "distributions"))
        result = subprocess.run(
            ["./apt/check-suites.sh", "--local", str(basedir), "--distro", "humble",
             "--require-published"],
            cwd=str(tmp), capture_output=True, text=True,
        )
        assert result.returncode == 1, result.stderr
        assert "could not read a version out of debian/changelog.in" in result.stderr, result.stderr
    finally:
        shutil.rmtree(str(tmp), ignore_errors=True)


def test_a_repository_that_does_not_answer_is_not_an_unpublished_one(served_root):
    # Costs a few seconds of retries on purpose. "The host is down" and "this
    # suite was never published" are the two states this file keeps apart --
    # a check answering "not published yet" to a dead host would be the
    # worst version of that confusion: a green-ish exit 2 for a repository
    # serving nothing at all.
    #
    # Port 1 on loopback: privileged, nothing binds it, so the connection is
    # refused immediately rather than timing out.
    result = run_check("http://127.0.0.1:1")
    assert result.returncode == 1, result.stderr
    assert "did not answer" in result.stderr
    assert "not a suite being" in result.stderr
    assert "NOT PUBLISHED" not in result.stderr


class _FailSecondReadHandler(http.server.SimpleHTTPRequestHandler):
    """Answers the amd64 Packages URL's status probe normally, then aborts
    every later request for it (a bare connection close, no bytes) -- the
    shape of a connection that answers the status probe fine and dies
    partway through the actual read (a reset, a reload mid-transfer). The
    other two probes (arm64 Packages, InRelease) always answer normally, so
    the script gets past "served and complete" into fetch_index -- the
    branch this handler exists to reach; nothing else here drives a real
    curl process into it.
    """

    _amd64_hits = 0

    def do_GET(self):
        if self.path.endswith("/binary-amd64/Packages"):
            type(self)._amd64_hits += 1
            if type(self)._amd64_hits > 1:
                self.close_connection = True
                return
        return super().do_GET()


def test_a_connection_that_dies_reading_is_not_a_broken_suite(served_root):
    # The status probe (http_code) and the read (fetch_index) retry the same
    # way for the same reason: a transient read failure used to produce an
    # empty body, which classify_index reported as BROKEN -- a specific,
    # false claim ("this suite serves another distribution's package") about
    # a repository merely unreachable for a moment. No fixture elsewhere in
    # this file drives a real subprocess into fetch_index's own
    # retry-exhausted branch; this one does.
    _publish_all(served_root)
    _FailSecondReadHandler._amd64_hits = 0
    handler = functools.partial(_FailSecondReadHandler, directory=str(served_root))
    handler.log_message = lambda *a, **k: None
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            base = "http://127.0.0.1:{}".format(httpd.server_address[1])
            result = run_check(base)
        finally:
            httpd.shutdown()
            thread.join(timeout=5)
    assert result.returncode == 1, result.stderr
    assert "did not answer while READING" in result.stderr
    assert "partway through, not a suite" in result.stderr
    assert "being unpublished or broken" in result.stderr
    assert "BROKEN" not in result.stderr
