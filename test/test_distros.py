# SPDX-License-Identifier: Apache-2.0
"""The distribution table, and what it renders.

`tools/distros.sh` is the one place that knows which ROS distributions this
package is built for and what differs between them. Nothing else in the suite
reaches it: it's shell, it runs before any container starts, and a mistake
there builds a package that is wrong but builds clean — `ros-<typo>-rclpy`, a
numpy bound fatal on one Ubuntu release and fine on another. So these checks
test the *rendered* result, not the table's text.
"""
import os
import pathlib
import re
import shutil
import subprocess
import tempfile

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]

SUPPORTED = ["humble", "jazzy", "lyrical"]


def sh(script, check=True):
    """Run a snippet with tools/distros.sh sourced, from the repository root."""
    return subprocess.run(
        ["bash", "-c", ". tools/distros.sh\n" + script],
        cwd=str(ROOT), capture_output=True, text=True, check=check,
    )


def test_the_table_lists_exactly_the_distributions_this_package_supports():
    # Named rather than counted: "three entries" is satisfied by any three, and
    # the mistake this guards is a distribution silently dropped or added.
    got = sh("printf '%s' \"$FLEETLESS_DISTROS\"").stdout.split()
    assert got == SUPPORTED
    # kilted is deliberately absent (not an LTS release). Asserted, because
    # "we decided not to" and "nobody got round to it" look the same in a list.
    assert "kilted" not in got


def test_every_supported_distribution_has_every_field():
    fields = sh("printf '%s' \"$FLEETLESS_DISTRO_FIELDS\"").stdout.split()
    assert fields, "the field list is empty, so the completeness check below is vacuous"
    for distro in SUPPORTED:
        for field in fields:
            r = sh("fleetless_distro_field {} {}".format(distro, field), check=False)
            assert r.returncode == 0, "{}/{} is missing: {}".format(distro, field, r.stderr)


def test_an_unknown_distribution_is_refused_by_name():
    r = sh("fleetless_distro_require jazy", check=False)
    assert r.returncode != 0, "a typo was accepted"
    # The refusal must be readable -- the name rejected and what it could have
    # been -- or a bare exit code sends the reader to the wrong file.
    assert "jazy" in r.stderr
    for distro in SUPPORTED:
        assert distro in r.stderr


def test_a_field_the_table_does_not_carry_is_fatal_rather_than_empty():
    # Guards a half-written entry: distribution listed, some fields present,
    # one missing. An empty answer renders `Depends:` with nothing after it,
    # or `ros--fleetless-bridge` -- and dpkg would build it.
    r = sh("fleetless_distro_field humble no_such_field", check=False)
    assert r.returncode != 0
    assert "humble" in r.stderr and "no_such_field" in r.stderr
    assert r.stdout == "", "a missing field answered with something"


@pytest.fixture(scope="module")
def rendered():
    """Every supported distribution, rendered once."""
    out = {}
    tmp = tempfile.mkdtemp(prefix="fleetless-render-")
    try:
        for distro in SUPPORTED:
            dest = pathlib.Path(tmp) / distro
            r = sh("fleetless_distro_render {} . {}".format(distro, dest), check=False)
            assert r.returncode == 0, "rendering {} failed: {}".format(distro, r.stderr)
            out[distro] = dest
        yield out
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_the_rendered_packaging_names_its_own_distribution_everywhere(rendered):
    for distro, tree in rendered.items():
        control = (tree / "debian" / "control").read_text()
        assert re.search(r"^Source: ros-{}-fleetless-bridge$".format(distro), control, re.M)
        assert re.search(r"^Package: ros-{}-fleetless-bridge$".format(distro), control, re.M)

        rules = (tree / "debian" / "rules").read_text()
        assert "INSTALL_PREFIX = /opt/ros/{}".format(distro) in rules
        assert "PKG = ros-{}-fleetless-bridge".format(distro) in rules

        # postinst carries no distribution-specific content any more -- empty
        # (see debian/postinst.in) now that nothing runs at configure time.
        # It still renders, as a template with nothing to substitute, and is
        # still swept below for another distribution's name.

        # No OTHER distribution's name survives anywhere in the rendered
        # packaging: catches a template only half converted -- a
        # `Depends: ros-humble-rclpy` under `Package: ros-jazzy-fleetless-bridge`
        # is a package apt installs and a robot cannot import from.
        for other in SUPPORTED:
            if other == distro:
                continue
            for name in ("control", "rules", "postinst"):
                # Comments excluded -- a stated hole, not an implied one.
                # debian/rules.in's own header compares all three
                # distributions by name in one paragraph (where each ROS
                # Python site lives); a comment-inclusive sweep would flag
                # that in every rendered copy, so excluding comments is what
                # lets it exist. debian/postinst is empty today, so no
                # comment carries stale distribution content now -- but
                # nothing here would catch it if one did. What this sweep
                # actually catches: a half-converted template in a line dpkg
                # or the shell reads -- Depends:, the package name, an
                # install path -- the failure mode that ships a robot the
                # wrong package.
                lines = [
                    line for line in (tree / "debian" / name).read_text().splitlines()
                    if line.strip() and not line.lstrip().startswith("#")
                ]
                offending = [line for line in lines if other in line]
                assert not offending, "{}: debian/{} still names {}: {}".format(
                    distro, name, other, offending
                )


def test_the_rendered_dependencies_are_the_table_s_and_nothing_else(rendered):
    for distro, tree in rendered.items():
        table = sh("fleetless_distro_field {} ros_deps".format(distro)).stdout.split()
        assert table, "the table has no dependencies for " + distro
        control = (tree / "debian" / "control").read_text()
        depends = re.search(r"^Depends:(.*?)^\S", control, re.M | re.S).group(1)
        listed = [d.strip().rstrip(",") for d in depends.split("\n") if d.strip()]
        ros = [d for d in listed if d.startswith("ros-")]
        # Equality, not containment: a table entry that never reached the
        # control file and a control file carrying a dependency no table names
        # are both failures, and only equality sees the second one.
        assert ros == table, "{}: control has {} and the table has {}".format(distro, ros, table)


#: The non-ROS runtime Depends, exactly. This was the only rendered Depends
#: line nothing in the suite compared against a fixed set --
#: `test_the_rendered_dependencies_are_the_table_s_and_nothing_else` filters
#: to `d.startswith("ros-")` before its equality assertion, so deleting
#: `python3-aiortc` from a clone -- the aiortc-import failure a robot would
#: actually hit -- left every existing suite green: 1117 passed, 8 skipped.
#: Same set on all three distributions; if that ever needs to differ per
#: distribution this becomes a lookup like `ros_deps` above, not a shared
#: constant.
NON_ROS_DEPENDS = [
    "python3", "python3-opencv", "python3-defusedxml", "python3-numpy",
    "python3-aiohttp", "python3-aiortc",
]


def test_the_rendered_dependencies_include_every_non_ros_runtime_import(rendered):
    for distro, tree in rendered.items():
        control = (tree / "debian" / "control").read_text()
        depends = re.search(r"^Depends:(.*?)^\S", control, re.M | re.S).group(1)
        listed = [d.strip().rstrip(",") for d in depends.split("\n") if d.strip()]
        non_ros = [d for d in listed if not d.startswith("ros-") and d != "${misc:Depends}"]
        assert non_ros == NON_ROS_DEPENDS, (
            "{}: control's non-ROS Depends is {}, expected exactly {}".format(
                distro, non_ros, NON_ROS_DEPENDS
            )
        )


def test_the_rendered_changelog_carries_this_distribution_s_codename(rendered):
    for distro, tree in rendered.items():
        codename = sh("fleetless_distro_field {} codename".format(distro)).stdout
        entries = [
            line for line in (tree / "debian" / "changelog").read_text().splitlines()
            if line.startswith("ros-")
        ]
        assert len(entries) >= 5, "the changelog history did not survive rendering"
        for line in entries:
            assert line.startswith("ros-{}-fleetless-bridge (".format(distro)), line
            assert "-0{}) {};".format(codename, codename) in line, line


# There used to be a test here for the numpy bound rendered into
# rosdep/constraints.txt. That file, the `numpy_constraint` field and the
# PyPI install it bounded are all gone together: every runtime dependency is
# an apt package now, resolved by apt's own dependency solver within one
# distribution's package set, which is what made a separate numpy ceiling
# necessary in the first place.


def test_the_rendered_rules_install_into_this_distribution_s_own_site_directory(rendered):
    # Guards a .deb that `apt install` accepted silently and that raised
    # ModuleNotFoundError on import: humble's ROS Python packages live in
    # `local/lib/pythonX.Y/dist-packages`, jazzy's and lyrical's in
    # `lib/pythonX.Y/site-packages` -- only the right one is on that
    # distribution's PYTHONPATH.
    seen = set()
    for distro, tree in rendered.items():
        site = sh("fleetless_distro_field {} python_site".format(distro)).stdout
        rules = (tree / "debian" / "rules").read_text()
        assert "PYTHON3_SITE = $(INSTALL_PREFIX)/{}".format(site) in rules, distro
        seen.add(site)
    # Same reasoning as the numpy bound: if every distribution rendered the
    # same directory the field would be decorative, and the bug it exists for
    # would be back the moment somebody simplified it away.
    assert len(seen) > 1, "every distribution rendered the same site directory: " + str(seen)


def test_no_placeholder_survives_into_a_rendered_file(rendered):
    for distro, tree in rendered.items():
        for name in ("debian/control", "debian/rules", "debian/changelog",
                     "debian/postinst"):
            text = (tree / name).read_text()
            left = re.findall(r"@[A-Z_]{2,}@", text)
            assert not left, "{}: {} still holds {}".format(distro, name, left)
        # and the templates themselves are gone from the rendered tree, so a
        # build cannot pick one up instead of its rendered form.
        assert not list(tree.rglob("*.in")), "a template survived into " + distro


def test_the_checkout_holds_templates_and_no_rendered_packaging():
    # The opposite failure: somebody commits a rendered debian/control beside
    # the template, dpkg builds the committed one, and the table stops
    # deciding anything while every test above still passes.
    for name in ("debian/control", "debian/rules", "debian/changelog",
                 "debian/postinst"):
        assert (ROOT / (name + ".in")).is_file(), name + ".in is missing"
        assert not (ROOT / name).is_file(), (
            "{} is committed beside its template; a build would use it and the "
            "distribution table would stop deciding anything".format(name)
        )
    # rosdep/constraints.txt(.in) does not exist in either form any more: it
    # existed to bound a PyPI numpy pull, and nothing installs via pip at
    # build or install time any more (see debian/postinst.in).
    assert not (ROOT / "rosdep" / "constraints.txt.in").exists()
    assert not (ROOT / "rosdep" / "constraints.txt").exists()
    # rosdep/fleetless-bridge.yaml does not exist either: it existed only to
    # resolve `jsonschema` at a version floor no public key met, and
    # `python3-jsonschema` turned out to be a public key on its own once that
    # floor was dropped (see rosdep/README.md). A repo-local source
    # reappearing here is the same kind of drift constraints.txt was.
    assert not (ROOT / "rosdep" / "fleetless-bridge.yaml").exists()


def test_run_tests_and_build_deb_both_refuse_an_unknown_distribution():
    for script in ("./run-tests.sh", "./build-deb.sh"):
        r = subprocess.run(
            [script, "--distro", "jazy"] if script.endswith("run-tests.sh") else [script, "jazy"],
            cwd=str(ROOT), capture_output=True, text=True,
        )
        assert r.returncode != 0, script + " accepted a typo"
        assert "jazy" in r.stderr, script + " did not name what it rejected"
        assert "humble" in r.stderr and "jazzy" in r.stderr, (
            script + " did not list the supported distributions"
        )
        # Nothing was built or pulled on the way to the refusal.
        assert "Unable to find image" not in r.stderr


# ---------------------------------------------------------------------------
# What a test run ran on: tools/test-image-report.sh, sourced by run-tests.sh
# ---------------------------------------------------------------------------

#: Stands in for `docker`. Logs every call, answers the four questions the
#: report asks, and can be told to fail each of them. Run as `bash <file>` so
#: DOCKER is two words, the way `sudo docker` is.
FAKE_DOCKER = r'''
printf '%s\n' "$*" >> "$FAKE_DOCKER_LOG"
case "$1" in
  pull)
    exit "${FAKE_PULL_STATUS:-0}" ;;
  image)
    fmt=$4 ref=$5
    case "$fmt" in
      *RepoDigests*)
        [ -z "${FAKE_NO_DIGEST:-}" ] || exit 1
        [ "$ref" = ros:jazzy ] && echo "ros@sha256:1111" ;;
      *RootFS*)
        if [ "$ref" = ros:jazzy ]; then echo "sha256:b1 sha256:b2"
        else echo "${FAKE_IMAGE_LAYERS:-sha256:b1 sha256:b2 sha256:t1}"; fi ;;
    esac ;;
  run)
    [ -z "${FAKE_NO_DPKG:-}" ] || exit 2
    printf 'ii  ros-jazzy-fastrtps 2.14.7-1noble.20260911.035053\n'
    printf 'un  ros-jazzy-fastrtps-doc \n'
    printf 'ii  ros-jazzy-rclpy 7.1.12-1noble.20260912.162354\n'
    # dpkg-query's own status when one of its patterns matched nothing
    exit 1 ;;
esac
'''


def _report(tmp_path, call, **env_extra):
    """Sources the helper under run-tests.sh's own shell options, makes one
    call, then echoes `after` -- so a helper that aborts the script shows up as
    a missing `after`, not as a pass."""
    fake = tmp_path / "fake-docker.sh"
    fake.write_text(FAKE_DOCKER)
    log = tmp_path / "docker.log"
    log.write_text("")
    env = dict(os.environ, DOCKER="bash {}".format(fake), FAKE_DOCKER_LOG=str(log), **env_extra)
    script = "set -euo pipefail\n. {}\n{}\necho after\n".format(
        ROOT / "tools" / "test-image-report.sh", call)
    r = subprocess.run(["bash", "-c", script], cwd=str(tmp_path), env=env,
                       capture_output=True, text=True)
    return r, log.read_text()


REPORT = "fleetless_report_test_image ros:jazzy fleetless-bridge-dev-jazzy jazzy"


def test_the_test_job_names_its_base_image_digest_and_package_versions(tmp_path):
    r, _ = _report(tmp_path, REPORT)
    assert r.returncode == 0, r.stderr
    assert r.stdout == "after\n"
    assert ("run-tests.sh: base image ros:jazzy = ros@sha256:1111 "
            "(test image fleetless-bridge-dev-jazzy is built on it)") in r.stderr
    assert "run-tests.sh: package ros-jazzy-fastrtps 2.14.7-1noble.20260911.035053" in r.stderr
    assert "run-tests.sh: package ros-jazzy-rclpy 7.1.12-1noble.20260912.162354" in r.stderr
    # Known to dpkg but not installed: not something this run ran.
    assert "fastrtps-doc" not in r.stderr


def test_package_globs_reach_dpkg_unexpanded_and_cover_every_family(tmp_path):
    # Files a careless unquoted glob would expand to, in the caller's cwd.
    for name in ("ros-jazzy-rmw-planted", "ros-jazzy-rcl-planted"):
        (tmp_path / name).write_text("")
    r, log = _report(tmp_path, REPORT)
    assert r.returncode == 0, r.stderr
    run_call = [line for line in log.splitlines() if line.startswith("run ")]
    assert len(run_call) == 1, log
    for family in ("rmw", "rcl", "fastrtps", "fastdds", "fastcdr", "cyclonedds"):
        assert "ros-jazzy-{}*".format(family) in run_call[0].split(), run_call[0]
    assert "planted" not in log


def test_a_test_image_not_built_on_the_reported_base_says_so(tmp_path):
    r, _ = _report(tmp_path, REPORT, FAKE_IMAGE_LAYERS="sha256:x1 sha256:t1")
    assert r.returncode == 0, r.stderr
    assert "(test image fleetless-bridge-dev-jazzy is NOT built on it" in r.stderr


def test_an_unreadable_digest_is_one_line_and_the_run_goes_on(tmp_path):
    r, _ = _report(tmp_path, REPORT, FAKE_NO_DIGEST="1")
    assert r.returncode == 0, r.stderr
    assert r.stdout == "after\n"
    lines = [l for l in r.stderr.splitlines() if "base image" in l]
    assert lines == ["run-tests.sh: base image ros:jazzy: digest unknown "
                     "(no local copy with a registry digest)"]
    assert "run-tests.sh: package ros-jazzy-rclpy" in r.stderr  # the versions still come


def test_unreadable_package_versions_are_one_line_and_the_run_goes_on(tmp_path):
    r, _ = _report(tmp_path, REPORT, FAKE_NO_DPKG="1")
    assert r.returncode == 0, r.stderr
    assert r.stdout == "after\n"
    assert "run-tests.sh: package " not in r.stderr
    assert r.stderr.count(
        "run-tests.sh: ROS/DDS package versions of fleetless-bridge-dev-jazzy: could not be read") == 1


def test_a_failed_pull_is_one_line_and_the_run_goes_on(tmp_path):
    r, log = _report(tmp_path, "fleetless_pull_base_image ros:jazzy", FAKE_PULL_STATUS="1")
    assert r.returncode == 0, r.stderr
    assert r.stdout == "after\n"
    assert r.stderr.splitlines() == [
        "run-tests.sh: could not pull ros:jazzy; building on the local copy, if there is one"]
    assert log.splitlines() == ["pull -q ros:jazzy"]


def test_run_tests_pulls_before_the_build_and_reports_once_after_it():
    text = (ROOT / "run-tests.sh").read_text()
    assert ". tools/test-image-report.sh" in text
    build = text.index("$DOCKER build")
    pull = text.index('fleetless_pull_base_image "ros:$DISTRO"')
    report_call = 'fleetless_report_test_image "ros:$DISTRO" "$IMAGE" "$DISTRO"'
    assert text.count(report_call) == 1
    assert pull < build < text.index(report_call)
    # The reference reported is the one the image is built from.
    assert "FROM ros:${ROS_DISTRO_TAG}" in (ROOT / "Dockerfile.dev").read_text()


# ---------------------------------------------------------------------------
# What the .deb SAYS, as opposed to what the packaging says
# ---------------------------------------------------------------------------

def _codename(distro):
    return sh("fleetless_distro_field {} codename".format(distro)).stdout


#: Every string that names one ROS distribution or one Ubuntu release, in the
#: only two spellings that reach a reader of an installed package.
DISTRO_WORDS = SUPPORTED + ["kilted"]
#: Derived from the table, not typed here a second time: a fourth
#: distribution added to SUPPORTED and to tools/distros.sh used to leave this
#: list unaware of its codename, so the sweep below stopped looking for it —
#: silently, because nothing here compared the two.
CODENAME_WORDS = [_codename(d) for d in SUPPORTED]


#: Places where naming one other distribution is the point, not a mistake.
#:
#: Keyed by ``(path, a distinctive fragment)``, valued with the reason.
#: Everything not in here is a hit. Deliberately an allow-list of *decisions*,
#: not a rule like "skip comments": both defects this check was written after
#: were in prose -- a changelog entry claiming to replace a package that never
#: existed on that distribution, and a docstring pointing at
#: ``/opt/ros/humble/...`` inside the Jazzy and Lyrical payloads. A
#: comment-skipping rule would have missed both.
#:
#: **The fragment is matched against the PARAGRAPH, not the hit line.** A
#: justification is written and reflowed as one paragraph; keying on the line
#: would need a new entry per reflow, and half these entries would be empty
#: continuations. The cost: an exemption covers its whole paragraph, so a new
#: and wrong sentence added inside one still passes. That is why each reason
#: names the specific claim being allowed -- a paragraph that grows a
#: different claim no longer matches its own reason, and that is a reviewer's
#: job, not this check's.
#:
#: Every entry is asserted to match something, so an exemption whose text moved
#: fails here instead of quietly widening the guard.
INSTALLED_DISTRO_EXEMPTIONS = {
    ("package.xml", "exist on Lyrical: upstream dropped that package there"):
        "explains why the test dependency changed, which is a fact about Lyrical",
    ("fleetless_bridge/client.py", "OLDEST distribution this one source is built for"):
        "names Humble as the oldest supported interpreter and says what the newer two do",
    ("fleetless_bridge/client.py", "bpo-37658"):
        "names the interpreter whose asyncio drops the cancellation, and says which "
        "version fixed it -- the newer two are past it, which is the point",
    ("fleetless_bridge/ros_runtime.py", "established against Humble's rclpy 3"):
        "names the distribution the executor race was observed in, and says the newer "
        "two carry no such observation",
    ("package.xml", "ros-lyrical-action-tutorials-py now depends on example_interfaces"):
        "the same explanation",
    ("package.xml", "example_interfaces ships Fibonacci.action on humble"):
        "enumerates all three deliberately: the point is that it is the same everywhere",
    ("package.xml", "jazzy and lyrical alike"):
        "the tail of that same enumeration",
    ("debian/changelog.in", "That one existed for Humble only, as"):
        "a Humble-only event, said to be Humble-only so it stays true in the other packages",
    ("debian/changelog.in", "0.2.0-0jammy; on every other distribution this entry"):
        "the version string of that Humble-only package, immediately qualified",
    ("debian/changelog.in", "Only `ros-humble-fleetless-bridge` was ever published before this"):
        "names which package predates the relicensing -- true regardless of which "
        "distribution's .deb this entry ships in, unlike the sentence it replaced",
    ("debian/changelog.in", "`ros-jazzy-fleetless-bridge` and `ros-lyrical-fleetless-bridge`"):
        "enumerates the other two deliberately: the point is that both are unaffected",
    ("debian/changelog.in", "as recently as this same package's previous"):
        "names Humble's 3.0.0 build specifically -- the only version ever published, "
        "and the one whose postinst still ran pip",
}


def _paragraphs(lines):
    """For each line, the text of the paragraph it belongs to.

    A paragraph is a maximal run of lines that still have content once the
    comment marker is taken off, so a bare ``#`` separates two comment
    paragraphs exactly as a blank line separates two prose ones. Without that,
    a whole ``#``-prefixed comment block would count as one paragraph and an
    exemption would cover far more than its author saw.
    """
    def content(line):
        return line.strip().lstrip("#").strip()

    out = [None] * len(lines)
    start = 0
    while start < len(lines):
        if not content(lines[start]):
            out[start] = ""
            start += 1
            continue
        end = start
        while end + 1 < len(lines) and content(lines[end + 1]):
            end += 1
        joined = "\n".join(lines[start:end + 1])
        for i in range(start, end + 1):
            out[i] = joined
        start = end + 1
    return out


def _installed_sources():
    """The source files whose bytes reach a robot, plus the packaging templates.

    Asked of ``scripts/internal_markers.installed_files``, which derives the set
    from ``setup.py``, ``debian/rules``, ``debian/control`` and dpkg's own
    maintainer-script names — rather than from a list here, which would go
    stale the first time a file moved. ``debian/control.in`` used to need
    adding explicitly, because the shared helper carried ``debian/copyright``
    and ``debian/changelog`` but not the control file itself — a sentence
    naming an Ubuntu release in its ``Description`` reaches every reader of
    the installed package (``apt-cache show``, ``dpkg -s``) and was invisible
    to this sweep until it did. Fixed in the shared helper now, not patched
    here a second time, so every other consumer of ``installed_files()`` sees
    it too.
    """
    import sys
    sys.path.insert(0, str(ROOT / "scripts"))
    from internal_markers import installed_files
    return set(installed_files(ROOT))


def test_the_installed_set_is_not_empty_and_holds_the_files_this_check_is_about():
    # Anti-vacuity. Everything below is a loop over this set; an empty or
    # truncated one passes every assertion and proves nothing.
    files = set(_installed_sources())
    assert len(files) >= 20, "the installed set collapsed to {} files: {}".format(
        len(files), sorted(files)
    )
    for expected in ("package.xml", "debian/changelog.in", "debian/postinst.in",
                     "debian/control.in", "fleetless_bridge/ros_runtime.py"):
        assert expected in files, (
            "{} is not in the installed set, so this check would not look at it; "
            "either it stopped shipping or installed_files() stopped seeing it".format(expected)
        )


def test_nothing_a_robot_receives_names_a_distribution_that_is_not_its_own():
    """The .deb is built from one source for three distributions, so a file
    naming one of them is wrong in the other two packages.

    Both defects this was written after survived every other check here: the
    rendered-packaging check above looks only at ``control``, ``rules`` and
    ``postinst``, skips comments, and nothing looked at the payload at all.
    Both were found by unpacking the built ``.deb`` -- the check that
    actually counts, and which no test here can run since the suite builds
    no ``.deb``. This is the closest substitute that runs every time: the
    same files, read from the source they're copied from.
    """
    used = set()
    hits = []
    words = re.compile(r"\b({})\b".format("|".join(DISTRO_WORDS + CODENAME_WORDS)), re.I)
    for path in sorted(_installed_sources()):
        absolute = ROOT / path
        if not absolute.is_file():
            continue
        raw = absolute.read_bytes()
        if b"\0" in raw[:8192]:
            continue
        lines = raw.decode("utf-8", "replace").splitlines()
        paragraphs = _paragraphs(lines)
        for number, line in enumerate(lines, start=1):
            if not words.search(line):
                continue
            # A template that substitutes the distribution in is correct by
            # construction: the rendered file names its own and no other.
            if "@ROS_DISTRO@" in line or "@DEB_CODENAME@" in line:
                continue
            paragraph = paragraphs[number - 1]
            exempt = [
                k for k in INSTALLED_DISTRO_EXEMPTIONS if k[0] == path and k[1] in paragraph
            ]
            if exempt:
                used.update(exempt)
                continue
            hits.append("{}:{}: {}".format(path, number, line.strip()))
    assert not hits, (
        "these lines reach every robot and name one distribution:\n  "
        + "\n  ".join(hits)
        + "\n\nEither make the line true in all three packages, or add it to "
          "INSTALLED_DISTRO_EXEMPTIONS with the reason it is deliberate."
    )
    unused = sorted(set(INSTALLED_DISTRO_EXEMPTIONS) - used)
    assert not unused, (
        "these exemptions matched nothing, so they are widening the guard for text that "
        "is no longer there: {}".format(unused)
    )
