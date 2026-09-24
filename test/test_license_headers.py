# SPDX-License-Identifier: Apache-2.0
"""Every source file says which licence it is under, and the four places that
declare the package's licence agree with the LICENSE file.

This package was Proprietary in four places and had no licence file or header
anywhere. A licence stated in one place and not another goes unnoticed until
somebody asks a lawyer -- so each is checked against the others, not against a
constant.

**What makes this fail**: drop the SPDX line from any source file -- Python,
shell, or a maintainer script with no extension at all -- change one of the
four declarations, or edit the LICENSE text by so much as a byte. Verified by
doing exactly that, including the last one: appending a commercial-use
restriction to LICENSE used to leave this file green.

**What makes it fail the harder way**: scanning nothing. The file set comes
from `scripts/internal_markers.py` -- the same computed union the prose guard
uses, so a file this cannot see is a file that is neither tracked nor
installed -- and the count is floored well above what a partial walk would
return.
"""
import hashlib
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from internal_markers import public_file_set  # noqa: E402

HEADER = "# SPDX-License-Identifier: Apache-2.0"
#: `.github/release/*.mjs` writes the same line as a JS comment, not a `#`
#: one. `_carries_a_comment_header` catches `release.mjs` and
#: `bridge-version.mjs` by their `node` shebang, the same accident that
#: catches `debian/rules` and `debian/postinst` -- not by `.mjs` extension,
#: which it deliberately does not check (see that function's docstring).
#: The two `*.test.mjs` files have no shebang, so this header shape is
#: checked on exactly the two files that need it and left unchecked on the
#: two that don't carry a shebang either.
JS_HEADER = "// SPDX-License-Identifier: Apache-2.0"

#: Below the real count (61 at the time of writing) and far above what any
#: partial walk would produce. A floor rather than an equality so that adding a
#: file is not a test change, and a floor rather than nothing so that a walk
#: that finds two files cannot pass.
PYTHON_FILE_FLOOR = 55

#: The same, for the source files that are not Python. This guard used to
#: filter the computed set down to `.py` and assert over that, which left
#: `run-tests.sh`, `run-bridge.sh`, `run-fake-robot.sh`, `debian/rules` and
#: `debian/postinst` outside every assertion -- two of them shipped inside the
#: .deb, one of them executed as root on every robot, all five covered by
#: `debian/copyright`'s `Files: *` claim that everything here is Apache-2.0.
#: The floor is what makes the *shell* half of the set unable to go quiet: a
#: filter that dropped it again would leave zero here, not a smaller number.
NON_PYTHON_FILE_FLOOR = 5

SET = public_file_set(ROOT)


def _first_line(path):
    # Latin-1 never raises, so a file that is not UTF-8 is classified rather
    # than crashing the collection -- and a shebang is ASCII either way.
    with open(path, "rb") as fh:
        return fh.readline().decode("latin-1")


def _carries_a_comment_header(rel):
    """Does this file take a comment header at all?

    Extension for the two languages this package writes as source (`.py`,
    `.sh`), and a shebang for everything else -- which is what catches
    `debian/rules` (make), `debian/postinst` (sh), neither of which has an
    extension, and the two vendored `.mjs` entry points, which do carry a
    shebang for `node`. A bare `.mjs` extension is deliberately not enough:
    `tools/live-proof/`'s two entry points already write their SPDX line
    inside a `/** */` block past line 2, a shape this guard does not parse,
    and widening the net by extension alone would fail them for a header
    they already carry rather than one they lack.
    """
    return rel.endswith((".py", ".sh")) or _first_line(ROOT / rel).startswith("#!")


HEADER_FILES = sorted({f for f in SET.tracked + SET.installed if _carries_a_comment_header(f)})
PYTHON_FILES = sorted(f for f in HEADER_FILES if f.endswith(".py"))
NON_PYTHON_FILES = sorted(f for f in HEADER_FILES if not f.endswith(".py"))


def test_every_source_file_in_the_public_set_carries_the_spdx_header():
    assert len(PYTHON_FILES) >= PYTHON_FILE_FLOOR, "only {} python files were found".format(len(PYTHON_FILES))
    assert len(NON_PYTHON_FILES) >= NON_PYTHON_FILE_FLOOR, "only {} non-python source files were found: {}".format(
        len(NON_PYTHON_FILES), NON_PYTHON_FILES
    )
    missing = []
    for f in HEADER_FILES:
        # A shebang has to stay on line 1, so the header is allowed on line 2
        # there and nowhere further down: a licence line ten lines into a file
        # is not what a tool looking for one reads.
        head = (ROOT / f).read_text().split("\n")[:2]
        wanted = JS_HEADER if f.endswith(".mjs") else HEADER
        if not any(line.strip() == wanted for line in head):
            missing.append(f)
    assert missing == [], "{} file(s) without an SPDX header: {}".format(len(missing), missing)


def test_the_set_reaches_past_the_package_directory():
    # Python and not: a `fleetless_bridge/`-only walk would pass the assertion
    # above and say nothing about the launch file in the .deb, the setup script
    # that declares the licence, or the tests; a `.py`-only filter would pass
    # it and say nothing about the maintainer script that runs as root on
    # every robot.
    for f in [
        "fleetless_bridge/main.py",
        "launch/bridge.launch.py",
        "setup.py",
        "test/test_license_headers.py",
        "tools/fake_robot.py",
        "scripts/internal_markers.py",
        # Shell and make, not control data -- templated because the
        # distribution is a variable, so `.in` is the tracked name, and the
        # tracked name is what is scanned.
        "debian/postinst.in",
        "debian/rules.in",
        "run-tests.sh",
        "run-bridge.sh",
        "run-fake-robot.sh",
        # The vendored release library: `//`, not `#`, is the header shape
        # that reaches into `.github/` too.
        ".github/release/release.mjs",
    ]:
        assert f in HEADER_FILES, "{} is outside the scanned set".format(f)


#: sha256 of the canonical Apache-2.0 text as the Apache Software Foundation
#: publishes it, which is also byte-for-byte what jammy ships as
#: /usr/share/common-licenses/Apache-2.0 -- the file debian/copyright points
#: every user of the .deb at. (md5 3b83ef96387f14655fc854ddc3c6bd57, the digest
#: every licence scanner recognises for this text.)
CANONICAL_APACHE_2_SHA256 = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"

#: Debian ships the same text here on every jammy system. Comparing against it
#: is the half of this guard that does not depend on a constant living in the
#: repository it is guarding: somebody editing LICENSE and the digest above in
#: one commit still fails this.
DEBIAN_COMMON_LICENSE = pathlib.Path("/usr/share/common-licenses/Apache-2.0")


def test_the_license_file_is_the_unmodified_apache_2_text():
    raw = (ROOT / "LICENSE").read_bytes()
    # A substring-and-length check cannot answer this, and used not to:
    # appending `ADDITIONAL TERM: commercial use requires a separate
    # agreement` to the Apache text leaves every named phrase present and the
    # file longer, so a test called "unmodified" passed over a modified
    # licence. The claim is that this file IS the Apache-2.0 text -- checked
    # over the whole file, not a substring.
    digest = hashlib.sha256(raw).hexdigest()
    assert digest == CANONICAL_APACHE_2_SHA256, (
        "LICENSE is not the canonical Apache-2.0 text (sha256 {}, expected {}). "
        "Four other files swear this package is Apache-2.0; if this one has "
        "been edited, they are all wrong.".format(digest, CANONICAL_APACHE_2_SHA256)
    )
    if DEBIAN_COMMON_LICENSE.is_file():
        assert raw == DEBIAN_COMMON_LICENSE.read_bytes(), (
            "LICENSE differs from {}, which is where debian/copyright sends "
            "every user of the .deb for the licence text".format(DEBIAN_COMMON_LICENSE)
        )
    assert (ROOT / "NOTICE").read_text().startswith("Fleetless Bridge\nCopyright 2026 Dehne Robotik GmbH")


def test_the_four_declarations_agree():
    manifest = (ROOT / "package.xml").read_text()
    assert re.search(r"<license>Apache-2\.0</license>", manifest)

    setup_py = (ROOT / "setup.py").read_text()
    assert re.search(r'license="Apache-2\.0"', setup_py)

    copyright_file = (ROOT / "debian" / "copyright").read_text()
    # The machine-readable format, the licence, the standard short text, and
    # Debian's required pointer. The only place a .deb user reads the licence,
    # so checked in full, not by its header line.
    assert copyright_file.startswith(
        "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/"
    )
    assert "\nLicense: Apache-2.0\n" in copyright_file
    assert "http://www.apache.org/licenses/LICENSE-2.0" in copyright_file
    assert "/usr/share/common-licenses/Apache-2.0" in copyright_file
    assert "Proprietary" not in copyright_file

    # Nothing in the public set still says Proprietary.
    #
    # One file has to name the word to forbid it: this one. The exemption is
    # bounded like the prose guard's -- exactly this name, inside the scanned
    # set, and it must still contain the word. An exemption that hides nothing
    # is one more file this guard would have quietly stopped reading.
    exempt = {"test/test_license_headers.py"}
    assert sorted(exempt) == ["test/test_license_headers.py"]
    for f in exempt:
        assert f in set(SET.readable), "{} is exempt but is outside the scanned set".format(f)
        assert "Proprietary" in (ROOT / f).read_text(), "{} no longer needs its exemption".format(f)

    for f in SET.readable:
        if f in exempt:
            continue
        assert "Proprietary" not in (ROOT / f).read_text(), "{} still says Proprietary".format(f)


def test_the_four_community_files_exist():
    for name in ["LICENSE", "NOTICE", "CONTRIBUTING.md", "SECURITY.md", "CODE_OF_CONDUCT.md"]:
        assert (ROOT / name).is_file(), "{} is missing".format(name)
    # The address the security file promises, asserted rather than assumed: a
    # SECURITY.md that names no way to reach anybody is worse than none.
    assert "security@fleetless.dev" in (ROOT / "SECURITY.md").read_text()
