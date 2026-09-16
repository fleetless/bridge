# SPDX-License-Identifier: Apache-2.0
"""ci/release-check.sh: what a tag must be before a release builds anything.

Every refusal is run for real, against a throwaway git repository holding a
copy of the script and a one-line fleetless_bridge/__init__.py. MAIN_REF is
`main` there because the throwaway has no remote.

**What makes this fail:** a tag pattern that lets `v3.1.0-rc1` through, a
version comparison that reads anything but `__version__`, or an ancestry check
that is skipped when the tag is not on main.
"""
import os
import pathlib
import shutil
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
IDENTITY = {
    "GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
    "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
}


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=str(repo), check=True, capture_output=True,
                   env={**os.environ, **IDENTITY})


def _check(repo, tag):
    return subprocess.run(["bash", str(repo / "ci" / "release-check.sh"), tag],
                          cwd=str(repo), capture_output=True, text=True,
                          env={**os.environ, "MAIN_REF": "main"})


@pytest.fixture
def repo(tmp_path):
    (tmp_path / "ci").mkdir()
    shutil.copy2(str(ROOT / "ci" / "release-check.sh"), str(tmp_path / "ci" / "release-check.sh"))
    (tmp_path / "fleetless_bridge").mkdir()
    (tmp_path / "fleetless_bridge" / "__init__.py").write_text('__version__ = "3.1.0"\n')
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "release")
    return tmp_path


def test_a_tag_on_main_that_is_the_version_passes(repo):
    _git(repo, "tag", "v3.1.0")
    result = _check(repo, "v3.1.0")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("tag", ["3.1.0", "v3.1", "v3.1.0-rc1", "v3.1.0.1", "v03.1.0x", ""])
def test_a_tag_that_is_not_vxyz_is_refused(repo, tag):
    result = _check(repo, tag)
    assert result.returncode == 2
    assert "is not a vX.Y.Z tag" in result.stderr


def test_a_tag_that_is_not_the_version_is_refused(repo):
    _git(repo, "tag", "v3.2.0")
    result = _check(repo, "v3.2.0")
    assert result.returncode == 1
    assert "3.2.0" in result.stderr and "3.1.0" in result.stderr


def test_a_tag_nobody_created_is_refused(repo):
    result = _check(repo, "v3.1.0")
    assert result.returncode == 1
    assert "no tag v3.1.0" in result.stderr


def test_a_tag_off_main_is_refused(repo):
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "side.txt").write_text("side\n")
    _git(repo, "add", "side.txt")
    _git(repo, "commit", "-q", "-m", "side")
    _git(repo, "tag", "v3.1.0")
    result = _check(repo, "v3.1.0")
    assert result.returncode == 1
    assert "not reachable from main" in result.stderr
