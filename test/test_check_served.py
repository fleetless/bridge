# SPDX-License-Identifier: Apache-2.0
"""apt/check-served.sh refuses what it cannot check before it starts a container.

The check itself installs from a served repository, which needs Docker and the
network, so it is proven by running it (the check-served workflow, and the apt
publishing workflow after every publish). What is provable here is the other
half: a mistyped version or distribution is refused by name, and no container
is started for it -- a check that pulled `ros:kilted` before noticing would
report an image error instead of the typo.
"""
import os
import pathlib
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "apt" / "check-served.sh"


@pytest.fixture
def docker_recorder(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    marker = tmp_path / "docker-was-called"
    fake = bindir / "docker"
    fake.write_text('#!/usr/bin/env bash\necho "$*" >> "{}"\nexit 1\n'.format(marker))
    fake.chmod(0o755)
    env = {**os.environ, "PATH": "{}:{}".format(bindir, os.environ["PATH"])}
    return env, marker


@pytest.mark.parametrize("args, says", [
    ([], "usage:"),
    (["https://apt.example.invalid"], "usage:"),
    (["https://apt.example.invalid", "v3.1.0"], "is not an upstream version"),
    (["https://apt.example.invalid", "3.1"], "is not an upstream version"),
    (["https://apt.example.invalid", "3.1.0", "kilted"], "is not a ROS distribution this package supports"),
])
def test_what_it_cannot_check_is_refused_before_any_container(docker_recorder, args, says):
    env, marker = docker_recorder
    result = subprocess.run(["bash", str(SCRIPT), *args], cwd=str(ROOT),
                            capture_output=True, text=True, env=env)
    assert result.returncode == 2, result.stderr
    assert says in result.stderr
    assert not marker.exists(), "docker was called: " + marker.read_text()
