# SPDX-License-Identifier: Apache-2.0
"""Keeps the three places that describe this package from drifting apart.

Not exercised by the other tests: a mismatch here breaks nothing at import
time -- it breaks `ros2 launch` on a robot, or makes the bridge report a
version to the cloud that no apt package ever had.
"""
import pathlib
import re

from fleetless_bridge import __version__

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_the_ros_manifest_is_well_formed_xml():
    """Nothing else here parses it, and the regexes below cannot tell.

    `package.xml` is read as XML by rosdep, ament and `ros2 pkg xml` on the
    robot -- never by this package's own code or by `dpkg-buildpackage`,
    which copies it verbatim. A malformed manifest builds a perfectly good
    .deb and surfaces first in `rosdep install` inside a container build,
    minutes away, in somebody else's error message. The trap: an XML comment
    can't contain `--`, ordinary to type in prose, and every regex here
    keeps matching past it.
    """
    from xml.etree import ElementTree

    ElementTree.parse(str(ROOT / "package.xml"))


def test_the_ros_manifest_agrees_with_the_package_version():
    manifest = (ROOT / "package.xml").read_text()
    declared = re.search(r"<version>([^<]+)</version>", manifest).group(1)
    assert declared == __version__


def test_the_launch_file_starts_the_executable_setup_py_installs():
    entry_point = re.search(
        r'"(\w+) = fleetless_bridge\.main:main"', (ROOT / "setup.py").read_text()
    ).group(1)
    launched = re.search(
        r'executable="([^"]+)"', (ROOT / "launch" / "bridge.launch.py").read_text()
    ).group(1)
    assert launched == entry_point


def test_the_debian_changelog_agrees_with_the_package_version():
    # A fourth place this version could drift from the other three.
    # debian/changelog.in's top entry reads "<source> (<upstream>-<debian
    # revision>) <distribution>; ..." — only the upstream part is read,
    # before the "-0@DEB_CODENAME@" revision suffix, which is packaging
    # metadata this test has no opinion about.
    #
    # `.in`: rendered per ROS distribution by tools/distros.sh, so the
    # source name and revision suffix are placeholders here. Matching the
    # placeholder rather than a distribution name keeps this test from
    # passing on a template frozen back to one distribution.
    changelog = (ROOT / "debian" / "changelog.in").read_text()
    entry_version = re.search(
        r"^ros-@ROS_DISTRO@-fleetless-bridge \(([^)]+)\)", changelog
    ).group(1)
    upstream_version = entry_version.split("-", 1)[0]
    assert upstream_version == __version__


def test_the_launch_file_respawns_the_bridge_on_a_real_robot():
    # A crashed process on a robot must come back by itself. This is a text
    # check, not an import-and-inspect one, because the launch file is never
    # imported by the rest of the suite (it needs a real `launch`/
    # `launch_ros` runtime) -- a reverted `respawn=True` would otherwise be
    # invisible to every other test here.
    launch_source = (ROOT / "launch" / "bridge.launch.py").read_text()
    assert "respawn=True" in launch_source
    assert "respawn_delay=" in launch_source
