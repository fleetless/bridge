# SPDX-License-Identifier: Apache-2.0
import pathlib
import re

from setuptools import setup

package_name = "fleetless_bridge"

# Read the version rather than repeat it: the bridge reports this same string to
# the cloud in its hello, and a test keeps package.xml in step with it.
version = re.search(
    r'^__version__ = "([^"]+)"',
    pathlib.Path(package_name, "__init__.py").read_text(),
    re.MULTILINE,
).group(1)

setup(
    name=package_name,
    version=version,
    packages=[package_name],
    # contracts_constants.json — vendored from the contracts package's own
    # constants artifact, loaded at import time by
    # ros_runtime.py. Without this, the file is only ever visible from a
    # source checkout, and would silently vanish for an installed package
    # the moment ros_runtime.py's `open()` looked for it next to itself.
    package_data={package_name: ["contracts_constants.json"]},
    # tools/ (fake_robot.py, dev-udp-only.xml) is a development fixture, never
    # installed here — dev-udp-only.xml in particular must never reach a real
    # robot (see its own header), so it is only ever referenced from the two
    # dev-container run scripts, outside of this package's install surface.
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
        ("share/" + package_name + "/launch", ["launch/bridge.launch.py"]),
    ],
    # **This is not the dependency list, and it never was.** The runtime
    # dependencies are declared in `package.xml`, which is what rosdep reads
    # and what the .deb is built from — this package is installed through a
    # distribution's package manager, not through pip, and a second list here
    # would be a second place to change and the weaker one would win. It has
    # already been incomplete for as long as it has existed: the contract
    # validator and the video client were never named here either.
    #
    # `setuptools` stays because `setup.py` itself needs it.
    install_requires=["setuptools"],
    zip_safe=True,
    description="Fleetless Bridge: connects a ROS2 robot to the Fleetless cloud.",
    license="Apache-2.0",
    author="Dehne Robotik GmbH",
    author_email="hello@fleetless.dev",
    url="https://github.com/fleetless/bridge",
    entry_points={
        "console_scripts": [
            "bridge = fleetless_bridge.main:main",
        ],
    },
)
