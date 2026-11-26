# SPDX-License-Identifier: Apache-2.0
"""The failsafe process's environment (`failsafe.child_environment`): the
bridge's own, with the import path made safe and Fast DDS on UDP unless the
robot chose its transports itself (fleetless/fleetless#1124)."""
import os

import pytest

from fleetless_bridge import failsafe
from fleetless_bridge.failsafe import CHILD_TRANSPORTS, TRANSPORTS_VARIABLE, child_environment


@pytest.mark.parametrize("value", [None, "", "DEFAULT", "default", " DEFAULT "])
def test_the_failsafe_process_uses_udp_unless_the_robot_chose_transports(value):
    environ = {"ROS_DOMAIN_ID": "7"}
    if value is not None:
        environ[TRANSPORTS_VARIABLE] = value
    env = child_environment(environ)
    assert env[TRANSPORTS_VARIABLE] == CHILD_TRANSPORTS == "UDPv4"
    assert env["ROS_DOMAIN_ID"] == "7"


@pytest.mark.parametrize("value", ["SHM", "LARGE_DATA", "UDPv6", "NONE"])
def test_an_explicit_transport_choice_is_kept(value):
    env = child_environment({TRANSPORTS_VARIABLE: value})
    assert env[TRANSPORTS_VARIABLE] == value


def test_the_bridge_s_environment_is_left_as_it_is():
    environ = {"PYTHONPATH": "/opt/ros/lib"}
    child_environment(environ)
    assert environ == {"PYTHONPATH": "/opt/ros/lib"}


def test_the_import_path_starts_with_this_package_and_skips_the_working_directory():
    env = child_environment({"PYTHONPATH": "/opt/ros/lib"})
    assert env["PYTHONSAFEPATH"] == "1"
    first, rest = env["PYTHONPATH"].split(os.pathsep, 1)
    assert os.path.isdir(os.path.join(first, "fleetless_bridge"))
    assert first == os.path.dirname(os.path.dirname(os.path.abspath(failsafe.__file__)))
    assert rest == "/opt/ros/lib"
