# SPDX-License-Identifier: Apache-2.0
"""The opt-in participant profile must be packageable and meet the timing budget."""

from pathlib import Path
import xml.etree.ElementTree as ET

from fleetless_bridge.ros_runtime import (
    ACTION_SERVER_LIVENESS_DEBOUNCE_S,
    FAILSAFE_CHECK_INTERVAL_S,
)


ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "fastdds" / "action-server-liveness.xml"
NS = {"dds": "http://www.eprosima.com/XMLSchemas/fastRTPS_Profiles"}


def _duration(node):
    return int(node.findtext("dds:sec", default="0", namespaces=NS)) + (
        int(node.findtext("dds:nanosec", default="0", namespaces=NS)) / 1_000_000_000
    )


def test_fastdds_profile_advertises_a_scoped_short_lease():
    root = ET.parse(PROFILE).getroot()
    participant = root.find("dds:participant", NS)
    assert participant is not None
    assert participant.attrib.get("is_default_profile") == "true"
    config = participant.find("dds:rtps/dds:builtin/dds:discovery_config", NS)
    assert config is not None
    lease = _duration(config.find("dds:leaseDuration", NS))
    announcement = _duration(config.find("dds:leaseAnnouncement", NS))
    assert 0 < announcement < lease <= 1.5
    assert lease + ACTION_SERVER_LIVENESS_DEBOUNCE_S + FAILSAFE_CHECK_INTERVAL_S <= 5.0


def test_fastdds_profile_is_in_the_bridge_package():
    setup = (ROOT / "setup.py").read_text()
    assert '"share/" + package_name + "/fastdds"' in setup
    assert '"fastdds/action-server-liveness.xml"' in setup
