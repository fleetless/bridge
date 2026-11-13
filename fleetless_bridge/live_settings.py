# SPDX-License-Identifier: Apache-2.0
"""The `live` settings: how the bridge sends video, for the whole robot.

Three layers, as for low-bandwidth mode: the vendored default, then the ROS
parameter on the bridge node, then the key of the same name in
`fleetless.yaml`. So a value can be tried on the robot with `ros2 param set`
and set for good from the console.

Nothing here touches ROS, asyncio or aiortc. `ros_runtime.py` declares the
parameter and asks `resolve` whether a proposed value is one; `client.py`
holds the two layers and resolves them; `live.py` applies the result.
"""
from dataclasses import dataclass
from typing import Any, Dict, Mapping

from fleetless_bridge.protocol import LIVE_BOUNDS, LIVE_DEFAULTS

#: The bounds of `max_packet_bytes`, inclusive: 576 to 1350. Read from the
#: vendored contracts constants, so a ROS parameter is refused by the same two
#: numbers the cloud refuses a published document by. The ceiling is 1350
#: because the SRTP library protects no packet above 1356 bytes
#: (`live.SRTP_MAX_PACKET_BYTES`): a larger setting would not be a bigger
#: packet but no video.
MAX_PACKET_BYTES_MIN: int = LIVE_BOUNDS["max_packet_bytes"]["min"]
MAX_PACKET_BYTES_MAX: int = LIVE_BOUNDS["max_packet_bytes"]["max"]


@dataclass(frozen=True)
class LiveSettings:
    #: The largest RTP packet the bridge sends, before SRTP: fixed header,
    #: header extensions and payload, retransmissions included.
    max_packet_bytes: int

    @classmethod
    def resolve(cls, params: Mapping[str, Any], yaml: Mapping[str, Any]) -> "LiveSettings":
        """Defaults <- ROS parameters <- `fleetless.yaml`, one layer at a time.

        A key present in the YAML wins over the parameter; a key absent in
        both is the vendored default. `None` in a layer is an undeclared or
        unset value and falls through rather than erasing the layer below.

        Unknown keys are ignored on purpose. The section is read whole, so a
        console that writes a newer key does not take an older bridge off the
        air; contracts refuses a misspelled key when the configuration is
        saved, and ROS refuses an undeclared parameter.

        Raises `ValueError` naming the key and the rule, so the parameter
        callback and the config apply refuse a bad value with the same words.
        Only the value that wins is judged: it is the one that would be in
        force.
        """
        merged: Dict[str, Any] = dict(LIVE_DEFAULTS)
        merged.update({k: v for k, v in params.items() if v is not None})
        merged.update({k: v for k, v in yaml.items() if v is not None})
        return cls(max_packet_bytes=check_max_packet_bytes(merged["max_packet_bytes"]))


def check_max_packet_bytes(value: Any) -> int:
    """`value` if it is a `max_packet_bytes` the bridge can send under,
    `ValueError` with the sentence naming the key and the rule if not.

    One place for the rule, read by `resolve` above and by `live.py` before
    it assigns anything. `bool` is an `int` in Python: `True` would otherwise
    be read as 1.
    """
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and MAX_PACKET_BYTES_MIN <= value <= MAX_PACKET_BYTES_MAX
    ):
        return value
    raise ValueError(
        "live.max_packet_bytes must be an integer in [{}, {}]".format(
            MAX_PACKET_BYTES_MIN, MAX_PACKET_BYTES_MAX
        )
    )
