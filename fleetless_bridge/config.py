# SPDX-License-Identifier: Apache-2.0
"""Bridge configuration.

The bridge is configured through its launch file or environment
variables: the Fleetless token binds it to exactly one robot of the
developer's org; the cloud URL defaults to production.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

DEFAULT_CLOUD_URL = "wss://api.fleetless.dev/bridge"


@dataclass(frozen=True, repr=False)
class BridgeConfig:
    token: str
    cloud_url: str
    uplink_kbps: Optional[int] = None
    uplink_reserve_pct: int = 20
    uplink_reserve_min_kbps: int = 128

    def __repr__(self) -> str:
        # The token is a credential; the default repr would leak it into
        # every log line, traceback and debugger session.
        return "BridgeConfig(token=<redacted>, cloud_url={!r})".format(self.cloud_url)

    @classmethod
    def from_env(cls) -> "BridgeConfig":
        token = os.environ.get("FLEETLESS_TOKEN", "").strip()
        if not token:
            raise ValueError(
                "FLEETLESS_TOKEN is not set. Create the robot in the Fleetless "
                "console and pass its token via the launch file or environment."
            )

        uplink_kbps_str = os.environ.get("FLEETLESS_UPLINK_KBPS", "").strip()
        uplink_kbps: Optional[int] = None
        if uplink_kbps_str:
            try:
                uplink_kbps = int(uplink_kbps_str)
            except ValueError:
                raise ValueError(
                    f"FLEETLESS_UPLINK_KBPS must be an integer, got {uplink_kbps_str!r}"
                )

        uplink_reserve_pct_str = os.environ.get("FLEETLESS_UPLINK_RESERVE_PCT", "20").strip()
        try:
            uplink_reserve_pct = int(uplink_reserve_pct_str)
        except ValueError:
            raise ValueError(
                f"FLEETLESS_UPLINK_RESERVE_PCT must be an integer, got {uplink_reserve_pct_str!r}"
            )

        uplink_reserve_min_kbps_str = os.environ.get("FLEETLESS_UPLINK_RESERVE_MIN_KBPS", "128").strip()
        try:
            uplink_reserve_min_kbps = int(uplink_reserve_min_kbps_str)
        except ValueError:
            raise ValueError(
                f"FLEETLESS_UPLINK_RESERVE_MIN_KBPS must be an integer, got {uplink_reserve_min_kbps_str!r}"
            )

        return cls(
            token=token,
            cloud_url=os.environ.get("FLEETLESS_CLOUD_URL", DEFAULT_CLOUD_URL),
            uplink_kbps=uplink_kbps,
            uplink_reserve_pct=uplink_reserve_pct,
            uplink_reserve_min_kbps=uplink_reserve_min_kbps,
        )
