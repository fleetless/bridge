# SPDX-License-Identifier: Apache-2.0
"""Bridge configuration.

The bridge is configured through its launch file or environment
variables: the Fleetless token binds it to exactly one robot of the
developer's org; the cloud URL defaults to production.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_CLOUD_URL = "wss://api.fleetless.dev/bridge"


@dataclass(frozen=True, repr=False)
class BridgeConfig:
    token: str
    cloud_url: str

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

        return cls(
            token=token,
            cloud_url=os.environ.get("FLEETLESS_CLOUD_URL", DEFAULT_CLOUD_URL),
        )
