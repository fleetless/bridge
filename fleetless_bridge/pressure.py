# SPDX-License-Identifier: Apache-2.0
"""Uplink budget enforcement for live video.

A developer configures the robot's uplink budget; the bridge reserves a
percentage of it for the control socket and admits video streams only if they
fit within the remainder.
"""
from __future__ import annotations

from typing import Optional


class UplinkBudget:
    """Tracks uplink budget and reserves a portion for the control socket.

    The effective budget is the environment value, or the live override if one
    has ever been set. From the effective budget, the reserve is
    max(ceil(reserve_pct/100 × effective), reserve_min_kbps); video may use
    the remainder.

    The floor exists because a percentage shrinks with the budget,
    and a small budget is exactly when the control channel needs protecting
    most.
    """

    def __init__(
        self,
        uplink_kbps: Optional[int],
        reserve_pct: int = 20,
        reserve_min_kbps: int = 128,
    ):
        """Initialize the budget tracker.

        Args:
            uplink_kbps: Total uplink budget in kbps, or None for no budget.
            reserve_pct: Percentage of budget to reserve for control socket.
            reserve_min_kbps: Absolute floor for the control socket reserve.
        """
        self._env_uplink_kbps = uplink_kbps
        self._override_kbps: Optional[int] = None
        self._reserve_pct = reserve_pct
        self._reserve_min_kbps = reserve_min_kbps

    def set_live_override(self, kbps: Optional[int]) -> None:
        """Override the environment uplink value, or clear the override.

        Args:
            kbps: New budget in kbps, 0 to disable video, or None to clear
                  the override and revert to the environment value.
        """
        self._override_kbps = kbps

    def video_budget_kbps(self) -> Optional[int]:
        """Return the budget available for video, or None if unbudgeted.

        Returns:
            None if no budget is set (neither env nor override),
            0 if the effective budget is 0 (video disabled),
            or the effective budget minus the control socket reserve.
        """
        effective = self._get_effective_budget()
        if effective is None:
            return None
        if effective == 0:
            return 0
        reserve = self._calculate_reserve(effective)
        return max(0, effective - reserve)

    def admits(self, new_kbps: int, active_sum_kbps: int) -> bool:
        """Check if a new stream can be admitted within the budget.

        Args:
            new_kbps: Bitrate of the stream requesting admission.
            active_sum_kbps: Sum of bitrates of currently active streams.

        Returns:
            True if unbudgeted or if active_sum + new fits within video budget.
        """
        budget = self.video_budget_kbps()
        if budget is None:
            return True
        return active_sum_kbps + new_kbps <= budget

    def snapshot(self) -> dict:
        """Return a snapshot of the budget state for logging.

        Returns:
            Dict with keys: uplink_kbps, override_kbps, video_budget_kbps,
            reserve_kbps.
        """
        effective = self._get_effective_budget()
        reserve = self._calculate_reserve(effective) if effective is not None else 0
        return {
            "uplink_kbps": self._env_uplink_kbps,
            "override_kbps": self._override_kbps,
            "video_budget_kbps": self.video_budget_kbps(),
            "reserve_kbps": reserve,
        }

    def _get_effective_budget(self) -> Optional[int]:
        """Get the effective budget: override if set, else env value."""
        if self._override_kbps is not None:
            return self._override_kbps
        return self._env_uplink_kbps

    def _calculate_reserve(self, effective_kbps: Optional[int]) -> int:
        """Calculate the control socket reserve from the effective budget."""
        if effective_kbps is None or effective_kbps == 0:
            return 0
        # Integer ceiling division avoids floating-point rounding errors
        # for e.g. pct=7, effective=100 where 7.0/100*100 == 7.000000000001
        pct_reserve = -(-(self._reserve_pct * effective_kbps) // 100)
        return max(pct_reserve, self._reserve_min_kbps)
