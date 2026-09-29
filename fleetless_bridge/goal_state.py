# SPDX-License-Identifier: Apache-2.0
"""The bridge's own job_id -> goal_id mapping, persisted across a restart.
Plain file I/O, no ROS dependency, so it is easy to unit-test in
isolation from `ros_runtime.py` (already 5000+ lines and
executor-thread-sensitive) — this module only reads and writes one small
JSON file.

Written before a goal is sent, so a mapping entry always exists by the time
there is anything to lose; removed once the job is terminal *and reported*.
On startup, `RosRuntime` reads whatever is here once ROS is up and
re-attaches to each goal that is still active, or fetches the result of
one that ended while the process was down — see `ros_runtime.py`'s own
startup reconciliation.

Only Fleetless's own action goals are ever written here — an external
goal's restart-survival is not a promise this file makes; only the own
job -> goal mapping needs to survive a restart. A bridge that restarts
mid-external-goal simply rediscovers it fresh and reports it under a
newly, deterministically derived id, identical to the one it would have
reported before the restart."""
from __future__ import annotations

import json
import logging
import os
import pathlib
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

#: Overridable for tests and for a packaging layout that wants the file
#: somewhere else; defaults to the XDG state directory. Never `/tmp` — a
#: mapping that must survive a restart must not live in a directory the OS
#: is free to clear on one.
_DEFAULT_STATE_DIR = pathlib.Path.home() / ".local" / "state" / "fleetless-bridge"

_MAPPING_FILENAME = "goal_state.json"


def state_dir() -> pathlib.Path:
    """`FLEETLESS_STATE_DIR`, or `~/.local/state/fleetless-bridge/` — never
    `/tmp`. The bridge has no systemd unit and installs under the shared
    `/opt/ros/<distro>` tree (read-only in spirit, not meant for runtime
    writes), so this is a greenfield choice, not an existing convention;
    see the package README, which documents it beside the two env vars it
    already documents (`FLEETLESS_TOKEN`, `FLEETLESS_CLOUD_URL`)."""
    override = os.environ.get("FLEETLESS_STATE_DIR")
    return pathlib.Path(override) if override else _DEFAULT_STATE_DIR


def mapping_path(directory: Optional[pathlib.Path] = None) -> pathlib.Path:
    return (directory if directory is not None else state_dir()) / _MAPPING_FILENAME


def load_mapping(path: pathlib.Path) -> Dict[str, Tuple[str, str]]:
    """`job_id -> (slug, goal_id)`, or `{}` if the file is missing, unreadable
    or corrupt — matching the bridge's general "never crash the process
    over housekeeping" posture. A malformed entry (wrong shape, wrong
    types) is dropped individually rather than invalidating the whole
    file: one bad line losing only itself is a smaller loss than one bad
    line losing every other job's restart-survival too."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        log.warning("Could not read the goal-state mapping at %s — starting empty", path, exc_info=True)
        return {}
    if not isinstance(raw, dict):
        log.warning("The goal-state mapping at %s is not a JSON object — starting empty", path)
        return {}
    mapping: Dict[str, Tuple[str, str]] = {}
    for job_id, entry in raw.items():
        if (
            isinstance(job_id, str)
            and isinstance(entry, list)
            and len(entry) == 2
            and all(isinstance(x, str) and x for x in entry)
        ):
            mapping[job_id] = (entry[0], entry[1])
        else:
            log.warning("Dropping a malformed goal-state entry for job %r", job_id)
    return mapping


def save_mapping(path: pathlib.Path, mapping: Dict[str, Tuple[str, str]]) -> None:
    """Atomic and durable: write to a temp file in the same directory,
    fsync it, `os.replace` it over the old file, then fsync the directory —
    a reader (this process's own next startup) never sees a half-written
    file, whether the write is interrupted by a crash or a power loss.
    Without the two fsyncs the rename alone is only atomic against a crash
    of this process: after a power loss the directory entry can reach the
    disk before the bytes it points at.

    Raises `OSError` for a directory it cannot create or a file it cannot
    write — the caller (`RosRuntime._save_goal_mapping`) decides what a
    failed write means, since only it knows whether it runs on the
    executor thread."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    payload = {job_id: [slug, goal_id] for job_id, (slug, goal_id) in mapping.items()}
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
