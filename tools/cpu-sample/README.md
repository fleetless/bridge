# The CPU sample

A development fixture, never installed on a robot by the package. One file,
standard library only.

```
python3 tools/cpu-sample/cpu_sample.py --match fleetless_bridge --seconds 300 --log bridge.log
```

It reads the process's CPU time from `/proc/<pid>/stat` when the window opens
and when it closes, and prints the mean share of one core in between. 100 %
is one core busy for the whole window; a process with several busy threads
can exceed it. Child processes are not counted. With `--log` it also counts
the lines the file gained during the window that say `The snapshot pull timed
out`, which is what the bridge logs when its ROS work queue has stalled.

`--pid` names the process; `--match` finds the one process whose command line
contains the text and refuses when there are none or several. `--interval`
prints the share of each stretch on the way (30 seconds by default), so a
window that was not steady shows as one.

A bridge started by `ros2 launch` is two processes whose command lines both
name the package: the launcher and the node. `--match` then refuses and lists
them with their pids; take the node's with `--pid`.

`--log` reads a file. For a bridge that logs to the journal, write one for
the length of the window first: `journalctl -u <unit> -f > bridge.log`.

## The comparison it is for

Two scenarios, five minutes each, with the bridge and its cameras running for
a minute before the window opens:

- **idle**: nobody watches. The bridge sends snapshots and nothing else.
- **live**: every camera is being watched for the whole window.

Run both for each state that is being compared: the bridge before a change
and after it, and after it once per value of `live.encoder` that matters. Keep
the robot, the cameras, their configured size and rate and the viewers the
same between runs; the number means nothing next to one taken under other
conditions.

The exit code is 0 for a result, 1 when the process ended during the window,
2 when it could not be found or read.
