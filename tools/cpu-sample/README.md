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

## The failsafe in the turtle

The same comparison without a robot, plus how late a failsafe goes out. The
turtle simulator's image is the robot; the bridge in it is replaced by a
checkout's.

| File | What it is |
|---|---|
| `Dockerfile.sim` | the turtle-sim image with a checkout's `fleetless_bridge/`, `py-spy`, this sample and the probe |
| `failsafe_probe.py` | a ROS node that times each failsafe on its topic |
| `drive.mjs` | a client that publishes in bursts through the cloud's REST API |
| `live_viewer.py`, `Dockerfile.viewer` | a viewer without a browser, in a container of its own |

Build the image once per checkout. The context is this directory; the named
context `bridge` says whose bridge goes in:

```
docker build -f tools/cpu-sample/Dockerfile.sim --build-context bridge=<checkout> \
    -t cpu-sample-sim:<name> tools/cpu-sample
docker build -f tools/cpu-sample/Dockerfile.viewer -t cpu-sample-viewer tools/cpu-sample
```

One container per run, on Docker's default network, never the host's: that
would join the ROS graph of everything on the machine. The robot's token
reaches it through `--env-file` (`FLEETLESS_TOKEN`, `FLEETLESS_CLOUD_URL`),
and the file is deleted once `docker run` has returned. A cloud on the same
machine is `host.docker.internal`, given by `--add-host
host.docker.internal:host-gateway`. `--cap-add SYS_PTRACE` is for `py-spy`,
which then runs as root inside (`docker exec -u root`).

A slower robot is `--cpuset-cpus` on half the cores, not `--cpus`. Under
load, `--cpus` measures the kernel's quota: once the container has spent its
share, every thread in it stops until the next period, whatever the bridge
does.

**Idle**: the turtle runs, nobody watches, nobody publishes. `cpu_sample.py`
inside the container (`docker exec`), once on the bridge and once on its
failsafe process (`--match fleetless_bridge.failsafe_process`), which is a
process of its own and so not in the bridge's number. A failsafe process
that the bridge restarts during the window ends that sample with exit code
1; its time is then in the bridge's `cutime` and `cstime` in
`/proc/<pid>/stat`. After the window:

```
py-spy record --pid <bridge> --nonblocking --idle --threads --format raw --duration 60
```

The executor is the thread `fleetless-ros-executor`. A sample whose
innermost frame is `wait_set.wait` in `rclpy/executors.py` is `rcl_wait`; one
under the bridge's watchdog methods is the checks; the rest of the thread
outside the bridge's own callbacks is rclpy's work per wake-up.

**Busy**: on top of idle, the cameras live and a client publishing.

- `drive.mjs` publishes to the publisher with the failsafe at 20 Hz for
  10 s, then stops for 1 s, over and over. It takes a developer session from
  a file, deletes the file and refreshes the session itself.
- Every camera gets a live session (`POST
  /api/robots/:id/cameras/:slug/live`), and `live_viewer.py` joins them all,
  with the sessions in its environment file. The bridge encodes a camera
  only while its track has a subscriber; without a viewer the load is not
  there. A cloud that hands out a loopback address for LiveKit hands the
  bridge its own container: a forwarder in the container from that port to
  `host.docker.internal` closes the gap.

**Lateness**: `failsafe_probe.py` in the same container, reading the
publisher's topic, type, `timeout_ms` and failsafe message from the published
file. It tells the failsafe from a client's message by content, so a client
message must differ from it (the default of `drive.mjs` turns on the spot).

```
docker exec <container> bash -c 'source /opt/turtle/environment.sh; \
    python3 /opt/cpu-sample/failsafe_probe.py \
    --config /opt/turtle/fleetless/turtle.fleetless.yaml --publisher drive --silences 200'
```

The writer's source timestamps are the number to read: they say when the
failsafe went out. The reception and callback clocks add the delivery and the
probe's own scheduling, which a loaded machine makes visible.

On a shared machine, run before and after back to back, more than
once, and keep `uptime` beside every number.
