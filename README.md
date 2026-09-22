# 🤖 fleetless_bridge

**The only part of Fleetless that runs on your robot.**

[Fleetless](https://fleetless.dev) turns a ROS 2 robot into a hosted REST and
realtime API. The bridge is the ROS 2 node that makes that possible: it opens
one WebSocket to the cloud, identifies the robot with its token, and from then
on does exactly what the published configuration names. It samples topics
into datapoints, runs services and actions as jobs, publishes messages with a
failsafe, streams cameras, and uploads URDF and mesh assets when asked.

It talks ROS on one side and the Fleetless wire protocol on the other, and
nothing else. Shipped as `ros-<distro>-fleetless-bridge`, Python, Apache-2.0.

## 🐢 Supported ROS 2 distributions

| ROS 2 distribution | Ubuntu | Package | apt suite | Supported until |
|---|---|---|---|---|
| Humble Hawksbill | 22.04 (jammy) | `ros-humble-fleetless-bridge` | `humble` | **May 2027** |
| Jazzy Jalisco | 24.04 (noble) | `ros-jazzy-fleetless-bridge` | `jazzy` | **May 2029** |
| Lyrical Luth | 26.04 (resolute) | `ros-lyrical-fleetless-bridge` | `lyrical` | **May 2031** |

The dates are not ours. Each is the end of life of that distribution from
[the ROS 2 release schedule](https://docs.ros.org/en/rolling/Releases.html),
and a suite stops receiving versions when its distribution does. Kilted Kaiju
is deliberately absent: it is not an LTS release and reaches end of life
before a robot running it would need a second year of updates.

## 📥 Install

The package is served signed from `apt.fleetless.dev`, one suite per
distribution. Substitute your own distribution for `humble`; the suite and the
package name change together, and nothing else does.

```sh
export ROS_DISTRO=humble          # or jazzy, or lyrical
curl -fsSL https://apt.fleetless.dev/key.gpg \
  | sudo tee /usr/share/keyrings/fleetless.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/fleetless.gpg] https://apt.fleetless.dev $ROS_DISTRO main" \
  | sudo tee /etc/apt/sources.list.d/fleetless.list
sudo apt update && sudo apt install "ros-$ROS_DISTRO-fleetless-bridge"
```

Every runtime dependency is an ordinary apt package, and nothing runs at
install time. A suite carries only its own distribution's package, so a
mismatched pair answers `Unable to locate package` rather than installing the
wrong thing.

## ▶️ Run

Create the robot in the [Fleetless Console](https://console.fleetless.dev),
copy its token, and launch:

```sh
export FLEETLESS_TOKEN=frt_...
ros2 launch fleetless_bridge bridge.launch.py
```

The robot shows up online in the console within a few seconds. From here on
the console decides what it exposes; the bridge only follows.

## ⚙️ Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `FLEETLESS_TOKEN` | yes | — | The robot's token, created once in the console. It binds this bridge to exactly one robot. |
| `FLEETLESS_CLOUD_URL` | no | `wss://api.fleetless.dev/bridge` | The cloud endpoint. |

Set them in the launch file or in the environment.

## 🐌 Low-bandwidth mode

Some uplinks are only nominally up. When the lag the cloud measures stays
high, or the bridge's own send queue backs up, the bridge stops pushing a
full telemetry stream through a pipe that cannot carry it: every datapoint
is held to one sample a second on average, live video is re-encoded smaller
or ended, new streams are refused, and buffered history waits. It leaves once
both measurements have been calm for a minute, and tells the cloud each
time it crosses, so the console can say why a robot went quiet.

| Parameter | Default | Meaning |
|---|---|---|
| `low_bandwidth.mode` | `auto` | `auto` decides from the measurements. `on` and `off` force it, for a link you already know about. |
| `low_bandwidth.enter_lag_ms` | `2000` | Lag or queue dwell above this enters the mode. |
| `low_bandwidth.enter_after_s` | `10` | How long that has to hold. A spike is not a narrow link. |
| `low_bandwidth.exit_lag_ms` | `500` | Lag and dwell both at or below this leave the mode. |
| `low_bandwidth.exit_after_s` | `60` | How long that has to hold. |
| `low_bandwidth.datapoint_max_hz` | `1.0` | What every datapoint is held to in the mode, unless it says `low_bandwidth: keep`. A long-run rate, not a minimum gap: after a quiet spell two samples may go out close together. |
| `low_bandwidth.camera` | `reduce` | `reduce` re-encodes a running stream smaller; `stop` ends it. New streams are refused either way. |
| `low_bandwidth.camera_bitrate_kbps` | `300` | What `reduce` aims at. The encoder clamps it — VP8 to 250 kbps…1.5 Mbps, H264 to 500 kbps…3 Mbps — and which codec is in use is the session's answer, not the bridge's choice, so under H264 this lands at 500. The log says so once per stream. |

They are ordinary ROS parameters, so they move at runtime:

```sh
ros2 param set /fleetless_bridge low_bandwidth.mode on
ros2 param set /fleetless_bridge low_bandwidth.datapoint_max_hz 0.5
```

A value the bridge cannot use is refused by the set, with the rule it broke,
and the parameter keeps what it had. Two things about `ros2 param set` that
are ROS's doing rather than the bridge's: the types are fixed at declaration,
so `datapoint_max_hz` takes `2.0` and not `2`, while the `_ms` and `_s` keys
take `2000` and not `2000.0`; and each parameter is validated on its own, so
lowering `enter_lag_ms` below the current `exit_lag_ms` is refused until
`exit_lag_ms` is lowered first. One rule spans two keys: `exit_lag_ms`
must stay at or below `enter_lag_ms`. Crossed, the same reading satisfies
both thresholds and the mode leaves as it arrives, so the bridge refuses the
pair — including when only one of the two is published and it crosses the
parameter or the default it lands on.

The published `fleetless.yaml` overrides all of them, so a fleet's settings
live in the console rather than on each robot:

```yaml
low_bandwidth:
  enter_lag_ms: 3000
  datapoint_max_hz: 0.5
  camera: stop
```

A datapoint that must keep its rate whatever the link is doing says so for
itself, and is the one thing the cap does not touch:

```yaml
datapoints:
  emergency_stop:
    topic: /estop
    type: std_msgs/msg/Bool
    field: data
    low_bandwidth: keep
```

A `camera_start` that arrives while the mode holds is refused, under
`reduce` as much as under `stop`: a new stream is new uplink, and the mode
exists because there is none to spare. The camera says so for itself —
`low_bandwidth`, with the sentence — rather than failing silently.

Nothing a retained datapoint held back is lost, only delayed: it buffers
every sample the ceiling kept off the wire, and the history fills in once the
link recovers. Without `retention` there is no buffer and the held-back
samples are gone, and a stream ended under `stop` stays ended until somebody
starts it again.

## 📄 What the robot exposes

Nothing, on its own. What a robot exposes is a document, `fleetless.yaml`,
which a developer writes in the console and the cloud publishes to the bridge:
the topics to sample, the cameras to stream, the services and actions a client
app may call. A cloud message naming anything the document does not name is
refused. The format is documented in the
[fleetless.yaml reference](https://docs.fleetless.dev/reference/fleetless-yaml/).

## 🔒 ROS-pure, by design

The bridge has no shell, systemd or HTTP features, and it will not get any.
There is no code path from a cloud message to a process launch or a system
change, and the bridge runs no server: no HTTP listener, no debug port, no
control channel other than the WebSocket it opened itself. The one exception
is the media path of a live video session, which is WebRTC and therefore binds
by construction; [SECURITY.md](SECURITY.md) states precisely when it exists.

A robot that needs local limits, such as a speed cap or a keep-out area, sets
them robot-side in ROS, in a node its owner controls. The bridge is not where
physics gets negotiated.

## 🚦 Exit codes

A supervisor cannot read log messages, so the exit code says what happened.

| Code | Meaning |
|---|---|
| 0 | Stopped on request (SIGINT/SIGTERM). Nothing is wrong. |
| 1 | Stopped by an unexpected error. |
| 2 | Stopped because retrying in this process cannot help: no token, an unusable cloud URL, a token the cloud rejects, a protocol version the cloud refuses, or another bridge that took the robot over. |

Code 2 is deliberate. A bridge that kept retrying a rejected token would
hammer the cloud and hide the real problem, and one that reconnected after
being superseded would trade the robot back and forth with its replacement
forever. Everything else is retried with a jittered exponential backoff from
1 s up to 30 s.

A protocol version the cloud refuses waits one to two minutes before that
exit, so the launch file's respawn retries every couple of minutes and a
fresh process picks up an upgraded package by itself. The fix is a newer
package, which the running process could never load.

## 📚 Documentation

- **[docs.fleetless.dev](https://docs.fleetless.dev)** — the developer
  documentation, from first connection to the API your app calls.
- **[fleetless.yaml](https://docs.fleetless.dev/reference/fleetless-yaml/)**
  — every section of the exposure document.
- **[When the link drops](https://docs.fleetless.dev/concepts/exposure/#when-the-link-drops)**
  and **[low-bandwidth mode](https://docs.fleetless.dev/reference/fleetless-yaml/low-bandwidth/)** —
  what the bridge does when the network does not cooperate, and what it
  stops doing so the rest keeps arriving.

Questions: hello@fleetless.dev.

## 🔐 Reporting a security issue

Email **security@fleetless.dev** rather than opening a public issue.
[SECURITY.md](SECURITY.md) states the bridge's surface in full and how a
report is handled.

## 🤝 Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) says how to build it, test it and get a
change merged. The test suite runs in a `ros:<distro>` container, so you need
Docker and nothing else. Issues belong at
[github.com/fleetless/bridge/issues](https://github.com/fleetless/bridge/issues).

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

## 📜 Licence

[Apache-2.0](LICENSE). Copyright 2026 Dehne Robotik GmbH; see
[NOTICE](NOTICE).
