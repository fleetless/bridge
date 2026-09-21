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
| `FLEETLESS_UPLINK_KBPS` | no | — | The robot's total uplink budget in kbps. Unset means live video is admitted without a bandwidth check. |
| `FLEETLESS_UPLINK_RESERVE_PCT` | no | `20` | Percentage of the budget held back for the control socket, so telemetry and commands are never crowded out by video. |
| `FLEETLESS_UPLINK_RESERVE_MIN_KBPS` | no | `128` | Absolute floor for that reserve, because a percentage shrinks with the budget, and a small budget is when the control channel needs protecting most. |

Set them in the launch file or in the environment. Live video may use what is
left after the reserve, and a camera start that would exceed it is refused
with a `camera_state` naming `uplink_budget` rather than left to fight for
bandwidth. The budget can also be changed while the bridge runs by publishing
a `std_msgs/msg/UInt32` on `/fleetless/uplink_kbps`; `0` stops every stream.

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
| 2 | Stopped because it will not heal by itself: no token, an unusable cloud URL, a token the cloud rejects, a protocol version the cloud refuses, or another bridge that took the robot over. |

Code 2 is deliberate. A bridge that kept retrying a rejected token would
hammer the cloud and hide the real problem, and one that reconnected after
being superseded would trade the robot back and forth with its replacement
forever. Everything else is retried with a jittered exponential backoff from
1 s up to 30 s.

## 📚 Documentation

- **[docs.fleetless.dev](https://docs.fleetless.dev)** — the developer
  documentation, from first connection to the API your app calls.
- **[fleetless.yaml](https://docs.fleetless.dev/reference/fleetless-yaml/)**
  — every section of the exposure document.
- **[When the link drops](https://docs.fleetless.dev/concepts/exposure/#when-the-link-drops)**
  and **[link pressure](https://docs.fleetless.dev/concepts/exposure/#link-pressure)** —
  what the bridge does when the network does not cooperate.

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
