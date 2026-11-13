# 🤖 fleetless_bridge

**The only part of Fleetless that runs on your robot.**

[Fleetless](https://fleetless.dev) turns a ROS 2 robot into a hosted REST and
realtime API. The bridge is the ROS 2 node that makes that possible: it opens
one WebSocket to the cloud, identifies the robot with its token, and from then
on does exactly what the published configuration names. It samples topics
into datapoints, runs services and actions as jobs, publishes messages,
streams cameras, and uploads URDF and mesh assets when asked. An optional
publisher timeout message is an operating aid sent by the running bridge
process; it cannot respond if that process or the robot computer fails.

It talks ROS on one side and the Fleetless wire protocol on the other, and
nothing else. Shipped as `ros-<distro>-fleetless-bridge`, Python, Apache-2.0.

## Safety notice

Version 1.0 — Effective from 1 November 2026

Fleetless is not a safety function. It is not a safety component, a safety-rated controller or an emergency-stop system, and it does not replace the risk assessment of your robot and its surroundings.

- Commands sent through Fleetless can be delayed, lost or executed out of order, for example when a network connection drops.
- Every robot must reach, on its own, the safe state defined in its risk assessment when the connection to Fleetless is lost or the bridge stops working. Depending on the robot, stopping at once is not always the safe state.
- The failsafe message of a bridge publisher is an operating aid, not a safety function. It is sent by the bridge process and cannot be sent if that process or the robot's computer fails.
- Keep a physical emergency stop within reach whenever a robot can move.
- AI agents that control robots through Fleetless can make mistakes. Use an AI agent to move a robot only while a person supervises every motion and can stop the robot at once. Do not let AI agents move robots unattended. Where Fleetless allows it, for example through the roles of an app, limit what an AI agent may do.

Commands may also be executed more than once, and displayed status may be stale. Safety functions, including the response to communication loss, must be implemented independently of Fleetless and validated for the specific robot and its hazards. Human supervision and an emergency stop do not replace the required protective measures. Nothing in this notice excludes our liability under mandatory law.

You are responsible for the safe integration and operation of your robots, as described in section 8 of our [Terms of Service](https://fleetless.dev/terms).

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

The package is served signed from `registry.fleetless.dev/apt`, one suite per
distribution. Substitute your own distribution for `humble`; the suite and the
package name change together, and nothing else does.

```sh
export ROS_DISTRO=humble          # or jazzy, or lyrical
curl -fsSL https://registry.fleetless.dev/apt/key.gpg \
  | sudo tee /usr/share/keyrings/fleetless.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/fleetless.gpg] https://registry.fleetless.dev/apt $ROS_DISTRO main" \
  | sudo tee /etc/apt/sources.list.d/fleetless.list
sudo apt update && sudo apt install "ros-$ROS_DISTRO-fleetless-bridge"
```

If you already installed from `apt.fleetless.dev`, change only the URL in
`/etc/apt/sources.list.d/fleetless.list` to `https://registry.fleetless.dev/apt`,
then run `sudo apt-get update`. The signing key is unchanged.

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
| `FLEETLESS_STATE_DIR` | no | `~/.local/state/fleetless-bridge` | Where the bridge persists its own job ↔ goal mapping across a restart. Never `/tmp`. If it cannot be written, jobs still run, but a restart finds their goals as external ones (a warning is logged). |

Set them in the launch file or in the environment.

### Fast DDS action-server discovery profile

`share/fleetless_bridge/fastdds/action-server-liveness.xml` is an **opt-in**
Fast DDS participant profile for a robot's selected action-server process
and its Fleetless Bridge process. It advertises a 1 s participant lease
every 250 ms. The bridge retains its 3 s liveness debounce, so a missed
announcement alone does not end a job. In three simulator kill tests, jobs
settled in 3.840–4.065 s. Measure discovery and false-loss behavior on the
robot's own link before enabling the profile there.

In a ROS launch file, add
`additional_env={'FASTRTPS_DEFAULT_PROFILES_FILE': profile_path}` to the
`Node` actions for the chosen action server and the Fleetless Bridge. Resolve
`profile_path` from the installed `fleetless_bridge` package share directory.
Do not export this variable for a whole robot launch or use it as a global
machine environment: it changes every participant in each affected process.
An action server sharing a process with unrelated ROS nodes cannot be scoped
independently through this profile; place it in its own process first.

To roll back, remove those two `additional_env` entries and restart the two
processes. Their ordinary Fast DDS discovery settings then apply again.

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

A datapoint that should bypass the bridge's low-bandwidth sampling cap says
so for itself:

```yaml
datapoints:
  motor_temperature:
    topic: /motor_temperature
    type: std_msgs/msg/Float32
    field: data
    low_bandwidth: keep
```

`keep` only bypasses that sampling cap. Network delay, loss and stale status
are still possible. Do not use it as an emergency-stop or other safety path.

A `camera_start` that arrives while the mode holds is refused, under
`reduce` as much as under `stop`: a new stream is new uplink, and the mode
exists because there is none to spare. The camera says so for itself —
`low_bandwidth`, with the sentence — rather than failing silently.

Nothing a retained datapoint held back is lost, only delayed: it buffers
every sample the ceiling kept off the wire, and the history fills in once the
link recovers. Without `retention` there is no buffer and the held-back
samples are gone, and a stream ended under `stop` stays ended until somebody
starts it again.

## 📦 Live video packet size

Live video leaves the robot as RTP packets, and the largest one is a setting.
The default is 1200 bytes, which is what browsers send and fits a path with an
MTU of 1280, the size NetBird and Tailscale set. A packet that does not fit
such a path is not trimmed on the way. It is dropped, and the viewer gets a
track and no picture.

| Parameter | Default | Meaning |
|---|---|---|
| `live.max_packet_bytes` | `1200` | The largest RTP packet the bridge sends, before SRTP: header, header extensions and payload, retransmissions included. An integer from `576` to `1350`. |

It holds for every camera of the robot, and it moves at runtime:

```sh
ros2 param set /fleetless_bridge live.max_packet_bytes 1000
```

The packets of the next frame obey the new value; no stream restarts. A value
outside the bounds, or one that is not an integer, is refused by the set with
the rule, and the parameter keeps what it had. The ceiling is not a taste: the
SRTP library protects no packet above 1356 bytes.

The published `fleetless.yaml` overrides the parameter for as long as it names
the key:

```yaml
live:
  max_packet_bytes: 1000
```

Lower it when a viewer sits behind a tunnel inside a tunnel: take the smallest
MTU on the path and subtract 80. Smaller packets mean more of them, so go as
low as the path needs and no lower.

## 🎛️ Live video encoder

By default the bridge encodes live video on the CPU. A robot with an encoder
chip can use that instead:

| Parameter | Default | Meaning |
|---|---|---|
| `live.encoder` | `software` | What encodes live video. `software` is the bridge's own encoder on the CPU, VP8 or H.264 as the media server picks. Every other value is H.264 from a hardware encoder. |

| Value | Encoder | GStreamer element, tried in this order |
|---|---|---|
| `software` | the CPU | none |
| `nvenc` | NVIDIA: a Jetson, then a desktop card | `nvv4l2h264enc`, then `nvh264enc` |
| `vaapi` | Intel and AMD graphics | `vah264enc`, then `vah264lpenc`, then `vaapih264enc` |
| `v4l2` | a V4L2 encoder, as on a Raspberry Pi 4 | `v4l2h264enc` |
| `auto` | the first of `nvenc`, `vaapi`, `v4l2` that starts | as above |

```sh
ros2 param set /fleetless_bridge live.encoder nvenc
```

It holds for every camera of the robot and applies at the next live start; a
running stream keeps the encoder it has. With any value but `software` a
camera's `width` and `height` must be even, since H.264 takes no odd size: the
cloud refuses such a configuration, and the bridge refuses the live start of a
camera it encodes with `live_unavailable` and a message naming the setting and
the size. Its snapshots go on. A passed-through camera meets its own check
instead: its stream cannot have an odd size, so it never matches the
configured one. The `live` section of the published `fleetless.yaml`
overrides the parameter for as long as it names the key:

```yaml
live:
  encoder: nvenc
```

**The hardware chains are written from the documentation of their elements,
and tested here through a software stand-in.** Which of them have been proven
on hardware is listed in the
[Live Video reference](https://docs.fleetless.dev/reference/fleetless-yaml/live/#which-encoders-are-proven).
Before relying on one, run `tools/encoder-proof/` on the machine: it runs the
chain with a moving pattern and says whether it delivers what the bridge
needs.

The hardware encoders are reached through GStreamer, which the package does
not depend on. Everything but `software` needs `python3-gi`,
`gir1.2-gstreamer-1.0`, `gir1.2-gst-plugins-base-1.0`,
`gstreamer1.0-plugins-base` and `gstreamer1.0-plugins-bad` on the robot, and
the plugin with the encoder element: on a Jetson it comes with L4T,
`gstreamer1.0-plugins-bad` has `nvh264enc`, `vah264enc` and `vah264lpenc`,
`gstreamer1.0-vaapi` has `vaapih264enc`, and `gstreamer1.0-plugins-good` has
`v4l2h264enc`.

**A hardware encoder that does not start costs no stream.** If GStreamer is
missing, no chain can be built, or a chain returns nothing within a second
for a frame, that session is encoded in software H.264 instead and the bridge
logs one warning with the backend, the chain and the reason. Until
`live.encoder` changes or the bridge restarts, a chain that cannot be built
is not tried again, and a chain that did not encode a camera's first picture
is not tried again for pictures of that size: one camera a chip does not
take leaves the chip to the others. The fallback catches an encoder that
fails. It does not catch one that draws a wrong picture, so look at the
stream once after choosing one.

With a hardware encoder the bitrate follows `bitrate_kbps` over its whole
range. The software encoders clamp it: VP8 to 250 to 1500 kbps, H.264 to 500
to 3000 kbps. A session that fell back from a hardware encoder is the
exception: its software H.264 keeps the whole range.

GStreamer is loaded with the first frame of the first hardware session after
the bridge starts. When its plugin registry is missing or stale, it then runs
its own plugin scanner, `gst-plugin-scanner`, as a child process, again for
the rest of the scan if a plugin crashes it, and then writes the registry to
`~/.cache/gstreamer-1.0`. That delays that one picture: by
0.2 s for the 250 plugins of this repository's test image, by more where more
are installed. It happens again after an update of a GStreamer package, and
at every start of a bridge whose user cannot write there; `GST_REGISTRY`
names another file. [SECURITY.md](SECURITY.md) says what that child is and is
not, and what else GStreamer's libraries may write.

A hardware encoder whose driver stops answering costs one late frame: the
session goes on in software, and a pipeline that does not shut down within
two seconds is left behind and its chain not built again until `live.encoder`
changes or the bridge restarts.

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
