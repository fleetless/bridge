# Security

## Reporting a vulnerability

Email **security@fleetless.dev** with what you did, what happened, what you
expected, and a version — the `ros-<distro>-fleetless-bridge` package version
you have installed, or the git commit. We acknowledge within three working
days and tell you what we intend to do.

Please do not open a public issue for a vulnerability until we have had a
chance to respond.

## What this package is, for the purpose of a threat model

The Fleetless Bridge runs on a robot, beside that robot's ROS 2 graph — the
one Fleetless component on hardware somebody else owns. That's why the
surface below is worth stating precisely.

### It runs no server; live video is the one thing that binds

Every connection the bridge establishes is outbound:

| to | why |
|---|---|
| the cloud, over `wss://` | the one control connection; carries configuration, commands and telemetry |
| the cloud's asset store, over `https://` | uploads URDF and mesh files, with a per-sync bearer credential |
| a LiveKit server, over WebRTC | live video, when a viewer asks for it |
| a camera, over `rtsp://`, `http(s)://` or a local `/dev/video*` device | only the sources the configuration names |

No HTTP server, no debug port, no control channel besides the WebSocket the
bridge opened itself. Nobody reaches the bridge without already being the
cloud it authenticated to.

**The residual, stated plainly: a live video session binds and accepts.**
WebRTC isn't outbound-only. While a stream runs, the ICE agent inside
`aiortc` binds local UDP sockets to gather host candidates, then accepts
inbound STUN binding requests, DTLS and SRTP on them from the negotiated
peer — the media path is bidirectional by construction. When `join` names a
TURN server (`connection_factory` in `live.py` passes `join.ice_servers`
through verbatim), the agent also opens an outbound connection to it and
relays through the allocation it gets there: a robot-initiated connection to
a server the cloud named, not a new listener on the robot. Three things
bound what's left exposed:

- Only while a stream runs. The ICE agent belongs to the peer connection
  `live.py` builds in `LivePublisher.start` and closes in
  `LivePublisher.stop` — including every path that tears down a lost
  session. No stream, no peer connection, nothing bound.
- Only the cloud can start one: a `camera_start` frame on the authenticated
  control socket, naming a camera the robot's configuration document
  declares. Nothing else in the package opens a room.
- What arrives is media for a negotiated session, not a control channel — no
  path from an inbound RTP packet to a ROS publish, a job, or a
  configuration change.

Need a robot that binds nothing at all? Don't configure a camera — the rest
of the bridge (datapoints, jobs, publishers, assets) never leaves the one
outbound WebSocket. Check with `ss -lunp` on the robot; it shows the media
sockets for as long as somebody is watching a camera.

### It runs no shell and manages no services

**The bridge is ROS-pure** — no shell, systemd or HTTP features, and that's a
security property, not a style choice. No code path runs from a cloud
message to a system change. None runs to a process launch or to a file write
outside the upload path either, with the one exception named below:
GStreamer's plugin scanner, a process whose command line the cloud has no say
in, and the files GStreamer and its libraries write for themselves. A robot
that needs local limits (a speed cap, an area
restriction) sets them robot-side, in ROS, in a node the robot's owner
controls — not through this bridge.

The one child process the bridge starts itself is a Python `multiprocessing`
worker that owns a V4L2 capture device, so a hung driver read can't block the
rest of the bridge. It runs this package's own function, not a command line —
the only configuration value that reaches it is the device path, and
`validate_device_path` refuses anything but a non-traversing path under
`/dev/`.

GStreamer starts a second one, and only on a robot whose `live.encoder` is
not `software`. The hardware encoders are reached through GStreamer, which
the bridge loads with the first live session that uses one. When GStreamer's
plugin registry is missing or stale, its initialisation runs GStreamer's own
plugin scanner (`gst-plugin-scanner`) as a child process, and starts it again
for the rest of the scan whenever a plugin crashes it, so one scan can start
it several times. The scanner writes the registry file to the bridge user's
cache directory, `$XDG_CACHE_HOME/gstreamer-1.0/` (`~/.cache/gstreamer-1.0/`
by default). That happens at the first hardware start, again after an update
of a GStreamer package, and at every start of a bridge whose user cannot
write that directory.

GStreamer's libraries may write short-lived files of their own besides the
registry. One is known: with liborc 0.4.42 the scanner creates a file for
code it compiles, `orcexec.*`, in `$XDG_RUNTIME_DIR` or else in `$HOME`, and
deletes it at once. The driver of an encoder chip is a library GStreamer
loads too, and may keep files of its own; this project has run none yet.

Nothing from a cloud message reaches the scanner's command line: the binary
and its arguments are GStreamer's. What the cloud can do is cause it to run,
by publishing a configuration that names a hardware encoder and starting a
camera. The bridge does not turn the child off (`GST_REGISTRY_FORK=no`):
scanning in the bridge's own process would load every installed plugin into
it.

### What the cloud can make it do

Everything the cloud can ask for is bounded by the robot's configuration
document, written by a developer in the Fleetless Console:

- **subscribe** to the topics named as datapoints or camera sources, and read
  those messages;
- **call** the services and actions named as jobs, with a message the developer
  wrote and in which only the positions marked `${name}` may be filled by a
  caller;
- **publish** the messages named as publishers, again only into the marked
  positions;
- **read** files reachable from a `package://` URI inside the ROS share
  directory, to answer an asset request.

A cloud message naming anything the document doesn't name is refused — it
can't introduce a topic, a service, an action or a file path of its own.

### File access

`package://` URIs resolve against the ROS package share directory, and the
result must stay inside it. The check is lexical (`normpath`, no symlink
following) — deliberately: a physical-path check rejects every file in a
`colcon build --symlink-install` workspace, the standard ROS development
layout. **The residual, stated plainly**: a symlink placed *inside* an
installed package that points outside it is followed. Anybody who can write
into the robot's install tree can already do more than that.

### Credentials

- `FLEETLESS_TOKEN` comes from the environment and binds the bridge to
  exactly one robot. Sent to the cloud on the handshake, never logged.
- Camera credentials arrive in the configuration document and stay with the
  source they belong to — redacted from every log line and every error the
  bridge reports upward. An error carries a type name, never the string that
  raised it.
- The asset upload bearer is used in an `Authorization` header, never in a URL
  or a query string.

### What is out of scope

- The ROS 2 graph itself. Anything already on the robot's DDS domain can
  publish onto the topics the bridge reads and call the services it calls —
  the bridge is a participant in that graph, not a boundary around it.
- The robot's own operating system, its user accounts and its network.
- The Fleetless cloud. This document covers the bridge only; report a
  cloud-side vulnerability to the same address, security@fleetless.dev.

## Supported versions

The most recent published version gets security fixes, **in each supported
ROS 2 distribution's own apt suite** — `humble`, `jazzy` and `lyrical` today.
Older versions don't, and neither does a distribution past its own end of
life: `README.md`'s **Supported ROS 2 distributions** table names each date,
and Humble's is May 2027.
