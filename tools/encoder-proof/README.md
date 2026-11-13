# The encoder proof

A development fixture, never installed on a robot by the package. It answers
one question on the machine that has the hardware: does this encoder chain do
what the bridge needs from it?

```
python3 tools/encoder-proof/encoder_proof.py --backend nvenc
```

`--backend` is a value of `live.encoder`: `nvenc`, `vaapi`, `v4l2`, or `auto`
for all of them in the order the bridge tries them. Each chain of the backend
is run through the shipped pipeline class (`fleetless_bridge.live_encoder`)
with a synthetic moving pattern. No camera, no cloud and no network are
involved.

## What it needs

The bridge's own Python dependencies, and what `live.encoder` needs at run
time: `python3-gi`, `gir1.2-gstreamer-1.0`, `gir1.2-gst-plugins-base-1.0`,
`gstreamer1.0-plugins-base`, `gstreamer1.0-plugins-bad`, and the plugin that
holds the encoder element of the chain. On a Jetson the last comes with
L4T. Run it from a checkout of this repository, or copy the one file next to
an installed bridge.

## What it checks

For each chain:

- it can be built and started;
- one access unit comes back for each frame, within one second;
- the first frame, and every frame a keyframe is asked for, is an IDR picture
  with SPS and PPS, and no other frame is;
- a decoder makes exactly one picture of each access unit, and it is the
  picture that went in. That rules out B-frames and lookahead, and it rules
  out the wrong picture: swapped colour planes pass every other check;
- flat colour bars come out in their colours when each is converted by the
  matrix and the range the stream names. A viewer converts by that label, so
  a stream that names BT.709 over BT.601 samples decodes without a fault and
  shows every colour a little wrong. A stream that names nothing leaves the
  viewer to assume, as the software encoder's does; the output says which it
  is;
- the stream shrinks when the target bitrate is cut to a quarter and grows
  when it is restored. Each of the three stretches is two seconds of frames
  and never fewer than thirty, so the check means the same at a camera's two
  frames a second as at thirty;
- the pipeline goes back to NULL.

Then it runs the encoder class the bridge installs into a live session and
says which chain that picks on this machine, or that it falls back to
software and why.

## What it prints

Timings for the build and per frame, the measured bitrates, one line per
check, and a verdict:

- `VERDICT: PASS (<chain>)`, exit code 0: the chain the bridge would use does
  everything above.
- `VERDICT: FAIL (<chain>)`, exit code 1: a chain runs here and misses a
  requirement. Do not set `live.encoder` to it; the bridge's fallback catches
  an encoder that does not start, not one that draws a wrong picture.
- `VERDICT: no chain of <backend> can be built on this machine`, exit code 2:
  the hardware, its driver or its GStreamer plugin is not there. The reasons
  are above the verdict.

The whole output is what to send with a report about a chain.

## Which chains are proven

The [Live Video reference](https://docs.fleetless.dev/reference/fleetless-yaml/live/#which-encoders-are-proven)
keeps the list. Every chain is written from the documentation of its elements,
and this tool is how one gets onto that list: on a Jetson, a desktop NVIDIA
card, an Intel or AMD machine, a Raspberry Pi 4.

`--backend stand-in` runs a software encoder element (`x264enc`, from
`gstreamer1.0-plugins-ugly`) through the same checks. It proves that the tool
and the pipeline class work on a machine, and nothing about a chip.

## Options

`--width`, `--height`, `--fps` and `--bitrate-kbps` set the stream; use what
the robot's cameras are configured with. `--frames` and `--keyframe-every`
set how long it runs and how often a keyframe is asked for. `--chain` limits
the run to one chain of the backend, by the name the output shows.
