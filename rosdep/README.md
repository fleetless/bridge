# Runtime dependencies and rosdep

Every runtime dependency this package declares (`package.xml`'s
`exec_depend`s) resolves through a **public** rosdistro key on every
distribution this bridge is built for: `python3-aiohttp`, `python3-aiortc`,
`python3-opencv`, `cv_bridge`, `python3-numpy`, `python3-defusedxml`,
`rclpy`, `rosidl_runtime_py`, `sensor_msgs`. `python3-jsonschema` is missing
on purpose — it's a `test_depend` (below), correctly absent from every
distribution's `Depends:`.

No repo-local rosdep source lives in this directory any more. It existed
only for `jsonschema`; its apt key turned out to already be public once the
version floor this suite needed was dropped (below). `Dockerfile.dev`'s
`rosdep install` needs nothing registered beyond `package.xml` itself.

**One import this list doesn't cover.** `live.py` imports `av` directly
(the frame type the encoder consumes); `python3-av` has no rosdep key in
the public database yet. Not undeclared in practice — `python3-aiortc`'s
own apt `Depends:` installs it, so the import is satisfied wherever
`python3-aiortc` is — but it's a gap between manifest and code.
`package.xml`'s own comment says so; this paragraph goes when the key is
filed.

## `python3-jsonschema`, the version story

Apt ships `python3-jsonschema` at three versions across the distributions
this bridge is built for (measured with `apt-cache policy` in each
`ros:<distro>` image): `3.2.0` on Humble, `4.10.3` on Jazzy, `4.19.2` on
Lyrical. `jsonschema.Draft202012Validator` has shipped since jsonschema
4.0.0, so Jazzy and Lyrical both carry it — **only Humble's 3.2.0 doesn't.**
(An earlier version of this file said "the first two," conflating this
with a different floor, 4.18, where `RefResolver` was replaced by
`referencing` — unrelated to which validator classes exist.)

`test/schemas.py` prefers `Draft202012Validator` where installed and falls
back to `Draft7Validator` on Humble alone. That fallback holds only as long
as no vendored schema under `test/contracts/` uses a keyword Draft 2020-12
added over Draft 7 (`prefixItems`, `unevaluatedProperties`,
`dependentRequired`, `dependentSchemas`, `minContains`/`maxContains`,
`$dynamicRef`, `$dynamicAnchor`) — true today, and kept true by a computed
test in `test/schemas.py` that walks every vendored schema for exactly those
keywords, rather than by a one-off manual sweep. See that module's
docstring for the guard and its break fixture.

`rosdep install` (`Dockerfile.dev`) therefore needs no `python3-pip` and no
PEP 668 override on any of the three distributions, for `jsonschema` or
anything else this package ships. **`tools/capture_camera_state.py` is the
one documented exception**: its usage recipe mints a token for that one-off
capture path with `livekit-api`, which is neither an apt package nor a
bridge dependency. The recipe installs it into a throwaway local venv, not
system-wide, so no PEP 668 override there either — but it's still a real
`pip install` against PyPI that a developer following the recipe will hit,
outside the installed package (`setup.py` doesn't ship `tools/`).

## One test dependency that does not exist on all three

`action_tutorials_interfaces` — used by the introspection and `RosRuntime`
suites for a real `Fibonacci` action — isn't released for Lyrical at all
(`rosdep` there: `Cannot locate rosdep definition for
[action_tutorials_interfaces]`; no `ros-lyrical-action-tutorials-interfaces`
package exists, and `ros-lyrical-action-tutorials-py` now depends on
`example_interfaces` instead). `example_interfaces` ships `Fibonacci.action`
on humble, jazzy and lyrical with identical field sets, so `package.xml`
names that one — a dependency removed rather than a difference to express.

## What a green `rosdep install` does and does not prove

It means the manifest is honest about everything it can name through a
public key — not that every import is declared. `python3-av` above is the
one gap, stated rather than left to be discovered.

**A manifest can be honest and still be lucky.** `sensor_msgs` is a
declared `exec_depend` — it was once `test_depend` only, and the bridge
imported it anyway, working purely because `cv_bridge`'s own apt package
happens to pull `sensor_msgs` in transitively. `rosdep install` reported
success either way; only the *reason* changed, from "declared" to "lucky."
A check measuring "only `package.xml` to go on" that also installs
`test_depend`s is measuring a wider environment than it claims to — hence
`--dependency-types=exec`.
