# 🤝 Contributing to the Fleetless Bridge

Thanks for showing up. This is how to build it, test it, and get it merged.

## Where the work happens

The canonical repository is
[github.com/fleetless/bridge](https://github.com/fleetless/bridge) — private
until the public release. **Pull requests are welcome** — we read them,
review them, land them.

**There is no CI.** Nothing runs automatically against a pull request: no
pipeline, no check, no bot. Every check this project has is a command in
this file, run by a person — the first person to run it should be you.
The checks:

| What | Command |
|---|---|
| the test suite | `./run-tests.sh` |
| the licence headers and declarations | `./run-tests.sh -k license` |
| the vendored contract schemas | `FLEETLESS_CONTRACTS_DIR=<a contracts package> ./run-tests.sh -k contracts_sync` |
| that the package still builds | `./build-deb.sh <distro>` (see below) |
| that no commit message cites something a public reader cannot open | `python3 scripts/verify_commit_messages.py` |

## Contributor Licence Agreement

By submitting a contribution you agree that it is licensed under the Apache
License 2.0 (see `LICENSE`), that you wrote it or otherwise have the right to
submit it, and that you grant Dehne Robotik GmbH the right to distribute it
under that licence and to relicense the project's own distribution of it.

## Running the tests

The suite needs a ROS 2 environment, a real `rclpy` node on a real DDS
domain, and OpenCV. You don't need any of that on your own machine:

```sh
./run-tests.sh                        # Humble, the default
./run-tests.sh --distro jazzy         # Jazzy
./run-tests.sh --distro lyrical       # Lyrical Luth
```

builds the image in `Dockerfile.dev` (`ros:<distro>` plus the dependencies
`package.xml` declares) and runs `pytest` inside it. The first run pays for
the downloads; Docker caches the rest.

**A green run on one distribution says nothing about another**, so the three
run separately and report separately. Each gets its own image tag
(`fleetless-bridge-dev-<distro>`), built from that distribution's own base
image — see `tools/distros.sh` for what else differs between them.

Extra arguments go to pytest, and **`-k` is the one that filters**:

```sh
./run-tests.sh -k reconnect -x        # only tests whose name matches
./run-tests.sh -k license             # the licence-header guard
```

`./run-tests.sh test/test_jobs.py` does **not** run only that file — the
argument is appended to a command that already names `test/`, so you get the
whole suite plus that file again, and the exit code is the suite's.

The suite *claims* a `ROS_DOMAIN_ID` — an exclusive `flock` over 70..101,
held for the whole run — so it can't hear, or be heard by, a robot on the
default domain or a second copy of itself. The number is printed on every
run; pass `ROS_DOMAIN_ID=<n>` to re-run one exact combination.
`run-tests.sh` says what that separation does and does not cover.

## Building the package

One command per ROS distribution, from a bare checkout:

```sh
./build-deb.sh humble       # -> dist/ros-humble-fleetless-bridge_<v>-0jammy_all.deb
./build-deb.sh jazzy        # -> dist/ros-jazzy-fleetless-bridge_<v>-0noble_all.deb
./build-deb.sh lyrical      # -> dist/ros-lyrical-fleetless-bridge_<v>-0resolute_all.deb
```

It builds in a container of that distribution's own base image, named
`fleetless-bridge-build-<distro>` and removed afterwards, and copies the
artifact into `dist/` before the container exits — `dpkg-buildpackage` writes
to the *parent* of the source tree, which inside that container is the
container's own root, and `--rm` discards it.

**There is no `debian/control` in this checkout, and that is the point.**
The packaging is templates (`debian/*.in`) plus one table of what differs
per distribution (`tools/distros.sh`); `build-deb.sh` renders them into
`build/deb/<distro>/` and builds there. So the distribution is a variable,
not three copies of `debian/` drifting apart: an unknown one is refused by
name before anything renders, and a dependency this table has no entry for
is a hard error — not a `ros-<guess>-` prefix nobody notices until
`apt install` on a robot.

Once all three are built, `./apt/verify-suites.sh` proves the apt side of
it: it builds a throwaway signed repository from
`apt/reprepro/conf/distributions`, serves it, and in each distribution's own
container installs that distribution's package and checks that the other
two are unreachable. Against a repository that is already served,
`./apt/check-suites.sh <base-url>` asks the cheaper index-level question and
distinguishes a suite nobody has published (exit 2) from one that is served
and wrong (exit 1). See `apt/README.md`.

`apt/README.md` describes what happens to the `.deb` afterwards.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/), in English:

```
fix(camera): a stalled V4L2 read no longer blocks teardown
```

`scripts/verify_commit_messages.py` checks the messages you're about to push
and refuses anything that names something a reader of the public history
can't resolve. Run it before you push:

```sh
python3 scripts/verify_commit_messages.py
```

## What a change needs

- **English** for code, comments, commit messages and documentation.
- **A test that fails without the change.** Write it, watch it go red, then
  fix the code. A test added afterward usually proves the code it was
  written against, not the behaviour that was asked for.
- **The SPDX header** `# SPDX-License-Identifier: Apache-2.0` at the top of
  every new source file — Python, shell, anything whose comments start with
  `#`, including the extension-less maintainer scripts under `debian/`. Line
  1, or line 2 where a shebang has to come first.
  `test/test_license_headers.py` fails without it, and checks every file in
  the published set, not just the `.py` half.
- **No shell, systemd or HTTP features.** The bridge is ROS-pure by design;
  `SECURITY.md` says why. A robot that needs local limits sets them
  robot-side, in ROS.
- **`timestamp_ms` is the bridge's own capture time**, never the moment
  something was received. The whole product rests on that promise.

If you've changed the vendored contract schemas under `test/contracts/`,
update `test/contracts/SOURCE.md` with the `@fleetless/contracts` version
they came from — it has the whole procedure. `test/test_contracts_sync.py`
catches drift, and **it only runs when you tell it where a real
`@fleetless/contracts` package is**:

```sh
npm pack @fleetless/contracts@1.0.5 && tar xf fleetless-contracts-1.0.5.tgz
FLEETLESS_CONTRACTS_DIR="$PWD/package" ./run-tests.sh
```

Without that variable those four comparisons skip — `run-tests.sh` prints
that they are skipping — because there is nothing to compare against. It
isn't inferred from a directory beside your clone: a directory named
`contracts` next to a checkout is not evidence that it holds the version
this package vendored, and a guard that can't tell those apart reports
agreement with a tree nobody chose.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
