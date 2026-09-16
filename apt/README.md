# apt.fleetless.dev — the bridge's apt repository

`ros-<distro>-fleetless-bridge` is published as a signed Debian package at
`apt.fleetless.dev`, served over HTTPS and managed with
[reprepro](https://salsa.debian.org/debian/reprepro). The component is `main`
and the package is `Architecture: all` — one build serves amd64 and arm64.

**One suite per ROS 2 distribution, named after it:** `humble`, `jazzy` and
`lyrical`. Each carries only its own distribution's package — that's the whole
isolation mechanism. A robot subscribes to one suite; the other two
distributions' packages are absent from the indices it fetches, not merely
unselected, so `apt` answers `Unable to locate package` rather than offering
something built for another Ubuntu release.

`reprepro/conf/` is the repository's own configuration, kept here rather than
on the server so the suite list, the architectures and the signing key's
fingerprint are reviewable in git. Its suites must match `tools/distros.sh`
exactly — `test/test_apt_suites.py` asserts it: a suite with no table entry
can never be built for, and a table entry with no suite has nowhere to go.

## Adding the repository to a robot or a dev machine

Substitute your own distribution for `humble` — the suite and the package name
change together, and nothing else does:

```sh
export ROS_DISTRO=humble          # or jazzy, or lyrical
curl -fsSL https://apt.fleetless.dev/key.gpg \
  | sudo tee /usr/share/keyrings/fleetless.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/fleetless.gpg] https://apt.fleetless.dev $ROS_DISTRO main" \
  | sudo tee /etc/apt/sources.list.d/fleetless.list
sudo apt update && sudo apt install "ros-$ROS_DISTRO-fleetless-bridge"
```

The key is served in **binary** form, not ASCII-armored. An armored key looks
entirely valid on its own — `gpg --show-keys` reads it, right fingerprint,
right uid — and `apt-get update` still fails with `NO_PUBKEY` naming that
exact key, because apt's `signed-by=` mechanism doesn't accept the armored
form. The commands above write whatever `key.gpg` contains, so the fix belongs
on the export, not the fetch.

## Building the `.deb` from this source tree

One command per ROS distribution:

```sh
./build-deb.sh humble       # -> dist/ros-humble-fleetless-bridge_<v>-0jammy_all.deb
./build-deb.sh jazzy        # -> dist/ros-jazzy-fleetless-bridge_<v>-0noble_all.deb
./build-deb.sh lyrical      # -> dist/ros-lyrical-fleetless-bridge_<v>-0resolute_all.deb
```

It builds in a container of that distribution's own base image
(`ros:<distro>`), named `fleetless-bridge-build-<distro>` and removed
afterwards, installing the Debian build tooling there (`debhelper`,
`dh-python`, `python3-all`, `python3-setuptools`, `fakeroot`, `dpkg-dev`).
Those six are named rather than left implicit: this section used to say the
image "already has them" when it had none, and `dpkg-buildpackage` aborts at
`dpkg-checkbuilddeps` before doing any work.

**The debian revision suffix is not a constant.** It's the Ubuntu codename the
ROS distribution targets — `-0jammy` for Humble, `-0noble` for Jazzy,
`-0resolute` for Lyrical — so the three packages are three different versions
of three differently-named source packages, each going into its own suite
(below).

**There is no `debian/control` in this checkout.** The packaging is templates
(`debian/*.in`), rendered per distribution from one table — `tools/distros.sh`
— into `build/deb/<distro>/`. That table is where a per-distribution
difference gets expressed; one is live today, the site directory an
apt-distributed ROS Python package installs into, explained there along with
the measurement behind it. (It used to be three fields: the numpy bound and
the pip flag Ubuntu 24.04+ needed for PEP 668 are gone, along with the PyPI
installs they existed for — every runtime dependency is an apt package now.)

The rendered changelog carries the package's whole upstream history with the
target distribution's revision suffix, so a Jazzy changelog shows
`3.0.0-0noble` for a version that was only ever published for Humble. Most
entries describe upstream changes that apply to every distribution; a handful
say explicitly where they don't (the 1.0.0 and 3.1.0 entries both name which
distribution a claim is true of, rather than leaving it to be read as
universal). Only the top entry corresponds to a build actually published here.

**The copy out of the container isn't optional, and it happens inside the
container on purpose.** `dpkg-buildpackage` always writes its `.deb`,
`.buildinfo` and `.changes` to the *parent* of the source tree — its own
behaviour, not this recipe's — and `--rm` throws that away. `build-deb.sh`
copies into `dist/` before the container exits and chowns the result back to
the invoking user; `dist/` is also where the publish step below reads from.

## Publishing

Publishing one built package is one command against a reprepro basedir that
has this directory's `reprepro/conf/` in place as its `conf/`:

```sh
./apt/publish-deb.sh /srv/apt dist/ros-jazzy-fleetless-bridge_3.1.0-0noble_all.deb
```

**Where that basedir is has changed (2026-09-12).** It used to be `/srv/apt`
on the VM that served `apt.fleetless.dev`, the one machine that held the
signing key. The key now lives in the platform's secret store and the
repository is published from the deploy runner, which builds the basedir in
a temporary directory from the served pool, calls this script, and uploads
the result; that pipeline belongs to the deployment, not to this package.
Nothing in this directory changed for that; the script below is what does the
filing either way.

**The suite is not an argument.** `reprepro includedeb <suite> <deb>` compares
nothing: `includedeb humble ros-jazzy-fleetless-bridge_*.deb` succeeds, and the
first thing that notices is a Humble robot offered a package built for Ubuntu
24.04. `publish-deb.sh` reads the `Package:` field out of the artifact with
`dpkg-deb` — not off the filename, which a rename can make say anything —
derives the suite from it, refuses a distribution the table doesn't know, and
refuses a package whose debian revision names a different distribution's
codename than its own name does. Then it prints what it decided before it
writes.

Publishing one distribution touches exactly one suite: reprepro rewrites only
the `dists/<suite>/` tree it filed into, so the other two are not re-read, not
re-signed and not changed.

### Asking a served repository whether the suites are right

```sh
./apt/check-suites.sh https://apt.fleetless.dev
./apt/check-suites.sh https://apt.fleetless.dev --require-published
```

It reads each `dists/<suite>/main/binary-<arch>/Packages` and requires the
`Package:` name in it to be exactly that suite's own, **its `Version:` to
match `debian/changelog.in`'s top entry** for that distribution, and
`InRelease` to be served beside it. It's deliberately cheap enough to run from
anywhere, and it **separates two answers that look alike**:

* **not published yet** — every index for a suite is 404. Exit **2**. Nobody
  has published that distribution yet; that's work outstanding, not a
  repository fault, and something running for another reason should say so
  and carry on. `--require-published` turns it into an error, for when
  publishing is the task.
* **broken** — the suite is served *and* wrong: it indexes another
  distribution's package, serves one architecture and not the other, or serves
  `Packages` with no `InRelease`. Exit **1**. None of this can happen before a
  publish, so none of it is red at base — each fires exactly when something
  went wrong.

A repository that doesn't answer at all exits 1 with its own message, not "not
published" — a dead host and an unpublished suite are the two states this is
for keeping apart.

`publish-deb.sh` calls this same script — `check-suites.sh --local <basedir>
--distro <name> --require-published`, reading the freshly-written files off
disk rather than a served URL — right after `reprepro includedeb`. A `.deb`
that some earlier hand-run `reprepro includedeb` filed into the wrong suite,
or at the wrong version, is caught at the moment somebody who can fix it is
looking, by the one comparison rather than a second copy of it.

### Verifying by installing

```sh
./build-deb.sh humble && ./build-deb.sh jazzy && ./build-deb.sh lyrical
./apt/verify-suites.sh
```

`verify-suites.sh` builds a **throwaway** reprepro repository from
`reprepro/conf/distributions` — the tracked file, with only `SignWith:`
substituted for a key generated inside the container and thrown away with it —
serves it over HTTP, then in each distribution's own `ros:<distro>` container:
adds the repository the way this README tells a user to, installs
`ros-<distro>-fleetless-bridge`, imports `fleetless_bridge`, and runs
`apt-get install` for the *other two* distributions' packages, which must
fail.

**Installing is the check; a file listing is not.** A package can sit in
`pool/` and be unreachable from every suite, and a suite can be indexed and
still serve the wrong package. Within the install check, `apt-cache show` is
the half that discriminates: with a Jazzy `.deb` deliberately filed into the
`humble` suite, `apt-get install ros-jazzy-fleetless-bridge` on jammy still
fails — on `ros-jazzy-cv-bridge`, which doesn't exist there — so an
install-only check reports the refusal working while the suite serves the
wrong package.

**`reprepro` won't replace a version with a different build of itself.** A
package whose version string is already in the distribution is refused,
whatever its checksums say — a feature, and also why a change that alters only
what ships inside the .deb still needs a new `debian/changelog` entry. Not
hypothetical: `3.0.0-0jammy` was published before the relicensing, and its
installed `/usr/share/doc/ros-humble-fleetless-bridge/copyright` declares the
software closed and not redistributable. The Apache-2.0 copyright reaches
robots as `3.1.0-0jammy` — no other version string could have carried it
there.

## Signing

The repository is signed with a key whose private half exists only in the
keyring of the machine that serves it — generated in place, not generated
elsewhere and copied in. No transfer step means nothing to secure in transit
and nothing to remember to delete afterwards.

`reprepro/conf/distributions` names the key by **fingerprint** in its
`SignWith:` line, and every command that touches the key names a fingerprint
rather than a uid. Not pedantry: a key was once generated under a guard of the
shape "generate unless a key with this uid exists", on a machine where a
retired key with a byte-identical uid was still in the keyring. The guard did
nothing at all, silently and successfully, and the next command would have
signed the repository with the retired key while every document said
otherwise. **A guard whose success and whose no-op look identical is not a
guard.**
