#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs the bridge test suite inside the container built from Dockerfile.dev, so
# no local ROS or Python setup is assumed. Extra arguments go to pytest, e.g.
#   ./run-tests.sh -k reconnect -x
# **They are APPENDED to `test/`, they do not replace it.** `./run-tests.sh
# test/test_foo.py` therefore runs the whole suite AND that file again, which
# looks exactly like running one file until you read the count. `-k <expr>` is
# what narrows a run.
#
# The ROS distribution is chosen with `--distro <name>`, which must come first:
#   ./run-tests.sh --distro jazzy -k urdf
# It is a named option and not a leading positional argument precisely because
# the arguments after it are pytest's: a bare first word would be ambiguous
# with a pytest expression, and the ambiguous reading is the one that silently
# runs Humble while the caller reads the result as Jazzy's.
#
# **A green run on one distribution says nothing about another.** Each
# distribution gets its own image (fleetless-bridge-dev-<distro>), built from
# its own base image straight from package.xml (see below — there is nothing
# left to render per distribution), and the image tag carries the
# distribution so that a stale build cannot answer for a different one.
set -euo pipefail
cd "$(dirname "$0")"

. tools/distros.sh

DISTRO=humble
# `first` rather than `${1:-}` twice: under `set -u`, `${1#--distro=}` on a run
# with no arguments at all is an unbound-variable error, and the default form
# does not carry into the substitution.
first=${1:-}
if [ "$first" = "--distro" ]; then
    if [ "$#" -lt 2 ]; then
        echo "run-tests.sh: --distro needs a distribution name." >&2
        echo "run-tests.sh: supported: $FLEETLESS_DISTROS" >&2
        exit 2
    fi
    DISTRO=$2
    shift 2
elif [ "$first" != "${first#--distro=}" ]; then
    DISTRO=${first#--distro=}
    shift
fi
# Refused by name, listing the supported ones, before an image is built: a typo
# must not fall back to the default and report a Humble result as somebody
# else's distribution.
fleetless_distro_require "$DISTRO"

# Same probe as run-bridge.sh: fall back to sudo when `docker info` does not
# answer, rather than failing on the socket. Not being in the docker group is
# the usual reason and it is not the only one, and the probe cannot tell them
# apart — a daemon that is down fails this way too, and then the sudo attempt
# fails in its turn, which is the readable outcome.
DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

# The image tag carries the distribution, so two distributions can never
# share a build cache entry or an image tag.
IMAGE="fleetless-bridge-dev-$DISTRO"

# Built straight from the checkout: Dockerfile.dev COPYs only package.xml,
# which is not a template, so there is nothing here for tools/distros.sh to
# render before the build. (That was not always true — this image used to
# also need a per-distribution rosdep/constraints.txt, rendered from
# rosdep/constraints.txt.in, and a repo-local rosdep source registered for
# `jsonschema`, before pip left the runtime dependency list entirely and
# `python3-jsonschema` turned out to be a public rosdep key on its own.)
# `fleetless_distro_require` above is still what refuses an unsupported
# distribution before this line ever runs.
$DOCKER build -q -t "$IMAGE" \
    --build-arg "ROS_DISTRO_TAG=$DISTRO" \
    -f Dockerfile.dev . >/dev/null
echo "run-tests.sh: ROS distribution $DISTRO (image $IMAGE)" >&2

# ROS_DOMAIN_ID: every test file that touches rclpy runs a real node on
# a real DDS domain, with real UDP multicast discovery — and by default that
# domain is 0, the same one run-bridge.sh/run-fake-robot.sh use for the demo
# robot. Nothing here namespaces topic/action names, so a live demo robot
# and this suite hearing each other is not bad luck, it is what an
# unisolated domain always does: a subscription meant to see nothing picks
# up a real publisher's frames, an action invoke meant to time out gets
# answered by a real server.
#
# **A fixed default is that same collision one domain further out**, and this
# script pinned 77 for everybody. That isolates the suite from the demo robot
# and not from a second copy of itself. Reproduced rather than reasoned about,
# two concurrent `-k urdf` runs (19 tests, ~4s) mirroring the `docker run`
# below:
#
#   two domains (control)                   2 pairs   2/2 green
#   one shared domain, as pinned            6 pairs   5/6 RED, BOTH sides
#   one shared domain, --network none       4 pairs   4/4 green
#
# So it is about five in six per pair, it reddens **both** runs at once, and
# the failure it produces is:
#
#   Failed: DID NOT RAISE TimeoutError
#
# on the URDF tests — the neighbour's `/robot_description` publisher
# delivering content a test asserts must not arrive. It points squarely at
# ros_runtime.py, which is the expensive direction to be pointed in: the tests
# pass in isolation, so the invited reading is a regression that is not there.
# If you see that assertion fail, look for a second run before you look at the
# code.
#
# **A random default was considered and rejected.** Two runs each drawing
# uniformly from a range of 32 collide at 1/32, 3.1% — the law, not a sample;
# an earlier draft of this line said "~1%" and was understating it threefold.
# Even at 3.1% it is strictly worse to live with than five in six, because a
# red nobody can reproduce is the failure mode that has cost most here; a red
# that happens most of the time gets diagnosed on its first day. So the domain
# is *claimed*, not guessed: an exclusive `flock` on a per-domain lock file,
# taken over 70..101 and held for the whole run by this shell. If every domain
# in the range is held, this **fails** rather than sharing one — falling back
# to a shared domain would defeat the entire change.
#
# **What that claiming does and does not guarantee.** This file has now
# overstated a domain-separation guarantee twice — once for the hash fallback,
# and then, in the very commit that fixed that one, for the locked path — so
# the scope is written down here: it separates runs that share
# ONE lock namespace, up to 32 of them at once. It does not separate runs that
# do not share a namespace — reproduced, two concurrent runs with different
# `$TMPDIR` both claimed domain 70, both printing "locked for this run" — and
# a 33rd concurrent run is refused rather than given a shared domain. Which
# namespace was used is therefore printed with the domain.
#
# `--network none` is the structurally right answer and is deliberately not
# used yet: it is a stronger claim than "no neighbour", and two camera-health
# tests were only passing under it by accident of a network timeout being
# slow. Those two are fixed; the mode itself is a separate decision, because
# it also removes the container's access to anything else.
#
# An explicit ROS_DOMAIN_ID from the caller wins and skips all of this — that
# is how you re-run a failing combination, and it is why the chosen domain is
# printed on every run: a number nobody can see is a number nobody can
# reproduce.
DOMAIN_FIRST=70
DOMAIN_LAST=101

# The lock directory sits in a world-writable place, so it is treated as
# hostile ground rather than as scratch space, and the name carries the uid so
# two users on one machine do not contend for one directory in the first place.
#
# The three properties below are not belt-and-braces, they close a measured
# hole. The original path was a fixed name under `/tmp` opened with `>`, and
# `>` TRUNCATES. Reproduced: pre-create the lock directory's own path as a
# symlink to a directory holding a file named `70`, run this script once, and
# that file is zero bytes — any local user could make a test run destroy any
# file the runner can write. To re-run that reproduction against the code
# below, plant the symlink at `$DOMAIN_LOCK_DIR` as it is spelt now; the
# planted name is the whole point of the exercise and the old one no longer
# reaches it. So:
#
#   * the name is per-uid, so two users never contend for one directory (two
#     runs by the SAME user share it, which is the point of it);
#   * the directory is refused if it is a symlink, is not a directory, or is
#     not owned by us, rather than being followed on trust — the check that
#     survives somebody creating the per-uid name first;
#   * the lock files are opened with `>>`, which creates but never truncates.
#     `flock` wants a file descriptor and nothing else; the truncation was
#     never doing any work.
#
# `-m 700` on the mkdir keeps a directory we did create from being filled with
# planted symlinks afterwards; the ownership check covers the case where we did
# not create it.
#
# **The residual, named rather than implied away: those checks are not atomic.**
# Between the symlink test, the ownership test and the open, the path can be
# swapped. What actually holds after that race is the pair that does not
# depend on timing — the open cannot truncate, and a directory this script
# created is writable by nobody else. The checks buy a clear refusal in the
# ordinary case; they are not the thing standing between a hostile `/tmp` and
# your files.
#
# TMPDIR: the namespace is per-TMPDIR, so two runs on one machine that disagree
# about TMPDIR lock different directories and can meet on one DDS domain. That
# is why the directory is printed beside the domain on every successful claim
# rather than only mentioned here — the same reason the domain itself is
# printed. Hardcoding `/tmp` would remove the dependence and also remove the
# only way to test this block without touching the real one.
DOMAIN_LOCK_DIR="${TMPDIR:-/tmp}/fleetless-bridge-ros-domains-$(id -u)"

# The lock files are empty and are never deleted, on purpose: unlinking one
# races the next run that is about to open it, and an empty file per domain in
# a temp directory costs nothing.
#
# `domain_lock_unopenable` counts the domains whose lock file could not be
# opened AT ALL, which is a different state from "held by another run" and used
# to be reported as that one: an unwritable directory printed 32 `Permission
# denied` lines and then the headline "every ROS domain is locked … wait for a
# run to finish", which names a cause the script had not established and gives
# advice that cannot help. The caller below reads this counter and says which
# of the two happened.
domain_lock_unopenable=0
claim_domain() {
    if [ -L "$DOMAIN_LOCK_DIR" ]; then
        echo "run-tests.sh: $DOMAIN_LOCK_DIR is a SYMLINK. Refusing to follow it:" >&2
        echo "run-tests.sh: this script would create and lock files through it, and in a" >&2
        echo "run-tests.sh: world-writable temp directory a symlink under a name this" >&2
        echo "run-tests.sh: script picks is somebody else's doing. Remove it, or set" >&2
        echo "run-tests.sh: TMPDIR to a directory you own." >&2
        return 2
    fi
    if [ -e "$DOMAIN_LOCK_DIR" ]; then
        if [ ! -d "$DOMAIN_LOCK_DIR" ]; then
            echo "run-tests.sh: $DOMAIN_LOCK_DIR exists and is not a directory." >&2
            echo "run-tests.sh: Remove it, or set TMPDIR to a directory you own." >&2
            return 2
        fi
        # `find -maxdepth 0 -user` rather than `stat`: the two stat flavours
        # disagree (`-c %u` against `-f %u`) and this script also has to work
        # on the bash 3.2 / BSD userland that macOS ships.
        if [ -z "$(find "$DOMAIN_LOCK_DIR" -maxdepth 0 -user "$(id -u)" 2>/dev/null)" ]; then
            echo "run-tests.sh: $DOMAIN_LOCK_DIR is not owned by you (uid $(id -u))." >&2
            echo "run-tests.sh: Refusing to create lock files in another user's directory." >&2
            echo "run-tests.sh: Remove it, or set TMPDIR to a directory you own." >&2
            return 2
        fi
    elif ! mkdir -m 700 -p "$DOMAIN_LOCK_DIR"; then
        echo "run-tests.sh: could not create the lock directory $DOMAIN_LOCK_DIR." >&2
        return 2
    fi
    d="$DOMAIN_FIRST"
    while [ "$d" -le "$DOMAIN_LAST" ]; do
        # Fixed fd 9 rather than bash's `{fd}` auto-assignment: this script
        # has to PARSE on the bash 3.2 that macOS still ships, and `{fd}>`
        # is a syntax error there — in the whole file, not only in a branch
        # nobody reaches.
        #
        # No `2>/dev/null` on this line, and that is not an oversight: `exec`
        # with no command applies its redirections to the SHELL, so it would
        # send the rest of this script's own stderr to /dev/null — every
        # message below included. It was written that way once and the run
        # printed no domain at all, which is the failure this whole block
        # exists to make impossible. A lock file that will not open is worth
        # seeing anyway.
        if exec 9>>"$DOMAIN_LOCK_DIR/$d"; then
            if flock -n 9; then
                TEST_ROS_DOMAIN_ID="$d"
                return 0
            fi
        else
            domain_lock_unopenable=$((domain_lock_unopenable + 1))
        fi
        d=$((d + 1))
    done
    return 1
}

if [ -n "${ROS_DOMAIN_ID:-}" ]; then
    TEST_ROS_DOMAIN_ID="$ROS_DOMAIN_ID"
    echo "run-tests.sh: ROS_DOMAIN_ID=$TEST_ROS_DOMAIN_ID (yours, taken as given)" >&2
elif command -v flock >/dev/null 2>&1; then
    claim_status=0
    claim_domain || claim_status=$?
    if [ "$claim_status" -eq 0 ]; then
        echo "run-tests.sh: ROS_DOMAIN_ID=$TEST_ROS_DOMAIN_ID (locked for this run;" >&2
        echo "run-tests.sh: pass it back to re-run this exact combination)" >&2
        # The lock namespace is per-TMPDIR, so this line is what tells you why
        # a run that "holds" domain 70 met another run that also holds 70.
        echo "run-tests.sh: lock directory $DOMAIN_LOCK_DIR (only runs sharing" >&2
        echo "run-tests.sh: this directory are separated from each other)" >&2
    elif [ "$claim_status" -eq 2 ]; then
        # The directory itself was refused; claim_domain has already said which
        # of its properties failed. Nothing about contention is known here.
        exit 1
    elif [ "$domain_lock_unopenable" -gt 0 ]; then
        # Not contention: this run never got far enough to ask whether anybody
        # holds these domains. Say the number, because "some could not be
        # opened and the rest were held" is a third state and is the one a
        # partially-broken directory produces.
        echo "run-tests.sh: could not OPEN $domain_lock_unopenable of the lock files in" >&2
        echo "run-tests.sh: $DOMAIN_LOCK_DIR" >&2
        echo "run-tests.sh: (the errors are printed above). That is not contention: no" >&2
        echo "run-tests.sh: domain was found free, but for those files this run never" >&2
        echo "run-tests.sh: asked. Waiting will not help. Fix the directory's permissions," >&2
        echo "run-tests.sh: or set TMPDIR to a directory you own, or pass ROS_DOMAIN_ID=<n>." >&2
        exit 1
    else
        # What is established: all 32 lock files opened, all 32 already held.
        # By WHOM is an inference — flock does not say — so the message offers
        # it as one. This script is the only thing that takes these locks, so
        # far as it knows.
        echo "run-tests.sh: every ROS domain in $DOMAIN_FIRST..$DOMAIN_LAST is held: all" >&2
        echo "run-tests.sh: $((DOMAIN_LAST - DOMAIN_FIRST + 1)) lock files opened, all $((DOMAIN_LAST - DOMAIN_FIRST + 1)) locked by somebody else. Nothing" >&2
        echo "run-tests.sh: but this script takes these locks, so look for other runs of" >&2
        echo "run-tests.sh: it against $DOMAIN_LOCK_DIR." >&2
        echo "run-tests.sh: Refusing to share a domain: two suites on one DDS domain redden" >&2
        echo "run-tests.sh: EACH OTHER about five times in six, and the failure reads as a" >&2
        echo "run-tests.sh: bug in ros_runtime.py. Wait for a run to finish, or pass" >&2
        echo "run-tests.sh: ROS_DOMAIN_ID=<n> to opt out on purpose." >&2
        exit 1
    fi
else
    # `flock` is util-linux and macOS does not ship it. Derived from the
    # checkout path, and **the only thing that buys is determinism**: the same
    # directory gives the same domain every time, so a failure is
    # reproducible, which a random number would not be.
    #
    # It guarantees nothing about two checkouts getting *different* domains,
    # and an earlier draft of this comment said it did. That is the same
    # mistake one level down from the pinned 77: a hash folded into a range of
    # 32 collides on unrelated inputs at one in 32, **3.12%**, whatever the
    # inputs are. **The law is what belongs in a comment, not a sample of it:**
    # two independent 200-pair draws of exactly this quantity gave 3.0% and
    # 5.5%, so a comment quoting one of them invites an argument about the
    # sample rather than about the mechanism. So this branch is a fallback, not
    # a second mechanism, and the message below says so in those terms.
    TEST_ROS_DOMAIN_ID=$(( ($(printf '%s' "$PWD" | cksum | cut -d' ' -f1)
        % (DOMAIN_LAST - DOMAIN_FIRST + 1)) + DOMAIN_FIRST ))
    echo "run-tests.sh: ROS_DOMAIN_ID=$TEST_ROS_DOMAIN_ID (UNLOCKED FALLBACK: no flock on" >&2
    echo "run-tests.sh: this machine, so the domain is only DERIVED from this checkout's" >&2
    echo "run-tests.sh: path. It is the same every time from here, and that is all it" >&2
    echo "run-tests.sh: promises: it does NOT separate two runs started from this" >&2
    echo "run-tests.sh: directory, and two different checkouts can still land on the" >&2
    echo "run-tests.sh: same number. If a URDF test fails with 'DID NOT RAISE" >&2
    echo "run-tests.sh: TimeoutError', look for a second run before you look at the code.)" >&2
    # **The sentence this replaces said the locked path "does guarantee
    # distinct domains".** It does not, unconditionally: the guarantee is
    # scoped to one lock namespace and to 32 concurrent runs. That overclaim
    # was introduced by the commit that fixed the overclaim one level up, in
    # the same file, which is why this one names its scope in the message
    # rather than only in a comment.
    echo "run-tests.sh: 'brew install util-linux' gives you the locked path, which does" >&2
    echo "run-tests.sh: separate runs — as far as one lock namespace and $((DOMAIN_LAST - DOMAIN_FIRST + 1)) concurrent" >&2
    echo "run-tests.sh: runs reach. Runs that disagree about \$TMPDIR lock different" >&2
    echo "run-tests.sh: directories and can still meet on one domain, and one run past" >&2
    echo "run-tests.sh: that count is refused rather than given a shared domain." >&2
fi

# The container runs as root on a mounted working tree, so anything it writes
# there lands as root-owned litter. Nothing needs to be written: no .pyc files,
# no pytest cache.
# `safe.directory`: the working tree is mounted from the host, so its files
# are owned by the host user while the container runs as root, and git refuses
# to read a repository it thinks somebody else owns ("detected dubious
# ownership", exit 128). Two tests ask git what is tracked -- the prose guard
# and the licence-header guard -- and both are meant to fail loudly rather than
# scan a smaller set, so this has to be granted rather than worked around.
# Passed as environment rather than written into a config file, so nothing is
# left behind in the mounted tree.
# nofile: every ROS image starts a container at a soft limit of 1024 open
# files, and this suite builds a RosRuntime in over two hundred tests. Humble
# and Jazzy stay under it; **Lyrical does not** -- a full run there reached test
# ~880 and then failed 135 tests, the first with `RuntimeError:
# eventfd_select_interrupter: Too many open files` inside `_rclpy.Node` and
# every one after it with `Context.init() must only be called once`, because
# the half-built context is never torn down. That cascade points at
# ros_runtime.py and is not about it at all, which is the expensive direction
# to be pointed in -- the same shape as two runs sharing a DDS domain, one
# block up.
#
# **The numbers below hold under the RAISED ceiling below, not the stock
# 1024-descriptor one.** At the stock limit Lyrical's run dies at test ~880
# (the paragraph above) and never reaches a shape to describe, so the raised
# ceiling is a precondition for the numbers, not something they were checked
# against afterward: the numbers say what the raised ceiling reveals over the
# suite's whole length, and reproducing them needs that same raised limit, not
# 1024. They are written down so nobody has to derive them twice. Open
# descriptors, counted after every test over a full run on each distribution:
#
#   humble    slope -0.0002 fds/test, ending at 15   (flat)
#   jazzy     slope -0.0004 fds/test, ending at 15   (flat)
#   lyrical   slope +0.34   fds/test, peak 1180      (grows, then falls back)
#
# Same harness code in all three, so it is not harness code. Narrowed with
# controls, on Lyrical:
#
#   one long-lived RosRuntime, 200 apply/remove config cycles   FLAT (25 -> 25)
#   plain rclpy init/create_node/destroy_node/shutdown x120     FLAT (31 -> 31)
#   RosRuntime created and stopped x120                         18 -> 1399
#
# So it is one RosRuntime lifecycle, 12 descriptors each: 6 sockets, 3 files, an
# eventpoll, a timerfd and an eventfd -- one Fast DDS participant. **And it is
# not a leak.** Thirty cycles took it from 18 to 379, and a single
# `gc.collect()` took it straight back to 19; dropping the references
# `RosRuntime.stop()` leaves in place (from outside, with no bridge code
# changed) keeps it flat at 19 throughout. rclpy 3 (Humble) releases the
# participant inside `destroy_node()`; rclpy 10 (Lyrical) releases it when the
# Python object is collected, and a thousand-test run allocates faster than the
# cyclic collector reclaims.
#
# **Nothing about that reaches a robot.** `main.py` builds exactly one
# RosRuntime per process and stops it once, in `_run_client`'s `finally`,
# immediately before the process exits -- so the deferred release costs twelve
# descriptors once, at shutdown, and the kernel takes them back. A robot
# running for weeks changes configuration, which is the case measured flat
# above. Making `stop()` drop its own references would make the teardown
# deterministic and is worth doing on its own merits; it is not a fix for
# anything here, and it belongs to whoever changes ros_runtime.py next.
#
# Raised for every distribution rather than only for the one that needs it: the
# limit is not a property this suite wants to differ per distribution, and a
# second policy beside the table would be a second place to look. 524288 is the
# hard limit in all three images, so 65536 is a raise this container is allowed
# to make for itself.
run() {
    $DOCKER run --rm --ulimit nofile=65536:524288 \
        -e PYTHONDONTWRITEBYTECODE=1 -e "ROS_DOMAIN_ID=${TEST_ROS_DOMAIN_ID}" \
        -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' "$@" \
        python3 -m pytest test/ -q -p no:cacheprovider ${pytest_args[@]+"${pytest_args[@]}"}
}
# `${a[@]+"${a[@]}"}` rather than `"${a[@]}"`: under `set -u`, bash 3.2 --
# which is what macOS ships, and what a developer runs this with -- treats an
# empty array as unset and aborts. So `./run-tests.sh` with no pytest argument
# died before starting a container, with `pytest_args[@]: unbound variable`
# and nothing else. Two people read that as a suite failure today.
pytest_args=("$@")

# The checkout, and nothing above it. This used to mount the checkout's whole
# PARENT directory read-write, on the condition that a sibling `contracts/`
# existed -- so on any machine where the clone happens to sit next to one, the
# container ran as root over everything else in that directory too, and the
# command that widened the mount looked identical to the one that did not.
run_args=(-v "$PWD":/ws -w /ws)

# test_contracts_sync.py compares the vendored schemas, the vendored constants
# and PROTOCOL_VERSION against a real @fleetless/contracts package. It needs to
# be told where one is, and there is exactly one way to tell it:
# FLEETLESS_CONTRACTS_DIR, pointing at a contracts checkout or an unpacked
# `npm pack @fleetless/contracts@<version>`. It used to be inferred from a
# sibling directory called `contracts` instead, which answered the wrong
# question twice over -- silently skipping wherever that layout was absent, and
# silently comparing against whatever tree carried the name where it was
# present. The mount is read-only and is that directory itself, not the
# directory it happens to sit in.
#
# Unset, every comparison in test_contracts_sync.py that needs a real contracts
# package skips. **No number here, deliberately**: the count was written as
# "four" when it was five, and a count in prose beside a set somebody will add
# to is a thing that goes stale silently, which is the whole failure this
# sentence exists to prevent one level up. `pytest -rs` prints the real list.
# This is said out loud rather than left to a lower-case `s` in pytest's
# output, because a drift guard that goes quiet looks exactly like one that
# passed.
if [ -n "${FLEETLESS_CONTRACTS_DIR:-}" ]; then
    contracts_dir="$(cd "$FLEETLESS_CONTRACTS_DIR" && pwd)"
    run_args+=(-v "$contracts_dir":/contracts:ro -e FLEETLESS_CONTRACTS_DIR=/contracts)
    echo "run-tests.sh: comparing vendored contracts against $contracts_dir" >&2
else
    echo "run-tests.sh: FLEETLESS_CONTRACTS_DIR is unset, so the vendored-contracts" >&2
    echo "run-tests.sh: comparisons will SKIP. Set it to a @fleetless/contracts" >&2
    echo "run-tests.sh: checkout (or an unpacked npm tarball) to run them." >&2
fi

run "${run_args[@]}" "$IMAGE"
