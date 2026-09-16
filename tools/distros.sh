# SPDX-License-Identifier: Apache-2.0
# The ROS distributions this package is built for, and everything that differs
# between them. Sourced by ../build-deb.sh and ../run-tests.sh; it is not
# executable on its own.
#
# **Why a table and not a prefix.** Every distro-specific dependency is spelt
# out in full here rather than derived as "ros-$distro-" + a shared list. The
# derived form cannot fail: a distribution that renamed a package, or one whose
# entry nobody wrote, still produces a plausible `ros-<something>-cv-bridge`
# line, and `apt install` is then the first thing that notices — on a robot,
# after publication. The table's `*)` arm below refuses instead, naming the
# distribution and the field.
#
# The four ROS package names happen to be identical across humble, jazzy and
# lyrical today (measured with `apt-cache policy` in each distribution's own
# `ros:<distro>` image, 2026-09-08). That is a fact about those three
# distributions, not a rule, and writing it as a rule is what would hide the
# fourth one that breaks it.
#
# Fields:
#   codename          the Ubuntu codename, which is the debian revision suffix
#                     (`3.1.0-0jammy`) and the changelog's distribution field
#   ros_deps          the ROS apt packages, full names, one per dependency
#   python_site       where apt-distributed ROS Python packages live under the
#                     prefix, which is NOT the same directory on every
#                     distribution and is not derivable from the Python
#                     version either
#
# This used to be five fields. `numpy_constraint` and `pip_env` are gone with
# the PyPI installs they existed for: this package's runtime dependencies are
# all apt packages now (see debian/control.in and package.xml), so there is no
# `pip3 install` left to bound a numpy ceiling for or to pass PEP 668's
# override flag to. `python_site` stays — it is a fact about where a
# Python interpreter and ROS's own packaging put things, not about pip, and it
# still genuinely differs per distribution:
#
#   site    the directory an apt-distributed ROS Python package installs into,
#           read off `dpkg -L ros-<distro>-rclpy` and checked against what
#           `/opt/ros/<distro>/setup.sh` actually puts on PYTHONPATH:
#             humble   /opt/ros/humble/local/lib/python3.10/dist-packages
#             jazzy    /opt/ros/jazzy/lib/python3.12/site-packages
#             lyrical  /opt/ros/lyrical/lib/python3.14/site-packages
#           Humble puts BOTH `lib/pythonX.Y/site-packages` and
#           `local/lib/pythonX.Y/dist-packages` on the path and its own packages
#           in the second; jazzy and lyrical put only the first on the path and
#           their packages in it. So the humble spelling is not merely a
#           different name for the same place -- built into
#           `local/lib/.../dist-packages` on jazzy, this package installs
#           cleanly, `apt install` succeeds, and `import fleetless_bridge`
#           raises ModuleNotFoundError. That is the failure `debian/rules.in`'s
#           PYTHON3_SITE comment already warned about for one distribution;
#           three make it a table entry rather than a constant.
FLEETLESS_DISTROS="humble jazzy lyrical"

# Every field the renderer substitutes. A distribution missing any one of these
# is refused before a build starts rather than at the line that needs it.
FLEETLESS_DISTRO_FIELDS="codename ros_deps python_site"

# fleetless_distro_field <distro> <field>
# Prints the value on stdout. Returns non-zero, having said which pair is
# missing, when the table has no entry — including for a field that exists for
# some other distribution, which is the shape a half-finished entry takes.
fleetless_distro_field() {
    case "$1/$2" in
        humble/codename)         printf '%s' 'jammy' ;;
        humble/ros_deps)         printf '%s' 'ros-humble-rclpy ros-humble-rosidl-runtime-py ros-humble-cv-bridge ros-humble-sensor-msgs' ;;
        humble/python_site)      printf '%s' 'local/lib/python$(PYTHON3_VERSION)/dist-packages' ;;

        jazzy/codename)          printf '%s' 'noble' ;;
        jazzy/ros_deps)          printf '%s' 'ros-jazzy-rclpy ros-jazzy-rosidl-runtime-py ros-jazzy-cv-bridge ros-jazzy-sensor-msgs' ;;
        jazzy/python_site)       printf '%s' 'lib/python$(PYTHON3_VERSION)/site-packages' ;;

        lyrical/codename)         printf '%s' 'resolute' ;;
        lyrical/ros_deps)         printf '%s' 'ros-lyrical-rclpy ros-lyrical-rosidl-runtime-py ros-lyrical-cv-bridge ros-lyrical-sensor-msgs' ;;
        lyrical/python_site)      printf '%s' 'lib/python$(PYTHON3_VERSION)/site-packages' ;;

        *)
            echo "distros.sh: the distribution table has no '$2' for '$1'." >&2
            echo "distros.sh: add it to fleetless_distro_field in tools/distros.sh." >&2
            echo "distros.sh: refusing to guess — a guessed dependency name builds a" >&2
            echo "distros.sh: package that only apt on a robot finds wrong." >&2
            return 1 ;;
    esac
}

# fleetless_distro_require <distro>
# Refuses an unsupported distribution by name, listing the supported ones, and
# then refuses one whose table entry is incomplete. Both before any work.
fleetless_distro_require() {
    _known=no
    for _d in $FLEETLESS_DISTROS; do
        if [ "$_d" = "$1" ]; then _known=yes; fi
    done
    if [ "$_known" != yes ]; then
        echo "$(basename "$0"): '$1' is not a ROS distribution this package supports." >&2
        echo "$(basename "$0"): supported: $FLEETLESS_DISTROS" >&2
        echo "$(basename "$0"): (kilted is deliberately absent: it is not an LTS release." >&2
        echo "$(basename "$0"): See docs and bridge/README.md for what is supported until when.)" >&2
        return 1
    fi
    for _f in $FLEETLESS_DISTRO_FIELDS; do
        fleetless_distro_field "$1" "$_f" >/dev/null || return 1
    done
    return 0
}

# fleetless_distro_render <distro> <src-root> <dest-root>
# Copies <src-root> to <dest-root> and turns every `<name>.in` into `<name>`
# with the table's values substituted. The copy is what gets built, so the
# checkout keeps exactly one copy of the packaging and no distribution's
# rendered form is ever committed.
fleetless_distro_render() {
    _distro=$1
    _src=$2
    _dest=$3
    fleetless_distro_require "$_distro" || return 1

    _codename=$(fleetless_distro_field "$_distro" codename) || return 1
    _rawdeps=$(fleetless_distro_field "$_distro" ros_deps) || return 1
    _pysite=$(fleetless_distro_field "$_distro" python_site) || return 1
    # One `Depends:` continuation line per package, each ending in a comma, so
    # control.in can place the block between the lines that do not vary.
    _deps=$(printf '%s\n' $_rawdeps | sed 's/^/ /; s/$/,/')

    rm -rf "$_dest"
    mkdir -p "$_dest"
    # The tracked files and nothing else. A plain `cp -R` of the checkout
    # copies in the root-owned build litter previous container runs leave
    # behind (debian/<pkg>/, .pybuild/, debian/files), which would make the
    # next package partly out of the last one. `git archive` was rejected for
    # the opposite reason: it answers with HEAD, so it would silently build a
    # tree that is not the one on disk.
    #
    # Read from a process substitution rather than through a pipe: a `while`
    # on the right of a pipe runs in a subshell, where the `return 1` below
    # returns from the subshell and the copy failure is lost.
    while IFS= read -r -d '' _f; do
        mkdir -p "$_dest/$(dirname "$_f")"
        cp -p "$_src/$_f" "$_dest/$_f" || return 1
    done < <(cd "$_src" && git ls-files -z)

    # awk, not sed: the dependency block is multi-line, and inserting newlines
    # from a variable is not portable in sed.
    _rendered=""
    for _in in $(cd "$_dest" && find . -name '*.in' -type f); do
        _out="${_in%.in}"
        awk -v distro="$_distro" -v codename="$_codename" -v deps="$_deps" \
            -v pysite="$_pysite" '
            { gsub(/@ROS_DISTRO@/, distro)
              gsub(/@DEB_CODENAME@/, codename)
              gsub(/@ROS_DEPS@/, deps)
              gsub(/@PYTHON_SITE@/, pysite)
              print }
        ' "$_dest/$_in" > "$_dest/$_out" || return 1
        # Executability is not carried by the redirection above, and
        # debian/rules must be executable or dpkg-buildpackage stops before it
        # starts.
        if [ -x "$_dest/$_in" ]; then chmod +x "$_dest/$_out"; fi
        rm -f "$_dest/$_in"
        _rendered="$_rendered $_dest/$_out"
    done
    if [ -z "$_rendered" ]; then
        echo "distros.sh: no *.in template was found under $_src." >&2
        echo "distros.sh: the packaging is generated from templates, so a tree with" >&2
        echo "distros.sh: none of them would build a package with no debian/control" >&2
        echo "distros.sh: at all." >&2
        return 1
    fi

    # A placeholder nobody substituted is a silent gap: it renders as a literal
    # `@SOMETHING@` into a control file or a maintainer script and the build
    # carries on. Only the rendered files are swept -- a placeholder-shaped
    # string anywhere else was never going to be substituted, and this file's
    # own awk program is full of them.
    _left=$(grep -l '@[A-Z_]\{2,\}@' $_rendered 2>/dev/null || true)
    if [ -n "$_left" ]; then
        echo "distros.sh: unsubstituted placeholders remain after rendering $_distro:" >&2
        echo "$_left" | sed 's/^/  /' >&2
        return 1
    fi
    return 0
}
