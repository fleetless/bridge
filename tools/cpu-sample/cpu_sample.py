#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure how much of one core a process uses over a fixed window.

A development fixture, never installed on a robot by the package. It reads
the process's CPU time from `/proc/<pid>/stat` at the start and at the end of
the window and prints the mean share of one core in between: 100 % is one
core busy the whole time, and a process with several busy threads can exceed
it. With `--log`, it also counts the lines a log file gained during the same
window that say the snapshot queue stalled.

    python3 cpu_sample.py --pid 4242 --seconds 300
    python3 cpu_sample.py --match fleetless_bridge --seconds 300 --log bridge.log

It is the instrument for one comparison: the bridge before and after a
change, idle (snapshots only) and live (every camera watched), five minutes
each. The number is the kernel's own accounting of user and system time for
the process and all its threads. Child processes are not in it.

Standard library only, so it runs wherever the bridge does.
"""
import argparse
import os
import sys
import time

#: The line `ros_runtime.py` logs when a snapshot pull gives up on the ROS
#: work queue.
STALL_LINE = "The snapshot pull timed out"


def cpu_ticks(stat_text):
    """User plus system time of a process, in clock ticks, from the text of
    its `/proc/<pid>/stat`.

    The second field is the command in parentheses and may itself hold
    spaces and parentheses, so the fields are counted from the last closing
    one: state is the first after it, and user and system time are the 12th
    and 13th (fields 14 and 15 of the line)."""
    after_command = stat_text.rsplit(")", 1)[1].split()
    return int(after_command[11]) + int(after_command[12])


def read_ticks(pid, proc="/proc"):
    with open(os.path.join(proc, str(pid), "stat")) as stat:
        return cpu_ticks(stat.read())


def share_of_one_core(ticks_before, ticks_after, seconds, ticks_per_second):
    """The mean share of one core, in percent, between two readings."""
    return 100.0 * (ticks_after - ticks_before) / ticks_per_second / seconds


def find_pid(text, proc="/proc", own_pid=None):
    """The one process whose command line contains `text`. More than one or
    none is an error that names what was found: a measurement of the wrong
    process is worse than none."""
    own_pid = os.getpid() if own_pid is None else own_pid
    found = []
    for name in os.listdir(proc):
        if not name.isdigit() or int(name) == own_pid:
            continue
        try:
            with open(os.path.join(proc, name, "cmdline"), "rb") as cmdline:
                command = cmdline.read().replace(b"\0", b" ").decode("utf-8", "replace").strip()
        except OSError:
            continue  # gone since the listing, or not ours to read
        if text in command:
            found.append((int(name), command))
    if len(found) != 1:
        raise LookupError(
            "{} processes match {!r}{}".format(
                len(found), text,
                "".join("\n  {} {}".format(pid, command[:120]) for pid, command in sorted(found)),
            )
        )
    return found[0][0]


def count_lines(path, offset, needle=STALL_LINE):
    """How many lines the file gained after `offset` that contain `needle`."""
    with open(path, "rb") as log:
        log.seek(offset)
        return sum(1 for line in log if needle.encode() in line)


def main(argv=None, out=None, sleep=time.sleep, clock=time.monotonic):
    out = out if out is not None else sys.stdout
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--pid", type=int, help="the process to measure")
    which.add_argument("--match", help="measure the one process whose command line contains this")
    parser.add_argument("--seconds", type=float, default=300.0, help="the window (default: 300)")
    parser.add_argument("--interval", type=float, default=30.0,
                        help="print the share of each such stretch on the way (0: only the result)")
    parser.add_argument("--log", help="a log file of the process; stalled-queue lines it gains are counted")
    parser.add_argument("--label", default="", help="a word for this run, repeated in the result line")
    args = parser.parse_args(argv)

    try:
        pid = args.pid if args.pid is not None else find_pid(args.match)
    except LookupError as error:
        print("cpu_sample: {}".format(error), file=out)
        return 2
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    try:
        log_offset = os.path.getsize(args.log) if args.log else 0
        first = previous = read_ticks(pid)
    except OSError as error:
        print("cpu_sample: {}".format(error), file=out)
        return 2

    print("cpu_sample: pid {}, {:g} s{}".format(
        pid, args.seconds, ", counting {!r} in {}".format(STALL_LINE, args.log) if args.log else ""),
        file=out, flush=True)
    started = previous_at = clock()
    ends = started + args.seconds
    try:
        while True:
            now = clock()
            if now >= ends:
                break
            step = ends - now if args.interval <= 0 else min(args.interval, ends - now)
            sleep(step)
            now = clock()
            ticks = read_ticks(pid)
            if args.interval > 0 and now < ends:
                print("  {:6.0f} s  {:6.1f} %".format(
                    now - started,
                    share_of_one_core(previous, ticks, now - previous_at, ticks_per_second)),
                    file=out, flush=True)
            previous, previous_at = ticks, now
    except OSError:
        print("cpu_sample: pid {} ended {:.0f} s into the window; no result".format(
            pid, clock() - started), file=out)
        return 1

    elapsed = previous_at - started
    share = share_of_one_core(first, previous, elapsed, ticks_per_second)
    result = "RESULT{}: {:.1f} % of one core over {:.0f} s".format(
        " " + args.label if args.label else "", share, elapsed)
    if args.log:
        result += ", {} stalled-queue line(s)".format(count_lines(args.log, log_offset))
    print(result, file=out, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
