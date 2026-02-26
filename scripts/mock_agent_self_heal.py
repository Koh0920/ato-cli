#!/usr/bin/env python3
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def main() -> int:
    cmd = [
        "cargo",
        "test",
        "-p",
        "ato-cli",
        "--test",
        "fail_closed_test",
        "test_19_self_healing_loop_recovers_from_policy_violation",
        "--",
        "--ignored",
        "--nocapture",
    ]
    proc = subprocess.run(cmd, cwd=ROOT)
    return proc.returncode


if __name__ == "__main__":
    sys.exit(main())
