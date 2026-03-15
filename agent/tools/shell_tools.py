from __future__ import annotations

import os
from pathlib import Path
import shlex
import subprocess


def run_shell(command: str, cwd: str | Path, timeout: int = 30) -> dict:
    args = shlex.split(command, posix=os.name != "nt")
    completed = subprocess.run(
        args,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
