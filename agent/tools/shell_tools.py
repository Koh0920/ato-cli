from __future__ import annotations

import subprocess
from pathlib import Path


def run_shell(command: str, cwd: str | Path, timeout: int = 30) -> dict:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        shell=True,
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
