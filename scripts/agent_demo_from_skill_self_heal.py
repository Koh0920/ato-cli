#!/usr/bin/env python3
import json
import os
import pathlib
import pty
import re
import select
import shutil
import signal
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler

ROOT = pathlib.Path(__file__).resolve().parents[1]
TEMPLATE_SKILL = ROOT / "scripts" / "demo" / "SKILL.md"
ATO_BIN = ROOT / "target" / "debug" / "ato"
PROMPT = "Approve this policy? [y/N]:"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"heal-ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def ensure_ato_binary() -> pathlib.Path:
    if ATO_BIN.exists():
        return ATO_BIN
    subprocess.run(["cargo", "build", "-p", "ato-cli"], cwd=ROOT, check=True)
    if not ATO_BIN.exists():
        raise RuntimeError(f"ato binary not found: {ATO_BIN}")
    return ATO_BIN


def run_with_auto_consent(cmd: list[str], env: dict[str, str], timeout_sec: int = 40):
    pid, fd = pty.fork()
    if pid == 0:
        os.execvpe(cmd[0], cmd, env)

    output = bytearray()
    start = time.time()
    responded = False

    try:
        while True:
            if time.time() - start > timeout_sec:
                os.kill(pid, signal.SIGKILL)
                _, status = os.waitpid(pid, 0)
                return (124, output.decode(errors="replace"), status)

            r, _, _ = select.select([fd], [], [], 0.2)
            if fd in r:
                try:
                    chunk = os.read(fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                output.extend(chunk)
                text = output.decode(errors="replace")
                if PROMPT in text and not responded:
                    os.write(fd, b"y\n")
                    responded = True

            waited, status = os.waitpid(pid, os.WNOHANG)
            if waited == pid:
                exit_code = os.waitstatus_to_exitcode(status)
                return (exit_code, output.decode(errors="replace"), status)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass

    _, status = os.waitpid(pid, 0)
    exit_code = os.waitstatus_to_exitcode(status)
    return (exit_code, output.decode(errors="replace"), status)


def extract_policy_target(text: str) -> str | None:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if payload.get("code") == "ATO_ERR_POLICY_VIOLATION" and payload.get("target"):
            return str(payload["target"])
    return None


def normalize_host(target: str) -> str:
    value = target.strip()
    value = re.sub(r"^https?://", "", value)
    value = value.split("/", 1)[0]
    value = value.split(":", 1)[0]
    return value


def patch_skill_allow_host(skill_path: pathlib.Path, host: str):
    text = skill_path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise RuntimeError("SKILL.md frontmatter not found")

    rest = text[4:]
    end = rest.find("\n---\n")
    if end < 0:
        raise RuntimeError("SKILL.md frontmatter closing marker not found")

    fm = rest[:end]
    body = rest[end + 5 :]

    if f"- {host}" in fm:
        return

    fm_out = fm.rstrip() + "\npermissions:\n  network:\n    allow_hosts:\n      - " + host + "\n"
    patched = "---\n" + fm_out + "---\n" + body
    skill_path.write_text(patched, encoding="utf-8")


def run_demo() -> int:
    ato = ensure_ato_binary()

    workspace = pathlib.Path(tempfile.mkdtemp(prefix="ato-skill-heal-"))
    home = pathlib.Path(tempfile.mkdtemp(prefix="ato-skill-home-"))
    skill_path = workspace / "SKILL.md"
    shutil.copyfile(TEMPLATE_SKILL, skill_path)

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        skill_raw = skill_path.read_text(encoding="utf-8")
        skill_path.write_text(skill_raw.replace("http://127.0.0.1:18080/health", f"http://127.0.0.1:{port}/health"), encoding="utf-8")

        env = os.environ.copy()
        env["HOME"] = str(home)

        cmd = [str(ato), "run", "--from-skill", str(skill_path)]
        code1, out1, _ = run_with_auto_consent(cmd, env)
        print("=== Phase1: Deny ===")
        print(out1)
        if code1 == 0:
            print("expected failure in phase1, but command succeeded")
            return 1

        target = extract_policy_target(out1)
        if not target:
            print("failed to parse ATO_ERR_POLICY_VIOLATION target from output")
            return 1
        host = normalize_host(target)

        patch_skill_allow_host(skill_path, host)
        print("=== Phase2: Healed SKILL.md ===")
        print(skill_path.read_text(encoding="utf-8"))

        code2, out2, _ = run_with_auto_consent(cmd, env)
        print("=== Phase3: Retry ===")
        print(out2)

        server.shutdown()
        server.server_close()

        if code2 != 0:
            print(f"phase2 failed with exit={code2}")
            return 1
        if "heal-ok" not in out2:
            print("phase2 succeeded but expected payload 'heal-ok' not found")
            return 1

        print("Self-healing demo succeeded (deny -> parse -> patch -> success)")
        return 0


if __name__ == "__main__":
    sys.exit(run_demo())
