#!/usr/bin/env python3
import subprocess
import sys
import json
from pathlib import Path
import difflib
import shutil
import re

SKILL_DIR = Path(__file__).resolve().parents[1]
SKILL_PATH = SKILL_DIR / "SKILL.md"

ATO_CMD = 'SKILL_DIR="' + str(SKILL_DIR) + '" && cd "$SKILL_DIR" && (command -v ato >/dev/null 2>&1 && ato run --from-skill ./SKILL.md --yes || ~/.cargo/bin/ato run --from-skill ./SKILL.md --yes)'

CSI = "\x1b["
BLUE = CSI + "34m"
GREEN = CSI + "32m"
RED = CSI + "31m"
YELLOW = CSI + "33m"
RESET = CSI + "0m"


def run_cmd(cmd):
    p = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    return p.returncode, p.stdout, p.stderr


def parse_jsonl_from_stderr(stderr):
    for line in stderr.splitlines():
        line = line.strip()
        if line.startswith('{') and 'code' in line:
            try:
                return json.loads(line)
            except Exception:
                continue
    return None


def propose_permissions(old_text, target_host):
    # insert permissions: egress_allow: ["<target_host>"] before the second '---' in the frontmatter
    parts = old_text.split('---')
    if len(parts) < 3:
        # no obvious frontmatter; append
        new = '---\npermissions:\n  egress_allow: ["%s"]\n---\n' % target_host + old_text
        return new
    # parts[0] is before first ---, parts[1] is frontmatter body, parts[2+] rest
    fm = parts[1]
    if 'egress_allow' in fm and target_host in fm:
        return old_text
    fm = fm.rstrip() + '\npermissions:\n  egress_allow: ["%s"]\n' % target_host
    new = '---'.join([parts[0], fm] + parts[2:])
    return new


def show_diff(a, b):
    a_lines = a.splitlines(keepends=True)
    b_lines = b.splitlines(keepends=True)
    diff = difflib.unified_diff(a_lines, b_lines, fromfile='SKILL.md (current)', tofile='SKILL.md (proposed)')
    for line in diff:
        if line.startswith('+') and not line.startswith('+++'):
            sys.stdout.write(GREEN + line + RESET)
        elif line.startswith('-') and not line.startswith('---'):
            sys.stdout.write(RED + line + RESET)
        elif line.startswith('@'):
            sys.stdout.write(YELLOW + line + RESET)
        else:
            sys.stdout.write(line)


def backup(path: Path):
    bak = path.with_suffix(path.suffix + '.bak')
    shutil.copy2(path, bak)
    return bak


def strip_permissions_from_frontmatter(text: str):
    lines = text.splitlines(keepends=True)
    if len(lines) < 3 or lines[0].strip() != '---':
        return text, False

    end_idx = None
    for i in range(1, len(lines)):
        if lines[i].strip() == '---':
            end_idx = i
            break
    if end_idx is None:
        return text, False

    fm = lines[1:end_idx]
    body = lines[end_idx:]

    new_fm = []
    removed = False
    i = 0
    while i < len(fm):
        line = fm[i]
        if re.match(r'^\s*permissions\s*:\s*$', line):
            removed = True
            i += 1
            while i < len(fm):
                nxt = fm[i]
                if re.match(r'^\s{2,}\S', nxt) or nxt.strip() == '':
                    i += 1
                    continue
                break
            continue

        if re.match(r'^\s*egress_allow\s*:', line):
            removed = True
            i += 1
            continue

        new_fm.append(line)
        i += 1

    new_text = ''.join([lines[0]] + new_fm + body)
    return new_text, removed


def main():
    current = SKILL_PATH.read_text()
    normalized, removed = strip_permissions_from_frontmatter(current)
    if removed:
        print(YELLOW + 'Detected existing permissions in SKILL.md; Phase 1 would not fail as intended.' + RESET)
        show_diff(current, normalized)
        ans = input(BLUE + '\nReset to fail-state before Phase 1? [Y/n]: ' + RESET).strip().lower()
        if ans in ('', 'y', 'yes'):
            bak = backup(SKILL_PATH)
            SKILL_PATH.write_text(normalized)
            print(GREEN + f'Reset complete. Backup: {bak.name}' + RESET)
        else:
            print(YELLOW + 'Skipped reset. Demo may not reproduce Phase 1 failure.' + RESET)

    print(BLUE + 'Phase 1: running skill (expected: policy violation)' + RESET)
    rc, out, err = run_cmd(ATO_CMD)
    if out:
        print(out)
    if rc == 0:
        print(GREEN + 'Phase 1 succeeded unexpectedly.' + RESET)
    else:
        print(RED + 'Phase 1 failed (rc=%d)' % rc + RESET)
        j = parse_jsonl_from_stderr(err)
        if j:
            print(YELLOW + 'Detected JSON error:' + RESET)
            print(json.dumps(j, indent=2, ensure_ascii=False))
            if j.get('code') == 'ATO_ERR_POLICY_VIOLATION':
                target = j.get('target') or 'api.github.com'
                print(BLUE + '\n--- Proposed Change (LLM suggestion) ---' + RESET)
                old = SKILL_PATH.read_text()
                new = propose_permissions(old, target)
                show_diff(old, new)
                ans = input(BLUE + '\nApply change and re-run with consent? [y/N]: ' + RESET).strip().lower()
                if ans == 'y':
                    bak = backup(SKILL_PATH)
                    SKILL_PATH.write_text(new)
                    print(GREEN + f'Backed up original to {bak.name} and wrote proposed SKILL.md' + RESET)
                    print(BLUE + '\nPhase 2: re-running skill with updated frontmatter' + RESET)
                    rc2, out2, err2 = run_cmd(ATO_CMD)
                    if out2:
                        print(out2)
                    if rc2 == 0:
                        print(GREEN + 'Phase 2 succeeded.' + RESET)
                    else:
                        print(RED + f'Phase 2 failed (rc={rc2})' + RESET)
                        if err2:
                            print(err2)
                else:
                    print(YELLOW + 'User declined to apply change. Exiting.' + RESET)
        else:
            print('No structured JSON error found in stderr:')
            print(err)


if __name__ == '__main__':
    main()
