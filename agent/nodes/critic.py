from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tomllib

RUST_RELEASE_ENTRYPOINT_PREFIX = "./target/release/"


def _update_manifest_line(content: str, key: str, value: str) -> str:
    lines = content.splitlines()
    replacement = f'{key} = {value}'
    for index, line in enumerate(lines):
        if line.strip().startswith(f"{key} = "):
            lines[index] = replacement
            return "\n".join(lines) + ("\n" if content.endswith("\n") else "")
    target_index = 0
    for index, line in enumerate(lines):
        if line.strip() == "[storage]":
            target_index = index
            break
    lines.insert(target_index, replacement)
    return "\n".join(lines) + ("\n" if content.endswith("\n") else "")


def _apply_manifest_updates(content: str, updates: list[tuple[str, str]]) -> str:
    updated = content
    for key, value in updates:
        updated = _update_manifest_line(updated, key, value)
    return updated


def _load_package_json(repo: Path) -> dict:
    package_json = repo / "package.json"
    if not package_json.exists():
        return {}
    try:
        return json.loads(package_json.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _load_cli_target(manifest: str) -> dict:
    try:
        parsed = tomllib.loads(manifest)
    except tomllib.TOMLDecodeError:
        return {}
    targets = parsed.get("targets") or {}
    cli_target = targets.get("cli") or {}
    return cli_target if isinstance(cli_target, dict) else {}


def _build_manifest_fix(state: dict) -> dict | None:
    repo = Path(state["repo_path"])
    manifest = state.get("capsule_toml", "")
    detected_lang = state.get("detected_lang")
    joined_log = "\n".join(state.get("execution_log", []))
    lowered_log = joined_log.lower()
    cli_target = _load_cli_target(manifest)
    entrypoint = cli_target.get("entrypoint")
    cmd = cli_target.get("cmd") or []

    if detected_lang == "rust" and isinstance(entrypoint, str) and entrypoint.startswith(
        RUST_RELEASE_ENTRYPOINT_PREFIX
    ):
        updated = _apply_manifest_updates(
            manifest,
            [("entrypoint", '"cargo"'), ("cmd", '["run"]')],
        )
        return {
            "type": "capsule_toml",
            "content": updated,
            "reason": "Rust repository runs should fall back to `cargo run` when no release binary exists.",
            "fingerprint": "rust:cargo-run-fallback",
        }

    if detected_lang == "node":
        package = _load_package_json(repo)
        scripts = package.get("scripts") or {}
        if entrypoint == "node" and cmd == ["index.js"] and "start" in scripts:
            updated = _apply_manifest_updates(
                manifest,
                [("entrypoint", '"npm"'), ("cmd", '["start"]')],
            )
            return {
                "type": "capsule_toml",
                "content": updated,
                "reason": "Switch Node repositories to the package.json start script after index.js execution failed.",
                "fingerprint": "node:npm-start",
            }
        if entrypoint == "node" and cmd == ["index.js"] and "dev" in scripts:
            updated = _apply_manifest_updates(
                manifest,
                [("entrypoint", '"npm"'), ("cmd", '["run", "dev"]')],
            )
            return {
                "type": "capsule_toml",
                "content": updated,
                "reason": "Switch Node repositories to the package.json dev script after index.js execution failed.",
                "fingerprint": "node:npm-dev",
            }

    if detected_lang == "python" and entrypoint == "main.py" and not (repo / "main.py").exists():
        for candidate in ("app.py", "manage.py"):
            if (repo / candidate).exists():
                updated = _update_manifest_line(manifest, "entrypoint", f'"{candidate}"')
                return {
                    "type": "capsule_toml",
                    "content": updated,
                    "reason": f"Switch Python entrypoint to `{candidate}` because `main.py` is absent.",
                    "fingerprint": f"python:{candidate}",
                }

    if "validation" in lowered_log or "capsule.toml" in lowered_log or "manifest" in lowered_log:
        compact_log = " ".join(joined_log.split())[:400]
        return {
            "type": "capsule_toml",
            "content": manifest,
            "reason": f"Validation failed but no safe manifest mutation heuristic matched: {compact_log}",
            "fingerprint": hashlib.sha256(compact_log.encode("utf-8")).hexdigest(),
        }

    return None


def critic_node(state: dict) -> dict:
    correction_count = int(state.get("correction_count", 0)) + 1
    max_corrections = int(state.get("max_corrections", 10))
    if correction_count >= max_corrections:
        return {
            **state,
            "correction_count": correction_count,
            "next_action": "give_up",
        }

    repair_history = list(state.get("repair_history", []))
    pending = _build_manifest_fix(state)
    if pending and pending.get("content") != state.get("capsule_toml") and pending.get("fingerprint") not in repair_history:
        next_action = "capsule_fix"
    else:
        next_action = "give_up"
        pending = {
            "type": "code",
            "path": "",
            "content": "",
            "reason": "No new safe automatic repair was derived from the latest failure logs.",
        }

    return {
        **state,
        "correction_count": correction_count,
        "pending_code_edit": pending,
        "next_action": next_action,
    }


def route_after_critic(state: dict) -> str:
    return state.get("next_action", "give_up")
