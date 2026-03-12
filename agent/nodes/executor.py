from __future__ import annotations

from pathlib import Path

from tools.shell_tools import run_shell


def _commands_for_repo(state: dict) -> list[str]:
    repo = Path(state["repo_path"])
    commands: list[str] = []
    ato_binary = state["config"].get("ato_binary")
    if ato_binary:
        commands.append(f'"{ato_binary}" validate "{repo / "capsule.toml"}"')

    lang = state.get("detected_lang")
    if lang == "rust":
        commands.append("cargo test")
    elif lang == "node":
        commands.append("npm test")
    elif lang == "python":
        commands.append("pytest")
    elif lang == "go":
        commands.append("go test ./...")
    return commands


def execute_node(state: dict) -> dict:
    repo = Path(state["repo_path"])
    manifest_path = repo / "capsule.toml"
    new_manifest = state.get("capsule_toml", "")
    current_manifest = manifest_path.read_text(encoding="utf-8") if manifest_path.exists() else None
    if current_manifest != new_manifest:
        manifest_path.write_text(new_manifest, encoding="utf-8")

    logs: list[str] = []
    results: dict[str, dict] = {}
    all_tests_passed = True

    for command in _commands_for_repo(state):
        outcome = run_shell(command, cwd=repo, timeout=300)
        logs.append(
            f"$ {command}\n{outcome['stdout']}{outcome['stderr']}".rstrip()
        )
        results[command] = outcome
        if outcome["returncode"] != 0:
            all_tests_passed = False
            break

    return {
        **state,
        "execution_log": logs,
        "test_results": results,
        "all_tests_passed": all_tests_passed,
    }


def route_after_execute(state: dict) -> str:
    return "success" if state.get("all_tests_passed") else "failure"
