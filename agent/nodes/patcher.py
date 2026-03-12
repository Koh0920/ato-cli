from __future__ import annotations

from pathlib import Path

from nodes.generator import generate_node


def patch_node(state: dict) -> dict:
    edit = state.get("pending_code_edit") or {}
    if edit.get("type") == "capsule_toml":
        refreshed = generate_node(state)
        repo = Path(state["repo_path"])
        (repo / "capsule.toml").write_text(refreshed["capsule_toml"], encoding="utf-8")
        return {
            **refreshed,
            "pending_code_edit": None,
        }

    return {
        **state,
        "execution_log": [
            *state.get("execution_log", []),
            "Code-edit approval reached, but this MVP only auto-applies capsule.toml updates.",
        ],
        "next_action": "give_up",
    }
