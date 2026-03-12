from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def _append_execution_log(state: dict, message: str) -> list[str]:
    return [*state.get("execution_log", []), message]


def _normalize_manifest(content: str) -> str:
    return "\n".join(line.rstrip() for line in content.strip().splitlines())


def _resolve_repo_path(repo: Path, candidate: str) -> Path:
    path = Path(candidate)
    if not path.is_absolute():
        path = repo / path
    resolved_repo = repo.resolve()
    resolved_path = path.resolve()
    if resolved_path != resolved_repo and not resolved_path.is_relative_to(resolved_repo):
        raise ValueError(f"Refusing to modify path outside repository: {candidate}")
    return resolved_path


def patch_node(state: dict) -> dict:
    edit = state.get("pending_code_edit") or {}
    if edit.get("type") == "capsule_toml":
        repo = Path(state["repo_path"])
        new_manifest = str(edit.get("content") or state.get("capsule_toml", ""))
        current_manifest = _normalize_manifest(str(state.get("capsule_toml", "")))
        if _normalize_manifest(new_manifest) == current_manifest:
            return {
                **state,
                "patch_outcome": "give_up",
                "pending_code_edit": None,
                "execution_log": _append_execution_log(
                    state,
                    f"Manifest repair skipped: {edit.get('reason', 'no new mutation was produced.')}",
                ),
            }
        (repo / "capsule.toml").write_text(new_manifest, encoding="utf-8")
        return {
            **state,
            "capsule_toml": new_manifest,
            "patch_outcome": "execute",
            "repair_history": [
                *state.get("repair_history", []),
                edit.get("fingerprint") or sha256(new_manifest.encode("utf-8")).hexdigest(),
            ],
            "pending_code_edit": None,
            "execution_log": _append_execution_log(
                state,
                f"Applied manifest repair: {edit.get('reason', 'updated capsule.toml')}",
            ),
        }

    if edit.get("type") == "code" and edit.get("path") and edit.get("content"):
        repo = Path(state["repo_path"])
        try:
            target = _resolve_repo_path(repo, str(edit["path"]))
        except ValueError as error:
            return {
                **state,
                "patch_outcome": "give_up",
                "pending_code_edit": None,
                "execution_log": _append_execution_log(state, str(error)),
            }
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(edit["content"]), encoding="utf-8")
        return {
            **state,
            "patch_outcome": "execute",
            "pending_code_edit": None,
            "execution_log": _append_execution_log(
                state,
                f"Applied code repair to {target.relative_to(repo)}: {edit.get('reason', 'updated source file')}",
            ),
        }

    return {
        **state,
        "patch_outcome": "give_up",
        "execution_log": _append_execution_log(
            state,
            "Code-edit approval reached, but no concrete source-code edit was available to apply.",
        ),
    }


def route_after_patch(state: dict) -> str:
    return state.get("patch_outcome", "execute")
