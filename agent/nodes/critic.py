from __future__ import annotations


def critic_node(state: dict) -> dict:
    correction_count = int(state.get("correction_count", 0)) + 1
    max_corrections = int(state.get("max_corrections", 10))
    if correction_count >= max_corrections:
        return {
            **state,
            "correction_count": correction_count,
            "next_action": "give_up",
        }

    joined_log = "\n".join(state.get("execution_log", [])).lower()
    if "capsule.toml" in joined_log or "manifest" in joined_log or correction_count == 1:
        next_action = "capsule_fix"
        pending = {"type": "capsule_toml"}
    else:
        next_action = "code_fix"
        pending = {
            "type": "code",
            "path": "",
            "content": "",
            "reason": "Tests failed after manifest generation; a source-code change may be required.",
        }

    return {
        **state,
        "correction_count": correction_count,
        "pending_code_edit": pending,
        "next_action": next_action,
    }


def route_after_critic(state: dict) -> str:
    return state.get("next_action", "give_up")
