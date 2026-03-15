from __future__ import annotations

import sys


def guard_node(state: dict) -> dict:
    edit = state.get("pending_code_edit") or {}
    config = dict(state.get("config") or {})
    policy = dict(config.get("approval_policy") or {})

    if edit.get("type") == "capsule_toml":
        return {**state, "user_approved": True}

    code_policy = policy.get("code", "confirm")
    if code_policy == "ignore":
        return {**state, "user_approved": None}
    if code_policy == "auto":
        return {**state, "user_approved": True}
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return {
            **state,
            "user_approved": False,
            "execution_log": [
                *state.get("execution_log", []),
                "Interactive code approval is unavailable in non-TTY environments; rejecting source-code change.",
            ],
        }

    path = edit.get("path") or "<repo>"
    reason = edit.get("reason") or "no reason provided"
    response = input(
        f"🤖 Agent wants to edit: {path}\nReason: {reason}\nApply code fix? [y/N/ignore-always] "
    ).strip().lower()
    if response == "ignore-always":
        policy["code"] = "ignore"
        config["approval_policy"] = policy
        return {**state, "config": config, "user_approved": None}
    return {**state, "user_approved": response in {"y", "yes"}}


def route_after_guard(state: dict) -> str:
    approved = state.get("user_approved")
    if approved is None:
        return "ignored"
    if approved:
        return "approved"
    return "rejected"
