from __future__ import annotations

from typing import Annotated, Any, TypedDict

try:
    from langgraph.graph import END, StateGraph
    from langgraph.graph.message import add_messages
except Exception:  # pragma: no cover - fallback path is for environments without langgraph
    END = "__end__"
    StateGraph = None

    def add_messages(existing: list[Any], new: list[Any]) -> list[Any]:
        return [*existing, *new]

from config import AtoConfig
from nodes.analyzer import analyze_node
from nodes.critic import critic_node, route_after_critic
from nodes.executor import execute_node, route_after_execute
from nodes.generator import generate_node
from nodes.guard import guard_node, route_after_guard
from nodes.patcher import patch_node


class AgentState(TypedDict, total=False):
    messages: Annotated[list[dict[str, Any]], add_messages]
    repo_path: str
    target_env: dict[str, Any]
    capsule_toml: str
    execution_log: list[str]
    test_results: dict[str, Any]
    all_tests_passed: bool
    correction_count: int
    max_corrections: int
    pending_code_edit: dict[str, Any] | None
    user_approved: bool | None
    detected_lang: str
    test_framework: str
    test_files: list[str]
    config: dict[str, Any]
    manifest_preexisting: bool
    next_action: str


def create_initial_state(config: AtoConfig) -> AgentState:
    return {
        "messages": [],
        "repo_path": config.repo_path,
        "target_env": config.target_env or {},
        "capsule_toml": "",
        "execution_log": [],
        "test_results": {},
        "all_tests_passed": False,
        "correction_count": 0,
        "max_corrections": config.max_corrections,
        "pending_code_edit": None,
        "user_approved": None,
        "detected_lang": "",
        "test_framework": "",
        "test_files": [],
        "config": {
            "provider": config.provider,
            "model": config.model,
            "approval_policy": config.approval_policy or {"capsule": "auto", "code": "confirm"},
            "ato_binary": config.ato_binary,
            "patterns_db": config.patterns_db,
            "checkpoint_db": config.checkpoint_db,
            "api_key": config.api_key,
        },
        "manifest_preexisting": False,
    }


def build_app():
    if StateGraph is None:
        return None

    workflow = StateGraph(AgentState)
    workflow.add_node("analyze", analyze_node)
    workflow.add_node("generate", generate_node)
    workflow.add_node("execute", execute_node)
    workflow.add_node("critic", critic_node)
    workflow.add_node("guard", guard_node)
    workflow.add_node("patch", patch_node)
    workflow.set_entry_point("analyze")
    workflow.add_edge("analyze", "generate")
    workflow.add_edge("generate", "execute")
    workflow.add_conditional_edges(
        "execute",
        route_after_execute,
        {
            "success": END,
            "failure": "critic",
        },
    )
    workflow.add_conditional_edges(
        "critic",
        route_after_critic,
        {
            "capsule_fix": "patch",
            "code_fix": "guard",
            "give_up": END,
        },
    )
    workflow.add_conditional_edges(
        "guard",
        route_after_guard,
        {
            "approved": "patch",
            "rejected": "critic",
            "ignored": END,
        },
    )
    workflow.add_edge("patch", "execute")
    return workflow.compile()


def run_agent(config: AtoConfig) -> AgentState:
    state = create_initial_state(config)
    app = build_app()
    if app is not None:
        return app.invoke(state)
    return run_linear_loop(state)


def run_linear_loop(state: AgentState) -> AgentState:
    state = analyze_node(state)
    state = generate_node(state)
    while True:
        state = execute_node(state)
        if route_after_execute(state) == "success":
            return state
        state = critic_node(state)
        action = route_after_critic(state)
        if action == "give_up":
            return state
        if action == "code_fix":
            state = guard_node(state)
            guard_result = route_after_guard(state)
            if guard_result == "ignored":
                return state
            if guard_result == "rejected":
                continue
        state = patch_node(state)
