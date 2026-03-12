from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any


@dataclass
class AtoConfig:
    repo_path: str
    ato_binary: str
    provider: str = "anthropic"
    model: str | None = None
    api_key: str = ""
    temperature: float = 0.0
    max_tokens: int = 8192
    max_corrections: int = 10
    approval_policy: dict[str, str] | None = None
    target_env: dict[str, Any] | None = None
    checkpoint_db: str = ""
    patterns_db: str = ""


MODEL_MAP = {
    "analyze": {
        "anthropic": "claude-haiku-4-5-20251001",
        "openai": "gpt-4o-mini",
    },
    "generate": {
        "anthropic": "claude-sonnet-4-6",
        "openai": "gpt-4o",
    },
    "code_fix": {
        "anthropic": "claude-opus-4-6",
        "openai": "gpt-4o",
    },
}


def load_config(path: str | os.PathLike[str]) -> AtoConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return AtoConfig(
        repo_path=raw["repo_path"],
        ato_binary=raw["ato_binary"],
        provider=raw.get("provider", "anthropic"),
        model=raw.get("model"),
        api_key=raw.get("api_key") or "",
        max_corrections=raw.get("max_corrections", 10),
        approval_policy=raw.get("approval_policy") or {"capsule": "auto", "code": "confirm"},
        target_env=raw.get("target_env") or {},
        checkpoint_db=raw.get("checkpoint_db", ""),
        patterns_db=raw.get("patterns_db", ""),
    )


def get_model(task: str, config: AtoConfig) -> str:
    if config.model:
        return config.model
    base = MODEL_MAP[task][config.provider]
    if config.provider == "anthropic":
        return f"anthropic/{base}"
    return base


def llm_call(task: str, messages: list[dict[str, Any]], tools: list[dict[str, Any]], config: AtoConfig):
    import litellm

    kwargs: dict[str, Any] = {
        "model": get_model(task, config),
        "messages": messages,
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
    }
    if tools:
        kwargs["tools"] = tools
    if config.api_key:
        kwargs["api_key"] = config.api_key
    return litellm.completion(**kwargs)
