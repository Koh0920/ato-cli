from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess


def _git_value(repo: Path, *args: str) -> str:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""
    return completed.stdout.strip()


def build_repo_identity(repo: Path, detected_lang: str = "", test_framework: str = "") -> dict[str, str]:
    canonical_repo = repo.resolve() if repo.exists() else repo
    return {
        "repo_path": str(canonical_repo),
        "repo_name": canonical_repo.name,
        "git_remote": _git_value(canonical_repo, "config", "--get", "remote.origin.url"),
        "git_head": _git_value(canonical_repo, "rev-parse", "HEAD"),
        "detected_lang": detected_lang,
        "test_framework": test_framework,
    }


def build_env_key(repo: Path, target_env: dict, detected_lang: str = "", test_framework: str = "") -> str:
    identity = build_repo_identity(repo, detected_lang=detected_lang, test_framework=test_framework)
    payload = {
        "repo": identity,
        "target_env": target_env,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def init_db(db_path: str) -> None:
    if not db_path:
        return
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    schema_path = Path(__file__).with_name("schema.sql")
    with sqlite3.connect(path) as conn:
        conn.executescript(schema_path.read_text(encoding="utf-8"))


def build_env_hash(repo: Path, target_env: dict, capsule_toml: str) -> str:
    digest = hashlib.sha256()
    digest.update(str(repo.resolve() if repo.exists() else repo).encode("utf-8"))
    digest.update(json.dumps(target_env, sort_keys=True).encode("utf-8"))
    digest.update(capsule_toml.encode("utf-8"))
    return digest.hexdigest()


def lookup_success_pattern(
    db_path: str,
    repo: Path,
    target_env: dict,
    detected_lang: str = "",
    test_framework: str = "",
) -> str | None:
    if not db_path or not Path(db_path).exists():
        return None
    env_key = build_env_key(
        repo,
        target_env,
        detected_lang=detected_lang,
        test_framework=test_framework,
    )
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT capsule_toml
            FROM success_patterns
            WHERE env_key = ?
            ORDER BY success_count DESC, updated_at DESC
            LIMIT 1
            """,
            (env_key,),
        ).fetchone()
    return row[0] if row else None


def store_success_pattern(db_path: str, config, state: dict) -> None:
    if not db_path or not state.get("all_tests_passed"):
        return
    capsule_toml = state.get("capsule_toml", "")
    repo = Path(config.repo_path)
    env_hash = build_env_hash(repo, config.target_env or {}, capsule_toml)
    env_key = build_env_key(
        repo,
        config.target_env or {},
        detected_lang=state.get("detected_lang", ""),
        test_framework=state.get("test_framework", ""),
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO success_patterns (
                env_hash,
                env_key,
                repo_path,
                capsule_toml,
                provider_used,
                model_used,
                success_count,
                correction_iter,
                test_framework,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(env_hash) DO UPDATE SET
                capsule_toml = excluded.capsule_toml,
                provider_used = excluded.provider_used,
                model_used = excluded.model_used,
                success_count = success_patterns.success_count + 1,
                correction_iter = excluded.correction_iter,
                test_framework = excluded.test_framework,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                env_hash,
                env_key,
                str(repo),
                capsule_toml,
                config.provider,
                config.model or "",
                int(state.get("correction_count", 0)),
                state.get("test_framework", ""),
            ),
        )
