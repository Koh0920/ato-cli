from __future__ import annotations

from pathlib import Path


def _detect_language(repo: Path) -> str:
    if (repo / "Cargo.toml").exists():
        return "rust"
    if (repo / "package.json").exists():
        return "node"
    if (repo / "pyproject.toml").exists() or (repo / "requirements.txt").exists():
        return "python"
    if (repo / "go.mod").exists():
        return "go"
    return "unknown"


def _detect_test_files(repo: Path, lang: str) -> tuple[str, list[str]]:
    if lang == "rust":
        rust_files = sorted(str(path.relative_to(repo)) for path in repo.glob("tests/**/*.rs"))
        if (repo / "src").exists():
            rust_files.extend(
                sorted(str(path.relative_to(repo)) for path in repo.glob("src/**/*test*.rs"))
            )
        return "cargo test", rust_files
    if lang == "node":
        patterns = ["tests/**/*.test.*", "src/**/*.test.*", "src/**/*.spec.*"]
        node_files: list[str] = []
        for pattern in patterns:
            node_files.extend(sorted(str(path.relative_to(repo)) for path in repo.glob(pattern)))
        return "npm test", node_files
    if lang == "python":
        python_files = sorted(str(path.relative_to(repo)) for path in repo.glob("tests/test_*.py"))
        return "pytest", python_files
    if lang == "go":
        go_files = sorted(str(path.relative_to(repo)) for path in repo.glob("**/*_test.go"))
        return "go test", go_files
    return "unknown", []


def analyze_node(state: dict) -> dict:
    repo = Path(state["repo_path"])
    detected_lang = _detect_language(repo)
    test_framework, test_files = _detect_test_files(repo, detected_lang)
    return {
        **state,
        "detected_lang": detected_lang,
        "test_framework": test_framework,
        "test_files": test_files,
    }
