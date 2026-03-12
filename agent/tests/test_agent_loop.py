from __future__ import annotations

import io
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent import main as agent_main  # type: ignore  # noqa: E402
from db.patterns import init_db, lookup_success_pattern, store_success_pattern  # type: ignore  # noqa: E402
from nodes.critic import critic_node  # type: ignore  # noqa: E402
from nodes.guard import guard_node  # type: ignore  # noqa: E402
from nodes.patcher import patch_node, route_after_patch  # type: ignore  # noqa: E402


RUST_MANIFEST = """# Capsule Manifest - multi-target Native v0.2
schema_version = "0.2"
name = "demo"
version = "0.1.0"
type = "app"
default_target = "cli"

[metadata]
description = "demo"

[requirements]

[targets.cli]
runtime = "source"
entrypoint = "./target/release/demo"

[storage]

[routing]
"""


class AgentLoopTests(unittest.TestCase):
    def test_critic_generates_rust_manifest_fix(self) -> None:
        with TemporaryDirectory() as repo_dir:
            repo = Path(repo_dir)
            (repo / "Cargo.toml").write_text(
                '[package]\nname = "demo"\nversion = "0.1.0"\nedition = "2021"\n',
                encoding="utf-8",
            )
            state = {
                "repo_path": str(repo),
                "capsule_toml": RUST_MANIFEST,
                "execution_log": ["$ cargo test\nerror: No such file or directory"],
                "correction_count": 0,
                "max_corrections": 3,
                "detected_lang": "rust",
                "repair_history": [],
            }

            updated = critic_node(state)

            self.assertEqual(updated["next_action"], "capsule_fix")
            self.assertIn('entrypoint = "cargo"', updated["pending_code_edit"]["content"])
            self.assertIn('cmd = ["run"]', updated["pending_code_edit"]["content"])

    def test_patch_node_applies_code_edit_when_present(self) -> None:
        with TemporaryDirectory() as repo_dir:
            repo = Path(repo_dir)
            state = {
                "repo_path": str(repo),
                "pending_code_edit": {
                    "type": "code",
                    "path": "src/app.py",
                    "content": "print('fixed')\n",
                    "reason": "apply concrete code patch",
                },
                "execution_log": [],
            }

            updated = patch_node(state)

            self.assertEqual(route_after_patch(updated), "execute")
            self.assertEqual((repo / "src/app.py").read_text(encoding="utf-8"), "print('fixed')\n")

    def test_critic_gives_up_after_repeating_same_manifest_fix(self) -> None:
        with TemporaryDirectory() as repo_dir:
            repo = Path(repo_dir)
            (repo / "Cargo.toml").write_text(
                '[package]\nname = "demo"\nversion = "0.1.0"\nedition = "2021"\n',
                encoding="utf-8",
            )
            state = {
                "repo_path": str(repo),
                "capsule_toml": RUST_MANIFEST,
                "execution_log": ["$ cargo test\nerror: No such file or directory"],
                "correction_count": 1,
                "max_corrections": 3,
                "detected_lang": "rust",
                "repair_history": ["rust:cargo-run-fallback"],
            }

            updated = critic_node(state)

            self.assertEqual(updated["next_action"], "give_up")

    def test_guard_fails_closed_without_tty(self) -> None:
        state = {
            "pending_code_edit": {
                "type": "code",
                "path": "src/app.py",
                "content": "print('fixed')\n",
                "reason": "apply concrete code patch",
            },
            "config": {"approval_policy": {"code": "confirm"}},
        }

        with mock.patch("nodes.guard.sys.stdin.isatty", return_value=False), mock.patch(
            "nodes.guard.sys.stdout.isatty", return_value=False
        ):
            updated = guard_node(state)

        self.assertFalse(updated["user_approved"])

    def test_success_pattern_lookup_uses_repo_identity(self) -> None:
        with TemporaryDirectory() as root_dir:
            root = Path(root_dir)
            repo_a = root / "workspace-a" / "demo"
            repo_b = root / "workspace-b" / "demo"
            repo_a.mkdir(parents=True)
            repo_b.mkdir(parents=True)
            db_path = root / "patterns.db"
            init_db(str(db_path))

            config = SimpleNamespace(
                repo_path=str(repo_a),
                target_env={"os": "linux", "arch": "x86_64"},
                provider="anthropic",
                model="",
            )
            state = {
                "all_tests_passed": True,
                "capsule_toml": RUST_MANIFEST,
                "correction_count": 1,
                "detected_lang": "rust",
                "test_framework": "cargo test",
            }

            store_success_pattern(str(db_path), config, state)

            reused = lookup_success_pattern(
                str(db_path),
                repo_a,
                {"os": "linux", "arch": "x86_64"},
                detected_lang="rust",
                test_framework="cargo test",
            )
            other_repo = lookup_success_pattern(
                str(db_path),
                repo_b,
                {"os": "linux", "arch": "x86_64"},
                detected_lang="rust",
                test_framework="cargo test",
            )

            self.assertEqual(reused, RUST_MANIFEST)
            self.assertIsNone(other_repo)

    def test_agent_main_cleans_generated_manifest_after_failure(self) -> None:
        with TemporaryDirectory() as repo_dir, TemporaryDirectory() as home_dir:
            repo = Path(repo_dir)
            manifest_path = repo / "capsule.toml"
            manifest_path.write_text(RUST_MANIFEST, encoding="utf-8")
            config_path = repo / "config.json"
            config_path.write_text(
                (
                    '{"repo_path": "%s", "ato_binary": "/bin/echo", "patterns_db": "%s/patterns.db", '
                    '"checkpoint_db": "%s/checkpoints.db"}'
                )
                % (repo, home_dir, home_dir),
                encoding="utf-8",
            )

            with mock.patch("agent.run_agent", return_value={"all_tests_passed": False, "manifest_preexisting": False}):
                with mock.patch.object(sys, "argv", ["agent.py", str(config_path)]), mock.patch(
                    "sys.stdout",
                    new=io.StringIO(),
                ):
                    exit_code = agent_main()

            self.assertEqual(exit_code, 1)
            self.assertFalse(manifest_path.exists())


if __name__ == "__main__":
    unittest.main()
