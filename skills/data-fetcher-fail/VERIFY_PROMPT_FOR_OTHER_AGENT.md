# 他エージェント検証用プロンプト（失敗→自己修復版）

以下を、そのまま別のLLMエージェントに渡してください。

---

あなたはこのリポジトリで作業するエージェントです。

目的:

- apps/ato-cli/skills/data-fetcher-fail/SKILL.md で、fail-closed を再現する。
- その後、frontmatter を自己修復して成功まで到達する。
- 生のホストコマンド実行は禁止。必ず Ato を使うこと。

必須ルール:

1. 実行前に絶対パスを固定する:
   - SKILL_DIR="/Users/egamikohsuke/Ekoh/projects/ato/capsuled-dev/apps/ato-cli/skills/data-fetcher-fail"
2. 次のコマンドのみ使用する:
   - cd "$SKILL_DIR" && (command -v ato >/dev/null 2>&1 && ato run --from-skill ./SKILL.md --yes || ~/.cargo/bin/ato run --from-skill ./SKILL.md --yes)
3. python / node / curl などの生実行は禁止。
4. 最終回答には進捗ログ（Created todos, Ran terminal command...）を書かない。
5. 実行は必ず 2 フェーズで行うこと:
   - Phase 1: 失敗を再現（permission 変更なし）
   - Phase 2: `SKILL.md` frontmatter を修復して再実行

期待結果（Phase 1）:

- 実行は失敗すること（成功させない）。
- stderr に JSONL で code=ATO_ERR_POLICY_VIOLATION を含むこと。
- target に api.github.com が含まれること。

自己修復手順（Phase 2）:

1. `SKILL.md` の frontmatter に以下を追加（存在しなければ新規作成）:

```yaml
permissions:
  egress_allow: ["api.github.com"]
```

2. 同じ実行コマンドを再実行する。
3. 今度は成功し、`repo:` 行が出ることを確認する。

最終報告フォーマット:

- 実行コマンド
- Phase 1 の結果（失敗/成功）
- Phase 1 JSONエラー抜粋（code/message/target）
- 修復内容（SKILL.md に追記した frontmatter）
- Phase 2 の結果（失敗/成功）
- 成功時の stdout 抜粋（`repo:` を含む）
- fail-closed が機能し、自己修復で回復した根拠

---
