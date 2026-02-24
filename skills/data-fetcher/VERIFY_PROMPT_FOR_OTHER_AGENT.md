# 他エージェント検証用プロンプト

以下を、そのまま別のLLMエージェント（Claude Code / Cursor Agent / OpenDevin など）に渡してください。

---

あなたはこのリポジトリで作業するエージェントです。

目的:

- `apps/ato-cli/skills/data-fetcher/SKILL.md` を使って、Ato経由で安全実行できることを確認する。
- 生のホストコマンド実行は禁止。必ず Ato を使うこと。

必須ルール:

1. `SKILL.md` の `CRITICAL EXECUTION RULES` に従うこと。
2. 実行前に絶対パスを変数化すること（相対パス禁止）:
   - `SKILL_DIR="/Users/egamikohsuke/Ekoh/projects/ato/capsuled-dev/apps/ato-cli/skills/data-fetcher"`
   - `cd "$SKILL_DIR"`
   - `pwd` が `$SKILL_DIR` と一致することを確認
3. 実行は必ず次のコマンドを使うこと:
   - `cd "$SKILL_DIR" && (command -v ato >/dev/null 2>&1 && ato run --from-skill ./SKILL.md --yes || ~/.cargo/bin/ato run --from-skill ./SKILL.md --yes)`
4. 失敗時に stderr に `"code":"ATO_ERR_POLICY_VIOLATION"` が含まれる場合:
   - まず JSON の `message` を確認
   - `ExecutionPlan consent` 系エラーなら、`--yes` 付きで再実行（frontmatter修正は不要）
   - network policy violation なら JSON の `target` を抽出し、`SKILL.md` frontmatter の `permissions.egress_allow` に追記して再実行
5. `python`, `node`, `curl` などの生実行は禁止。
6. 最終回答には、進捗ログ（例: Created todos, Starting..., Ran terminal command...）を含めないこと。
7. 1回目が成功しても、2回目を必ず同一コマンドで再実行して再現性を示すこと（2回目の直前にも `cd "$SKILL_DIR" && pwd` を実施）。
8. 実行コマンド表記は必ず `ato run --from-skill ./SKILL.md --yes` を含めること。

最終報告フォーマット:

- 実行コマンド
- 1回目の結果（成功/失敗、失敗ならJSONエラー抜粋）
- 修復内容（追記した permission）
- 2回目の結果
- この skill が「Ato経由のみ」で動作した証拠

備考:

- 1回目成功時の「修復内容」は `なし（既存permissionで充足）` と明記すること。
- 2回目結果は `再実行不要` と書かず、実際に再実行した結果を記載すること。
- `cd: no such file or directory` は検証失敗（手順違反）として扱い、報告前に絶対パスでやり直してから最終結果を記載すること。

---
