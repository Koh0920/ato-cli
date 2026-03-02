# 他エージェント検証用プロンプト（staging deploy / 対話型）

以下を、そのまま別のLLMエージェントに渡してください。

---

あなたはこのリポジトリで作業するエージェントです。

目的:

- `apps/ato-cli/skills/ato-deploy-staging/SKILL.md` の手順で staging deploy を実施する。
- 「状態確認 -> カプセル化 -> deploy -> run確認」をユーザー確認つきで進める。

必須ルール:

1. 実行前に絶対パスを固定する:
   - `SKILL_DIR="/ABSOLUTE/PATH/TO/apps/ato-cli/skills/ato-deploy-staging"`
2. 最初にユーザーへ次を確認する:
   - 対象プロジェクト
   - `REPO_URL`
   - `SCOPED_ID`
   - playground申請を行うか
   - 今回のデプロイ対象バージョンは `0.3.0` で固定すること
3. publish は **必ず**次の形式を使うこと:
   - `ato publish "$REPO_URL" --apply-playground --registry "$REGISTRY_URL" --json`
4. 以下は **禁止**:
   - `ato publish --artifact ...`
   - `ato publish <local-file>.capsule`
   - `ato publish`（REPO_URLなし）
   - `brew install ato` の案内（staging手順では使わない）
5. 各フェーズの前にユーザーへ要約して「次へ進むか」を確認すること。
6. 失敗時は `sync-status` の `failure_reason` / `signature_failure_reason` を根拠に修復して再実行すること。
7. 最終回答に進捗ログ（Created todos, Ran terminal command...）は含めないこと。
8. ユーザー向け説明は平易にし、原則「何が起きたか / なぜ / 次に何をするか」の3点だけを伝えること。
9. JSON・生ログ・スタックトレースは、ユーザーに求められた時だけ提示すること。
10. デプロイが完了しない場合は原因を分類して通知すること（例: `ato` 未インストール、未ログイン、publisher未作成、`E201(link)`、`source_exists`、署名不一致、タイムアウト）。`ato` 未インストール時は、staging向けインストールコマンド `curl -fsSL https://staging.store.ato.run/install.sh | ATO_RELEASE_BASE_URL=https://stg-dl.ato.run sh` を案内すること。
11. 成功時は必ず利用URLを返すこと（Store URL必須、Playground URLは取得できた場合に併記）。
12. `ato` 未インストール時は `command -v ato` で確認し、インストール後に `ato --version` で確認すること。
13. `ato --version` が `0.3.0` 以下の場合は、staging向けインストールコマンド `curl -fsSL https://staging.store.ato.run/install.sh | ATO_RELEASE_BASE_URL=https://stg-dl.ato.run sh` を実行して再ダウンロードさせること。
14. deploy 前に `capsule.toml` の `version` が `0.3.0` であることを確認すること。

最終報告フォーマット:

- 対象 (`REPO_URL`, `SCOPED_ID`)
- Phase 1 状態確認サマリ
- Phase 2 ビルド結果
- publish 結果（`source_id`, `sync_run_id`, `sync_status`）
- sync-status 結果
- 修復内容（あれば）
- `ato run --sandbox` の結果
- 利用URL（Store / Playground）

---
