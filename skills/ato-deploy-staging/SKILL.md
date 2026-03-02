---
name: ato-deploy-staging
description: GitHubリポジトリをAto staging環境へデプロイし、同期・実行確認まで行う運用スキル
permissions:
  egress_allow: ["<STAGING_API_DOMAIN>", "api.github.com", "github.com"]
---

# Ato Deploy Staging Skill

## ⚠️ CRITICAL EXECUTION RULES

- staging デプロイでは **GitHub Source 経路のみ**を使うこと。
- **禁止**:
  - `ato publish --artifact ...`
  - `ato publish <local-file>.capsule`
  - `ato publish`（`REPO_URL` なし）
  - `brew install ato` など staging 手順と無関係なインストール案内
- 必須:
  - `ato publish "$REPO_URL" --apply-playground --registry "$REGISTRY_URL" --json`

## Goal

- deploy 前に現プロジェクト状態を確認し、ユーザー意図を段階的に確認しながら進める。
- 対象 GitHub リポジトリを staging レジストリへ publish する。
- `sync_status=completed` を確認する。
- staging 上で `ato run --sandbox` による起動確認まで行う。
- 失敗時は原因を分類し、ユーザーへ分かりやすい日本語で通知する。
- 成功時は利用可能URLを返す。

## User Communication Policy

- ユーザーには、原則として次の3点だけ伝える:
  1. 何が起きたか
  2. なぜ起きたか（推定ではなく根拠付き）
  3. 次に何をすればよいか
- JSON・スタックトレース・生ログは、**ユーザーに求められた場合のみ**提示する。
- エラー通知は専門用語を避け、1メッセージ1原因で簡潔にする。

## Required Inputs

実行前に次の値を必ず設定すること:

- `REPO_URL` 例: `https://github.com/koh0920/byok-dynamic-http`
- `SCOPED_ID` 例: `koh0920/byok-dynamic-http`
- `REGISTRY_URL` 固定: `https://<STAGING_API_DOMAIN>`
- `TARGET_ATO_VERSION` 固定: `0.3.0`

## Procedure

### Phase 0: 対話開始（必須）

最初に次をユーザーへ確認してから作業を開始する:

1. 今回の対象プロジェクト（パス / リポジトリURL）
2. deploy先（staging）でよいか
3. playground申請（`--apply-playground`）を行うか
4. 既存バージョンを上げるか（version更新方針）
5. 今回は `TARGET_ATO_VERSION=0.3.0` でデプロイすること

### Phase 1: 現在状態の確認（必須）

1. プロジェクトの整合性を確認する
   - `pwd`
   - `git remote get-url origin`
   - `git status --short`
   - `cat capsule.toml`（name/version/target/runtime/driverの確認）
   - `command -v ato`（CLI有無を確認）
   - `ato --version`（インストール済み時のみ）
2. 状態サマリをユーザーに提示し、**「この状態で次へ進むか」** を確認する。
3. `ato` 未インストール時、または `ato --version` が `0.3.0` 以下の場合は、次の staging 専用コマンドで再ダウンロードさせる:
  - `curl -fsSL https://<STAGING_STORE_DOMAIN>/install.sh | ATO_RELEASE_BASE_URL=https://<STAGING_DOWNLOAD_DOMAIN> sh`
4. 再ダウンロード後は `ato --version` を再確認し、`TARGET_ATO_VERSION(0.3.0)` 未満ならエラーとして停止する。

### Phase 2: カプセル化（必須）

1. `ato build` を実行して `.capsule` 生成可否を確認する。
2. 生成結果（出力ファイル名・サイズ・エラー有無）をユーザーに報告する。
3. **「このビルド結果で deploy してよいか」** を確認する。
4. `capsule.toml` の `version` が `0.3.0` であることを確認し、異なる場合は deploy 前に修正してから進める。

### Phase 3: deploy（staging）

1. 前提検証（ここで失敗したら publish しない）
   - `echo "$REPO_URL" | grep -E '^https://github.com/.+/.+$'`
   - `git remote get-url origin` が `REPO_URL` と一致することを確認
   - `gh auth status`
   - `ato whoami`
   - `ato publish --help` を見て、staging では `--artifact` を使わないことを確認

2. staging へ publish する（playground申請を行う場合）
   - `ato publish "$REPO_URL" --apply-playground --registry "$REGISTRY_URL" --json`
   - 出力 JSON の `source_id`, `sync_run_id`, `sync_status` を保存する。

3. 同期状態を確認する
   - `ato source sync-status --source-id "$SOURCE_ID" --sync-run-id "$SYNC_RUN_ID" --registry "$REGISTRY_URL" --json`
   - `status=completed` なら次へ進む。

4. 失敗時の自己修復
   - `E201 (link)` の場合:
     - `REPO_URL` が正しい GitHub リポジトリか確認
     - GitHub App / token 権限を確認し、`gh auth status` が正常であることを確認
     - 修復後、**同じ publish コマンド**を再実行する
   - `signature_failure_reason=target_commit_unverified` の場合:
     - GitHub Release に `*.capsule` 対応の `*.capsule.sig` が存在するか確認する。
     - 不足していれば追加し、`ato publish ...` を再実行する。
   - `failure_reason=dispatch_failed` かつ GitHub 403 の場合:
     - GitHub token / installation 権限を確認し、復旧後に `ato source rebuild --source-id "$SOURCE_ID" --ref main --registry "$REGISTRY_URL"` を実行する。

5. デプロイが完了しない場合の原因分類と通知
   - `ato` コマンド自体が見つからない:
    - 通知: 「Ato CLI が未インストールです。次を実行してインストールしてください: `curl -fsSL https://<STAGING_STORE_DOMAIN>/install.sh | ATO_RELEASE_BASE_URL=https://<STAGING_DOWNLOAD_DOMAIN> sh`」
   - `ato` バージョンが `0.3.0` 以下:
    - 通知: 「Ato CLI が古いため再ダウンロードします。`curl -fsSL https://<STAGING_STORE_DOMAIN>/install.sh | ATO_RELEASE_BASE_URL=https://<STAGING_DOWNLOAD_DOMAIN> sh` を実行後、`ato --version` を確認してください。」
   - `ato whoami` で未ログイン:
     - 通知: 「Ato にログインされていません。`ato login` を実行してください。」
   - publisher / アカウント未作成で publish 拒否:
     - 通知: 「公開者アカウントの設定が未完了です。Publisher 登録を先に完了してください。」
   - `E201 (link)`:
     - 通知: 「GitHub リポジトリ連携に失敗しました。リポジトリ権限または連携設定を確認してください。」
   - `source_exists`:
     - 通知: 「同じソースは既に登録済みです。既存ソースを更新して続行します。」
   - `signature_failure_reason=target_commit_unverified`:
     - 通知: 「署名検証が通っていません。Release の `.capsule.sig` を追加して再実行してください。」
   - `SHA256 mismatch`:
     - 通知: 「配布アーティファクト整合性エラーです。Release asset を再作成して再同期してください。」
   - 一定時間（例: 10分）で `sync_status=completed` にならない:
     - 通知: 「staging側処理が完了していません。現在は保留状態です。再試行または管理者確認が必要です。」

### Phase 4: staging 実行確認

1. `ato run --sandbox "$SCOPED_ID" --registry "$REGISTRY_URL" -y`
2. Web ターゲットの場合は `http://127.0.0.1:<port>` の疎通を確認する。
3. 実行確認結果をユーザーへ報告し、**「playground申請まで完了とするか」** を確認する。

### Phase 5: 成功時URL返却（必須）

成功時は次のURLをユーザーに返す:

1. Store URL（必須）  
  - `https://<STAGING_STORE_DOMAIN>/capsules/<publisher>/<slug>`
2. Playground URL（取得できた場合）  
   - API応答の `playground_url` または `playground_theater_url` を優先して返す。

## Output Format

最終報告は次の順で出力すること:

1. 対象 (`REPO_URL`, `SCOPED_ID`)
2. Phase 1 状態確認サマリ（git/capsule.toml）
3. Phase 2 ビルド結果
4. publish 実行コマンドと結果 JSON 要約
5. sync-status の結果
6. 実施した修復（あれば）
7. `ato run --sandbox` の結果（起動可否と確認ログ）
8. 利用URL（Store / Playground）

## Notes

- staging では署名・アーティファクト整合性チェックにより、旧リリースやダミー資産が弾かれる場合がある。
- `--allow-unverified` に依存した運用は避け、`signature_status=verified` 経路を優先する。
- `.capsule` ローカルファイルを直接 public/staging registry へ upload する運用は対象外。GitHub Source を正とする。
