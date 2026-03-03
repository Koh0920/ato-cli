# ato-cli

日本語 | [English](README.md)

`ato` は `capsule.toml` を解釈して、実行・配布・インストールを行うメタCLIです。  
Zero-Trust / fail-closed を前提に、通常実行時は静かに動作し、同意や違反時のみ明示的に出力します。

## 主要コマンド

```bash
ato run [path|publisher/slug] [--registry <url>]
ato open [path] [--watch]                 # 互換コマンド（非推奨; run を推奨）
ato ps
ato close --id <capsule-id> | --name <name> [--all] [--force]
ato logs --id <capsule-id> [--follow]
ato install <publisher/slug> [--registry <url>]
ato build [dir] [--force-large-payload]
ato publish [--registry <url>] [--artifact <file.capsule>] [--scoped-id <publisher/slug>] [--allow-existing] [--prepare] [--build] [--deploy] [--legacy-full-publish] [--force-large-payload]
ato publish --dry-run
ato publish --ci
ato search [query]
ato config engine install --engine nacelle [--version <ver>]
ato setup --engine nacelle [--version <ver>] # 互換コマンド（非推奨）
ato registry serve --host 127.0.0.1 --port 18787 [--auth-token <token>]
```

## クイックスタート（ローカル）

```bash
# build
cargo build -p ato-cli

# nacelle エンジンを未導入の場合（推奨）
./target/debug/ato config engine install --engine nacelle

# 互換: setup サブコマンド
./target/debug/ato setup --engine nacelle

# 実行
./target/debug/ato run .

# 開発時ホットリロード
./target/debug/ato open . --watch

# バックグラウンド管理
./target/debug/ato run . --background
./target/debug/ato ps
./target/debug/ato logs --id <capsule-id> --follow
./target/debug/ato close --id <capsule-id>
```

## 公開モデル（公式 / ローカル）

- 公式レジストリ（`https://api.ato.run`, `https://staging.api.ato.run`）:
  `ato publish` は CI-first（OIDC）で公開します。ローカルからの直接アップロードは行いません。
  既定フェーズは `deploy` のみ（handoff/diagnostics）です。ローカルで build 検証が必要な場合は `--build`（必要なら `--prepare --build`）を明示してください。
- ローカル/私設レジストリ（上記以外の `--registry`）:
  `ato publish --registry ...` で直接アップロードします。`--artifact` 指定を推奨します（再パッキング回避）。
  `--artifact` はローカル `capsule.toml` がなくても単体で publish できます。
  `--allow-existing` は private/local の deploy フェーズ（`--deploy`）でのみ利用できます。

`ato publish` は固定順 `prepare -> build -> deploy` の3フェーズで実行されます。

- フェーズ指定なし（official）: `deploy` のみ実行
- フェーズ指定なし（private/local）: 3フェーズすべて実行
- `--prepare/--build/--deploy` のいずれか指定時: 指定フェーズのみ実行
- `--artifact` 指定時: build フェーズは常に skip
- `official + deploy` は handoff のみ（ローカル upload はしない）
- `--legacy-full-publish`（official専用）は旧既定（`prepare -> build -> deploy`）へ一時的に戻す互換フラグです。非推奨で、次回メジャーリリースで削除予定です。
- `--ci` / `--dry-run` とフェーズ指定は併用不可

```bash
# 事前ビルド + private registry へ直接 publish（推奨）
ato build .
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:18787 --artifact ./<name>.capsule

# フェーズ指定の実行例
ato publish --prepare
ato publish --build
ATO_TOKEN=pwd ato publish --deploy --artifact ./<name>.capsule --registry http://127.0.0.1:18787
ato publish --registry https://api.ato.run           # 既定: deployのみ
ato publish --registry https://api.ato.run --build   # 明示的にローカルbuild + official handoff
ato publish --deploy --registry https://api.ato.run

# 一時互換フラグ（official専用・非推奨・次回メジャーで削除予定）
ato publish --registry https://api.ato.run --legacy-full-publish

# 同一 version/同一内容の再実行を成功扱いにする（idempotent / CI再試行の推奨）
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:18787 --artifact ./<name>.capsule --allow-existing
```

## Proto 再生成（メンテナンス時のみ）

通常ビルドでは `protoc` は不要です。  
`core/proto/tsnet/v1/tsnet.proto` を変更したときだけ、次を実行してください。

```bash
./core/scripts/gen_tsnet_proto.sh
```

## ローカルレジストリ E2E

```bash
# ターミナル1: ローカルHTTPレジストリ起動
ato registry serve --host 127.0.0.1 --port 18787

# ターミナル2: build -> publish(artifact) -> install -> run
ato build .
ATO_TOKEN=pwd ato publish --artifact ./<name>.capsule --registry http://127.0.0.1:18787
ato install <publisher>/<slug> --registry http://127.0.0.1:18787
ato run <publisher>/<slug> --registry http://127.0.0.1:18787 --yes
```

補足:
- 書き込み（publish）は `ATO_TOKEN` が必要です（`registry serve --auth-token` 設定時）。
- 読み取り（search/install/download）は無認証のまま利用できます。
- ローカル検証では `18787` を使うと、worker 等が `8787` を使うアプリとのポート衝突を避けられます。
- `publish --artifact` はローカル用途向けの推奨経路です。
- `--scoped-id` で artifact upload 時の publisher/slug を明示指定できます。
- `--allow-existing` は単なる競合無視ではなく、artifact hash / manifest 整合性チェック付きの冪等操作です。
- エンタープライズCIの再試行経路では、`--allow-existing` を付与して再実行を安全に決定論化することを推奨します。
- version 競合は `E202` で返り、次アクション（version更新 / `--allow-existing` / ローカルレジストリ初期化）を表示します。

## 別デバイス公開（VPN / Tailscale 想定）

```bash
# サーバー側: 非loopback公開時は --auth-token 必須
ato registry serve --host 0.0.0.0 --port 18787 --auth-token pwd

# クライアント側: install/run は token 不要（読み取りAPI）
ato install <publisher>/<slug> --registry http://100.x.y.z:18787
ato run <publisher>/<slug> --registry http://100.x.y.z:18787

# パブリッシュ時のみ token 必須
ATO_TOKEN=pwd ato publish --registry http://100.x.y.z:18787 --artifact ./<name>.capsule
```

## 実行前の環境変数チェック

`ato run` は起動前に必須環境変数を検証します。未設定または空文字なら fail-closed で停止します。

- `targets.<label>.required_env = ["KEY1", "KEY2"]`（推奨）
- 既存互換: `targets.<label>.env.ATO_ORCH_REQUIRED_ENVS = "KEY1,KEY2"`

## 動的アプリのカプセル化手順（Web + Deno Orchestrator）

複数サービス（例: dashboard + API + worker）を1つのカプセルで動かす場合は、`web/deno` ターゲット1つに統一し、`ato-entry.ts` で子プロセスを起動します。

1. パッキング前に成果物を事前ビルドする（例: `next build`、worker build、lockfile）。
2. `[pack].include` で実行成果物だけを同梱する（生の `node_modules`、`.venv`、キャッシュは同梱しない）。
3. `ato build` で一度だけ作成し、`publish --artifact` で再パッキングを避ける。

最小構成の `capsule.toml` 例:

```toml
schema_version = "0.2"
name = "my-dynamic-app"
version = "0.1.0"
default_target = "default"

[pack]
include = [
  "ato-entry.ts",
  "capsule.toml",
  "capsule.lock",
  "apps/dashboard/.next/standalone/**",
  "apps/dashboard/.next/static/**",
  "apps/control-plane/src/**",
  "apps/control-plane/pyproject.toml",
  "apps/control-plane/uv.lock",
  "apps/worker/src/**",
  "apps/worker/wrangler.dev.jsonc"
]
exclude = [
  ".deno/**",
  "node_modules/**",
  "**/__pycache__/**",
  "apps/dashboard/.next/cache/**"
]

[targets.default]
runtime = "web"
driver = "deno"
runtime_version = "1.46.3"
runtime_tools = { node = "20.11.0", python = "3.11.10" }
entrypoint = "ato-entry.ts"
port = 4173
required_env = ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID"]
```

推奨フロー:

```bash
# 1) 事前ビルド
npm run capsule:prepare

# 2) カプセル化
ato build .

# 3) 成果物をpublish（private/local registry）
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:18787 --artifact ./my-dynamic-app.capsule

# 4) install + run
ato install <publisher>/<slug> --registry http://127.0.0.1:18787
ato run <publisher>/<slug> --registry http://127.0.0.1:18787
```

補足:
- Next.js standalone は `ato build` 前に `.next/static`（必要なら `public` も）を standalone 出力へコピーしてください。
- `required_env` 未設定時、`ato run` は起動前に停止します。
- `ato-entry.ts` は、子プロセス1つの異常終了で全体停止する fail-fast 実装を推奨します。

## ランタイム隔離ポリシー（Tier）

- `web/static`: Tier1（`driver = "static"` + `targets.<label>.port` 必須。`capsule.lock` 不要）
- `web/deno`: Tier1（`capsule.lock` + `deno.lock` または `package-lock.json`）
- `web/node`: Tier1（Deno compat 実行。`capsule.lock` + `package-lock.json` 必須）
- `web/python`: Tier2（`uv.lock` 必須、`--sandbox` 推奨）
- `source/deno`: Tier1（`capsule.lock` + `deno.lock` または `package-lock.json`）
- `source/node`: Tier1（Deno compat 実行。`capsule.lock` + `package-lock.json` 必須）
- `source/python`: Tier2（`uv.lock` 必須、`--sandbox` 推奨）
- `source/native`: Tier2（`--sandbox` 推奨）

補足:
- Node は Tier1 として `--unsafe` 不要です。
- Tier2（`source/native|python`, `web/python`）は `nacelle` エンジンが必須です。
  未登録時は fail-closed で停止するため、事前に `ato engine register` か `--nacelle` / `NACELLE_PATH` で設定してください。
- Legacy 互換で `--unsafe` / `--unsafe-bypass-sandbox` は残っていますが、利用は非推奨です。
- Node/Python で非対応・逸脱が発生した場合は自動フォールバックせず fail-closed で停止します。
- `runtime=web` は `driver` が必須です（`static|node|deno|python`）。
- `runtime=web` では `public` は廃止されました。
- `runtime=web` 実行時、CLI は URL を表示します（ブラウザ自動起動はしません）。

## SKILL 実行

```bash
# 名前解決して実行（標準探索パス）
ato run --skill <skill-name>

# SKILL.md を直接指定
ato run --from-skill /path/to/SKILL.md
```

`--skill` と `--from-skill` は排他的です。

## UX方針（Silent Runner）

- 正常時は最小出力（ツールの標準出力中心）
- 同意が必要なときのみプロンプト表示
- 非対話環境では `-y/--yes` で同意を自動承認できます
- ポリシー違反や未充足は `ATO_ERR_*` JSONL を `stderr` に出力

## セキュリティと実行ポリシー（Zero-Trust / Fail-closed）

- 必須環境変数検証: `targets.<label>.required_env`（または `ATO_ORCH_REQUIRED_ENVS`）が未設定/空文字なら起動前に停止
- 危険フラグ制御: `--dangerously-skip-permissions` は `CAPSULE_ALLOW_UNSAFE=1` がない限り拒否
- ローカルレジストリ書き込み認証: `registry serve --auth-token` 利用時、publish は `ATO_TOKEN` 必須
- エンジン自動取得: チェックサム取得/検証に失敗した場合は fail-closed で停止

## 環境変数リファレンス（主要）

- `CAPSULE_WATCH_DEBOUNCE_MS`: `open --watch` のデバウンス間隔（ms, default: `300`）
- `CAPSULE_ALLOW_UNSAFE`: `--dangerously-skip-permissions` の明示許可（`1` のみ有効）
- `ATO_TOKEN`: ローカル/私設レジストリへの publish 認証トークン
- `ATO_STORE_API_URL`: `ato search` / install 系で使う API ベースURL（default: `https://api.ato.run`）
- `ATO_STORE_SITE_URL`: ストアWebのベースURL（default: `https://store.ato.run`）
- `ATO_SESSION_TOKEN`: セッション認証トークン（`CAPSULE_SESSION_TOKEN` は互換）

## 検索・認証

```bash
ato search ai
ato login
ato whoami
```

既定API:
- `ATO_STORE_API_URL` (default: `https://api.ato.run`)
- `ATO_STORE_SITE_URL` (default: `https://store.ato.run`)
- `ATO_SESSION_TOKEN` (`CAPSULE_SESSION_TOKEN` は互換)

## 開発用テスト

```bash
cargo test -p capsule-core execution_plan:: --lib
cargo test -p ato-cli --test local_registry_e2e -- --nocapture
```

## License

Apache License 2.0 (SPDX: Apache-2.0). See [LICENSE](LICENSE).
