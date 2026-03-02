# ato-cli

`ato` は `capsule.toml` を解釈して、実行・配布・インストールを行うメタCLIです。  
Zero-Trust / fail-closed を前提に、通常実行時は静かに動作し、同意や違反時のみ明示的に出力します。

## 主要コマンド

```bash
ato run [path|publisher/slug] [--registry <url>]
ato install <publisher/slug> [--registry <url>]
ato build [dir] [--force-large-payload]
ato publish [--registry <url>] [--artifact <file.capsule>] [--force-large-payload]
ato publish --dry-run
ato publish --ci
ato search [query]
ato registry serve --host 127.0.0.1 --port 8787 [--auth-token <token>]
```

## クイックスタート（ローカル）

```bash
# build
cargo build -p ato-cli

# 実行
./target/debug/ato run .
```

## 公開モデル（公式 / ローカル）

- 公式レジストリ（`https://api.ato.run`, `https://staging.api.ato.run`）:
  `ato publish` は CI-first（OIDC）で公開します。ローカルからの直接アップロードは行いません。
- ローカル/私設レジストリ（上記以外の `--registry`）:
  `ato publish --registry ...` で直接アップロードします。`--artifact` 指定を推奨します（再パッキング回避）。

```bash
# 事前ビルド + private registry へ直接 publish（推奨）
ato build .
ATO_TOKEN=pwd ato publish --registry http://127.0.0.1:8787 --artifact ./<name>.capsule
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
ato registry serve --host 127.0.0.1 --port 8787

# ターミナル2: build -> publish(artifact) -> install -> run
ato build .
ATO_TOKEN=pwd ato publish --artifact ./<name>.capsule --registry http://127.0.0.1:8787
ato install <publisher>/<slug> --registry http://127.0.0.1:8787
ato run <publisher>/<slug> --registry http://127.0.0.1:8787 --yes
```

補足:
- 書き込み（publish）は `ATO_TOKEN` が必要です（`registry serve --auth-token` 設定時）。
- 読み取り（search/install/download）は無認証のまま利用できます。
- `publish --artifact` はローカル用途向けの推奨経路です。

## 別デバイス公開（VPN / Tailscale 想定）

```bash
# サーバー側: 非loopback公開時は --auth-token 必須
ato registry serve --host 0.0.0.0 --port 8787 --auth-token pwd

# クライアント側: install/run は token 不要（読み取りAPI）
ato install <publisher>/<slug> --registry http://100.x.y.z:8787
ato run <publisher>/<slug> --registry http://100.x.y.z:8787

# パブリッシュ時のみ token 必須
ATO_TOKEN=pwd ato publish --registry http://100.x.y.z:8787 --artifact ./<name>.capsule
```

## 実行前の環境変数チェック

`ato run` は起動前に必須環境変数を検証します。未設定または空文字なら fail-closed で停止します。

- `targets.<label>.required_env = ["KEY1", "KEY2"]`（推奨）
- 既存互換: `targets.<label>.env.ATO_ORCH_REQUIRED_ENVS = "KEY1,KEY2"`

## ランタイム隔離ポリシー（Tier）

- `web/static`: Tier1（`driver = "static"` + `targets.<label>.port` 必須。`capsule.lock` 不要）
- `web/deno`: Tier1（`capsule.lock` + `deno.lock` または `package-lock.json`）
- `web/node`: Tier1（Deno compat 実行。`capsule.lock` + `package-lock.json` 必須）
- `web/python`: Tier2（`uv.lock` 必須、`--unsafe` 必須）
- `source/deno`: Tier1（`capsule.lock` + `deno.lock` または `package-lock.json`）
- `source/node`: Tier1（Deno compat 実行。`capsule.lock` + `package-lock.json` 必須）
- `source/python`: Tier2（`uv.lock` 必須、`--unsafe` 必須）
- `source/native`: Tier2（`--unsafe` 必須）

補足:
- Node は Tier1 として `--unsafe` 不要です。
- Tier2（`source/native|python`, `web/python`）は `nacelle` エンジンが必須です。
  未登録時は fail-closed で停止するため、事前に `ato engine register` か `--nacelle` / `NACELLE_PATH` で設定してください。
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
