# ato-cli

`ato` は **メタレイヤー / CLI** として、`capsule.toml` を読み、適切な下位エンジン（例: `nacelle`）へディスパッチします。

このワークスペースでは、プロセス境界（JSON over stdio）の最小契約に従って `nacelle internal ...` を呼び出します。

- 契約: ../nacelle/docs/ENGINE_INTERFACE_CONTRACT.md

## Quick Start

```bash
# nacelle をビルド（エンジン）
cd ../nacelle/cli
cargo build

# capsule をビルド（メタCLI）
cd ../../ato-cli
cargo build

# 一度だけ engine を登録（PATH探索は無効化されているため）
./target/debug/ato config engine register --name default --path ../nacelle/target/debug/nacelle --default

# 1) Create
./target/debug/ato init my-app --template node
cd my-app

# 2) Run (dev)
# `execution.dev.entrypoint` があればそれを優先
../target/debug/ato run .

# (Node/Bun) 3) Build (recommended)
# `execution.release.entrypoint` を実行できるように、配布前にビルド成果物を生成
bun install
bun run build

# (Bun) Note: `--hot` / `--watch` のフラグ位置
# Bun のフラグは `bun` の直後に置きます（`bun --hot run ...`）。
# `bun run <script> --hot` のように末尾へ置くと、フラグがスクリプト側に渡って Bun が解釈しないことがあります。
# `capsule.toml` の `execution.dev.entrypoint` では、以下のような形を推奨します:
#   - bun --hot run src/index.ts
#   - bun --watch run src/index.ts

# 3) Ship (bundle)
../target/debug/ato build .

# 4) Execute (deploy artifact)
./nacelle-bundle
```

## Dev / Release Profiles

`capsule.toml` の `[execution]` に加えて、以下のプロファイルをサポートします。

- `[execution.dev]`:
	- `ato run` が優先して使う
- `[execution.release]`:
	- `nacelle-bundle` 実行（=配布物）が優先して使う

## Package Search

公開されているパッケージは次のコマンドで検索できます。

```bash
ato search ai
ato search --category productivity --limit 10
```

互換のため、旧 `ato package search ...` も引き続き利用できます。

既定の検索先は `https://api.ato.run` です。`ATO_STORE_API_URL` または `--registry` で上書きできます。

## Login (Device Flow)

`ato login` はブラウザを開いて `store.ato.run` の認証を行い、完了後に CLI へセッションを自動引き継ぎします。

```bash
ato login
```

互換のため、従来の PAT フローも利用可能です。

```bash
ato login --token <github-personal-access-token>
```

## Environment Variables

- `ATO_STORE_API_URL` (default: `https://api.ato.run`)
- `ATO_STORE_SITE_URL` (default: `https://store.ato.run`)
- `ATO_SESSION_TOKEN` (優先)
- `CAPSULE_SESSION_TOKEN` (legacy fallback)

## .capsuleignore

`.capsuleignore`（オプション）を置くと、bundle に含めるファイルを制御できます。

- Node/Bun の場合は `node_modules/` を除外し、`bun build` の成果物（例: `dist/`）だけを同梱する運用を推奨します。

## build / isolation

`capsule.toml` は packaging-time / runtime-time の追加設定をサポートします。

```toml
[build]
# `.capsuleignore` に追加で適用される除外パターン（pack / bundle の両方）
exclude_libs = ["**/.venv/**", "**/site-packages/torch/**"]

# GPU向けのスキャフォールド/テンプレ選択用（挙動の自動変更はしない）
gpu = true

# OCI pack のためのビルド設定（任意）
# dockerfile を指定すると docker build を実行します。
dockerfile = "./Dockerfile"   # デフォルト: ./Dockerfile (gpu=true の場合は Dockerfile.cuda を優先)
context = "."                 # デフォルト: manifest ディレクトリ
image = "my-org/my-app"        # デフォルト: manifest.name
tag = "v0.1.0"                 # デフォルト: manifest.version または latest
target = "runtime"             # 任意: マルチステージビルドの --target

[isolation]
# ホスト環境変数の透過を allowlist 方式で許可（bundle 実行時）
allow_env = ["HF_TOKEN", "LD_LIBRARY_PATH"]

[execution.env]
GUMBALL_MODEL = "qwen3-8b"
```

## Scaffold Docker

self-extracting bundle（`nacelle-bundle`）をコンテナで実行するための雛形を生成します。

```bash
# capsule.toml の build.gpu を見て Dockerfile を選択
ato scaffold docker --manifest capsule.toml

# 既存ファイルを上書き
ato scaffold docker --force
```

## License

Apache License 2.0 (SPDX: Apache-2.0). See [LICENSE](LICENSE).
