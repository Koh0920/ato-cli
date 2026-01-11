# capsule-cli

`capsule` は **メタレイヤー / CLI** として、`capsule.toml` を読み、適切な下位エンジン（例: `nacelle`）へディスパッチします。

このワークスペースでは、プロセス境界（JSON over stdio）の最小契約に従って `nacelle internal ...` を呼び出します。

- 契約: ../nacelle/docs/ENGINE_INTERFACE_CONTRACT.md

## Quick Start

```bash
# nacelle をビルド（エンジン）
cd ../nacelle/cli
cargo build

# capsule をビルド（メタCLI）
cd ../../capsule-cli
cargo build

# 一度だけ engine を登録（PATH探索は無効化されているため）
./target/debug/capsule engine register --name default --path ../nacelle/target/debug/nacelle --default

# 1) Create
./target/debug/capsule new my-app --template node
cd my-app

# 2) Run (dev)
# `execution.dev.entrypoint` があればそれを優先
../target/debug/capsule dev

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
../target/debug/capsule pack --bundle

# 4) Execute (deploy artifact)
./nacelle-bundle
```

## Dev / Release Profiles

`capsule.toml` の `[execution]` に加えて、以下のプロファイルをサポートします。

- `[execution.dev]`:
	- `capsule dev` / `nacelle dev` が優先して使う
- `[execution.release]`:
	- `nacelle-bundle` 実行（=配布物）が優先して使う

## .capsuleignore

`.capsuleignore`（オプション）を置くと、bundle に含めるファイルを制御できます。

- Node/Bun の場合は `node_modules/` を除外し、`bun build` の成果物（例: `dist/`）だけを同梱する運用を推奨します。
