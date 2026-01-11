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

# 1) Create
./target/debug/capsule new my-app
cd my-app

# 2) Run (dev)
# ホストに Python が無くても OK（必要なら nacelle が JIT で Python を取得）
NACELLE_PATH=../../nacelle/target/debug/nacelle ../target/debug/capsule dev

# 3) Ship (bundle)
NACELLE_PATH=../../nacelle/target/debug/nacelle ../target/debug/capsule pack --bundle

# 4) Execute (deploy artifact)
./nacelle-bundle
```
