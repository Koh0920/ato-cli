# Nacelle Bundle Runtime Panic - Bug Report

## 概要

`capsule open` または `capsule pack` で作成したバンドルを実行すると、Tokio ランタイム関連のパニックが発生し、アプリケーションが起動できない。

## エラーメッセージ

```
thread 'main' (PID) panicked at /Users/user/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tokio-1.49.0/src/time/interval.rs:138:26:
there is no reactor running, must be called from the context of a Tokio 1.x runtime
```

## 再現手順

### 手順1: カプセルマニフェストを作成

```bash
mkdir -p /tmp/test-capsule
cat > /tmp/test-capsule/capsule.toml << 'EOF'
name = "test-capsule"
version = "0.1.0"
type = "app"

[execution]
runtime = "native"
entrypoint = "bash"
command = "-c 'echo Hello && sleep 1'"
EOF
```

### 手順2: カプセルをパック

```bash
cd /path/to/capsule-cli
cargo build
./target/debug/capsule pack /tmp/test-capsule
```

**結果:** ✅ 成功 (バンドル作成)

```plaintext
📦 Capsule Pack - Pure Runtime Architecture v3.0
   Performing build-time validations...
🧭 RuntimeRouter: Source (default to source runtime)
   ...
✅ Pack complete: /private/tmp/test-capsule/nacelle-bundle
```

### 手順3: カプセルを実行

```bash
./target/debug/capsule open /tmp/test-capsule
```

**結果:** ❌ パニック発生

```plaintext
🧭 RuntimeRouter: running in normal mode
⚠️  Sidecar not available (no TSNET env)
✓ No runtime bundled (entrypoint: "bash")
ℹ️  Note: This bundle will require the entrypoint runtime...
✓ Creating bundle archive...
...
✓ Using nacelle binary: "/path/to/nacelle/target/debug/nacelle" (7770 KB)

thread 'main' (56739063) panicked at ...tokio-1.49.0/src/time/interval.rs:138:26:
there is no reactor running, must be called from the context of a Tokio 1.x runtime
```

## 影響範囲

- ** capsule-cli (動作確認)**
  - `capsule pack` ✅ 正常動作
  - `capsule open --background` ✅ PIDファイル作成まで正常
  - `capsule open` (フォアグラウンド) ❌ パニック発生

- ** バンドル直接実行 **
  ```bash
  cd /tmp/test-capsule && ./nacelle-bundle
  ```
  **結果:** ❌ 同样的パニック

## 環境情報

| 項目 | 値 |
|------|------|
| OS | macOS Sequoia 15.2 |
| Arch | arm64 (Apple Silicon) |
| Rust | 1.83.0 |
| capsule-cli | 0.2.0 |
| nacelle | 0.2.0 |
| tokio | 1.49.0 |

## 原因分析

### 問題のあるコード

**ファイル:** `nacelle/src/main.rs`

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if is_self_extracting_bundle()? {
        return bootstrap_bundled_runtime().await;  // ← 問題発生箇所
    }
    // ...
}

async fn bootstrap_bundled_runtime() -> anyhow::Result<()> {
    // ...
    nacelle::manager::r3_supervisor::run_services_from_config(&config, &temp_dir, sandbox_ref)
        .await  // ← この呼び出しでパニック
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}
```

### 問題の詳細

`bootstrap_bundled_runtime()` は `#[tokio::main]` 属性が付与された `main()` 関数から呼び出されていますが、内部で `tokio::time::interval()` を使用するコードが `main()` の呼び出しコンテキスト外で実行されている可能性があります。

パニック発生箇所 `tokio-1.49.0/src/time/interval.rs:138` は、 interval が作成される際に reactor (I/O driver) が初期化されていない場合に発生します。

## 修正案

### 案1: `Builder` を使用して明示的にRuntimeを作成

```rust
async fn bootstrap_bundled_runtime() -> anyhow::Result<()> {
    // 新しいRuntimeを作成
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    
    rt.block_on(async {
        // 既存のロジック...
        nacelle::manager::r3_supervisor::run_services_from_config(&config, &temp_dir, sandbox_ref)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    })
}
```

### 案2: バンドルモードでのみ新しいRuntimeを開始

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if is_self_extracting_bundle()? {
        // バンドルモード専用のRuntime
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(bootstrap_bundled_runtime())
    } else {
        bootstrap_bundled_runtime().await
    }
}
```

### 案3: `main.rs` の構造を簡素化

`bootstrap_bundled_runtime()` を完全に分離し、メインの Tokio Runtime とは独立したコンテキストで実行する。

## テスト環境

スモークテストで作成したテストカプセル:

| テストケース | 種類 | 結果 |
|-------------|------|------|
| `python-web` | Python HTTPサーバー | Pack: ✅, Run: ❌ |
| `nodejs-app` | Node.js 常駐プロセス | Pack: ✅, Run: ❌ |
| `shell-script` | シェルスクリプト | Pack: ✅, Run: ❌ |
| `go-app` | Go コンソールアプリ | Pack: ✅, Run: ❌ |
| `rust-app` | Rust アプリ | Pack: ✅, Run: ❌ |

すべてのテストカプセルで `pack` は成功しますが、実行時にパニックが発生します。

## 優先度

**高 (High)**

- バンドル実行が完全に動作capsule openしない
- `` コマンドの実用性が失われる

## 担当者

nacelle チーム (Runtime 担当)

## 関連ファイル

- `nacelle/src/main.rs` - メインエントリポイント
- `nacelle/src/manager/r3_supervisor.rs` - サービス実行ロジック
- `tokio-1.49.0/src/time/interval.rs:138` - パニック発生箇所

## 参考情報

- Tokio 公式ドキュメント: Runtime Configuration
- Rust コミュニティ similar issues: "there is no reactor running" panic with tokio

---

## 付録A: nacelle バージョン情報

```bash
$ nacelle --version
nacelle 0.2.0

$ nacelle engine features
{
  "capabilities": {
    "jit_provisioning": true,
    "languages": ["python"],
    "sandbox": ["macos-seatbelt"],
    "socket_activation": true,
    "workloads": ["source", "bundle"]
  },
  "engine": {
    "engine_version": "0.2.0",
    "name": "nacelle",
    "platform": "macos-aarch64"
  },
  "ok": true,
  "spec_version": "0.1.0"
}
```

## 付録B: nacelle Cargo.toml 依存関係

```toml
tokio = { version = "1", features = ["rt-multi-thread", "macros", "fs", "io-util", "sync"] }
```

## 付録C: バックトレース（可能な場合）

RUST_BACKTRACE=1 で実行時の追加情報:
- tokio::time::interval 関数で発生
-  вероятно `nacelle::manager::r3_supervisor::run_services_from_config` から呼び出し

## 付録D: 回避策

現時点では以下の回避策があります:

1. **nacelle を直接実行** (bundle を使用しない)
   ```bash
   nacelle internal run --manifest /path/to/capsule.toml
   ```

2. **デバッグモードで実行**
   ```bash
   RUST_BACKTRACE=1 ./target/debug/capsule open /path/to/capsule
   ```

3. **既存の nacelle バイナリを使用**
   - self-extracting bundle モードではなく、nacelle バイナリを直接指定

## 付録E: 影響を受ける機能

| 機能 | 状態 | 備考 |
|------|------|------|
| `capsule pack` | ✅ 正常 | バンドル作成は問題なし |
| `capsule open --background` | ⚠️ 部分的 | PIDファイル作成まで正常、実行でpanic |
| `capsule open` | ❌ 失敗 | パニック発生 |
| `nacelle-bundle` 直接実行 | ❌ 失敗 | パニック発生 |
| `capsule ps` | ✅ 正常 | PIDファイルから情報表示 |
| `capsule close` | ⚠️ 部分的 | PIDファイルは削除されるが、実プロセス終了しない |

