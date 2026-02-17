# ato-cli TODO

## Phase 1: 基盤整備 ✅

### 完了したタスク
- [x] `src/env.rs` - 環境変数モジュール作成
  - `read_env()` 関数定義
  - `ENV_NACELLE_PATH`, `ENV_SIDECAR_PATH`, `ENV_DEV_MODE` 定数定義

- [x] `src/error_codes.rs` - エラーコード定数定義
  - `EXIT_SUCCESS: i32 = 0`
  - `EXIT_USER_ERROR: i32 = 1`
  - `EXIT_SYSTEM_ERROR: i32 = 2`
  - `EXIT_NETWORK_ERROR: i32 = 3`
  - `EXIT_VERIFICATION_ERROR: i32 = 4`
  - `EXIT_RUNTIME_ERROR: i32 = 5`
  - `EXIT_SIGTERM: i32 = 128`
  - `EXIT_SIGINT: i32 = 130`

- [x] `main.rs` - モジュール追加とエラーハンドリング修正
  - `mod env;` と `mod error_codes;` を追加
  - エラー発生時、`error_codes::*` 定数を使用して `std::process::exit()` を呼ぶように修正

---

## Phase 2: open/watch 実装 ✅

### 実装した機能
- [x] `src/commands/mod.rs` - コマンドモジュール作成
- [x] `src/commands/open.rs` - openコマンド実装（--watch対応）
- [x] `src/commands/open/watch.rs` - ファイル監視機能実装

### watch モードの機能
- ファイル監視（notify クレートを使用）
- デバンスタイマー（デフォルト300ms）
- 無視パターン（`*.log`, `target/*`, `node_modules/*`, `.git/*`）
- 変更検出時にカプセル再起動（未実装 - TODO）
- 環境変数 `CAPSULE_WATCH_DEBOUNCE_MS` と `CAPSULE_WATCH_IGNORE` に対応

### open コマンドの変更点
- `dev` → `open` に変更
- `--watch` フラグの追加（ホットリロード機能）
- `.capsule` とディレクトリの両方を指定可能

### watch モードの機能
- ファイル監視（notify クレートを使用）
- デバンスタイマー（デフォルト300ms）
- 無視パターン（`*.log`, `target/*`, `node_modules/*`, `.git/*`）
- 変更検出時にカプセル再起動
- 環境変数 `CAPSULE_WATCH_DEBOUNCE_MS` と `CAPSULE_WATCH_IGNORE` に対応

### CLI 引数の変更
```rust
Open {
    path: PathBuf,       // .capsule or directory
    watch: bool,         // Hot reload mode
    nacelle: Option<PathBuf>,
    enforcement: EnforcementMode,
}
```

---

## コードレビューの修正 ✅

### 1. Cargo.toml workspace 設定 ✅
**課題:**
- workspace 設定が削除されていたため、ドキュメント化された `cargo build` / `cargo test` コマンドが動作しない

**解決策:**
- `Cargo.toml` に workspace 設定を追加:
```toml
[workspace]
members = [
    ".",
    "core",
]
resolver = "2"
```

### 2. watch モードでの sidecar リーク修正 ✅
**課題:**
- watch モードでカプセルを再起動する際、子プロセスの `ato open` を kill しても、ato-tsnetd (sidecar) がリークする可能性がある

**解決策:**
- `CAPSULE_WATCH_MODE=1` 環境変数を子プロセスに渡す
- `sidecar.rs` で `CAPSULE_WATCH_MODE` が設定されている場合は sidecar を起動しない
- 親プロセスの `ato open --watch` が sidecar を管理

### 3. NO_PROXY 環境変数の上書き問題修正 ✅
**課題:**
- `proxy_env_for_socks5` が既存の `NO_PROXY` 環境変数を上書きしていた
- ユーザー設定の proxy 除外設定が失われる問題

**解決策:**
- 既存の `NO_PROXY` 環境変数を読み取り、新しく生成するリストに追加
- 重複排除も維持

---

## 依存関係の課題と解決策

### 1. futures バージョン競合
**課題:**
- core は futures 0.3.11
- nacelle は futures 0.3.35
- 依存関係でコンパイルエラーが発生

**解決策:**
- バージョンを 0.3.35 に統一
- または、workspace でバージョン管理
- core/Cargo.toml と nacelle/Cargo.toml の futures バージョンを確認

### 2. Cargo.toml の特殊文字
**課題:**
- ファイル保存時にエンコーディングが壊れる問題
- 特殊文字が含まれると正しく保存されない

**解決策:**
- ヒアドキュメント文字列（`"`, `\n`）でテスト
- エディタで直接編集せず、ツール経由で編集

### 3. notify クレートの追加
**課題:**
- `notify = "7"` を Cargo.toml に追加済み
- ファイル監視機能に必要

**解決策:**
- ✅ 依存に追加済み

### 4. core の futures 依存
**課題:**
- core/Cargo.toml で futures 0.3.11 を使用
- ato-cli で futures 0.3.35 を使用

**解決策:**
- futures 0.3.35 にバージョンアップグレード
- または、workspace で依存管理
- core/Cargo.toml の更新も検討

---

## コンパイルエラーの対応状況

### 解決した課題
1. **futures バージョンの統一** ✅
   - `Cargo.toml` で futures を "0.3" に統一
   - 不要な features (`io`, `alloc`, `executor`) を削除
   - コンパイル成功

2. **bollard クレートの依存追加** ✅
   - `ato-cli/Cargo.toml` に `bollard = "0.15"` を追加
   - OCI executor の依存を解決

3. **Arc<CliReporter> と Arc<dyn CapsuleReporter> の型不一致** ✅
   - `OpenArgs::reporter` を `Arc<CliReporter>` に統一
   - `watch_directory` でも `Arc<CliReporter>` を使用
   - trait メソッド呼び出しを `CapsuleReporter::method(&*reporter, ...)` に修正

### Phase 2: 完了 ✅

---

## Phase 2 の課題解決 ✅

### 解決したissue
1. **重複するCommands::Open match arm** ✅
   - 古いOpenコマンドの実装（行473-487）を削除
   - unreachable pattern コンパイルエラーを解消

2. **watch modeでtokio runtimeが必要** ✅
   - `watch_directory` をasyncからsyncに変更
   - `tokio::spawn` を `std::thread::spawn` に変更
   - `tokio::sync::Mutex` を `std::sync::Mutex` に変更
   - CapsuleReporterのasyncメソッドを `futures::executor::block_on` で呼び出す

3. **watcherがdropされる問題** ✅
    - `watch_directory` でwatcherを返すように変更
    - `execute_watch_mode` でwatcherを保持してblocking
    - `std::thread::park()` で無限待機
    - CtrlCハンドラーを追加

### Phase 2b: 完了 ✅
- **CapsuleHandle struct** の追加
  - プロセス管理用のstruct (`process_handle: Arc<Mutex<Option<Child>>>`)
  - `stop()` と `is_running()` メソッド
- **CtrlCハンドラー** の実装
  - Ctrl+C でカプセルを停止して終了
  - クリーンなシャットダウン
- **再起動ロック** (restart_lock) の実装
  - 再起動中に新しいイベントを無視
  - デバンス処理との連携
- **watch_directory のシグネチャ変更**
  - `Result<RecommendedWatcher>` → `Result<(RecommendedWatcher, CapsuleHandle)>`
- **カプセルの完全再起動機能** ✅
  - ファイル変更検出時に古いプロセスをkill
  - 古いプロセスの終了を待機
  - 新しいプロセスをspawn
  - ユーザーへの通知 ("🔄 Stopping capsule...", "🚀 Starting capsule...", "✅ Capsule restarted")
  - エラーハンドリング (再起動失敗時の警告表示)

---

## Phase 3: ランタイム管理 ✅

### 実装した機能
- [x] `src/engine_manager.rs` - エンジン管理モジュール作成
- [x] Cloudflare R2 からのバイナリダウンロード（`reqwest` を使用）
- [x] `~/.capsule/engines/` への保存
- [x] `setup` コマンドの実装

### setup コマンドの機能
- エンジン名とバージョンの指定
- 自動バージョン検出（latest）
- プラットフォーム自動検出（darwin/linux, x64/arm64）
- エンジン登録とデフォルト設定

---

## Phase 4: プロセス管理 ✅

### 実装した機能
- [x] `src/process_manager.rs` - PIDファイル管理モジュール
  - PIDファイルの作成と読み取り
  - プロセス生存チェック
  - ゾンビPIDファイルのクリーンアップ
  - SIGTERM/SIGKILL サポート

- [x] `src/commands/ps.rs` - psコマンド実装
  - PID, ID, NAME, STATUS, RUNTIME, UPTIME 表示
  - `--all` フラグで停止したプロセス也表示
  - `--json` フラグでJSON出力

- [x] `src/commands/close.rs` - closeコマンド実装
  - `--id` でID指定停止
  - `--name` で名前指定停止
  - `--all` フラグで同名全インスタンス停止
  - `--force` フラグでSIGKILL使用

- [x] `src/commands/logs.rs` - logsコマンド実装
  - `--follow` フラグでリアルタイム監視
  - `--tail` フラグで最終N行表示
  - `--id` / `--name` で対象指定

### プロセス管理の既知の制限
- 現在は `ato open` がPIDファイルを書き出さない（将来実装）
- `ps` コマンドは既存のPIDファイルを表示（実プロセスとの照会は実装済み）

---

## Phase 5: pack 強化 ✅

### 完了した機能
- [x] `--init` フラグの強化
  - 対話ウィザードは `init` モジュールで既実装
  - `pack --init` で自動的に `init::execute` を呼び出し

- [x] `--key` フラグの実装
  - `main.rs` で既に実装済み
  - バンドルと同じディレクトリに `.sig` ファイルを生成
  - 分離署名の検証も実装済み

---

## 実装した新しい依存関係

- `reqwest = { version = "0.11", features = ["blocking"] }` - HTTPダウンロード
- `libc = "0.2"` - プロセス制御（SIGTERM/SIGKILL）

---

## E2Eテストの更新

### 既存のテスト
- [x] 単体テスト（unit tests, e2e tests）はコンパイル可能な状態

### 追加したテスト
- [x] watch モードの単体テスト (phase 2 で実装済み)
- [x] `ato open --watch` のE2Eテスト (phase 2 で実装済み)
- [x] CLI 引数の検証テスト (`tests/cli_tests.rs`)
- [x] `ato open <file>` のE2Eテスト (phase 2 で実装済み)
- [x] エラーコードの検証テスト (phase 1 で実装済み)

### テスト結果
- 単体テスト: 29件合格
- CLI統合テスト: 11件合格
- E2Eテスト: 1件合格

---

## コードレビューで発見されたバグの修正

### 1. ProtoState::Stopping のマッピング修正 ✅
**課題:**
- `core/src/tsnet/client.rs:81` で `ProtoState::Stopping` が `TsnetState::Starting` にマッピングされていた
- `wait_for_ready` がサイドカーが停止中にタイムアウトまでポーリングし続ける問題

**修正:**
```rust
// 修正前
ProtoState::Stopping => TsnetState::Starting,

// 修正後
ProtoState::Stopping => TsnetState::Stopped,
```

### 2. reqwest の TLS バックエンド修正 ✅
**課題:**
- `Cargo.toml` の `reqwest` がデフォルト機能を使用していた
- native-tls/OpenSSL に依存し、musl 環境などでビルドが失敗する可能性

**修正:**
```toml
# 修正前
reqwest = { version = "0.11", features = ["blocking"] }

# 修正後
reqwest = { version = "0.11", default-features = false, features = ["blocking", "rustls-tls"] }
```

---

## 技術的ブロッカーと対策

### 1. GitHub API Rate Limit (Phase 3)
**課題:**
- ランタイムのダウンロードに GitHub Releases API を使用
- 未認証リクエストでレート制限に引っかかる

**対策:**
- Cloudflare R2 にバイナリを配置
- 固定の URL パターン（`.../releases/latest/download/...`）を使用
- API トークン不要でレート制限を回避

### 2. プロセスの「完全な」バックグラウンド化 (Phase 2)
**課題:**
- Rust の `Command::spawn()` は親プロセス（CLI）が終了すると、子プロセスも道連れに終了する場合がある
- `ato open`（バックグラウンドモード）は、CLI が終了してもランタイムが生き続ける必要がある

**対策:**
- まず macOS/Linux のみサポート
- double fork または setsid を使用
- デタッチ処理の実装

### 3. Stale PID (ゾンビPIDファイル) 問題 (Phase 4)
**課題:**
- マシンが強制終了したり、カプセルがクラッシュした場合、PIDファイルが残る
- 次に `ato ps` したとき、その PID が偶然別のプロセス（OSの無関係なプロセス）に割り当たると、誤判定を起こす

**対策:**
- PIDファイルの存在確認だけでなく、「プロセス名が期待通りか（nacelle / ato-tsnetd か）」も検証するロジックを `process_manager.rs` に追加
- sysinfo クレートを活用

### 4. Windows対応 (Phase 2)
**課題:**
- notify クレートの Windows 対応状況を確認
- パス区切り文字（\ vs /）の扱い

**対策:**
- ✅ notify クレートは Windows 対応（ReadDirectoryChangesW API）
- ✅ PathBuf を使用していれば問題なし

---

## 次のステップ

1. ✅ 依存関係の解消 (完了)
   - futures バージョンを "0.3" に統一
   - bollard クレートを ato-cli に追加
   - コンパイル成功

2. ✅ Phase 2 の完了 (基本機能のみ)
   - `ato open` コマンドの実装
   - `--watch` フラグの実装 (ファイル監視、デバンス、無視パターン)
   - 型不一致を解決 (Arc<CliReporter> 統一)
   - watch モードの基本実装完了

3. ✅ Phase 2b: watch モードの実装 (完了)
   - CapsuleHandle struct の追加
   - CtrlCハンドラーの実装（カプセル停止付き）
   - `watch_directory` のシグネチャ変更
   - 再起動中のイベント無視ロジック（restart_lock）実装
   - カプセル完全再起動機能の実装
     - kill → wait → spawn の完全実装
     - エラーハンドリング付き
     - ユーザー通知付き

4. ✅ Phase 3 の完了
   - ランタイム自動ダウンローダー (engine_manager.rs)
   - setup コマンドの実装
   - Cloudflare R2 からのバイナリダウンロード

5. ✅ Phase 4 の完了
   - プロセス管理コマンドの実装
   - ps, close, logs コマンド
   - PIDファイル管理とゾンビPID処理

6. ✅ Phase 5 の完了
   - pack コマンドの強化
   - --init フラグと --key フラグの実装

7. 次のタスク
    - [x] テストの追加実装 (完了)
    - [x] PIDファイル書き出し機能の実装（ato open 時）- Source executor対応
    - [x] バックグラウンドモードの実装 (`--background` フラグ追加)

---

## 備考事項

- ✅ Phase 1 (基盤整備) - 完了
- ✅ Phase 2 (open/watch 実装) - 完了
- ✅ Phase 3 (ランタイム管理) - 完了
- ✅ Phase 4 (プロセス管理) - 完了
- ✅ Phase 5 (pack 強化) - 完了
- macOS/Linux のみを対象（Windows 対応は後回し）
- テストは常に最新の状態に保つ
- 現在のビルド: `cargo build` 成功、`cargo test` 成功 (警告のみあり)
