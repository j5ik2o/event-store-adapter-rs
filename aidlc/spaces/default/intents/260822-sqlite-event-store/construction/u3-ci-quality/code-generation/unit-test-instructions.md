# ユニットテスト手順 — u3-ci-quality (unit-test-instructions)

## テストの位置づけ

U3はCI設定のユニットのため、新規のRustテストコードは追加しない。本ユニットの「テスト」は、追加するCIジョブと同一のコマンド列をローカルで実行して緑であることの検証（test-after — 設定を書いた後にその検証コマンドを実行する）。

## このユニットの検証コマンド（ユニットスコープ厳守）

最初の検証ステップ（Step 5）より前に、各ツール（cargo-deny・nightly rustfmt）が実行可能なことを確認する（ランナー準備）:

```bash
# audit ジョブ相当（Step 2の成果物 deny.toml の検証）
cargo deny check advisories licenses

# clippy ジョブ相当
cargo clippy --workspace --all-targets -- -D warnings

# feature-matrix ジョブ相当（ビルドのみの3構成）
cargo build -p event-store-adapter-rs --no-default-features --features dynamodb
cargo build -p event-store-adapter-rs --no-default-features --features bigtable
cargo build -p event-store-adapter-rs --all-features

# feature-matrix ジョブ相当（ビルド＋テストの3構成 — いずれもDocker不要）
cargo test -p event-store-adapter-rs --no-default-features
cargo test -p event-store-adapter-rs --no-default-features --features sqlite
cargo test -p event-store-adapter-rs --no-default-features --features sqlite-system

# 依存グラフ検査・unsafe不在検査
cargo tree -p event-store-adapter-rs -e normal --no-default-features | grep -E "aws-|tonic|googleapis" && echo NG || echo OK
cargo tree -p event-store-adapter-rs -e normal --no-default-features --features sqlite | grep hashlink && echo NG || echo OK
grep -rn "unsafe impl" lib/src/ && echo NG || echo OK
```

- 既存の全体スイート（`cargo test --all-features` — Docker込み）はベースライン（Step 1）と最終確認でのみ実行し、上記ユニットスコープ検証とは区別する

## 期待カバレッジ

- カバレッジ計測なし（チーム確定Q3）。品質は「CIジョブと同一コマンドのローカル緑化」＋「YAML構文妥当性」で担保
- 既存テスト（24件）が緑のまま変わらないこと（U3はコードに触れない — clippy対応のStep 4のみ例外で、テスト挙動を変えない範囲に限定）

## モック/スタブ方針・テストデータ管理

- 該当なし（テストコードを追加しないため）
