# 公開 API の形の照合（rs）

共通契約 v5 が公開 API に求める「形」を、`event-store-adapter-rs` の公開 API が満たすかを CI で照合します。形の定義と読み方はハブの [api-surface/](https://github.com/j5ik2o/event-store-adapter/tree/main/api-surface) にあります（形式 0.1.0。まだ受け入れ条件ではありません）。

| ファイル | 内容 |
|:--|:--|
| `surface.json` | ハブの定義の写し。手で直さない。CI の lint のジョブで SHA-256（`37c91f8f…0e32`）を照合する |
| `rs.json` | 共通の名前と rs の公開 API の対応表。型の引数、値の種類、2 つの封筒、`snapshot_read`、ストア（トレイト `EventStore`）、エラーの列挙 `EventStoreError`、rs の作法（`async_trait`、`Result<T, EventStoreError>`、`Option`、`Vec`）、型の引数に許す制約を書く |
| `check/` | 照合のクレート（`event-store-adapter-api-surface-rs`、公開しない） |
| `fixtures/` | 形をわざとずらした小さな定義（`event-store-adapter-api-surface-fixtures-rs`、公開しない）。否定の試験で使う |

## 照合のしかた

固定日の nightly（rustdoc の JSON 出力に要る）を入れてから、試験を実行します。

```sh
rustup toolchain install nightly-2026-09-27 --profile minimal
cargo test -p event-store-adapter-api-surface-rs
```

試験は、固定日の nightly で `event-store-adapter-rs` を feature なしで文書化して JSON を作り、`rustdoc-types`（0.61.0、形式の版 61）で読みます。期待する形は `surface.json` と `rs.json` から導き、ずれを `<場所>: <メッセージ>` の形で報告します（例: `event_envelope.seq_nr: EventEnvelope::seq_nr returns u32, want SeqNr`）。`rs.json` にない共通の項目も、飛ばさずに報告します。`cargo test --workspace` にも含まれるので、この nightly が要ります。

nightly の日付を上げるときは、`check/src/rustdoc.rs` の `TOOLCHAIN`、`.github/workflows/ci.yml` の `api-surface` ジョブ、`check/Cargo.toml` の `rustdoc-types` の版の 3 か所を一緒に変えます。`surface.json` を差し替えるときは、`ci.yml` の SHA-256 も更新します。

## 確かめていること

- 各型（`EventEnvelope`・`SnapshotEnvelope`・`SnapshotRead`）があり、各要素を読む取得のメソッドがあって、戻り値の型が値の種類と合う（`seq_nr` は `SeqNr`、`timestamp` は `&chrono::DateTime<chrono::Utc>`、`string` は `&str`、`aggregate_id` は `&AID`、`snapshot` は `Option<&SnapshotEnvelope<A>>`）。
- 構築の関数（`new`）が `required: true` の要素を必ず受け取り、`required: false` の要素（`manifest`）は受け取らず、`with_manifest` で足せる。
- ストア（`EventStore`）に 4 つの操作があり、`async_trait` が書き換えた戻り値（`Pin<Box<dyn Future<Output = …>>>`）の中身が `Result<T, EventStoreError>` で、`T` と入力が `surface.json` の入出力と合う（`none` は `()`、`optional<T>` は `Option<T>`、`list<T>` は `Vec<T>`、集約 ID の入力は `&Self::AID`）。
- `EventStoreError` に 5 つの分類の値がある。
- 型の引数 `P`・`A`（データの型の定義と inherent の `impl`、ストアの関連型）に、`Send`・`Sync`・`'static` 以外の制約がない。ストアの関連型 `AID` は `AggregateId` の制約を持つ。

否定の試験（`check/tests/api_surface_test.rs`）は、形を 1 か所ずらした定義を `fixtures/` に置き、期待する 1 か所だけが報告されることを確かめる。

- 否定の 7 つ: `occurred_at()` を消す／`manifest()` の名前を変える／`seq_nr()` の戻り値を `u32` にする／`new` から必須の `seq_nr` の引数を消す／`get_events_by_id_since_seq_nr` が `Vec<Self::P>` を返す／`EventStoreError` から `Storage` を消す／`EventStore` の `P` に `Clone` を足す。
- 追加: データの型の引数（`SnapshotRead` の `A`）に `Clone` を足す。

## 確かめられないこと

- 振る舞い（`manifest` の既定値 `""`、照合、保持、順序、どの操作がどの分類を返すか、エラーの値の中身）。ハブの README のとおり、形では表せない。振る舞いは適合テストデータ（`conformance/`）で確かめる。
- 余分なメソッド・列挙の値・操作（形が定めていないので報告しない）。
- 型の一致はクレート名と型名で見る。同名の別の型は区別しない。
- メモリと DynamoDB の実装、feature で増える公開の型。
