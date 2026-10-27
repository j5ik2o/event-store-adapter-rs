#![cfg(all(feature = "spanner", feature = "test-hooks"))]
//! README とスキーマ文書のコード例が、公開する型と一致し、実際に動くことを確かめる（エミュレーター）。
//!
//! - README（英語と日本語）の Spanner の例は、このファイルの `README:spanner-use` と `README:spanner` の
//!   2 つの区間と、一字一句同じである。この試験は、同じコードを実際に実行する。
//! - スキーマ文書（英語と日本語）の SQL は、直前の行が `<!-- spanner-schema:<名前> -->` の `sql` コードブロックにある。
//!   `ddl`（5 テーブルの CREATE TABLE）・`ttl-policy`（snapshot_history の行削除ポリシー）・`config`（手で初期化するときの
//!   config の INSERT）を実行し、生成が成功する。文書の例は、接頭辞 `esa`・保持件数 2・猶予 7 日の期限切れ方式の設定である。

mod spanner_support;

use event_store_adapter_rs::{
  ConfigurationReason, EventStoreError, EventStoreForSpanner, SpannerOptions, SpannerRetention,
};
use serde_json::Value;
use spanner_support::Fixture;

// README:spanner-use:begin
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_spanner_store, AggregateId, EventEnvelope, EventStore, SnapshotEnvelope, SpannerConnection, SpannerTables,
};
use serde_json::json;

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    self.0.clone()
  }
}
// README:spanner-use:end

/// README の例の本体。`channel` と `tables` は、README の `main` が先に作る（エミュレーターへの平文の接続と、導いたテーブルの接頭辞）。
async fn readme_example(
  channel: tonic::transport::Channel,
  tables: SpannerTables,
) -> Result<(), Box<dyn std::error::Error>> {
  // README:spanner:begin
  let connection = SpannerConnection::new(channel);
  let options = SpannerOptions {
    retention: SpannerRetention::KeepLatest { count: 2 },
    ..SpannerOptions::default()
  };
  // Run once for new tables; they are no longer uninitialized afterwards.
  initialize_spanner_store(&connection, &tables, &options).await?;
  let store: EventStoreForSpanner<AccountId, Value, Value> =
    EventStoreForSpanner::open(connection, tables, options).await?;
  let id = AccountId("1".into());

  store
    .persist_event(
      EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"})).with_manifest("account-created/v1"),
    )
    .await?;
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
      SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
    )
    .await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  // README:spanner:end
  Ok(())
}

const README_EN: &str = include_str!("../../README.md");
const README_JA: &str = include_str!("../../README.ja.md");
const SCHEMA_EN_PATH: &str = "docs/SPANNER_DATABASE_SCHEMA.md";
const SCHEMA_JA_PATH: &str = "docs/SPANNER_DATABASE_SCHEMA.ja.md";
const THIS_FILE: &str = include_str!("spanner_readme_test.rs");

/// リポジトリの根からの相対パスの文書を、実行時に読む（文書がないときにこのクレートの全試験のビルドを止めない）。
fn read_document(relative_path: &str) -> String {
  let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
    .join("..")
    .join(relative_path);
  std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{} を読める: {error}", path.display()))
}

/// このファイルの `// README:<name>:begin` と `// README:<name>:end` の間の行を、そのまま返す。
fn example_region(name: &str) -> String {
  let begin = format!("// README:{name}:begin");
  let end = format!("// README:{name}:end");
  let mut lines = THIS_FILE.lines().skip_while(|line| line.trim() != begin);
  assert!(lines.next().is_some(), "{begin} がある");
  let body: Vec<&str> = lines.take_while(|line| line.trim() != end).collect();
  assert!(!body.is_empty(), "{name} の区間が空でない");
  body.join("\n")
}

/// 文書の `<!-- spanner-schema:<name> -->` の直後の `sql` コードブロックの中身を返す。
fn schema_sql(document: &str, name: &str) -> String {
  let marker = format!("<!-- spanner-schema:{name} -->");
  let mut lines = document.lines().skip_while(|line| line.trim() != marker);
  assert!(lines.next().is_some(), "{marker} がある");
  let mut lines = lines.skip_while(|line| !line.trim_start().starts_with("```sql"));
  assert!(lines.next().is_some(), "{marker} の後に ```sql のブロックがある");
  lines
    .take_while(|line| !line.trim_start().starts_with("```"))
    .collect::<Vec<_>>()
    .join("\n")
}

fn statements_of(sql: &str) -> Vec<String> {
  sql
    .split(';')
    .map(str::trim)
    .filter(|statement| !statement.is_empty())
    .map(str::to_string)
    .collect()
}

#[test]
fn should_spanner_readme_of_both_languages_contain_the_example_that_this_test_runs() {
  for (language, readme) in [("English", README_EN), ("日本語", README_JA)] {
    for region in ["spanner-use", "spanner"] {
      let code = example_region(region);

      assert!(
        readme.contains(&code),
        "{language} の README に、試験が実行する例（{region}）と同じコードがある:\n{code}"
      );
    }
  }
}

#[tokio::test]
async fn should_spanner_readme_example_run_against_the_emulator() {
  let fixture = Fixture::new().await;

  let result = readme_example(fixture.channel().await, fixture.tables.clone()).await;

  result.expect("README の例が成功する");
  assert_eq!(fixture.journal_rows().await.len(), 2);
  assert_eq!(
    fixture.history_rows().await.len(),
    1,
    "snapshot を 1 件書いたので、履歴は 1 件（保持件数 2）"
  );
}

#[test]
fn should_spanner_schema_documents_of_both_languages_have_the_same_sql() {
  let (english, japanese) = (read_document(SCHEMA_EN_PATH), read_document(SCHEMA_JA_PATH));
  for name in ["ddl", "ttl-policy", "config"] {
    assert_eq!(
      schema_sql(&english, name),
      schema_sql(&japanese, name),
      "英語と日本語の文書で、SQL（{name}）が同じ"
    );
  }
}

#[tokio::test]
async fn should_spanner_schema_documents_sql_create_a_store_that_opens_with_the_documented_options() {
  for (language, path) in [("English", SCHEMA_EN_PATH), ("日本語", SCHEMA_JA_PATH)] {
    let document = read_document(path);
    let mut statements = statements_of(&schema_sql(&document, "ddl"));
    statements.extend(statements_of(&schema_sql(&document, "ttl-policy")));
    let fixture = Fixture::with_statements(statements, "esa").await;
    let config = schema_sql(&document, "config");
    for statement in statements_of(&config) {
      fixture.raw_execute(&statement, &[]).await;
    }

    let opened = fixture
      .open(SpannerOptions {
        retention: SpannerRetention::KeepLatestWithTtl {
          count: 2,
          grace_days: 7,
        },
        ..SpannerOptions::default()
      })
      .await;

    assert!(
      opened.is_ok(),
      "{language} の文書の SQL で作ったストアを、文書の設定で開ける: {opened:?}"
    );
    let mismatched = fixture.open(SpannerOptions::default()).await;
    assert!(
      matches!(
        mismatched,
        Err(EventStoreError::Configuration {
          reason: ConfigurationReason::SpannerConfigurationMismatch
        })
      ),
      "文書の config の例は、記録した設定として検査される: {mismatched:?}"
    );
  }
}
