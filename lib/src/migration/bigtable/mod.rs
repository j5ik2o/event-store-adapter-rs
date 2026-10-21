//! v3.0.6 の Bigtable（journal と snapshot の 2 テーブル）を、v5 の Bigtable（journal と head の 2 テーブル）へ移す
//! （機能 `bigtable-migration`）。
//!
//! 新しい 2 テーブルは、利用者が列ファミリ付きで作り、[`initialize_bigtable_store`](crate::initialize_bigtable_store) で
//! 初期化した、データの行がないテーブルである。旧の 2 テーブルは読み取り（ReadRows）だけで、変えない。

mod inspect;
mod legacy;
mod read;
mod write;

use std::collections::HashMap;
use std::sync::Arc;

use crate::bigtable::auth::Authorizer;
use crate::bigtable::backend::BigtableShared;
use crate::bigtable::configuration::open_checked;
use crate::bigtable::rows::TableTarget;
use crate::bigtable::{BigtableConnection, BigtableOptions, BigtableTables, BigtableTokenSource};
use crate::error::StorageOperation;
use crate::migration::{MigrationError, MigrationRejection, MigrationReport};

/// 移行元の既定配置の 2 テーブルを指定する。
///
/// 旧の 2 テーブルは、新しい 2 テーブルとは別のプロジェクト・インスタンスにあってよい。
#[derive(Debug, Clone)]
pub struct LegacyBigtableTables {
  pub project_id: String,
  pub instance_id: String,
  pub journal_table_id: String,
  pub snapshot_table_id: String,
}

fn table_path(project_id: &str, instance_id: &str, table_id: &str) -> String {
  format!("projects/{project_id}/instances/{instance_id}/tables/{table_id}")
}

/// 全件検査の後、v3 の既定の配置の Bigtable を、v5 の Bigtable へ転写する（プロファイル 10 章）。
///
/// # 前提
///
/// - 呼び出す前に、旧の 2 テーブルへの書き手を止める。この関数は書き込みの停止を強制しない。
/// - 新しい 2 テーブルは、利用者が列ファミリ付きで作り、[`initialize_bigtable_store`](crate::initialize_bigtable_store)
///   で初期化しておく。`options` は初期化に使ったものと同じにする。この関数は、テーブル・列ファミリ・設定行を作ったり、
///   補ったりしない。
/// - `token_source` は、本番の Cloud Bigtable で認証が要るときに渡す。旧と新の両方のテーブルへの要求に、
///   `authorization: Bearer <トークン>` が付く。エミュレーターでは `None` にする。接続（`connection`）は、旧と新の
///   テーブルが同じものを共有する。
///
/// # 読み方
///
/// - 型名と値は、既定の `DefaultKeyResolver` の配置の行キーから取り出す。パーティションキーの型名と行キーの型名が
///   一致することを確かめる。行キーを一意に読み解けない行と、既定でない配置の行は拒否する。`aggregate_id` のセルは、
///   取り出した値との照合にだけ使う。
/// - 型名に `-` を含む場合だけ `type_mapping` で置き換える。対応表がない、置き換え後に `-` が残る、対応後の aid が
///   衝突する、aid が 1024 バイトを超える、のどれも拒否する。
/// - 発生時刻は RFC3339 の文字列をナノ秒の値として移す。読めない文字列と範囲の外の値は拒否する。manifest は旧の値を
///   移す。スナップショットに manifest はないので空文字列にする。旧の `version` と `last_updated_at` は移さない。
///   payload は直列化をやり直さず、バイト列のまま移す。
/// - 集約ごとに、seq_nr が 1 から欠けずに続き、重複がないことを確かめる。最大の seq_nr のイベントから head を作る。
///
/// # 履歴の読み方
///
/// 旧 snapshot の履歴の行は、置き換えられる前のスナップショットをその位置で書いたものである。v5 は、head の今のスナップ
/// ショットも履歴の件数に数える（プロファイル 7 章）ので、旧の履歴の位置と今のスナップショットの位置を合わせた集合から、
/// 位置の新しい `n` 件（`n` は `options` の保持件数）を、journal の行の active な履歴として移す。保持件数がなければ
/// 履歴を移さない。移さなかった位置に履歴は補作しない。`event:has_snapshot` は、集合に入る位置で 1 にする。
/// 旧の履歴の行は、移さない場合も検査する。
///
/// # 結果
///
/// - 検査の拒否（行キーや値の誤り、封筒の上限の超過、新しいテーブルにデータの行がある、旧と新のテーブルが重なる、など）は
///   `reasons` で返す。このとき新しい 2 テーブルには何も書かず、旧テーブルも変えない。
/// - 書き込みの途中で、作ろうとした head が既にあった場合（検査の後に別の書き手が入ったとき）も、`reasons` で返す。
///   このときは、それまでに書いた行が新しい 2 テーブルに残り、`aggregates`・`events`・`snapshots` はその分の件数になる。
///   `reasons` が空でなく件数が 0 でもないときは、下の「書き込みの途中で失敗したとき」の手順でやり直す。
/// - 設定の食い違い（保持件数や封筒の上限の違い、列ファミリの違い）と、未初期化の新しいテーブルは、何も書かずに
///   `Err(MigrationError)` で返す。通信の失敗も `Err(MigrationError)` で返し、`report` にそこまでの件数を入れる。
/// - 検査の後に旧 journal の行が変わった場合（集約の行が増えた、なくなった、seq_nr が続かなくなった）も、
///   `Err(MigrationError)` で返す。このときは、それまでに書いた行が新しい 2 テーブルに残り、`report` にその分の件数を入れる。
///   下の「書き込みの途中で失敗したとき」の手順でやり直す。
/// - `aggregates` は作った head の数、`events` は書いた journal の行の数、`snapshots` は書いた旧スナップショットの数
///   （今のスナップショットと、移した履歴の行）である。
///
/// # 書き込みの途中で失敗したとき
///
/// Bigtable には複数の行にまたがるトランザクションがないので、書き込みの途中で失敗すると、新しい 2 テーブルに部分的な
/// 移行が残る。この関数は続きから再開しない。次の手順でやり直す。
///
/// 1. 旧の 2 テーブルへの書き手を止めたままにする。旧のテーブルは消さない。
/// 2. 新しい 2 テーブルを消し、列ファミリ付きで作り直す。
/// 3. [`initialize_bigtable_store`](crate::initialize_bigtable_store) を、同じ設定で呼んで初期化し直す。
/// 4. 同じ入力で、この関数をもう一度呼ぶ。
pub async fn migrate_v3_bigtable(
  connection: &BigtableConnection,
  token_source: Option<Arc<dyn BigtableTokenSource>>,
  legacy_tables: &LegacyBigtableTables,
  tables: &BigtableTables,
  options: &BigtableOptions,
  type_mapping: &HashMap<String, String>,
) -> Result<MigrationReport, MigrationError> {
  let mut report = MigrationReport::default();
  let legacy_journal = table_path(
    &legacy_tables.project_id,
    &legacy_tables.instance_id,
    &legacy_tables.journal_table_id,
  );
  let legacy_snapshot = table_path(
    &legacy_tables.project_id,
    &legacy_tables.instance_id,
    &legacy_tables.snapshot_table_id,
  );
  let new_journal = table_path(&tables.project_id, &tables.instance_id, &tables.journal_table_id);
  let new_head = table_path(&tables.project_id, &tables.instance_id, &tables.head_table_id);
  // テーブルの ID だけでなく、プロジェクトとインスタンスを含む完全なパスで比べる。要求は 1 つも送らない。
  if [&legacy_journal, &legacy_snapshot]
    .iter()
    .any(|legacy| [&new_journal, &new_head].contains(legacy))
  {
    report.reasons.push(MigrationRejection {
      table: String::new(),
      pkey: None,
      skey: None,
      aid: None,
      reason: "旧2テーブルと新2テーブルには互いに異なるテーブルが必要です".into(),
    });
    return Ok(report);
  }

  let authorizer = token_source.map(Authorizer::new).unwrap_or_default();
  // 列ファミリと設定行の照合は、公開の生成と同じコードで行う。テーブルも設定行も作らない。
  let opened = open_checked(connection, tables, options, &authorizer)
    .await
    .map_err(|error| MigrationError::new(&report, "新しいテーブルの照合", error))?;
  for (target, path) in [(&opened.journal, &new_journal), (&opened.head, &new_head)] {
    let occupied = read::first_data_row_exists(target, StorageOperation::LoadEvents)
      .await
      .map_err(|error| MigrationError::new(&report, "新しいテーブルの確認", error))?;
    if occupied {
      report.reasons.push(MigrationRejection {
        table: path.clone(),
        pkey: None,
        skey: None,
        aid: None,
        reason: "新しいテーブルに設定行以外の行があります".into(),
      });
    }
  }
  if !report.reasons.is_empty() {
    return Ok(report);
  }

  // 旧テーブルへの要求に app profile は付けない。v3.0.6 も付けておらず、新しいストアの app profile が旧のインスタンスに
  // あるとは限らない。
  let old_journal = TableTarget::new(connection.data.clone(), legacy_journal, "", authorizer.clone());
  let old_snapshot = TableTarget::new(connection.data.clone(), legacy_snapshot, "", authorizer);
  let aggregates = inspect::inspect(
    &old_journal,
    &old_snapshot,
    type_mapping,
    opened.keep_snapshot_count,
    opened.envelope_limit_bytes,
    &mut report,
  )
  .await?;
  if !report.reasons.is_empty() {
    return Ok(report);
  }

  let shared = BigtableShared {
    journal: opened.journal,
    head: opened.head,
    keep_snapshot_count: opened.keep_snapshot_count,
    envelope_limit_bytes: opened.envelope_limit_bytes,
    store_id: opened.store_id,
    #[cfg(feature = "test-hooks")]
    hooks: None,
  };
  write::write(
    &shared,
    &old_journal,
    &old_snapshot,
    type_mapping,
    &aggregates,
    &mut report,
  )
  .await?;
  Ok(report)
}
