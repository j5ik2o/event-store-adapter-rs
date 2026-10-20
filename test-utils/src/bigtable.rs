use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
  gc_rule, table, ColumnFamily, CreateTableRequest, GcRule, ListTablesRequest, Table,
};
use tonic::transport::{Channel, Endpoint};

/// journal テーブルの列ファミリ（BT-2）。
pub const JOURNAL_FAMILIES: [&str; 3] = ["event", "history", "config"];
/// head テーブルの列ファミリ（BT-2）。
pub const HEAD_FAMILIES: [&str; 3] = ["head", "snapshot", "config"];

/// 列ファミリのガベージコレクション（GC）の設定。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GarbageCollection {
  /// 版数の GC（最大 `n` 版）だけを設定する。BT-2 が求める形は `MaxVersions(1)`。
  MaxVersions(i32),
  /// 年齢による GC（`seconds` 秒）だけを設定する。BT-2 は年齢による GC を禁じる。
  MaxAgeSeconds(i64),
  /// GC の規則を設定しない。
  None,
}

/// 作る列ファミリ 1 つ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilySpec {
  pub name: &'static str,
  pub garbage_collection: GarbageCollection,
}

impl FamilySpec {
  /// BT-2 のとおり、版数の GC を最大 1 にした列ファミリを返す。
  pub const fn current(name: &'static str) -> Self {
    Self {
      name,
      garbage_collection: GarbageCollection::MaxVersions(1),
    }
  }
}

/// エミュレーターの公開ポートへ接続する。認証は使わない。
pub async fn connect(port: u16) -> Result<Channel> {
  let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?;
  Ok(endpoint.connect().await?)
}

/// 待ち合わせの 1 回の試行（接続と `ListTables`）の上限。応答しないコンテナで 1 回の試行が終わらなくなるのを防ぐ。
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// 管理 API の `ListTables` が成功するまで待つ（250 ミリ秒ごと、上限 120 秒）。
/// 1 回の試行（接続と `ListTables`）は 5 秒で打ち切り、時間切れはその試行の失敗として扱う。
/// 上限を超えたときは、最後の誤りを含めて `Err` を返す。
pub(crate) async fn wait_until_list_tables_succeeds(port: u16) -> Result<()> {
  let started_at = Instant::now();
  loop {
    let attempt = tokio::time::timeout(ATTEMPT_TIMEOUT, async {
      let channel = connect(port).await?;
      BigtableTableAdminClient::new(channel)
        .list_tables(ListTablesRequest {
          parent: "projects/wait/instances/wait".to_string(),
          ..Default::default()
        })
        .await?;
      anyhow::Ok(())
    })
    .await
    .unwrap_or_else(|_| {
      Err(anyhow::anyhow!(
        "ListTables attempt did not finish within {} seconds",
        ATTEMPT_TIMEOUT.as_secs()
      ))
    });
    match attempt {
      Ok(()) => return Ok(()),
      Err(error) => {
        if started_at.elapsed() >= Duration::from_secs(120) {
          anyhow::bail!("ListTables did not succeed within 120 seconds: {error:?}");
        }
      }
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
  }
}

fn gc_rule_of(garbage_collection: GarbageCollection) -> Option<GcRule> {
  match garbage_collection {
    GarbageCollection::MaxVersions(versions) => Some(GcRule {
      rule: Some(gc_rule::Rule::MaxNumVersions(versions)),
    }),
    GarbageCollection::MaxAgeSeconds(seconds) => Some(GcRule {
      rule: Some(gc_rule::Rule::MaxAge(prost_types::Duration { seconds, nanos: 0 })),
    }),
    GarbageCollection::None => None,
  }
}

/// 指定した列ファミリを持つテーブルを 1 つ作る。タイムスタンプの粒度はミリ秒にする。
pub async fn create_table(
  channel: &Channel,
  project_id: &str,
  instance_id: &str,
  table_id: &str,
  families: &[FamilySpec],
) -> Result<()> {
  let column_families: HashMap<String, ColumnFamily> = families
    .iter()
    .map(|family| {
      (
        family.name.to_string(),
        ColumnFamily {
          gc_rule: gc_rule_of(family.garbage_collection),
          ..Default::default()
        },
      )
    })
    .collect();
  BigtableTableAdminClient::new(channel.clone())
    .create_table(CreateTableRequest {
      parent: format!("projects/{project_id}/instances/{instance_id}"),
      table_id: table_id.to_string(),
      table: Some(Table {
        column_families,
        granularity: table::TimestampGranularity::Millis as i32,
        ..Default::default()
      }),
      initial_splits: vec![],
    })
    .await?;
  Ok(())
}

/// プロファイルの配置（BT-2）どおりに、journal と head の 2 テーブルを作る。
///
/// journal は `event`・`history`・`config`、head は `head`・`snapshot`・`config` を持ち、
/// どの列ファミリも年齢による GC はなく、版数の GC は最大 1 にする。設定行は作らない。
pub async fn create_tables(
  channel: &Channel,
  project_id: &str,
  instance_id: &str,
  journal_table_id: &str,
  head_table_id: &str,
) -> Result<()> {
  let journal: Vec<FamilySpec> = JOURNAL_FAMILIES.iter().map(|name| FamilySpec::current(name)).collect();
  let head: Vec<FamilySpec> = HEAD_FAMILIES.iter().map(|name| FamilySpec::current(name)).collect();
  create_table(channel, project_id, instance_id, journal_table_id, &journal).await?;
  create_table(channel, project_id, instance_id, head_table_id, &head).await?;
  Ok(())
}
