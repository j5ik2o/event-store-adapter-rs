use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{gc_rule, table, GetTableRequest};
use testcontainers::{ContainerAsync, GenericImage, Image};
use tonic::transport::Channel;

use crate::bigtable::{
  connect, create_table, create_tables, FamilySpec, GarbageCollection, HEAD_FAMILIES, JOURNAL_FAMILIES,
};
use crate::docker::{bigtable_emulator, BIGTABLE_EMULATOR_PORT};

/// Bigtable エミュレーターを起動し、コンテナ・公開ポート・管理 API の接続を返す。
/// コンテナは呼び出し側が保持する（drop で削除される）。
async fn start() -> (ContainerAsync<GenericImage>, Channel) {
  let container = bigtable_emulator()
    .await
    .expect("failed to start the Bigtable emulator");
  let port = container
    .get_host_port_ipv4(BIGTABLE_EMULATOR_PORT)
    .await
    .expect("failed to resolve the mapped port");
  let channel = connect(port).await.expect("failed to connect to the Bigtable emulator");
  (container, channel)
}

async fn schema_of(channel: &Channel, table_id: &str) -> Vec<(String, Option<gc_rule::Rule>)> {
  let table = BigtableTableAdminClient::new(channel.clone())
    .get_table(GetTableRequest {
      name: format!("projects/p/instances/i/tables/{table_id}"),
      view: table::View::SchemaView as i32,
    })
    .await
    .unwrap_or_else(|error| panic!("GetTable({table_id}) failed: {error:?}"))
    .into_inner();
  let mut families: Vec<(String, Option<gc_rule::Rule>)> = table
    .column_families
    .into_iter()
    .map(|(name, family)| (name, family.gc_rule.and_then(|gc| gc.rule)))
    .collect();
  families.sort_by(|left, right| left.0.cmp(&right.0));
  families
}

// 起動したコンテナは、digest で固定した google-cloud-cli 587.0.0-emulators のイメージから作られる。
#[tokio::test]
async fn test_bigtable_emulator_starts_from_the_digest_pinned_image() {
  let (container, _channel) = start().await;

  let image = container.image();

  assert_eq!(image.name(), "gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256");
  assert_eq!(
    image.tag(),
    "8d8573471c489a409f03891d2c24d177fa531276184f7002b9a2d853d1b3ed54"
  );
}

// 動くタグ（`emulators` や `latest`）で起動しない。参照の末尾は 64 桁の 16 進数の digest だけである。
#[tokio::test]
async fn test_bigtable_emulator_never_starts_from_a_moving_tag() {
  let (container, _channel) = start().await;

  let image = container.image();

  assert!(
    image.name().ends_with("@sha256"),
    "イメージ名は digest 参照の形: {}",
    image.name()
  );
  assert_eq!(image.tag().len(), 64, "tag は digest の 16 進数 64 桁: {}", image.tag());
  assert!(
    image.tag().chars().all(|c| c.is_ascii_hexdigit()),
    "tag は digest の 16 進数だけ: {}",
    image.tag()
  );
  assert!(!matches!(image.tag(), "emulators" | "latest"));
}

// `create_tables` は BT-2 のとおり、6 つの列ファミリを版数の GC（最大 1）だけで作り、
// エミュレーターは GetTable（SCHEMA_VIEW）でそれを返す。
#[tokio::test]
async fn test_create_tables_creates_the_profile_column_families_with_version_gc_only() {
  let (_container, channel) = start().await;

  create_tables(&channel, "p", "i", "journal-a", "head-a")
    .await
    .expect("failed to create the tables");

  let journal = schema_of(&channel, "journal-a").await;
  let head = schema_of(&channel, "head-a").await;
  let one_version = Some(gc_rule::Rule::MaxNumVersions(1));
  assert_eq!(
    journal,
    JOURNAL_FAMILIES
      .iter()
      .collect::<std::collections::BTreeSet<_>>()
      .into_iter()
      .map(|name| (name.to_string(), one_version.clone()))
      .collect::<Vec<_>>()
  );
  assert_eq!(
    head,
    HEAD_FAMILIES
      .iter()
      .collect::<std::collections::BTreeSet<_>>()
      .into_iter()
      .map(|name| (name.to_string(), one_version.clone()))
      .collect::<Vec<_>>()
  );
}

// 年齢による GC や GC なしの列ファミリを持つテーブルも作れ、GetTable はその違いを返す。
// 生成時の照合が拒否すべき表を、試験が用意できることを確かめる。
#[tokio::test]
async fn test_create_table_can_build_a_family_with_age_gc_or_without_gc() {
  let (_container, channel) = start().await;

  create_table(
    &channel,
    "p",
    "i",
    "variant",
    &[
      FamilySpec {
        name: "aged",
        garbage_collection: GarbageCollection::MaxAgeSeconds(3600),
      },
      FamilySpec {
        name: "plain",
        garbage_collection: GarbageCollection::None,
      },
      FamilySpec::current("current"),
    ],
  )
  .await
  .expect("failed to create the table");

  let families = schema_of(&channel, "variant").await;
  assert_eq!(
    families,
    vec![
      (
        "aged".to_string(),
        Some(gc_rule::Rule::MaxAge(prost_types::Duration {
          seconds: 3600,
          nanos: 0
        }))
      ),
      ("current".to_string(), Some(gc_rule::Rule::MaxNumVersions(1))),
      ("plain".to_string(), None),
    ]
  );
}
