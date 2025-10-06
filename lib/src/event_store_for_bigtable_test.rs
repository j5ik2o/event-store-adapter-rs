use event_store_adapter_test_utils_rs::docker::bigtable_emulator;
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::gc_rule;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::table;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
  ColumnFamily, CreateTableRequest, GcRule, Table,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use tonic::transport::Channel;

use crate::event_store_for_bigtable::EventStoreForBigtable;
use crate::event_store_test_support::{exercise_user_account_flow, init_tracing, UserAccountId};

#[tokio::test]
async fn test_event_store_on_bigtable() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");

  let endpoint = format!("http://127.0.0.1:{}", port);
  let channel = Channel::from_shared(endpoint.clone())
    .expect("invalid endpoint")
    .connect()
    .await
    .expect("failed to connect to emulator");

  let project = "test-project";
  let instance = "test-instance";
  let parent = format!("projects/{}/instances/{}", project, instance);

  let mut table_admin = BigtableTableAdminClient::new(channel.clone());
  create_table(&mut table_admin, &parent, "journal", &["event"]).await;
  create_table(&mut table_admin, &parent, "snapshot", &["snapshot"]).await;

  let client = BigtableClient::new(channel.clone());

  let mut event_store = EventStoreForBigtable::new(
    client,
    project.to_string(),
    instance.to_string(),
    "journal".to_string(),
    "snapshot".to_string(),
    64,
  );

  let id_value = id_generate();
  let id = UserAccountId::new(id_value.to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

async fn create_table(client: &mut BigtableTableAdminClient<Channel>, parent: &str, table_id: &str, families: &[&str]) {
  let mut column_families = std::collections::HashMap::new();
  for &family in families {
    column_families.insert(
      family.to_string(),
      ColumnFamily {
        gc_rule: Some(GcRule {
          rule: Some(gc_rule::Rule::MaxNumVersions(1)),
        }),
        ..Default::default()
      },
    );
  }

  let table = Table {
    column_families,
    granularity: table::TimestampGranularity::Millis as i32,
    ..Default::default()
  };

  client
    .create_table(CreateTableRequest {
      parent: parent.to_string(),
      table_id: table_id.to_string(),
      table: Some(table),
      initial_splits: vec![],
    })
    .await
    .expect("failed to create table");
}
