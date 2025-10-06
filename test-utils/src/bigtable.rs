use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::gc_rule;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::table;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
  ColumnFamily, CreateTableRequest, GcRule, Table,
};
use tonic::transport::Channel;

pub async fn create_table(
  client: &mut BigtableTableAdminClient<Channel>,
  parent: &str,
  table_id: &str,
  families: &[&str],
) {
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
