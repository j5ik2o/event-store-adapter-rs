use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::types::AttributeValue;

use super::clock::Clock;
use super::DynamoDbTables;
use crate::aggregate_id::AidString;
use crate::retention::ttl_expires_epoch_seconds;
use crate::seq_nr::SeqNr;

pub(super) async fn mark_history(
  client: &aws_sdk_dynamodb::Client,
  tables: &DynamoDbTables,
  clock: &dyn Clock,
  aid: &AidString,
  expired: &[SeqNr],
  grace_seconds: u64,
) -> Result<(), String> {
  for seq_nr in expired {
    let expires = ttl_expires_epoch_seconds(clock.now_epoch_seconds(), grace_seconds);
    let result = client
      .update_item()
      .table_name(&tables.snapshot_table_name)
      .key("aid", AttributeValue::S(aid.as_str().into()))
      .key("skey", AttributeValue::N(seq_nr.to_string()))
      .update_expression("SET #ttl = :expires REMOVE active_history_seq_nr")
      .condition_expression("attribute_exists(active_history_seq_nr)")
      .expression_attribute_names("#ttl", "ttl")
      .expression_attribute_values(":expires", AttributeValue::N(expires.to_string()))
      .send()
      .await;
    if let Err(error) = result {
      if !error
        .as_service_error()
        .is_some_and(|error| error.is_conditional_check_failed_exception())
      {
        return Err(DisplayErrorContext(&error).to_string());
      }
    }
  }
  Ok(())
}
