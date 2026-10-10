use super::*;
use aws_sdk_dynamodb::types::{AttributeDefinition, ScalarAttributeType};

fn declared() -> Value {
  serde_json::from_str::<Value>(include_str!("../../../conformance/dynamodb/layout.json")).unwrap()["cases"][0].clone()
}

#[test]
fn should_check_actual_keys_indexes_streams_and_ttl_modes() {
  let declared = declared();
  let names = RequestLayout::new("j", "s", "h", "history").unwrap();
  let mut actual = declared["tables"].clone();
  for table in actual.as_array_mut().unwrap() {
    table["ttl"] = json!({"status":"DISABLED","attribute":null});
    for index in table["gsi"].as_array_mut().unwrap() {
      index["actual_name"] = json!("history");
    }
  }
  assert!(check_tables(&declared["tables"], &actual, &names, false).is_ok());
  actual[1]["ttl"] = json!({"status":"ENABLED","attribute":"ttl"});
  assert!(check_tables(&declared["tables"], &actual, &names, true).is_ok());
  actual[1]["gsi"][0]["projection"] = json!("ALL");
  assert!(check_tables(&declared["tables"], &actual, &names, true).is_err());
  let table = TableDescription::builder()
    .attribute_definitions(
      AttributeDefinition::builder()
        .attribute_name("aid")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap(),
    )
    .build();
  let schema = [KeySchemaElement::builder()
    .attribute_name("aid")
    .key_type(KeyType::Hash)
    .build()
    .unwrap()];
  assert_eq!(
    key(&table, &schema, KeyType::Hash).unwrap(),
    json!({"name":"aid","type":"S"})
  );
  assert_eq!(key(&table, &schema, KeyType::Range).unwrap(), Value::Null);
}

#[test]
fn should_require_all_eight_real_item_shapes() {
  let declared = declared();
  let mut observed = BTreeMap::new();
  for item in declared["items"].as_array().unwrap() {
    let table = item["table"].as_str().unwrap();
    let actual = super::super::items::build_item(item, &mut BTreeMap::new()).unwrap();
    observed.insert(item_kind(table, &actual).unwrap(), (table.into(), actual));
  }
  assert_eq!(observed.len(), 8);
  assert!(check_items(&declared["items"], &observed).is_ok());
  observed.remove("marked-history");
  assert!(check_items(&declared["items"], &observed).is_err());
  assert!(kind("snapshot", "Order-9", None, false).is_err());
}
