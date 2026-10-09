use super::*;

fn declaration() -> Value {
  json!({"table":"head","attributes":{"aid":"S","events":"L"},
    "nested_attributes":{"events[0]":{"seq_nr":"N","payload":"B","manifest":"S"}},
    "values":{"aid":"Order-9","events":[{"seq_nr":"1","manifest":""}]},"binary_json":{"events[0].payload":{"number":1}}})
}

#[test]
fn should_compare_exact_attributes_nested_binary_json_and_integer_values() {
  let declared = declaration();
  let mut item = build_item(&declared, &mut BTreeMap::new()).unwrap();
  assert!(compare_item(&declared, &item, &mut BTreeMap::new()).is_ok());
  let AttributeValue::L(list) = item.get_mut("events").unwrap() else {
    panic!("L")
  };
  let AttributeValue::M(event) = &mut list[0] else {
    panic!("M")
  };
  event.insert("seq_nr".into(), AttributeValue::N("1.0".into()));
  event.insert(
    "payload".into(),
    AttributeValue::B(Blob::new(b" { \"number\" : 1 } ".to_vec())),
  );
  assert!(compare_item(&declared, &item, &mut BTreeMap::new()).is_ok());
  item.insert("unexpected".into(), AttributeValue::S("x".into()));
  assert!(compare_item(&declared, &item, &mut BTreeMap::new()).is_err());
  item.remove("unexpected");
  let AttributeValue::L(list) = item.get_mut("events").unwrap() else {
    panic!("L")
  };
  list.clear();
  assert!(check_shape(&declared, &item).is_err());
  assert_eq!(key(&declared).unwrap().len(), 1);
  assert_eq!(path_pointer("events[0].payload"), "/events/0/payload");
}

#[test]
fn should_require_generated_store_ids_to_be_nonempty_and_bound_across_tables() {
  let declared = json!({"table":"head","attributes":{"aid":"S","store_id":"S"},"values":{"aid":"__config__"},"bindings":{"store_id":"generated-store-id"}});
  let mut seed_bindings = BTreeMap::from([("generated-store-id".into(), "seed-id".into())]);
  let mut item = build_item(&declared, &mut seed_bindings).unwrap();
  let mut observations = BTreeMap::new();
  compare_item(&declared, &item, &mut observations).unwrap();
  item.insert("store_id".into(), AttributeValue::S("other-id".into()));
  assert!(compare_item(&declared, &item, &mut observations).is_err());
  item.insert("store_id".into(), AttributeValue::S(String::new()));
  assert!(compare_item(&declared, &item, &mut BTreeMap::new()).is_err());
}

#[test]
fn should_reject_missing_binary_wrong_nested_type_and_invalid_number() {
  let declared = declaration();
  let mut item = build_item(&declared, &mut BTreeMap::new()).unwrap();
  let AttributeValue::L(list) = item.get_mut("events").unwrap() else {
    panic!("L")
  };
  let AttributeValue::M(event) = &mut list[0] else {
    panic!("M")
  };
  event.insert("payload".into(), AttributeValue::S("{}".into()));
  assert!(check_shape(&declared, &item).is_err());
  assert!(integer("1.1").is_err());
  assert_eq!(integer("1e2").unwrap(), 100);
  assert_eq!(
    wire_attribute(&AttributeValue::B(Blob::new([1, 2]))),
    json!({"B":"AQI="})
  );
  assert_eq!(wire_attribute(&AttributeValue::Bool(true)), json!({"BOOL":true}));
}
