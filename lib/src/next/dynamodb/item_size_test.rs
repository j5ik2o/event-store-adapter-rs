use aws_sdk_dynamodb::primitives::Blob;

use super::*;

#[test]
fn should_count_utf8_names_values_and_raw_binary_bytes() {
  let item = HashMap::from([
    ("名".into(), AttributeValue::S("é🙂".into())),
    ("payload".into(), AttributeValue::B(Blob::new(vec![0, 255, 1]))),
  ]);
  assert_eq!(item_size_upper_bound(&item), 3 + 2 + 4 + 7 + 3);
}

#[test]
fn should_bound_numbers_independently_of_their_decimal_text() {
  for number in [
    "0",
    "9007199254740991",
    "-9223372036854775808",
    "-9.9999999999999999999999999999999999999E+125",
  ] {
    let item = HashMap::from([("n".into(), AttributeValue::N(number.into()))]);
    assert_eq!(item_size_upper_bound(&item), 1 + 21);
  }
}

#[test]
fn should_count_list_map_frames_and_each_nested_element() {
  let item = HashMap::from([(
    "events".into(),
    AttributeValue::L(vec![AttributeValue::M(HashMap::from([
      ("seq_nr".into(), AttributeValue::N("1".into())),
      ("occurred_at".into(), AttributeValue::N("123".into())),
      ("manifest".into(), AttributeValue::S("".into())),
      ("payload".into(), AttributeValue::B(Blob::new(vec![9, 8]))),
    ]))]),
  )]);
  assert_eq!(
    item_size_upper_bound(&item),
    6 + 3 + 1 + 3 + 4 + 6 + 21 + 11 + 21 + 8 + 7 + 2
  );
  for empty in [AttributeValue::L(vec![]), AttributeValue::M(HashMap::new())] {
    assert_eq!(item_size_upper_bound(&HashMap::from([("x".into(), empty)])), 1 + 3);
  }
}

#[test]
fn should_count_scalar_sets_without_list_overhead() {
  let item = HashMap::from([
    ("s".into(), AttributeValue::Ss(vec!["é".into(), "abc".into()])),
    ("n".into(), AttributeValue::Ns(vec!["0".into(), "1".into()])),
    (
      "b".into(),
      AttributeValue::Bs(vec![Blob::new(vec![1, 2]), Blob::new(vec![3])]),
    ),
    ("true".into(), AttributeValue::Bool(true)),
    ("null".into(), AttributeValue::Null(true)),
  ]);
  assert_eq!(item_size_upper_bound(&item), 1 + 5 + 1 + 42 + 1 + 3 + 4 + 1 + 4 + 1);
}

#[test]
fn should_distinguish_the_journal_limit_from_head_only_overhead() {
  let type_name = "x".repeat(1022);
  let aid = format!("{type_name}-");
  let metadata = HashMap::from([
    ("seq_nr".into(), AttributeValue::N("1".into())),
    ("occurred_at".into(), AttributeValue::N("-1".into())),
    ("manifest".into(), AttributeValue::S(String::new())),
    ("payload".into(), AttributeValue::B(Blob::new(vec![0; 408002]))),
  ]);
  let mut journal = metadata.clone();
  journal.insert("aid".into(), AttributeValue::S(aid.clone()));
  let head = HashMap::from([
    ("aid".into(), AttributeValue::S(aid)),
    ("type_name".into(), AttributeValue::S(type_name)),
    ("seq_nr".into(), AttributeValue::N("1".into())),
    ("events".into(), AttributeValue::L(vec![AttributeValue::M(metadata)])),
  ]);
  assert!(item_size_upper_bound(&journal) <= ITEM_SIZE_LIMIT);
  assert!(item_size_upper_bound(&head) > ITEM_SIZE_LIMIT);
}
