use serde_json::json;

use super::*;

/// 1 つ目と、生成済みの `second` を持つ表。中身は、どのストアかを区別できる文字列にする。
fn registry_with_second() -> StoreRegistry<&'static str> {
  let mut registry = StoreRegistry::new("primary-store");
  registry.insert("second".to_owned(), "second-store");
  registry
}

#[test]
fn should_select_the_primary_store_when_the_step_has_no_store_and_the_named_store_when_it_has() {
  let registry = registry_with_second();

  assert_eq!(registry.select(&json!({"op": "read"})), Ok(&"primary-store"));
  assert_eq!(registry.select(&json!({"store": "primary"})), Ok(&"primary-store"));
  assert_eq!(registry.select(&json!({"store": "second"})), Ok(&"second-store"));
}

#[test]
fn should_fail_to_select_a_store_that_was_not_created_without_falling_back_to_the_primary() {
  let registry = registry_with_second();

  assert!(registry.select(&json!({"store": "third"})).is_err());
}

#[test]
fn should_fail_to_parse_a_create_with_a_name_already_created_and_keep_the_created_store() {
  let registry = registry_with_second();

  let result = registry.parse_create(&json!({"name": "second", "storage": "new"}));

  assert!(result.is_err());
  assert_eq!(registry.select(&json!({"store": "second"})), Ok(&"second-store"));
}

#[test]
fn should_fail_to_parse_a_create_with_the_reserved_name() {
  let registry = registry_with_second();

  let result = registry.parse_create(&json!({"name": "primary", "storage": "new"}));

  assert!(result.is_err());
}

#[test]
fn should_parse_a_shared_create_to_the_store_named_by_shared_with() {
  let registry = registry_with_second();

  let request = registry
    .parse_create(&json!({"name": "third", "storage": "shared", "shared_with": "second"}))
    .unwrap();

  assert_eq!(request.name, "third");
  assert!(matches!(request.storage, StorageRequest::Shared(&"second-store")));
}

#[test]
fn should_parse_a_new_create_with_the_given_name() {
  let registry = registry_with_second();

  let request = registry
    .parse_create(&json!({"name": "third", "storage": "new"}))
    .unwrap();

  assert_eq!(request.name, "third");
  assert!(matches!(request.storage, StorageRequest::New));
}

#[test]
fn should_fail_to_parse_a_shared_create_whose_shared_with_is_not_created_without_falling_back_to_the_primary() {
  let registry = StoreRegistry::new("primary-store");

  let result = registry.parse_create(&json!({"name": "second", "storage": "shared", "shared_with": "ghost"}));

  assert!(result.is_err());
}

#[test]
fn should_fail_to_parse_a_create_with_an_unknown_storage_and_not_add_the_store() {
  let registry = StoreRegistry::new("primary-store");

  let result = registry.parse_create(&json!({"name": "second", "storage": "copy"}));

  assert!(result.is_err());
  assert!(registry.select(&json!({"store": "second"})).is_err());
}
