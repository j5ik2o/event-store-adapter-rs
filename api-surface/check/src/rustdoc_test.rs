use rustdoc_types::FORMAT_VERSION;

use crate::rustdoc::{parse_rustdoc_json, TOOLCHAIN};

#[test]
fn should_reject_a_json_with_another_format_version_and_say_how_to_fix_it() {
  let json = format!("{{\"format_version\": {}}}", FORMAT_VERSION + 1);

  let error = parse_rustdoc_json(json.as_bytes()).expect_err("形式の版が違う JSON は読まない");

  assert!(
    error.contains(&format!("形式の版が {}", FORMAT_VERSION + 1)) && error.contains(TOOLCHAIN),
    "食い違う版と nightly の日付を案内する: {error}"
  );
}

#[test]
fn should_reject_a_json_without_format_version() {
  let error = parse_rustdoc_json(b"{}").expect_err("format_version がない JSON は読まない");

  assert!(error.contains("format_version がない"), "案内する: {error}");
}

#[test]
fn should_reject_something_that_is_not_json() {
  let error = parse_rustdoc_json(b"not json").expect_err("JSON でないものは読まない");

  assert!(error.contains("rustdoc の JSON を読めない"), "案内する: {error}");
}
