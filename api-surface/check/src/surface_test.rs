use crate::surface::TypeExpr;

fn expr(name: &str, args: Vec<TypeExpr>) -> TypeExpr {
  TypeExpr {
    name: name.to_string(),
    args,
  }
}

#[test]
fn should_parse_a_plain_name() {
  assert_eq!(TypeExpr::parse("seq_nr"), Ok(expr("seq_nr", vec![])));
}

#[test]
fn should_parse_nested_type_arguments() {
  let parsed = TypeExpr::parse("optional<snapshot_envelope<A>>");

  assert_eq!(
    parsed,
    Ok(expr(
      "optional",
      vec![expr("snapshot_envelope", vec![expr("A", vec![])])]
    ))
  );
}

#[test]
fn should_reject_an_unclosed_type_argument_list() {
  assert!(TypeExpr::parse("list<event_envelope<P>").is_err());
}

#[test]
fn should_reject_trailing_text() {
  assert!(TypeExpr::parse("none>").is_err());
}
