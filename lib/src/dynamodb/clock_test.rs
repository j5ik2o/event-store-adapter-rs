use super::*;

#[test]
fn should_return_the_current_epoch_seconds() {
  let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
  let actual = SystemClock.now_epoch_seconds();
  let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
  assert!((before..=after).contains(&actual));
}
