use super::*;

#[test]
fn should_replay_creation_and_rename_without_a_snapshot() {
  let id = UserAccountId::new("1".into());
  let events = [
    UserAccountEvent::Created { name: "first".into() },
    UserAccountEvent::Renamed { name: "second".into() },
  ];
  let state = UserAccount::replay_from(&id, None, events).unwrap();
  assert_eq!(state, UserAccount::new(id, "second".into()).0);
}

#[test]
fn should_replay_differential_events_from_a_snapshot() {
  let id = UserAccountId::new("1".into());
  let snapshot = UserAccount::new(id.clone(), "first".into()).0;
  let state = UserAccount::replay_from(
    &id,
    Some(snapshot),
    [UserAccountEvent::Renamed { name: "second".into() }],
  )
  .unwrap();
  assert_eq!(state, UserAccount::new(id, "second".into()).0);
}

#[test]
fn should_keep_a_snapshot_without_differential_events() {
  let id = UserAccountId::new("1".into());
  let snapshot = UserAccount::new(id.clone(), "first".into()).0;
  assert_eq!(
    UserAccount::replay_from(&id, Some(snapshot.clone()), []).unwrap(),
    snapshot
  );
}

#[test]
fn should_reject_replay_without_a_creation_event() {
  let id = UserAccountId::new("1".into());
  assert!(matches!(
    UserAccount::replay_from(&id, None, []),
    Err(UserAccountError::InvalidReplay(_))
  ));
  assert!(matches!(
    UserAccount::replay_from(&id, None, [UserAccountEvent::Renamed { name: "first".into() }]),
    Err(UserAccountError::InvalidReplay(_))
  ));
}

#[test]
fn should_reject_duplicate_creation_during_replay() {
  let id = UserAccountId::new("1".into());
  let (snapshot, created) = UserAccount::new(id.clone(), "first".into());
  assert!(matches!(
    UserAccount::replay_from(&id, Some(snapshot), [created]),
    Err(UserAccountError::InvalidReplay(_))
  ));
}
