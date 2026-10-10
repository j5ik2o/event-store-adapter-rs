#![cfg(feature = "migration")]

#[path = "dynamodb_migration_test/support.rs"]
mod support;

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue as A};
use event_store_adapter_rs::migrate_v3_dynamodb;
use serde_json::{json, Value};
use std::collections::HashMap;
use support::*;

fn mapping() -> HashMap<String, String> {
  HashMap::from([("Old-Account".into(), "Account".into())])
}

fn assert_no_old_writes(traces: &[Value], fixture: &Fixture) {
  for trace in traces {
    if trace["input"]["TableName"] == fixture.legacy.journal_table_name
      || trace["input"]["TableName"] == fixture.legacy.snapshot_table_name
    {
      assert_eq!(trace["api"], "Scan");
      assert_eq!(trace["input"]["ConsistentRead"], true);
      assert!(trace["input"].get("IndexName").is_none());
    }
  }
}

#[tokio::test]
async fn should_migrate_current_history_marked_history_and_read_raw_payloads_publicly() {
  let fixture = Fixture::new().await;
  let events = vec![
    raw_event(
      "Old-Account-7",
      "Old-Account-value-with-hyphen-1",
      "1",
      vec![255, 0, 128],
      None,
    ),
    raw_event(
      "Old-Account-7",
      "Old-Account-value-with-hyphen-2",
      "2",
      vec![1, 254],
      Some("e\u{301}🙂"),
    ),
    raw_event(
      "Old-Account-7",
      "Old-Account-value-with-hyphen-3",
      "3",
      vec![3, 0],
      Some("last-event"),
    ),
    raw_event("Other-2", "Other-second-1", "1", vec![9, 255], None),
  ];
  let snapshots = vec![
    raw_snapshot(
      "Old-Account-7",
      "Old-Account-value-with-hyphen-0",
      "2",
      vec![255, 22],
      "0",
    ),
    raw_snapshot(
      "Old-Account-7",
      "Old-Account-value-with-hyphen-2",
      "2",
      vec![255, 22],
      "0",
    ),
    raw_snapshot(
      "Old-Account-7",
      "Old-Account-value-with-hyphen-1",
      "1",
      vec![255, 11],
      "4102444860",
    ),
  ];
  fixture.seed(&events, &snapshots).await;
  let old_before = fixture.old_items().await;
  let observed = Observed::new(&fixture.endpoint, &fixture.raw);
  let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &mapping())
    .await
    .unwrap();
  assert!(report.reasons.is_empty());
  assert_eq!((report.aggregates, report.events, report.snapshots), (2, 4, 3));
  let aid = "Account-value-with-hyphen";
  let mut actual_events = Vec::new();
  for seq in ["1", "2", "3"] {
    let item = get(
      &fixture.raw,
      &fixture.tables.journal_table_name,
      aid,
      Some(("seq_nr", seq)),
    )
    .await;
    assert_shapes(
      &item,
      &[
        ("aid", "S"),
        ("seq_nr", "N"),
        ("occurred_at", "N"),
        ("manifest", "S"),
        ("payload", "B"),
      ],
    );
    assert_eq!(item["occurred_at"], A::N("-876543211".into()));
    actual_events.push(wire(&item));
  }
  assert_eq!(actual_events[0]["manifest"], json!({"S":""}));
  assert_eq!(actual_events[1]["manifest"], json!({"S":"e\u{301}🙂"}));
  let head = get(&fixture.raw, &fixture.tables.head_table_name, aid, None).await;
  assert_shapes(
    &head,
    &[("aid", "S"), ("type_name", "S"), ("seq_nr", "N"), ("events", "L")],
  );
  assert_eq!(head["seq_nr"], A::N("3".into()));
  let head_events = head["events"].as_l().unwrap();
  assert_eq!(head_events.len(), 1);
  let last = head_events[0].as_m().unwrap();
  assert_shapes(
    last,
    &[
      ("seq_nr", "N"),
      ("occurred_at", "N"),
      ("manifest", "S"),
      ("payload", "B"),
    ],
  );
  assert_eq!(last["payload"], A::B(Blob::new([3, 0])));
  let mut actual_snapshots = Vec::new();
  for (seq, kind) in [("0", "current"), ("2", "active"), ("1", "marked")] {
    let item = get(
      &fixture.raw,
      &fixture.tables.snapshot_table_name,
      aid,
      Some(("skey", seq)),
    )
    .await;
    let mut shape = vec![
      ("aid", "S"),
      ("skey", "N"),
      ("seq_nr", "N"),
      ("manifest", "S"),
      ("payload", "B"),
      ("last_updated_at", "N"),
    ];
    if kind == "active" {
      shape.push(("active_history_seq_nr", "N"));
    }
    if kind == "marked" {
      shape.push(("ttl", "N"));
    }
    assert_shapes(&item, &shape);
    assert_eq!(item["manifest"], A::S("".into()));
    assert_eq!(item["last_updated_at"], A::N("-877".into()));
    if kind == "current" {
      assert_eq!(item["seq_nr"], A::N("2".into()));
    }
    if kind == "active" {
      assert_eq!(item["active_history_seq_nr"], A::N("2".into()));
    }
    if kind == "marked" {
      assert_eq!(item["ttl"], A::N("4102444860".into()));
    }
    actual_snapshots.push(wire(&item));
  }
  let store = fixture.open().await;
  let id = Id("Account".into(), "value-with-hyphen".into());
  let read = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(read.len(), 3);
  for (event, seq, bytes, manifest) in [
    (&read[0], 1, vec![255, 0, 128], ""),
    (&read[1], 2, vec![1, 254], "e\u{301}🙂"),
    (&read[2], 3, vec![3, 0], "last-event"),
  ] {
    assert_eq!(event.aggregate_id(), &id);
    assert_eq!(event.seq_nr(), seq);
    assert_eq!(event.payload(), &bytes);
    assert_eq!(event.manifest(), manifest);
    assert_eq!(event.occurred_at().timestamp_nanos_opt(), Some(-876543211));
  }
  let read_snapshot = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(read_snapshot.head_seq_nr(), 3);
  let snapshot = read_snapshot.snapshot().unwrap();
  assert_eq!(snapshot.seq_nr(), 2);
  assert_eq!(snapshot.aggregate(), &vec![255, 22]);
  assert_eq!(snapshot.manifest(), "");
  let second = store
    .get_events_by_id_since_seq_nr(&Id("Other".into(), "second".into()), 0)
    .await
    .unwrap();
  assert_eq!(second[0].payload(), &vec![9, 255]);
  assert_eq!(old_before, fixture.old_items().await);
  let traces = observed.traces.lock().unwrap().clone();
  assert_no_old_writes(&traces, &fixture);
  let first_put = traces.iter().position(|trace| trace["api"] == "PutItem").unwrap();
  let inspected_snapshot = traces
    .iter()
    .position(|trace| trace["api"] == "Scan" && trace["input"]["TableName"] == fixture.legacy.snapshot_table_name)
    .unwrap();
  assert!(inspected_snapshot < first_put);
  for trace in traces.iter().filter(|trace| trace["api"] == "PutItem") {
    assert_eq!(trace["input"]["ConditionExpression"], "attribute_not_exists(aid)");
  }
  let gets: Vec<_> = traces.iter().filter(|trace| trace["api"] == "GetItem").collect();
  assert_eq!(gets.len(), 2);
  let max_get = gets
    .iter()
    .find(|trace| trace["input"]["Key"]["aid"]["S"] == aid)
    .unwrap();
  assert_eq!(max_get["input"]["ConsistentRead"], true);
  assert_eq!(max_get["input"]["Key"]["seq_nr"]["N"], "3");
  evidence(
    "migration-success",
    json!({"original_old_journal":events.iter().map(wire).collect::<Vec<_>>(),"original_old_snapshot":snapshots.iter().map(wire).collect::<Vec<_>>(),"report":report,"requests_and_actual_responses":traces,"actual_new_journal":actual_events,"actual_new_snapshot":actual_snapshots,"actual_head":wire(&head),"public_read":{"seq_nrs":read.iter().map(|event|event.seq_nr()).collect::<Vec<_>>(),"head_seq_nr":read_snapshot.head_seq_nr(),"snapshot_seq_nr":snapshot.seq_nr()},"old_unchanged":true}),
  );
  let rerun = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &mapping())
    .await
    .unwrap();
  assert!(!rerun.reasons.is_empty());
  assert_eq!((rerun.aggregates, rerun.events, rerun.snapshots), (0, 0, 0));
}

#[tokio::test]
async fn should_list_all_rejections_and_send_no_data_or_modify_old_tables() {
  let fixture = Fixture::new().await;
  let events = vec![
    raw_event("Gap-2", "Gap-a-1", "1", vec![0], None),
    raw_event("Gap-2", "Gap-a-3", "3", vec![3], None),
    raw_event("Over-1", "Over-a-9007199254740992", "9007199254740992", vec![0], None),
    raw_event("Old-Account-7", "Old-Account-b-1", "1", vec![0], None),
    raw_event("Mapped-Old-2", "Mapped-Old-a-1", "1", vec![0], None),
    raw_event("Bad-2", "Wrong-a-1", "1", vec![0], None),
    raw_event("Huge-1", "Huge-a-1", "1", vec![0; 409479], None),
    raw_event("Wide-Old-2", "Wide-Old-a-1", "1", vec![0], None),
  ];
  let snapshots = vec![
    raw_snapshot("Alone-9", "Alone-a-0", "1", vec![1], "0"),
    raw_snapshot("Gap-2", "Gap-a-0", "4", vec![4], "0"),
    raw_snapshot("Gap-2", "Gap-a-5", "5", vec![5], "0"),
  ];
  fixture.seed(&events, &snapshots).await;
  let old_before = fixture.old_items().await;
  let observed = Observed::new(&fixture.endpoint, &fixture.raw);
  let bad_mapping = HashMap::from([
    ("Mapped-Old".into(), "Still-Hyphen".into()),
    ("Wide-Old".into(), "あ".repeat(342)),
  ]);
  let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &bad_mapping)
    .await
    .unwrap();
  assert_eq!((report.events, report.snapshots, report.aggregates), (0, 0, 0));
  for reason in ["P-22", "P-23", "P-36", "T-9", "T-11", "T-12", "D-7", "孤立snapshot"] {
    assert!(
      report.reasons.iter().any(|rejection| rejection.reason.contains(reason)),
      "missing {reason}: {:?}",
      report
    );
  }
  assert_eq!(
    report
      .reasons
      .iter()
      .filter(|rejection| rejection.reason.contains("未来snapshot"))
      .count(),
    2
  );
  let traces = observed.traces.lock().unwrap().clone();
  assert!(traces.iter().all(|trace| trace["api"] != "PutItem"));
  assert_no_old_writes(&traces, &fixture);
  for table in [
    &fixture.tables.journal_table_name,
    &fixture.tables.snapshot_table_name,
    &fixture.tables.head_table_name,
  ] {
    let actual = scan(&fixture.raw, table).await;
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0]["aid"]["S"], "__config__");
  }
  assert_eq!(old_before, fixture.old_items().await);
  evidence(
    "migration-rejections",
    json!({"original_old_journal":events.iter().map(wire).collect::<Vec<_>>(),"original_old_snapshot":snapshots.iter().map(wire).collect::<Vec<_>>(),"report":report,"requests_and_actual_responses":traces,"old_unchanged":true,"new_data_writes":0}),
  );
}

#[tokio::test]
async fn should_reject_nonempty_data_in_each_new_table() {
  let fixture = Fixture::new().await;
  for (table, sort) in [
    (&fixture.tables.journal_table_name, Some(("seq_nr", "1"))),
    (&fixture.tables.snapshot_table_name, Some(("skey", "0"))),
    (&fixture.tables.head_table_name, None),
  ] {
    let mut item = Item::from([("aid".into(), A::S("Occupied-a".into()))]);
    if let Some((name, value)) = sort {
      item.insert(name.into(), A::N(value.into()));
    }
    fixture
      .raw
      .put_item()
      .table_name(table)
      .set_item(Some(item.clone()))
      .send()
      .await
      .unwrap();
    let observed = Observed::new(&fixture.endpoint, &fixture.raw);
    let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &HashMap::new())
      .await
      .unwrap();
    assert!(report.reasons.iter().any(|rejection| rejection.table == *table));
    assert_eq!(observed.traces.lock().unwrap().len(), 3);
    assert_eq!(get(&fixture.raw, table, "Occupied-a", sort).await, item);
    fixture
      .raw
      .delete_item()
      .table_name(table)
      .set_key(Some(item))
      .send()
      .await
      .unwrap();
  }
}

#[tokio::test]
async fn should_preserve_competing_journal_head_and_snapshot_items_in_the_same_migration() {
  let mut results = Vec::new();
  for target in ["journal", "head", "snapshot"] {
    let fixture = Fixture::new().await;
    fixture
      .seed(
        &[raw_event("Account-7", "Account-a-1", "1", vec![255], None)],
        &[raw_snapshot("Account-7", "Account-a-0", "1", vec![128], "0")],
      )
      .await;
    let old = fixture.old_items().await;
    let (table, sort) = match target {
      "journal" => (&fixture.tables.journal_table_name, Some(("seq_nr", "1"))),
      "head" => (&fixture.tables.head_table_name, None),
      _ => (&fixture.tables.snapshot_table_name, Some(("skey", "0"))),
    };
    let mut rival = Item::from([
      ("aid".into(), A::S("Account-a".into())),
      ("sentinel".into(), A::S("competing item".into())),
    ]);
    if let Some((name, value)) = sort {
      rival.insert(name.into(), A::N(value.into()));
    }
    let observed = Observed::new(&fixture.endpoint, &fixture.raw);
    *observed.install.lock().unwrap() = Some(Install {
      table: table.clone(),
      item: rival.clone(),
    });
    let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &HashMap::new())
      .await
      .unwrap();
    assert_eq!(report.reasons.len(), 1);
    assert_eq!(report.reasons[0].table, *table);
    assert!(report.reasons[0].reason.contains("条件競合"));
    assert!(observed.install.lock().unwrap().is_none());
    assert_eq!(get(&fixture.raw, table, "Account-a", sort).await, rival);
    assert_eq!(old, fixture.old_items().await);
    let traces = observed.traces.lock().unwrap().clone();
    let failed = traces.iter().find(|trace| trace["status"] == 400).unwrap();
    assert_eq!(failed["api"], "PutItem");
    assert!(failed["output"]["__type"]
      .as_str()
      .unwrap()
      .contains("ConditionalCheckFailedException"));
    assert_eq!(
      traces
        .iter()
        .filter(|trace| trace["api"] == "PutItem" && trace["input"]["TableName"] == *table)
        .count(),
      1
    );
    results.push(json!({"target":target,"report":report,"rival":wire(&rival),"requests_and_actual_responses":traces,"old_unchanged":true}));
  }
  evidence("migration-conflicts", json!(results));
}

#[tokio::test]
async fn should_follow_real_scan_pages_twice_for_both_old_tables() {
  let fixture = Fixture::new().await;
  let events: Vec<_> = (1..=10)
    .map(|seq| {
      raw_event(
        "Pages-7",
        &format!("Pages-a-{seq}"),
        &seq.to_string(),
        vec![seq as u8; 300000],
        None,
      )
    })
    .collect();
  let snapshots: Vec<_> = (1..=10)
    .map(|seq| {
      raw_snapshot(
        "Pages-7",
        &format!("Pages-a-{seq}"),
        &seq.to_string(),
        vec![seq as u8; 300000],
        "0",
      )
    })
    .collect();
  fixture.seed(&events, &snapshots).await;
  let before = fixture.old_items().await;
  let observed = Observed::new(&fixture.endpoint, &fixture.raw);
  let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &HashMap::new())
    .await
    .unwrap();
  assert!(report.reasons.is_empty());
  assert_eq!((report.events, report.snapshots, report.aggregates), (10, 10, 1));
  let traces = observed.traces.lock().unwrap().clone();
  for table in [&fixture.legacy.journal_table_name, &fixture.legacy.snapshot_table_name] {
    let requests: Vec<_> = traces
      .iter()
      .filter(|trace| trace["api"] == "Scan" && trace["input"]["TableName"] == *table)
      .collect();
    let mut passes = Vec::new();
    let mut pass = Vec::new();
    for trace in requests {
      assert_eq!(trace["input"]["ConsistentRead"], true);
      assert!(trace["input"].get("IndexName").is_none());
      if pass.is_empty() {
        assert!(trace["input"].get("ExclusiveStartKey").is_none());
      } else {
        let previous: &&Value = pass.last().unwrap();
        assert_eq!(
          trace["input"]["ExclusiveStartKey"],
          previous["output"]["LastEvaluatedKey"]
        );
      }
      pass.push(trace);
      if trace["output"]
        .get("LastEvaluatedKey")
        .is_none_or(|key| key.as_object().unwrap().is_empty())
      {
        passes.push(std::mem::take(&mut pass));
      }
    }
    assert!(pass.is_empty());
    assert_eq!(passes.len(), 2);
    for pass in passes {
      assert!(pass.len() > 1);
      assert_eq!(
        pass
          .iter()
          .map(|trace| trace["output"]["Items"].as_array().unwrap().len())
          .sum::<usize>(),
        10
      );
    }
  }
  let first_write = traces.iter().position(|trace| trace["api"] == "PutItem").unwrap();
  let inspected_snapshot_pages: Vec<_> = traces[..first_write]
    .iter()
    .filter(|trace| trace["api"] == "Scan" && trace["input"]["TableName"] == fixture.legacy.snapshot_table_name)
    .collect();
  assert!(inspected_snapshot_pages.len() > 1);
  assert!(inspected_snapshot_pages.last().unwrap()["output"]
    .get("LastEvaluatedKey")
    .is_none());
  assert_eq!(before, fixture.old_items().await);
  let store = fixture.open().await;
  let read = store
    .get_events_by_id_since_seq_nr(&Id("Pages".into(), "a".into()), 0)
    .await
    .unwrap();
  assert_eq!(read.len(), 10);
  for (index, event) in read.iter().enumerate() {
    assert_eq!(event.seq_nr(), index as u64 + 1);
    assert_eq!(event.payload(), &vec![index as u8 + 1; 300000]);
  }
  evidence(
    "migration-real-pages",
    json!({"report":report,"requests_and_actual_responses":traces,"public_read_seq_nrs":read.iter().map(|event|event.seq_nr()).collect::<Vec<_>>(),"old_unchanged":true}),
  );
}

#[tokio::test]
async fn should_reject_colliding_mapping_or_multiple_old_partitions_before_transfer() {
  let fixture = Fixture::new().await;
  fixture
    .seed(
      &[
        raw_event("Account-1", "Account-a-1", "1", vec![1], None),
        raw_event("Old-Account-7", "Old-Account-a-1", "1", vec![2], None),
      ],
      &[],
    )
    .await;
  let observed = Observed::new(&fixture.endpoint, &fixture.raw);
  let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &mapping())
    .await
    .unwrap();
  assert!(report
    .reasons
    .iter()
    .any(|rejection| rejection.reason.contains("複数の旧パーティション")));
  assert!(observed
    .traces
    .lock()
    .unwrap()
    .iter()
    .all(|trace| trace["api"] != "PutItem"));
}

#[tokio::test]
async fn should_collect_orphan_snapshot_rejections_across_all_real_pages_before_writing() {
  let fixture = Fixture::new().await;
  let snapshots: Vec<_> = (1..=10)
    .map(|seq| {
      raw_snapshot(
        "Orphan-7",
        &format!("Orphan-a-{seq}"),
        &seq.to_string(),
        vec![seq as u8; 300000],
        "0",
      )
    })
    .collect();
  fixture.seed(&[], &snapshots).await;
  let before = fixture.old_items().await;
  let observed = Observed::new(&fixture.endpoint, &fixture.raw);
  let report = migrate_v3_dynamodb(&observed.client, &fixture.legacy, &fixture.tables, &HashMap::new())
    .await
    .unwrap();
  assert_eq!(report.reasons.len(), 10);
  assert!(report
    .reasons
    .iter()
    .all(|rejection| rejection.reason.contains("孤立snapshot")));
  assert_eq!((report.events, report.snapshots, report.aggregates), (0, 0, 0));
  let traces = observed.traces.lock().unwrap().clone();
  let pages: Vec<_> = traces
    .iter()
    .filter(|trace| trace["api"] == "Scan" && trace["input"]["TableName"] == fixture.legacy.snapshot_table_name)
    .collect();
  assert!(pages.len() > 1);
  assert_eq!(
    pages
      .iter()
      .map(|trace| trace["output"]["Items"].as_array().unwrap().len())
      .sum::<usize>(),
    10
  );
  assert!(pages.last().unwrap()["output"].get("LastEvaluatedKey").is_none());
  assert!(traces.iter().all(|trace| trace["api"] != "PutItem"));
  assert_eq!(before, fixture.old_items().await);
  evidence(
    "migration-paged-rejections",
    json!({"report":report,"requests_and_actual_responses":traces,"old_unchanged":true,"new_data_writes":0}),
  );
}
