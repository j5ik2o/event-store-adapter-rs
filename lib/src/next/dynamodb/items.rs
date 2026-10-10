use std::collections::HashMap;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;

use crate::next::aggregate_id::AidString;
use crate::next::seq_nr::SeqNr;

pub(crate) type Item = HashMap<String, AttributeValue>;

/// 検査済みのメタデータと直列化済みpayloadを保持する。
pub(crate) struct StoredEvent {
  pub seq_nr: SeqNr,
  pub occurred_at: String,
  pub manifest: String,
  pub payload: Blob,
}

impl StoredEvent {
  fn metadata(&self) -> Item {
    HashMap::from([
      ("seq_nr".into(), AttributeValue::N(self.seq_nr.to_string())),
      ("occurred_at".into(), AttributeValue::N(self.occurred_at.clone())),
      ("manifest".into(), AttributeValue::S(self.manifest.clone())),
      ("payload".into(), AttributeValue::B(self.payload.clone())),
    ])
  }

  pub(crate) fn journal(&self, aid: &AidString) -> Item {
    let mut item = self.metadata();
    item.insert("aid".into(), AttributeValue::S(aid.as_str().into()));
    item
  }

  pub(crate) fn head(&self, aid: &AidString) -> Item {
    let (type_name, _) = aid.as_str().split_once('-').expect("検査済みaidの区切り");
    HashMap::from([
      ("aid".into(), AttributeValue::S(aid.as_str().into())),
      ("type_name".into(), AttributeValue::S(type_name.into())),
      ("seq_nr".into(), AttributeValue::N(self.seq_nr.to_string())),
      (
        "events".into(),
        AttributeValue::L(vec![AttributeValue::M(self.metadata())]),
      ),
    ])
  }
}

pub(crate) enum SnapshotKind {
  Current,
  History { ttl: Option<String> },
}

pub(crate) struct StoredSnapshot {
  pub seq_nr: SeqNr,
  pub manifest: String,
  pub payload: Blob,
  pub last_updated_at: String,
}

impl StoredSnapshot {
  pub(crate) fn item(&self, aid: &AidString, kind: SnapshotKind) -> Item {
    let mut item = HashMap::from([
      ("aid".into(), AttributeValue::S(aid.as_str().into())),
      ("skey".into(), AttributeValue::N("0".into())),
      ("seq_nr".into(), AttributeValue::N(self.seq_nr.to_string())),
      ("manifest".into(), AttributeValue::S(self.manifest.clone())),
      ("payload".into(), AttributeValue::B(self.payload.clone())),
      (
        "last_updated_at".into(),
        AttributeValue::N(self.last_updated_at.clone()),
      ),
    ]);
    if let SnapshotKind::History { ttl } = kind {
      item.insert("skey".into(), AttributeValue::N(self.seq_nr.to_string()));
      match ttl {
        Some(ttl) => {
          item.insert("ttl".into(), AttributeValue::N(ttl));
        }
        None => {
          item.insert(
            "active_history_seq_nr".into(),
            AttributeValue::N(self.seq_nr.to_string()),
          );
        }
      }
    }
    item
  }
}

#[cfg(test)]
#[path = "items_test.rs"]
mod tests;
