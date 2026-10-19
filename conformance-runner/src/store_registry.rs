//! 場面の中の、参照名から引けるストアの表。
//!
//! 参照名の規則（`conformance/README.md` の `store` と `createStore`）を持つのは、ここだけにする。
//! メモリと SQLite の実行器は、この表で名前を引き、`createStore` の引数を検査してから、保存先に固有の部分
//! （保存先の作成・共有、ファイルの開き直し）を行う。

use std::collections::BTreeMap;

use serde_json::Value;

use crate::case::{required, string};

/// 1 つ目のストアの予約名を表す。`store` を省略した操作はこのストアに対する操作になる。
const PRIMARY: &str = "primary";

/// `createStore` が求める、保存先の用意の仕方を表す。
pub(crate) enum StorageRequest<'a, S> {
  /// 空の保存先を新しく作る。
  New,
  /// 表にある `shared_with` のストアと共有する。
  Shared(&'a S),
}

/// 検査を通った `createStore` の引数を表す。
pub(crate) struct CreateRequest<'a, S> {
  pub(crate) name: String,
  pub(crate) storage: StorageRequest<'a, S>,
}

/// 参照名から引けるストアの表を表す。1 つ目のストアは、必ず予約名 `primary` で引ける。
pub(crate) struct StoreRegistry<S> {
  primary: S,
  created: BTreeMap<String, S>,
}

impl<S> StoreRegistry<S> {
  /// 1 つ目のストアを予約名 `primary` として持つ表を作る。
  pub(crate) fn new(primary: S) -> Self {
    Self {
      primary,
      created: BTreeMap::new(),
    }
  }

  /// 1 つ目のストアを返す。
  pub(crate) fn primary(&self) -> &S {
    &self.primary
  }

  fn get(&self, name: &str) -> Option<&S> {
    if name == PRIMARY {
      Some(&self.primary)
    } else {
      self.created.get(name)
    }
  }

  /// 操作の `store` が指すストアを返す。省略時は 1 つ目のストア。生成前の名前は誤りとして返す。
  pub(crate) fn select(&self, step: &Value) -> Result<&S, String> {
    let name = match step.get("store") {
      Some(name) => string(name)?,
      None => PRIMARY,
    };
    self
      .get(name)
      .ok_or_else(|| format!("ストア {name} はまだ生成していない"))
  }

  /// `createStore` の引数を検査する。表は変えない。
  ///
  /// 予約名・使用済みの名前、生成前の `shared_with`、未知の `storage` は誤りとして返す。
  /// 検査を通った名前のストアは、保存先を用意してから [`StoreRegistry::insert`] で加える。
  pub(crate) fn parse_create(&self, arguments: &Value) -> Result<CreateRequest<'_, S>, String> {
    let name = string(required(arguments, "name")?)?.to_owned();
    if self.get(&name).is_some() {
      return Err(format!("ストア名 {name} は予約名か、生成済みの名前である"));
    }
    let storage = match string(required(arguments, "storage")?)? {
      "new" => StorageRequest::New,
      "shared" => {
        let shared_with = string(required(arguments, "shared_with")?)?;
        let shared = self
          .get(shared_with)
          .ok_or_else(|| format!("shared_with のストア {shared_with} はまだ生成していない"))?;
        StorageRequest::Shared(shared)
      }
      other => return Err(format!("未知の storage: {other}")),
    };
    Ok(CreateRequest { name, storage })
  }

  /// [`StoreRegistry::parse_create`] で検査した名前のストアを、表へ加える。
  pub(crate) fn insert(&mut self, name: String, store: S) {
    self.created.insert(name, store);
  }
}

#[cfg(test)]
#[path = "store_registry_test.rs"]
mod tests;
