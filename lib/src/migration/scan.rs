use aws_sdk_dynamodb::Client;

use super::{MigrationError, MigrationReport};
use crate::dynamodb::items::Item;

pub(super) struct Scan<'a> {
  client: &'a Client,
  table: &'a str,
  exclusive_start_key: Option<Item>,
  finished: bool,
}

impl<'a> Scan<'a> {
  pub fn new(client: &'a Client, table: &'a str) -> Self {
    Self {
      client,
      table,
      exclusive_start_key: None,
      finished: false,
    }
  }

  pub async fn next(&mut self, report: &MigrationReport) -> Result<Option<Vec<Item>>, MigrationError> {
    if self.finished {
      return Ok(None);
    }
    let page = self
      .client
      .scan()
      .table_name(self.table)
      .consistent_read(true)
      .set_exclusive_start_key(self.exclusive_start_key.take())
      .send()
      .await
      .map_err(|error| MigrationError::new(report, format!("Scan {}", self.table), error))?;
    self.exclusive_start_key = page.last_evaluated_key.filter(|key| !key.is_empty());
    self.finished = self.exclusive_start_key.is_none();
    Ok(Some(page.items.unwrap_or_default()))
  }
}

#[cfg(test)]
#[path = "scan_test.rs"]
mod tests;
