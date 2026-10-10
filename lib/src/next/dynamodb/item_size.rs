use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

pub(crate) const ITEM_SIZE_LIMIT: usize = 409600;

// DynamoDBの最大38桁と符号を含む固定上界。実際の数値表現の長さに依存しない。
const NUMBER_MAX_BYTES: usize = 21;

/// 属性名・値・入れ子のオーバーヘッドから、保存項目のサイズ上界を返す（D-7）。
pub(crate) fn item_size_upper_bound(item: &HashMap<String, AttributeValue>) -> usize {
  item.iter().fold(0usize, |size, (name, value)| {
    size.saturating_add(name.len()).saturating_add(value_size(value))
  })
}

fn value_size(value: &AttributeValue) -> usize {
  match value {
    AttributeValue::S(value) => value.len(),
    AttributeValue::N(_) => NUMBER_MAX_BYTES,
    AttributeValue::B(value) => value.as_ref().len(),
    AttributeValue::L(values) => values.iter().fold(3usize, |size, value| {
      size.saturating_add(1).saturating_add(value_size(value))
    }),
    AttributeValue::M(values) => item_size_upper_bound(values)
      .saturating_add(3)
      .saturating_add(values.len()),
    AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
    AttributeValue::Ss(values) => values
      .iter()
      .fold(0usize, |size, value| size.saturating_add(value.len())),
    AttributeValue::Ns(values) => values.len().saturating_mul(NUMBER_MAX_BYTES),
    AttributeValue::Bs(values) => values
      .iter()
      .fold(0usize, |size, value| size.saturating_add(value.as_ref().len())),
    // 将来のSDK属性を計数できない場合、上界を小さくして送信を許可しない。
    _ => usize::MAX,
  }
}

#[cfg(test)]
#[path = "item_size_test.rs"]
mod tests;
