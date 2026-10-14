//! 公開 API の形 (共通契約 v5、`api-surface/surface.json`) を、rs の公開 API と照合する。
//!
//! 対応表 `api-surface/rs.json` を読み、固定日の nightly が出力する rustdoc の JSON と照合して、
//! ずれを `<場所>: <メッセージ>` の形の `Problem` で返す。使い方は `api-surface/README.md` を参照。

mod check;
mod mapping;
mod render;
mod rustdoc;
mod surface;

pub use check::{check_api, Problem};
pub use mapping::Mapping;
pub use rustdoc::document_library;
pub use surface::Surface;

#[cfg(test)]
mod rustdoc_test;
#[cfg(test)]
mod surface_test;
