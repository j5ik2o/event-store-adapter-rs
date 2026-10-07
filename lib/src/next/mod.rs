//! 次のメジャー版の中核。旧 API との共存のため `#[doc(hidden)]` で公開し、PR 20 で直下へ移す。

pub mod aggregate_id;
pub mod error;
pub mod event_envelope;
pub mod event_store;
pub mod retention;
pub mod seq_nr;
pub mod serializer;

pub(crate) mod generic_event_store;
pub(crate) mod storage_backend;

#[cfg(test)]
mod aggregate_id_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod event_envelope_test;
#[cfg(test)]
mod generic_event_store_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod seq_nr_test;
#[cfg(test)]
mod serializer_test;
#[cfg(test)]
mod storage_backend_test;
