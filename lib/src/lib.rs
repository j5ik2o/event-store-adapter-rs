//! Event stores for Memory and DynamoDB using the shared event envelope contract.
//! Legacy DynamoDB data is supported only by the feature-gated migration module.

pub mod aggregate_id;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod error;
pub mod event_envelope;
pub mod event_store;
pub mod memory;
#[cfg(feature = "migration")]
pub mod migration;
pub mod retention;
pub mod seq_nr;
pub mod serializer;

mod generic_event_store;
mod storage_backend;

pub use aggregate_id::{AggregateId, AidString};
#[cfg(feature = "dynamodb")]
pub use dynamodb::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
pub use error::{ConfigurationReason, ContractRule, EventStoreError, SerializationPhase, StorageOperation};
pub use event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
pub use event_store::EventStore;
pub use memory::{EventStoreForMemory, MemoryStorage};
#[cfg(feature = "migration")]
pub use migration::{migrate_v3_dynamodb, LegacyDynamoDbTables, MigrationError, MigrationRejection, MigrationReport};
pub use retention::{select_expired_history, ttl_expires_epoch_seconds, RetentionMode, RetentionSettings};
pub use seq_nr::{SeqNr, SEQ_NR_MAX};
pub use serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};

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
