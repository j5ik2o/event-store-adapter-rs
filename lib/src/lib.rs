//! Event stores for Memory, DynamoDB, SQLite, Spanner, and Bigtable using the shared event envelope contract.
//! Legacy v3 DynamoDB, SQLite, and Bigtable data is supported only by the feature-gated migration module
//! (features `migration`, `sqlite-migration`, and `bigtable-migration`).

pub mod aggregate_id;
#[cfg(feature = "bigtable")]
pub mod bigtable;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod error;
pub mod event_envelope;
pub mod event_store;
pub mod memory;
#[cfg(any(feature = "migration", feature = "sqlite-migration", feature = "bigtable-migration"))]
pub mod migration;
pub mod retention;
pub mod seq_nr;
pub mod serializer;
#[cfg(feature = "spanner")]
pub mod spanner;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(any(feature = "bigtable", feature = "spanner"))]
mod bearer_token;
mod generic_event_store;
#[cfg(any(feature = "bigtable", feature = "spanner"))]
mod key_hash;
mod storage_backend;

pub use aggregate_id::{AggregateId, AidString};
#[cfg(feature = "bigtable")]
pub use bigtable::{
  initialize_bigtable_store, initialize_bigtable_store_with_token_source, BigtableConnection, BigtableOptions,
  BigtableTables, BigtableTokenSource, EventStoreForBigtable,
};
#[cfg(feature = "dynamodb")]
pub use dynamodb::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
pub use error::{ConfigurationReason, ContractRule, EventStoreError, SerializationPhase, StorageOperation};
pub use event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
pub use event_store::EventStore;
pub use memory::{EventStoreForMemory, MemoryStorage};
#[cfg(feature = "sqlite-migration")]
pub use migration::migrate_v3_sqlite;
#[cfg(feature = "bigtable-migration")]
pub use migration::{migrate_v3_bigtable, LegacyBigtableTables};
#[cfg(feature = "migration")]
pub use migration::{migrate_v3_dynamodb, LegacyDynamoDbTables};
#[cfg(any(feature = "migration", feature = "sqlite-migration", feature = "bigtable-migration"))]
pub use migration::{MigrationError, MigrationRejection, MigrationReport};
pub use retention::{select_expired_history, ttl_expires_epoch_seconds, RetentionMode, RetentionSettings};
pub use seq_nr::{SeqNr, SEQ_NR_MAX};
pub use serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};
#[cfg(feature = "spanner")]
pub use spanner::{
  initialize_spanner_store, EventStoreForSpanner, SpannerConnection, SpannerOptions, SpannerRetention, SpannerTables,
  SpannerTokenSource,
};
#[cfg(feature = "sqlite")]
pub use sqlite::{initialize_sqlite_store, EventStoreForSqlite, SqliteOptions};

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
