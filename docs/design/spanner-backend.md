# Design of the Spanner storage

> This document is the design record for the Spanner storage (feature `spanner`). For current usage, see the [README](../../README.md) and [the Spanner schema](../SPANNER_DATABASE_SCHEMA.md).

This document records the data shapes, the module ownership, the design decisions, and the evidence for the rules that the shared conformance data cannot express. It does not change or add rules.

## 1. Scope and sources

- Normative sources, in the hub repository (`j5ik2o/event-store-adapter`): the common contract v5 (`docs/spec/core-contract.md`), the Spanner profile (`docs/spec/storage/spanner.md`, rules `SPN-1` to `SPN-17`, decisions `SPN-D1a` to `SPN-D8`), the hash specification (`docs/spec/storage/hash.md`, rules `K-1` and `K-2`), and the ADRs.
- The conformance test data is `conformance/` in this repository, version 1.3.0. `conformance/coverage.json` leaves some rules to each language's tests: `SPN-1`, `SPN-10`, `SPN-12`, and `K-2` are out of scope of the data, and `SP-1` is a documentation obligation. Chapter 4 maps each of them to a test or a document.
- The row shape written and read by the JavaScript implementation (`spanner-persist-event.ts` of `event-store-adapter-js`) is the reference for the stored format. This crate had no Spanner storage before, so the layout is new for Rust and nothing is migrated.
- Not provided in this release: a change feed (`SPN-D8`, `SPN-17`; `SPN-14` and `SPN-15` apply only when a feed is provided), the PostgreSQL dialect (`SPN-D1a`), and a migration tool.

## 2. Public types, modules, and resource ownership

| Item | Where | Role |
|:-----|:------|:-----|
| `EventStoreForSpanner<AID, A, P>` | `lib/src/spanner/mod.rs` | `EventStore` through `GenericEventStore` and `SpannerBackend`; `open`, `open_with_serializers` |
| `initialize_spanner_store` | `lib/src/spanner/mod.rs`, `configuration.rs` | Check, then write the one configuration row |
| `SpannerConnection`, `SpannerTables`, `SpannerOptions`, `SpannerRetention` | `lib/src/spanner/mod.rs` | Connection, the database and prefix, and settings (`retention`, `change_feed`, `transaction_retry_limit`) |
| `SpannerTokenSource` | `lib/src/spanner/auth.rs` | A token supplier that the user implements |
| `ConfigurationReason::Spanner*` (8 variants) | `lib/src/error.rs` | Configuration errors, see the schema document |
| `configuration.rs`, `backend.rs`, `retention.rs` | `lib/src/spanner/` | Open-time check, append and read, retention |
| `client.rs`, `values.rs` | `lib/src/spanner/` | Sessions and transactions; value encoding and the merge of split results |
| `key_hash.rs`, `bearer_token.rs` | `lib/src/` | FNV-1a 64 and the bearer header value, shared with Bigtable |

The `spanner` feature enables `tonic`, the generated Spanner data and database admin gRPC clients (`googleapis-tonic-google-spanner-v1` 0.45.0 and `googleapis-tonic-google-spanner-admin-database-v1` 0.40.0), `prost-types`, `base64`, `tokio` (time only), and `uuid`. It does not enable any other storage or `test-hooks`. CI checks with `cargo tree` that the AWS, SQLite, Bigtable, and Google authentication crates are absent from that graph.

Resource ownership follows the Bigtable storage:

- The user creates and owns the `tonic::transport::Channel` inside `SpannerConnection`. The library creates no channel, depends on no TLS stack and no Google authentication library, and starts no background task.
- Each public operation opens a session, uses it, and deletes it (the deletion is best-effort). There is no session pool. A store and its clones share the channel and an `Arc` of the table names, the validated options, and the `store_id`; dropping a store does no work.
- Every read-write transaction that is begun is committed or rolled back. The emulator accepts one read-write transaction at a time, and a transaction left open makes the following ones abort.

## 3. Design decisions

| # | Decision | Reason | Alternative not taken |
|:--|:---------|:-------|:----------------------|
| D-1 | Call the Spanner gRPC API directly through the generated clients, with the channel owned by the user | Same ownership as Bigtable; no authentication or TLS crate in the dependency graph, which the `feature-matrix` job checks | A Spanner client library with its own channel and authentication. Not adopted because of that ownership; its retry behavior was not evaluated here |
| D-2 | One session per public operation | No state to pool, expire, or recover | A session pool or multiplexed sessions: more state; costs two extra requests per operation here |
| D-3 | Appends are one `Commit` of mutations: journal `Insert`; head `Insert` (with `type_name`) at `seq_nr = 1`, `Update` after; `snapshot_current` `InsertOrUpdate`; `snapshot_history` `Insert` with `expired_at` NULL | The head is read in the same transaction, which decides the optimistic lock (`SPN-6`); `Insert` never overwrites a journal row | DML statements for the append: no benefit, because the decision comes from the head read |
| D-4 | A duplicate-key failure at commit is a storage error | The contract allows an optimistic lock only for a duplicate that is confirmed; the head read already decided every confirmed case | Mapping every `ALREADY_EXISTS` to an optimistic lock |
| D-5 | Retry only ABORTED, with the same envelope and `seq_nr`; the limit does not count the first attempt (default 5); wait 10 ms, doubling up to 1 s | `SPN-7`; the emulator aborts overlapping read-write transactions even without a conflict, so concurrent results would otherwise depend on timing | Retrying other failures: could repeat a commit whose outcome is unknown |
| D-6 | A failure after the commit response (`after_commit` in tests) is a storage error of unknown outcome, never retried or compensated | `SPN-8` | Deleting the rows on failure: unsafe when the commit did happen |
| D-7 | Retention runs after the append commits, in separate SERIALIZABLE transactions with DML: `DELETE`, or `UPDATE ... SET expired_at = CURRENT_TIMESTAMP() ... AND expired_at IS NULL`; at most 1000 rows per transaction, selected again each time; a transaction with nothing to do is rolled back. An ABORTED retention transaction is retried with the same limit as an append and selects its candidates again; running out of retries is a retention failure, not an append failure | `SPN-11` to `SPN-13`; a retention failure is reported as a `RetentionFailure` through `tracing` and does not change the committed append | Retention inside the append transaction: a retention failure could roll back an append |
| D-8 | The open-time check reads `INFORMATION_SCHEMA.COLUMNS`, `INDEX_COLUMNS`, `TABLES.ROW_DELETION_POLICY_EXPRESSION`, and the admin `GetDatabase`; the comparison is a pure function (`reconcile`); the policy is read by a tokenizer that accepts only `OLDER_THAN(expired_at, INTERVAL <n> DAY)` in any letter case and spacing | `SPN-2`, `SPN-16`; the pure function is unit-tested without a service | Comparing the policy text exactly: breaks on spacing the service may normalize |
| D-9 | INT64 is sent and read as a decimal string, BYTES as base64; split results (`chunked_value`) are joined as strings only, and a stream that ends inside a split value or a row is discarded as an error | `SPN-5`, `SPN-10`; no value passes through a floating-point number | Returning the rows read so far |
| D-10 | The table prefix derives all five names, and the names are checked as identifiers before any request | `SPN-D1b`; the names are quoted in every statement | Individual table names |
| D-11 | Hooks are in the `test-hooks` feature and hidden from the documentation (`SpannerTestHooks`, `open_with_hooks`, `history_view`, `fnv1a64_for_test`) | The failures the contract requires (lost commit response, ABORTED, retention failures) cannot be produced by the emulator on demand | Public hooks: would widen the public API |
| D-12 | The conformance runner adds the fault kind `transaction-aborted` (phase `commit`, injection `replace-request`, scenes whose `backends` is exactly `["spanner"]`), runs each scene in a new database, and drops the database afterwards | The shared data declares the ABORTED scenes this way. An early run without the drop failed with `Too many databases for instance` from the emulator | Reusing one database across scenes: leftover rows would leak between scenes |
| D-13 | The FNV-1a 64 value table is run by `conformance-runner/src/hash_values.rs` for both Bigtable and Spanner, each passing its own library function | One reading of the data's `0x` and 16-digit format; each storage still proves its own function | Duplicating the runner code |

## 4. Rules outside the shared data, and the tests that cover them

Test names are in `lib/tests/spanner_test.rs` (called `integration`), `lib/tests/spanner_hooks_test.rs` (`hooks`), `lib/tests/spanner_notification_test.rs`, `lib/tests/spanner_readme_test.rs`, `lib/src/spanner/values_test.rs`, `lib/src/spanner/configuration_test.rs`, and `test-utils/src/spanner_emulator_test.rs`. All of them except the two unit-test files run against the real emulator.

| Rule | Requirement | Evidence |
|:-----|:------------|:---------|
| `SPN-1` | One database; SERIALIZABLE for read-write transactions; strong reads and no old read time | hooks: `should_spanner_transaction_options_of_an_event_append_be_one_read_write_serializable_transaction`, `should_spanner_transaction_options_of_a_snapshot_append_be_serializable_for_the_append_and_for_the_retention`, `should_spanner_transaction_options_of_reads_be_one_read_only_strong_transaction_each` (they capture the `TransactionOptions` that are sent) |
| `SP-1` | The profile states its preconditions | README "Spanner" (last paragraph) and "Consistency preconditions" in the schema document. `tools/user-docs/check.py` checks only their headings, so the text is reviewed by people |
| `SPN-2` | Check the layout, configuration row, and retention style at open; run no DDL | integration: `should_spanner_open_before_initialization_be_a_configuration_error_and_create_nothing`, `should_spanner_open_reject_a_layout_that_differs_from_the_profile`, `should_spanner_open_reject_a_database_of_the_postgresql_dialect`, `should_spanner_open_check_the_recorded_configuration_and_the_row_deletion_policy_against_the_client`, `should_spanner_open_reject_a_malformed_configuration_row`, `should_spanner_derive_the_five_table_names_from_the_prefix_and_check_only_those`; unit: 5 tests in `configuration_test.rs` for the policy text |
| `SPN-D5` | Initialization writes one row after checking, and creates nothing | integration: `should_spanner_initialization_record_one_configuration_row_for_each_retention_style`, `should_spanner_second_initialization_be_already_initialized_and_keep_the_recorded_configuration`, `should_spanner_initialization_of_non_empty_tables_be_already_initialized_and_write_no_configuration`, `should_spanner_initialization_never_create_a_missing_table` |
| `SPN-3`, `SPN-5` | Integer and time encoding without loss; the stored columns | integration: `should_spanner_keep_the_nanosecond_boundaries_without_loss_in_the_int64_column` (the 1677 and 2262 boundaries), `should_spanner_write_rows_in_the_javascript_shape` |
| `K-1` | FNV-1a 64 values | The four `fnv1a64` cases of the data, run by `conformance-runner/tests/spanner_test.rs`: `should_run_spanner_pass_the_fnv1a64_cases_with_the_hash_the_row_keys_use`, and `should_run_spanner_fail_an_fnv1a64_case_whose_expected_value_differs_from_the_row_key_hash` |
| `K-2` | The hash is embedded in the key as 8 bytes, big-endian | hooks: `should_spanner_default_hash_be_the_fnv_prefix_of_the_aid_when_no_hook_overrides_it` (the raw `hash_prefix` equals an independent computation); integration: `should_spanner_write_rows_in_the_javascript_shape` |
| `SPN-4`, `SP-2` | `aid` is at most 1024 bytes; keys match exactly, never by hash alone | `lib/src/aggregate_id_test.rs` (1024 bytes accepted, 1025 rejected, counted in UTF-8 bytes); integration: `should_spanner_compare_keys_by_exact_match_and_not_by_the_hash_prefix_alone`; hooks: `should_spanner_hash_collision_not_mix_the_event_ranges_of_different_aids`, `should_spanner_hash_collision_not_retire_the_history_of_other_aids` |
| `SPN-6`, `H-1`, `W-*` | One transaction decides the append; the commit is the commit point | integration: `should_spanner_round_trip_events_and_snapshots_and_read_them_again_after_a_reopen`, `should_spanner_creation_with_events_only_leave_no_snapshot_row_and_read_as_an_existing_aggregate`, `should_spanner_duplicate_stale_and_existing_creation_appends_be_optimistic_locks_that_leave_rows_untouched`, `should_spanner_gap_append_and_update_without_a_head_be_a_w8_contract_violation_and_write_nothing`, `should_spanner_concurrent_creations_of_the_same_aggregate_commit_exactly_one` |
| `SPN-7` | ABORTED is retried with the same envelope; the limit; only ABORTED | hooks: `should_spanner_aborted_commit_within_the_limit_be_retried_with_the_same_envelope_and_commit_once`, `should_spanner_aborted_commit_beyond_the_limit_be_a_storage_error_and_commit_nothing`, `should_spanner_aborted_is_the_only_failure_that_is_retried`, `should_spanner_aborted_append_that_lost_to_an_earlier_writer_be_an_optimistic_lock_without_renumbering`; conformance cases `spanner-aborted-retry-within-limit`, `spanner-aborted-retry-limit-exceeded` |
| `SPN-8`, `W-7` | A lost commit response is an unknown outcome | hooks: `should_spanner_lost_commit_response_be_an_unknown_outcome_storage_error_without_retry_or_compensation`; conformance case `spanner-commit-response-lost` |
| `SPN-9`, `R-8` | The head and the current snapshot come from one read point | hooks: `should_spanner_latest_snapshot_read_the_head_and_the_current_snapshot_at_the_same_read_point` (a writer commits inside the read, and its content must not appear) |
| `SPN-10`, `R-5` | Events are read up to the head that was read; split results are joined; gaps are errors | hooks: `should_spanner_read_events_return_the_rows_up_to_the_head_that_was_read_at_the_same_read_point`; integration: `should_spanner_get_events_return_the_range_from_the_requested_number_up_to_the_head_in_ascending_order`, `should_spanner_read_events_report_a_storage_error_for_a_missing_journal_row`, `should_spanner_round_trip_a_payload_that_the_service_returns_in_chunks` (2 MiB, split by the emulator); unit: 9 tests in `values_test.rs` for two-part and three-part joins, rows split across responses, and a stream cut off inside a value or a row; test-utils: `test_spanner_emulator_streams_a_one_mebibyte_value_in_chunks` |
| `SPN-11`, `SPN-12`, `S-1` to `S-3` | Keep the newest `n`; delete, or mark once; never touch other tables | integration: `should_spanner_retention_keep_the_newest_n_history_rows_and_never_touch_journal_head_or_current`, `should_spanner_events_only_appends_and_current_only_stores_leave_the_history_untouched`, `should_spanner_ttl_retention_mark_old_history_once_and_never_count_or_remark_marked_rows` |
| `SPN-13` (split work), `S-1`, `S-3` | More than 1000 candidates are split, each split selects again, and the newest `n` rows are never removed or marked | hooks: `should_spanner_retention_split_more_than_one_thousand_candidates_and_never_remove_the_newest_rows` and `should_spanner_ttl_retention_split_more_than_one_thousand_candidates_and_mark_only_the_old_rows` (1005 history rows with `n = 2`; the recorded batches are rows 1 to 1000 and then 1001 to 1003, and only the newest 2 rows remain unmarked) |
| `SPN-13`, `S-4` | A retention failure leaves the committed append successful and is retried at the next snapshot | hooks: `should_spanner_retention_delete_failure_keep_the_append_successful_and_retry_on_the_next_snapshot`, `should_spanner_retention_query_failure_keep_the_append_successful`, `should_spanner_retention_mark_failure_keep_the_append_successful_and_mark_on_the_next_snapshot`, and for an ABORTED retention commit (`before_retention_commit`) `should_spanner_retention_aborted_commit_within_the_limit_be_retried_with_the_candidates_selected_again` and `should_spanner_retention_aborted_commit_beyond_the_limit_keep_the_append_successful_and_retry_on_the_next_snapshot`; notification: `should_spanner_committed_append_stay_successful_when_the_notification_subscriber_panics` asserts that the subscriber was called exactly once and the append still returned success |
| `SPN-16` | Grace days: positive, in range, and equal in the client, the row, and the policy | integration: `should_spanner_configuration_options_that_cannot_be_recorded_be_rejected_before_any_request`, `should_spanner_largest_representable_options_pass_the_check_and_reach_the_network`; conformance cases `spanner-ttl-grace-days-not-positive`, `spanner-seeded-ttl-grace-config-mismatch`, `spanner-seeded-ttl-grace-policy-mismatch`, `spanner-seeded-ttl-grace-client-mismatch`, `spanner-seeded-ttl-configuration-match` |
| `SPN-17` | A change feed is a configuration error | conformance cases `spanner-change-feed-unsupported`, `spanner-seeded-change-feed-enabled` |
| `SPN-14`, `SPN-15` | Change feed rules | Not provided. They apply only when a feed is provided |
| `E-*` | A payload that cannot be decoded is a serialization error | integration: `should_spanner_undecodable_payloads_be_serialization_errors_and_not_storage_errors` |
| Stored format | Rows are read and written in the JavaScript shape | integration: `should_spanner_read_rows_written_with_the_javascript_statements`, `should_spanner_write_rows_in_the_javascript_shape` |
| Connection | The user's channel; a failing endpoint is a storage error; the token source is called for every request | integration: `should_spanner_open_against_an_unreachable_endpoint_be_a_storage_error`, `should_spanner_ask_the_token_source_for_every_public_operation_and_succeed`, `should_spanner_failing_token_source_be_a_storage_error_and_send_no_request` |
| Documents | The README example and the schema SQL run | `lib/tests/spanner_readme_test.rs` (4 tests): the README example is the code the test runs, and the schema DDL, TTL policy, and configuration row open a store with the documented options |

## 5. Conformance result

Command, run on the working tree on top of revision `6fda5c942def5450b165116bb5e26f7f0811c827` with the emulator started by the runner:

```sh
cargo run -p event-store-adapter-conformance-rs --release --features spanner -- \
  --backend spanner --data conformance --report conformance-report-spanner.json --require-all
```

Result for conformance data 1.3.0 (manifest SHA-256 `0157c012042afa9a19f0bff509a433a4bd5ddc5b674de864a41bec453c460691`), 169 cases:

| Status | Cases | Content |
|:-------|------:|:--------|
| passed | 77 | 9 `buildAid` value cases, 52 scenes (42 common, 14 only for Spanner, 1 shared-store scene, minus the 5 below that cannot be represented), and 16 value cases (the four `fnv1a64` cases among them) |
| failed | 0 | |
| not-applicable | 92 | 82 for other storages (`backend-not-targeted`: scenes of the other storages, the layout cases, and their value tables), 8 for millisecond precision (`time-precision`), and 2 for a negative `SeqNr` (`unrepresentable`) |
| unverified | 0 | |

By rule: `SPN-2` 5, `SPN-7` 2, `SPN-8` 1, `SPN-16` 5, `SPN-17` 2, and `K-1` 4 cases passed, with no failure. The 14 scenes only for Spanner all passed. No Spanner case is classed as "not provided". The CI job `conformance (spanner)` runs the same command with `--require-all`, so a case that is not run fails the job, and it uploads the report as the artifact `conformance-spanner`.

## 6. Emulator limits and what is not verified

- The emulator (1.5.58, pinned by digest) accepts one read-write transaction at a time. Code and tests commit or roll back every transaction; a transaction that was left open made 23 of 33 integration tests fail in an early run, and the cause was fixed in retention.
- It aborts overlapping read-write transactions even without a conflict, as the profile notes. Concurrent tests therefore depend on the retry of `SPN-7`, and a pass on the emulator does not prove the service's conflict detection.
- It rejects creating more databases for one instance after some number of them (`Too many databases for instance`). The tests create a new database for each test, and the conformance runner drops each scene's database. The exact limit was not measured.
- The commit response cannot be lost, and a result stream cannot be cut off, on demand. They are covered by the hooks (`after_commit`) and by the unit tests of the merge.
- Not verified, because no real service may be contacted in this work: Cloud Spanner itself (TLS, access tokens, multi-region configurations), the service-side limits on mutation and commit size, and the physical deletion by the row deletion policy. The library does not rely on physical deletion (`S-3`). These remain obligations to check outside this repository before production use.

## 7. Reproducing

```sh
cargo test -p event-store-adapter-rs --all-features --test spanner_test --test spanner_hooks_test --test spanner_notification_test --test spanner_readme_test
cargo test -p event-store-adapter-rs --all-features --lib spanner::
cargo test -p event-store-adapter-conformance-rs --all-features
python3 tools/user-docs/check.py
```

Docker must be running for the first and third commands.
