> この文書は [spanner-backend.md](spanner-backend.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# Spannerの保存先の設計

> この文書は、Spannerの保存先（feature `spanner`）の設計の記録です。現在の使い方は、[README](../../README.ja.md)と[Spannerのスキーマ](../SPANNER_DATABASE_SCHEMA.ja.md)を参照してください。

この文書は、データの形、モジュールの所有範囲、設計判断、そして共通の適合データでは表せない規則の証拠を記録します。規則を変えることも、追加することもしません。

## 1. 範囲と出典

- 規範は、ハブのリポジトリ（`j5ik2o/event-store-adapter`）にある、共通契約 v5（`docs/spec/core-contract.md`）、Spannerのプロファイル（`docs/spec/storage/spanner.md`。規則 `SPN-1`〜`SPN-17`、決定 `SPN-D1a`〜`SPN-D8`）、ハッシュの仕様（`docs/spec/storage/hash.md`。規則 `K-1`・`K-2`）、ADRです。
- 適合テストデータは、このリポジトリの `conformance/`（版 1.3.0）です。`conformance/coverage.json` は、いくつかの規則を各言語の試験に任せています。`SPN-1`・`SPN-10`・`SPN-12`・`K-2` はデータの対象外で、`SP-1` は文書に書く義務です。4章で、それぞれを試験か文書に対応づけます。
- 保存形式の基準は、JavaScript実装（`event-store-adapter-js` の `spanner-persist-event.ts`）が書き読みする行の形です。このクレートには以前Spannerの保存先がなかったため、配置はRustでは新しいものであり、移行するものはありません。
- この版で提供しないものは、変更フィード（`SPN-D8`・`SPN-17`。`SPN-14`・`SPN-15` はフィードを提供するときだけ適用）、PostgreSQL方言（`SPN-D1a`）、移行ツールです。

## 2. 公開する型、モジュール、資源の所有

| 項目 | 場所 | 役割 |
|:-----|:-----|:-----|
| `EventStoreForSpanner<AID, A, P>` | `lib/src/spanner/mod.rs` | `GenericEventStore`と`SpannerBackend`を通した`EventStore`。`open`、`open_with_serializers` |
| `initialize_spanner_store` | `lib/src/spanner/mod.rs`、`configuration.rs` | 照合してから、設定の行を1行書く |
| `SpannerConnection`、`SpannerTables`、`SpannerOptions`、`SpannerRetention` | `lib/src/spanner/mod.rs` | 接続、データベースと接頭辞、設定（`retention`・`change_feed`・`transaction_retry_limit`） |
| `SpannerTokenSource` | `lib/src/spanner/auth.rs` | 利用者が実装するトークンの取得元 |
| `ConfigurationReason::Spanner*`（8種類） | `lib/src/error.rs` | 設定エラー。スキーマの文書を参照 |
| `configuration.rs`、`backend.rs`、`retention.rs` | `lib/src/spanner/` | 開くときの照合、追記と読み取り、保持 |
| `client.rs`、`values.rs` | `lib/src/spanner/` | セッションとトランザクション。値の符号化と、分割された結果の結合 |
| `key_hash.rs`、`bearer_token.rs` | `lib/src/` | FNV-1a 64と、bearerのヘッダー値。Bigtableと共有 |

feature `spanner` は、`tonic`、生成されたSpannerのデータAPIとデータベース管理APIのgRPCクライアント（`googleapis-tonic-google-spanner-v1` 0.45.0と`googleapis-tonic-google-spanner-admin-database-v1` 0.40.0）、`prost-types`、`base64`、`tokio`（timeだけ）、`uuid`を有効にします。ほかの保存先も`test-hooks`も有効にしません。CIは`cargo tree`で、このグラフにAWS・SQLite・Bigtable・Googleの認証のクレートがないことを確かめます。

資源の所有はBigtableの保存先に合わせます。

- `SpannerConnection`の中の`tonic::transport::Channel`は、利用者が作って所有します。ライブラリはchannelを作らず、TLSの実装にもGoogleの認証のライブラリにも依存せず、バックグラウンドのタスクも起動しません。
- 公開操作ごとに、セッションを作り、使い、削除します（削除はできる範囲で行います）。セッションのプールはありません。storeとそのcloneは、channelと、テーブル名・検査済みの設定・`store_id`を持つ`Arc`を共有し、storeを破棄しても何も処理しません。
- 開始した読み書きトランザクションは、必ずコミットかロールバックします。エミュレーターは読み書きトランザクションを同時に1つしか受け付けず、開いたままのトランザクションがあると、後続のトランザクションが中断されます。

## 3. 設計判断

| # | 判断 | 理由 | 採らなかった案 |
|:--|:-----|:-----|:---------------|
| D-1 | 生成されたクライアントでSpannerのgRPC APIを直接呼び、channelは利用者が所有する | Bigtableと所有の仕方が同じ。依存グラフに認証やTLSのクレートが入らず、`feature-matrix` のジョブがこれを確かめる | 独自のchannelと認証を持つSpannerのクライアントライブラリ。所有の仕方が合わないため採用しなかった。再試行の挙動は、この作業では評価していない |
| D-2 | 公開操作ごとにセッションを1つ使う | プール・期限切れ・回復の状態を持たない | セッションのプールや多重化したセッション。状態が増える。ここでは操作ごとに要求が2回増える |
| D-3 | 追記は、ミューテーションの `Commit` 1回。journalは`Insert`、headは`seq_nr = 1`で`Insert`（`type_name`を含む）、以後は`Update`、`snapshot_current`は`InsertOrUpdate`、`snapshot_history`は`expired_at`をNULLにして`Insert` | headを同じトランザクションで読み、楽観ロックを決める（`SPN-6`）。`Insert`はjournalの行を上書きしない | 追記にDML文を使う案。判断はheadの読み取りで決まるため、利点がない |
| D-4 | コミット時の主キーの重複は、保存先エラーにする | 契約は、確認できた重複だけを楽観ロックにしてよいとする。確認できる場合は、すでにheadの読み取りで判断している | `ALREADY_EXISTS`をすべて楽観ロックにする案 |
| D-5 | 中断（ABORTED）だけを、同じ封筒・同じ`seq_nr`で再試行する。上限は最初の試行を数えず（既定5）、10ミリ秒から倍にして1秒まで待つ | `SPN-7`。エミュレーターは競合がなくても重なった読み書きトランザクションを中断するため、待たないと並行実行の結果が時刻に左右される | 他の失敗も再試行する案。結果が不明なコミットを繰り返す恐れがある |
| D-6 | コミットの応答を受けた後の失敗（試験では`after_commit`）は、結果が不明な保存先エラーにし、再試行も補償もしない | `SPN-8` | 失敗時に行を削除する案。コミットされていた場合に危険 |
| D-7 | 保持は、追記のコミットの後に、別のSERIALIZABLEのトランザクションでDMLを使って行う。`DELETE`、または`UPDATE ... SET expired_at = CURRENT_TIMESTAMP() ... AND expired_at IS NULL`。1トランザクションあたり最大1000行で、毎回選び直す。行う作業がないトランザクションはロールバックする。中断（ABORTED）された保持のトランザクションは、追記と同じ上限で、候補を選び直して再試行する。再試行を使い切ると、追記の失敗ではなく保持の失敗になる | `SPN-11`〜`SPN-13`。保持の失敗は`RetentionFailure`として`tracing`で知らせ、確定した追記は変えない | 追記のトランザクションの中で保持する案。保持の失敗で追記が巻き戻る恐れがある |
| D-8 | 開くときの照合は、`INFORMATION_SCHEMA.COLUMNS`・`INDEX_COLUMNS`・`TABLES.ROW_DELETION_POLICY_EXPRESSION`と、管理APIの`GetDatabase`を読む。比較は純粋関数（`reconcile`）で、ポリシーは、大文字小文字と空白が違ってもよい`OLDER_THAN(expired_at, INTERVAL <n> DAY)`だけを受け付ける字句の分割で読む | `SPN-2`・`SPN-16`。純粋関数なので、サービスなしで単体試験できる | ポリシーの文字列を完全一致で比べる案。サービスが空白を整える場合に壊れる |
| D-9 | INT64は10進の文字列、BYTESはbase64で送受信する。分割された結果（`chunked_value`）は文字列としてだけ結合し、分割された値や行の途中で終わったストリームは、誤りとして捨てる | `SPN-5`・`SPN-10`。浮動小数点数を経由する値がない | それまでに読んだ行を返す案 |
| D-10 | 接頭辞から5つの名前をすべて導き、要求の前にSQLの識別子として検査する | `SPN-D1b`。すべての文で名前を引用符で囲む | テーブル名を個別に指定する案 |
| D-11 | フックは機能`test-hooks`の中にあり、文書には出さない（`SpannerTestHooks`、`open_with_hooks`、`history_view`、`fnv1a64_for_test`） | 契約が求める失敗（コミットの応答の消失、ABORTED、保持の失敗）は、エミュレーターで任意に起こせない | フックを公開する案。公開APIが広がる |
| D-12 | 適合の実行器に、障害の種類`transaction-aborted`（段階`commit`、方式`replace-request`、`backends`がちょうど`["spanner"]`の場面）を加え、場面ごとに新しいデータベースで実行し、終わったら削除する | 共通データがABORTEDの場面をこの形で宣言している。削除しない初期の実行は、エミュレーターの `Too many databases for instance` で失敗した | 場面をまたいで1つのデータベースを使い回す案。場面の間で行が残って混ざる |
| D-13 | FNV-1a 64の値の表は、BigtableとSpannerの両方について `conformance-runner/src/hash_values.rs` が実行し、それぞれが自分のライブラリの関数を渡す | データの`0x`と16桁の形の読み方を1か所にする。それぞれの保存先が自分の関数を確かめる | 実行器のコードを複製する案 |

## 4. 共通データの外にある規則と、それを確かめる試験

試験の名前は、`lib/tests/spanner_test.rs`（以下「integration」）、`lib/tests/spanner_hooks_test.rs`（「hooks」）、`lib/tests/spanner_notification_test.rs`、`lib/tests/spanner_readme_test.rs`、`lib/src/spanner/values_test.rs`、`lib/src/spanner/configuration_test.rs`、`test-utils/src/spanner_emulator_test.rs`にあります。2つの単体試験のファイルを除き、すべて実エミュレーターに対して動きます。

| 規則 | 要件 | 証拠 |
|:-----|:-----|:-----|
| `SPN-1` | 1つのデータベース。読み書きトランザクションはSERIALIZABLE。読み取りはstrongで、過去の時刻を使わない | hooks: `should_spanner_transaction_options_of_an_event_append_be_one_read_write_serializable_transaction`、`should_spanner_transaction_options_of_a_snapshot_append_be_serializable_for_the_append_and_for_the_retention`、`should_spanner_transaction_options_of_reads_be_one_read_only_strong_transaction_each`（送られる`TransactionOptions`を捕まえる） |
| `SP-1` | プロファイルが前提を書く | READMEの「Spanner」（最後の段落）と、スキーマの文書の「一貫性の前提」。`tools/user-docs/check.py`が見るのは見出しだけなので、文章は人が確かめる |
| `SPN-2` | 開くときに、配置・設定の行・保持の方式を照合し、DDLは実行しない | integration: `should_spanner_open_before_initialization_be_a_configuration_error_and_create_nothing`、`should_spanner_open_reject_a_layout_that_differs_from_the_profile`、`should_spanner_open_reject_a_database_of_the_postgresql_dialect`、`should_spanner_open_check_the_recorded_configuration_and_the_row_deletion_policy_against_the_client`、`should_spanner_open_reject_a_malformed_configuration_row`、`should_spanner_derive_the_five_table_names_from_the_prefix_and_check_only_those`。単体: ポリシーの文字列についての`configuration_test.rs`の5本 |
| `SPN-D5` | 初期化は、照合の後に1行を書き、何も作らない | integration: `should_spanner_initialization_record_one_configuration_row_for_each_retention_style`、`should_spanner_second_initialization_be_already_initialized_and_keep_the_recorded_configuration`、`should_spanner_initialization_of_non_empty_tables_be_already_initialized_and_write_no_configuration`、`should_spanner_initialization_never_create_a_missing_table` |
| `SPN-3`・`SPN-5` | 整数と時刻を損失なく符号化する。保存する列 | integration: `should_spanner_keep_the_nanosecond_boundaries_without_loss_in_the_int64_column`（1677年と2262年の境界）、`should_spanner_write_rows_in_the_javascript_shape` |
| `K-1` | FNV-1a 64の値 | データの `fnv1a64` の4件。`conformance-runner/tests/spanner_test.rs` が実行する: `should_run_spanner_pass_the_fnv1a64_cases_with_the_hash_the_row_keys_use`、`should_run_spanner_fail_an_fnv1a64_case_whose_expected_value_differs_from_the_row_key_hash` |
| `K-2` | ハッシュを8バイトのビッグエンディアンでキーに埋め込む | hooks: `should_spanner_default_hash_be_the_fnv_prefix_of_the_aid_when_no_hook_overrides_it`（生の`hash_prefix`が独立した計算と等しい）。integration: `should_spanner_write_rows_in_the_javascript_shape` |
| `SPN-4`・`SP-2` | `aid`は1024バイト以下。キーは完全一致で比べ、ハッシュだけでは比べない | `lib/src/aggregate_id_test.rs`（1024バイトは許し、1025バイトは拒否し、UTF-8のバイト数で数える）。integration: `should_spanner_compare_keys_by_exact_match_and_not_by_the_hash_prefix_alone`。hooks: `should_spanner_hash_collision_not_mix_the_event_ranges_of_different_aids`、`should_spanner_hash_collision_not_retire_the_history_of_other_aids` |
| `SPN-6`・`H-1`・`W-*` | 1つのトランザクションで追記を決め、コミットが確定点 | integration: `should_spanner_round_trip_events_and_snapshots_and_read_them_again_after_a_reopen`、`should_spanner_creation_with_events_only_leave_no_snapshot_row_and_read_as_an_existing_aggregate`、`should_spanner_duplicate_stale_and_existing_creation_appends_be_optimistic_locks_that_leave_rows_untouched`、`should_spanner_gap_append_and_update_without_a_head_be_a_w8_contract_violation_and_write_nothing`、`should_spanner_concurrent_creations_of_the_same_aggregate_commit_exactly_one` |
| `SPN-7` | ABORTEDは同じ封筒で再試行する。上限。ABORTEDだけ | hooks: `should_spanner_aborted_commit_within_the_limit_be_retried_with_the_same_envelope_and_commit_once`、`should_spanner_aborted_commit_beyond_the_limit_be_a_storage_error_and_commit_nothing`、`should_spanner_aborted_is_the_only_failure_that_is_retried`、`should_spanner_aborted_append_that_lost_to_an_earlier_writer_be_an_optimistic_lock_without_renumbering`。適合のケース `spanner-aborted-retry-within-limit`、`spanner-aborted-retry-limit-exceeded` |
| `SPN-8`・`W-7` | コミットの応答の消失は結果が不明 | hooks: `should_spanner_lost_commit_response_be_an_unknown_outcome_storage_error_without_retry_or_compensation`。適合のケース `spanner-commit-response-lost` |
| `SPN-9`・`R-8` | headと現在のsnapshotは同じ読み取り時点 | hooks: `should_spanner_latest_snapshot_read_the_head_and_the_current_snapshot_at_the_same_read_point`（読み取りの途中で書き手がコミットしても、その内容は現れない） |
| `SPN-10`・`R-5` | 読んだheadまでのイベントを読む。分割された結果を結合する。欠けは誤り | hooks: `should_spanner_read_events_return_the_rows_up_to_the_head_that_was_read_at_the_same_read_point`。integration: `should_spanner_get_events_return_the_range_from_the_requested_number_up_to_the_head_in_ascending_order`、`should_spanner_read_events_report_a_storage_error_for_a_missing_journal_row`、`should_spanner_round_trip_a_payload_that_the_service_returns_in_chunks`（2 MiB。エミュレーターが分割する）。単体: `values_test.rs`の9本（2分割・3分割の結合、複数の応答にまたがる行、値や行の途中で切れたストリーム）。test-utils: `test_spanner_emulator_streams_a_one_mebibyte_value_in_chunks` |
| `SPN-11`・`SPN-12`・`S-1`〜`S-3` | 新しい`n`件を残す。削除、または1度だけ印を付ける。ほかのテーブルに触れない | integration: `should_spanner_retention_keep_the_newest_n_history_rows_and_never_touch_journal_head_or_current`、`should_spanner_events_only_appends_and_current_only_stores_leave_the_history_untouched`、`should_spanner_ttl_retention_mark_old_history_once_and_never_count_or_remark_marked_rows` |
| `SPN-13`（分割）・`S-1`・`S-3` | 1000件を超える候補は分割し、分割ごとに選び直し、新しい`n`件は削除も印付けもしない | hooks: `should_spanner_retention_split_more_than_one_thousand_candidates_and_never_remove_the_newest_rows`、`should_spanner_ttl_retention_split_more_than_one_thousand_candidates_and_mark_only_the_old_rows`（`n = 2`で履歴1005行。記録した分割は1〜1000行目、次に1001〜1003行目で、印のないまま残るのは新しい2行だけ） |
| `SPN-13`・`S-4` | 保持の失敗は、確定した追記を成功のままにし、次のsnapshotで再び試みる | hooks: `should_spanner_retention_delete_failure_keep_the_append_successful_and_retry_on_the_next_snapshot`、`should_spanner_retention_query_failure_keep_the_append_successful`、`should_spanner_retention_mark_failure_keep_the_append_successful_and_mark_on_the_next_snapshot`、および保持のCommitがABORTEDになる場合（`before_retention_commit`）の`should_spanner_retention_aborted_commit_within_the_limit_be_retried_with_the_candidates_selected_again`と`should_spanner_retention_aborted_commit_beyond_the_limit_keep_the_append_successful_and_retry_on_the_next_snapshot`。notification: `should_spanner_committed_append_stay_successful_when_the_notification_subscriber_panics`は、購読側がちょうど1回呼ばれたことと、追記が成功を返したことを確かめる |
| `SPN-16` | 猶予の日数は、正で範囲内であり、クライアント・行・ポリシーで等しい | integration: `should_spanner_configuration_options_that_cannot_be_recorded_be_rejected_before_any_request`、`should_spanner_largest_representable_options_pass_the_check_and_reach_the_network`。適合のケース `spanner-ttl-grace-days-not-positive`、`spanner-seeded-ttl-grace-config-mismatch`、`spanner-seeded-ttl-grace-policy-mismatch`、`spanner-seeded-ttl-grace-client-mismatch`、`spanner-seeded-ttl-configuration-match` |
| `SPN-17` | 変更フィードは設定エラー | 適合のケース `spanner-change-feed-unsupported`、`spanner-seeded-change-feed-enabled` |
| `SPN-14`・`SPN-15` | 変更フィードの規則 | 提供しない。フィードを提供するときだけ適用する |
| `E-*` | 復号できないpayloadは直列化のエラー | integration: `should_spanner_undecodable_payloads_be_serialization_errors_and_not_storage_errors` |
| 保存形式 | JavaScript版と同じ形で行を読み書きする | integration: `should_spanner_read_rows_written_with_the_javascript_statements`、`should_spanner_write_rows_in_the_javascript_shape` |
| 接続 | 利用者のchannel。到達できない接続先は保存先エラー。トークンの取得元は要求ごとに呼ばれる | integration: `should_spanner_open_against_an_unreachable_endpoint_be_a_storage_error`、`should_spanner_ask_the_token_source_for_every_public_operation_and_succeed`、`should_spanner_failing_token_source_be_a_storage_error_and_send_no_request` |
| 文書 | READMEの例とスキーマのSQLが動く | `lib/tests/spanner_readme_test.rs`（4本）。READMEの例は試験が実行するコードそのもので、スキーマのDDL・TTLのポリシー・設定の行は、文書の設定でストアが開ける |

## 5. 適合の結果

実行したコマンドです。リビジョン `6fda5c942def5450b165116bb5e26f7f0811c827` の上の作業ツリーで、実行器が起動したエミュレーターを使いました。

```sh
cargo run -p event-store-adapter-conformance-rs --release --features spanner -- \
  --backend spanner --data conformance --report conformance-report-spanner.json --require-all
```

適合データ 1.3.0（manifest の SHA-256 `0157c012042afa9a19f0bff509a433a4bd5ddc5b674de864a41bec453c460691`）の169件の結果です。

| 状態 | 件数 | 内容 |
|:-----|-----:|:-----|
| passed | 77 | `buildAid` の値の表9件、場面52件（共通42件、Spannerだけの14件、保存先を共有する場面1件、から表現できない5件を除いたもの）、値の表16件（`fnv1a64`の4件を含む） |
| failed | 0 | |
| not-applicable | 92 | ほかの保存先が対象のもの82件（`backend-not-targeted`: ほかの保存先の場面、配置のケース、その値の表）、ミリ秒精度のもの8件（`time-precision`）、負の`SeqNr`のもの2件（`unrepresentable`） |
| unverified | 0 | |

規則別では、`SPN-2` 5件、`SPN-7` 2件、`SPN-8` 1件、`SPN-16` 5件、`SPN-17` 2件、`K-1` 4件が成功し、失敗はありません。Spannerだけの14の場面はすべて成功しました。Spannerのケースを「提供しない」に分類したものはありません。CIのジョブ `conformance (spanner)` は同じコマンドを `--require-all` 付きで実行するため、実行されないケースがあるとジョブが失敗し、報告を成果物 `conformance-spanner` として保存します。

## 6. エミュレーターの限界と、確かめていないこと

- エミュレーター（1.5.58、digestで固定）は、読み書きトランザクションを同時に1つしか受け付けません。コードと試験は、すべてのトランザクションをコミットかロールバックします。開いたままのトランザクションがあったために、初期の実行では33本の結合試験のうち23本が失敗し、原因を保持処理で直しました。
- プロファイルが述べるとおり、競合がなくても重なった読み書きトランザクションを中断します。そのため並行実行の試験は`SPN-7`の再試行に頼り、エミュレーターでの成功は、サービス側の競合検出を証明しません。
- 1つのインスタンスで作れるデータベースの数に上限があり、超えると `Too many databases for instance` で拒否されます。試験は試験ごとに新しいデータベースを作り、適合の実行器は場面ごとにそのデータベースを削除します。上限の数は測っていません。
- コミットの応答の消失と、結果のストリームの途中切断は、任意に起こせません。フック（`after_commit`）と、結合の単体試験が代わりに確かめます。
- この作業では実サービスに接続してはならないため、確かめていないものがあります。Cloud Spanner自体（TLS・アクセストークン・マルチリージョンの構成）、ミューテーションとコミットの大きさのサービス側の上限、行削除ポリシーによる物理的な削除です。ライブラリは物理的な削除に依存しません（`S-3`）。本番で使う前に、このリポジトリの外で確かめる義務が残ります。

## 7. 再現の手順

```sh
cargo test -p event-store-adapter-rs --all-features --test spanner_test --test spanner_hooks_test --test spanner_notification_test --test spanner_readme_test
cargo test -p event-store-adapter-rs --all-features --lib spanner::
cargo test -p event-store-adapter-conformance-rs --all-features
python3 tools/user-docs/check.py
```

1つ目と3つ目のコマンドには、Dockerの起動が必要です。
