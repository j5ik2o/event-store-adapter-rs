<!-- INVARIANT: examples are single-line HTML comments so a fresh template parses to total=0 (MEMORY_EMPTY). Do NOT un-comment or split across lines. t100 guards this. -->
> This file is kept up to date automatically while the stage runs. Add observations at the review step, not by editing here directly.

## Interpretations
<!-- example: 2026-05-29T10:14:32Z — chose REST over GraphQL; the consuming team only needs CRUD, revisit if subscriptions land -->
- 2026-08-23T01:45:54Z — DynamoDBの楽観ロック失敗はSDKエラーから実バージョンが取れないためBR1.2「判明分」規約に基づきactual_versionなしの基本形とした; Bigtable/Memoryは判明するため付加。
- 2026-08-23T01:45:54Z — Memoryの重複create（既存スナップショットあり）はDynamoDBの条件付きPut失敗と対称にOptimisticLockErrorを返す設計とした; 4バックエンドの挙動契約統一（US3.1）の解釈。

- 2026-08-23T09:59:07Z — u4-docs: Standard戦略の「コンポーネントあたり5〜8テスト」を、新規コンポーネントを持たない文書ユニットでは「exampleの実行成功＋記載照合チェック群」に読み替えた; exampleの実行そのものが実DBでのend-to-end検証（作成→リネーム→リプレイ）であることを根拠とした。

## Deviations
<!-- example: 2026-05-29T10:14:32Z — skipped the optional caching layer the stage prose suggested; the dataset is small enough that it adds risk -->
- 2026-08-23T01:45:54Z — cargo tree検査はsecurity-design.mdの素のコマンドではなく `-p event-store-adapter-rs -e normal` 形で実施; 素のコマンドはdev-dependencies（test-utils経由SDK）も表示し利用者依存グラフの検証にならないため。U3のCI昇格時もこの形を推奨（引き渡し事項）。
- 2026-08-23T01:45:54Z — Memoryファサードに with_keep_snapshot_count / with_delete_ttl / maintenance を追加（計画の明示外）; 他バックエンドとのAPI対称性とno-feature構成のdead_code警告解消のため。Memoryでは保持設定は現状no-op。

- 2026-08-23T09:59:07Z — u4-docs: 開発エージェントが計画から2点軽微に逸脱 — (1) example依存から未使用のserde_jsonを除外、(2) ULID生成をtest-utils非依存（AWS依存を引き込むため）としOnceLockベースのid_generateをuser_account.rs内に実装; いずれも「最小依存」の設計意図に沿う方向の逸脱でcode-summary.mdに記録済み。

## Tradeoffs
<!-- example: 2026-05-29T10:14:32Z — picked TDD over BDD this run; the team is unit-first and the domain is well-understood -->
- 2026-08-23T05:31:38Z — U2: CAS実装（Step 5/9）はStorageBackendトレイト実装の性質上Step 3のモジュール作成と一体で実装し、テストのみ計画ステップ順に追加・実行してtest-after順序を維持; トレイトの部分実装を段階分けする不自然さを避けた。スロット行のseq_nr列は常に0維持（現行読取は WHERE aid=? AND seq_nr=0 — DynamoDB対称）。

## Open questions
<!-- example: 2026-05-29T10:14:32Z — confirm the retention window with compliance before the next stage hardens the schema -->
- 2026-08-23T08:42:05Z — U3: cargo-denyのadvisoriesで既存AWS SDKレガシーTLSスタック由来のRUSTSEC 4件（h2/rustls-webpki）を理由付きignoreで緑化 — 当該メジャー系列に修正版なし、解消はAWS SDK TLS構成移行（バックログ）; CI初回実行（PR時）で6構成マトリクスの実機green確認が残る（NFR-4.11の受け入れ最終確認）。
- 2026-08-23T01:45:54Z — bigtable既存コードの本番経路にprintln!（RowAccumulator::start_cell内DEBUG出力）が残存 — U1範囲外のため未変更、バックログ推奨; clippy既存負債1件（generic_event_storeテスト内eager clone）も同様。
