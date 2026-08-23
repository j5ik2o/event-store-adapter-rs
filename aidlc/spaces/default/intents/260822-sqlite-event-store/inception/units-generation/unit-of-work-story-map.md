# ストーリー・ユニット対応表 (unit-of-work-story-map)

ストーリー（`../user-stories/stories.md`）の全12件をユニット（`unit-of-work.md`）へ割り当てる。要件定義書（`../requirements-analysis/requirements.md`）のFR/NFRとの対応はストーリー側のトレーサビリティを継承。ユニット定義・依存はコンポーネントカタログ（`../domain-design/components.md`）と依存DAG（`unit-of-work-dependency.md`）に整合。

## 割り当て表

| Story ID | ストーリー | Unit ID | Directory |
|---|---|---|---|
| US2.1 | 必要なバックエンドだけを依存に含める | U1 | u1-backend-features |
| US2.2 | バックエンド中立なエラー型 | U1 | u1-backend-features |
| US3.1 | 4バックエンドの挙動契約統一 | U1 | u1-backend-features |
| US1.1 | SQLiteへのイベント永続化と復元 | U2 | u2-sqlite-backend |
| US1.2 | 楽観的ロックによる並行更新の保護 | U2 | u2-sqlite-backend |
| US1.3 | `:memory:` モードでの利用 | U2 | u2-sqlite-backend |
| US1.4 | スナップショット保持ポリシー | U2 | u2-sqlite-backend |
| US2.3 | バンドル／システムSQLiteの選択 | U2 | u2-sqlite-backend |
| US3.2 | 既存バックエンドと同等のテスト | U2 | u2-sqlite-backend |
| US3.3 | CIによる品質保証 | U3 | u3-ci-quality |
| US4.1 | 使い方がわかるドキュメントと例 | U4 | u4-docs |
| US4.2 | スキーマと変更内容の把握 | U4 | u4-docs |

## 複数ユニットにまたがるストーリー（横断関心事）

- **US3.2（同等テスト）**: 主担当はU2だが、AC3.2.1の共有シナリオ搭載はU1のMemory準拠化（US3.1）が前提。U1完了時点でMemoryが共有シナリオに乗り、U2でSQLiteが加わる。
- **US2.1（feature分割）のAC2.1.1/AC2.1.3のうち `sqlite` featureを含む検証**: featureの器はU1で定義するが、sqlite単独ビルドの完全な検証はU2完了後（最終確認はU3のCIマトリクスが恒久化）。

## ユニット内のストーリー実装順序

- **U1**: US2.2（エラー型）→ US2.1（feature分割）→ US3.1（Memory準拠化）。エラー型の確定がfeature独立コンパイルの前提（stories.md の依存欄どおり）
- **U2**: US1.1（スケルトン中核）→ US1.2 → US1.3 → US2.3 → US3.2 → US1.4（Could・最後）
- **U3**: US3.3のみ
- **U4**: US4.1 → US4.2

## カバレッジ検証

- 全12ストーリーが割り当て済み（漏れなし）
- 全4ユニットに1件以上のストーリーが割り当て済み（空ユニットなし）

## Assumptions & Open Questions

None.
