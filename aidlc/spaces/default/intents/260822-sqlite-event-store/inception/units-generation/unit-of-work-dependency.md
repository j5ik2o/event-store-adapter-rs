# ユニット依存DAG (unit-of-work-dependency)

ユニット定義（`unit-of-work.md`）の4ユニット間の依存トポロジー。本書は「何が何に依存できるか」のみを記述する。どれを先に出荷するかの経済的順序づけはデリバリ計画が決定する。前提はコンポーネントカタログ（`../domain-design/components.md`）・ADR（`../domain-design/decisions.md`）・要件定義書（`../requirements-analysis/requirements.md`）・ストーリー（`../user-stories/stories.md`）。

## 機械可読エッジブロック

```yaml
units:
  - name: u1-backend-features
    kind: library
    depends_on: []
  - name: u2-sqlite-backend
    kind: library
    depends_on: [u1-backend-features]
  - name: u3-ci-quality
    kind: packaging
    depends_on: [u2-sqlite-backend]
  - name: u4-docs
    kind: packaging
    depends_on: [u2-sqlite-backend]
```

## 依存の根拠

| エッジ | 根拠 |
|---|---|
| u2-sqlite-backend → u1-backend-features | SQLite実装は feature の器（`sqlite` feature宣言）と中立エラー型（ADR-001）の完成が前提（US1.1/US1.2の依存欄と整合） |
| u3-ci-quality → u2-sqlite-backend | featureマトリクスCI（FR-5.1）は `sqlite` feature の実装完了後でないと全組み合わせを検証できない |
| u4-docs → u2-sqlite-backend | ドキュメント（FR-6.1〜6.4）はU1/U2の最終的な公開APIとスキーマを記述する（U1へは推移的に依存） |

## 統合点

| ユニット間 | 統合点 |
|---|---|
| U1 ↔ U2 | `StorageBackend` トレイト（5メソッド）と中立エラー型 — U1が形を確定し、U2が実装する |
| U2 ↔ U3 | cargo feature 名（`sqlite` / バンドル・システムfeature）— CIマトリクスの軸 |
| U2 ↔ U4 | SQLiteスキーマ（journal / snapshot）と `EventStoreForSqlite` API — ドキュメントの記述対象 |

## 並行開発の機会

- **U3（CI）とU4（ドキュメント）は相互に独立** — U2完了後、並行して進められる（複数の有効なトポロジカル順序が存在する）
- U1→U2は直列（U2の前提がU1）。U1内部でも、エラー型再設計とfeature分割は同一の変更群として一体で進む（ADR-001/US2.1-US2.2の一体実施）

## Assumptions & Open Questions

None.
