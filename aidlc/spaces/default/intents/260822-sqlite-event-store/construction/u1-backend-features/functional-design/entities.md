# エンティティモデル — u1-backend-features (entities)

ユニット定義（`../../../inception/units-generation/unit-of-work.md` U1）とストーリー対応（`../../../inception/units-generation/unit-of-work-story-map.md`: US2.1/US2.2/US3.1）、要件（`../../../inception/requirements-analysis/requirements.md` FR-2.x/FR-3.x）、コンポーネントカタログ（`../../../inception/domain-design/components.md`）、契約（`../../../inception/contract-design/contract-summary.md` C-1/C-2/C-3）に基づく。U1は永続データを持たない基盤整合ユニットであり、エンティティは「エラー表現」と「Memoryの内部状態」の2つの型モデルに限られる。

## Source of truth

```yaml
entities:
  - name: OptimisticLockErrorContext
    description: 楽観的ロック失敗の説明文字列（Q&AでA確定 — 単一String。構造体ではない）
    attributes:
      - name: message
        logical_type: string
        required: true
        unique: false
        constraints: "集約ID・期待バージョン・実バージョン（判明分）を人間可読な1行に整形して含める"
    entity_constraints:
      - "バックエンドSDK型（TransactionCanceledException等）への参照を含まない（FR-2.4）"
    relationships: []

  - name: InMemoryStoreState
    description: Memoryバックエンドの内部状態（StorageBackend準拠化後の保持構造 — 公開されない）
    attributes:
      - name: aid
        logical_type: string
        required: true
        unique: true
        constraints: "集約ID文字列 — エントリのキー"
      - name: events
        logical_type: list<serialized-event>
        required: true
        unique: false
        constraints: "seq_nr昇順"
      - name: snapshots
        logical_type: list<serialized-snapshot>
        required: true
        unique: false
        constraints: "最新スナップショットが解決可能であること"
    entity_constraints:
      - "共有ストア（Clone間で状態を共有する参照型の内部構造）とし、Clone時の状態分岐（既知欠陥）を解消する"
    relationships: []
```

## サマリー

- **OptimisticLockErrorContext**: 公開エラー enum `EventStoreWriteError::OptimisticLockError(String)` の中身。全4バックエンドが同一形式で整形する（形式は rules.md BR1.2）
- **InMemoryStoreState**: Memory準拠化（FR-3.1）後の内部状態。`StorageBackend` 実装が読む/書く唯一の構造で、Clone間共有により既知のClone状態分岐を解消する
- U1はDBスキーマを持たない（SQLiteスキーマはU2の設計対象）。feature分割・依存削除はエンティティを生まない構成変更であり、rules.md / functional-spec.md 側で規定する

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-22T23:19:32Z
**Iteration:** 2

### Findings

| # | Severity | Location | Finding | Status |
|---|---|---|---|---|
| 1 (iter1) | Critical | rules.md, BR1.2の`logic` | `logic: >-` のブロックスカラー化と例文からのコロン除去を確認。`python3 -c "import yaml; yaml.safe_load(...)"` でrules.mdの`\`\`\`yaml`ブロックを再パースし、9件のルール（BR1.1〜BR1.9）全件を含めて正常にロードできることを実機で再検証した（entities.mdの2エンティティも同様にOK）。 | Resolved |
| 2 (iter1) | Major | traceability.json（AC2.2.2 target）／functional-spec.mdワークフロー1手順3 | traceability.jsonのAC2.2.2エントリの`target`フィールドに「BR1.2（U1範囲の検証はMemory側のみ。sqlite側のランタイム検証はU2完了時に充足 — unit-of-work-story-map.mdの橋渡しACと同扱い）」の限定注記が追加され、`python3 -m json.tool`相当のパース（`json.load`）でも構文エラーなしを確認した。functional-spec.mdワークフロー1手順3にも同旨の注記（「AC2.2.2のU1範囲の充足はMemory側のみで、sqlite側のランタイム検証はU2完了時に成立する（橋渡しAC）」）が追記され、両ファイルの記述が整合している。unit-of-work.md U1境界注記（sqlite実装はU2）およびunit-of-work-story-map.mdの既存の橋渡しAC注記パターン（AC2.1.1/AC2.1.3）との整合も確認した。 | Resolved |

新規のCritical/Major所見なし。

### Validation Tool Results

| Tool | Result | Interpretation |
|---|---|---|
| YAML再パース（entities.md / rules.mdの`\`\`\`yaml`ブロック） | 両方OK（entities: 2件、rules: 9件全ロード） | Finding #1（iter1）の解消を裏付け |
| JSON再パース（traceability.json、`json.load`） | OK（構文エラーなし） | Finding #2（iter1）の注記追加後もJSONとして妥当 |
| AC⇄注記の突合（AC2.2.2の限定注記とfunctional-spec.mdワークフロー1手順3の記述） | 一致（Memory側のみ充足、sqlite側はU2完了時） | Finding #2（iter1）の解消を裏付け |

### Summary

前回指摘の2件（rules.mdのYAML構文誤り、traceability.jsonのAC2.2.2過大主張）はいずれも実機での再検証（YAML/JSON再パース、注記内容の突合）により解消を確認した。新たなCritical/Major所見はなく、他ファイルの改変も本レビュー対象外の範囲に収まっている。READYと判定する。
