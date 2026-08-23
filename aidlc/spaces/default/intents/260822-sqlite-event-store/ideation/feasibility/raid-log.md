# RAIDログ: SQLite対応EventStoreとバックエンドfeature分割

本ログは実現性評価（`feasibility-assessment.md`）の詳細リスク台帳である。前提はインテントステートメント（`../intent-capture/intent-statement.md`、intent-statement）、競合分析（`../market-research/competitive-analysis.md`、competitive-analysis）、市場トレンド（`../market-research/market-trends.md`、market-trends）、Build vs Buy 評価（`../market-research/build-vs-buy.md`、build-vs-buy）。

## Risks（リスク）

| ID | リスク | 発生可能性 | 影響 | 対応方針 |
|---|---|---|---|---|
| R1 | SQLiteの単一ライタ特性により、同時書き込みが直列化され、既存バックエンドと並行動作特性が異なる | 中 | 低（CLI用途では単一プロセス利用が主 — market-trends） | 受容＋テストで挙動を明示検証。ドキュメントに特性を記載 |
| R2 | featureマトリクス（バックエンド3種＋バンドル/システム）の組み合わせ増でCI時間・保守負荷が増える | 高 | 中 | 軽減: 代表的な組み合わせに絞ったマトリクス設計（CI変更は自由 — 制約C9） |
| R3 | バンドル方式がCコンパイラ等のビルド環境依存を持ち込み、一部利用者環境でビルドできない | 低 | 中 | 軽減: システムSQLite版featureとの両対応（制約C2）で回避手段を提供 |
| R4 | デフォルトfeatureなし化により、既存利用者のビルドが更新時に壊れ、問い合わせ・混乱が発生する | 高（意図された破壊的変更） | 中 | 受容: CHANGELOGでの明示（intent-statement のリリース方針）。移行手順の記載を検討 |
| R5 | 楽観的ロックのSQLite実装がトランザクション分離レベルの理解不足により競合条件を持つ | 低 | 高 | 軽減: 既存バックエンドと同等のテスト（成功指標）＋並行書き込みテストの追加を後続工程で検討 |

## Assumptions（前提）

| ID | 前提 | 検証方法 |
|---|---|---|
| A1 | 主要なSQLiteドライバクレートが依存最小・バンドル/システム両対応の条件を満たす | 設計工程でのドライバ選定・検証 |
| A2 | 既存 `StorageBackend` トレイト契約（スナップショット・イベント取得・条件付き更新）はSQLiteのトランザクションで完全に実装できる | 設計工程でのスキーマ設計とプロトタイプ検証 |
| A3 | SQLiteバックエンドのテストはコンテナ不要（ファイル/`:memory:`）で既存テスト基盤に統合できる | テスト設計時に確認 |

## Issues（顕在化した問題）

| ID | 問題 | 状態 |
|---|---|---|
| — | 現時点でなし | — |

## Dependencies（依存関係）

| ID | 依存 | 種別 | 備考 |
|---|---|---|---|
| D1 | SQLiteドライバクレート（1つのみ許容 — 制約C1。選定は設計工程） | 外部 | build-vs-buy の自作方針の範囲内（ドライバ利用は許容） |
| D2 | 既存のトレイト定義（`StorageBackend` / `EventStore`）とテスト基盤（test-utils） | 内部 | シグネチャ不変更（制約C3） |
| D3 | GitHub Actions CI（featureマトリクス追加先） | 内部 | 変更自由（制約C9） |

## Assumptions & Open Questions

None.（本ログのAssumptionsセクションが前提の台帳であり、追加の未確認事項はない）
