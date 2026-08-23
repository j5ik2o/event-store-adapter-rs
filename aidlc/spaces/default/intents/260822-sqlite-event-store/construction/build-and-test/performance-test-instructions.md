# 性能テスト手順 — build-and-test (performance-test-instructions)

## 適用判定 — 対象なし

Standard戦略では性能テスト手順書は生成対象外であり、かつ本ワークフローには数値性能NFRが存在しない（NFR要件工程の全ユニットで性能系要件書はキンド適用によりN/A。requirements.md のNFRにも数値性能目標はない）。負荷テスト・ベンチマーク・回帰検出の対象となるサービス・エンドポイントも存在しない（OSSライブラリ — クラウドインフラなし）。

## 参考（将来必要になった場合）

- ベンチマークが必要になった場合は `criterion` 等のベンチクレートを dev-dependencies に追加し、`persist_event` / `get_latest_snapshot_by_id` のマイクロベンチとして実装するのが最小構成
- SQLiteの実運用性能はDB配置（ファイル/`:memory:`）とトランザクション頻度に支配されるため、負荷特性はアプリケーション側での計測が正
