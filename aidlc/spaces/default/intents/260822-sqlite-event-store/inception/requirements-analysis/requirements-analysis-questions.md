# 要件分析 質問票 (requirements-analysis-questions)

前提: インテントステートメント・スコープ定義書・チームプラクティス（team-practices.md）・コード知識ベース（business-overview / architecture / code-structure）で要件の大半は確定済み。ここでは要件化にあたり未確定の3点のみを確認する。

## Q1. Memory バックエンドの扱いを確定させてください。コード解析で、Memory だけが内部抽象（StorageBackend）に準拠せず、作成イベントを persist_event に渡すと panic する非対称が見つかっています（TD-05）。feature分割で Memory は常時有効のまま残ります。

A. 今回のスコープで抽象準拠に直す — StorageBackend + GenericEventStore 経由へリファクタし、panic も Err 返却に修正（挙動契約を4バックエンドで統一）
B. panic の Err 化のみ今回直す — 抽象準拠リファクタは行わない（最小修正）
C. 今回は触らない — feature分割の対象外とし現状維持（非対称はバックログ）
D. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. 今回のスコープで抽象準拠に直す — StorageBackend + GenericEventStore 経由へリファクタし、panic も Err 返却に修正（挙動契約を4バックエンドで統一）
## Q2. 未使用・宣言のみの依存の整理を今回のスコープに含めますか？（コード解析の指摘: `aws-http` は未使用のまま無条件依存、`prost` は宣言のみ、`serial_test` は未使用）

A. 含める — feature分割作業の一部として未使用依存を削除する
B. 含めない — 依存整理は別途（今回は feature 化対象の依存のみ触る）
X. Other (please specify)

[Answer]: A. 含める — feature分割作業の一部として未使用依存を削除する
## Q3. スナップショット保持ポリシー（保持数 keep_snapshot_count・TTL削除 delete_ttl）のSQLite対応は Could 優先度で確定済みですが、既定の扱いを決めてください。参考: 既存でも完全実装は DynamoDB のみで、Bigtable は設定できても効かないサイレント無効です（TD-08）。

A. SQLiteでは実装する — DynamoDBと同等の保持ポリシーをSQLiteでも提供（Couldだが実装を既定に）
B. 今回は実装せず機能差を文書化する — 設定APIは受け付けず（または明示的に未対応と文書化し）、Bigtableのようなサイレント無効は作らない
C. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. SQLiteでは実装する — DynamoDBと同等の保持ポリシーをSQLiteでも提供（Couldだが実装を既定に）

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q3）を提示し、要件書生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct