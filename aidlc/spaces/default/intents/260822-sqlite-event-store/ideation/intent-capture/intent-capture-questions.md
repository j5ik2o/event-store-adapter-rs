# インテント把握 質問票 (intent-capture-questions)

## Sources

- [desc] Initial description: "SQLiteに対応したEventStore実装を作りたい。またどのイベントストアを使うかfeatureを考慮してほしい。3種類分"
- [scope] Workflow-selected scope: `library`.

## Q1. このSQLite対応で解決したい中心的な課題は何ですか？

現在の event-store-adapter-rs は DynamoDB / Bigtable / Memory バックエンドを提供しています。SQLite対応を加える動機として最も近いものを選んでください。

A. ローカル開発・テストを外部サービス（DynamoDB Local等）なしで完結させたい
B. 組み込み・スタンドアロン・小規模本番（CLI、デスクトップ、単一サーバ等）でクラウド非依存の軽量ストアとして使いたい
C. ライブラリとしてのバックエンドの網羅性を高め、採用の間口を広げたい（OSSとしての魅力向上）
D. まだ明確に定義していない（Not yet defined）
X. Other (please specify)

[Answer]: X. Other — 「CLIツールで使いたいのです。」（クラウド非依存のCLIツールでの利用が目的）
## Q2. 「3種類分」のfeature分割の意図を確認させてください

「どのイベントストアを使うかfeatureを考慮してほしい。3種類分」とのことですが、cargo featureで切り替える対象の解釈として正しいものを選んでください。

A. `dynamodb` / `bigtable` / `sqlite` の3つをfeature化する（Memoryは常時有効のまま）
B. `dynamodb` / `bigtable` / `memory` / `sqlite` の4つすべてをfeature化する（「3種類」は既存3バックエンドを指していた）
C. `dynamodb` / `sqlite` / `memory` の3つをfeature化する（Bigtableは対象外・現状維持または削除）
D. まだ決めていないので提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: X. Other — 「dynamodb, cloudspanner, sqliteでは？bigtable対応」（意図が曖昧なため Q9 で明確化）
## Q3. デフォルトfeature（何も指定しない利用者が得るもの）はどうしますか？

既存利用者は現在すべてのバックエンドが使える状態です。feature化により既存利用者への影響（後方互換性）が変わります。

A. デフォルトは従来どおり全バックエンド有効（既存利用者は無変更で動く。opt-outで軽量化）
B. デフォルトは DynamoDB のみ（クレートの主目的を維持しつつ依存を削減。他はopt-in）
C. デフォルトfeatureなし（利用者が必ず明示選択。破壊的変更だが最も明確）
D. まだ決めていないので提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: C. デフォルトfeatureなし（利用者が必ず明示選択。破壊的変更だが最も明確）
## Q4. この機能の主な利用者（顧客）は誰ですか？

A. 外部のOSS利用者（crates.io経由でこのライブラリを使う一般のRust開発者）
B. 自分自身・自分のプロジェクト群（本ライブラリを使う自作アプリケーション）
C. 両方（外部利用者と自プロジェクトの双方）
D. 特定していない（Not identified）
X. Other (please specify)

[Answer]: A. 外部のOSS利用者（crates.io経由でこのライブラリを使う一般のRust開発者）
## Q5. 成功をどう測りますか？（測定可能な成果）

A. SQLiteバックエンドが既存のEventStoreトレイトの全機能（イベント永続化・スナップショット・楽観的ロック）をパスし、既存テストと同等のテストが緑になること
B. Aに加えて、feature単位のビルドが各組み合わせで通ること（CI でfeatureマトリクスを検証）
C. Aに加えて、依存削減の実測（例: sqliteのみ利用時にAWS SDK等がビルド対象から外れる）
D. まだ定義していない（Not yet defined）
X. Other (please specify)

[Answer]: A. SQLiteバックエンドが既存のEventStoreトレイトの全機能（イベント永続化・スナップショット・楽観的ロック）をパスし、既存テストと同等のテストが緑になること
## Q6. このイニシアチブのきっかけ（なぜ今か）は何ですか？

A. 実際に使いたいユースケース・プロジェクトが今ある
B. 利用者・コミュニティからの要望があった
C. 技術的負債の解消（feature化はいずれ必要で、SQLite追加が良い契機）
D. 特にない・機会があったから（None）
X. Other (please specify)

[Answer]: A. 実際に使いたいユースケース・プロジェクトが今ある（CLIツールでの利用）
## Q7. ステークホルダーと意思決定者を確認させてください

A. 意思決定者は自分（メンテナ）のみ。他に影響を受けるのはOSS利用者（後方互換性に配慮が必要）
B. 意思決定者は自分のみ。外部利用者への影響は考慮不要（破壊的変更も自由に可能）
C. 複数のメンテナ・コントリビュータの合意が必要
D. 特定していない（Not identified）
X. Other (please specify)

[Answer]: A. 意思決定者は自分（メンテナ）のみ。他に影響を受けるのはOSS利用者（後方互換性に配慮が必要）
## Q8. ワークフローのスコープは `library`（ライブラリ開発向け・運用フェーズなし）を選択しています。この範囲はあなたの意図するプロダクト境界と合っていますか？

A. 合っている（ライブラリ本体の機能追加として進める。デプロイ・運用工程は不要）
B. ほぼ合っているが、examples やドキュメントの整備も成果物に含めたい
C. 合っていない（別の範囲を意図している — Xで具体的に指定してください）
X. Other (please specify)

[Answer]: A. 合っている（ライブラリ本体の機能追加として進める。デプロイ・運用工程は不要）
## Q9. （Q2フォローアップ）feature分割対象の確認 — 現在このクレートに実装済みなのは Bigtable であり、Cloud Spanner 対応は存在しません

Q2のご回答で「cloudspanner」に言及がありましたが、現状のバックエンドは DynamoDB / Bigtable / Memory です。feature 3種の対象を確定させてください。

A. `dynamodb` / `bigtable` / `sqlite` の3つをfeature化する（既存Bigtableをそのまま対象に。Memoryは常時有効）
B. `dynamodb` / `cloudspanner` / `sqlite` の3つ — Bigtableを廃止し、Cloud Spanner対応を新規に追加する（大幅なスコープ拡大）
C. `dynamodb` / `bigtable` / `sqlite` の3つ＋将来的にCloud Spanner追加を見据えた拡張可能なfeature構成にする（今回実装は3つ）
D. 再考したい（Not yet defined）
X. Other (please specify)

[Answer]: A. `dynamodb` / `bigtable` / `sqlite` の3つをfeature化する（既存Bigtableをそのまま対象に。Memoryは常時有効）※ユーザー確認: 「私の間違いかも。bigtableの対応のみで、cloudspannerは未対応か」— cloudspanner言及は誤認と確定

## Q10. （矛盾解消フォローアップ）「デフォルトfeatureなし（破壊的変更）」と「OSS利用者への後方互換性配慮」の両立方法

Q3で「デフォルトfeatureなし（破壊的変更だが最も明確）」を、Q7で「OSS利用者への後方互換性に配慮が必要」を選択されました。この2つを両立させる方針を確定させてください。

A. メジャーバージョンアップとしてリリースする（semver準拠で破壊的変更を明示し、READMEに移行ガイドを記載）
B. Q3を見直し、デフォルトは従来どおり全バックエンド有効にする（破壊的変更を避ける）
C. 破壊的変更として通常リリースする（配慮はCHANGELOGへの記載のみ）
D. まだ決めていない（Not yet defined）
X. Other (please specify)

[Answer]: C. 破壊的変更として通常リリースする（配慮はCHANGELOGへの記載のみ）

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q10）を提示し、アーティファクト生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct