# NFR設計 質問 — u1-backend-features

> U1（feature分割・エラー型中立化・Memory準拠化）のNFR設計工程の質問。
> 設計パターンの大枠は上流（NFR要件・契約・機能設計）で確定済みのため、
> 実装を拘束する残余の設計判断のみを確認する。

## Q1: Memory準拠化のスレッド安全設計

Memoryバックエンドを `StorageBackend` + `GenericEventStore` 準拠へ書き換える際、共有状態 `InMemoryStoreState`（aid → events / snapshots）の同期プリミティブが未確定です。手書き `unsafe impl Send/Sync` を禁止（NFR-4.2）し自動導出で `Send + Sync + Clone` を満たす必要があります。

A. `Arc<Mutex<HashMap<...>>>` — 最もシンプル。読み書きとも排他。テスト用途のバックエンドとして十分で、自動導出も確実
B. `Arc<RwLock<HashMap<...>>>` — 読み取り並行性が高い。わずかに複雑だが読み出し中心のリプレイに有利
C. `DashMap` 等の並行コレクション導入 — 依存が1つ増える（NFR-2の依存最小方針と緊張関係）
X. Other (please specify)

[Answer]: A. `Arc<Mutex<HashMap<...>>>` — ただしユーザー追記の設計制約付き: 「Arc<Mutex<T>>をユーザにまるごと公開するとユーザがロック管理しないといけない。可能であれば、Arc<Mutex<T>>を隠蔽したほうがいい」。よって Arc<Mutex<...>> はバックエンド構造体の非公開フィールドとして完全に隠蔽し、公開API（`EventStoreForMemory::new()` / `EventStore` トレイト）にはロック型・ガードを一切露出させない。ロック取得は各 StorageBackend メソッド内部で完結する

## Q2: NFR検証手順の設計上の位置づけ（U1時点）

NFR-2.1（依存グラフに含まれるクラウドSDKの検査 — cargo tree）・NFR-4.2（unsafe不在の検査 — grep）・NFR-3.1（featureマトリクスのビルド確認）の検証は、U1時点ではCI未整備（CI化はU3の責務）です。

A. 設計書に手動ローカル検証手順（コマンド列）として記載する — U3がそれをCIマトリクスへ昇格する
B. U1でリポジトリに検証スクリプト（shell等）を追加し、設計書はそれを参照する
X. Other (please specify)

[Answer]: A. 設計書に手動ローカル検証手順（コマンド列）として記載する

## Q3: 楽観的ロック失敗メッセージの正式書式

BR1.2の例示形式を正式書式として設計固定するかの確認です。全バックエンド共通の整形文字列とし、実バージョンが判明する場合のみ付加情報を許します。

A. `optimistic lock failed, aid=<id>, expected_version=<n>` を基本形とし、判明時のみ `, actual_version=<m>` を付加する形で固定する
B. 書式を変更する（具体案を指定してください）
X. Other (please specify)

[Answer]: A. 基本形で固定する（判明時のみ actual_version を付加）

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
