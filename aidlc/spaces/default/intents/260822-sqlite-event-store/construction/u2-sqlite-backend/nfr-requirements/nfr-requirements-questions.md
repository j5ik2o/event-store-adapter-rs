# NFR要件 質問 — u2-sqlite-backend

> U2（SQLiteバックエンド本体）のNFR要件工程の質問。NFRターゲットの大半は
> 上流（要件NFR-1〜NFR-5、契約C-1〜C-4、チームプラクティス確定事項、
> U2機能設計のQ&A）で確定済みのため、未確定の残余のみを確認する。

## Q1: rusqlite のバージョン方針

U2で追加する唯一のランタイム依存 rusqlite のバージョン管理方針の確認です。

A. 追加時点の最新安定版を workspace の依存テーブルに固定し、以後は Renovate の自動更新（minor/patch automerge）に追随する — 既存依存と同じ運用
B. 特定バージョンに固定し Renovate 対象から除外する（理由を指定してください）
X. Other (please specify)

[Answer]: A. 追加時点の最新安定版を workspace の依存テーブルに固定し、以後は Renovate の自動更新に追随する

## Q2: 同梱（bundled）SQLite の脆弱性対応の担保

`sqlite` feature の既定はバンドル（SQLite本体のC実装を同梱コンパイル）のため、SQLite本体由来のCVEはクレートの供給網に入ります。対応の担保方法の確認です。

A. RUSTSEC照合（cargo-audit / cargo-deny advisories）はU3のCI（日次実行）が担保し、SQLite本体の更新は rusqlite / libsqlite3-sys のバージョン更新（Renovate追随）で取り込む — 手動の追加運用なし
B. 追加の担保を設ける（内容を指定してください）
X. Other (please specify)

[Answer]: A. RUSTSEC照合はU3のCI（日次）が担保し、SQLite本体更新は rusqlite / libsqlite3-sys のバージョン更新（Renovate追随）で取り込む

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
