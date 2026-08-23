# NFR設計 質問 — u3-ci-quality

> U3（CI品質保証）のNFR設計工程の質問。ツール選定（cargo-deny）・実行タイミング
> （全トリガー）はNFR要件工程で確定済み。設計上の残余のみを確認する。

## Q1: deny.toml のライセンス検査方針

cargo-deny の licenses チェックには許可ライセンスの列挙（allow-list）が必要です。

A. 現依存グラフの実ライセンス構成から許可リストを生成（MIT / Apache-2.0 / BSD系 / Unicode系等の許容的ライセンス）し、コピーレフト（GPL系）は不許可とする — 標準的な許容的OSSクレートの方針。将来の依存追加で未許可ライセンスが入ると検査が失敗し顕在化する
B. licenses チェックは見送り advisories のみ検査する（NFR要件のQ1確定を縮小 — 非推奨）
X. Other (please specify)

[Answer]: A. 現依存グラフの実ライセンス構成から許可リストを生成し、コピーレフト（GPL系）は不許可とする

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
