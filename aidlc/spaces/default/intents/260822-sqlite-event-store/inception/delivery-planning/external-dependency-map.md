# 外部依存マップ (external-dependency-map)

Bolt計画（`bolt-plan.md`）の各Bolt（設計〜実装〜テストを一気に通すビルドパス）が待つ外部要因の台帳。

## 外部依存

本ビルドは完全にAI＋メンテナで完結し、外部チーム・外部承認・データ提供窓口への依存はない。ライブラリとしての外部依存はクレート取得のみ:

| 依存 | 種別 | 所有 | ブロックするBolt | 遅延時の代替 |
|---|---|---|---|---|
| rusqlite（crates.io） | 依存クレート取得 | crates.io / rusqliteメンテナ | Bolt 1 | バージョン固定で取得安定化。取得不能時は既知の安定版へピン |
| cargo-audit / cargo-deny（CIツール） | CIツール取得 | RustSec / EmbarkStudios | Bolt 3 | どちらか取得可能な方を採用（要件は「いずれか」） |

## 補足

- 承認リードタイム: すべてのゲート承認はメンテナ本人のため、外部起因の待ちはない
- 契約（`../contract-design/contract-summary.md`）上の外部消費者（crates.io利用者）はリリース後の関係であり、ビルドをブロックしない

## Assumptions & Open Questions

None.
