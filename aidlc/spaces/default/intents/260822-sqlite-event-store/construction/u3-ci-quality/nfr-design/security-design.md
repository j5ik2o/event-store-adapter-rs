# セキュリティ設計 — u3-ci-quality (security-design)

セキュリティ要件（`../nfr-requirements/security-requirements.md` NFR-2.5/2.6・NFR-4.9〜4.11）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D6）を、U3（CI品質保証）の具体設計へ落とす。性能系設計書・logical-components はキンド適用で対象外（packagingユニット）。本ステージが consumes に数える機能設計成果物（functional-spec 等）はU3ではキンド適用で存在せず（機能設計工程で確認済み）、契約 `contract-summary.md`（C-3 feature表）とNFR要件が設計入力の正となる。

## 依存監査の設計（NFR-2.5）

- **`deny.toml`**（リポジトリルート・新規）:
  - `[advisories]`: RUSTSEC照合。重大度に関わらず未対応advisoriesは検査失敗（ignoreは理由コメント付きでのみ許容）
  - `[licenses]`: **許可リスト方式**（Q1=A確定） — 現依存グラフの実ライセンス構成（MIT / Apache-2.0 / BSD-2-Clause / BSD-3-Clause / Unicode系 / ISC / Zlib 等の許容的ライセンス — 実列挙はコード生成時に `cargo deny check licenses` の失敗出力から確定）を許可し、コピーレフト（GPL / AGPL / LGPL系）は列挙しない（＝不許可）。将来の依存追加で未許可ライセンスが入ると検査失敗で顕在化する
- **実行ジョブ**: `ci.yml` に `audit` ジョブを追加し、既存トリガー（PR・main push・日次cron）すべてで `cargo deny check advisories licenses` を実行（D2）。Renovate automerge に対するRUSTSEC照合ゲート

## featureマトリクスと検証の設計（NFR-2.6 / NFR-3.3 / NFR-4.10）

- **マトリクスジョブ**: 6構成（未指定／dynamodb／bigtable／sqlite／sqlite-system／全feature）のビルド＋Docker不要テスト（D4/D5 — testcontainers前提の統合テストは既存 `test-lib` に残し重複実行しない）。`sqlite-system` 構成は `libsqlite3-dev` を導入
- **依存グラフ検査**: feature未指定でクラウドSDK不在・sqlite構成で hashlink 不在（`cargo tree -e normal` — U1/U2引き渡しのgrepパターン）
- **unsafe不在検査**: `grep -rn "unsafe impl" lib/src/` が0件（U2引き渡し 項目4）

## 静的解析の設計（NFR-4.9）

- `clippy` ジョブ: `cargo clippy --workspace --all-targets -- -D warnings`（stable）。既知の既存負債1件（generic_event_storeテスト内eager clone）はコード生成時に扱いを確定（修正または理由付き最小抑制 — 新規モジュールのクリーン維持が最低線）

## CIジョブ健全性の設計（NFR-4.11）

- 監査・マトリクス・clippyジョブは資格情報・新規シークレット不要。ジョブ失敗はCIの失敗として顕在化（continue-on-error を使わない）
- 既存ジョブ（lint / test-lib）・ブランチ保護・リリースフローには触れない（必須チェックへの昇格は運用判断としてU3スコープ外 — ジョブ追加のみ）

## 検証手順（U3実装後の受け入れ確認）

```bash
cargo deny check advisories licenses      # ローカルでdeny.tomlの成立を確認
cargo clippy --workspace --all-targets -- -D warnings
# マトリクス6構成のビルド＋Docker不要テスト（CI上で全構成緑を確認）
```

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T07:02:48Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Minor | security-design.md「featureマトリクスと検証の設計」節（14行目）／「検証手順」節（32行目） | 「マトリクスジョブ: 6構成（未指定／dynamodb／bigtable／sqlite／sqlite-system／全feature）のビルド＋Docker不要テスト（D4/D5 — testcontainers前提の統合テストは既存test-libに残し重複実行しない）」と記載しているが、実際の`lib.rs`のcfgゲート（`#[cfg(all(test, feature = "dynamodb"))] mod event_store_for_dynamodb_test;` 等）により、`dynamodb`単独・`bigtable`単独・`全feature`の3構成では素の`cargo test --features <feature>`を実行すると、testcontainers（Docker）を要する`test_event_store_on_dynamodb`/`test_event_store_on_bigtable`がそのままテストバイナリに含まれ実行されてしまう。「未指定」「sqlite」「sqlite-system」の3構成は該当テストモジュールがコンパイル対象外のため文字どおり「Docker不要テスト」が成立するが、残り3構成では素のコマンドでは成立せず、D5が明言する目的（「マトリクス側で重複実行しない」＝CI時間の抑制）を達成するには、何らかのテストフィルタ（例: `--no-run`でビルドのみに留める、または`-- --skip <docker依存テスト名>`でスキップする）が必要になる。しかし本節にも「検証手順」節（32行目）にも、この6構成のうちどの3構成にどの機構を適用するかの記載がなく、後者は実コマンドを伴わないコメント行のみ（`# マトリクス6構成のビルド＋Docker不要テスト（CI上で全構成緑を確認）`）に留まっている。ライセンス許可リストの具体列挙は「実列挙はコード生成時に確定」と明示的に実装時決定へ委譲されているのに対し、この機構は委譲の断りなく既定事項として記述されており、実装者が3構成それぞれにどのテスト実行方式を採るか判断に迷う可能性がある。 | 「featureマトリクスと検証の設計」に、`dynamodb`／`bigtable`／`全feature`の3構成では（a）`cargo test --no-run`でビルド検証のみとする、または（b）`--skip test_event_store_on_dynamodb --skip test_event_store_on_bigtable`等のフィルタでDocker依存テストを除外する、のいずれかを明記する（または「実装時にコード生成時点で確定」と明示的に委譲する）。「検証手順」節のコメント行も、選択した機構を反映した実コマンドに置き換える。 |

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| `cargo deny --version` | `cargo-deny not found`（未インストール） | 依頼記録の指示どおり、実挙動確認は断念し文書ベースで判断した。cargo-denyの`[licenses]`許可リスト方式（未列挙ライセンスは検査失敗）は公知の仕様と整合しており、Q1=A確定の反映（許可リスト方式・GPL系不許可）は技術的に妥当と判断した。 |
| `lib/src/lib.rs`のcfgゲート再確認（`#[cfg(all(test, feature = "dynamodb"))]`等） | 確認済み | 所見#1の直接根拠。`dynamodb`/`bigtable`単独featureビルドでは対応するtestcontainers依存テストモジュールがテストバイナリに含まれる。 |
| traceability.json 6 ID（NFR-2.5, 2.6, 3.3, 4.9, 4.10, 4.11）とsecurity-requirements.md/tech-stack-decisions.mdの再突合 | PASS | 6件とも上流（nfr-requirements、iteration 2でREADY確定済み）の派生IDと1:1で一致し、NFR-1・NFR-5（いずれもN/A）が正しく除外されている。target記述はいずれも本書内の実在する節を指している。 |
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS | 構文エラーなし。 |
| D1〜D6（tech-stack-decisions.md）との整合確認 | PASS | deny.toml（D1）・全トリガー実行（D2）・clippy（D3）・6構成マトリクス（D4）・重複回避の責務分担（D5）・新規アクション最小化（D6）のいずれも本書の記述と矛盾なく対応している。 |
| `.github/workflows/ci.yml`のブランチ保護不変更の主張確認（`required_status_checks.contexts: ["test-lib"]`のみ、既出確認の再想起） | PASS | 「既存ジョブ...ブランチ保護...には触れない」の記述は、以前のU1/U2レビューで確認済みの実ブランチ保護設定（`test-lib`のみが必須チェック）と矛盾しない。 |

### Summary

Q1確定（許可リスト方式・GPL系不許可）の反映は正確で、cargo-denyの公知の仕様とも技術的に整合しています。NFR-2.5/2.6・NFR-3.3・NFR-4.9〜4.11の6件はいずれも上流のnfr-requirements（iteration 2でREADY確定済み）と過不足なく対応し、D1〜D6の技術スタック決定とも矛盾していません。唯一の所見は、featureマトリクスの「Docker不要テスト」という表現が、`dynamodb`／`bigtable`／`全feature`の3構成では実際のcfgゲート構成上、素のコマンドでは成立せず、実現機構（ビルドのみ留めるかテストフィルタで除外するか）が明記されていない点です。ライセンス列挙のような明示的な実装時委譲の断りもないため、実装者が判断に迷う可能性がありますが、標準的な手段（`--no-run`または`--skip`）で解決可能な粒度の不足でありMinorとしました。Critical 0件・Major 0件・Minor 1件のためREADYと判定します。
