<!-- INVARIANT: examples are single-line HTML comments so a fresh template parses to total=0 (MEMORY_EMPTY). Do NOT un-comment or split across lines. t100 guards this. -->
> This file is kept up to date automatically while the stage runs. Add observations at the review step, not by editing here directly.

## Interpretations
<!-- example: 2026-05-29T10:14:32Z — chose REST over GraphQL; the consuming team only needs CRUD, revisit if subscriptions land -->
- 2026-08-22T23:28:58Z — U1はキンド適用でperformance/scalability/reliability/observabilityの要件書が対象外となるため、互換性（NFR-1.x）・ビルド（NFR-3.x）の派生要件は tech-stack-decisions.md に置いた; 専用要件書を持たないNFRの置き場としてこの解釈を採った（security系はsecurity-requirements.mdに既配置）。
- 2026-08-22T23:28:58Z — security-requirements.md 書込時のupstream-coverageセンサー失敗（3件）は前ステージのスラッグ（functional-design）の消費契約に対する照合であり本ステージの契約とは不一致; 本ステージの消費物（functional-spec/rules/requirements/contract-summary/technology-stack）は成果物冒頭で全参照済みのため助言扱いとし修正不要と判断。

## Deviations
<!-- example: 2026-05-29T10:14:32Z — skipped the optional caching layer the stage prose suggested; the dataset is small enough that it adds risk -->
- 2026-08-22T23:28:58Z — U1では質問工程（Step 4-5）を実施しなかった; NFRターゲット・技術選定はすべて上流（要件NFR-1〜5、契約C-3、team.md確定事項）で決定済みで未確定の残余がなく、Constructionの質問は例外運用（真のギャップのみ）という規範に従った。

## Tradeoffs
<!-- example: 2026-05-29T10:14:32Z — picked TDD over BDD this run; the team is unit-first and the domain is well-understood -->
- 2026-08-23T03:54:31Z — U2: cargoのfeature加算性により契約C-3の「sqlite-system併用で切替」は実現不能のため、sqlite-system単独指定を正規の使い方とし併用時はbundled優先（libsqlite3-sys優先規則）と確定; feature名はC-3のまま維持し意味論のみ確定、U4ドキュメントへ明記を引き渡し（tech-stack-decisions D3）。派生NFR IDはU1との衝突回避で連番継続（NFR-2.3〜/NFR-4.5〜/NFR-5.1）。
- 2026-08-22T23:28:58Z — NFR-5（テスト実行環境）はU1のトレーサビリティでN/Aとした; U1スコープの派生要件（例: 既存テスト維持）を起こす案もあったが、既存テスト緑維持はBR1.9が既に担っており二重定義を避けた。SQLiteテスト環境要件はU2のNFR要件工程で導出する。

## Open questions
<!-- example: 2026-05-29T10:14:32Z — confirm the retention window with compliance before the next stage hardens the schema -->
