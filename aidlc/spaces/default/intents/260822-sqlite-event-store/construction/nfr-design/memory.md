<!-- INVARIANT: examples are single-line HTML comments so a fresh template parses to total=0 (MEMORY_EMPTY). Do NOT un-comment or split across lines. t100 guards this. -->
> This file is kept up to date automatically while the stage runs. Add observations at the review step, not by editing here directly.

## Interpretations
<!-- example: 2026-05-29T10:14:32Z — chose REST over GraphQL; the consuming team only needs CRUD, revisit if subscriptions land -->
- 2026-08-23T04:29:41Z — U2: 同一ファイルDBの複数同時オープンはユーザー指示によりサポート外と確定（PRAGMA調整なし・SQLITE_BUSYはIOError即時返却・多重起動防止はアプリ責務）; サポートされる共有単位は「1インスタンスとそのClone」のみで、U4ドキュメントへの境界明記を引き渡し。
- 2026-08-22T23:53:44Z — U1はキンド適用でperformance/scalability/reliability/observabilityの設計書が対象外のため、成果物はsecurity-design.md・logical-components.md・traceability.jsonの3点とした; nfr-requirements工程のQ1確定（3点構成）と対称。

## Deviations
<!-- example: 2026-05-29T10:14:32Z — skipped the optional caching layer the stage prose suggested; the dataset is small enough that it adds risk -->

## Tradeoffs
<!-- example: 2026-05-29T10:14:32Z — picked TDD over BDD this run; the team is unit-first and the domain is well-understood -->
- 2026-08-22T23:53:44Z — Memory準拠化の同期はArc<Mutex>採用（RwLock・並行コレクション案を退けた — テスト用途で読み書き競合が小さく依存最小方針に合う）; ユーザー指示によりロック型は公開APIへ非露出・完全隠蔽とし、std::sync::Mutexでガードを跨ぐawaitを禁止する設計に固定。
- 2026-08-22T23:53:44Z — NFR検証はU1では手動コマンド列として設計書に記載（スクリプト化はせず、U3がCIマトリクスへ昇格する分担）; リポジトリへの一時スクリプト追加はU3のCI設計と二重化するため回避。

## Open questions
<!-- example: 2026-05-29T10:14:32Z — confirm the retention window with compliance before the next stage hardens the schema -->
