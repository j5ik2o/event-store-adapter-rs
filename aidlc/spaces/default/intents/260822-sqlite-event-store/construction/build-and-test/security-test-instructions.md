# セキュリティテスト手順 — build-and-test (security-test-instructions)

devsecops観点のセキュリティ検証手順。OSSライブラリのため、DAST・認証テスト等のランタイム検査は対象外。検証面は (1) 依存の脆弱性・ライセンス、(2) unsafe/panic/機密の静的検査、(3) エラーメッセージの機密非混入、の3つ。

## 1. 依存監査（U3で恒久化済み — SCA）

```bash
cargo deny check advisories licenses
```

- `deny.toml` の許可リスト（MIT / Apache-2.0 / BSD-2/3 / ISC / Unicode-3.0）と RUSTSEC ignore 4件（h2 / rustls-webpki — AWS SDKレガシーTLSスタック由来、理由付き）を適用
- CIでは cargo-deny-action@v2 が PR・push・日次cronで実行（`.github/workflows/ci.yml` audit ジョブ）

## 2. 静的検査（コード衛生）

```bash
grep -rn "unsafe impl" lib/src/ && echo NG || echo OK        # 手書きunsafe不在（project.md Forbidden）
grep -rn "panic!\|unwrap()" lib/src/event_store_for_sqlite.rs | grep -v "_test" | grep -v "#\[cfg(test)\]" || echo "本体経路にpanicなし"
grep -rnE "(api[_-]?key|secret|password|token)\s*=" README.md README.ja.md CHANGELOG.md docs/ examples/ lib/src/ || echo "機密リテラルなし"
```

## 3. エラー契約テスト（機密・内部情報の非漏えい）

- `test_optimistic_lock_message*`（U1）: BR1.2書式のメッセージに aid / version 以外の情報（接続文字列・パス等）が混入しないことを固定
- `test_event_store_on_sqlite_error_contract`（U2）: バックエンド中立なエラー表現（AWS型リーク・panicなし）を固定

```bash
cargo test -p event-store-adapter-rs --all-features test_optimistic_lock_message
cargo test -p event-store-adapter-rs --no-default-features --features sqlite test_event_store_on_sqlite
```

## 判定基準

- cargo-deny: advisories / licenses 両チェックがエラー0（ignore済みRUSTSEC 4件は理由付き受容 — 解消はAWS SDK TLS移行バックログ）
- grep検査: unsafe impl 0件・機密リテラル0件
- エラー契約テスト: 全パス
