# Open Job Protocol (OJP) PoC

Requester が事前許可した予算内で Parent Worker が自己資金や都度承認なしに Child を発注し、
成功・失敗・失効・異議・再試行・同時実行を経ても資金と成果物への権利が破綻しないことを
証明する PoC 実装。設計は [docs/poc-implementation-plan.md](docs/poc-implementation-plan.md) を正本とする。

## 現在の実装状況

Phase 1（uv環境・型・DB migration・共有Clock・fixture）まで実装済み。
Phase 2 以降（Budget/Journal/Job/Lease/CLI/MCP tools 本体）は未実装。

## 解決した版

実装開始時点（2026-09-06）で解決・固定した版:

| 要素 | 版 |
|---|---|
| Python | CPython 3.12.14（`.python-version` で固定） |
| MCP Python SDK | 2.1.1（`mcp>=2.1.1,<3` 上限付きで lock 固定） |
| Pydantic | 2.13.5 |
| pytest | 8.4.2 |

uv.lock に完全な依存閉包が記録されている。

## セットアップ

```bash
uv sync --locked --group dev
```

システム Python へ pip install しない。依存はプロジェクト内 `.venv` へ入る。

## テスト

```bash
uv run pytest
```

- `tests/unit/`: 金額の整数変換（6桁小数文字列⇔1 mock-USDC=1,000,000 units）、
  ドメイン型・policy 不変条件、固定カタログ fixture
- `tests/integration/`: 実ファイル SQLite での新規 DB 作成・外部キー・UNIQUE・
  非負制約・明示トランザクションの rollback/commit、実プロセス複数起動による
  共有 Clock の観測・再起動後の保持・後退拒否・mode 不一致拒否、
  MCP SDK 2.x の stdio 接続スモークテスト

## パッケージ構成

- `src/ojp/domain.py`: モデル型・状態・policy・不変条件・金額変換（第5節）
- `src/ojp/db.py`: 接続PRAGMA（foreign_keys / WAL / synchronous=FULL / busy_timeout）、
  明示 BEGIN/COMMIT/ROLLBACK、番号付き SQL migration 実行（第16節）
- `src/ojp/migrations/001_initial.sql`: 初期スキーマ。全参照に外部キー、金額に非負制約、
  RuntimeClock 行（singleton_id=1）を含む
- `src/ojp/clock.py`: 全プロセス共有 Clock。書込 transaction 内で1回だけ now を採取、
  test mode は DB 保存時刻、harness 専用の時刻前進（後退拒否・同値 no-op）（第7節）
- `src/ojp/mcp_server.py`: MCP stdio サーバー（Phase 1 は接続確認用の最小構成）
- `tests/fixtures/poc_catalog.json`: 第11節の固定カタログ
  （Root: sum-v1 / Child: part-1..3、各予算上限 20.000000）

## MCP サーバー起動（stdio）

```bash
uv run python -m ojp.mcp_server
```

## Clock の扱い

- 書込処理は `BEGIN IMMEDIATE` 取得後に `runtime_clock` を読み、realtime なら UTC 実時刻、
  test mode なら保存済み `test_now_utc_us` をその transaction の now として1回だけ採取する
- mode は DB 初期化時に固定し、既存 DB ではトリガーで変更不可。
  起動設定と DB の mode が異なれば `MODE_MISMATCH` で起動を拒否する
- 時刻前進（`clock.set_test_now`）はテスト harness 専用。後退は `CLOCK_BACKWARD` で拒否、
  同じ時刻への設定は no-op。通常 CLI・MCP・Job API に時刻操作は存在しない
