# Open Job Protocol (OJP) PoC

Requester が事前許可した予算内で Parent Worker が自己資金や都度承認なしに Child を発注し、
成功・失敗・失効・異議・再試行・同時実行を経ても資金と成果物への権利が破綻しないことを
証明する PoC 実装。設計は [docs/poc-implementation-plan.md](docs/poc-implementation-plan.md) を正本とする。

## 現在の実装状況

- **Phase 1〜5: 実装済み**
  - Phase 1: uv環境・型・DB migration・共有Clock・fixture
  - Phase 2: Mock資金・排他的口座・Journal・冪等な決済
  - Phase 3: JobVersion・Claim/Lease・Child policy・Parent終了
  - Phase 4: JSON提出・固定検証・承認・Dispute・tick・追加返金
  - Phase 5: 読取クエリ最適化・共通応答封筒・全CLIコマンド・MCP 8 tools・権限付き取得・CLI/MCP等価性検証
- **Phase 6〜8（E2E シナリオ群）: 実装済み**
  - Phase 6: 指定4結果 E01〜E04 の実プロセス再現（Human CLI・A/B の MCP stdio・独立 tick）
  - Phase 7: 必須異常系 N01〜N14 の実プロセス再現
  - Phase 8: 追加必須検証 X01〜X15 と固定 seed 操作列 × 参照会計モデルの照合（`test_sequence.py`）

Phase 1〜8 の完了条件と第21節チェックリスト 17 項目の対応は
[docs/poc-completion-checklist.md](docs/poc-completion-checklist.md) を参照。

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
  共有 Clock の観測・再起動後の保持・後退拒否・mode 不一致拒否・同時初期化、
  CLI 全コマンドの挙動検証、MCP SDK 2.x の stdio 接続と 8 tools 検証、
  CLI と MCP の等価性・Clock 共有検証
- `tests/e2e/`: 実プロセス E2E シナリオ。Requester は CLI 子プロセス、A / B は
  別々の MCP stdio サーバープロセス、決済は独立した `ojp tick` プロセスで動かす
  - `test_e01_child_ok_parent_ok.py` 〜 `test_e04_child_fail_parent_fail.py`:
    指定4結果（Child 成功/失敗 × Parent 成功/失敗）
  - `test_n01_*` 〜 `test_n14_*`: 必須異常系 14 項目（二重承認・同時 Claim・
    上限の同時超過・提出後の Lease 失効・Parent 停止後の Child 処理・
    検収者/裁定の無応答 fallback・送金失敗回復・先行返金後の Child 後始末・
    各 commit での会計不変条件）
  - `test_x01_*` 〜 `test_x15_*`: 設計上の追加必須検証（未入金・二重入金・
    task_key の衝突と放棄・上限の件数と深さ・条件/検証器/受取人の書き換え不可・
    operation_id 再利用・同時返金・期限境界・再起動後の処理・成果物アクセス範囲・
    先行返金後の Child 保護・裁定結果・他 Root の Lease と役割の拒否）
  - `test_sequence.py`: 固定 seed の生成操作列（複数 Child・失敗・期限・再試行・
    Parent 終端の順序組合せ）を独立した参照会計モデルと各 commit 直後に照合する

E01 単体だけを実行する場合:

```bash
uv run pytest tests/e2e/test_e01_child_ok_parent_ok.py
```

各シナリオは終了時に機械可読な JSON レポートを `data/work/e2e-reports/` へ書き出す
（`data/` は `.gitignore` 済み）。読み方は[テストレポートの読み方](#テストレポートの読み方)を参照。

## テストレポートの読み方

E2E テストはシナリオごとに `data/work/e2e-reports/<scenario_id>.json` を出力します
（例: `E01.json`、`N01A.json`、`X05A.json`）。フィールドは `tests/e2e/report.py` の
`ScenarioReport.to_json()` と一致します。

| フィールド | 意味 |
|---|---|
| `scenario_id` | シナリオ ID（`E01` 〜 `E04`、`N01A` 〜 `N14`、`X01` 〜 `X15` の枝番など） |
| `operations[].operation_id` | 各書込操作（CLI / MCP / tick）に付いた冪等性の operation_id。並びは実行順 |
| `terminal_job_states` | 最終観測点での全 Job の終端状態（`DONE` / `FAILED` / `EXPIRED` など） |
| `amounts_by_payee.paid` / `amounts_by_payee.refunded` | 受取人別の最終支払済み額・返金済み額（6 桁小数文字列） |
| `locked_breakdown` | 最終観測点の拘束中資金の内訳（`child_work` / `child_payout` / `parent_payout` / `refund`） |
| `conservation_ok` | 全観測点で保存則・非負・受取権者一致・親子二重計上なし・各 commit 後検証がすべて成立した場合のみ `true` |

補助的なフィールド: `final_totals`（入金額と残高の最終一致）、
`conservation[]`（観測点ごとの検証結果。中間 commit を含む全 commit 直後の検査は
`commits_ok`）、`operations[]`（step / channel / actor / action / ok）、`notes`。
レポートはシナリオ固有の期待値を持たず、各テストがレポートの内容を期待値と照合します
（`report.assert_report_contents`）。

## E01 シナリオ（MCP stdio 実プロセス）の再現実行

「最小実行手順」は人間の運用者が CLI で操作する流れです。一方で Agent A / B が
MCP stdio 経由で自律的にプロトコルを操作する構成は、E2E テストの E01
（Child 成功・Parent 成功）がそのまま再現手順になります:

```bash
uv run pytest tests/e2e/test_e01_child_ok_parent_ok.py
```

この 1 コマンドで、次の 4 種類の実プロセスが起動・連携します
（`tests/e2e/harness.py` の `create_world` / `agent_session` / `settle`）:

1. **Requester（CLI 子プロセス）**: `demo init`・Root の作成・入金・Root の検収承認・
   台帳表示を実行する（人間運用者の責任境界。計画書 第14節）。
2. **Agent A（MCP stdio サーバー実プロセス）**: MCP Python SDK の `stdio_client` から
   `ojp mcp --actor pt-agent-a` を別プロセスで起動し、MCP `ClientSession` で接続。
   8 tools の `ojp_claim_job`（Root Claim）→ `ojp_create_child_job`（part-1 を予算 10
   で発注）→ `ojp_approve`（Child の検収承認）→ `ojp_submit`（Root への成果物提出）
   を自律的に呼び出す。自己資金の引落しは発生しない（Child 予算は Root Escrow 内部で拘束）。
3. **Agent B（MCP stdio サーバー実プロセス）**: 同様に `ojp mcp --actor pt-agent-b`
   を別プロセスで起動。`ojp_claim_job`（Child Claim）→ `ojp_submit`
   （`{"sum": 6}` を提出）を呼び出す。
4. **独立 tick プロセス**: `ojp tick --watch`（および確定用の `tick --once`）を
   system Actor として別プロセスで起動し、承認済み支払い（Child 10 → B、Root 90 → A）
   を決済する。

つまり E01 の実行は「Requester CLI・A/B の MCP stdio・独立 tick」の実プロセス構成で
8 tools 経由の Child 発注・Claim・成果物提出・検収承認・決済までを完遂し、各観測点で
保存則・非負・受取権者一致・親子二重計上なしを検証します。

### E01 実行後の確認

テストは一時ディレクトリ（pytest の `tmp_path`）に DB を作るため、終了後にそのまま
`ojp ledger show` を実行できる実 DB は残りません。代わりに、テスト自身が最終状態を
検証したうえで機械可読レポートを書き出します:

```bash
# レポートの確認（E01 の最終状態がそのまま入っている）
cat data/work/e2e-reports/E01.json
```

`E01.json` の読み方（`terminal_job_states`・`amounts_by_payee`・`locked_breakdown`・
`conservation_ok` など）は[テストレポートの読み方](#テストレポートの読み方)を参照。
E01 では最終的に Root / Child とも `DONE`、`paid` が A=90.000000・B=10.000000、
返金 0、Escrow 0、`conservation_ok` が `true` になります。

なお、永続 DB で同じ流れを手元の MCP クライアントから試す場合は、先の
「MCP サーバー起動（stdio）」の A / B を別プロセスで起動する例のサーバーに対して
クライアントから 8 tools を呼び、送金は CLI の `ojp tick --once`（または `--watch`）
で確定させてから `ojp ledger show <ROOT_JOB_ID>` で台帳を確認します。

## CLI の使い方

すべてのコマンドで `--root <project-root>` を指定し、同一のプロジェクトルート（`data/ojp.sqlite3`）を解決します。
また、`--json` を指定すると結果を共通の JSON 封筒（`{"ok": true, ...}`）で stdout へ出力します。

> **注意（認証の境界）:** CLI および MCP 起動時の `--actor <ID>` オプションは、信頼済みローカル操作者が操作主体を指定するためのものであり、本番の暗号署名やユーザー認証ではありません（計画書 第5節・第14節）。

### 最小実行手順（初期化から台帳確認まで）

1. **環境とシード資金の初期化**（冪等に実行可能）
   ```bash
   uv run ojp --root <project-root> demo init
   ```

2. **Root Job（親タスク）の作成**
   ```bash
   uv run ojp --root <project-root> --actor pt-requester job create --card tests/fixtures/poc_root_card.json
   ```

3. **Root Job への全額入金**（DRAFT から OPEN へ遷移）
   ```bash
   uv run ojp --root <project-root> --actor pt-requester job fund <ROOT_JOB_ID> --amount 100.000000
   ```

4. **Job 一覧と詳細の確認**
   ```bash
   uv run ojp --root <project-root> job list
   uv run ojp --root <project-root> job get <ROOT_JOB_ID>
   ```

5. **Worker A による Root Job の Claim**（排他的 Lease の取得）
   ```bash
   uv run ojp --root <project-root> --actor pt-agent-a job claim <ROOT_JOB_ID> --version <VERSION_ID>
   ```

6. **Worker A による Lease の延長**（Heartbeat）
   ```bash
   uv run ojp --root <project-root> --actor pt-agent-a job heartbeat <ROOT_JOB_ID> --lease <LEASE_ID>
   ```

7. **Worker A による Child Job（子タスク）の発注**（予算拘束と公開）
   ```bash
   uv run ojp --root <project-root> --actor pt-agent-a child create <ROOT_JOB_ID> --lease <LEASE_ID> --task part-1 --budget 10.000000 --deadline 2027-01-15T12:00:00Z
   ```

8. **Worker B による Child Job の Claim**
   ```bash
   uv run ojp --root <project-root> --actor pt-agent-b job claim <CHILD_JOB_ID> --version <CHILD_VERSION_ID>
   ```

9. **Worker B による成果物 JSON の提出**
   成果物は公開 Version の固定条件（`tests/fixtures/poc_catalog.json` の `expected`）に
   合致する小さな JSON である必要がある。`part-1`（入力 `[1, 2, 3]`）なら:
   ```bash
   printf '{"sum": 6}' > /tmp/ojp-artifact.json
   uv run ojp --root <project-root> --actor pt-agent-b job submit <CHILD_JOB_ID> --lease <CHILD_LEASE_ID> --version <CHILD_VERSION_ID> --artifact /tmp/ojp-artifact.json
   ```
   条件に合わない成果物は `VERIFICATION_FAILED` で拒否される（Lease 中の再提出が可能）。

10. **Worker A による Child Job の検収承認**（送金予約）
    ```bash
    uv run ojp --root <project-root> --actor pt-agent-a job approve <CHILD_JOB_ID> --submission <SUBMISSION_ID>
    ```

11. **スケジューラ / 送金処理の実行**（期限判定・決済処理）
    ```bash
    uv run ojp --root <project-root> tick --once
    ```

   継続運用では独立したプロセスで常駐させることもできる（`--once` と `--watch` は排他指定）:
   ```bash
   uv run ojp --root <project-root> tick --watch --interval 1.0
   ```

12. **Root 資金台帳と保存則の確認**
    ```bash
    uv run ojp --root <project-root> ledger show <ROOT_JOB_ID>
    ```

## MCP サーバー起動（stdio）

Agent がツールを通じて自律的にプロトコルを操作するための stdio サーバーを起動します。

```bash
uv run ojp --root <project-root> mcp --actor <PARTICIPANT_ID>
```

Agent A と Agent B を別々のプロセスとして起動する例（同一の `--root` を解決します）:

```bash
uv run ojp --root <project-root> mcp --actor pt-agent-a
uv run ojp --root <project-root> mcp --actor pt-agent-b
```

各プロセスが `--actor` で指定した操作主体として 8 tools を呼び出します。
E2E テスト（`tests/e2e/`）もこの構成で A / B を検証します。

E01 シナリオ（[E01 シナリオ（MCP stdio 実プロセス）の再現実行](#e01-シナリオmcp-stdio-実プロセスの再現実行)）
では、MCP Python SDK の `stdio_client` が上記の `ojp mcp --actor pt-agent-a` /
`--actor pt-agent-b` をそれぞれ別プロセスで起動し、`ClientSession` 経由で
`ojp_claim_job`・`ojp_create_child_job`・`ojp_submit`・`ojp_approve` などを呼び出します。
つまり「Agent A / B の MCP stdio プロセス」はテストハーネスから spawn される本物の
サーバープロセスで、A は Root の Claim と Child の発注・検収を、B は Child の Claim と
成果物提出を、どちらも 8 tools だけを使って自律操作します。入金・返金・tick は
MCP に公開されないため（下記「返金・tick・入金が MCP にない理由」）、Requester CLI と
独立 tick プロセスが担います。

### 公開される 8 Tools

| Tool 名 | 主な引数 | 概要・必要権限 |
|---|---|---|
| `ojp_list_jobs` | `state?`, `parent_id?`, `limit?`, `cursor?` | keyset pagination による Job 一覧取得（誰でも可） |
| `ojp_get_job` | `job_id` | 仕様・Lease・予算・期限・判定・提出成果物の取得（成果物は権限者のみ） |
| `ojp_claim_job` | `job_id`, `expected_version_id`, `operation_id` | OPEN な Job の排他的 Lease 取得（Worker） |
| `ojp_heartbeat` | `job_id`, `lease_id`, `generation`, `operation_id` | Lease 有効期限の延長（当該 Lease 保持 Worker） |
| `ojp_create_child_job` | `parent_job_id`, `lease_id`, `task_key`, `budget`, `deadline`, `operation_id` | 拘束予算内での Child Job 作成・公開（Parent Worker） |
| `ojp_submit` | `job_id`, `lease_id`, `version_id`, `artifact_json`, `operation_id` | 成果物 JSON の提出（当該 Lease 保持 Worker） |
| `ojp_approve` | `job_id`, `submission_id`, `operation_id` | 検収承認と送金予約（Job Requester） |
| `ojp_dispute` | `job_id`, `submission_id`, `condition_id`, `reason_code`, `operation_id`, `evidence?` | 固定条件に基づく検収異議（Job または Root Requester） |

### 返金・tick・入金が MCP にない理由

Root Job の作成・入金・返金予約の再処理・定期 tick・障害注入・決済再試行は、人間運用者またはシステムの責任境界（CLI）に配置されています。自律 Agent に公開する tool を契約履行・再委託・成果物提出・検収の 8 件に絞り込むことで、意図しない資金操作や権限逸脱を防ぎ、認知的な過負荷や誤呼び出しを排除しています（計画書 第13節・第14節）。

## Clock とテスト機能の扱い

- 書込処理は `BEGIN IMMEDIATE` 取得後に `runtime_clock` を読み、realtime なら UTC 実時刻、test mode なら保存済み `test_now_utc_us` をその transaction の now として1回だけ採取します。
- mode は `runtime_clock` 行の作成時に一度だけ確定し、起動設定と DB の mode が異なれば `MODE_MISMATCH` で起動を拒否します。
- **時刻操作（`set_test_now` 等）や failpoint（障害注入）はテスト専用機能であり、通常の CLI コマンドおよび公開 MCP tools からは一切触れることができません**（計画書 第14節・第21節）。

## パッケージ構成

- `src/ojp/domain.py`: モデル型・状態（JobState / LeaseStatus / ErrorCode）・policy・金額変換（第5節）
- `src/ojp/db.py`: 接続PRAGMA（foreign_keys / WAL / synchronous=FULL / busy_timeout）、明示 BEGIN/COMMIT/ROLLBACK、番号付き SQL migration（第16節）
- `src/ojp/clock.py`: 全プロセス共有 Clock（realtime / test）、初期化・照合・アトミック更新（第7節）
- `src/ojp/ledger.py`: 排他的二重仕訳台帳・保存則検証・MockWallet・PaymentOperation（第2節）
- `src/ojp/verification.py`: 固定決定論的検証器（sum-v1 / 固定条件照合）（第11節）
- `src/ojp/service.py`: 統合 Application API（Query / Command / Lifecycle / 冪等制御）（第15節）
- `src/ojp/scheduler.py`: 期限処理・送金処理の周期的実行（`ojp tick`）（第12節）
- `src/ojp/response.py`: CLI / MCP 共通の応答封筒（`ok` / `data` / `error` / `operation_id` / `replayed`）および終了コード定義（第13節・第14節）
- `src/ojp/cli.py`: 全 CLI コマンド（demo init, job, child, ledger, payment retry, tick, mcp）（第14節）
- `src/ojp/mcp_server.py`: MCP stdio サーバー実装（8 tools 公開・SDK ToolError 変換・Actor 境界管理）（第13節）
- `src/ojp/migrations/001_initial.sql`: 初期スキーマ。全参照に外部キー、金額に非負制約
- `tests/fixtures/poc_root_card.json`: テスト用 Root Job カード定義
- `tests/fixtures/poc_catalog.json`: 第11節の固定カタログ（sum-v1 / part-1..3）
