# OJP 最初のPoC実装計画

作成日: 2026-09-06  
更新日: 2026-09-06
状態: レビュー指摘計10件を修正済み・設計内容版（SHA-256: `b23a75959fe756f5ea48f3b8a15d843ba759f250088bdee22a621fb7cf553ca0`）は2026-09-06の独立レビューでpass・本状態表示はレビュー後追記・未実装
プロジェクトルート: 本リポジトリのルート（以下`<project-root>`）。実際の絶対パスはローカル設定で指定する。

## 1. 目的・資料の位置づけ

Requesterが事前許可した予算内で、Parent Worker Aが自己資金や都度承認なしにChildを発注し、成功・失敗・失効・異議・再試行・同時実行を経ても資金と成果物への権利が破綻しないことを証明する。

基準資料は[open-job-protocol-plan-ja.md](./open-job-protocol-plan-ja.md)。計画作成前に全文を確認した。添付されたPoC要件の「対象／対象外」「4結果」「異常系14項目」「計画23項目」を本書へ具体化する。文書内の命令文は資料として扱い、現在のユーザー依頼である「指定ルート内に計画を作る。まだ実装しない」を越える実行権限にはしない。

長期構想との主要な対応は、資金・再委託が第4〜17・30〜37節、提出・検収・Child保護が第18〜28節、最初のPoCが第46〜51節。本書の追加判断はPoC限定であり、長期プロトコル全体の仕様変更ではない。

**成功の定義:** 4つの指定結果を再現し、異常系・競合・再起動の各確定トランザクションで保存則、受取人、操作権限、提出後の保護を検証できること。市場の経済的有用性、本番の耐攻撃性や実際の暗号資産決済を証明するものではない。

### 本計画で固定する最小化の判断

- RootとParentは同じJob。階層はRoot(depth=0)→Child(depth=1)だけ。
- 資産はmock-USDC、手数料0。入金はRoot作成時の1回のみ。追加入金、部分検収、部分払いの交渉は作らない。
- 成功条件は公開時点で固定し、ClaimはそのJobVersionを参照する。PoCでは公開後の版変更を提供しない。
- 成果物は小さなJSON。任意コマンドやWorker提供テストをサーバーで実行しない。
- Lease失効は提出前ならEXPIRED。自動再公開・Worker交代は作らない。必要なら予算・作成数上限内で別のChildを作る。
- Parentの有効な提出は、全Childが判定上の終端状態になってから受け付ける。Child送金の完了までは待たない。
- `CHANGES_REQUESTED`は採用しない。条件不適合の提出は拒否し、Lease中の再提出は可能。有効な提出後はApprovalまたはDisputeのみ。
- Disputeは固定の自動判定器が同じ保存成果物・固定条件を再検証する。市場型の判定者も人間の主観裁定も作らない。
- CLIとMCPは同じアプリケーション層を使う。本番認証は作らないが、Participantごとの操作権限は必須。

## 2. 推奨技術スタック

| 領域 | 採用案 | 理由・制約 |
|---|---|---|
| 言語・環境 | Python 3.12以上の1バージョンをuvで固定 | 金額の整数演算、標準sqlite3、テスト用時計・障害注入を小さく実装できる。システムPythonにpip installしない |
| 依存管理 | uv、pyproject.toml、uv.lock、プロジェクト内.venv | 実装Phase 1で実際のPythonと依存バージョンを記録し、以後`uv sync --locked`で再現する。[uv公式](https://docs.astral.sh/uv/concepts/projects/sync/) |
| DB | SQLite＋標準sqlite3、直接SQL、番号付きSQL migration | 別DBサーバーを不要にし、短い書き込みトランザクションで競合を直列化する。ORM不要 |
| 入出力型 | Pydantic | MCPとCLIで共通の入力検証を行い、金額・policy・成果物の曖昧な型変換を防ぐ |
| MCP | 公式Python SDKの2.x、stdio | 2026-09-06確認時点の公式READMEは2.xを安定系列と案内。実装開始時に安定版を解決し`<3`上限とlockで固定。旧v1のAPIを混在させない。[公式SDK](https://github.com/modelcontextprotocol/python-sdk) |
| CLI | 標準argparse | コマンド数が少なく追加フレームワーク不要。JSON入出力を主とする |
| 検証・テスト | 固定JSON検証器、pytest、標準subprocess／並行実行機能 | 実DB・複数プロセス・MCP stdioを通した再現可能なE2Eを作る |
| 時間処理 | 共通Clock＋DB期限検索 | 通常はUTC実時刻、test modeはDBの単一行に保存した固定UTC時刻を全プロセスで共有する。第7節の規約に従い、同一tickを常駐ループとテストから呼ぶ |

SQLiteは同時書き込みが1つで、`BEGIN IMMEDIATE`が先に書き込みトランザクションを開始する。この性質を利用する設計である。[SQLite公式](https://www.sqlite.org/lang_transaction.html) Python側は自動開始と明示的BEGINを混在させず、明示SQLでBEGIN／COMMIT／ROLLBACKを統一する。[sqlite3公式](https://docs.python.org/3/library/sqlite3.html#transaction-control)

今回は依存のインストールや環境構築を行わない。具体的なpatch版の解決と接続スモークテストはPhase 1の完了条件にする。

## 3. 全体アーキテクチャ

```text
Human / Requester                Agent A / Agent B
       | CLI                           | MCP stdio
       +-------------------------------+
                       |
            ActorContext + 入力検証
                       |
       +---------------v----------------+
       | OJP Application（同一パッケージ）|
       | Job / Lease / Child / Submit    |
       | Verify / Approve / Dispute      |
       | Budget / Settlement             |
       +-----------+---------------------+
                   |       Clock / tick / failpoint
                   |
       +-----------v---------------------+
       | SQLite 1ファイル                |
       | Jobs / Versions / Leases        |
       | JSON成果物 / 判定証跡            |
       | Budget / Journal / Operations   |
       | Mock Wallet / Transfer Receipt  |
       +---------------------------------+
```

「1アプリケーション」は1コードベース・1DBを意味する。Actorごとのstdioプロセス、CLI、tickプロセスを起動しても、DBとドメインルールは共通。ネットワークAPIサーバー、メッセージブローカー、マイクロサービスは追加しない。

成果物は最大1 MiBのJSONとして同じDBに保存する。検証・取得可能性・Job遷移を同一トランザクションに収め、ファイルストアとの二重書き込み問題を避ける。長時間の処理や外部通信をDBロック中に実行しない。

Mock Escrowはドメイン判断と分離したポートとする。ドメインが「誰へ、いくら、何を根拠に」を決め、ポートがfund／transfer／lookupを実行する。将来の本番アダプター追加時には最終性・外部照会・認証等の再設計が必要であり、本PoCで差し替え先を実装しない。

## 4. ディレクトリ構成

アプリケーションのsrc、tests、DBは今後の配置案であり、現在は未作成。基準資料と本書、.gitignoreは作成済みで、data/workには非公開の作業記録を保存できる。

```text
<project-root>/
├── docs/
│   ├── open-job-protocol-plan-ja.md       # 提供原文・基準資料
│   └── poc-implementation-plan.md        # 本書（設計判断も集約）
├── pyproject.toml                       # Phase 1以降
├── uv.lock
├── .python-version
├── .gitignore
├── README.md                            # 起動・再現方法
├── src/ojp/
│   ├── domain.py                        # 型、状態遷移、policy、不変条件
│   ├── service.py                       # 権限、transaction境界、ユースケース
│   ├── db.py
│   ├── migrations/001_initial.sql
│   ├── ledger.py                        # Budget・Journal・決済ポートとMock
│   ├── verification.py                  # 固定検証器・固定仲裁
│   ├── scheduler.py                     # Lease/無応答/Dispute/決済tick
│   ├── clock.py
│   ├── mcp_server.py
│   └── cli.py
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── e2e/
│   └── fixtures/                        # 成功条件・デモ入力・期待結果
└── data/                                # git対象外、実装時の永続データ
    ├── ojp.sqlite3
    ├── local-actors.json                # ローカル実行主体の設定
    └── work/                            # テスト一時DB・レポート
```

DB・一時データ・テスト成果物も原則このルート内。作業メモはローカル運用ルールが指定する非公開の記録先へ都度保存し、履歴管理・知識ベースへ反映する。個人用の保存先や記録内容を本リポジトリへ含めない。設計正本を他プロジェクトに複製しない。docsの追加分割は本書の保守が難しくなった時だけ行う。

## 5. データモデル

IDは不透明な文字列。日時はサーバーClock由来のUTC。全参照に外部キー、金額に非負制約を設ける。

| モデル | 最低限のフィールド | 制約・責務 |
|---|---|---|
| Participant | id, label, kind(human/agent/system) | Requester/Workerを固定ロールにしない。役割はJobとの関係で判定 |
| Job | id, root_id, parent_id, requester_id, state, version_id, active_lease_id, row_version, created_at | Rootはparent_id=null・root_id=id。Childのparent_idはRootのみ |
| JobVersion | id, job_id, version, title, budget_units, asset, input_json, verifier_id/hash, conditions_json/hash, subcontract_policy, task_catalog, timing_policy, artifact_access_policy, deadline | 公開後immutable、UNIQUE(job_id,version)。PoCは公開版1つ |
| Lease（Claimを内包） | id, job_id, worker_id, version_id, generation, claimed_at, heartbeat_at, expires_at, closed_reason | 有効LeaseはJob当たり最大1。過去Leaseは監査用に残す |
| Submission | id, job_id, lease_id, version_id, artifact_json, artifact_hash, verification_result, verification_evidence, submitted_at, valid_at, review_due_at | 条件一致・保存済みで有効化。Job当たり有効Submissionは最大1 |
| SubmissionAttempt | id, job_id, lease_id, input_hash, outcome, reason, attempted_at | 不適合提出を記録。無効データは公開・検収タイマー対象にしない |
| Child関係 | Job.parent_id＋task_key＋creator_lease_id | 専用グラフテーブル不要。ChildのRequesterは作成時のA、ultimate RequesterはRootから辿る |
| BudgetAccount | id, root_id, owner_job_id, bucket, purpose, amount_units, beneficiary_id, source_key | available／lockedの正本。lockedの用途を区別し、同じ金額を別口座へ重複保存しない |
| JournalTransaction / Entry | operation_id, reason, created_at／account_id, delta_units | 操作単位で貸借が一致する追記専用台帳。残高更新と同一transaction |
| Operation | operation_id, actor_id, kind, payload_hash, business_key, status, result | クライアント再送の冪等性。IDを別内容に流用したら拒否 |
| PaymentOperation | operation_id, business_key, root_id, job_id, source_account_id, amount_units, payee_id, kind, status, attempt_count, next_retry_at, last_error, receipt_id | PENDING/RETRYABLE/SUCCEEDED。原資と受取人は作成時に固定 |
| MockWallet / TransferReceipt | participant_id, asset, balance／operation_id, amount, payee, receipt_id | Receipt.operation_id UNIQUE。Wallet・Escrow・Receiptは同じDBで原子的更新 |
| Acceptance | job_id, submission_id, decision(APPROVED/REJECTED), decided_by, reason, decided_at | Jobごとに最終判定1つ。Job状態と送金状態を分離 |
| Dispute | id, job_id, submission_id, opened_by, reason_code, condition_id, evidence, opened_at, due_at, status, resolution | 1有効提出につき最大1。OPEN→RESOLVEDのみ。金額の一部裁定はしない |
| Event | id, root_id, job_id, actor_id, action, object_id, at | 状態・権限判断を追跡する小さな監査記録。イベント基盤やevent sourcingは不要 |
| RuntimeClock | singleton_id=1, mode(realtime/test), test_now_utc_us | migration 001に含めるDB当たり1行。testのみ固定UTC時刻を整数マイクロ秒で保持。modeはDB初期化時に固定し、既存DBでは変更不可。通常はtest_now_utc_us=null |

JSONの金額は小数を文字列で渡し、内部は1 mock-USDC=1,000,000 unitsの整数。例: `100.000000`=100,000,000。float経由の変換は禁止。6桁を超える小数、負数、ゼロ入金・ゼロChild予算、SQLiteの64bit整数範囲を越えるRoot予算を拒否する。Root間の資金移動も禁止。

### Actorと成果物への権限

- Root作成・入金・Root承認はRoot Requester。Claim／heartbeat／submitは該当Worker。Child作成は現在有効なParent LeaseのWorker。
- Child承認は作成時Requester A。ただし検証PASSが必須で、Aの意思だけでは支払えない。A停止後はsystemの期限処理で代替する。
- 異議はそのJobのRequesterまたはRoot Requesterが固定条件に対して提出できる。裁定権限はsystemのみ。
- サーバー起動設定からActorContextを作る。MCP引数にactor_id・payee_idを受け付けない。CLIのActor選択は信頼済みローカル操作者用で、本番認証を意味しない。
- Requesterが資金拠出したChildの有効成果物は、A停止・Parent終了後もRoot Requesterが取得できる。利用権をJobVersionに事前明記し、読取権をParentの状態やAの接続に依存させない。
- DBを直接編集できる同一OSユーザーを攻撃者から隔離する設計は対象外。サービス経由の別Actor・別Jobへの不正操作は必ず拒否する。

## 6. Job State Machine

Jobは`DRAFT / OPEN / LEASED / SUBMITTED / DISPUTED / DONE / FAILED / EXPIRED`を持つ。ApprovalはAcceptanceの記録であり、送金結果の別名にしない。

```text
DRAFT --Root入金 / Child予算拘束--> OPEN --Claim--> LEASED
                                  |                 |
                                  |期限             |有効な提出
                                  v                 v
                               EXPIRED          SUBMITTED
                                                   |   |
                                         承認/PASS |   |期限内異議
                                                   v   v
                                                 DONE DISPUTED
                                                       |    |
                                           固定裁定PASS |    |固定裁定FAIL
                                                       v    v
                                                     DONE FAILED
```

| 元状態 | イベントと条件 | 次状態・副作用 |
|---|---|---|
| DRAFT | Rootの正確な全額入金が確定 | OPEN。Claim時には送金しない |
| 作成中Child | Parent権限・上限検査と予算拘束が成功 | Job・Version・lockedを同時作成してOPEN。拘束前の公開はない |
| OPEN | 他に有効Leaseなし、deadline前、Requester本人でないWorkerがClaim | LEASED。LeaseとVersion参照を固定 |
| OPEN | deadline到来 | EXPIRED。原資を返却／返金予約 |
| LEASED | 有効Workerのheartbeat、期限前 | LEASED。expiryを更新 |
| LEASED | 保存・検証・権限・期限検査がすべて成功 | SUBMITTED。Leaseを提出完了で閉じ、review_due_atを設定 |
| LEASED | 無効な提出 | LEASEDのまま。Attemptだけ記録、タイマーを設定しない |
| LEASED | Worker自身がabandon（有効提出なし） | FAILED。Child返却またはRoot未拘束残額の返金予約 |
| LEASED | Lease失効またはJob deadline到来、有効提出なし | EXPIRED。同上。自動OPEN復帰なし |
| SUBMITTED | Requester承認、または異議なしでreview_due_at到来 | DONE＋Acceptance APPROVED＋支払い予約 |
| SUBMITTED | review_due_atより前に有効な異議 | DISPUTED。資金を維持 |
| DISPUTED | 固定裁定PASS、または判定者無応答時の事前PASS証跡による既定裁定 | DONE＋Acceptance APPROVED＋支払い予約 |
| DISPUTED | 固定裁定が既存条件不適合を再現し証跡を確定 | FAILED＋Acceptance REJECTED＋返却／返金予約 |

禁止: SUBMITTED／DISPUTEDからRequesterのfail/refund操作でFAILEDへ移す、DONEから送金失敗を理由にFAILEDへ戻す、提出後のLease失効でOPEN／EXPIREDへ戻す、終端JobをClaim／heartbeat／submit／再公開する、Parent終端をChildへ伝播する。

PoCにRequester用の任意fail/cancelコマンドは設けない。Parentの失敗はWorkerの提出前abandon、失効、または固定裁定による。Root返金コマンドは既に確定した返金予約を処理するだけで、Jobの判定を変更できない。

## 7. Lease設計

Claimのtransaction内で、Job OPEN・deadline未到来・有効Leaseなし・expected_version_idと公開版の一致を再確認し、Lease作成とJob更新を一緒にcommitする。`UNIQUE(job_id) WHERE closed_reason IS NULL`等の制約と条件付きUPDATEを併用する。

デモ既定値はLease 60秒、heartbeat目安20秒、検収待ち30秒、Dispute判定待ち30秒、tick間隔1秒。JobVersionに実際の値を保存する。`expires_at=min(now+60秒, Job deadline)`で、heartbeatによってJob deadlineを越えない。

時計の比較は`now < deadline`を有効、`now >= deadline`を失効とする。Claim／heartbeat／submit／Child作成はtick未実行でも期限を検査する。クライアント時刻を信用しない。書込ロックを取得した後のサーバー時刻で決め、期限ちょうどは失効側を優先する。

### 全プロセスで共有するClock

- 書込処理は`BEGIN IMMEDIATE`取得後、RuntimeClockを読み、通常はUTC実時刻、test modeでは保存済み`test_now_utc_us`をそのtransactionの`now`として1回だけ採取する。同じtransaction内の期限比較・記録日時はこの値で統一する。読取処理も同じDB snapshotのClockを使い、プロセス単位で固定時刻をキャッシュしない。
- MCP・CLI・tickは同じDBからmodeと時刻を読む。test DBの利用には各プロセスで明示的なtest mode起動が必要で、起動設定とDBのmodeが違えば起動を拒否する。通常DBからtest時刻へ切り替える入口は設けない。
- テストharnessだけが専用test modeの補助処理で、書込transaction内の単一行を指定UTC時刻へ進める。後退は拒否、同じ時刻への設定はno-opとする。一般のJob API、通常CLI、MCPの8 toolsには時刻操作を追加しない。時刻更新transactionにはJob処理を混在させない。
- 全プロセス停止・再起動でも固定時刻をDBに保持する。実時間経過では自動加算せず、停止中に期限を過ぎるテストもharnessが明示的に進める。次のtransactionは更新後の時刻を読むため、期限ちょうどを決定的に再現できる。
- 期限処理のE2Eは時刻更新commit後に独立プロセスの`ojp tick --once`完了を待つ。`--watch`の存続確認は観測条件と有限timeoutで待ち、Job期限を実時間sleepで待たない。通信・プロセス待機timeoutにはOSの単調時計を使い、固定Clockに依存させない。

Submitとexpiryが競合した時は同じDB transaction境界で直列化する。先に有効Submitがcommitしたなら、以後のexpiryは何もしない。先にexpiryが確定したらSubmitを拒否する。heartbeatも閉じたLeaseを復活させない。

提出後はLeaseを閉じるのでheartbeat不要。review/dispute期限が進行を担う。Agent Aが停止してもChild BのLease・提出・期限処理は独立に継続する。

## 8. Parent / Child設計

### 作成と固定条件

Child作成はParentがLEASEDかつLease有効な間だけ。作成時のA、Lease、Root policy、指定task_keyを固定する。Root資金からの内部移動とJob公開を一つのtransactionにする。Child自身からの再委託は必ず拒否する。

Root RequesterはRoot公開時に使用可能な客観的Childタスクのカタログと予算上限を許可する。Aはその範囲内でタスク・予算を選ぶ。カタログ外の成功条件や入力へのすり替えは拒否する。これは都度承認ではなく事前許可の一部である。

同じtask_keyには同時に生存中または成功済みChildを複数作れない。失敗・失効したタスクの再発注は新Childとして可能だが、累計max_childrenを消費する。再発注時も条件は同じカタログに束縛する。

### Parent終了時

| Parentの状態 | 新Child | 既存Child | Rootの未拘束額 |
|---|---|---|---|
| LEASEDで有効Leaseあり | policy内で可 | 通常進行 | availableとして保持 |
| SUBMITTED / DISPUTED | 禁止 | 提出受付時点で全Child判定終端 | Parent判定のため保持 |
| DONE | 禁止 | Child支払い待ちは継続 | A向け支払い予約へ移動 |
| FAILED / EXPIRED | 禁止 | OPEN・LEASED・SUBMITTED・DISPUTEDを含め各自の条件で継続 | Requester向け返金予約へ移動 |

ParentをEXPIREDにしてもChildを自動EXPIREDにしない。Childは独自の有限deadline・検収期限・異議期限を持ち、Parent deadlineを越えるものも許可する。Child作成時点でそのdeadlineが未来であることを確認する。

Parent提出は全ChildがDONE／FAILED／EXPIREDである場合に限る。未解決Childがあれば`CHILDREN_UNRESOLVED`で拒否し、検収タイマーを開始しない。この制約でParent成功後の残額追加払いを省く。Child DONEの送金待ちは金額が既に確定・拘束済みなのでParent提出を妨げない。

Parent失敗後にChild失敗が確定した場合、Child未払い額をParent availableへ戻し、同じtransactionでRequesterへの**追加返金予約**へ移す。Child成功ならBへの支払いへ進む。返金済みParentのJob状態を復活させない。

## 9. Budget / Mock Escrow設計

### 排他的な資金区分

Rootごとの資金を次の排他的口座で表す。集計表示は口座から導出し、親集計と子口座を足し直さない。

| 表示 | 意味 |
|---|---|
| available | Parent内の未拘束資金。新Childへ使えるのはParentの有効Lease中だけ |
| locked.child_work | 各Childの検収未確定資金 |
| locked.child_payout | 成功Childへの送金予約資金。失敗時返却対象にならない |
| locked.parent_payout | 成功Parentへの送金予約資金 |
| locked.refund | Root Requesterへの返金予約資金 |
| paid | 実際にMockWalletへ移転済みのWorker支払い累計。受取人・Child/Parent別内訳あり |
| refunded | 実際にRoot RequesterのMockWalletへ移転済みの返金累計 |

`locked`は上記4用途の合計。PENDING/RETRYABLEの送金もEscrow内でlockedに残る。**APPROVEDやDONEになっただけではpaidを増やさない。** Parentが失敗してもChildのwork／payout口座は返金予約へ移せない。

```text
D = Root入金総額（1回）
E = available + locked
D = E + paid + refunded
D = available + locked + paid + refunded
```

全口座は非負。Child支払額は当該Child拘束額以下。MockWalletの入金前残高を含めた全口座間のJournal合計も0。Rootへの入金は事前seedしたRequester MockWalletからの引落しとEscrow増額を同一transactionで行う。seedはデモ準備専用で、Job資金操作から呼べない。

### 100→10の操作一覧

| 操作 | 内部移動 | 外への移転 |
|---|---|---|
| Root fund 100 | Escrow available +100 | Requester MockWallet -100 |
| Claim | なし | なし |
| Child create 10 | available -10 / child_work +10 | なし |
| Child approval | child_work -10 / child_payout +10 | まだなし |
| Child payment success | child_payout -10 / paid(B) +10 | B MockWallet +10 |
| Child failure（判定未確定の原資） | child_work -10 / available +10 | なし |
| Parent approval（Child成功済み） | available -90 / parent_payout +90 | まだなし |
| Parent payment success | parent_payout -90 / paid(A) +90 | A MockWallet +90 |
| Parent failure（Child拘束10） | available -90 / refund +90 | まだなし |
| Refund success | refund -90 / refunded +90 | Root Requester MockWallet +90 |

Parent失敗後のChild失敗では`child_work→available→refund`の2移動を一つのtransactionに記録する。途中のavailableは外から再利用できない。ChildがDONEだが送金障害中の場合は失敗返却を禁止する。

### Operationと送金失敗

1. 承認／失敗判定のtransactionでAcceptance、Job終端、資金予約、PaymentOperationをまとめて確定する。
2. 共通settlement処理がPENDING/RETRYABLEを取得し、同じoperation_idでMock.transferを呼ぶ。
3. Mock.transferはReceiptを先に照会し、既存Receiptの金額・受取人・原資が予約と一致することを確認する。未処理の場合のみlocked減額・Wallet増額・paid/refunded増額・Journal・Receiptを一つのDB transactionで確定する。
4. 呼出側が結果を記録する前に停止しても、再起動後にReceiptを照会してSUCCEEDEDへ収束させる。Receiptなしでpaidを増やさない。
5. DB commit前の障害なら全体rollback。commit後の応答消失ならReceiptが正本。再試行でJobをFAILEDにしない。

送金は有限回で打ち切って返金へ切り替えず、上限付きバックオフ（例1/2/4/8/30秒以後30秒）で再試行する。永続障害はCLIで失敗理由と予約金を可視化する。進行保証はアプリ・DBが復旧することを前提にし、停止中の処理実行を保証しない。

## 10. 再委託上限

Root公開時の例はenabled=true、max_amount=30、max_ratio_bps=3000、max_children=3、max_depth=1。APIでは百分率文字列を保存せずbasis points整数とする。

```text
L = min(max_amount_units, floor(root_deposit_units × max_ratio_bps / 10000))
U = Child支払い済み総額 + Σ(child_work + child_payout)
作成可能 ⇔ U + 新Child予算 <= L
          かつ 新Child予算 <= available
          かつ 累計Child作成数 + 1 <= max_children
          かつ Parent Lease有効、depth=0、enabled=true
```

Child送金待ちもUに含める。そうしないと未送金の成功額を再利用できてしまう。返却済みの失敗Child額はUから外すが、max_childrenは**累計作成件数**とし枠を戻さない。例として1件目10失敗後に2件目10を作成できるが、累計3件を使い切った後は作れない。

max_ratio_bpsは0〜10000、max_amountは非負、max_childrenは非負整数、max_depthは0または1。enabled=false／depth=0／上限0ならChild作成不可。両方の金額制約が指定されることを必須にし、より厳しい方を適用する。

上限判定、task_keyの重複判定、Job作成、available減額、locked増額、作成数更新を同じ書込transactionで行う。先にtransaction外で読んだ残高を判断材料にしない。

## 11. Submission / Verification

### 最小の実証タスク

Rootは固定整数配列の合計をJSONで返す`sum-v1`。共通fixtureは入力[1,2,3,4,5]、Parent期待結果は`{"sum":15}`。Root公開Versionのカタログには次の3件を固定する。Aは外注した部分の結果と自分で計算した残りを統合する。

| task_key | 固定入力 | 期待JSON | 1件の予算上限（mock-USDC） |
|---|---|---|---:|
| part-1 | [1,2,3] | {"sum":6} | 20.000000 |
| part-2 | [4] | {"sum":4} | 20.000000 |
| part-3 | [5] | {"sum":5} | 20.000000 |

入力・期待値・検証器・各予算上限は公開後変更できない。Child予算は正かつカタログの1件上限以下で、さらに第10節のRoot全体上限を満たす必要がある。既定のChild予算は10、N07・X02等の競合検証では20を指定する。3件すべての外注は必須ではない。

未知のtask_keyやカタログ条件のすり替えは`TASK_NOT_ALLOWED`、生存中または成功済みtask_keyの再作成は`TASK_CONFLICT`。予算・件数・深さ上限の拒否は`POLICY_LIMIT`とし、`details.reason`で`TASK_BUDGET`／`MAX_AMOUNT`／`MAX_RATIO`／`MAX_CHILDREN`／`MAX_DEPTH`を区別する。金額と比率を同時に超える場合は`MAX_AMOUNT`を返す。Actor・親Job・親Leaseの有効性を確認した後、親がdepth=1ならカタログ参照、task_key重複、予算・件数の検査より先に`POLICY_LIMIT/MAX_DEPTH`を返す。depth=0ではカタログと重複を確認してから予算・件数を検査する。

検証器は整数のみ、キー完全一致、正確な合計を検証する。boolを整数として受理しない。入力件数・整数桁数・JSONサイズ／深さ・重複キーにも上限・拒否規則を定め、NaN/Infinityを許可しない。既定は最大1,000要素、各値の絶対値10^9以下、最大1 MiB、深さ8とする。

期待値、対象入力、検証器の版・hashはRoot Requesterが公開したVersionとカタログから導出する。A/Bが提出と一緒に期待結果・テスト・判定器を渡しても採用しない。Aは条件を緩めた架空タスクを発注できない。ただし別Actorの実質同一人物性や市場価格の妥当性までは検証しない。

### 有効な提出の境界

1. サイズ・JSON構造の検査を行う。
2. 書込transactionでActor、Lease、Job状態、現在時刻、固定Version、Parentなら全Child判定終端を検査する。
3. 有限時間の固定JSON検証を行い、保存対象をcanonical形式にしhashを計算する。
4. PASSならJSON本体・検証証跡・読取権限・valid_at・review_due_atを保存し、SUBMITTEDへ移行してLeaseを閉じる。全体commitにより有効提出となる。
5. FAILならAttemptを記録して`VERIFICATION_FAILED`を返す。JobをSUBMITTEDにせず、タイマーも支払い予約も作らない。期限内の修正再提出は可能だが、JobVersionの変更は不可。

成功条件を満たした成果物はDB保存と取得権限設定の完了なしに有効化しない。検証器例外・一時的DB失敗は`VERIFICATION_UNAVAILABLE`等の再試行可能エラーで、Workerの検証FAILとは区別する。

小さな同期検証のためVERIFYING状態・専用Workerキューは不要。期限はtransactionで検証開始を受け付けた時点で判定し、処理は有限入力に制限する。無効Attemptには成果物を公開せず、正常な取得は有効Submissionのみとする。

## 12. Approval / Dispute・無応答

Requesterがapproveできるのは、自分が検収者であるSUBMITTEDの有効Submissionのみ。保存済みPASS・Version/hash一致が必須。amount/payeeをapprove引数に持たせず、受取人は当該LeaseのWorkerとする。

`review_due_at=valid_at+review_window`。期限前の異議は固定condition_idと理由（例: CONDITION_MISMATCH / ARTIFACT_INTEGRITY）を必要とし、価格交渉、新条件、主観的な不満は受理しない。異議は最大1件、期限延長・再オープンなし。

DISPUTEDでは予約前の対象資金を保持する。tickの固定判定器が保存JSON、Claim時Version、元検証証跡を使って再検証する。PASSならDONE、既存条件へのFAILが再現された場合だけFAILED。通常は決定的な検証器なのでPASSが再現され、FAIL経路は判定器不具合等を模した専用fixtureで検証する。

判定待ち期限まで固定判定器が応答しない場合は、**有効提出時の保存済みPASSを採用して自動承認する**。このfallbackをJobVersionに事前記録する。Requester/Aの無応答で資金を永久凍結しない代わりに、保存時PASSを最終的な根拠にできる小さな客観タスクだけを対象にする。

期限が来た操作はtickだけでなくapprove/disputeコマンドでも同じ期限判定を使う。`now >= review_due_at`なら新規異議は拒否して自動承認処理へ進む。既に有効な異議が記録されていれば承認タイマーは何もしない。approveとdisputeが競合した時は先にcommitした有効遷移が勝つ。

`ojp tick --watch`の独立プロセスをデモ中に稼働させる。AのMCP接続終了だけでは止まらない。全プロセス停止後も期限はDBに残り、再起動時に過期限分を処理する。MCPリクエストが来なければ進まない実装にはしない。

## 13. MCP設計

stdioのみ。読み取りはJobVersion・予算・次の期限・現在必要な操作を返し、Agentが手続きの意味を判断できるようにする。MCPのstdoutにログを混ぜない。

共通書込引数は`operation_id`。Job取得や一覧以外に必須とし、同一Actor・同一payloadの再送に同じ結果を返す。Actorはセッション設定から解決する。金額文字列は6桁小数形式。

| Tool | 主な引数（共通operation_id以外） | 戻り値のdata |
|---|---|---|
| ojp_list_jobs | state?, parent_id?, limit?, cursor? | Job Card要約配列、next_cursor |
| ojp_get_job | job_id | 固定Version、Job state、Lease公開情報、budget、children、期限、判定・送金状態。権限があれば提出JSONと証跡 |
| ojp_claim_job | job_id, expected_version_id | lease_id, generation, expires_at, version_id |
| ojp_heartbeat | job_id, lease_id, generation | expires_at |
| ojp_create_child_job | parent_job_id, lease_id, task_key, budget, deadline | child_id, version_id, parent_budget, policy_used |
| ojp_submit | job_id, lease_id, version_id, artifact_json | submission_id, artifact_hash, verification, state, review_due_at |
| ojp_approve | job_id, submission_id | acceptance, job_state, payment_operation_id, payment_status |
| ojp_dispute | job_id, submission_id, condition_id, reason_code, evidence | dispute_id, state, due_at |

共通応答は成功時`ok=true, data, operation_id?, replayed?`、失敗時`ok=false, error={code,message,retryable,details}`。ドメインエラーはSDKのtoolエラーとしても識別可能にする。代表コードはFORBIDDEN、INVALID_STATE、INVALID_TARGET、LEASE_EXPIRED、CLAIM_CONFLICT、TASK_NOT_ALLOWED、TASK_CONFLICT、POLICY_LIMIT、CHILDREN_UNRESOLVED、VERIFICATION_FAILED、DISPUTE_WINDOW_CLOSED、IDEMPOTENCY_CONFLICT、DB_BUSY。

Paginationの上限は100、既定20。Childの成果物取得はget_jobへ含め、追加のストレージAPIを作らない。Rootの作成・入金・障害注入・tick・決済再試行はCLIへ置き、MCP tool数を8に保つ。

## 14. CLI設計

すべて`--root '<project-root>'`としてローカル設定で固定された同一のプロジェクトルートを解決し、作業ディレクトリの違いで別DBを作らない。個人用絶対パスを公開コードや資料に埋め込まない。書込は`--operation-id`を受け付け、未指定時に生成して結果へ必ず返す。再送時は同じIDを渡す。

| コマンド案 | 目的 |
|---|---|
| ojp demo init | テスト用Requester/A/B/systemとMockWallet seedを準備。既存DBを消さず冪等に動く |
| ojp job create --card FILE | Root DRAFTと固定公開候補Versionを作成 |
| ojp job fund JOB --amount 100.000000 | 正確な全額入金を確認してOPENにする |
| ojp job list / ojp job get JOB | 一覧、仕様、提出物（権限付き）、期限、予算、送金状態 |
| ojp job claim JOB --version VERSION | Claim／Lease作成 |
| ojp job heartbeat JOB --lease LEASE | Lease更新 |
| ojp child create PARENT --lease LEASE --task part-1 --budget 10.000000 --deadline TIME | 許可範囲内で内部拘束しChild公開 |
| ojp job submit JOB --lease LEASE --version VERSION --artifact FILE | JSON成果物提出 |
| ojp job approve JOB --submission SUBMISSION | 検収を確定し送金予約 |
| ojp job dispute JOB --submission SUBMISSION --condition ID --reason CODE | 固定条件に対する異議 |
| ojp job abandon JOB --lease LEASE | Workerの提出前放棄。Requesterの一方的failには使えない |
| ojp job refund ROOT | 確定済み返金予約だけを再処理。有効提出からの直接返金は拒否 |
| ojp ledger show ROOT | D/E/available/locked内訳/paid/refunded、受取人、Operation、保存則 |
| ojp payment retry OPERATION | 同じ支払いIDを再処理。金額・受取人を変更しない |
| ojp tick --once / --watch | 期限処理・固定裁定・送金処理。system専用 |
| ojp mcp --actor ALIAS | 信頼済み設定からActorを選びstdioサーバー起動 |

通常CLIも`--actor`で信頼済みローカルActorを選べる。tick／seedはsystem、refundはRoot Requesterまたはsystem、payment retryは予約の支払元Requester・受取Worker・systemに限定する。どの経路も原資や受取人を変更できない。

`refund`は対象JobからRootを解決してActor権限を先に検査する。無権限なら`FORBIDDEN`、権限があっても対象がChildなら`INVALID_TARGET`。Rootの返金予約が1件もなければ`INVALID_STATE`とし、新しい返金予約は作らない。予約済みのものだけを処理し、送金済みなら既存結果を返す。Lifecycleで返金可能額が0のときに予約を作らない規則とは区別する。

`abandon`は対象JobのLeaseに記録されたWorkerとのActor一致、JobがLEASEDであること、Leaseの指定・有効性の順に検査する。提出後もLease履歴を使ってActorを確認できるため、Child Requester Aなら`FORBIDDEN`、提出済みChildのWorker Bなら`INVALID_STATE`となる。いずれもChildを失敗へ変更できない。

共通`--json`を備える。終了コードは成功0、入力・権限・状態違反2、一時障害3。operation replayは成功0。テスト用時刻操作・failpointは専用test modeのみで有効にし、通常MCPから触れない。

## 15. 内部API

HTTP/RESTは作らない。MCPとCLIが次の同一Application APIを呼ぶ。

- Query: list_jobs / get_job / get_ledger。
- Command: create_root / fund_root / claim / heartbeat / create_child / submit / approve / dispute / abandon。
- Lifecycle: expire_due_leases / approve_due_submissions / resolve_due_disputes / reserve_refundable_balance / process_payments。
- Escrow port: fund(operation_id, immutable_payload)、transfer(operation_id, immutable_payload)、lookup(operation_id)。

各CommandはActorContext、operation_id、型付きpayloadを受け取り、結果を返す。transport層やCLIに独自の残高計算・権限判定を書かない。service層がtransactionを所有し、下位ledger関数はそのtransactionを受け取る。

本番Escrow用のchain_id、wallet署名、x402ヘッダー、外部HTTP APIの空実装は追加しない。

## 16. 同時実行・重複防止

### DB境界

全書込を`BEGIN IMMEDIATE`で開始し、最新Job・Lease・Budgetを読み、権限と期限を検査し、制約付き更新・Journal・Operation結果をcommitする。例外時はrollback。接続ごとにforeign_keysを有効化し、ローカルDBにWAL、耐久性重視のsynchronous設定、有限busy_timeoutを適用する。

DB_BUSYは同じoperation_idでtransaction全体を再試行する。プロセス内Lockだけには依存しない。SQLiteの制約違反を正常なドメイン競合へ変換する。トランザクション内から別接続の書き込みや外部呼出しを行わない。

### 二段階の一意性

1. **再送ID:** operation_id UNIQUE＋actor/kind/payload_hash照合。同ID別payloadや別Actorでの再利用はIDEMPOTENCY_CONFLICT。
2. **業務上一意な効果:** 異なるoperation_idでも同じChildへの支払いや同じ原資の返金が重複しないUNIQUE business_keyを設ける。

| 効果 | 業務キーの例 |
|---|---|
| Root入金 | fund:{root_id} |
| Child拘束 | allocate:{child_id}（Child作成要求operation_idもJobに一意結合） |
| Child原資返却 | return:{child_id} |
| Child報酬 | payout:{child_id} |
| Parent残額報酬 | payout:{root_id} |
| Parent終了時の返金 | refund:{root_id}:terminal |
| 終了後にChildから戻る追加返金 | refund:{root_id}:child-return:{child_id} |

同じChildを別IDで二重approveしても既存Acceptanceと既存PaymentOperationを返す。返金額を毎回再計算して別送金するのではなく、確定した**返金原資の単位**に同じ予約を結合する。残額0なら支払いOperationは作らず正常なno-opにする。

自動処理も一意IDを持つ。例: expiry:{lease_id}、auto-approve:{submission_id}、resolve:{dispute_id}。一連の処理から生じる各資金移動には派生IDを割り当てる。Journalの同操作内複数Entryはoperation_id＋entry_noで一意にする。

### 競合時の期待動作

| 競合 | 結果 |
|---|---|
| 同時Claim | 1件だけLease作成、他はCLAIM_CONFLICT |
| 同時Child作成 | commit済みの拘束・支払い・件数を読み直し、超過側を拒否 |
| Child作成とParent失効/放棄 | 先にChild拘束が確定なら保護、Parent終了が先なら作成拒否 |
| SubmitとLease失効 | 第7節の境界を適用。提出権の二重所有なし |
| 同時approve／自動approve | Acceptanceと支払い予約が1組だけ |
| approveとdispute | 先に成立した合法遷移だけ有効。期限判定を共通化 |
| 同時refund | 同じ返金原資から1予約・1移転だけ |
| Parent終了とChild成功/失敗 | どちらの順でもChild原資を保護し、返金総額が一致 |
| Receipt作成後の再実行 | 既存Receiptで照合し、WalletもJournalも増やさない |

## 17. テスト計画

| 層 | 検証対象 | 手法 |
|---|---|---|
| Unit | 状態遷移表、Actor権限、上限計算、整数変換、検証器、期限境界 | 固定Clock、表駆動、許可・禁止の両方を検証 |
| Integration | 外部キー・UNIQUE・残高／Journal・rollback・Payment Receipt復旧 | メモリDBだけで済ませず実ファイルSQLite。新接続で永続結果を確認 |
| Concurrent Integration | Claim、Child作成、Approval、Refund、Submit/expiry | 独立接続・独立プロセスをbarrierで同時開始。共有メモリLockに頼らない |
| E2E | Human CLI＋A/B MCP stdio＋独立tick＋実DB | MCP handshakeと8 toolを実クライアントから操作。内部関数の直呼びだけで代替しない |
| Sequence | 複数Child、失敗・再試行・期限の順序組合せ | 固定seedの生成操作列を小さな参照会計モデルと比較。各commit後に保存則と権限不変条件を確認 |

4結果は決済完了後に判定する。途中ではlockedやRETRYABLEが残って正しい場合があるため、中間状態の期待値も別に照合する。各E2Eは新しいtest DBと第11節の固定カタログを用い、第7節のDB共有Clockをharnessから進める。N08は後述の専用timing policyで有効提出後・検収期限前に旧Lease期限だけを越え、X09は期限直前／ちょうど／直後、X10は全プロセス停止と時刻更新・再起動を組み合わせる。CLI・MCP・tickが更新後の同じ時刻を観測した証拠を、記録日時と期限判定から確認する。

障害注入は決済commit前、commit後応答消失、Receipt確定後アプリのstatus更新前、裁定応答なし。再起動テストでは実際にプロセスを終了し、新プロセスが同じDBを読んで回復することを確認する。

テストレポートにscenario_id、operation_id、終端Job状態、受取人別金額、locked内訳、保存則の結果を残す。経済的に誤った受取人への支払いは保存則だけでは検出できないため、受取権者との一致を独立にassertする。

## 18. 必須E2Eシナリオ

### 指定4結果

各シナリオはRoot入金100、Aの`part-1`へのChild発注10から始める。「Child失敗」は提出前abandonまたはexpiry、「Parent失敗」も提出前abandonまたはexpiryで再現し、不正な一方的返金で代替しない。

| ID | Child | Parent | A支払い | B支払い | Requester返金 | 最終Escrow |
|---|---|---|---:|---:|---:|---:|
| E01 | 成功 | 成功 | 90 | 10 | 0 | 0 |
| E02 | 失敗 | 成功 | 100 | 0 | 0 | 0 |
| E03 | 成功 | 失敗 | 0 | 10 | 90 | 0 |
| E04 | 失敗 | 失敗 | 0 | 0 | 100 | 0 |

E01ではRequesterがCLIでRootを作成・入金し、AがMCPでClaim/Child作成、BがMCPでClaim/submit、AとRequesterが承認する。Aへの都度Child承認要求や自己資金引落しが存在しないことも確認する。E02〜E04もMCP＋CLIから再現する。

### 添付要件の異常系14項目との対応

| 要件番号・ID | 操作 | 期待結果 |
|---|---|---|
| 1 / N01 | Child未提出の失敗 | child_work 10がParent availableへ戻る。A Walletへの返金ではない |
| 2 / N02 | Bへ10支払い後、Parent失敗 | B=10維持、A=0、Requester=90 |
| 3 / N03 | ChildをLEASED／SUBMITTED／DISPUTEDそれぞれにしてParent失敗 | Child状態と10の拘束を保持。最初の返金は90だけ。各Childは後続処理可能 |
| 4 / N04 | Bのpart-1有効提出後、下記N04の具体的呼び出しを実行。Root自身の有効提出後も別fixtureで確認 | 呼び出しごとの確定エラー、Job状態・全口座残高・予約・Receipt不変。MCPに返金・任意失敗toolなし |
| 5 / N05 | 同じChildを2回承認（同ID・別ID・同時） | Acceptance/PaymentOperation/Receiptはそれぞれ1件。Bへ10だけ |
| 6 / N06 | AとBが同じJobへ同時Claim | 1人だけ成立。敗者のheartbeat/submitを拒否 |
| 7 / N07 | 新規Rootでmax_amount=30、max_ratio_bps=10000、max_children=3。異なるIDでpart-1/part-2を各20で同時作成 | 1件だけ成立、敗者はPOLICY_LIMIT/MAX_AMOUNT。U=20、available=80。重複task_key拒否では代替しない。比率・件数の独立ケースは下記 |
| 8 / N08 | 下記の専用timing policyで有効提出後、review_due_at前かつ旧Lease expires_at後へ進めてtick | SUBMITTEDを維持しOPEN/EXPIREDへ戻らない。Acceptance・支払い予約なし、Leaseは提出完了で閉じたまま |
| 9 / N09 | Aプロセス停止、Parent失効、B提出 | Bは固定検証と期限承認で10受領。Requesterは成果物取得可 |
| 10 / N10 | 不正JSON／誤答／古いVersion／取得不能となる保存失敗 | 有効Submission・review_due_atなし、支払いなし |
| 11 / N11 | 有効提出後Requesterが無応答 | 異議なしなら期限後承認。異議ありなら固定裁定。裁定無応答も既定PASSで解消 |
| 12 / N12 | 送金commit前失敗・commit後応答消失・再起動・再試行 | Job DONE維持、支払い1回。pendingの原資を再利用しない |
| 13 / N13 | Parent先行返金90、その後Child失敗 | 追加返金10のみで累計100。Child成功なら累計90のまま |
| 14 / N14 | E01〜E04、N01〜N13、追加シナリオの各commitを観測 | D=E+paid+refunded、口座非負、親子二重計上なし |

### 設計上の追加必須検証

| ID | シナリオ | 期待結果 |
|---|---|---|
| X01 | 未入金RootをClaim／二重fund／金額不足入金 | 未入金OPENなし。入金・引落しは1回だけ |
| X02 | max_amount=30、max_ratio_bps=10000、max_children=3、max_depth=1。part-1を20で承認し送金commit前で停止、part-2を20で作成試行後、別operation_idで同じpart-2を10で作成 | 20はPOLICY_LIMIT/MAX_AMOUNT、10は成功。拒否された20の試行はJobもtask_keyも確保しないため、同じpart-2の10は重複扱いにならない。前者のU=20・available=80を維持し、後者はU=30・available=70。child_payoutもUに含む |
| X03 | Child送金停止中にParent成功 | Parent判定・90支払いは可能。Bの10はlockedに残り復旧後1回支払う |
| X04 | part-1がLEASEDの間にParent提出 | CHILDREN_UNRESOLVED、タイマーなし。Child判定後に提出できる |
| X05 | max_children=3でpart-1を10で作成→失敗を3回繰り返し、4件目のpart-1を試行。別Rootではpart-1 Childの有効Worker Bが、そのChildを親としてpart-2の孫発注を試行 | 2・3件目の作成は成功し各失敗後U=0・available=100。4件目はPOLICY_LIMIT/MAX_CHILDREN。孫要求はtask_key検査より先にPOLICY_LIMIT/MAX_DEPTH、資金移動なし |
| X06 | A/Bが成功条件・検証器・入力・受取人を書換え | 拒否。公開Versionのhash・受取権者維持 |
| X07 | 同一operation_idに別金額・別Actor、異なるIDで同じ返金効果 | ID流用拒否、業務キーでも効果は一度だけ |
| X08 | 同時Refund（初回90・追加10それぞれ）、Parent終了とChild returnの競合 | 返金累計100以下。どちらの順でも同じ最終結果 |
| X09 | heartbeat/submitと失効、approve/disputeと期限ちょうど | 境界定義どおり。閉じたLeaseや終端状態が復活しない |
| X10 | 全プロセス再起動後の過期限処理とReceipt照会 | 保存期限から処理再開。二重裁定・二重送金なし |
| X11 | Root RequesterがA停止後にBの成果物取得、無関係Actorが取得 | Requesterには保存内容/hash一致、無関係Actorには非公開 |
| X12 | max_amount=30、max_ratio_bps=3000、max_children=3、max_depth=1でpart-1/2/3を各10で作成。part-1成功・Bへ10支払済み、part-2失敗返却、part-3はBが未提出LEASEDのままParent失敗 | 先行返金80、part-3のchild_work=10を保護。別fixtureでpart-3成功ならB累計20・返金80、失敗ならB累計10・返金90。A=0、最終Escrow=0 |
| X13 | part-1が生存中／成功済みの別fixtureでpart-1を再作成。別ケースで未知part-unknownを指定してカタログ外タスク作成を試行 | 重複はTASK_CONFLICT、未知はTASK_NOT_ALLOWED。予算・件数枠に余裕を持たせ、POLICY_LIMITに隠さない。入力等の書換え拒否はX06でも検証。都度Requester確認なし |
| X14 | 決定的裁定PASS／証拠付きFAIL／裁定応答なし | PASS→支払い、FAIL→返却、無応答→保存PASS採用。全件解決状態へ |
| X15 | 別Root・別Lease・非検収者からの操作 | 権限拒否しJob・資金・成果物公開範囲が不変 |

#### N07の独立した上限競合fixture

各ケースは新しいRoot・入金100・有効なParent Leaseで、part-1とpart-2を異なるoperation_idで同時作成する。カタログは第11節のまま、policyだけをRoot公開前に設定する。金額ケースはN07表の設定、比率ケースはmax_amount=100／max_ratio_bps=3000／max_children=3で各20とし、敗者は`POLICY_LIMIT/MAX_RATIO`、U=20・available=80。件数ケースはmax_amount=100／max_ratio_bps=10000／max_children=1で各10とし、敗者は`POLICY_LIMIT/MAX_CHILDREN`、作成数1・U=10・available=90。通信成功だけでなく、拒否理由と確定残高まで照合する。

#### N08の専用時刻fixture

このfixtureだけはRoot JobVersionのLeaseを300秒、Root deadlineを`t0+300秒`より後にし、Child JobVersionのLeaseを60秒、review_windowを120秒、Child deadlineを`t0+180秒`より後に設定する。共有Clockを`t0`に固定したままAがRootをClaimしてChildを作成し、BもChildをClaimするため、AのRoot Leaseは`expires_at=t0+300秒`となる。Bは`expires_at=t0+60秒`を得て、共有Clockの`t0+1秒`で有効提出する。これにより`review_due_at=t0+121秒`となり、旧Child Lease期限より後になる。harnessは共有Clockを`t0+61秒`へ進め、`child_expires_at < now < child_review_due_at < root_lease_expires_at`を確認してから独立tickを1回実行する。

tick後もChildはSUBMITTED、AcceptanceとPaymentOperationは0件、全口座残高は提出直後から不変であることを確認する。Leaseの`expires_at`は監査値として`t0+60秒`のまま、`closed_reason`は提出完了のままで、expiry処理によるEventや新Leaseは作らない。その後は同じfixtureをreview_due_atまで進め、通常の自動承認経路が機能することをN11の一部として確認してよい。

#### N04の具体的呼び出し

RはRoot Requester、AはParent WorkerかつChild Requester、BはChild Worker。まずParentは有効LEASED、Childはpart-1・予算10・Bの有効提出によるSUBMITTED、返金予約なし、検収期限前の固定時刻を用いる。拒否確認中はtickを進めず、各呼び出しに新しいoperation_idを使う。

| 呼び出し（共通root指定・JSON・operation-idは省略） | 期待コード |
|---|---|
| `ojp --actor A job refund CHILD` | FORBIDDEN（Childから辿ったRootの返金権限なし） |
| `ojp --actor A job refund ROOT` | FORBIDDEN |
| `ojp --actor A job abandon CHILD --lease B_LEASE` | FORBIDDEN（AはChild Workerではない） |
| `ojp --actor B job abandon CHILD --lease B_LEASE` | INVALID_STATE（有効提出後） |
| `ojp --actor R job refund CHILD` | INVALID_TARGET（返金対象はRootのみ） |
| `ojp --actor R job refund ROOT` | INVALID_STATE（返金予約なし） |

Root自身の有効提出後は別fixtureで全Child判定終端・Root SUBMITTED・返金予約なしを作り、Rの`job refund ROOT`とAの`job abandon ROOT --lease A_LEASE`がどちらも`INVALID_STATE`となることを確認する。全拒否操作のCLI終了コードは2。Job状態、有効Submission、Acceptance、全口座残高、Journal、PaymentOperation、Receiptを前後比較し、変化を認めない。拒否のOperation結果・監査記録だけは追加可能。MCPの`tools/list`は第13節の8件と完全一致し、返金・任意fail/cancel toolがないこともassertする。テストのために操作面を追加しない。

## 19. 実装順序

本書の作成をPhase 0とする。今回の作業はここまで。後続の実装依頼を受けたら基準資料と本書を読み直し、次の順で進める。

```text
Phase 1 環境・型・DB
   ↓
Phase 2 Mock資金・冪等な決済
   ↓
Phase 3 Job・Lease・Child
   ↓
Phase 4 提出・検収・異議・期限
   ↓
Phase 5 MCP・CLI
   ↓
Phase 6 E2E・再起動・競合・再現手順
```

各Phaseは後述の完了条件を満たしてから進む。前段の検査で問題が出た場合はそのPhaseを修正する。Agentを利用する場合もこの依存順と検証ゲートを維持し、役割分担を理由に機能を追加しない。

計画の修正は直接行う。後続で委任を使う場合は、その時点のユーザー許可と適用ルールに従い、担当範囲とレビュー対象を本計画へ対応させる。運用記録は非公開の所定の場所へ保存し、計画の自己点検と独立レビューの完了状態を混同しない。

## 20. 各Phaseの完了条件

| Phase | 作業 | 次へ進める条件 |
|---|---|---|
| 0 | 資料配置・実装計画 | 原文一致、今回の対象・禁止事項・状態・台帳・API・全必須テストが文書化済み。実装コードなし |
| 1 | uv環境、依存lock、型、migration 001（RuntimeClockを含む）、Actor、Clock、3タスクのfixture | 選んだPython／MCP SDKの版を記録。新DBが作れ外部キー・UNIQUE・整数変換のテストが通る。独立プロセスが同じ注入時刻を観測し再起動後も保持、時刻後退・mode不一致を拒否。SDK stdio接続の互換性確認 |
| 2 | Root funding、Budget、Journal、PaymentOperation、MockWallet/Receipt | fund・reserve・return・pay・refundごとに保存則成立。二重操作・障害注入・応答消失からの復旧Integrationが通る |
| 3 | JobVersion、Claim/Lease、heartbeat/expiry、Child policy、Parent終了 | 同時Claim・同時Child作成・上限・提出前失効・Parent失敗時Child保護が通る |
| 4 | JSON提出、固定検証、承認、Dispute、tick、追加返金 | 有効提出後の直接返金拒否、条件固定、無効提出タイマーなし、A停止／固定裁定／無応答fallbackが通る |
| 5 | CLIと8 MCP tools、共通エラー、権限付きget | 独立Actorのstdioから8 toolsを呼べる。CLIも同じ状態・金額を返す。MCP・CLI・tickがDB共有Clockを使い、通常経路に時刻操作がない。transport側に別ロジックなし |
| 6 | 全E2E・競合・再起動・再現README | E01〜E04、N01〜N14、X01〜X15が通る。各commitの会計と受取権を照合。クリーンDBから人間が手順を再現できる |

現在はPhase 0のみ完了対象。表の後続チェックは計画であり、実装・テスト済みという意味ではない。

## 21. PoC完了条件チェックリスト

- [ ] mock-USDC／手数料0／Root→Child一段／単一ローカルDBに限定される。
- [ ] 未入金Jobは公開されず、Claimでは送金しない。
- [ ] Aの自己資金・都度承認なしにChildを発注できる。
- [ ] 支払い済み＋拘束中の再委託上限、金額・比率・件数・深さ制限を同時実行でも守る。
- [ ] Workerが条件・テスト・入力・受取人をすり替えられず、ClaimしたVersionに束縛される。
- [ ] 提出前Lease失効と提出後検収タイマーが分離される。
- [ ] 有効提出後の一方的な失敗／即返金を拒否する。
- [ ] Parent FAILED/EXPIREDでも既存Childの資金・進行・成果物取得権が残る。
- [ ] A停止後のChild処理と、検収者／裁定処理の無応答fallbackが実動する。
- [ ] Child失敗はParent内部返却、Parent終了後は正確な追加返金になる。
- [ ] Jobの検収と送金結果が独立し、送金失敗でDONEが戻らない。
- [ ] operation_idと業務キーにより二重拘束・二重払い・二重返金を防ぐ。
- [ ] 4結果と必須異常系14項目・追加テストが自動E2Eで再現される。
- [ ] すべての確定会計操作で保存則・非負性・正しい受取人を検証する。
- [ ] MCPの8 toolsとCLI、独立tick、DB再起動復旧を実プロセスで確認する。
- [ ] READMEの手順で依存lockから起動・実行・台帳確認・テストを再現できる。
- [ ] 除外機能を実装せず、docsと作業記録に実装済み／保留が正しく記録される。

## 22. 主なリスクと設計上の対処

| リスク | 対処・PoCの限界 |
|---|---|
| 親の集計と子口座の二重計上 | 排他的口座を正本にし、Root集計は導出。各操作後にJournalと保存則を検証 |
| 承認済み未送金額の流用 | child_payoutをlockedと再委託使用額に含め、Receiptがある時だけpaidへ移す |
| Parentが先に返金して後からChild失敗 | terminal返金とchild-return返金を別の原資キーで予約。返却・追加予約は原子的 |
| 異なるoperation_idによる二重効果 | business_key UNIQUEと原資口座の一回限り消費を併用 |
| A消失でChild検収不能 | 独立tick・固定裁定・有限期限・保存PASS fallback。アプリ全停止中の進行は保証しない |
| 提出と失効の競合 | 同一transaction、DBに記録した期限、境界値テスト、提出後Leaseを閉じる |
| 見かけ上PASSの無価値Childで予算消費 | Requester事前許可の入力・タスクカタログに限定。経済的価値やSybil耐性の一般解は対象外 |
| 検証器誤りとDispute fallback | 決定的で小さなJSON課題だけに限定し、Version/hash・証跡を保持。一般成果物へ無検討で流用しない |
| DBへの直接アクセスでActor偽装 | ローカル信頼環境の制約を明示。サービス権限はテストするが本番認証を付け足さない |
| SQLiteロック中の処理肥大 | サイズ上限付き同期JSON検証、外部I/O禁止、短いtransaction、DB_BUSYの有限待機と再試行 |
| Parent提出がChild解決待ちで失効 | 事前に示したPoC制約として扱う。Parent/Child deadlineをデモで適切に設定し、未解決時の拒否を表示 |
| SDKの変化 | 実装開始時に公式安定版を確認しlock。Phase 1でstdio互換性を検証。版不一致を曖昧なまま進めない |
| 将来Escrowの交換を過小評価 | portで責務分離するが、本番の最終性・照会・障害回復・署名は別フェーズとして再設計 |

## 23. 今回絶対に実装しないもの

- Echtgeld／実資金、Mainnet、本番USDC、本番Escrow Smart Contract。
- x402本番接続、独自x402 Scheme、外部Wallet／Facilitatorへの決済接続。
- 任意深度のParent/Child、複数Registry同期、高度な分散構成、マイクロサービス。
- Reputation、Stake、ZK、Reviewer市場、Arbiter市場。
- 主観的成果物の判定、複雑な修正ループ、公開後の仕様変更交渉。
- GitHub連携、Upwork連携、CrowdWorks連携、Web Marketplace。
- 本番認証基盤、公開HTTP API、任意コード実行基盤。
- この計画作成ターンでの実装コード、依存導入、DB作成、アプリテスト実行。

## この計画のまま実装開始してよいか

**判定: YES**

**レビュー状態: 指摘計10件を修正済み。設計内容版（SHA-256: `b23a75959fe756f5ea48f3b8a15d843ba759f250088bdee22a621fb7cf553ca0`）は2026-09-06に独立Reviewerがpass判定し、本状態表示はレビュー後に追記。**

複数Childの固定カタログと上限競合fixture、全プロセス共有のテスト時計、有効提出後の返金拒否の呼び出しと期待エラーに加え、N08のParent／Child時刻関係、孫発注の検査優先順位、X02のtask_key、X02／X12のpolicyを具体化した。基本の資金・権利ルールとPoC対象範囲を維持しており、Phase 1から追加の重要仕様判断なしに着手できる計画である。

独立レビューpassの対象は上記SHA-256の設計内容版であり、本状態表示はその後の管理情報として追記した。計画作成者の判定YESと独立レビューpassは、実装済みまたはユーザーによる実装着手承認を意味しない。今回の依頼は計画修正までであり、実装コード・依存導入・アプリテストは次の段階で扱う。
