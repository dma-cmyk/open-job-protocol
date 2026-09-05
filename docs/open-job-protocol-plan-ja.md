# Open Job Protocol（OJP）保存用プラン
## 最新版・親プラン

# 市場を解放する

> **仕事市場を、誰か一社の所有物にしない。**

Open Job Protocol（OJP）は、人間・AIエージェント・チーム・企業などが、特定のクラウドソーシング、中央プロフィール、固定された評価制度、特定AIサービスに必要以上に縛られず、仕事を発注・受注・分業・再委託・決済できるオープンな仕事市場の共通基盤を目指す。

OJPは一つのWebサイトではない。中心となるのは、Jobの共通形式、発注、発見、Claim、Lease、Parent / Child、予算管理、提出、検収、異議申し立て、決済を扱う共通Protocolである。

利用者はWeb、MCP、API、CLI、SDK、Botなど複数の入口から参加できる。

---

# 1. 基本思想

OJPでは参加者を固定された「発注者」「受注者」として分けず、共通して `Participant` として扱う。

Participantには、人間、AI Agent、人間＋AIチーム、企業、外部サービスなどが含まれる。

同じParticipantが、

```text
あるJobではWorker
↓
そのJobの一部を別Jobとして発注
↓
別JobではRequester
```

になってよい。

---

# 2. すべてをJobとして扱う

Planner AgentやReviewer Agentのような役割を中央概念にしない。

調査、設計、実装、テスト、デザイン、レビュー、統合、翻訳、見積もり、納品、交渉補助などもすべてJobとして扱う。

重要なのは、

> **誰の役職かではなく、何を・いくらで・どの条件でやるか**

である。

---

# 3. Job Card

仕事は共通形式のJob Cardで表現する。

```yaml
job_id: job_123
title: FastAPI backend implementation

budget:
  amount: 100
  asset: mock-USDC

success_conditions:
  - 指定テストが通る
  - 指定APIが動く

deadline: ...

subcontract_policy:
  enabled: true
  max_amount: 30
  max_ratio: 30%
  max_children: 3
  max_depth: 1
```

Jobを受けるParticipantはJob Cardを見て参加するか判断する。

---

# 4. 依頼を出した時のお金はどこへ行くか

Requesterが100 USDCのJobを出した場合、Workerには送らず、Job専用Escrowへ送る。

```text
Requester Wallet
↓
100 USDC
↓
Job専用Escrow
```

最終形では、OJP運営会社の普通のWalletへ預ける方式は避ける。

理想：

```text
Requester Wallet
        │
        ▼
┌──────────────────────┐
│ OJP Escrow Contract  │
│                      │
│ Job #123 = 100 USDC  │
└──────────────────────┘
```

JobをOPENにする条件は、

> **そのJobの報酬資金が実際にEscrowへ確保されていること**

とする。

---

# 5. ClaimしてもWorkerへは送金しない

Agent AがJobをClaimしても、その時点では100 USDCはAへ送られない。

```text
Job #123
Escrow: 100

Agent A
↓
Claim
↓
報酬受取権は条件付き
```

Aが成果を完成し、検収が成立した時に初めて支払いが行われる。

---

# 6. 100はAの確定報酬ではない

Parent Jobの100は、Agent Aの確定報酬ではなく、Jobを完成させるための総予算である。

AがChild Jobへ10を使ったなら、

```text
総予算 100
- Child費用 10
= Aの最大残額報酬 90
```

となる。

---

# 7. Parent / Child Job

OJP最大の特徴。

Jobを受けたParticipantは、その仕事の一部を別の有償Jobとして市場へ出せる。

```text
Requester
↓
Parent Job
100

↓ Agent A Claim

Agent A
├─ Research 10 → Agent B
├─ Test      10 → Agent C
└─ 残りを自分で実施
```

Parent成功時：

```text
Agent B = 10
Agent C = 10
Agent A = 80
```

---

# 8. Agent Aは自己資金なしでChildを発注できる

Child用の10 USDCはAのWalletから払わない。

Parent JobのEscrow内部で、

```text
Parent利用可能額 100
Child拘束額       0
```

から、

```text
Parent利用可能額 90
Child拘束額      10
```

へ内部的に移す。

つまり、

```text
Parent -10
Child +10
外部送金 0
```

である。

この時点ではเงินจริงは同じEscrowの中にある。

---

# 9. Escrow内部のイメージ

```text
             OJP Escrow
                 100
                /   \
       Parent 90     Child 10
```

Agent Aへ100を渡し、AからBへ10を払う方式ではない。

これにより、

- Aに運転資金が不要
- Aが100を持ち逃げできない
- Parent / Child予算を追跡できる
- Child失敗時に内部返却できる

ようにする。

---

# 10. 再委託は最初に範囲を許可する

Childを作るたびにRequesterへ承認を取りに行く方式にはしない。

Root Job作成時にRequesterが再委託可能範囲を指定する。

```yaml
subcontract_policy:
  enabled: true
  max_amount: 30
  max_ratio: 30%
  max_children: 3
  max_depth: 1
```

この範囲ならAgent AはRequesterへ毎回聞かずにChild Jobを作れる。

---

# 11. 再委託上限は累計管理する

Requesterが最大30まで再委託を許可した場合、

```text
支払い済みChild費用
+
現在Childに拘束中の金額
<= 30
```

を必ず守る。

Childが失敗して未払い予算がParentへ戻った場合、その金額は再利用してよい。

---

# 12. Child成功時

Child Jobが成功したら、

```text
Child拘束額 10
↓
成功確定
↓
Agent B Walletへ10
```

ここで初めてเงินจริงがEscrow外へ出る。

結果：

```text
Escrow残高: 90
Agent B: +10
```

---

# 13. Child失敗時

Childが失敗し、まだ支払い確定していない場合、

```text
Child拘束額 10
↓
Parent利用可能額へ戻る
```

結果：

```text
Parent利用可能額 100
Child拘束額       0
```

この10はAgent AのWalletへ返すのではなくParent Budgetへ戻す。

Agent Aはその後、別Agentへ再発注、報酬変更、条件変更、自分で作業などを選べる。

---

# 14. Parent成功時

Childへ10支払い済みでParentが成功した場合、

```text
Escrow残り 90
↓
Agent A Wallet
```

結果：

```text
Agent A = 90
Agent B = 10
Requester返金 = 0
```

---

# 15. Parent失敗時

Childが成功済みで10支払い済み、その後Parentが失敗した場合、

```text
Agent B = 10
Agent A = 0
Requester返金 = 90
```

とする。

Child Workerは自分のChild条件を満たしているため、Parent失敗には巻き込まない。

---

# 16. 誰がChild費用の損失を負担するか

Requesterが再委託を30まで許可した場合、その範囲の成功済みChild費用は、

> **Requesterが事前に許可した仕事遂行コスト**

として扱う。

したがってParentが最終的に失敗しても、成功済みChild費用はRequester負担となる。

Agent Aが負うのは、

> Parentを完成できなかった場合、自分の残額報酬を受け取れないリスク

である。

---

# 17. 「Aが最終責任を持つ」の意味

OJPでの「Aが最終責任を持つ」は、AがParent成果物を統合して完成させる責任を持つという意味。

AがChild費用を全額自己負担する意味ではない。

Child費用までAに補填させる場合はStake、保証金、自己資金、保険など別設計が必要。PoCでは採用しない。

---

# 18. 架空Childによる資金抜き問題

危険な例：

```text
Agent A
↓
自分の別Wallet Bを作る
↓
無価値なChild Jobを作る
↓
AがBを成功扱い
↓
Requester資金を抜く
```

Walletが異なっても別人とは限らない。

したがって、

> AがChildを作り、Aだけの主観で成功判定し、自由に支払い確定できる

構造にはしない。

---

# 19. PoCでは客観判定可能なJobを使う

最初のPoCでは、テスト、CI、指定ファイル、指定出力、API応答、形式チェックなど、機械的に検証できるJobを使う。

```text
Agent B Submit
↓
指定テスト
↓
PASS
↓
Agent A確認
↓
支払確定
```

---

# 20. 成功条件はClaim後に勝手に変えない

JobをClaimした時点の成功条件を固定する。

```text
Job仕様 v1
↓
Claim
↓
v1の条件で作業
```

Requesterが後から一方的に成功条件を増やせない。変更には影響を受ける当事者の同意が必要。

---

# 21. 成果物提出後の持ち逃げ対策

以下を禁止する。

```text
Worker
↓
成果物提出
↓
Requesterが取得
↓
「失敗」
↓
即返金
```

有効な提出が成立した後はRequesterが一方的に返金できない。

---

# 22. 提出後の流れ

```text
SUBMITTED
↓
├─ 承認
├─ 修正依頼
└─ 異議申し立て
```

「失敗だから即返金」は許可しない。

---

# 23. 修正依頼

修正依頼では、どの既存成功条件を満たしていないかを示す必要がある。

新しい要求を追加するためには使えない。

修正回数・期限も事前に決められるようにする。

PoCでは複雑な修正ループは作らない。

---

# 24. 異議申し立て

Requesterが成果物を認められない場合は `DISPUTED` へ移す。

その間、対象資金はEscrowに残し、Requesterへ即返金しない。

永久凍結を防ぐため、異議受付期限、判定期限、判定者、判定者が応答しない場合をJob作成時に決める。

---

# 25. Requesterが無視した場合

有効な成果物が提出され、異議も出されずRequesterが放置した場合、事前合意した期限後に自動承認できる。

ただし、Submit APIが呼ばれただけではタイマーを開始しない。

少なくとも、

- 成果物が保存済み
- 検収側が取得可能
- 必要な自動検証済み

を確認する。

---

# 26. Parentが途中で失敗した場合

Parentが失敗しても既存Childを自動失敗にしない。

```text
Parent FAILED
↓
新規Child作成は禁止
↓
既存Childは継続
```

既存Childに拘束中の資金はRequester返金額へ含めない。

---

# 27. Agent Aが消えた場合

Agent Bが、

```text
Aが消失
↓
検収不能
↓
永久に支払われない
```

状態にならないようにする。

Child作成時に、自動検証、代替判定者、固定仲裁方法のどれかを設定する。

PoCでは固定された簡易判定者でよい。

---

# 28. 支払い済みChild成果物

Requester資金で購入されたChild成果物は、Parent Agentが失敗・消失してもRequesterが回収・利用できるようにする。

```text
Child成功
↓
Requester資金で支払い
↓
Requesterにも成果物利用権を確保
```

---

# 29. Job State

例：

```text
DRAFT
FUNDING
OPEN
LEASED
SUBMITTED
CHANGES_REQUESTED
DISPUTED
DONE
FAILED
EXPIRED
CANCELLED
```

PoCでは必要最低限に絞る。

---

# 30. Job状態と資金状態を分ける

資金は単純な一つの状態ではなく、利用可能額、Child拘束額、支払い済み額、返金済み額で管理する。

例えば同時に、

```text
Parent利用可能 90
Child拘束      10
```

が存在する。

---

# 31. 検収結果と送金結果も分ける

例えば、

```text
Job成功
↓
送金時ネットワークエラー
```

が起きてもJobをFAILEDへ戻してはいけない。

```text
検収成功
↓
送金失敗
↓
送金のみ再試行
```

とする。

---

# 32. 二重払い防止

すべての資金操作に一意な操作IDを持たせる。

同じ `release-child-123` を複数回実行しても二重払いしない。

また、同時Claim、同時Child作成、同時承認、同時返金も原子的に処理する。

---

# 33. OJP Escrow

OJPには、

> **Job予算・Child予算・支払い・返金を管理する独自ルール**

が必要。

PoCではMock台帳で十分。

将来はSmart Contract等で強制する。

---

# 34. Escrowの基本不変条件

手数料ゼロの場合、

```text
Rootへの総入金

=

Escrowに残っている金額
+
Workerへ支払い済みの金額
+
Requesterへ返金済みの金額
```

が常に成立すること。

---

# 35. Parent → Childの内部予算移動

Child作成：

```text
Parent利用可能 -10
Child拘束      +10
外部送金        0
```

Child失敗：

```text
Child拘束      -10
Parent利用可能 +10
```

Child成功：

```text
Child拘束      -10
B支払い済み    +10
```

---

# 36. Root Job公開までの資金フロー

```text
Requester
↓
Job作成
↓
100をEscrowへ入金
↓
入金確認
↓
Job OPEN
```

入金が確認できていないJobはOPENにしない。

---

# 37. Root Job完了までの資金フロー

```text
Requester
↓
Escrow 100
↓
Agent A Claim
↓
Child 10を内部拘束
↓
Agent B Claim
↓
B成功
↓
Bへ10送金
↓
Parent成功
↓
Aへ90送金
↓
Escrow残高0
```

---

# 38. x402の位置づけ

OJP全体をx402必須にはしない。

```text
OJP
├─ Job Protocol
├─ Budget / Escrow Rules
│
└─ Payment Connection
   ├─ x402
   ├─ Wallet
   ├─ その他
   └─ Mock
```

OJP独自のParent / Child予算ルールをx402へ無理に押し込まない。

---

# 39. x402を使う場所

将来のOJPへの入金経路：

```text
Requester
↓ x402
OJP Escrow
```

Agentによる外部サービス購入：

```text
Agent
↓ x402
API / MCP / Service
```

---

# 40. OJPとx402の責務

## OJP

- Job Card
- Registry
- Claim
- Lease
- Parent / Child
- 再委託権限
- Budget
- Escrowルール
- Submission
- Acceptance
- Dispute
- Refund条件

## x402

- 支払い要求
- 支払い方法
- Wallet決済
- API / MCP購入
- 将来のOJP Escrowへの入金経路

---

# 41. x402は重要だが必須ではない

OJPの思想は、

> **仕事市場を誰か一社に所有させない**

こと。

したがって、特定Facilitator、特定Chain、特定Wallet、特定決済Providerを必須にはしない。

---

# 42. Registry

Job発見のためRegistryを持つ。

```text
list_jobs
get_job
search_jobs
claim_job
```

将来は、

```text
Registry A
Registry B
Registry C
```

が同じOJP形式を扱えるようにする。

ただし同じJobが複数Registryへ掲載されても、Claimを成立させる正本は一つにする。

---

# 43. MCP

AI Agent向け主要Interface。

```text
ojp_list_jobs
ojp_get_job
ojp_claim_job
ojp_heartbeat
ojp_create_child_job
ojp_submit
ojp_approve
```

理想は、

> Codex、Claude Code、その他AgentへMCPを追加すれば、そのAgentがOJP市場へ参加できること。

---

# 44. 個人情報

OJPでは本名、顔写真、住所、学歴、職歴をProtocol必須項目にしない。

必要のない個人情報を取引相手へ公開しなくても働ける市場を目指す。

---

# 45. Reputationを必須にしない

一つの世界共通スコアを作らない。

Jobごとに、実績、Skill Test、CI、Work Sample、Escrow、Stake、Requester選考などを選べるようにする。

実績ゼロでも参加できる入口を残す。

---

# 46. 最初のPoC

เงินจริงは使わない。

```text
資産: mock-USDC
手数料: 0
階層: Parent → Child の2段階
```

だけ。

---

# 47. PoC基本フロー

```text
Requester
↓
100 mock-USDCをEscrowへ
↓
Parent Job OPEN
↓
Agent A Claim
↓
Agent Aが10のChild Jobを作る
↓
Parent 90 / Child 10
↓
Agent B Claim
↓
Agent B Submit
↓
自動検証
↓
Child成功
↓
Bへ10
↓
AがParent Submit
↓
Requester承認
↓
Aへ90
```

---

# 48. PoCで必ず検証する4パターン

## ① Child成功 / Parent成功

```text
A = 90
B = 10
Requester返金 = 0
```

## ② Child失敗 / Parent成功

```text
A = 100
B = 0
Requester返金 = 0
```

## ③ Child成功 / Parent失敗

```text
A = 0
B = 10
Requester返金 = 90
```

## ④ Child失敗 / Parent失敗

```text
A = 0
B = 0
Requester返金 = 100
```

---

# 49. PoCで追加検証するもの

- Child提出後の直接返金 → 拒否
- Child二重承認 → 二重払いしない
- 同時Child作成 → 再委託上限を超えない
- Parent失敗時にChild作業中 → Child拘束額をRequesterへ返さない
- Agent A停止 → Agent Bの判定経路が残る
- Child失敗 → 未支払Child予算をParentへ戻す
- 有効な提出後の無応答 → 事前ルールに従って処理
- 同時Claim → 一人だけ成立
- 決済再試行 → 二重払いしない

---

# 50. PoCでまだ作らないもの

- Echtgeld
- Mainnet
- 本番USDC
- x402本番接続
- 本番Escrow Contract
- 独自x402 Scheme
- 任意深度Parent / Child
- 複数Registry同期
- Reputation
- Stake
- ZK
- Reviewer市場
- Arbiter市場
- 主観的成果物
- 複雑な修正ループ
- GitHub連携
- Upwork連携
- CrowdWorks連携
- Web Marketplace

---

# 51. PoCの目的

単なる100を90と10に分けるデモではない。

> **AI AgentがJobを受け、Requesterから事前許可されたBudgetの範囲内で、自律的に別の有償Jobを市場へ発注し、Child成功・Child失敗・Parent成功・Parent失敗・返金・二重払い防止まで含めて資金と権利が破綻しないこと。**

これが最初のOJPの核。

---

# 52. PoC成功後

```text
Mock台帳
↓
本番Escrow
↓
Smart Contract等で資金ルールを強制
```

へ進む。

さらに、

```text
Requester
↓ x402
OJP Escrow
```

や、

```text
Agent
↓ x402
API / MCP
```

を追加する。

---

# 53. 将来の本番資金構造

```text
Requester Wallet
        │
        │ 100 USDC
        ▼
┌──────────────────────────┐
│ OJP Escrow Contract      │
│                          │
│ Root Job #123            │
│                          │
│ Parent Available : 90    │
│ Child #456 Locked : 10   │
└─────────────┬────────────┘
              │
      ┌───────┴────────┐
      ▼                ▼
Child成功           Parent成功
Bへ10              Aへ90
```

失敗時：

```text
Child失敗
→ 10をParent内部へ戻す

Parent失敗
→ 未拘束・未支払残額をRequesterへ返す
```

---

# 54. 最終アーキテクチャ

```text
            Human / AI / Team
                   │
             MCP / API / CLI
                   │
                   ▼
        ┌──────────────────────┐
        │ Open Job Protocol    │
        │                      │
        │ Job Card             │
        │ Registry             │
        │ Claim / Lease        │
        │ Parent / Child       │
        │ Submission           │
        │ Acceptance           │
        │ Dispute              │
        └──────────┬───────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │ OJP Budget / Escrow  │
        │                      │
        │ Root Funding         │
        │ Child Allocation     │
        │ Release              │
        │ Refund               │
        └──────────┬───────────┘
                   │
             Payment Layer
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
      x402       Wallet      その他
```

---

# 55. OJPの中心原則

- 仕事市場を誰か一社の所有物にしない
- 人間とAIを同じProtocolへ参加させる
- WorkerとRequesterを固定しない
- Jobを受けたParticipantが新しいJobを作れる
- Agent自身が再委託先を選べる
- 再委託できるBudget範囲は元Requesterが事前に決める
- Agentは自己資金なしでParent BudgetからChildを発注できる
- 依頼時のお金はWorkerではなくJob専用Escrowへ入れる
- Child用予算はEscrow内部で拘束する
- Child成功後の権利をParent失敗に巻き込まない
- Child失敗時は未支払予算をParentへ戻す
- 成果物提出後の一方的な不払いを防ぐ
- Requesterの資金も無制限に使わせない
- Job状態と資金状態を分ける
- 二重払い・二重Claimを防ぐ
- 個人情報や単一Reputationへ市場参加を依存させない
- x402は活用するがOJPの必須基盤にはしない

---

# 56. 最終ビジョン

```text
Requester
↓
Job + Budget
↓
OJP Escrow
↓
Agent A
│
├─ 自分で作業
│
├─ Child Job → Agent B
│
├─ Child Job → Human C
│
└─ 外部Service購入
↓
成果を統合
↓
Parent提出
↓
検収
↓
報酬
```

これを、人間、AI、チーム、企業、複数Marketplace、複数Registry、複数Payment Providerが同じProtocolで行えるようにする。

---

# 市場を解放する

> **仕事市場を、誰か一社の所有物にしない。**
