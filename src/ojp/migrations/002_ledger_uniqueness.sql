-- OJP PoC migration 002: 台帳の一意性実効化と MockWallet の Journal 上の表現
-- （計画書 第9節・第16節）。001 は変更せず、番号付きで追記する。
--
-- 1. operations.business_key は 001 では UNIQUE が無く、異なる operation_id で
--    同じ業務効果（二重 fund・同じ Child の二重拘束・同じ原資の二重返金予約）
--    を防げなかった。NULL は業務キーを持たない操作（自動処理の個別 ID 等）を
--    許すため、NULL 非表示の通常 UNIQUE INDEX で一意化する（SQLite の UNIQUE は
--    NULL 同士を重複とみなさない）。
CREATE UNIQUE INDEX operations_business_key_unique
    ON operations (business_key);

-- 2. budget_accounts の 001 の UNIQUE (root_id, owner_job_id, bucket,
--    beneficiary_id, source_key) は NULL 列を含むため SQLite では重複を防げず、
--    同一口座（例: Root の available 行）が二重に作られ得た。COALESCE で NULL を
--    正規化した一意 INDEX で実効化する。冪等再試行の INSERT OR IGNORE が
--    口座を「作る」効果の単位としても使う。テーブルはブロック4で作り直すため
--    INDEX はそこで 1 回だけ作る（ここで作っても DROP TABLE と一緒に消え、
--    同名 INDEX の二重定義で失敗する）。

-- 3. journal_entries.account_id は budget_accounts への FK のため、MockWallet 側の
--    出入り（Root funding、将来の送金）を Journal に載せる表現が必要。
--    Wallet の台帳上の相手口座を保持する専用テーブルを追加し、行が必ず
--    mock_wallets の行と対応することをトリガーで強制する（逆方向は FK）。
--    台帳不変条件（全口座非負・Journal 合計 0）を Wallet 含めて検証できる。
CREATE TABLE wallet_ledger_accounts (
    account_id      TEXT PRIMARY KEY REFERENCES budget_accounts (id),
    participant_id  TEXT NOT NULL UNIQUE REFERENCES mock_wallets (participant_id)
);

CREATE TRIGGER wallet_ledger_account_requires_wallet
BEFORE INSERT ON wallet_ledger_accounts
WHEN NOT EXISTS (
    SELECT 1 FROM mock_wallets WHERE participant_id = NEW.participant_id
)
BEGIN
    SELECT RAISE(ABORT, 'wallet_ledger_accounts requires an existing mock_wallets row');
END;

-- 4. 上記の Wallet 台帳口座は budget_accounts へも行を持つが、001 の
--    budget_accounts は root_id / owner_job_id の jobs への FK を要求する。
--    Participant は Job ではないため、Wallet 台帳口座専用の deferred FK 代替
--    トリガーで「通常口座は jobs 参照、Wallet 台帳口座は participants 参照」に
--    置き換える。INSERT だけでなく UPDATE も同じ規則で検査し、参照されている
--    jobs / participants 行の削除は BEFORE DELETE トリガーで拒否する
--    （001 の FK の ON DELETE 規則は変えない）。
--
--    安全性の根拠: この再作成は外部キーを有効なまま実行する。SQLite は
--    transaction 中の PRAGMA foreign_keys を無視し（公式仕様）、migrate() は
--    各 migration を BEGIN IMMEDIATE transaction 内で実行するため、FK を切る
--    ことはそもそも不可能である（以前あった PRAGMA foreign_keys = OFF/ON は
--    no-op であり「FK を切っている」ように見せないため削除した）。
--    外部キー有効のままの DROP TABLE budget_accounts が安全なのは、002 適用
--    時点では参照元テーブル（journal_entries / wallet_ledger_accounts /
--    payment_operations.source_account_id）がすべて空だからである。空でなければ
--    下の検査用テーブルと即時検証トリガーが ABORT で migration 全体を
--    ROLLBACK させ、中途半端な状態を残さない（SQLite の RAISE() はトリガー
--    内でのみ有効なため、検査は INSERT 時発火のトリガーで行う）。
CREATE TABLE budget_accounts_recreate_guard (id INTEGER PRIMARY KEY);

CREATE TRIGGER budget_accounts_must_be_empty_before_recreate
BEFORE INSERT ON budget_accounts_recreate_guard
WHEN EXISTS (SELECT 1 FROM journal_entries)
    OR EXISTS (SELECT 1 FROM wallet_ledger_accounts)
    OR EXISTS (
        SELECT 1 FROM payment_operations
        WHERE source_account_id IS NOT NULL
    )
BEGIN
    SELECT RAISE(
        ABORT,
        '002: budget_accounts referencing tables must be empty before recreation'
    );
END;

INSERT INTO budget_accounts_recreate_guard (id) VALUES (1);

DROP TRIGGER budget_accounts_must_be_empty_before_recreate;
DROP TABLE budget_accounts_recreate_guard;

CREATE TABLE budget_accounts_new (
    id             TEXT PRIMARY KEY,
    root_id        TEXT NOT NULL,
    owner_job_id   TEXT NOT NULL,
    bucket         TEXT NOT NULL CHECK (bucket IN (
                       'available', 'child_work', 'child_payout',
                       'parent_payout', 'refund', 'paid', 'refunded')),
    purpose        TEXT,
    amount_units   INTEGER NOT NULL CHECK (amount_units >= 0),
    beneficiary_id TEXT REFERENCES participants (id),
    source_key     TEXT,
    UNIQUE (root_id, owner_job_id, bucket, beneficiary_id, source_key)
);

INSERT INTO budget_accounts_new
    SELECT id, root_id, owner_job_id, bucket, purpose, amount_units,
           beneficiary_id, source_key FROM budget_accounts;

-- 検査済みならこの時点でも FK 違反は起きない（budget_accounts を参照する
-- 行は存在しない）。
DROP TABLE budget_accounts;
ALTER TABLE budget_accounts_new RENAME TO budget_accounts;

CREATE UNIQUE INDEX budget_accounts_natural_unique
    ON budget_accounts (
        root_id,
        owner_job_id,
        bucket,
        COALESCE(beneficiary_id, ''),
        COALESCE(source_key, '')
    );

-- 通常口座: root_id は jobs（Root）、owner_job_id は jobs。即時検証のトリガー。
-- INSERT だけでなく UPDATE も検査する（root_id / owner_job_id / source_key の
-- 書き換えで通常口座と Wallet 台帳口座の区別が壊れないようにする）。
CREATE TRIGGER budget_accounts_root_job_check
BEFORE INSERT ON budget_accounts
WHEN NEW.source_key IS NOT 'wallet-ledger'
   AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.id = NEW.root_id)
BEGIN
    SELECT RAISE(ABORT, 'budget_accounts.root_id must reference jobs');
END;

CREATE TRIGGER budget_accounts_root_job_check_update
BEFORE UPDATE ON budget_accounts
WHEN NEW.source_key IS NOT 'wallet-ledger'
   AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.id = NEW.root_id)
BEGIN
    SELECT RAISE(ABORT, 'budget_accounts.root_id must reference jobs');
END;

CREATE TRIGGER budget_accounts_owner_job_check
BEFORE INSERT ON budget_accounts
WHEN NEW.source_key IS NOT 'wallet-ledger'
   AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.id = NEW.owner_job_id)
BEGIN
    SELECT RAISE(ABORT, 'budget_accounts.owner_job_id must reference jobs');
END;

CREATE TRIGGER budget_accounts_owner_job_check_update
BEFORE UPDATE ON budget_accounts
WHEN NEW.source_key IS NOT 'wallet-ledger'
   AND NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.id = NEW.owner_job_id)
BEGIN
    SELECT RAISE(ABORT, 'budget_accounts.owner_job_id must reference jobs');
END;

-- Wallet 台帳口座: root_id = owner_job_id = participant_id で participants を参照。
CREATE TRIGGER budget_accounts_wallet_participant_check
BEFORE INSERT ON budget_accounts
WHEN NEW.source_key IS 'wallet-ledger'
   AND NOT EXISTS (
       SELECT 1 FROM participants
       WHERE participants.id = NEW.root_id AND participants.id = NEW.owner_job_id
   )
BEGIN
    SELECT RAISE(ABORT, 'wallet ledger account must reference participants');
END;

CREATE TRIGGER budget_accounts_wallet_participant_check_update
BEFORE UPDATE ON budget_accounts
WHEN NEW.source_key IS 'wallet-ledger'
   AND NOT EXISTS (
       SELECT 1 FROM participants
       WHERE participants.id = NEW.root_id AND participants.id = NEW.owner_job_id
   )
BEGIN
    SELECT RAISE(ABORT, 'wallet ledger account must reference participants');
END;

-- 参照されている jobs 行の削除を拒否する（FK 代替トリガーに ON DELETE の
-- 効力は無いため、budget_accounts の孤児化を BEFORE DELETE で塞ぐ）。
CREATE TRIGGER jobs_delete_blocked_by_budget_accounts
BEFORE DELETE ON jobs
WHEN EXISTS (
    SELECT 1 FROM budget_accounts
    WHERE budget_accounts.root_id = OLD.id
       OR budget_accounts.owner_job_id = OLD.id
)
BEGIN
    SELECT RAISE(ABORT, 'jobs row is referenced by budget_accounts');
END;

-- participants も同様（beneficiary_id の FK と Wallet 台帳口座の両方を守る）。
CREATE TRIGGER participants_delete_blocked_by_budget_accounts
BEFORE DELETE ON participants
WHEN EXISTS (
    SELECT 1 FROM budget_accounts
    WHERE budget_accounts.beneficiary_id = OLD.id
       OR (
           budget_accounts.source_key IS 'wallet-ledger'
           AND budget_accounts.root_id = OLD.id
       )
)
BEGIN
    SELECT RAISE(ABORT, 'participants row is referenced by budget_accounts');
END;
