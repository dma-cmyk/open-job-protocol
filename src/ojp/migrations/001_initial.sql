-- OJP PoC migration 001: 初期スキーマ（計画書 第5節）。
-- 全参照に外部キー、金額に非負制約。RuntimeClock 行を DB 当たり1行含める。

PRAGMA foreign_keys = ON;

CREATE TABLE participants (
    id          TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    kind        TEXT NOT NULL CHECK (kind IN ('human', 'agent', 'system'))
);

CREATE TABLE jobs (
    id               TEXT PRIMARY KEY,
    root_id          TEXT NOT NULL REFERENCES jobs (id),
    parent_id        TEXT REFERENCES jobs (id),
    requester_id     TEXT NOT NULL REFERENCES participants (id),
    state            TEXT NOT NULL CHECK (state IN (
                         'DRAFT', 'OPEN', 'LEASED', 'SUBMITTED',
                         'DISPUTED', 'DONE', 'FAILED', 'EXPIRED')),
    version_id       TEXT REFERENCES job_versions (id),
    active_lease_id  TEXT REFERENCES leases (id),
    row_version      INTEGER NOT NULL CHECK (row_version >= 0),
    created_at_us    INTEGER NOT NULL CHECK (created_at_us >= 0),
    task_key         TEXT,
    creator_lease_id TEXT REFERENCES leases (id),
    CHECK ((parent_id IS NULL) = (root_id = id)),
    CHECK (parent_id IS NULL OR parent_id = root_id)
);

CREATE TABLE job_versions (
    id                    TEXT PRIMARY KEY,
    job_id                TEXT NOT NULL REFERENCES jobs (id),
    version               INTEGER NOT NULL CHECK (version >= 1),
    title                 TEXT NOT NULL,
    budget_units          INTEGER NOT NULL CHECK (budget_units > 0),
    asset                 TEXT NOT NULL,
    input_json            TEXT,
    verifier_id           TEXT,
    verifier_hash         TEXT,
    conditions_json       TEXT,
    conditions_hash       TEXT,
    subcontract_policy    TEXT NOT NULL,
    task_catalog          TEXT NOT NULL,
    timing_policy         TEXT NOT NULL,
    artifact_access_policy TEXT,
    deadline_us           INTEGER NOT NULL CHECK (deadline_us >= 0),
    UNIQUE (job_id, version)
);

CREATE TABLE leases (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL REFERENCES jobs (id),
    worker_id       TEXT NOT NULL REFERENCES participants (id),
    version_id      TEXT NOT NULL REFERENCES job_versions (id),
    generation      INTEGER NOT NULL CHECK (generation >= 1),
    claimed_at_us   INTEGER NOT NULL CHECK (claimed_at_us >= 0),
    heartbeat_at_us INTEGER NOT NULL CHECK (heartbeat_at_us >= 0),
    expires_at_us   INTEGER NOT NULL CHECK (expires_at_us >= 0),
    closed_reason   TEXT CHECK (closed_reason IN ('submitted', 'abandoned', 'expired')),
    UNIQUE (job_id, generation)
);
CREATE UNIQUE INDEX leases_one_active_per_job
    ON leases (job_id) WHERE closed_reason IS NULL;

CREATE TABLE submissions (
    id                    TEXT PRIMARY KEY,
    job_id                TEXT NOT NULL REFERENCES jobs (id),
    lease_id              TEXT NOT NULL REFERENCES leases (id),
    version_id            TEXT NOT NULL REFERENCES job_versions (id),
    artifact_json         TEXT NOT NULL,
    artifact_hash         TEXT NOT NULL,
    verification_result   TEXT NOT NULL CHECK (verification_result IN ('PASS', 'FAIL')),
    verification_evidence TEXT NOT NULL,
    submitted_at_us       INTEGER NOT NULL CHECK (submitted_at_us >= 0),
    valid_at_us           INTEGER NOT NULL CHECK (valid_at_us >= 0),
    review_due_at_us      INTEGER NOT NULL CHECK (review_due_at_us >= 0),
    CHECK (verification_result = 'PASS')
);
CREATE UNIQUE INDEX submissions_one_valid_per_job ON submissions (job_id);

CREATE TABLE submission_attempts (
    id             TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL REFERENCES jobs (id),
    lease_id       TEXT NOT NULL REFERENCES leases (id),
    input_hash     TEXT NOT NULL,
    outcome        TEXT NOT NULL CHECK (outcome IN ('PASS', 'FAIL')),
    reason         TEXT NOT NULL,
    attempted_at_us INTEGER NOT NULL CHECK (attempted_at_us >= 0)
);

CREATE TABLE budget_accounts (
    id             TEXT PRIMARY KEY,
    root_id        TEXT NOT NULL REFERENCES jobs (id),
    owner_job_id   TEXT NOT NULL REFERENCES jobs (id),
    bucket         TEXT NOT NULL CHECK (bucket IN (
                       'available', 'child_work', 'child_payout',
                       'parent_payout', 'refund', 'paid', 'refunded')),
    purpose        TEXT,
    amount_units   INTEGER NOT NULL CHECK (amount_units >= 0),
    beneficiary_id TEXT REFERENCES participants (id),
    source_key     TEXT,
    UNIQUE (root_id, owner_job_id, bucket, beneficiary_id, source_key)
);

CREATE TABLE journal_transactions (
    operation_id TEXT PRIMARY KEY,
    reason       TEXT NOT NULL,
    created_at_us INTEGER NOT NULL CHECK (created_at_us >= 0)
);

CREATE TABLE journal_entries (
    operation_id TEXT NOT NULL REFERENCES journal_transactions (operation_id),
    entry_no     INTEGER NOT NULL CHECK (entry_no >= 0),
    account_id   TEXT NOT NULL REFERENCES budget_accounts (id),
    delta_units  INTEGER NOT NULL CHECK (delta_units <> 0),
    PRIMARY KEY (operation_id, entry_no)
);

CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    actor_id     TEXT NOT NULL REFERENCES participants (id),
    kind         TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    business_key TEXT,
    status       TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED', 'REJECTED')),
    result       TEXT,
    created_at_us INTEGER NOT NULL CHECK (created_at_us >= 0)
);

CREATE TABLE payment_operations (
    operation_id      TEXT PRIMARY KEY REFERENCES operations (operation_id),
    business_key      TEXT NOT NULL UNIQUE,
    root_id           TEXT NOT NULL REFERENCES jobs (id),
    job_id            TEXT NOT NULL REFERENCES jobs (id),
    source_account_id TEXT NOT NULL REFERENCES budget_accounts (id),
    amount_units      INTEGER NOT NULL CHECK (amount_units > 0),
    payee_id          TEXT NOT NULL REFERENCES participants (id),
    kind              TEXT NOT NULL CHECK (kind IN ('payout', 'refund')),
    status            TEXT NOT NULL CHECK (status IN ('PENDING', 'RETRYABLE', 'SUCCEEDED')),
    attempt_count     INTEGER NOT NULL CHECK (attempt_count >= 0) DEFAULT 0,
    next_retry_at_us  INTEGER CHECK (next_retry_at_us >= 0),
    last_error        TEXT,
    receipt_id        TEXT UNIQUE REFERENCES transfer_receipts (receipt_id)
);

CREATE TABLE mock_wallets (
    participant_id TEXT PRIMARY KEY REFERENCES participants (id),
    asset          TEXT NOT NULL,
    balance_units  INTEGER NOT NULL CHECK (balance_units >= 0)
);

CREATE TABLE transfer_receipts (
    receipt_id   TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE REFERENCES payment_operations (operation_id),
    amount_units INTEGER NOT NULL CHECK (amount_units >= 0),
    payee_id     TEXT NOT NULL REFERENCES participants (id),
    asset        TEXT NOT NULL
);

CREATE TABLE acceptances (
    job_id        TEXT PRIMARY KEY REFERENCES jobs (id),
    submission_id TEXT NOT NULL UNIQUE REFERENCES submissions (id),
    decision      TEXT NOT NULL CHECK (decision IN ('APPROVED', 'REJECTED')),
    decided_by    TEXT NOT NULL REFERENCES participants (id),
    reason        TEXT,
    decided_at_us INTEGER NOT NULL CHECK (decided_at_us >= 0)
);

CREATE TABLE disputes (
    id            TEXT PRIMARY KEY,
    job_id        TEXT NOT NULL REFERENCES jobs (id),
    submission_id TEXT NOT NULL UNIQUE REFERENCES submissions (id),
    opened_by     TEXT NOT NULL REFERENCES participants (id),
    reason_code   TEXT NOT NULL,
    condition_id  TEXT NOT NULL,
    evidence      TEXT,
    opened_at_us  INTEGER NOT NULL CHECK (opened_at_us >= 0),
    due_at_us     INTEGER NOT NULL CHECK (due_at_us >= 0),
    status        TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    resolution    TEXT,
    CHECK (status <> 'RESOLVED' OR resolution IS NOT NULL)
);

CREATE TABLE events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id   TEXT NOT NULL REFERENCES jobs (id),
    job_id    TEXT NOT NULL REFERENCES jobs (id),
    actor_id  TEXT NOT NULL REFERENCES participants (id),
    action    TEXT NOT NULL,
    object_id TEXT,
    at_us     INTEGER NOT NULL CHECK (at_us >= 0)
);

-- DB 当たり1行。行の作成は initialize_database が入力検証後に単一 transaction で
-- 行うため、migration には INSERT を含めない（mode・時刻の初期確定が原子的になる）。
CREATE TABLE runtime_clock (
    singleton_id     INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    mode             TEXT NOT NULL CHECK (mode IN ('realtime', 'test')),
    test_now_utc_us  INTEGER CHECK (test_now_utc_us >= 0),
    CHECK (mode <> 'test' OR test_now_utc_us IS NOT NULL),
    CHECK (mode <> 'realtime' OR test_now_utc_us IS NULL)
);

-- mode は行作成時に一度だけ確定し、以降の UPDATE では一切変更不可
-- （realtime → test への変更も含む。通常 DB から test 時刻へ切り替える入口は
--   計画第7節で「設けない」と定義されている）。
CREATE TRIGGER runtime_clock_mode_immutable
BEFORE UPDATE ON runtime_clock
WHEN NEW.mode <> OLD.mode
BEGIN
    SELECT RAISE(ABORT, 'runtime_clock mode is immutable');
END;

-- singleton_id の書き換えで行を差し替えることも許さない。
CREATE TRIGGER runtime_clock_row_immutable
BEFORE UPDATE ON runtime_clock
WHEN NEW.singleton_id <> OLD.singleton_id
BEGIN
    SELECT RAISE(ABORT, 'runtime_clock singleton_id is immutable');
END;

-- 行の削除も許さない。
CREATE TRIGGER runtime_clock_no_delete
BEFORE DELETE ON runtime_clock
BEGIN
    SELECT RAISE(ABORT, 'runtime_clock row cannot be deleted');
END;

-- realtime DB に固定時刻を保存しない。
CREATE TRIGGER runtime_clock_test_now_requires_test_mode
BEFORE UPDATE ON runtime_clock
WHEN NEW.mode = 'realtime' AND NEW.test_now_utc_us IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'test_now_utc_us requires test mode');
END;
