-- OJP PoC migration 003: TransferReceipt に原資口座（source_account_id）を
-- NOT NULL 制約付きで追加する（計画書 第9節「Mock.transfer は既存 Receipt の
-- 金額・受取人・原資が予約と一致することを確認する」の照合対象を正本として
-- 保持するため）。001 の TransferReceipt 定義には原資列がなく、Receipt を
-- 正本とする照合が「金額・受取人」だけでは原資の食い違いを検出できない。
-- nullable な列追加では NULL の Receipt を作れてしまい domain.TransferReceipt
-- の型（source_account_id: str 必須）とも不一致になるため、テーブルを
-- 作り直して NOT NULL REFERENCES budget_accounts (id) にする。
--
-- 適用手順は「コピーを作ってから差し替える」順にする。先に旧テーブルを
-- DROP するとその瞬間に存在しない列へ新 INSERT が走って失敗するため、
-- 作り直しとデータコピー（001 由来の行には原資列が無いので、コピー前に
-- 必ず空であることを検査する）を済ませてから切り替える。
--
-- 安全性の根拠: 002 と同じく、外部キーを有効なまま実行し、「適用時点で
-- 対象テーブルが空であること」を検査用テーブルと即時検証トリガーで確認する
-- （この migration より前に送金機能は存在せず Receipt 行は無い前提）。
-- 空でなければ ABORT で migration 全体を ROLLBACK させ、中途半端な状態を
-- 残さない。payment_operations.receipt_id の FK を壊さないため、主キーと
-- receipt_id の値はそのままコピーする（検査済みなら 0 行のコピー）。
CREATE TABLE transfer_receipts_recreate_guard (id INTEGER PRIMARY KEY);

CREATE TRIGGER transfer_receipts_must_be_empty_before_recreate
BEFORE INSERT ON transfer_receipts_recreate_guard
WHEN EXISTS (SELECT 1 FROM transfer_receipts)
BEGIN
    SELECT RAISE(
        ABORT,
        '003: transfer_receipts must be empty before recreation'
    );
END;

INSERT INTO transfer_receipts_recreate_guard (id) VALUES (1);

DROP TRIGGER transfer_receipts_must_be_empty_before_recreate;
DROP TABLE transfer_receipts_recreate_guard;

CREATE TABLE transfer_receipts_new (
    receipt_id        TEXT PRIMARY KEY,
    operation_id      TEXT NOT NULL UNIQUE REFERENCES payment_operations (operation_id),
    amount_units      INTEGER NOT NULL CHECK (amount_units >= 0),
    payee_id          TEXT NOT NULL REFERENCES participants (id),
    asset             TEXT NOT NULL,
    source_account_id TEXT NOT NULL REFERENCES budget_accounts (id)
);

-- 検査済みなら 0 行のコピー（001 由来の行に source_account_id は無く、
-- 行があれば上のガードが既に ABORT している）。
INSERT INTO transfer_receipts_new
    SELECT receipt_id, operation_id, amount_units, payee_id, asset,
           NULL FROM transfer_receipts;

-- 検査済みならこの時点で transfer_receipts は空であり、
-- payment_operations.receipt_id が参照する行も存在しない。
DROP TABLE transfer_receipts;
ALTER TABLE transfer_receipts_new RENAME TO transfer_receipts;
