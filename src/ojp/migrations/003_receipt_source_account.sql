-- OJP PoC migration 003: TransferReceipt に原資口座（source_account_id）を追加する
-- （計画書 第9節「Mock.transfer は既存 Receipt の金額・受取人・原資が予約と
--   一致することを確認する」の照合対象を正本として保存するため）。
-- 001 の TransferReceipt 定義には原資列がなく、Receipt を正本とする照合が
-- 「金額・受取人」だけでは原資の食い違いを検出できない。
-- 既存 DB には Receipt 行が無い前提（001/002 適用済み DB は送金機能未実装の
-- Phase 2 途中までしか使われない）だが、万が一行があれば NOT NULL 制約の
-- ALTER ADD COLUMN が失敗して migration 全体が ROLLBACK するため安全側に倒れる。
ALTER TABLE transfer_receipts
    ADD COLUMN source_account_id TEXT REFERENCES budget_accounts (id);
