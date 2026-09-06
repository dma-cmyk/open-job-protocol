-- OJP PoC migration 004: job_versions の公開後 immutable を DB レベルで強制する
-- （計画書 第5節「公開後immutable」）。001_initial.sql の runtime_clock_*
-- トリガーと同じく、UPDATE / DELETE を BEFORE トリガーの RAISE(ABORT) で
-- 拒否する。アプリ側は job_versions を UPDATE / DELETE する経路を持たないが、
-- 今後の実装ミスや迂回経路から公開版の改ざん・消失を防ぐため、DB 自体に
-- 拒否を置く。既存 migration ファイルは変更せず、新しい番号で追記する。

CREATE TRIGGER job_versions_no_update
BEFORE UPDATE ON job_versions
BEGIN
    SELECT RAISE(ABORT, 'job_versions rows are immutable after publication');
END;

CREATE TRIGGER job_versions_no_delete
BEFORE DELETE ON job_versions
BEGIN
    SELECT RAISE(ABORT, 'job_versions rows cannot be deleted');
END;
