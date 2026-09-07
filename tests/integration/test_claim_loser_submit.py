"""Phase 4 S3 の integration テスト: Phase 3 から繰り越した受入条件
（同時 Claim の敗者が service.submit の実経路で拒否されること）。

受入条件:
- 同時 Claim の敗者が**未成立 Lease ID** で submit → INVALID_TARGET
- 同時 Claim の敗者が**勝者の Lease ID** で submit → FORBIDDEN
- いずれの拒否後も submissions / submission_attempts / jobs / leases /
  operations / journal_transactions / journal_entries /
  payment_operations / 全口座残高が不変
- その後**勝者 Actor が同じ Lease から submit できる**

同時 Claim を実プロセス・barrier で起こす（既存
test_job_lifecycle.py / test_create_child_race.py の方法に合わせる。
共有メモリ Lock に頼らない。直列化は DB の BEGIN IMMEDIATE）。

状態は create_root → fund_root → claim（→ submit）の実経路だけで作る
（require_active_lease の直呼びで代替しない）。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

from ojp import db, ledger, service, verification
from ojp.domain import ErrorCode, JobState, OjpError
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日


def _prepare_db(handle):
    """固定 Actor + Requester Wallet seed を test mode DB に投入する。"""
    with db.transaction(handle.conn, immediate=True):
        insert_demo_participants(handle.conn)
        ledger.seed_mock_wallet_for_demo(
            handle.conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=ROOT_BUDGET_UNITS,
        )


def _create_open_root(handle, suffix):
    """create_root（第11節の固定カタログ・検証器つき）→ fund_root 済みの
    OPEN Root を返す（(job_id, version_id)）。submit が検証まで進めるよう
    公開 Version に検証器情報を保存する。"""
    root_def, catalog = load_poc_catalog()
    created = service.create_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        input_json=ledger.canonical_json_dumps(root_def.input_values),
        verifier_id=verification.VERIFIER_ID,
        verifier_hash=verification.verifier_hash(),
        conditions_json=ledger.canonical_json_dumps(root_def.expected),
        operation_id=f"create:{suffix}",
    )
    job_id = created.data["job_id"]
    service.fund_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        root_id=job_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:{suffix}",
    )
    return job_id, created.data["version_id"]


# 同時 Claim を起こすスクリプト（既存 test_job_lifecycle.py と同じ規約）
_CLAIM_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, job_id, version_id, actor_id, op_id, barrier_path, now_us) = (
        sys.argv[1:8]
    )
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier: 両プロセスが書き込むまで待つ。
    # 直列化は DB の BEGIN IMMEDIATE が担う（第16節）。
    with open(barrier_path, "a") as f:
        f.write(actor_id + "\\n")
    deadline = time.monotonic() + 10
    while True:
        with open(barrier_path) as f:
            if len(f.read().split()) >= 2:
                break
        if time.monotonic() > deadline:
            print("barrier timeout", flush=True)
            sys.exit(9)
        time.sleep(0.005)
    try:
        result = service.claim(
            conn,
            actor_id=actor_id,
            job_id=job_id,
            expected_version_id=version_id,
            operation_id=op_id,
        )
        print(f"ok lease={result.data['lease_id']}", flush=True)
    except OjpError as exc:
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    conn.close()
    """
)


def _full_state_snapshot(conn):
    """受入条件で不変を確認する全表のスナップショット。"""

    def _rows(sql):
        return tuple(tuple(r) for r in conn.execute(sql).fetchall())

    return (
        _rows("SELECT * FROM jobs ORDER BY id"),
        _rows("SELECT * FROM leases ORDER BY id"),
        _rows("SELECT * FROM submissions ORDER BY id"),
        _rows("SELECT * FROM submission_attempts ORDER BY id"),
        _rows("SELECT * FROM operations ORDER BY operation_id"),
        _rows("SELECT * FROM journal_transactions ORDER BY operation_id"),
        _rows("SELECT * FROM journal_entries ORDER BY operation_id, entry_no"),
        _rows("SELECT * FROM payment_operations ORDER BY operation_id"),
        _rows("SELECT * FROM budget_accounts ORDER BY id"),
    )


def test_claim_loser_submit_rejected_and_winner_can_submit(test_db, tmp_path):
    """同時 Claim の敗者（B）が service.submit の実経路で:

    1. 未成立 Lease ID（敗者の lease:claim:pt-agent-b）で submit →
       INVALID_TARGET（Lease 行が存在しない）
    2. 勝者の Lease ID で submit → FORBIDDEN（Lease の Worker は A）

    の両方で拒否される。いずれの拒否後も全テーブル（submissions /
    submission_attempts / jobs / leases / operations /
    journal_transactions / journal_entries / payment_operations /
    全口座残高）が不変であり、その後勝者 A が同じ Lease から
    submit できる。
    """
    _prepare_db(test_db)
    job_id, version_id = _create_open_root(test_db, "loser-submit")

    # 同時 Claim を実プロセス・barrier で起こす
    barrier = tmp_path / "claim-loser-submit-barrier.txt"
    barrier.write_text("")
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _CLAIM_SCRIPT,
                str(test_db.path),
                job_id,
                version_id,
                actor_id,
                f"claim:{actor_id}",
                str(barrier),
                str(TEST_T0_US),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for actor_id in (AGENT_A_ID, AGENT_B_ID)
    ]
    results = {}
    for proc, actor_id in zip(procs, (AGENT_A_ID, AGENT_B_ID)):
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, f"stdout: {out}\nstderr: {err}"
        results[actor_id] = out.strip()

    # 1 勝 1 敗
    if "error=CLAIM_CONFLICT" in results[AGENT_A_ID]:
        loser, winner = AGENT_A_ID, AGENT_B_ID
    else:
        assert "error=CLAIM_CONFLICT" in results[AGENT_B_ID], results
        loser, winner = AGENT_B_ID, AGENT_A_ID
    assert results[winner].startswith("ok "), results
    winner_lease_id = results[winner].split("lease=", 1)[1]

    fresh = test_db.fresh_conn()
    try:
        # 敗者には Lease 行が一切無い（未成立 Lease ID）
        loser_leases = fresh.execute(
            "SELECT COUNT(*) AS c FROM leases WHERE job_id = ? AND worker_id = ?",
            (job_id, loser),
        ).fetchone()["c"]
        assert loser_leases == 0
        # 勝者の Lease が 1 件だけ
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM leases WHERE job_id = ?", (job_id,)
            ).fetchone()["c"]
            == 1
        )
        assert (
            fresh.execute(
                "SELECT state FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()["state"]
            == JobState.LEASED.value
        )

        # (1) 敗者が未成立 Lease ID で submit → INVALID_TARGET
        loser_lease_id = f"lease:claim:{loser}"
        before = _full_state_snapshot(fresh)
        with pytest.raises(OjpError) as exc_info:
            service.submit(
                fresh,
                actor_id=loser,
                job_id=job_id,
                lease_id=loser_lease_id,
                version_id=version_id,
                artifact_json='{"sum": 15}',
                operation_id=f"submit:loser-unknown-{loser}",
            )
        assert exc_info.value.code == ErrorCode.INVALID_TARGET.value
        assert _full_state_snapshot(fresh) == before

        # (2) 敗者が勝者の Lease ID で submit → FORBIDDEN
        with pytest.raises(OjpError) as exc_info:
            service.submit(
                fresh,
                actor_id=loser,
                job_id=job_id,
                lease_id=winner_lease_id,
                version_id=version_id,
                artifact_json='{"sum": 15}',
                operation_id=f"submit:loser-winner-lease-{loser}",
            )
        assert exc_info.value.code == ErrorCode.FORBIDDEN.value
        assert _full_state_snapshot(fresh) == before

        # 敗者の拒否試行は Operation も残さない（権限・対象の検査が
        # apply_effects の先頭で弾かれ、transaction ごと rollback される）
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM operations"
                " WHERE operation_id LIKE ?",
                (f"submit:loser-%",),
            ).fetchone()["c"]
            == 0
        )

        # (3) その後勝者 Actor が同じ Lease から submit できる
        submitted = service.submit(
            fresh,
            actor_id=winner,
            job_id=job_id,
            lease_id=winner_lease_id,
            version_id=version_id,
            artifact_json='{"sum": 15}',
            operation_id=f"submit:winner-{winner}",
        )
        assert submitted.data["state"] == JobState.SUBMITTED.value
        assert submitted.data["verification"] == "PASS"
        root_id = fresh.execute(
            "SELECT root_id FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()["root_id"]
        ledger.assert_ledger_invariants(fresh, root_id)
        # submissions は勝者の 1 件だけ
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM submissions WHERE job_id = ?",
                (job_id,),
            ).fetchone()["c"]
            == 1
        )
    finally:
        fresh.close()
