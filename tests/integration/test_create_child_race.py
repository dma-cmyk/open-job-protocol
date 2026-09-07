"""Phase 3 S2 の integration テスト: 同時 Child 作成（計画書 第10節・第16節
「同時Child作成: commit済みの拘束・支払い・件数を読み直し、超過側を拒否」）。

Done when の対応:
- 1（同時 Child 作成: 独立プロセス・独立接続を barrier で同時開始し、
  commit 済みの拘束・支払い・件数を読み直して超過側が拒否される。
  共有メモリ Lock に頼らない。第18節 N07 の 3 fixture をそのまま実装する。
  いずれも拒否理由（POLICY_LIMIT の details.reason）と確定残高の両方を
  照合し、重複 task_key 拒否で代替しない）
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

from ojp import db, ledger, service
from ojp.domain import JobState, SubcontractPolicy
from tests.conftest import (
    AGENT_A_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    TEST_T0_US,
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


def _prepare_leased_root(handle, suffix, policy):
    """第11節の固定カタログで create_root → fund_root → A が Claim 済みの
    Root を返す（(root_id, parent_lease_id)）。policy は Root 公開前に設定
    する（N07: カタログは第11節のまま、policy だけを Root 公開前に設定）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=policy,
        task_catalog=catalog,
        operation_id=f"create:{suffix}",
    )
    root_id = created.data["job_id"]
    service.fund_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:{suffix}",
    )
    claimed = service.claim(
        handle.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}",
    )
    return root_id, claimed.data["lease_id"]


# ---------------------------------------------------------------------------
# Done when 1: 同時 Child 作成（独立プロセス・独立接続を barrier で同時開始）
# ---------------------------------------------------------------------------

_CREATE_CHILD_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, parent_job_id, lease_id, actor_id, task_key, budget_units,
     deadline_us, op_id, barrier_path, now_us) = sys.argv[1:11]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier: 両プロセスが書き込むまで待つ。
    # 共有メモリ Lock には頼らず、開始の同期だけをファイルで行う
    # （作成の直列化は DB の BEGIN IMMEDIATE と同一 transaction 内の
    #   上限判定が担う。第10節・第16節）。
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
        result = service.create_child(
            conn,
            actor_id=actor_id,
            parent_job_id=parent_job_id,
            lease_id=lease_id,
            task_key=task_key,
            budget_units=int(budget_units),
            deadline_us=int(deadline_us),
            operation_id=op_id,
        )
    except OjpError as exc:
        reason = (exc.details or {}).get("reason", "")
        print(f"error={exc.code} reason={reason}", flush=True)
        sys.exit(0)
    print(f"ok child={result.data['child_id']}", flush=True)
    conn.close()
    """
)


def _run_create_child_race(test_db, tmp_path, *, suffix, policy, budget_units):
    """part-1 / part-2 を異なる operation_id で同時作成する 2 プロセスを
    barrier で同時開始し、両者の出力を返す。"""
    root_id, lease_id = _prepare_leased_root(test_db, suffix, policy)
    barrier = tmp_path / f"child-race-barrier-{suffix}.txt"
    barrier.write_text("")
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _CREATE_CHILD_SCRIPT,
                str(test_db.path),
                root_id,
                lease_id,
                AGENT_A_ID,
                task_key,
                str(budget_units),
                str(DEADLINE_US),
                f"create:{suffix}-{task_key}",
                str(barrier),
                str(TEST_T0_US),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for task_key in ("part-1", "part-2")
    ]
    outputs = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, f"stdout: {out}\nstderr: {err}"
        outputs.append(out.strip())
    return root_id, outputs


def _assert_race_result(
    test_db,
    root_id,
    outputs,
    *,
    expected_reason,
    expected_u,
    expected_available,
    expected_count=1,
):
    """1 件だけ成立・敗者の拒否理由・確定残高を読み直して照合する。"""
    winners = [o for o in outputs if o.startswith("ok ")]
    losers = [o for o in outputs if o.startswith("error=")]
    assert len(winners) == 1, f"outputs: {outputs}"
    assert len(losers) == 1, f"outputs: {outputs}"
    # 拒否理由の照合（重複 task_key 拒否では代替しない）
    assert losers[0] == f"error=POLICY_LIMIT reason={expected_reason}", (
        f"outputs: {outputs}"
    )

    fresh = test_db.fresh_conn()
    try:
        # commit 済みの拘束・支払い・件数を読み直して照合する（第16節）
        usage = ledger.get_subcontract_usage(fresh, root_id)
        assert usage.in_use_units == expected_u
        assert usage.available_units == expected_available
        assert usage.child_count == expected_count
        children = fresh.execute(
            "SELECT task_key, state FROM jobs WHERE parent_id = ?", (root_id,)
        ).fetchall()
        assert len(children) == expected_count
        assert children[0]["task_key"] in ("part-1", "part-2")
        assert children[0]["state"] == JobState.OPEN.value
        # 敗者の試行は Job も operation も残さない（rollback）
        loser_task_key = (
            "part-2" if children[0]["task_key"] == "part-1" else "part-1"
        )
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM operations"
                " WHERE operation_id LIKE ?",
                (f"create:%-{loser_task_key}",),
            ).fetchone()["c"]
            == 0
        )
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()


def test_concurrent_create_child_max_amount(test_db, tmp_path):
    """N07 金額ケース: max_amount=30 / max_ratio_bps=10000 / max_children=3。
    part-1 と part-2 を各 20 で同時作成 → 1 件成立、敗者は
    POLICY_LIMIT/MAX_AMOUNT、U=20、available=80。"""
    _prepare_db(test_db)
    root_id, outputs = _run_create_child_race(
        test_db,
        tmp_path,
        suffix="race-amount",
        policy=SubcontractPolicy(
            enabled=True,
            max_amount_units=30_000_000,
            max_ratio_bps=10_000,
            max_children=3,
            max_depth=1,
        ),
        budget_units=20_000_000,
    )
    _assert_race_result(
        test_db,
        root_id,
        outputs,
        expected_reason="MAX_AMOUNT",
        expected_u=20_000_000,
        expected_available=80_000_000,
    )


def test_concurrent_create_child_max_ratio(test_db, tmp_path):
    """N07 比率ケース: max_amount=100 / max_ratio_bps=3000 / max_children=3。
    各 20 → 敗者は POLICY_LIMIT/MAX_RATIO、U=20、available=80。"""
    _prepare_db(test_db)
    root_id, outputs = _run_create_child_race(
        test_db,
        tmp_path,
        suffix="race-ratio",
        policy=SubcontractPolicy(
            enabled=True,
            max_amount_units=100_000_000,
            max_ratio_bps=3000,
            max_children=3,
            max_depth=1,
        ),
        budget_units=20_000_000,
    )
    _assert_race_result(
        test_db,
        root_id,
        outputs,
        expected_reason="MAX_RATIO",
        expected_u=20_000_000,
        expected_available=80_000_000,
    )


def test_concurrent_create_child_max_children(test_db, tmp_path):
    """N07 件数ケース: max_amount=100 / max_ratio_bps=10000 / max_children=1。
    各 10 → 敗者は POLICY_LIMIT/MAX_CHILDREN、作成数 1、U=10、available=90。"""
    _prepare_db(test_db)
    root_id, outputs = _run_create_child_race(
        test_db,
        tmp_path,
        suffix="race-children",
        policy=SubcontractPolicy(
            enabled=True,
            max_amount_units=100_000_000,
            max_ratio_bps=10_000,
            max_children=1,
            max_depth=1,
        ),
        budget_units=10_000_000,
    )
    _assert_race_result(
        test_db,
        root_id,
        outputs,
        expected_reason="MAX_CHILDREN",
        expected_u=10_000_000,
        expected_available=90_000_000,
        expected_count=1,
    )
