"""Phase 3 S3 の integration テスト: Child 作成と Parent 失効の競合
（計画書 第16節「Child作成とParent失効/放棄: 先にChild拘束が確定なら保護、
Parent終了が先なら作成拒否」）。

Done when の対応:
- 6（独立プロセス・barrier で同時開始し、どちらの順でも
  「先に Child 拘束が確定したら保護（Child は OPEN・child_work 保持・
  先行返金は未拘束分だけ）／Parent 終了が先なら作成拒否（LEASE_EXPIRED）」
  になる。共有メモリ Lock に頼らない）
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

from ojp import db, ledger, service
from ojp.domain import JobState
from tests.conftest import (
    AGENT_A_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    SYSTEM_ID,
    TEST_T0_US,
    default_subcontract_policy,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日
TEN = 10_000_000


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


def _prepare_leased_root(handle, suffix):
    """第11節の固定カタログで create_root → fund_root → A が Claim 済みの
    Root を返す（(root_id, parent_lease_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
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
# Done when 6: Child 作成と Parent 失効の競合（独立プロセス・barrier）
# ---------------------------------------------------------------------------

_CREATE_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, parent_job_id, lease_id, actor_id, task_key, budget_units,
     deadline_us, op_id, barrier_path, now_us) = sys.argv[1:11]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier: 両プロセスが書き込むまで待つ。
    # 共有メモリ Lock には頼らず、開始の同期だけをファイルで行う
    # （直列化は DB の BEGIN IMMEDIATE が担う。第16節）。
    with open(barrier_path, "a") as f:
        f.write("create\\n")
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
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    print(f"ok child={result.data['child_id']}", flush=True)
    conn.close()
    """
)

_EXPIRE_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    path, new_now_us, actor_id, barrier_path, now_us = sys.argv[1:6]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier（上の create スクリプトと同じ規約）
    with open(barrier_path, "a") as f:
        f.write("expire\\n")
    deadline = time.monotonic() + 10
    while True:
        with open(barrier_path) as f:
            if len(f.read().split()) >= 2:
                break
        if time.monotonic() > deadline:
            print("barrier timeout", flush=True)
            sys.exit(9)
        time.sleep(0.005)
    clock.set_test_now(conn, int(new_now_us))
    results = service.expire_due_leases(conn, actor_id=actor_id)
    for r in results:
        print(f"expired job={r.data['job_id']}", flush=True)
    print(f"done count={len(results)}", flush=True)
    conn.close()
    """
)


def test_concurrent_create_child_and_parent_expiry(test_db, tmp_path):
    """Child 作成と Parent 失効を独立プロセス・barrier で同時開始する。

    どちらの順でも次のどちらか一方だけが成立する:

    - 先に Child 拘束が確定: Child は OPEN のまま保護され、child_work=10、
      先行返金は未拘束分 90 だけ（Child 拘束額は返金に含めない）
    - Parent 終了が先: create_child は拒否（LEASE_EXPIRED）、Child は作られず、
      返金は 100

    共有メモリ Lock に頼らない（直列化は DB の BEGIN IMMEDIATE）。
    """
    _prepare_db(test_db)
    root_id, lease_id = _prepare_leased_root(test_db, "term-race")
    barrier = tmp_path / "term-race-barrier.txt"
    barrier.write_text("")
    create_proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _CREATE_SCRIPT,
            str(test_db.path),
            root_id,
            lease_id,
            AGENT_A_ID,
            "part-1",
            str(TEN),
            str(DEADLINE_US),
            "create:term-race-c1",
            str(barrier),
            str(TEST_T0_US),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    expire_proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _EXPIRE_SCRIPT,
            str(test_db.path),
            str(TEST_T0_US + 61_000_000),  # Parent Lease（60秒）を越える
            SYSTEM_ID,
            str(barrier),
            str(TEST_T0_US),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    create_out, create_err = create_proc.communicate(timeout=60)
    expire_out, expire_err = expire_proc.communicate(timeout=60)
    assert create_proc.returncode == 0, f"stdout: {create_out}\nstderr: {create_err}"
    assert expire_proc.returncode == 0, f"stdout: {expire_out}\nstderr: {expire_err}"
    create_lines = create_out.strip().splitlines()
    expire_lines = expire_out.strip().splitlines()

    create_won = any(line.startswith("ok ") for line in create_lines)
    create_rejected = any("error=LEASE_EXPIRED" in line for line in create_lines)
    # どちらか一方だけが成立する
    assert create_won != create_rejected, (
        f"create: {create_lines}, expire: {expire_lines}"
    )

    fresh = test_db.fresh_conn()
    try:
        # どちらの順でも Parent は EXPIRED
        parent = fresh.execute(
            "SELECT state FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()
        assert parent["state"] == JobState.EXPIRED.value, (
            f"create: {create_lines}, expire: {expire_lines}"
        )
        assert any(f"expired job={root_id}" in line for line in expire_lines), (
            f"expire: {expire_lines}"
        )
        view = ledger.get_root_ledger_view(fresh, root_id)
        children = fresh.execute(
            "SELECT * FROM jobs WHERE parent_id = ?", (root_id,)
        ).fetchall()
        if create_won:
            # 先に Child 拘束が確定 → Child は保護（OPEN・child_work=10）、
            # 先行返金は未拘束分 90 だけ
            assert len(children) == 1
            assert children[0]["state"] == JobState.OPEN.value
            assert view.locked_breakdown_units["child_work"] == TEN
            assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS - TEN
        else:
            # Parent 終了が先 → 作成拒否（Child は作られない）、返金は 100
            assert len(children) == 0
            assert view.locked_breakdown_units["child_work"] == 0
            assert view.locked_breakdown_units["refund"] == ROOT_BUDGET_UNITS
            # 拒否された試行は Job も operation も残さない
            assert (
                fresh.execute(
                    "SELECT COUNT(*) AS c FROM operations"
                    " WHERE operation_id = 'create:term-race-c1'"
                ).fetchone()["c"]
                == 0
            )
        assert view.available_units == 0
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()
