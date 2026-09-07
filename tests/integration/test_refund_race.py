"""Phase 4 S3 の integration テスト: X08（同時 refund と Parent 終了 /
child return の競合）。

計画書 第16節「競合時の期待動作」:
- 同時refund: 同じ返金原資から1予約・1移転だけ
- Parent終了とChild成功/失敗: どちらの順でもChild原資を保護し、返金総額が一致

実ファイル SQLite ＋ 独立接続・独立プロセスを barrier で同時開始する
（既存 test_parent_terminal_race.py / test_create_child_race.py の方法に
合わせる。共有メモリ Lock に頼らない。直列化は DB の BEGIN IMMEDIATE）。

X08 の検証項目:
- 同時 refund（初回 90・追加 10 それぞれ）: 返金累計は 100 を超えない
- Parent 終了と child return の競合: どちらの順でも同じ最終結果
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

from ojp import clock, db, ledger, service, verification
from ojp.domain import JobState
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
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
    """固定 Actor + Worker/Requester Wallet seed を test mode DB に投入する。"""
    with db.transaction(handle.conn, immediate=True):
        insert_demo_participants(handle.conn)
        for participant_id, balance in (
            (REQUESTER_ID, ROOT_BUDGET_UNITS),
            (AGENT_A_ID, 0),
            (AGENT_B_ID, 0),
        ):
            ledger.seed_mock_wallet_for_demo(
                handle.conn,
                participant_id=participant_id,
                asset="mock-USDC",
                balance_units=balance,
            )


def _leased_child(handle, *, suffix):
    """create_root → fund → A Claim → create_child(part-1, 10) → B Claim まで
    済みの Child を返す
    （(root_id, child_id, child_lease_id, root_lease_id)）。"""
    _root_def, catalog = load_poc_catalog()
    created = service.create_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        title=f"root {suffix}",
        budget="100.000000",
        deadline_us=DEADLINE_US,
        subcontract_policy=default_subcontract_policy(),
        task_catalog=catalog,
        input_json=ledger.canonical_json_dumps(_root_def.input_values),
        verifier_id=verification.VERIFIER_ID,
        verifier_hash=verification.verifier_hash(),
        conditions_json=ledger.canonical_json_dumps(_root_def.expected),
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
    claimed_root = service.claim(
        handle.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-a",
    )
    root_lease = claimed_root.data["lease_id"]
    created_child = service.create_child(
        handle.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=root_lease,
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}-c1",
    )
    child_id = created_child.data["child_id"]
    claimed_child = service.claim(
        handle.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created_child.data["version_id"],
        operation_id=f"claim:{suffix}-c1-b",
    )
    return root_id, child_id, claimed_child.data["lease_id"], root_lease


def _wait_barrier_procs(procs):
    """barrier 付きサブプロセスの完了を待ち、(stdout 行群, stderr) を返す。"""
    outputs = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, f"stdout: {out}\nstderr: {err}"
        outputs.append(out.strip().splitlines())
    return outputs


# ---------------------------------------------------------------------------
# 同時 refund（初回 90・追加 10 それぞれ）を独立プロセスで同時開始
# ---------------------------------------------------------------------------

_REFUND_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, actor_id, job_id, op_id, barrier_path, now_us) = sys.argv[1:7]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier: 両プロセスが書き込むまで待つ。
    # 直列化は DB の BEGIN IMMEDIATE が担う（第16節）。
    with open(barrier_path, "a") as f:
        f.write("refund\\n")
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
        result = service.refund(
            conn, actor_id=actor_id, job_id=job_id, operation_id=op_id,
        )
        print(f"ok count={result.data['refund_count']}", flush=True)
    except OjpError as exc:
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    conn.close()
    """
)


def test_x08_concurrent_refund_never_exceeds_deposit(test_db, tmp_path):
    """X08（同時 refund）: Parent 終了（先行返金 90）と Child 失敗
    （追加返金 10）が確定した後、Root Requester と system が同時に
    refund を呼んでも:

    - 返金累計（Receipt の集計）は 100 を超えない（各予約は 1 移転だけ）
    - Receipt は予約ごとに 1 件（terminal 90・child-return 10 の計 2 件）
    - Wallet の Requester 残高は seed - 100 + 100 = seed に一致
    """
    _prepare_db(test_db)
    root_id, child_id, child_lease, root_lease = _leased_child(
        test_db, suffix="x08-ref"
    )
    # Parent 失敗 → 先行返金 90 / Child 失敗 → 追加返金 10（両方確定済み）
    service.abandon(
        test_db.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        lease_id=root_lease,
        operation_id="abandon:x08-ref-p",
    )
    service.abandon(
        test_db.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=child_lease,
        operation_id="abandon:x08-ref-c1",
    )

    barrier = tmp_path / "x08-refund-barrier.txt"
    barrier.write_text("")
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _REFUND_SCRIPT,
                str(test_db.path),
                actor_id,
                root_id,
                f"refund:x08-conc-{actor_id}",
                str(barrier),
                str(TEST_T0_US),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for actor_id in (REQUESTER_ID, SYSTEM_ID)
    ]
    outputs = _wait_barrier_procs(procs)
    # 両プロセスとも拒否されずに完了する（権限のある Actor 同士。同じ予約を
    # 処理するだけで、二重送金にはならない）
    assert all(any(l.startswith("ok ") for l in lines) for lines in outputs), outputs

    fresh = test_db.fresh_conn()
    try:
        view = ledger.get_root_ledger_view(fresh, root_id)
        # 返金累計は 100 を超えない（入金額と一致）
        assert view.refunded_units == ROOT_BUDGET_UNITS
        assert view.refunded_units <= view.deposit_units
        # Receipt は予約ごとに 1 件（計 2 件）
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM transfer_receipts r"
                " JOIN payment_operations p ON p.operation_id = r.operation_id"
                " WHERE p.root_id = ?",
                (root_id,),
            ).fetchone()["c"]
            == 2
        )
        # 予約も 2 件だけ（新しい返金予約は作られていない）
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM payment_operations"
                " WHERE root_id = ? AND kind = 'refund'",
                (root_id,),
            ).fetchone()["c"]
            == 2
        )
        # Wallet: seed - 100（入金）+ 100（返金） = seed
        wallet = fresh.execute(
            "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
            (REQUESTER_ID,),
        ).fetchone()
        assert int(wallet["balance_units"]) == ROOT_BUDGET_UNITS
        assert view.available_units == 0
        assert view.locked_units == 0
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# Parent 終了と child return の競合（どちらの順でも同じ最終結果）
# ---------------------------------------------------------------------------

_EXPIRE_PARENT_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, root_id, actor_id, barrier_path, now_us, new_now_us) = sys.argv[1:7]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
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
    try:
        results = service.expire_due_leases(conn, actor_id=actor_id)
        print(f"expired={[r.data['job_id'] for r in results]}", flush=True)
    except OjpError as exc:
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    conn.close()
    """
)

_ABANDON_CHILD_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, child_id, lease_id, actor_id, barrier_path, now_us, op_id) = (
        sys.argv[1:8]
    )
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    with open(barrier_path, "a") as f:
        f.write("abandon\\n")
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
        result = service.abandon(
            conn, actor_id=actor_id, job_id=child_id, lease_id=lease_id,
            operation_id=op_id,
        )
        print(f"ok state={result.data['state']}", flush=True)
    except OjpError as exc:
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    conn.close()
    """
)


def _concurrent_parent_terminal_and_child_return(test_db, tmp_path, *, suffix):
    """Parent 失効（t0+61秒への Clock 更新を伴う）と Child abandon を
    barrier で同時開始する。BEGIN IMMEDIATE の直列化でどちらかが先に
    確定し、どちらの順でも同じ最終結果（返金総額の一致）になる:

    - Child abandon が先: Child FAILED（Parent 生存中の child_work →
      available）→ Parent EXPIRED で available 100 が terminal 返金
      （1 件の予約で全額）
    - Parent 失効が先: Child の Lease も同じ期限で失効し Child EXPIRED
      （Parent 終端後の child_work → available → refund の追加返金）。
      abandon プロセスは INVALID_STATE（既に終端）で拒否される

    どちらの場合も返金総額（予約の合計）は 100 で一致する。
    """
    _prepare_db(test_db)
    root_id, child_id, child_lease, _root_lease = _leased_child(
        test_db, suffix=suffix
    )
    barrier = tmp_path / f"x08-{suffix}-barrier.txt"
    barrier.write_text("")
    expire_proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _EXPIRE_PARENT_SCRIPT,
            str(test_db.path),
            root_id,
            SYSTEM_ID,
            str(barrier),
            str(TEST_T0_US),
            str(TEST_T0_US + 61_000_000),  # Parent Lease（60秒）を越える
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    abandon_proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _ABANDON_CHILD_SCRIPT,
            str(test_db.path),
            child_id,
            child_lease,
            AGENT_B_ID,
            str(barrier),
            str(TEST_T0_US),
            f"abandon:{suffix}-c1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    outputs = _wait_barrier_procs([expire_proc, abandon_proc])
    del outputs

    fresh = test_db.fresh_conn()
    try:
        # どちらの順でも Parent は EXPIRED
        parent_state = fresh.execute(
            "SELECT state FROM jobs WHERE id = ?", (root_id,)
        ).fetchone()["state"]
        child_state = fresh.execute(
            "SELECT state FROM jobs WHERE id = ?", (child_id,)
        ).fetchone()["state"]
        assert parent_state == JobState.EXPIRED.value
        # Child は abandon 先着なら FAILED、Parent 失効先着なら EXPIRED。
        # どちらも判定上の終端であり Child の 10 は保護された
        assert child_state in (JobState.FAILED.value, JobState.EXPIRED.value)

        # 返金予約の合計は 100（どちらの順でも同じ返金総額）
        refund_ops = fresh.execute(
            "SELECT business_key, amount_units FROM payment_operations"
            " WHERE root_id = ? AND kind = 'refund' ORDER BY business_key",
            (root_id,),
        ).fetchall()
        total_refund_reserved = sum(int(r["amount_units"]) for r in refund_ops)
        assert total_refund_reserved == ROOT_BUDGET_UNITS
        if child_state == JobState.FAILED.value:
            # Child abandon が先: 1 件の terminal 予約（available 100）だけ
            assert len(refund_ops) == 1
            assert refund_ops[0]["business_key"] == f"refund:{root_id}:terminal"
            assert int(refund_ops[0]["amount_units"]) == ROOT_BUDGET_UNITS
        else:
            # Parent 失効が先: terminal 90 + child-return 10 の 2 件
            assert len(refund_ops) == 2
            by_key = {
                row["business_key"]: int(row["amount_units"]) for row in refund_ops
            }
            assert by_key == {
                f"refund:{root_id}:child-return:{child_id}": TEN,
                f"refund:{root_id}:terminal": ROOT_BUDGET_UNITS - TEN,
            }
        ledger.assert_ledger_invariants(fresh, root_id)
        return root_id
    finally:
        fresh.close()


def test_x08_concurrent_parent_terminal_and_child_return_order_a(
    test_db, tmp_path
):
    """X08（Parent 終了と child return の競合）: barrier 同時開始の結果は
    直列化順序に依らず同じ返金総額（100）に収束する。"""

    root_id = _concurrent_parent_terminal_and_child_return(
        test_db, tmp_path, suffix="race-a"
    )
    # 送金まで完了させる（fresh 接続で行う）
    conn = test_db.fresh_conn()
    try:
        service.process_payments(conn)
        view = ledger.get_root_ledger_view(conn, root_id)
        assert view.refunded_units == ROOT_BUDGET_UNITS  # 返金累計 = 入金額
        assert view.refunded_units <= view.deposit_units
        assert view.available_units == 0
        assert view.locked_units == 0
        wallet = conn.execute(
            "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
            (REQUESTER_ID,),
        ).fetchone()
        assert int(wallet["balance_units"]) == ROOT_BUDGET_UNITS
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()


def test_x08_concurrent_parent_terminal_and_child_return_order_b(
    test_db, tmp_path
):
    """X08 の再現性: 同じ競合をもう一度別 DB で実行しても同じ最終結果
    （返金総額 100）に収束する（順序非依存・決定的）。"""
    root_id = _concurrent_parent_terminal_and_child_return(
        test_db, tmp_path, suffix="race-b"
    )
    conn = test_db.fresh_conn()
    try:
        service.process_payments(conn)
        view = ledger.get_root_ledger_view(conn, root_id)
        assert view.refunded_units == ROOT_BUDGET_UNITS
        assert view.refunded_units <= view.deposit_units
        assert view.available_units == 0
        assert view.locked_units == 0
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()
