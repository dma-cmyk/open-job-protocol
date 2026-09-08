"""Phase 4 レビュー R2 の integration テスト: N05 の同時 approve 契約
（計画書 第9節・第16節「同時approve／自動approve: Acceptance と支払い予約が
1組だけ」・第18節 N05「Acceptance / PaymentOperation / Receipt はそれぞれ
1件。Bへ10だけ」）。

実ファイル SQLite ＋ 独立プロセスを barrier で同時開始する
（test_create_child_race.py / test_refund_race.py と同じ方法。共有メモリ
Lock に頼らない。直列化は DB の BEGIN IMMEDIATE と business_key の UNIQUE
が担う）:

(a) 同時 approve: 同じ Child の同じ Submission に対し、異なる operation_id
    で 2 つの独立プロセスが同時に approve する。一方が新規確定し、他方は
    既存結果を replay する（どちらもエラーにしない）。確定後に
    acceptances 1 件・payment_operations（business_key=payout:{child_id}）
    1 件であることを新しい接続から確認する
(b) 手動 approve と自動 approve（期限承認）の競合: 検収期限を越えた時刻で、
    独立プロセスの `python -m ojp.scheduler --root <path> --once --json
    --clock-mode test`（自動承認。barrier 同期のため、同じ entry point
    scheduler.main を同じ引数で呼ぶ wrapper として起動）と、Requester の
    手動 approve を barrier で同時に開始する。Acceptance と支払い予約が
    1 組だけで、acceptances.submission_id が当該 Submission であることを
    確認する
(c) 決済後の確定: (a) の後に送金処理（service.process_payments）を実行し、
    transfer_receipts が 1 件・payee_id == B・amount_units == 10_000_000・
    B の mock_wallets.balance_units が +10.000000 だけ増える・
    payment_operations.status == 'SUCCEEDED'・保存則
    D = available + locked + paid + refunded が成立することを確認する。
    送金を二度実行しても Receipt は 1 件のまま（二重送金していない）

Job 期限は実時間 sleep で待たない（固定 Clock を clock.set_test_now で
進める）。プロセス待機の timeout は OS の単調時計に依存する通常の
subprocess timeout を使う（計画書 第7節）。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

from ojp import clock, db, ledger, service, verification
from ojp.domain import JobState, PaymentStatus
from tests.conftest import (
    AGENT_A_ID,
    AGENT_B_ID,
    REQUESTER_ID,
    ROOT_BUDGET_UNITS,
    TEST_T0_US,
    SYSTEM_ID,
    default_subcontract_policy,
    insert_demo_participants,
    load_poc_catalog,
)

DEADLINE_US = TEST_T0_US + 86_400_000_000  # t0 + 1日
TEN = 10_000_000


def _prepare_db(handle):
    """固定 Actor + Wallet seed を test mode DB に投入する（Worker も 0）。"""
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


def _submitted_child(handle, *, suffix):
    """create_root → fund → A Claim → create_child(part-1, 10) → B Claim →
    B submit（有効提出）まで済みの Child を返す
    （(root_id, child_id, submission_id)）。実経路だけで作る。"""
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
        operation_id=f"create:{suffix}-r",
    )
    root_id = created.data["job_id"]
    service.fund_root(
        handle.conn,
        actor_id=REQUESTER_ID,
        root_id=root_id,
        requester_id=REQUESTER_ID,
        expected_amount_units=ROOT_BUDGET_UNITS,
        amount_units=ROOT_BUDGET_UNITS,
        operation_id=f"fund:{suffix}-r",
    )
    claimed_root = service.claim(
        handle.conn,
        actor_id=AGENT_A_ID,
        job_id=root_id,
        expected_version_id=created.data["version_id"],
        operation_id=f"claim:{suffix}-a",
    )
    created_child = service.create_child(
        handle.conn,
        actor_id=AGENT_A_ID,
        parent_job_id=root_id,
        lease_id=claimed_root.data["lease_id"],
        task_key="part-1",
        budget_units=TEN,
        deadline_us=DEADLINE_US,
        operation_id=f"create:{suffix}-c",
    )
    child_id = created_child.data["child_id"]
    claimed_child = service.claim(
        handle.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        expected_version_id=created_child.data["version_id"],
        operation_id=f"claim:{suffix}-b",
    )
    submitted = service.submit(
        handle.conn,
        actor_id=AGENT_B_ID,
        job_id=child_id,
        lease_id=claimed_child.data["lease_id"],
        version_id=created_child.data["version_id"],
        artifact_json='{"sum": 6}',
        operation_id=f"submit:{suffix}",
    )
    return root_id, child_id, submitted.data["submission_id"]


def _job(conn, job_id):
    return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def _wallet_units(conn, participant_id):
    return int(
        conn.execute(
            "SELECT balance_units FROM mock_wallets WHERE participant_id = ?",
            (participant_id,),
        ).fetchone()["balance_units"]
    )


def _wait_barrier_procs(procs):
    """barrier 付きサブプロセスの完了を待ち、stdout の行群を返す。"""
    outputs = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, f"stdout: {out}\nstderr: {err}"
        outputs.append(out.strip().splitlines())
    return outputs


# ---------------------------------------------------------------------------
# 独立プロセス script（実ファイル SQLite ＋ barrier で同時開始）
# ---------------------------------------------------------------------------

_APPROVE_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import clock, service
    from ojp.domain import ClockMode, OjpError

    (path, actor_id, job_id, submission_id, op_id, barrier_path, now_us) = (
        sys.argv[1:8]
    )
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))
    # 共有ファイルへの追記で barrier: 両プロセスが書き込むまで待つ。
    # 直列化は DB の BEGIN IMMEDIATE が担う（第16節）
    with open(barrier_path, "a") as f:
        f.write("approve\\n")
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
        result = service.approve(
            conn,
            actor_id=actor_id,
            job_id=job_id,
            submission_id=submission_id,
            operation_id=op_id,
        )
        print(f"ok replayed={result.replayed}", flush=True)
    except OjpError as exc:
        print(f"error={exc.code}", flush=True)
        sys.exit(0)
    conn.close()
    """
)

_TICK_SCRIPT = textwrap.dedent(
    """
    import sys, time
    from ojp import scheduler

    (root_dir, barrier_path) = sys.argv[1:3]
    # 手動 approve プロセスとの barrier（開始の同期のみ。直列化は DB の
    # BEGIN IMMEDIATE が担う）。barrier 通過後は独立プロセスの
    # `python -m ojp.scheduler --root <path> --once --json --clock-mode
    # test` と同じ entry point（scheduler.main）を同じ引数で呼ぶ
    with open(barrier_path, "a") as f:
        f.write("tick\\n")
    deadline = time.monotonic() + 10
    while True:
        with open(barrier_path) as f:
            if len(f.read().split()) >= 2:
                break
        if time.monotonic() > deadline:
            print("barrier timeout", flush=True)
            sys.exit(9)
        time.sleep(0.005)
    code = scheduler.main(
        ["--root", root_dir, "--once", "--json", "--clock-mode", "test"]
    )
    print(f"tick-exit={code}", flush=True)
    sys.exit(code)
    """
)


def _spawn_approve(db_path, child_id, submission_id, op_id, barrier):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            _APPROVE_SCRIPT,
            str(db_path),
            AGENT_A_ID,
            child_id,
            submission_id,
            op_id,
            str(barrier),
            str(TEST_T0_US),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _run_approve_race(test_db, tmp_path, *, suffix):
    """(a) のセットアップと race: 2 つの独立プロセスが異なる operation_id
    で同時に approve し、両者ともエラーなく完了（1 件が新規確定・1 件が
    既存結果の replay）したことを確認して
    (root_id, child_id, submission_id) を返す。"""
    _prepare_db(test_db)
    root_id, child_id, submission_id = _submitted_child(test_db, suffix=suffix)
    barrier = tmp_path / f"approve-race-barrier-{suffix}.txt"
    barrier.write_text("")
    procs = [
        _spawn_approve(
            test_db.path, child_id, submission_id, f"approve:{suffix}-{i}",
            barrier,
        )
        for i in (1, 2)
    ]
    outputs = _wait_barrier_procs(procs)
    ok = [line for lines in outputs for line in lines if line.startswith("ok ")]
    # どちらもエラーにしない（第16節）。1 件が新規確定（replayed=False）、
    # もう 1 件は既存結果の replay（replayed=True）
    assert len(ok) == 2, f"outputs: {outputs}"
    assert sorted(line.rsplit("replayed=", 1)[1] for line in ok) == [
        "False",
        "True",
    ], f"outputs: {outputs}"
    return root_id, child_id, submission_id


# ---------------------------------------------------------------------------
# (a) 同時 approve（異なる operation_id の 2 独立プロセス）
# ---------------------------------------------------------------------------


def test_concurrent_approve_single_acceptance_and_payment(test_db, tmp_path):
    """N05 (a): 同じ Child の同じ Submission への同時 approve（異なる
    operation_id・独立プロセス・barrier 同時開始）で、Acceptance と
    支払い予約が 1 組だけ。"""
    root_id, child_id, submission_id = _run_approve_race(
        test_db, tmp_path, suffix="a"
    )

    fresh = test_db.fresh_conn()
    try:
        assert _job(fresh, child_id)["state"] == JobState.DONE.value
        acceptances = fresh.execute(
            "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
        ).fetchall()
        assert len(acceptances) == 1
        assert acceptances[0]["submission_id"] == submission_id
        payments = fresh.execute(
            "SELECT * FROM payment_operations WHERE business_key = ?",
            (f"payout:{child_id}",),
        ).fetchall()
        assert len(payments) == 1
        # まだ決済していない段階なので予約は PENDING・Receipt は 0 件。
        # 決済後の Receipt 1 件は下の
        # test_settlement_after_concurrent_approve_single_receipt で確認する
        assert payments[0]["status"] == PaymentStatus.PENDING.value
        assert (
            fresh.execute(
                "SELECT COUNT(*) AS c FROM transfer_receipts"
            ).fetchone()["c"]
            == 0
        )
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# (b) 手動 approve と自動 approve（期限承認）の競合
# ---------------------------------------------------------------------------


def test_concurrent_manual_and_auto_approve_single_acceptance(
    test_db, tmp_path
):
    """N05 (b): 検収期限を越えた時刻で、独立プロセスの scheduler tick
    （自動承認）と Requester の手動 approve を barrier で同時に開始しても、
    Acceptance と支払い予約が 1 組だけ（第16節「同時approve／自動
    approve」）。acceptances.submission_id は当該 Submission を指す。"""
    _prepare_db(test_db)
    root_id, child_id, submission_id = _submitted_child(test_db, suffix="b")
    review_due_at = test_db.conn.execute(
        "SELECT review_due_at_us FROM submissions WHERE id = ?",
        (submission_id,),
    ).fetchone()["review_due_at_us"]
    # 検収期限を越えた時刻へ進める（時刻更新 transaction を Job 処理と
    # 混在させない。実時間 sleep ではなく固定 Clock を進める）
    clock.set_test_now(test_db.conn, review_due_at)

    # scheduler は <root>/data/ojp.sqlite3 を解決するため、接続を閉じて
    # WAL を checkpoint させてからプロジェクトルート風の配置へ複製する
    root_dir = tmp_path / "proj"
    (root_dir / "data").mkdir(parents=True)
    db_path = root_dir / "data" / "ojp.sqlite3"
    test_db.conn.close()
    db_path.write_bytes(test_db.path.read_bytes())

    barrier = tmp_path / "approve-auto-barrier.txt"
    barrier.write_text("")
    approve_proc = _spawn_approve(
        db_path, child_id, submission_id, "approve:b-manual", barrier
    )
    tick_proc = subprocess.Popen(
        [sys.executable, "-c", _TICK_SCRIPT, str(root_dir), str(barrier)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    outputs = _wait_barrier_procs([approve_proc, tick_proc])
    # 手動 approve は新規確定・既存結果の replay のどちらでもエラーに
    # しない。tick は終了コード 0（自動承認は冪等で、承認済みなら skip）
    assert any(line.startswith("ok ") for line in outputs[0]), outputs
    assert "tick-exit=0" in outputs[1], outputs

    fresh = db.connect(db_path)
    try:
        assert _job(fresh, child_id)["state"] == JobState.DONE.value
        acceptances = fresh.execute(
            "SELECT * FROM acceptances WHERE job_id = ?", (child_id,)
        ).fetchall()
        assert len(acceptances) == 1
        assert acceptances[0]["submission_id"] == submission_id
        payments = fresh.execute(
            "SELECT * FROM payment_operations WHERE business_key = ?",
            (f"payout:{child_id}",),
        ).fetchall()
        assert len(payments) == 1
        ledger.assert_ledger_invariants(fresh, root_id)
    finally:
        fresh.close()


# ---------------------------------------------------------------------------
# (c) 決済後の確定（Receipt 1 件・B への 10 支払い・二重送金なし）
# ---------------------------------------------------------------------------


def test_settlement_after_concurrent_approve_single_receipt(test_db, tmp_path):
    """N05 (c): 同時 approve の後の決済で、Receipt が 1 件・payee_id == B・
    amount_units == 10_000_000・B の Wallet が +10.000000 だけ増える・
    payment_operations.status == 'SUCCEEDED'・保存則
    D = available + locked + paid + refunded が成立する。
    送金を二度実行しても Receipt は 1 件のまま（二重送金していない）。"""
    root_id, child_id, _submission_id = _run_approve_race(
        test_db, tmp_path, suffix="c"
    )

    conn = test_db.fresh_conn()
    try:
        wallet_before = _wallet_units(conn, AGENT_B_ID)
        assert wallet_before == 0  # Worker は 0 で seed

        # 共通 settlement 処理（計画書 第9節「共通settlement処理が
        # PENDING/RETRYABLEを取得し、同じoperation_idでMock.transferを呼ぶ」）
        results = service.process_payments(conn, actor_id=SYSTEM_ID)
        assert len(results) == 1

        # Receipt は 1 件・受取人は B・金額は 10.000000
        receipts = conn.execute(
            "SELECT r.* FROM transfer_receipts r"
            " JOIN payment_operations p ON p.operation_id = r.operation_id"
            " WHERE p.job_id = ?",
            (child_id,),
        ).fetchall()
        assert len(receipts) == 1
        assert receipts[0]["payee_id"] == AGENT_B_ID
        assert int(receipts[0]["amount_units"]) == TEN
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM transfer_receipts"
            ).fetchone()["c"]
            == 1
        )

        # B の Wallet は +10.000000 だけ増える
        wallet_after = _wallet_units(conn, AGENT_B_ID)
        assert wallet_after - wallet_before == TEN

        payment = conn.execute(
            "SELECT * FROM payment_operations WHERE business_key = ?",
            (f"payout:{child_id}",),
        ).fetchone()
        assert payment["status"] == PaymentStatus.SUCCEEDED.value

        # D = available + locked + paid + refunded（ライフサイクル不変条件）
        view = ledger.get_root_ledger_view(conn, root_id)
        assert view.deposit_units == (
            view.available_units
            + view.locked_units
            + view.paid_units
            + view.refunded_units
        )
        assert view.paid_units == TEN
        ledger.assert_ledger_invariants(conn, root_id)

        # 送金を二度実行しても Receipt は 1 件のまま・Wallet も増えない
        # （Receipt が正本。第16節「Receipt作成後の再実行: 既存Receiptで
        # 照合し、WalletもJournalも増やさない」）
        again = service.process_payments(conn, actor_id=SYSTEM_ID)
        assert again == []
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM transfer_receipts"
            ).fetchone()["c"]
            == 1
        )
        assert _wallet_units(conn, AGENT_B_ID) == wallet_after
        ledger.assert_ledger_invariants(conn, root_id)
    finally:
        conn.close()
