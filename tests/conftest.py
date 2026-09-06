"""テスト共通 helper: 固定カタログの読み込みと DB セットアップ。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from ojp import clock, db, ledger
from ojp.domain import (
    ClockMode,
    ParticipantKind,
    RootTaskDefinition,
    SubcontractPolicy,
    TaskCatalogEntry,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

TEST_T0_US = 1_800_000_000_000_000  # 2027-01-15T00:00:00Z 相当の固定時刻

# 台帳テスト用の固定 Actor（demo init 相当の最小構成）
REQUESTER_ID = "pt-requester"
AGENT_A_ID = "pt-agent-a"
AGENT_B_ID = "pt-agent-b"
SYSTEM_ID = "pt-system"

ROOT_BUDGET_UNITS = 100_000_000  # 100.000000 mock-USDC
CHILD_BUDGET_UNITS = 10_000_000  # 10.000000 mock-USDC


def load_poc_catalog() -> tuple[RootTaskDefinition, list[TaskCatalogEntry]]:
    data = json.loads((FIXTURES_DIR / "poc_catalog.json").read_text(encoding="utf-8"))
    root = RootTaskDefinition.model_validate(data["root"])
    catalog = [
        TaskCatalogEntry.model_validate(
            {
                "task_key": e["task_key"],
                "input_values": e["input_values"],
                "expected": e["expected"],
                "budget_cap_units": parse_budget_cap(e["budget_cap"]),
            }
        )
        for e in data["catalog"]
    ]
    return root, catalog


def parse_budget_cap(value: str) -> int:
    whole, frac = value.split(".")
    assert len(frac) == 6
    return int(whole) * 1_000_000 + int(frac)


def default_subcontract_policy() -> SubcontractPolicy:
    """デモ既定の Root policy（第10節の例）。"""
    return SubcontractPolicy(
        enabled=True,
        max_amount_units=30_000_000,
        max_ratio_bps=3000,
        max_children=3,
        max_depth=1,
    )


@dataclass
class DbHandle:
    path: Path
    conn: sqlite3.Connection

    def fresh_conn(self) -> sqlite3.Connection:
        return db.connect(self.path)


@pytest.fixture
def realtime_db(tmp_path: Path) -> DbHandle:
    path = tmp_path / "ojp-realtime.sqlite3"
    conn = clock.initialize_database(path, ClockMode.REALTIME)
    return DbHandle(path=path, conn=conn)


@pytest.fixture
def test_db(tmp_path: Path) -> DbHandle:
    path = tmp_path / "ojp-test.sqlite3"
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=TEST_T0_US)
    return DbHandle(path=path, conn=conn)


# ---------------------------------------------------------------------------
# Phase 2 台帳テスト用 helper（Job 状態機械は Phase 3 のため最小限の行作成のみ）
# ---------------------------------------------------------------------------


def insert_participant(
    conn: sqlite3.Connection, participant_id: str, kind: ParticipantKind
) -> None:
    conn.execute(
        "INSERT INTO participants (id, label, kind) VALUES (?, ?, ?)",
        (participant_id, participant_id, kind.value),
    )


def insert_root_job(
    conn: sqlite3.Connection,
    root_id: str,
    *,
    requester_id: str = REQUESTER_ID,
    budget_units: int = ROOT_BUDGET_UNITS,
) -> None:
    """台帳テスト用の最小 Root 行を作る（Job 状態機械・JobVersion は Phase 3）。

    budget_accounts の FK を満たすための行であり、`create_root` コマンドの
    実装ではない。requester_id と budget_units だけを台帳の引数に使う。
    """
    conn.execute(
        "INSERT INTO jobs (id, root_id, parent_id, requester_id, state, row_version,"
        " created_at_us) VALUES (?, ?, NULL, ?, 'DRAFT', 0, ?)",
        (root_id, root_id, requester_id, TEST_T0_US),
    )


def insert_child_job(
    conn: sqlite3.Connection,
    root_id: str,
    child_id: str,
    *,
    requester_id: str = AGENT_A_ID,
) -> None:
    """台帳テスト用の最小 Child 行を作る（Child 作成の policy 判定は Phase 3）。"""
    conn.execute(
        "INSERT INTO jobs (id, root_id, parent_id, requester_id, state, row_version,"
        " created_at_us, task_key) VALUES (?, ?, ?, ?, 'OPEN', 0, ?, 'part-1')",
        (child_id, root_id, root_id, requester_id, TEST_T0_US),
    )


def setup_ledger_demo_world(
    conn: sqlite3.Connection,
    root_id: str,
    *,
    requester_balance_units: int = ROOT_BUDGET_UNITS,
    seed_workers: bool = False,
) -> None:
    """participants + Root 行 + Requester Wallet seed を 1 transaction で準備する。

    seed は ledger.seed_mock_wallet_for_demo（デモ準備専用）を使い、
    Job 資金操作（fund_root）の経路とは分離されていることをテストでも確認する。
    seed_workers=True で Worker（A/B）の Wallet も 0 で seed する。S2 の送金
    （Mock.transfer）が受取人の Wallet を要求するため、送金を含むテストで使う。
    """
    with db.transaction(conn, immediate=True):
        insert_participant(conn, REQUESTER_ID, ParticipantKind.HUMAN)
        insert_participant(conn, AGENT_A_ID, ParticipantKind.AGENT)
        insert_participant(conn, AGENT_B_ID, ParticipantKind.AGENT)
        insert_participant(conn, SYSTEM_ID, ParticipantKind.SYSTEM)
        insert_root_job(conn, root_id)
        ledger.seed_mock_wallet_for_demo(
            conn,
            participant_id=REQUESTER_ID,
            asset="mock-USDC",
            balance_units=requester_balance_units,
        )
        if seed_workers:
            for worker_id in (AGENT_A_ID, AGENT_B_ID):
                ledger.seed_mock_wallet_for_demo(
                    conn,
                    participant_id=worker_id,
                    asset="mock-USDC",
                    balance_units=0,
                )
