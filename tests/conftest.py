"""テスト共通 helper: 固定カタログの読み込みと DB セットアップ。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from ojp import clock, db
from ojp.domain import ClockMode, RootTaskDefinition, SubcontractPolicy, TaskCatalogEntry

FIXTURES_DIR = Path(__file__).parent / "fixtures"

TEST_T0_US = 1_800_000_000_000_000  # 2027-01-15T00:00:00Z 相当の固定時刻


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
