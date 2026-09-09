"""E2E テストレポート出力機構（計画書 第17節）。

第17節「テストレポートに scenario_id、operation_id、終端 Job 状態、受取人別金額、
locked 内訳、保存則の結果を残す」を満たす機械可読な JSON を書き出す。

出力先は `data/work/e2e-reports/<scenario_id>.json`（計画書 第4節。`data/` は
`.gitignore` 済みなので生成物はコミットされない）。テストが失敗した場合でも、
その時点までに蓄積した観測結果を書き出す。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# tests/e2e/report.py -> tests/e2e -> tests -> <project root>
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = PROJECT_ROOT / "data" / "work" / "e2e-reports"


@dataclass(frozen=True)
class OperationRecord:
    """1 回の書込操作（CLI / MCP / tick）の記録。"""

    step: int
    channel: str
    actor: str
    action: str
    operation_id: str | None
    ok: bool

    def to_json(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "channel": self.channel,
            "actor": self.actor,
            "action": self.action,
            "operation_id": self.operation_id,
            "ok": self.ok,
        }


@dataclass(frozen=True)
class ObservationRecord:
    """1 観測点での保存則・非負・受取権者一致・親子二重計上なしの結果。"""

    label: str
    totals: dict[str, str]
    locked_breakdown: dict[str, str]
    conservation_ok: bool
    accounts_non_negative: bool
    negative_accounts: list[str]
    payee_entitlement_ok: bool
    payee_entitlement_violations: list[str]
    job_states: dict[str, str]
    paid_by_payee: dict[str, str]
    refunded_by_payee: dict[str, str]
    no_double_counting_ok: bool
    double_counting_violations: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "totals": self.totals,
            "locked_breakdown": self.locked_breakdown,
            "conservation_ok": self.conservation_ok,
            "accounts_non_negative": self.accounts_non_negative,
            "negative_accounts": self.negative_accounts,
            "payee_entitlement_ok": self.payee_entitlement_ok,
            "payee_entitlement_violations": self.payee_entitlement_violations,
            "job_states": self.job_states,
            "paid_by_payee": self.paid_by_payee,
            "refunded_by_payee": self.refunded_by_payee,
            "no_double_counting_ok": self.no_double_counting_ok,
            "double_counting_violations": self.double_counting_violations,
        }


@dataclass
class ScenarioReport:
    """1 シナリオ分のレポート。シナリオ固有の期待値は持たない。"""

    scenario_id: str
    reports_dir: Path = REPORTS_DIR
    operations: list[OperationRecord] = field(default_factory=list)
    observations: list[ObservationRecord] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add_operation(
        self,
        *,
        channel: str,
        actor: str,
        action: str,
        operation_id: str | None,
        ok: bool,
    ) -> None:
        self.operations.append(
            OperationRecord(
                step=len(self.operations) + 1,
                channel=channel,
                actor=actor,
                action=action,
                operation_id=operation_id,
                ok=ok,
            )
        )

    def add_observation(self, record: ObservationRecord) -> None:
        self.observations.append(record)

    def note(self, text: str) -> None:
        self.notes.append(text)

    # ------------------------------------------------------------------
    # 出力
    # ------------------------------------------------------------------

    def to_json(self) -> dict[str, Any]:
        final = self.observations[-1] if self.observations else None
        return {
            "scenario_id": self.scenario_id,
            "operation_ids": [
                op.operation_id for op in self.operations if op.operation_id is not None
            ],
            "operations": [op.to_json() for op in self.operations],
            "terminal_job_states": dict(final.job_states) if final else {},
            "amounts_by_payee": {
                "paid": dict(final.paid_by_payee) if final else {},
                "refunded": dict(final.refunded_by_payee) if final else {},
            },
            "locked_breakdown": dict(final.locked_breakdown) if final else {},
            "final_totals": dict(final.totals) if final else {},
            "conservation": [obs.to_json() for obs in self.observations],
            "conservation_ok": all(
                obs.conservation_ok
                and obs.accounts_non_negative
                and obs.payee_entitlement_ok
                and obs.no_double_counting_ok
                for obs in self.observations
            )
            if self.observations
            else False,
            "notes": list(self.notes),
        }

    def write(self) -> Path:
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        path = self.reports_dir / f"{self.scenario_id}.json"
        path.write_text(
            json.dumps(self.to_json(), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        return path


def assert_report_contents(
    path: Path,
    *,
    scenario_id: str,
    terminal_job_states: dict[str, str],
    paid: dict[str, str],
    refunded: dict[str, str],
    locked_breakdown: dict[str, str],
) -> dict[str, Any]:
    """書き出したレポートが第17節の 6 項目を期待どおり含むことを検証する。

    期待値は呼出側（シナリオ）が渡す。レポート機構自体はシナリオ固有の
    期待値を持たない。
    """
    data = json.loads(path.read_text(encoding="utf-8"))

    assert data["scenario_id"] == scenario_id
    assert data["operation_ids"], "operation_id が 1 件も記録されていない"
    assert data["terminal_job_states"] == terminal_job_states
    assert data["amounts_by_payee"]["paid"] == paid
    assert data["amounts_by_payee"]["refunded"] == refunded
    assert data["locked_breakdown"] == locked_breakdown
    assert data["conservation_ok"] is True
    assert all(
        obs["conservation_ok"]
        and obs["accounts_non_negative"]
        and obs["payee_entitlement_ok"]
        and obs["no_double_counting_ok"]
        for obs in data["conservation"]
    )
    return data
