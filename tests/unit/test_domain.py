"""ドメイン型・policy・fixture カタログの Unit テスト。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ojp.domain import (
    Job,
    JobState,
    JobVersion,
    JournalEntry,
    Lease,
    PaymentOperation,
    SubcontractPolicy,
    TaskCatalogEntry,
    RootTaskDefinition,
    TERMINAL_JOB_STATES,
    TimingPolicy,
)
from tests.conftest import default_subcontract_policy, load_poc_catalog


class TestSubcontractPolicy:
    def test_default_policy(self) -> None:
        p = default_subcontract_policy()
        assert p.enabled is True
        assert p.max_amount_units == 30_000_000
        assert p.max_ratio_bps == 3000
        assert p.max_children == 3
        assert p.max_depth == 1

    def test_ratio_bounds(self) -> None:
        with pytest.raises(ValidationError):
            SubcontractPolicy(
                enabled=True, max_amount_units=1, max_ratio_bps=10_001, max_children=1, max_depth=1
            )
        SubcontractPolicy(
            enabled=True, max_amount_units=0, max_ratio_bps=10_000, max_children=0, max_depth=0
        )

    def test_depth_bounds(self) -> None:
        with pytest.raises(ValidationError):
            SubcontractPolicy(
                enabled=True, max_amount_units=1, max_ratio_bps=100, max_children=1, max_depth=2
            )

    def test_negative_amount_rejected(self) -> None:
        with pytest.raises(ValidationError):
            SubcontractPolicy(
                enabled=True, max_amount_units=-1, max_ratio_bps=100, max_children=1, max_depth=1
            )


class TestJobInvariants:
    def test_root_and_child(self) -> None:
        root = Job(
            id="j1", root_id="j1", parent_id=None, requester_id="p1",
            state=JobState.OPEN, row_version=0, created_at_us=1,
        )
        assert root.is_root() and not root.is_child()
        child = Job(
            id="j2", root_id="j1", parent_id="j1", requester_id="p1",
            state=JobState.OPEN, row_version=0, created_at_us=1, task_key="part-1",
        )
        assert child.is_child() and not child.is_root()

    def test_terminal_states(self) -> None:
        assert TERMINAL_JOB_STATES == frozenset(
            {JobState.DONE, JobState.FAILED, JobState.EXPIRED}
        )


class TestTaskCatalogEntry:
    def test_bool_not_accepted_as_int(self) -> None:
        with pytest.raises(ValidationError):
            TaskCatalogEntry(
                task_key="x", input_values=[True, 2], expected={"sum": 3}, budget_cap_units=1
            )

    def test_input_abs_limit(self) -> None:
        with pytest.raises(ValidationError):
            TaskCatalogEntry(
                task_key="x", input_values=[10**9 + 1], expected={"sum": 1}, budget_cap_units=1
            )

    def test_expected_must_be_int(self) -> None:
        with pytest.raises(ValidationError):
            TaskCatalogEntry(
                task_key="x", input_values=[1], expected={"sum": 1.5}, budget_cap_units=1
            )


class TestStrictAmountUnits:
    """金額型は True・float・文字列数値の曖昧な変換を拒否する（レビュー指摘4）。"""

    def _base_kwargs(self) -> dict:
        return dict(
            id="v1", job_id="j1", version=1, title="t", asset="mock-USDC",
            deadline_us=1, subcontract_policy=default_subcontract_policy(),
            timing_policy=TimingPolicy(),
        )

    def test_job_version_budget_units_strict(self) -> None:
        JobVersion(budget_units=1, **self._base_kwargs())
        for bad in (True, 1.0, "1"):
            with pytest.raises(ValidationError):
                JobVersion(budget_units=bad, **self._base_kwargs())

    def test_payment_operation_amount_units_strict(self) -> None:
        base = dict(
            operation_id="op1", business_key="bk", root_id="r1", job_id="j1",
            source_account_id="a1", payee_id="w1", kind="payout", status="PENDING",
        )
        PaymentOperation(amount_units=1, **base)
        for bad in (True, 1.0, "1"):
            with pytest.raises(ValidationError):
                PaymentOperation(amount_units=bad, **base)

    def test_journal_entry_delta_units_strict(self) -> None:
        JournalEntry(operation_id="op1", entry_no=0, account_id="a1", delta_units=-100)
        for bad in (True, 1.0, "1"):
            with pytest.raises(ValidationError):
                JournalEntry(operation_id="op1", entry_no=0, account_id="a1", delta_units=bad)

    def test_lease_generation_and_job_row_version_strict(self) -> None:
        base = dict(
            id="l1", job_id="j1", worker_id="w1", version_id="v1",
            claimed_at_us=1, heartbeat_at_us=1, expires_at_us=2,
        )
        Lease(generation=1, **base)
        for bad in (True, 1.0, "1"):
            with pytest.raises(ValidationError):
                Lease(generation=bad, **base)


class TestPocCatalogFixture:
    def test_catalog_matches_plan_section_11(self) -> None:
        root, catalog = load_poc_catalog()
        assert root.task_key == "sum-v1"
        assert root.input_values == [1, 2, 3, 4, 5]
        assert root.expected == {"sum": 15}

        by_key = {e.task_key: e for e in catalog}
        assert set(by_key) == {"part-1", "part-2", "part-3"}
        assert by_key["part-1"].input_values == [1, 2, 3]
        assert by_key["part-1"].expected == {"sum": 6}
        assert by_key["part-2"].input_values == [4]
        assert by_key["part-2"].expected == {"sum": 4}
        assert by_key["part-3"].input_values == [5]
        assert by_key["part-3"].expected == {"sum": 5}
        for entry in catalog:
            assert entry.budget_cap_units == 20_000_000
