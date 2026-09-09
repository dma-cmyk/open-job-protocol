"""障害注入 seam を発火させる独立プロセス（計画書 第17節「障害注入は決済
commit 前、commit 後応答消失、Receipt 確定後アプリの status 更新前」）。

seam（`verification.failpoint_before_submission_commit` /
`ledger.failpoint_before_commit` / `failpoint_after_commit` /
`failpoint_after_receipt`）は「テストが属性へ関数を代入する」方式のモジュール
属性で、test mode の DB でのみ発火する（第14節）。CLI / MCP には障害注入の
操作面が無く、追加もしない。そのため注入が必要な観測点だけは、同じ実ファイル
DB へ独立プロセスから接続して seam を立てた上で該当 Application API を呼ぶ。

いずれのスクリプトも:

- `clock.initialize_database(..., ClockMode.TEST, ...)` で**共有 Clock を
  進めずに**既存 DB へ接続する（`test_now_us` は現在値をそのまま渡す）
- 標準出力へ 1 行の JSON を出し、呼出側が結果を構造として読む
- 異常終了は exit code 3（`crash`）で表す
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from typing import Any

from .harness import PROCESS_TIMEOUT_SECONDS, E2EWorld, HarnessError

# submit の保存フェーズ（submissions と Job 更新の書込直後・commit 直前）で
# 例外を投げ、transaction 全体を rollback させる（第11節「取得不能となる
# 保存失敗」）。service.submit はこれを VERIFICATION_UNAVAILABLE へ正規化する。
_SUBMIT_STORAGE_FAILURE_SCRIPT = textwrap.dedent(
    """
    import json, sys
    from ojp import clock, service, verification
    from ojp.domain import ClockMode, OjpError

    path, actor_id, job_id, lease_id, version_id, artifact_json, op_id, now_us = (
        sys.argv[1:9]
    )
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))

    def boom(name):
        raise RuntimeError(f"injected storage failure at {name}")

    verification.failpoint_before_submission_commit = boom
    try:
        result = service.submit(
            conn,
            actor_id=actor_id,
            job_id=job_id,
            lease_id=lease_id,
            version_id=version_id,
            artifact_json=artifact_json,
            operation_id=op_id,
        )
    except OjpError as exc:
        print(json.dumps({"ok": False, "code": exc.code}), flush=True)
        sys.exit(0)
    print(json.dumps({"ok": True, "data": result.data}), flush=True)
    conn.close()
    """
)

# 裁定応答なしを再現する tick（第12節「裁定応答なしも既定 PASS で解消」）。
# `verification.failpoint_arbiter_unresponsive` を立てた独立プロセスで
# `scheduler.tick_once` を回すため、期限前は何も書かず、期限到来後は保存済み
# PASS の fallback が適用される。tick の集計 JSON をそのまま出力する。
_UNRESPONSIVE_ARBITER_TICK_SCRIPT = textwrap.dedent(
    """
    import json, sys
    from ojp import clock, scheduler, verification
    from ojp.domain import ClockMode, OjpError

    path, actor_id, now_us = sys.argv[1:4]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))

    def unresponsive(name):
        raise OjpError("VERIFICATION_UNAVAILABLE", f"arbiter is unresponsive: {name}")

    verification.failpoint_arbiter_unresponsive = unresponsive
    result = scheduler.tick_once(conn, actor_id=actor_id)
    print(json.dumps(result), flush=True)
    conn.close()
    """
)

# 送金の障害注入。`seam` は ledger モジュールの failpoint 属性名。
# 発火すると RuntimeError で異常終了する（exit 3）。
_PAYMENT_CRASH_SCRIPT = textwrap.dedent(
    """
    import sys
    from ojp import clock, ledger, service
    from ojp.domain import ClockMode

    path, seam, payment_op_id, now_us = sys.argv[1:5]
    conn = clock.initialize_database(path, ClockMode.TEST, test_now_us=int(now_us))

    def boom(name):
        raise RuntimeError(f"injected crash at {name}")

    setattr(ledger, seam, boom)
    try:
        service.process_single_payment(conn, operation_id=payment_op_id)
    except RuntimeError:
        sys.exit(3)
    print("unexpected success", flush=True)
    sys.exit(0)
    """
)


@dataclass(frozen=True)
class ScriptResult:
    """障害注入プロセスの実行結果。"""

    returncode: int
    stdout: str
    stderr: str

    @property
    def payload(self) -> dict[str, Any]:
        try:
            return json.loads(self.stdout.strip())
        except json.JSONDecodeError as exc:
            raise HarnessError(
                f"failpoint script produced non-JSON stdout: {self.stdout!r}\n"
                f"stderr={self.stderr}"
            ) from exc


def _run(world: E2EWorld, script: str, *args: str) -> ScriptResult:
    completed = subprocess.run(
        [sys.executable, "-c", script, *args],
        capture_output=True,
        text=True,
        timeout=PROCESS_TIMEOUT_SECONDS,
    )
    return ScriptResult(
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def submit_with_storage_failure(
    world: E2EWorld,
    *,
    actor: str,
    job_id: str,
    lease_id: str,
    version_id: str,
    artifact_json: str,
    operation_id: str,
) -> str:
    """保存失敗 seam を立てた独立プロセスから submit し、エラーコードを返す。

    seam が例外を投げるため submissions・Job 更新は transaction ごと
    rollback され、有効 Submission も review_due_at も残らない
    （service.submit はこれを VERIFICATION_UNAVAILABLE へ正規化する）。
    """
    result = _run(
        world,
        _SUBMIT_STORAGE_FAILURE_SCRIPT,
        str(world.db_path),
        actor,
        job_id,
        lease_id,
        version_id,
        artifact_json,
        operation_id,
        str(world.current_clock_us()),
    )
    if result.returncode != 0:
        raise HarnessError(
            f"submit storage-failure script failed: exit={result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    payload = result.payload
    world.report.add_operation(
        channel="failpoint",
        actor=actor,
        action="submit (storage failure injected)",
        operation_id=operation_id,
        ok=bool(payload.get("ok")),
    )
    if payload.get("ok"):
        raise HarnessError(
            f"submit unexpectedly succeeded under the storage failure seam:"
            f" {payload}"
        )
    return str(payload["code"])


def crash_payment(
    world: E2EWorld,
    *,
    seam: str,
    payment_operation_id: str,
    label: str,
) -> ScriptResult:
    """指定した送金 seam を発火させて独立プロセスを異常終了させる。

    `seam` は `failpoint_before_commit`（決済 commit 前の失敗）/
    `failpoint_after_commit`（commit 後の応答消失）/
    `failpoint_after_receipt`（Receipt 確定後・status 更新前の停止）。
    プロセスは実際に終了する（exit 3）。
    """
    result = _run(
        world,
        _PAYMENT_CRASH_SCRIPT,
        str(world.db_path),
        seam,
        payment_operation_id,
        str(world.current_clock_us()),
    )
    world.report.add_operation(
        channel="failpoint",
        actor="pt-system",
        action=f"{label} ({seam})",
        operation_id=payment_operation_id,
        ok=False,
    )
    if result.returncode != 3:
        raise HarnessError(
            f"expected the injected crash (exit 3) at {seam},"
            f" got exit={result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    return result


def tick_with_unresponsive_arbiter(
    world: E2EWorld, *, label: str
) -> dict[str, Any]:
    """裁定応答なし seam を立てた独立プロセスで tick を 1 回だけ回す。

    期限（`disputes.due_at_us`）前は何も書かず異議を OPEN のまま残し、
    期限到来後は保存済み PASS の fallback で解消する（第12節）。
    戻り値は tick の集計 JSON。
    """
    result = _run(
        world,
        _UNRESPONSIVE_ARBITER_TICK_SCRIPT,
        str(world.db_path),
        "pt-system",
        str(world.current_clock_us()),
    )
    if result.returncode != 0:
        raise HarnessError(
            f"unresponsive-arbiter tick failed: exit={result.returncode}\n"
            f"stdout={result.stdout}\nstderr={result.stderr}"
        )
    payload = result.payload
    world.report.add_operation(
        channel="tick",
        actor="pt-system",
        action=f"{label} (arbiter unresponsive)",
        operation_id=None,
        ok=payload.get("error") is None,
    )
    if payload.get("error") is not None:
        raise HarnessError(f"tick reported an error: {payload['error']}")
    return payload
