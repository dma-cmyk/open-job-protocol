"""barrier で同期した複数 CLI プロセスの同時開始（計画書 第16節・第17節）。

同時 Claim（N06）・同時 approve（N05）を**実プロセス**で再現するための helper。
共有メモリ Lock には頼らず、開始の同期だけを共有ファイルへの追記で行う
（直列化は DB の BEGIN IMMEDIATE と business_key の UNIQUE が担う。
tests/integration/test_create_child_race.py と同じ方法）。

barrier 待機の wrapper は `os.execv` で本物の CLI（`python -m ojp.cli`）へ
置き換わるため、観測対象は harness.run_cli と同じ実 CLI プロセスになる。
待機の timeout は OS の単調時計（`time.monotonic`）を使い、固定 Clock に
依存させない。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from dataclasses import dataclass

from .harness import PROCESS_TIMEOUT_SECONDS, CliResult, E2EWorld

# argv[0] 以降をそのまま exec する barrier wrapper。
# sys.argv[1]=barrier ファイル / sys.argv[2]=待ち合わせ人数 / sys.argv[3:]=CLI argv
_BARRIER_WRAPPER = textwrap.dedent(
    """
    import os, sys, time

    barrier_path = sys.argv[1]
    expected = int(sys.argv[2])
    argv = sys.argv[3:]
    with open(barrier_path, "a") as f:
        f.write("x\\n")
    deadline = time.monotonic() + 30.0
    while True:
        with open(barrier_path) as f:
            if len(f.read().split()) >= expected:
                break
        if time.monotonic() > deadline:
            sys.stderr.write("barrier timeout\\n")
            sys.exit(9)
        time.sleep(0.005)
    os.execv(argv[0], argv)
    """
)


@dataclass(frozen=True)
class Invocation:
    """同時開始する 1 プロセス分の CLI 呼び出し。"""

    args: list[str]
    actor: str
    operation_id: str | None = None
    action: str | None = None


def run_simultaneously(
    world: E2EWorld,
    invocations: list[Invocation],
    *,
    barrier_name: str,
) -> list[CliResult]:
    """複数の CLI 呼び出しを barrier で同時開始し、結果を入力順で返す。

    どのプロセスも成功／失敗の共通封筒を stdout へ出すため、成否は呼出側が
    判定する（この helper は expect_ok を持たない）。各呼び出しは
    world.report へ記録する。
    """
    barrier = world.project_root / f"barrier-{barrier_name}.txt"
    barrier.write_text("", encoding="utf-8")
    expected = len(invocations)
    started: list[tuple[Invocation, list[str], subprocess.Popen[str]]] = []
    for invocation in invocations:
        argv = world.cli_argv(
            invocation.args,
            actor=invocation.actor,
            operation_id=invocation.operation_id,
        )
        process = subprocess.Popen(
            [sys.executable, "-c", _BARRIER_WRAPPER, str(barrier), str(expected)]
            + argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        started.append((invocation, argv, process))

    results: list[CliResult] = []
    for invocation, argv, process in started:
        stdout, stderr = process.communicate(timeout=PROCESS_TIMEOUT_SECONDS)
        result = CliResult(
            argv=argv,
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
            payload=E2EWorld.parse_stdout_json(stdout),
        )
        world.report.add_operation(
            channel="cli",
            actor=invocation.actor,
            action=invocation.action or " ".join(invocation.args[:2]),
            operation_id=result.operation_id or invocation.operation_id,
            ok=result.ok,
        )
        results.append(result)
    return results
