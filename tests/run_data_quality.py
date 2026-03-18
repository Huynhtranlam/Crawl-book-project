from __future__ import annotations

import os
import pathlib
import subprocess
import sys

from dbt.cli.main import dbtRunner


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"


def load_default_env() -> None:
    if not ENV_EXAMPLE_PATH.exists():
        return

    for raw_line in ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key, value)


def run_unit_tests() -> None:
    command = [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-p",
        "test_*.py",
    ]
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def run_dbt_command(args: list[str], label: str) -> None:
    print(f"Running {label}...")
    result = dbtRunner().invoke(args)
    if not result.success:
        raise SystemExit(f"{label} failed.")


def main() -> int:
    load_default_env()

    print("Running unit tests...")
    run_unit_tests()

    dbt_args = ["--project-dir", "dbt", "--profiles-dir", "dbt"]
    run_dbt_command(["source", "freshness", *dbt_args], "dbt source freshness")
    run_dbt_command(["test", *dbt_args], "dbt test")

    print("Data quality checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
