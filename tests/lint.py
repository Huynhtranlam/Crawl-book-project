from __future__ import annotations

import pathlib
import py_compile
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
TEXT_FILE_SUFFIXES = {".md", ".py", ".sql", ".yml", ".yaml"}
SKIP_PARTS = {".git", "__pycache__"}
INCLUDED_ROOTS = {".github", "airflow", "apps", "dashboard", "dbt", "tests"}
INCLUDED_FILES = {".env.example", "README.md", "docker-compose.yml", "requirements.txt"}


def iter_files() -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        relative_path = path.relative_to(REPO_ROOT)
        if (
            relative_path.parts[0] not in INCLUDED_ROOTS
            and relative_path.as_posix() not in INCLUDED_FILES
        ):
            continue
        if path.suffix in TEXT_FILE_SUFFIXES:
            files.append(path)
    return sorted(files)


def lint_text_file(path: pathlib.Path) -> list[str]:
    errors: list[str] = []
    content = path.read_text(encoding="utf-8")

    if content and not content.endswith("\n"):
        errors.append("missing trailing newline")

    for line_number, line in enumerate(content.splitlines(), start=1):
        if line.rstrip() != line:
            errors.append(f"line {line_number}: trailing whitespace")
        if "\t" in line:
            errors.append(f"line {line_number}: tab character")

    return errors


def lint_python_file(path: pathlib.Path) -> list[str]:
    errors = lint_text_file(path)
    try:
        py_compile.compile(str(path), doraise=True)
    except py_compile.PyCompileError as exc:
        errors.append(f"py_compile failed: {exc.msg}")
    return errors


def main() -> int:
    failures: list[str] = []

    for path in iter_files():
        if path.suffix == ".py":
            errors = lint_python_file(path)
        else:
            errors = lint_text_file(path)

        for error in errors:
            failures.append(f"{path.relative_to(REPO_ROOT)}: {error}")

    if failures:
        print("\n".join(failures))
        return 1

    print("Lint checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
