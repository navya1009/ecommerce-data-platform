# tests/unit/test_no_hardcoded_credentials.py
# -----------------------------------------------------------------------------
# Guards the Priority 4 change: database passwords come from the environment, not
# from literals in src/. Same check as:
#     grep -rn "root123\|postgres123" src/
# Compiled __pycache__ files are skipped, because they are regenerated from source.
# -----------------------------------------------------------------------------
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC = PROJECT_ROOT / "src"
FORBIDDEN = re.compile(r"root123|postgres123")


def _source_files():
    return sorted(p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts)


def test_src_has_source_files_to_scan():
    assert (
        _source_files()
    ), "no .py files found under src/; the check would pass vacuously"


def test_src_contains_no_hardcoded_database_passwords():
    hits = []
    for path in _source_files():
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if FORBIDDEN.search(line):
                hits.append(
                    f"{path.relative_to(PROJECT_ROOT)}:{lineno}: {line.strip()}"
                )
    assert not hits, "hardcoded database passwords found in src/:\n" + "\n".join(hits)


@pytest.mark.parametrize(
    "module",
    [
        "spark_jobs/transform_orders.py",
        "spark_jobs/transform_clickstream.py",
        "spark_jobs/compute_aggregates.py",
    ],
)
def test_spark_job_reads_credentials_from_environment(module):
    text = (SRC / module).read_text(encoding="utf-8")
    # black may write double quotes; compare with single quotes throughout
    text = text.replace('"', "'")
    for name in ("MYSQL_USER", "MYSQL_PASSWORD", "POSTGRES_USER", "POSTGRES_PASSWORD"):
        if f"'{name}'" in text:
            assert (
                f"_required_env('{name}')" in text
            ), f"{module} reads {name} without _required_env"
    assert "_required_env('MYSQL_PASSWORD')" in text
    assert "_required_env('POSTGRES_PASSWORD')" in text
