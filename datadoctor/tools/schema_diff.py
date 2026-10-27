"""
datadoctor/tools/schema_diff.py
────────────────────────────────
Tool: compare the expected schema (from the registry) against the incoming
batch columns to surface the exact structural delta.
Used by the Diagnostic Agent as a LangChain Tool.
"""

import duckdb
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "data" / "quarantine.duckdb"


def fetch_expected_schema(pipeline_name: str) -> dict[str, str]:
    """
    Fetches the expected schema for a pipeline from the DuckDB schema registry.
    Returns {column_name: expected_type}.
    """
    if not DB_PATH.exists():
        # Fallback for testing before DB is initialised
        return {}

    con = duckdb.connect(str(DB_PATH), read_only=True)
    rows = con.execute(
        "SELECT column_name, expected_type FROM expected_schemas WHERE pipeline_name = ?",
        [pipeline_name],
    ).fetchall()
    con.close()
    return {row[0]: row[1] for row in rows}


def diff_schemas(
    pipeline_name: str,
    incoming_columns: list[str],
) -> dict[str, Any]:
    """
    Computes the structural delta between expected and incoming schemas.

    Returns:
        {
          "missing_columns":  [...],   # in expected, NOT in incoming
          "extra_columns":    [...],   # in incoming, NOT in expected
          "matched_columns":  [...],   # in both
          "rename_candidates": [...],  # (missing_col, extra_col) pairs that look like renames
          "expected_schema":  {...},
          "incoming_columns": [...],
        }
    """
    expected = fetch_expected_schema(pipeline_name)
    expected_cols = set(expected.keys())
    incoming_cols = set(incoming_columns)

    missing  = sorted(expected_cols - incoming_cols)
    extra    = sorted(incoming_cols - expected_cols)
    matched  = sorted(expected_cols & incoming_cols)

    # Heuristic: if one column disappeared and one appeared, it's likely a rename
    rename_candidates = []
    if len(missing) == len(extra) and len(missing) > 0:
        for m, e in zip(missing, extra):
            rename_candidates.append({"from": e, "to": m})  # incoming "e" should be renamed to expected "m"

    return {
        "missing_columns":   missing,
        "extra_columns":     extra,
        "matched_columns":   matched,
        "rename_candidates": rename_candidates,
        "expected_schema":   expected,
        "incoming_columns":  sorted(incoming_columns),
    }
