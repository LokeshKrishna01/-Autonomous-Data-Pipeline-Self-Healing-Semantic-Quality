"""
datadoctor/tools/log_parser.py
───────────────────────────────
Tool: parse a raw pipeline error log and extract structured signals.
Used by the Diagnostic Agent as a LangChain Tool.
"""

import re
from typing import Any


def parse_error_log(raw_error_log: str) -> dict[str, Any]:
    """
    Extracts structured signals from a raw pipeline error / stack trace.

    Returns a dict with:
        error_type      — high-level class of error (str)
        missing_columns — columns missing from the incoming batch (list)
        extra_columns   — unexpected columns in the incoming batch (list)
        bad_column      — the specific column involved (str | None)
        sample_values   — sample values from the error message (list)
        raw_exception   — the final exception line (str)
    """
    result: dict[str, Any] = {
        "error_type":      "UNKNOWN",
        "missing_columns": [],
        "extra_columns":   [],
        "bad_column":      None,
        "sample_values":   [],
        "raw_exception":   "",
    }

    # Detect schema validation errors
    if "SchemaValidationError" in raw_error_log or "Missing columns" in raw_error_log:
        result["error_type"] = "SCHEMA_DRIFT"

        missing_match = re.search(r"Missing\s*:\s*(\[.*?\])", raw_error_log)
        if missing_match:
            result["missing_columns"] = _parse_list_str(missing_match.group(1))

        extra_match = re.search(r"Extra\s*:\s*(\[.*?\])", raw_error_log)
        if extra_match:
            result["extra_columns"] = _parse_list_str(extra_match.group(1))

    # Detect date / format parsing errors
    elif "FormatParsingError" in raw_error_log or "to_datetime" in raw_error_log or "ValueError" in raw_error_log:
        result["error_type"] = "FORMAT_ENCODING"

        col_match = re.search(r"parse '(\w+)' column", raw_error_log)
        if col_match:
            result["bad_column"] = col_match.group(1)

        sample_match = re.search(r"Sample values\s*:\s*(\[.*?\])", raw_error_log)
        if sample_match:
            result["sample_values"] = _parse_list_str(sample_match.group(1))

    # Detect null / constraint violations
    elif "NOT NULL" in raw_error_log or "NullConstraintViolation" in raw_error_log:
        result["error_type"] = "NULL_VIOLATION"
        col_match = re.search(r"column[:\s]+['\"]?(\w+)['\"]?", raw_error_log, re.IGNORECASE)
        if col_match:
            result["bad_column"] = col_match.group(1)

    # Detect type mismatch
    elif "TypeError" in raw_error_log or "CastError" in raw_error_log or "type mismatch" in raw_error_log.lower():
        result["error_type"] = "TYPE_MISMATCH"

    # Grab the last exception line
    lines = [l.strip() for l in raw_error_log.strip().splitlines() if l.strip()]
    if lines:
        result["raw_exception"] = lines[-1]

    return result


def _parse_list_str(list_str: str) -> list[str]:
    """Converts a stringified Python list like "['a', 'b']" into a real list."""
    list_str = list_str.strip("[]")
    items = [item.strip().strip("'\"") for item in list_str.split(",") if item.strip()]
    return items
