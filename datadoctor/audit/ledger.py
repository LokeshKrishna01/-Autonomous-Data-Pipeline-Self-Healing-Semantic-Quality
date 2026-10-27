"""
datadoctor/audit/ledger.py
───────────────────────────
Immutable audit ledger — persists every agent state transition to DuckDB.
Every node entry/exit is recorded with a full state snapshot and timestamp.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

ROOT    = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "data" / "audit.duckdb"


def record_state_transition(state: dict, node: str, event_type: str = "EXIT") -> None:
    """
    Writes a single audit record to the audit_logs table.
    Safe to call even if the DB has not been initialised yet (logs a warning).
    """
    if not DB_PATH.exists():
        logger.warning(f"[Audit] DB not found at {DB_PATH}. Run scripts/setup_db.py first.")
        return

    try:
        con = duckdb.connect(str(DB_PATH))
        con.execute(
            """
            INSERT INTO audit_logs
                (id, incident_id, node_name, event_type, state_snapshot, git_commit_sha, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                state.get("incident_id", "unknown"),
                node,
                event_type,
                json.dumps(_safe_state_snapshot(state), default=str),
                state.get("git_commit_sha"),
                datetime.now(timezone.utc).isoformat(),
            ],
        )
        con.close()
    except Exception as e:
        # Audit failures must never crash the main pipeline
        logger.error(f"[Audit] Failed to write ledger entry: {e}")


def _safe_state_snapshot(state: dict) -> dict:
    """
    Returns a sanitised, serialisable copy of the state.
    Truncates very long fields to keep the ledger compact.
    """
    snapshot = {}
    for k, v in state.items():
        if k == "raw_error_log" and isinstance(v, str) and len(v) > 500:
            snapshot[k] = v[:500] + "… [truncated]"
        elif k == "sandbox_test_logs" and isinstance(v, str) and len(v) > 500:
            snapshot[k] = v[:500] + "… [truncated]"
        else:
            snapshot[k] = v
    return snapshot
