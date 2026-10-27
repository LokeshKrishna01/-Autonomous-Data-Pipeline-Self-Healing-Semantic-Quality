"""
scripts/setup_db.py

Initialises the local DuckDB databases for DataDoctor Day 1:

   data/quarantine.duckdb   stores quarantined (bad) records + profiling stats
   data/audit.duckdb        immutable audit ledger for state transitions

Run once before starting the system:
    python scripts/setup_db.py
"""

import duckdb
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

QUARANTINE_DB = DATA_DIR / "quarantine.duckdb"
AUDIT_DB      = DATA_DIR / "audit.duckdb"


#  Quarantine DB 

def setup_quarantine_db() -> None:
    con = duckdb.connect(str(QUARANTINE_DB))
    print(f"[SETUP] Quarantine DB -> {QUARANTINE_DB}")

    # Stores the raw, unmodified quarantined records as JSON blobs
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantined_records (
            id                  VARCHAR   DEFAULT gen_random_uuid(),
            incident_id         VARCHAR   NOT NULL,
            pipeline_name       VARCHAR   NOT NULL,
            run_id              VARCHAR   NOT NULL,
            failure_category    VARCHAR   NOT NULL,
            record_json         JSON      NOT NULL,
            quarantined_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (id)
        )
    """)

    # Stores per-incident profiling summary
    con.execute("""
        CREATE TABLE IF NOT EXISTS quarantine_profiles (
            incident_id             VARCHAR   PRIMARY KEY,
            pipeline_name           VARCHAR   NOT NULL,
            run_id                  VARCHAR   NOT NULL,
            total_record_count      INTEGER   NOT NULL,
            quarantined_record_count INTEGER  NOT NULL,
            null_ratio_json         JSON,
            type_distribution_json  JSON,
            profiled_at             TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # Reference table  the expected schema for each pipeline
    con.execute("""
        CREATE TABLE IF NOT EXISTS expected_schemas (
            pipeline_name   VARCHAR   NOT NULL,
            column_name     VARCHAR   NOT NULL,
            expected_type   VARCHAR   NOT NULL,
            nullable        BOOLEAN   DEFAULT true,
            registered_at   TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (pipeline_name, column_name)
        )
    """)

    # Seed the expected schema for the orders pipeline (used by Day 1 mock)
    orders_schema = [
        ("orders_daily_loader", "order_id",    "VARCHAR", False),
        ("orders_daily_loader", "customer_id", "VARCHAR", False),
        ("orders_daily_loader", "postal_code", "VARCHAR", False),
        ("orders_daily_loader", "amount",      "DOUBLE",  False),
        ("orders_daily_loader", "order_date",  "DATE",    False),
        ("orders_daily_loader", "status",      "VARCHAR", False),
    ]
    con.executemany("""
        INSERT OR REPLACE INTO expected_schemas
            (pipeline_name, column_name, expected_type, nullable)
        VALUES (?, ?, ?, ?)
    """, orders_schema)

    print(f"[SETUP]    quarantined_records table")
    print(f"[SETUP]    quarantine_profiles table")
    print(f"[SETUP]    expected_schemas table (seeded with orders_daily_loader schema)")
    con.close()


#  Audit DB 

def setup_audit_db() -> None:
    con = duckdb.connect(str(AUDIT_DB))
    print(f"\n[SETUP] Audit DB  {AUDIT_DB}")

    # Immutable log of every agent state transition
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id              VARCHAR   DEFAULT gen_random_uuid(),
            incident_id     VARCHAR   NOT NULL,
            node_name       VARCHAR   NOT NULL,  -- e.g. "diagnostic_agent", "patch_agent"
            event_type      VARCHAR   NOT NULL,  -- "ENTER" | "EXIT" | "ERROR"
            state_snapshot  JSON,               -- full DataDoctorState at this moment
            git_commit_sha  VARCHAR,
            timestamp       TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (id)
        )
    """)

    print(f"[SETUP]    audit_logs table")
    con.close()


#  Main 

if __name__ == "__main__":
    print("=" * 55)
    print("  DataDoctor  Local Database Setup")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    setup_quarantine_db()
    setup_audit_db()

    print(f"\n[SETUP]  All databases initialised successfully.")
    print(f"[SETUP] Next step -> python scripts/simulate_failure.py")

