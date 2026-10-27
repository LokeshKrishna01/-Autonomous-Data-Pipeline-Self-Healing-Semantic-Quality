"""
scripts/simulate_failure.py

Simulates a real-world data pipeline that processes an incoming batch of
customer order records. It deliberately injects two classic failure modes:

  MODE 1  Schema Drift (column rename)
      Incoming batch uses `zip_code` but the pipeline expects `postal_code`.

  MODE 2  Format / Encoding Failure (date format change)
      Incoming batch sends dates as "DD-MM-YYYY" instead of "YYYY-MM-DD".

Run:
    python scripts/simulate_failure.py --mode schema_drift
    python scripts/simulate_failure.py --mode date_format
    python scripts/simulate_failure.py --mode both        (default)
"""

import argparse
import json
import sys
import traceback
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

#  Paths 
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "quarantine.duckdb"

#  Expected schema (what the pipeline was built against) 
EXPECTED_SCHEMA = {
    "order_id":    "VARCHAR",
    "customer_id": "VARCHAR",
    "postal_code": "VARCHAR",   # < pipeline expects THIS column name
    "amount":      "DOUBLE",
    "order_date":  "DATE",      # < pipeline expects YYYY-MM-DD
    "status":      "VARCHAR",
}

#  Mock "good" batch 
GOOD_BATCH = [
    {"order_id": "ORD-001", "customer_id": "C-100", "postal_code": "10001", "amount": 149.99, "order_date": "2024-01-15", "status": "shipped"},
    {"order_id": "ORD-002", "customer_id": "C-101", "postal_code": "90210", "amount": 89.50,  "order_date": "2024-01-16", "status": "pending"},
    {"order_id": "ORD-003", "customer_id": "C-102", "postal_code": "30301", "amount": 210.00, "order_date": "2024-01-17", "status": "delivered"},
]

#  Failing batch: MODE 1  column renamed postal_code  zip_code 
SCHEMA_DRIFT_BATCH = [
    {"order_id": "ORD-004", "customer_id": "C-103", "zip_code": "10002",  "amount": 55.00,  "order_date": "2024-01-18", "status": "shipped"},
    {"order_id": "ORD-005", "customer_id": "C-104", "zip_code": "94102",  "amount": 320.75, "order_date": "2024-01-19", "status": "pending"},
    {"order_id": "ORD-006", "customer_id": "C-105", "zip_code": "77001",  "amount": 18.99,  "order_date": "2024-01-20", "status": "delivered"},
]

#  Failing batch: MODE 2  date format switched to DD-MM-YYYY 
DATE_FORMAT_BATCH = [
    {"order_id": "ORD-007", "customer_id": "C-106", "postal_code": "10003", "amount": 75.00,  "order_date": "18-01-2024", "status": "shipped"},
    {"order_id": "ORD-008", "customer_id": "C-107", "postal_code": "60601", "amount": 430.10, "order_date": "19-01-2024", "status": "pending"},
]


#  Pipeline logic 

def setup_database(con: duckdb.DuckDBPyConnection) -> None:
    """Create the target orders table if it doesn't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id    VARCHAR PRIMARY KEY,
            customer_id VARCHAR NOT NULL,
            postal_code VARCHAR NOT NULL,
            amount      DOUBLE  NOT NULL,
            order_date  DATE    NOT NULL,
            status      VARCHAR NOT NULL
        )
    """)
    print("[DB] orders table ready.")


def run_pipeline(batch: list[dict], label: str) -> None:
    """
    Simulates a pipeline run: validates schema then inserts records.
    Raises descriptive errors that mimic real Airflow / dbt stack traces.
    """
    print(f"\n{'='*60}")
    print(f"[PIPELINE] Starting run: {label}")
    print(f"[PIPELINE] Batch size: {len(batch)} records")
    print(f"{'='*60}")

    df = pd.DataFrame(batch)
    print(f"[PIPELINE] Incoming columns: {list(df.columns)}")

    #  Schema validation 
    expected_cols = set(EXPECTED_SCHEMA.keys())
    incoming_cols = set(df.columns)
    missing_cols  = expected_cols - incoming_cols
    extra_cols    = incoming_cols - expected_cols

    if missing_cols:
        error_msg = (
            f"SchemaValidationError: Column(s) missing from incoming batch.\n"
            f"  Expected : {sorted(expected_cols)}\n"
            f"  Received : {sorted(incoming_cols)}\n"
            f"  Missing  : {sorted(missing_cols)}\n"
            f"  Extra    : {sorted(extra_cols)}\n"
            f"Traceback (most recent call last):\n"
            f"  File 'pipeline/loaders/orders_loader.py', line 84, in validate_schema\n"
            f"    raise SchemaValidationError(f'Missing columns: {{missing}}')\n"
            f"SchemaValidationError: Missing columns: {sorted(missing_cols)}"
        )
        raise RuntimeError(error_msg)

    #  Type / format validation 
    try:
        df["order_date"] = pd.to_datetime(df["order_date"], format="%Y-%m-%d")
    except Exception as e:
        error_msg = (
            f"FormatParsingError: Could not parse 'order_date' column.\n"
            f"  Expected format : YYYY-MM-DD\n"
            f"  Sample values   : {df['order_date'].tolist()[:3]}\n"
            f"Traceback (most recent call last):\n"
            f"  File 'pipeline/transformers/date_normalizer.py', line 42, in parse_dates\n"
            f"    df['order_date'] = pd.to_datetime(df['order_date'], format='%Y-%m-%d')\n"
            f"  File 'pandas/core/tools/datetimes.py', line 1067, in to_datetime\n"
            f"ValueError: {e}"
        )
        raise RuntimeError(error_msg)

    print(f"[PIPELINE]  Schema validation passed.")
    print(f"[PIPELINE]  Inserting {len(df)} records into orders table...")


def emit_webhook_payload(
    pipeline_name: str,
    run_id: str,
    error_log: str,
    mode: str,
    batch: list[dict],
) -> dict:
    """Builds the webhook payload that DataDoctor would receive."""
    payload = {
        "incident": {
            "pipeline_name": pipeline_name,
            "run_id": run_id,
            "git_commit_sha": "a1b2c3d4e5f6",
            "triggered_at": datetime.utcnow().isoformat() + "Z",
            "failure_mode": mode,
        },
        "error": {
            "raw_error_log": error_log,
        },
        "batch_metadata": {
            "total_records": len(batch),
            "sample_columns": list(batch[0].keys()) if batch else [],
            "sample_rows": batch[:2],
        },
    }
    return payload


#  Entry point 

def main():
    parser = argparse.ArgumentParser(description="DataDoctor  Failure Simulator")
    parser.add_argument(
        "--mode",
        choices=["schema_drift", "date_format", "both"],
        default="both",
        help="Which failure mode to simulate",
    )
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))
    setup_database(con)

    failures = []

    #  Run healthy batch first 
    try:
        run_pipeline(GOOD_BATCH, label="GOOD_BATCH (control run)")
        print("[PIPELINE]  Control run succeeded  no failures.\n")
    except RuntimeError as e:
        print(f"[PIPELINE]  Unexpected failure in control run:\n{e}")

    #  Inject failure modes 
    modes_to_run = []
    if args.mode in ("schema_drift", "both"):
        modes_to_run.append(("schema_drift", SCHEMA_DRIFT_BATCH))
    if args.mode in ("date_format", "both"):
        modes_to_run.append(("date_format", DATE_FORMAT_BATCH))

    for mode, batch in modes_to_run:
        run_id = f"run_{mode}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        try:
            run_pipeline(batch, label=f"FAILING_BATCH [{mode.upper()}]")
            print(f"[PIPELINE]   Expected failure did not occur for mode={mode}")
        except RuntimeError as e:
            error_log = str(e)
            print(f"\n[PIPELINE]  Pipeline FAILED  capturing error...\n")
            print(f"{''*60}")
            print(error_log)
            print(f"{''*60}\n")

            payload = emit_webhook_payload(
                pipeline_name="orders_daily_loader",
                run_id=run_id,
                error_log=error_log,
                mode=mode,
                batch=batch,
            )
            failures.append(payload)

            # Save webhook payload to fixtures for DataDoctor to consume
            out_path = ROOT / "tests" / "fixtures" / f"sample_{mode}_webhook.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            print(f"[SIMULATOR]  Webhook payload saved  {out_path}")

    con.close()

    print(f"\n{'='*60}")
    print(f"[SIMULATOR] Done. {len(failures)} failure(s) captured.")
    print(f"[SIMULATOR] Feed these to DataDoctor:")
    for p in failures:
        print(f"             {p['incident']['run_id']} [{p['incident']['failure_mode']}]")
    print(f"{'='*60}\n")
    return failures


if __name__ == "__main__":
    main()

