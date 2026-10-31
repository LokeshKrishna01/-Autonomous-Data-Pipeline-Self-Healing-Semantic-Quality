import pandas as pd

# DataDoctor auto-generated patch — SCHEMA_DRIFT / COLUMN_RENAME
# Rename incoming columns to match the expected schema.
rename_map = {"zip_code": "postal_code"}
df = df.rename(columns=rename_map)
# Verify all expected columns are present after rename
expected = ["order_id", "customer_id", "postal_code", "amount", "order_date", "status"]
missing_after = [c for c in expected if c not in df.columns]
if missing_after:
    raise ValueError(f"Post-patch columns still missing: {missing_after}")
