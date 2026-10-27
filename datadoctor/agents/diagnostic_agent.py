"""
datadoctor/agents/diagnostic_agent.py
──────────────────────────────────────
LangGraph Node: Diagnostic Agent (FR-2)

Responsibilities:
  1. Parse the raw error log to extract structured signals.
  2. Fetch the expected schema from the registry and diff it against the
     incoming batch columns.
  3. Call the LLM with all structured context to:
       • Classify the failure category (SCHEMA_DRIFT / TYPE_MISMATCH /
         NULL_VIOLATION / FORMAT_ENCODING / SEMANTIC_VIOLATION)
       • Produce a concise, human-readable root cause explanation.
       • Identify the exact offending column(s).

Input state fields consumed:
    raw_error_log, pipeline_name, run_id, incident_id

Output state fields populated:
    failure_category, root_cause_explanation, offending_columns
"""

import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from datadoctor.core.state import DataDoctorState
from datadoctor.tools.log_parser import parse_error_log
from datadoctor.tools.schema_diff import diff_schemas
from datadoctor.audit.ledger import record_state_transition

logger = logging.getLogger(__name__)

# ── LLM ───────────────────────────────────────────────────────────────────────

def _get_llm() -> ChatOpenAI:
    from datadoctor.core.config import settings
    return ChatOpenAI(
        model=settings.llm_model,
        temperature=settings.llm_temperature,
        api_key=settings.openai_api_key,
    )


# ── System prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are DataDoctor's Diagnostic Agent — an expert data engineer specialising in 
pipeline failure root-cause analysis.

You will receive:
  1. A parsed error log with structured signals already extracted.
  2. A schema diff showing what columns were expected vs what arrived.

Your job is to respond with a JSON object (no markdown, no explanation — raw JSON only):

{
  "failure_category": "<one of: SCHEMA_DRIFT | TYPE_MISMATCH | NULL_VIOLATION | FORMAT_ENCODING | SEMANTIC_VIOLATION>",
  "root_cause_explanation": "<clear, 2–4 sentence human-readable explanation for a data engineer>",
  "offending_columns": ["<col1>", "<col2>"]
}

Rules:
- SCHEMA_DRIFT: A column was renamed, added, or deleted in the upstream source.
- TYPE_MISMATCH: A column's data type changed (e.g., int → string).
- NULL_VIOLATION: A NOT NULL column is receiving null values.
- FORMAT_ENCODING: Values are syntactically valid but in an unexpected format (e.g., dates, JSON).
- SEMANTIC_VIOLATION: Values are within type range but violate business rules (e.g., negative amounts).
- Be precise. If the schema diff shows a rename candidate, classify as SCHEMA_DRIFT.
- Never expose PII. Only reason about column names and types, not actual data values.
"""


# ── LangGraph node ─────────────────────────────────────────────────────────────

async def run_diagnostic_agent(state: DataDoctorState) -> DataDoctorState:
    """
    LangGraph node — runs the full diagnostic pipeline:
      parse log → diff schema → LLM classification → update state
    """
    incident_id   = state["incident_id"]
    pipeline_name = state["pipeline_name"]
    raw_error_log = state["raw_error_log"]

    logger.info("[DiagnosticAgent] Starting", extra={"incident_id": incident_id})
    record_state_transition(state, node="diagnostic_agent:enter")

    # ── Step 1: Parse the error log ────────────────────────────────────────────
    parsed_log = parse_error_log(raw_error_log)
    logger.info(f"[DiagnosticAgent] Parsed error type: {parsed_log['error_type']}")

    # ── Step 2: Schema diff ────────────────────────────────────────────────────
    # Extract incoming columns from the parsed log signals
    incoming_columns = _infer_incoming_columns(state, parsed_log)
    schema_diff = diff_schemas(pipeline_name, incoming_columns)
    logger.info(f"[DiagnosticAgent] Schema diff: missing={schema_diff['missing_columns']}, extra={schema_diff['extra_columns']}")

    # ── Step 3: LLM classification ─────────────────────────────────────────────
    user_content = _build_user_message(parsed_log, schema_diff, raw_error_log)

    try:
        llm = _get_llm()
        response = await llm.ainvoke([
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=user_content),
        ])
        diagnosis = _parse_llm_response(response.content)
    except Exception as e:
        logger.warning(f"[DiagnosticAgent] LLM call failed, falling back to rule-based: {e}")
        diagnosis = _rule_based_fallback(parsed_log, schema_diff)

    logger.info(
        f"[DiagnosticAgent] Diagnosis: category={diagnosis['failure_category']}, "
        f"columns={diagnosis['offending_columns']}"
    )

    # ── Step 4: Update state ───────────────────────────────────────────────────
    updated_state: DataDoctorState = {
        **state,
        "failure_category":       diagnosis["failure_category"],
        "root_cause_explanation": diagnosis["root_cause_explanation"],
        "offending_columns":      diagnosis["offending_columns"],
    }

    record_state_transition(updated_state, node="diagnostic_agent:exit")
    return updated_state


# ── Helpers ────────────────────────────────────────────────────────────────────

def _infer_incoming_columns(
    state: DataDoctorState,
    parsed_log: dict[str, Any],
) -> list[str]:
    """
    Best-effort extraction of incoming column names.
    Priority: sample_data in state → extra_columns from log → empty list.
    """
    if state.get("raw_sample_data"):
        return list(state["raw_sample_data"][0].keys())

    # Reconstruct from schema diff signals:
    # expected - missing + extra = incoming
    from datadoctor.tools.schema_diff import fetch_expected_schema
    expected = fetch_expected_schema(state["pipeline_name"])
    expected_cols = set(expected.keys())
    missing = set(parsed_log.get("missing_columns", []))
    extra   = set(parsed_log.get("extra_columns", []))
    incoming = (expected_cols - missing) | extra
    return sorted(incoming)


def _build_user_message(
    parsed_log: dict[str, Any],
    schema_diff: dict[str, Any],
    raw_error_log: str,
) -> str:
    return f"""## Parsed Error Log Signals
{json.dumps(parsed_log, indent=2)}

## Schema Diff (Expected vs Incoming)
{json.dumps(schema_diff, indent=2)}

## Raw Error Log (first 1500 chars)
{raw_error_log[:1500]}

Diagnose the root cause and return the JSON object as instructed.
"""


def _parse_llm_response(content: str) -> dict[str, Any]:
    """Parse the LLM's raw JSON response."""
    content = content.strip()
    # Strip markdown code fences if present
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
    return json.loads(content.strip())


def _rule_based_fallback(
    parsed_log: dict[str, Any],
    schema_diff: dict[str, Any],
) -> dict[str, Any]:
    """
    Pure rule-based fallback — used if the LLM is unavailable.
    Covers the two Day 1 failure modes without any AI.
    """
    error_type = parsed_log.get("error_type", "UNKNOWN")
    missing    = schema_diff.get("missing_columns", [])
    extra      = schema_diff.get("extra_columns", [])
    renames    = schema_diff.get("rename_candidates", [])
    bad_col    = parsed_log.get("bad_column")

    if error_type == "SCHEMA_DRIFT" or renames:
        rename_desc = ", ".join(f"'{r['from']}' → '{r['to']}'" for r in renames)
        explanation = (
            f"The upstream source renamed column(s): {rename_desc}. "
            f"The pipeline expected {missing} but received {extra} instead. "
            f"This is a schema drift event — likely an upstream contract change."
        )
        return {
            "failure_category":       "SCHEMA_DRIFT",
            "root_cause_explanation": explanation,
            "offending_columns":      missing + extra,
        }

    if error_type == "FORMAT_ENCODING":
        col = bad_col or "order_date"
        explanation = (
            f"Column '{col}' contains values in an unexpected format. "
            f"The pipeline expected ISO-8601 (YYYY-MM-DD) date strings but received a "
            f"different format (e.g., DD-MM-YYYY). This is a format/encoding failure."
        )
        return {
            "failure_category":       "FORMAT_ENCODING",
            "root_cause_explanation": explanation,
            "offending_columns":      [col],
        }

    return {
        "failure_category":       error_type,
        "root_cause_explanation": f"Unclassified pipeline failure. Raw error type: {error_type}.",
        "offending_columns":      missing,
    }
