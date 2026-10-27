# Product Requirement Document (PRD)
## DataDoctor — Autonomous Data Pipeline Self-Healing & Semantic Quality Agent

---

## 1. Executive Summary
**DataDoctor** is an agentic AI system engineered to detect, diagnose, and self-heal modern data ingestion and transformation pipelines (such as dbt, Apache Airflow, and Prefect). In modern data environments, unexpected schema drift, upstream API contract mutations, and malformed batch records frequently halt critical pipelines or silently corrupt downstream analytical models. 

DataDoctor intercepts pipeline failure events, isolates problematic records into non-destructive quarantine stores, plans schema migrations or data transformation patches, tests fixes within an isolated containerized sandbox, and opens auditable pull requests with human-in-the-loop validation.

---

## 2. Problem Statement & Market Opportunity
* **High Maintenance Overhead:** Data engineering teams spend up to 30–40% of their sprints tracking down pipeline failures caused by benign upstream contract changes (e.g., column renamed from `postal_code` to `zip_code`, or integer fields cast to string values).
* **Silent Data Loss:** Existing naive handling either fails an entire multi-hour batch job or drops failing rows without an audit trail, compromising analytical integrity.
* **Lack of Safe Auto-Remediation:** Automated scripts lack semantic reasoning and contextual testing capabilities, creating risk if allowed to write directly to production tables without verification.

---

## 3. Goals and Objectives
* **Reduce Mean Time to Resolution (MTTR):** Cut pipeline failure triage and remediation time from hours to under 5 minutes.
* **Zero Data Loss Guarantee:** Ensure 100% of invalid or unparseable records are partitioned into an auditable quarantine area rather than silently discarded.
* **Deterministic Sandbox Validation:** Require all LLM-generated code/schema fixes to compile, pass data distribution checks, and pass regression tests in an isolated sandbox before human submission.
* **Human-in-the-Loop Governance:** Restrict production writes to human approval via automated, evidence-linked Pull Requests and notifications.

---

## 4. User Personas
* **Persona 1: Lead Data Engineer (Alex):** Responsible for pipeline uptime and infrastructure reliability. Needs fast root-cause diagnostics and clean Git PRs that follow repository standards.
* **Persona 2: Analytics Engineer (Priya):** Owns dbt transformation models and downstream dashboard metrics. Needs to ensure changes do not skew metric aggregates or introduce null spikes.
* **Persona 3: Compliance & Data Governance Officer (Marcus):** Enforces data privacy and auditability. Requires reproducible audit logs proving no data was deleted or altered without authorization.

---

## 5. System Architecture & Multi-Agent Flow

```
   [Upstream Pipeline Failure Event (Airflow / dbt / Prefect Webhook)]
                                  │
                                  ▼
   ┌───────────────────────────────────────────────────────────────┐
   │                    DataDoctor Orchestrator                    │
   │                                                               │
   │  ┌───────────────────────┐         ┌───────────────────────┐  │
   │  │ 1. Diagnostic Agent   │         │ 2. Quarantine Agent   │  │
   │  │  - Ingest stack trace │ ──────► │  - Profile bad slice  │  │
   │  │  - Diff schemas       │         │  - Isolate raw batch  │  │
   │  └───────────────────────┘         └───────────────────────┘  │
   │              │                                 │              │
   │              ▼                                 ▼              │
   │  ┌───────────────────────┐         ┌───────────────────────┐  │
   │  │ 3. Patch Agent        │         │ 4. Verification Agent │  │
   │  │  - Draft SQL/dbt code │ ──────► │  - Sandbox execution  │  │
   │  │  - Adapt schema map   │         │  - Distribution diff  │  │
   │  └───────────────────────┘         └───────────────────────┘  │
   │                                                │              │
   └────────────────────────────────────────────────┼──────────────┘
                                                    │
                                                    ▼
   ┌───────────────────────────────────────────────────────────────┐
   │                    Audit & Delivery Gateway                   │
   │  • Git Pull Request (GitHub/GitLab)                           │
   │  • Evidence-backed Slack/Teams Notification                   │
   │  • Interactive Human-in-the-Loop Approval UI                  │
   └───────────────────────────────────────────────────────────────┘
```

---

## 6. Functional Requirements & Feature Specifications

### FR-1: Pipeline Event Ingestion & Webhook Listener
* **FR-1.1:** Support incoming webhooks from Apache Airflow task failure listeners, dbt Cloud run failure webhooks, Prefect state handlers, and standard GitHub Actions.
* **FR-1.2:** Ingest error payloads containing execution run ID, target model/DAG name, stack trace, and git commit SHA.

### FR-2: Root Cause Analysis (Diagnostic Agent)
* **FR-2.1:** Parse error stack traces to classify failure mode:
  * *Schema Drift:* Added, renamed, deleted, or re-typed columns.
  * *Semantic Violation:* Nulls in `NOT NULL` columns, primary key uniqueness violations, out-of-range values.
  * *Format/Encoding Failure:* Unparseable timestamps, unexpected JSON string payloads.
* **FR-2.2:** Fetch target schema definitions from repository manifests (`schema.yml`, target DDL) and compare against incoming batch payload structures.

### FR-3: Non-Destructive Quarantine & Data Profiling
* **FR-3.1:** Automatically isolate records that violate schema constraints into an encrypted S3/GCS or Snowflake/DuckDB quarantine bucket.
* **FR-3.2:** Profile clean vs. dirty data, logging: total row count, quarantined row count, null ratio, and type distribution.

### FR-4: Sandboxed Code Generation & Self-Healing
* **FR-4.1:** The Patch Agent generates candidate transformation logic:
  * For renamed columns: SQL alias or dbt column renaming macro.
  * For type mutations: Safe type casting with fallback (`TRY_CAST` / `COALESCE`).
  * For nested schema changes: JSON path extraction adapters.
* **FR-4.2:** Spin up an ephemeral sandbox using DuckDB or Dockerized PostgreSQL.
* **FR-4.3:** Execute the generated patch against the failing batch within the sandbox.

### FR-5: Automated Verification & Regression Safeguards
* **FR-5.1:** Validate that post-patch execution terminates with an exit code of `0`.
* **FR-5.2:** Compare pre- and post-patch data distributions. If the patch results in unexplained record deletions (> 0%), abort and flag for manual intervention.
* **FR-5.3:** Run existing dbt unit tests and data quality checks (`dbt test`, Great Expectations) in the sandbox.

### FR-6: Pull Request & Notification Gateway
* **FR-6.1:** Open a GitHub/GitLab Pull Request containing:
  * Minimal patch code (clean diff).
  * Root Cause Analysis breakdown.
  * Sandbox validation test logs.
  * Side-by-side distribution summary.
* **FR-6.2:** Post an interactive alert to Slack/Teams with options: `[Review PR]`, `[Rerun Sandbox]`, `[Dismiss]`.

---

## 7. Non-Functional Requirements (NFRs)

### Performance & Latency
* **Triage Time:** Root-cause diagnosis completed within 45 seconds of webhook trigger.
* **Sandbox Verification:** Patch synthesis and sandbox test cycle completed within 3 minutes for batch sizes up to 100,000 sample rows.

### Security & Privacy
* **Zero PII Exposure to LLM:** Raw data values containing PII (names, emails, IDs) must be masked or tokenized prior to prompt context ingestion. The LLM must only receive structural metadata, data types, and anonymized sample tokens.
* **Restricted Execution:** Sandbox environments must have strict network isolation (no egress to the public internet during test code execution).

### Auditability & Compliance
* **Deterministic Logging:** Every state transition, prompt, and tool output must be persisted in an immutable audit ledger (`audit_logs` table) with associated timestamps and Git references.

---

## 8. Data Schema & State Machine Model

### LangGraph Agent State Definition (Python)
```python
from typing import TypedDict, Optional, List, Dict, Any

class DataDoctorState(TypedDict):
    # Incident Context
    incident_id: str
    pipeline_name: str
    run_id: str
    raw_error_log: str
    
    # Diagnosis
    failure_category: str  # "SCHEMA_DRIFT", "TYPE_MISMATCH", "NULL_VIOLATION"
    root_cause_explanation: str
    offending_columns: List[str]
    
    # Data Isolation
    raw_sample_data: List[Dict[str, Any]]
    quarantined_record_count: int
    quarantine_location: str
    
    # Patch & Sandbox Validation
    proposed_patch_code: str
    sandbox_execution_status: str  # "PENDING", "SUCCESS", "FAILED"
    sandbox_test_logs: str
    distribution_diff: Dict[str, Any]
    
    # Governance & Output
    pull_request_url: Optional[str]
    human_approved: bool
```

---

## 9. Success Metrics (KPIs)

| Metric | Baseline (Manual) | Target with DataDoctor |
| :--- | :--- | :--- |
| **Mean Time to Triage (MTTT)** | 45 minutes | < 1 minute |
| **Mean Time to Repair (MTTR)** | 3.5 hours | < 10 minutes (inclusive of human PR review) |
| **Data Pipeline Availability** | 98.2% | > 99.8% |
| **Data Loss due to Silent Discards** | Variable | 0% (enforced by quarantine protocol) |
| **False-Positive Patch Rate** | N/A | < 2% of generated PRs rejected |

---

## 10. Roadmap & Implementation Phases

* **Phase 1: Diagnostic & Profiling Engine (Weeks 1–4)**
  * Airflow/dbt webhook listener setup.
  * Error log parser and semantic root-cause classifier.
  * Automated data quarantine to DuckDB storage.
* **Phase 2: Sandboxing & Patch Generation (Weeks 5–8)**
  * LLM-driven SQL/dbt patch generation using custom ReAct tools.
  * Ephemeral DuckDB sandbox runner with unit test verification.
  * Distribution and null-ratio diff comparison tool.
* **Phase 3: Integration & Human-in-the-Loop Gateway (Weeks 9–12)**
  * GitHub API automation for PR creation.
  * Interactive Slack Bolt app for incident alerts and 1-click approvals.
  * Enterprise telemetry and audit logging dashboard (Streamlit/Next.js).
