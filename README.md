# 🩺 DataDoctor
### Autonomous Data Pipeline Self-Healing & Semantic Quality Agent

> **DataDoctor** detects, diagnoses, and self-heals broken data pipelines — automatically. Powered by a LangGraph multi-agent system, it quarantines bad records, generates semantic patches, validates fixes in an isolated sandbox, and opens a human-reviewed Pull Request. All in under 5 minutes.

[![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)](https://python.org)
[![LangGraph](https://img.shields.io/badge/LangGraph-Multi--Agent-purple)](https://github.com/langchain-ai/langgraph)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

## 🚨 The Problem

Modern data teams lose **30–40% of sprint time** chasing pipeline failures caused by simple upstream changes — a renamed column, a type cast, a null where none was expected. Existing tools either crash the entire batch job or silently drop rows with no audit trail.

**DataDoctor fixes this automatically.**

---

## ✨ Key Features

| Feature | Description |
|---|---|
| 🔍 **Root Cause Analysis** | Classifies failures as schema drift, type mismatch, null violation, or format error |
| 🔒 **Zero Data Loss Quarantine** | Isolates bad records to DuckDB / S3 — nothing is ever silently dropped |
| 🤖 **AI Patch Generation** | LLM generates SQL / dbt YAML fixes (column renames, type casts, JSON path adapters) |
| 🧪 **Sandboxed Verification** | Every patch is tested in an ephemeral DuckDB / Docker sandbox before it touches production |
| 📬 **Pull Request Gateway** | Opens a GitHub / GitLab PR with full evidence: diff, RCA, test logs, distribution summary |
| 📣 **Slack / Teams Alerts** | Interactive notifications with `[Review PR]`, `[Rerun Sandbox]`, `[Dismiss]` actions |
| 📋 **Immutable Audit Ledger** | Every state transition, prompt, and tool output is logged for compliance |

---

## 🏗️ Architecture

```
  [Pipeline Failure Webhook]
  (Airflow / dbt / Prefect)
            │
            ▼
  ┌─────────────────────────────────────────────────┐
  │           DataDoctor Orchestrator               │
  │              (LangGraph)                        │
  │                                                 │
  │  ① Diagnostic Agent      ② Quarantine Agent    │
  │   · Parse stack trace  ──▶  · Isolate records  │
  │   · Diff schemas            · Profile data      │
  │          │                       │              │
  │          ▼                       ▼              │
  │  ③ Patch Agent          ④ Verification Agent   │
  │   · Generate SQL/dbt  ──▶  · Run in sandbox    │
  │   · Adapt schema map        · Distribution diff │
  └───────────────────────────────┬─────────────────┘
                                  │
                                  ▼
  ┌─────────────────────────────────────────────────┐
  │           Audit & Delivery Gateway              │
  │  • GitHub / GitLab Pull Request                 │
  │  • Slack / Teams Interactive Notification       │
  │  • Streamlit Human-in-the-Loop Dashboard        │
  └─────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
datadoctor/
├── agents/
│   ├── diagnostic_agent.py       # FR-2: Root cause analysis
│   ├── quarantine_agent.py       # FR-3: Non-destructive data isolation
│   ├── patch_agent.py            # FR-4: LLM-driven patch generation
│   └── verification_agent.py    # FR-5: Sandbox regression testing
├── api/
│   ├── webhook_listener.py       # FR-1: Airflow / dbt / Prefect webhooks
│   ├── routes.py
│   └── schemas.py
├── core/
│   ├── state.py                  # LangGraph DataDoctorState TypedDict
│   ├── graph.py                  # Agent wiring & conditional edges
│   ├── orchestrator.py           # Pipeline entry point
│   └── config.py                 # Pydantic settings
├── tools/
│   ├── schema_diff.py
│   ├── log_parser.py
│   ├── pii_masker.py             # NFR: Zero PII exposure to LLM
│   ├── distribution_diff.py
│   └── dbt_runner.py
├── quarantine/
│   ├── isolator.py
│   ├── profiler.py
│   └── storage.py                # DuckDB / S3 / GCS backends
├── sandbox/
│   ├── duckdb_sandbox.py
│   ├── docker_sandbox.py
│   └── runner.py
├── gateway/
│   ├── github_pr.py
│   ├── gitlab_pr.py
│   ├── slack_notifier.py
│   └── teams_notifier.py
└── audit/
    ├── ledger.py                 # Immutable state transition log
    └── logger.py

tests/
├── unit/agents/                  # Agent unit tests
├── unit/tools/                   # Tool unit tests
├── integration/                  # Webhook & flow integration tests
├── e2e/                          # Full pipeline healing tests
└── fixtures/                     # Sample webhook payloads & schema drifts

dashboard/                        # Streamlit human-in-the-loop UI
infra/
├── docker/                       # Dockerfile + docker-compose
├── terraform/                    # Cloud infra (S3, DuckDB, secrets)
└── ci/                           # GitHub Actions workflows
docs/                             # Architecture diagrams & API reference
scripts/                          # Dev utilities (simulate failures, seed DB)
```

---

## ⚡ Quickstart

### 1. Clone & Install

```bash
git clone https://github.com/LokeshKrishna01/-Autonomous-Data-Pipeline-Self-Healing-Semantic-Quality.git
cd -Autonomous-Data-Pipeline-Self-Healing-Semantic-Quality
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env — add your OpenAI key, GitHub token, Slack token
```

### 3. Start the Webhook Listener

```bash
uvicorn datadoctor.api.webhook_listener:app --reload --port 8000
```

### 4. Launch the Dashboard (optional)

```bash
streamlit run dashboard/app.py
```

---

## 🔌 Supported Integrations

| Category | Tools |
|---|---|
| **Pipeline Orchestrators** | Apache Airflow, dbt Cloud, Prefect |
| **Quarantine Storage** | DuckDB, Amazon S3, Google Cloud Storage |
| **Sandbox Execution** | DuckDB (embedded), Dockerized PostgreSQL |
| **Version Control** | GitHub, GitLab |
| **Notifications** | Slack (Bolt), Microsoft Teams |
| **Data Quality** | dbt test, Great Expectations |

---

## 📊 Performance Targets

| Metric | Baseline | Target |
|---|---|---|
| Mean Time to Triage | 45 minutes | **< 1 minute** |
| Mean Time to Repair | 3.5 hours | **< 10 minutes** |
| Pipeline Availability | 98.2% | **> 99.8%** |
| Silent Data Loss | Variable | **0%** |
| False-Positive Patch Rate | N/A | **< 2%** |

---

## 🗺️ Roadmap

- [x] **Phase 1** — Diagnostic & Profiling Engine (Weeks 1–4)
  - Webhook listener for Airflow / dbt / Prefect
  - Error log parser & semantic root-cause classifier
  - Automated DuckDB quarantine
- [ ] **Phase 2** — Sandboxing & Patch Generation (Weeks 5–8)
  - LLM-driven SQL / dbt patch generation
  - Ephemeral sandbox runner with regression tests
  - Distribution diff comparison tool
- [ ] **Phase 3** — Integration & Human-in-the-Loop Gateway (Weeks 9–12)
  - GitHub API PR automation
  - Interactive Slack Bolt approval flow
  - Streamlit / Next.js audit dashboard

---

## 🔐 Security & Compliance

- **Zero PII Exposure:** All raw data is masked/tokenized before reaching the LLM. Only structural metadata and anonymized samples are sent.
- **Network-Isolated Sandbox:** No egress to the public internet during sandbox code execution.
- **Immutable Audit Ledger:** Every action is timestamped and git-referenced for full compliance traceability.

---

## 🤝 Contributing

Pull requests are welcome! Please open an issue first to discuss what you'd like to change.

---

## 📄 License

[MIT](LICENSE) — built with ❤️ using LangGraph, FastAPI, DuckDB, and Streamlit.
