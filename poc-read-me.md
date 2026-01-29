# 📌 MASTER CONTEXT PROMPT: Enhanced Version
## Snowflake + dbt + Airflow Financial Data Platform POC (10-Day Plan)

## 1. Background & Objective

I am a Principal Data Engineer who has recently completed internal technical training and must build a hands-on Proof of Concept (POC) before client onboarding (Morgan Stanley).

### Primary Evaluation Goals:

- Demonstrate deep Snowflake understanding with production-realistic patterns
- Show end-to-end data platform thinking with governance and security
- Prove effective integration of Snowflake, dbt, Airflow, Python
- Model financial data safely and realistically with clear business value
- Handle real-world data engineering concerns (quality, late data, cost, governance, scalability)

**Key Differentiator:** This is not production, but must demonstrate production-ready thinking with justifiable architecture decisions.

## 2. Prerequisites & Setup Requirements

### Local Software (Windows)

| # | Software | Purpose | Installation |
|---|----------|---------|--------------|
| 1 | Docker Desktop | Runs Airflow containers | Download from docker.com |
| 2 | Python 3.10 | Core programming language | Download from python.org |
| 3 | Git | Version control | Download from git-scm.com |
| 4 | VS Code | Code editor | Download from code.visualstudio.com |
| 5 | dbt Core | Analytics engineering | `pip install dbt-core dbt-snowflake` |
| 6 | DBeaver | Snowflake GUI client | Download from dbeaver.io |

### Cloud Accounts (Free Tier)

| # | Account | Purpose | Credits/Limits |
|---|---------|---------|----------------|
| 1 | AWS Free Tier | S3 storage for data landing zone | 5GB storage free |
| 2 | Snowflake Trial #1 | Producer account (data engineering) | $400 credits, 30 days |
| 3 | Snowflake Trial #2 | Consumer account (analytics) | $400 credits, 30 days |

**Note:** Total setup time approximately 3-4 hours. All software and accounts remain within free tier limits when following cost control best practices.

## 3. Core Technology Stack & Explicit Decisions

### Technology Stack:

- Snowflake (Enterprise Data Platform)
- dbt Core (Analytics Engineering)
- Apache Airflow (Orchestration)
- Python (Custom transformations/API)
- Docker (Local execution environment)
- AWS S3 Free Tier (External stages and Snowpipe source)

### Architectural Decisions with Justification:

| Decision | Justification | Production Consideration |
|----------|---------------|--------------------------|
| Airflow & dbt run locally via Docker | Decouples orchestration logic from cloud vendor lock-in, simplifies local development/debugging, demonstrates pipeline portability. Enables quick iteration during POC phase. | Would migrate to MWAA/Astronomer or Kubernetes for production scale and collaboration. |
| AWS used only for S3 | Simulates common enterprise pattern where raw data lands in cloud object store (data lake) owned separately from warehouse. Enables cost attribution and independent lifecycle policies. | Production would leverage S3 Intelligent Tiering and proper IAM roles with assumed Snowflake access. |
| No EC2, MWAA, or cloud compute hosting | Keeps POC cost at zero while demonstrating complete architecture. Focus remains on data patterns, not infrastructure management. | Production would require managed services for reliability and scalability. |
| Two Snowflake Trial Accounts | Models real-world producer/consumer data mesh pattern. Enforces clean data contracts via Secure Sharing. Isolates cost and permissions between internal (prod) and external (consumer) environments. | Mirrors enterprise multi-account strategy for security and compliance boundaries. |

## 3. Snowflake Environment Strategy

### Producer Account (Data Engineering Team):

- All ingestion pipelines (Snowpipe, batch)
- Transformations (dbt models)
- Internal staging and quality checks
- Data product creation
- Admin privileges for: Warehouses, Databases, Schemas, Roles, Shares, Tasks, Snowpipe

### Consumer Account (Business/Analytics Team):

- Consumes curated data products via Secure Data Sharing
- Has masked/restricted views of sensitive data
- Runs analytical queries with separate compute
- Limited privileges: Read-only access to shares, dedicated query warehouse

**Key Pattern:** Separation of concerns with explicit data contracts via sharing.

## 4. Data Domain & Business Context

**Domain:** Mutual Funds + Market Data (Hybrid Finance Domain)

### Finance Knowledge Constraint Acknowledgement:

- I am new to finance domain
- Financial logic must be: Simple, Explainable, Realistic
- NAV and prices are consumed, not derived from scratch

### Business Metrics Focus (Simple but Meaningful):

- Daily Fund Performance (NAV % change day-over-day)
- Assets Under Management (AUM) by fund (simplified calculation)
- Investor Transaction Trends (weekly summary)

## 5. Data Sources & Ingestion Patterns (5+ Sources)

| Source | Format | Ingestion Pattern | Business Purpose |
|--------|--------|-------------------|------------------|
| Mutual Fund NAV | CSV | Snowpipe (S3 → Snowflake) | Core pricing data, updates daily |
| Fund Metadata | CSV | Batch load (Airflow) | Fund characteristics, slow-changing |
| Market Prices (Index) | JSON (Mock API) | Python → S3 → Snowpipe | Benchmark comparison data |
| Investor Transactions | Synthetic CSV | Batch load with idempotency | Investor activity simulation |
| Reference Data | Static CSV | dbt Seeds | Calendar, Currency codes |
| NEW: Fund Manager Info | CSV | Batch with SCD Type 2 | Slowly changing dimension demo |

### Ingestion Patterns Demonstrated:

- Batch (full/partial with merge)
- API ingestion (simulated)
- Micro-batch (Snowpipe auto-ingest)
- Idempotent processing patterns

## 6. Architecture & Modeling Approach

### Data Layering (Modified Medallion Architecture):

- **RAW (Bronze)** – As-is ingestion, timestamped, VARIANT/JSON where appropriate
- **STAGING (Silver)** – Cleaned, typed, deduplicated, business keys established
- **MARTS (Gold)** – Business-ready datasets delivered as:
  - Star schemas (facts/dimensions) for analyst consumption
  - Metric-specific tables for dashboarding
  - Data products ready for sharing

**Rationale:** Combines flexibility of modern medallion architecture with usability of Kimball dimensional modeling for end consumers.

## 7. dbt Implementation Strategy (Senior Level)

### Models Architecture:

```
models/
├── staging/           # Source-aligned, 1:1 with sources
├── intermediate/      # Business transformations, reusable
├── marts/            # Consumer-ready data products
│   ├── finance/      # Fund performance, AUM
│   └── reporting/    # Aggregated metrics
```

### Advanced Features Demonstrated:

- **Tests:** not_null, unique, relationships, custom data quality tests (value bounds, freshness)
- **Snapshots:** SCD Type 2 for fund metadata and manager info
- **Seeds:** Reference data with versioning
- **Macros:** Reusable SQL for financial calculations (e.g., daily returns)
- **Exposures:** Downstream BI tool connections (simulated)
- **Documentation:** dbt docs with business context
- **Meta Tags:** Data classification (pii: true, business_critical: true)

## 8. Airflow Orchestration Pattern

**Design Philosophy:** Airflow as pure orchestrator, no business logic.

### DAG Structure:

```python
# Example high-level DAG design
ingest_raw_data >> 
validate_raw_quality >> 
trigger_dbt_staging >> 
monitor_dbt_run >> 
trigger_dbt_marts >> 
update_data_shares >> 
generate_observability_metrics
```

### Features Demonstrated:

- Retries & SLAs with appropriate timeouts
- TaskFlow API for clean Python dependencies
- XComs for metadata passing (e.g., processed date ranges)
- Branch operators for conditional flows (e.g., full vs incremental)
- Sensors (simulated) for external dependency checks

### Anti-Patterns Avoided:

- No business logic in Airflow
- No data movement in Airflow tasks
- Minimal use of PythonOperator (prefer SQL/Snowflake operators)

## 9. Snowflake Features Demonstrated (Idiomatic Usage)

| Feature | Demonstration Purpose | Production Pattern |
|---------|----------------------|-------------------|
| External Stages (S3) | Landing zone pattern | Lifecycle policies, encryption |
| Snowpipe Auto-ingest | Event-driven micro-batch | Error queue monitoring, auto-retry |
| Streams | CDC for incremental processing | Late-arriving data handling |
| Secure Data Sharing | Data product distribution | Cross-account, read-only isolation |
| Time Travel | Safe development, error recovery | Backup strategy complement |
| Warehouse Management | Size scaling, auto-suspend | Workload isolation, cost control |
| Tasks | Internal scheduling | Dependency chaining, snowflake-native pipelines |
| Materialized Views | Performance optimization | Pre-aggregated metrics |
| NEW: Dynamic Data Masking | Column-level security in Consumer account | Role-based data protection |

## 10. Non-Functional & Real-World Scenarios

| Concern | Implementation Strategy | Production Readiness Signal |
|---------|------------------------|---------------------------|
| Data Quality | dbt tests + custom assertions, anomaly detection (3-sigma), freshness monitoring | Proactive quality framework |
| Idempotency | Merge statements, incremental models, idempotent batch loads | Safe reprocessing capability |
| Late-arriving Data | Streams + merge logic, windowed processing | Graceful handling of real-world latency |
| Schema Evolution | VARIANT for JSON, ALTER TABLE for CSV, versioned views | Backward compatibility strategy |
| Observability | Airflow logs + dbt artifacts loaded to Snowflake, simple dashboard | Operational visibility |
| Cost Control | Warehouse sizing, auto-suspend, workload isolation, query tagging | Financial accountability |
| Governance | Tagging (pii, cost_center), masking policies, data lineage | Compliance foundation |
| Scalability | Warehouse scaling patterns, incremental models | Growth preparedness |

## 11. Data Governance & Security Layer

### Classification & Tagging:

```sql
-- Applied in Producer account
ALTER TABLE raw.investor_transactions 
  SET TAG data_classification.pii_identifier = 'true';
ALTER TABLE marts.fund_performance 
  SET TAG business_domain.cost_center = 'finance';
```

### Security Implementation:

- **Producer Account:** Least privilege roles (loader, transformer, viewer)
- **Consumer Account:** Read-only role + dynamic masking policy

```sql
CREATE MASKING POLICY mask_email AS (val string) RETURNS string ->
  CASE WHEN CURRENT_ROLE() = 'ANALYST' THEN val
       ELSE REGEXP_REPLACE(val, '^(.).*@(.).*\\.(..).*$', '\\1****@\\2****.\\3') 
  END;
```

### Lineage & Documentation:

- dbt exposures for downstream dependencies
- dbt docs with business definitions
- Simple lineage diagram in documentation

## 12. Observability & Operations Strategy

### Three-Layer Observability:

- **Pipeline Health (Airflow):** Success/failure rates, duration SLAs
- **Data Quality (dbt):** Test failure rates, freshness metrics
- **Platform Performance (Snowflake):** Warehouse credit usage, query performance

### dbt Artifacts Pipeline:

```
dbt run → artifacts generated → load to Snowflake → queryable history
```

Enables SQL analysis of: model run times, test failures over time, data freshness trends

### Simple Dashboard Concept:

Single pane showing: Last successful runs, Current data latency, Active data quality issues, Daily credit consumption

## 13. Execution Timeline (10 Days - Enhanced)

| Day | Focus Area | Key Deliverables | Success Criteria |
|-----|-----------|------------------|------------------|
| 1 | Foundation | Snowflake accounts, dataset understanding, define 2-3 key business metrics | Clear scope, mock data ready |
| 2 | Architecture | Finalize naming standards, model design, lineage diagram | Approved architecture, documented decisions |
| 3 | AWS + Snowflake | S3 buckets, IAM roles, Snowflake objects, tagging strategy | Secure infrastructure, cost tags applied |
| 4 | Snowpipe Ingestion | Auto-ingest setup, error handling, idempotency pattern | Streaming data flows to RAW |
| 5 | Batch Processing | Batch loads, SCD patterns, data quality checks | Reliable batch ingestion with quality gates |
| 6 | dbt Staging | Source models, staging layer, custom tests | Clean, tested staging models |
| 7 | dbt Marts & Metrics | Business metrics, star schemas, exposures | Business-ready data products |
| 8 | Airflow Orchestration | DAGs, dependencies, observability hooks | End-to-end orchestration working |
| 9 | Security & Sharing | Secure Share setup, masking policies, consumer views | Consumer can access masked data |
| 10 | Polish & Narrative | Documentation, walkthrough prep, business metric trace | Compelling story from S3 to business insight |

## 14. Success Criteria & Evaluation Narrative

The POC is successful if it demonstrates I can:

1. **Design & Implement** an end-to-end financial data platform with production-realistic patterns
2. **Use Snowflake Idiomatically** with advanced features (sharing, streams, masking, tasks)
3. **Apply dbt as a Framework** not just a transformer, with tests, docs, and reusable patterns
4. **Orchestrate Responsibly** with Airflow as a coordinator, not a workflow engine
5. **Think Like a Principal Engineer** by anticipating scale, cost, governance, and security
6. **Communicate Business Value** by tracing a simple metric from raw source to consumer insight
7. **Justify Every Decision** with clear trade-offs and production migration considerations

### Walkthrough Narrative Structure:

1. Start with the business question: "What was Fund X's performance yesterday?"
2. Trace backward through: Consumer dashboard → Shared data → Curated marts → Cleaned staging → Raw ingestion → Source systems
3. Highlight key decisions at each layer with "why" explanations
4. Demonstrate handling of a real-world scenario (e.g., late-arriving NAV data)
5. Show observability and cost control mechanisms
6. End with scalability and production migration path
