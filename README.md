# SentinelFlow — Real-Time AML Compliance Pipeline

> An end-to-end data engineering pipeline on Databricks that automates Anti-Money Laundering compliance — processing ISO 20022 payment streams through a Bronze-Silver-Gold Medallion architecture with live sanctions screening, fraud detection, and automated SAR reporting.

[![CI Pipeline](https://github.com/Haritha1752/sentinelflow-aml-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Haritha1752/sentinelflow-aml-pipeline/actions)

---

## Why This Project Exists

In 2025, global regulators imposed **$1.23 billion in fines** for AML compliance failures — a 417% increase over 2024. The EU Instant Payments Regulation now requires real-time sanctions screening, and banks using legacy batch processing are falling behind. SentinelFlow demonstrates a production-grade solution to this exact problem using modern data engineering tools.

---

## Architecture

```
                         SentinelFlow Architecture
                         ========================

  Transaction          Unity Catalog              Databricks SQL
  Generator            Volume                     Dashboard
  (Python)             (Cloud Storage)            (Live BI)
      |                     |                          ^
      v                     v                          |
  [JSON files] ──> [Auto Loader] ──> [Bronze] ──> [Silver] ──> [Gold]
                   (Streaming)       (Raw)        (Enriched)   (SAR Reports)
                                       |              |             |
                                       |         Live APIs:         |
                                       |         - OFAC API         |
                                       |         - ECB FX API       |
                                       |         - FATF Rules       |
                                       |              |             |
                                       |         [GNN Model]        |
                                       |         (MLflow)           |
                                       |              |             |
                                       v              v             v
                                   [Unity Catalog — Full Data Lineage]
                                              |
                                   [Databricks Workflow]
                                   (Automated Daily at 8am)
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Compute** | Databricks Free Edition (Serverless) | Pipeline execution and ML training |
| **Storage** | Delta Lake | ACID-compliant data lakehouse tables |
| **Ingestion** | Auto Loader (Structured Streaming) | Incremental file ingestion — only reads new files |
| **Processing** | PySpark, PySpark UDFs | Distributed data transformation and compliance logic |
| **Governance** | Unity Catalog | Data lineage, audit trail, access control |
| **ML** | PyTorch, MLflow Model Registry | GNN fraud detection model training and serving |
| **Orchestration** | Databricks Workflows | Automated daily pipeline execution |
| **CI/CD** | GitHub Actions, pytest | 32 automated unit tests on every push |
| **APIs** | OFAC (US Treasury), ECB Frankfurter, FATF rules | Live sanctions screening, exchange rates |
| **Language** | Python 3.11, SQL | Pipeline logic and dashboard queries |

---

## Pipeline Layers

### Bronze — Raw Ingestion
- Auto Loader watches the Unity Catalog Volume for new JSON files
- Each new file is ingested **incrementally** — files already processed are never re-read
- Every record is saved exactly as received with ingestion metadata (timestamp, source file)
- Data accumulates over time — never deleted (regulatory requirement)

### Silver — Enrichment & Compliance Screening
- **OFAC Sanctions Screening**: Every sender and receiver name checked against the live US Treasury SDN list (468+ sanctioned entities)
- **FATF Travel Rule Validation**: All required originator and beneficiary fields verified — missing fields trigger a compliance violation
- **Currency Normalization**: All amounts converted to USD using live ECB exchange rates (30+ currencies)
- **Risk Flagging**: Transactions flagged for sanctions hits, Travel Rule violations, high-risk countries (FATF grey/black list), or large amounts (>$500K USD)
- **Flag Rate**: 3.4% — consistent with real-world AML systems

### Gold — SAR Report Generation
- Only flagged transactions promoted to Gold layer
- Each flagged transaction assigned a **SAR severity level** (CRITICAL / HIGH / MEDIUM)
- Unique SAR reference numbers generated with timestamps
- Full audit trail preserved — every field traceable back to its source file and ingestion timestamp
- Reports are regulator-ready for FinCEN filing

---

## GNN Fraud Detection Model

| Metric | Score |
|--------|-------|
| **AUC** | 0.9792 |
| **Precision** | 0.8304 |
| **Recall** | 0.8944 |
| **F1 Score** | 0.8612 |
| **Accuracy** | 97% |

- **Architecture**: GraphSAGE-style 3-layer neural network (128 → 64 → 1) with batch normalization and dropout
- **Training Data**: Elliptic Bitcoin Dataset — 203,769 transactions, 234,355 edges, 166 features per node
- **Tracking**: All experiments, parameters, and model versions tracked in MLflow Model Registry
- **Deployment**: Registered model loaded from MLflow and used to score 110,000+ transactions with fraud risk scores (0.0 to 1.0)

---

## Data Quality Framework

23 automated data quality checks across all three pipeline layers:

**Bronze Layer:**
- Record count validation
- Null checks on all required fields (transaction_id, sender_name, amount, currency, etc.)
- Duplicate detection on transaction_id
- Positive amount validation
- Ingestion timestamp presence

**Silver Layer:**
- Record count consistency with Bronze
- USD conversion completeness
- Sanctions status population
- Travel Rule status population
- Realistic flag rate validation (1-10%)

**Gold Layer:**
- Gold count matches Silver flagged count
- SAR reference uniqueness
- Severity level completeness
- Report status verification

---

## CI/CD Pipeline

GitHub Actions runs automatically on every push to `main`:

1. **Python 3.11 Setup** — consistent environment
2. **Dependency Installation** — pytest and required libraries
3. **32 Unit Tests** covering:
   - OFAC sanctions screening logic (8 tests)
   - FATF Travel Rule validation (6 tests)
   - USD currency conversion (7 tests)
   - Transaction generator output (7 tests)
   - Data schema validation (4 tests)
4. **Project Structure Verification** — ensures all required directories exist

---

## Real-World Data Accumulation

Unlike static portfolio projects, SentinelFlow generates **fresh transaction data on every pipeline run**:

```
Day 1:  100,000 original + 10,000 new = 110,000 total
Day 2:  110,000 + 10,000 new          = 120,000 total
Day 3:  120,000 + 10,000 new          = 130,000 total
...
```

- Auto Loader's persistent checkpoint tracks which files have been processed
- Each run generates a new batch file named with today's date (e.g., `batch_20260629.json`)
- Bronze table grows continuously — data is never deleted
- Silver and Gold tables are rebuilt from the full Bronze on each run
- Dashboard metrics update on refresh to reflect the latest pipeline run

---

## Dashboard

The Databricks SQL compliance dashboard provides real-time visibility:

- **Counter Tiles**: Total transactions, flagged count, flag rate %
- **SAR Severity Chart**: Breakdown of CRITICAL / HIGH / MEDIUM alerts
- **Flag Reason Pie Chart**: Travel Rule violations vs High-risk country flags
- **Fraud Score Distribution**: Histogram of GNN risk scores across all transactions
- **Live SAR Table**: Top 50 flagged transactions with SAR reference, severity, sender, amount, and fraud score

---

## Design Decisions & Trade-offs

| Decision | Rationale |
|----------|-----------|
| **Auto Loader over Kafka** | Databricks Free Edition cannot connect to external Kafka. Auto Loader is the production-standard ingestion pattern on Databricks and handles incremental file processing natively. In production, cloud-managed Kafka (Confluent/MSK) would feed into Auto Loader. |
| **Unity Catalog over DBFS** | DBFS is disabled on Free Edition. Unity Catalog provides superior data governance, lineage tracking, and is the modern standard at financial institutions. |
| **Exact OFAC matching** | Partial/substring matching caused false positives on common name fragments (e.g., "Al-Rashid"). Exact matching eliminates false positives while catching intentionally injected sanctioned names. In production, fuzzy matching with confidence scores would be added. |
| **$500K large transaction threshold** | Initial $50K threshold flagged 30% of normal business payments. $500K aligns with real-world SAR filing thresholds and produces a realistic 3.4% flag rate. |
| **Elliptic Bitcoin dataset for GNN** | Only publicly available labeled transaction graph dataset. Graph topology patterns (layering, structuring) are domain-agnostic. In production, the model would be retrained on proprietary bank transaction data. |
| **Silver overwrite / Bronze append** | Bronze is append-only (regulatory requirement — never delete transaction records). Silver is rebuilt from full Bronze each run to ensure all transactions are screened against the latest OFAC list and exchange rates. |
| **Timestamp-based checkpoints** | Fixed checkpoint paths caused data duplication across runs. Persistent checkpoints with unique paths ensure Auto Loader correctly tracks processed files. |

---

## Project Structure

```
sentinelflow-aml-pipeline/
├── .github/workflows/
│   └── ci.yml                          # GitHub Actions CI/CD pipeline
├── data_generator/
│   └── transaction_generator.py        # ISO 20022 transaction simulator (Kafka version)
├── databricks/
│   ├── bronze/
│   │   └── 01_bronze_ingestion.ipynb   # Auto Loader → Bronze Delta table
│   ├── silver/
│   │   └── 02_silver_enrichment.ipynb  # OFAC + Travel Rule + FX enrichment
│   ├── gold/
│   │   └── 03_gold_sar_reports.ipynb   # SAR report generation + audit trail
│   ├── ml/
│   │   ├── 05_gnn_fraud_model.ipynb    # GNN training on Elliptic dataset
│   │   └── 06_gnn_scoring.ipynb        # Model scoring (used in workflow)
│   ├── 04_scale_data_generator.ipynb   # Full pipeline — daily batch + Bronze/Silver/Gold
│   └── 07_data_quality_checks.ipynb    # 23 automated data quality checks
├── tests/
│   └── test_compliance.py              # 32 unit tests for compliance functions
├── apis/                               # API helper modules
├── config/
│   └── settings.py                     # Configuration
├── docs/                               # Documentation
├── docker-compose.yml                  # Kafka + Zookeeper (local dev)
├── .env                                # API keys (not committed)
├── .gitignore
├── LICENSE
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Docker Desktop
- Databricks account (Free Edition)
- GitHub account

### Setup
```bash
# Clone the repository
git clone https://github.com/Haritha1752/sentinelflow-aml-pipeline.git
cd sentinelflow-aml-pipeline

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install kafka-python faker requests python-dotenv pyspark pandas pytest

# Start Kafka (optional — for local development)
docker-compose up -d

# Run tests
pytest tests/test_compliance.py -v
```

### Databricks Setup
1. Create a Databricks Free Edition account
2. Create catalog: `aml_pipeline` → database: `transactions` → volumes: `raw_data`, `elliptic_data`
3. Upload the Elliptic Bitcoin dataset to `elliptic_data` volume
4. Import notebooks from the `databricks/` folder
5. Run notebook `04_scale_data_generator` to execute the full pipeline
6. Set up a Databricks Workflow with `04_scale_data_generator` → `06_gnn_scoring`

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Total transactions processed | 110,000+ (growing daily) |
| Flag rate | 3.4% |
| SAR reports generated | 3,696+ |
| GNN model AUC | 0.9792 |
| Data quality checks | 23 (all passing) |
| CI/CD unit tests | 32 (all passing) |
| Pipeline automation | Daily at 8:00 AM ET |
| Live API integrations | OFAC, ECB, FATF rules |

---

## Future Enhancements

- [ ] Confluent Cloud Kafka integration for true real-time streaming
- [ ] Fuzzy name matching for OFAC screening with confidence scores
- [ ] Great Expectations integration for advanced data quality
- [ ] Streamlit frontend for compliance officer UI
- [ ] Alerting via email/Slack when CRITICAL SAR is generated
- [ ] Scale to 10M+ transactions for production load testing
- [ ] Add OpenSanctions API for global watchlist coverage

---

## License

MIT License — see [LICENSE](LICENSE) for details.