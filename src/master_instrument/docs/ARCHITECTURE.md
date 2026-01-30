# Master-Instrument Architecture

## Overview

Master-Instrument is a **financial data warehouse** designed to ingest, transform, and serve financial instrument data (equities, market data, fundamentals) from external sources into a unified data model.

### Technology Stack

| Component | Technology | Role |
|-----------|------------|------|
| **Orchestration** | Dagster | Scheduling, dependencies, monitoring |
| **Transformation** | DBT | SQL models (staging → intermediate → master) |
| **Ingestion** | Sling | Data replication from external sources |
| **ORM** | SQLAlchemy 2.0 | Master table definitions (golden source) |
| **Migrations** | Alembic | Database schema versioning |
| **Database** | PostgreSQL + TimescaleDB | Storage with time-series optimization |
| **Containerization** | Docker Compose | Service deployment |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL SOURCES                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │
│  │ Refinitiv   │    │    AWS S3   │    │   Seeds     │                      │
│  │   QA DB     │    │  (GICS,etc) │    │   (CSV)     │                      │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                      │
└─────────┼──────────────────┼──────────────────┼─────────────────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION (Sling)                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  raw.qa_*  │  raw.s3_*  │  seed.*  (reference data)                  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TRANSFORMATION (DBT)                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐             │
│  │   STAGING      │    │  INTERMEDIATE  │    │    MASTER      │             │
│  │   (stg_*)      │───▶│    (int_*)     │───▶│    (vw_*)      │             │
│  │                │    │                │    │                │             │
│  │ • stg_qa_co... │    │ • int_company  │    │ • vw_company   │             │
│  │ • stg_qa_eq... │    │ • int_equity   │    │ • vw_equity    │             │
│  │ • stg_qa_qu... │    │ • int_quote    │    │ • vw_market_*  │             │
│  │ • stg_s3_gi... │    │ • int_market_* │    │ • vw_fx_rate   │             │
│  └────────────────┘    └────────────────┘    └────────────────┘             │
│                                │                                             │
│                                │ Mapping external → internal IDs             │
│                                ▼                                             │
│                        ┌────────────────┐                                    │
│                        │ DATA QUALITY   │                                    │
│                        │   (dq_*)       │                                    │
│                        │                │                                    │
│                        │ • dq_cdc_rec.. │                                    │
│                        │ • dq_instru... │                                    │
│                        │ • dq_table_... │                                    │
│                        └────────────────┘                                    │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     LOADING (Python Framework)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  Loading Configs: MergeConfig, UpsertConfig, CDCConfig, InsertConfig │   │
│  │  ───────────────────────────────────────────────────────────────────  │   │
│  │  Patterns:                                                            │   │
│  │  • Inheritance (Entity → Company, Instrument → Equity)                │   │
│  │  • Self-reference (Company.primary_company_id)                        │   │
│  │  • Mapping tables (external_id → internal_id)                         │   │
│  │  • Batch strategies (FixedInterval, VolumeBased)                      │   │
│  │  • CDC (Change Data Capture) with version tracking                    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     STORAGE (PostgreSQL + TimescaleDB)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        MASTER SCHEMA                                 │    │
│  │  ─────────────────────────────────────────────────────────────────   │    │
│  │  Reference Tables:          │  Time-Series (Hypertables):           │    │
│  │  • entity, company          │  • market_data (20 partitions)        │    │
│  │  • instrument, equity       │  • fx_rate (4 partitions)             │    │
│  │  • quote, venue             │  • company_market_cap                 │    │
│  │  • currency, country        │                                       │    │
│  │  • classification_*         │  Mapping Tables:                      │    │
│  │  • dividend, corpact_*      │  • entity_mapping                     │    │
│  │  • std_financial_*          │  • instrument_mapping                 │    │
│  │                             │  • quote_mapping, venue_mapping       │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION (Dagster)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  Sling       │  │  DBT         │  │  Loading     │  │Infrastructure│    │
│  │  Assets      │  │  Assets      │  │  Assets      │  │  Assets      │    │
│  │              │  │              │  │              │  │              │    │
│  │ • qa_repl    │  │ • stg_*      │  │ • reference  │  │ • seed_idx   │    │
│  │ • s3_repl    │  │ • int_*      │  │ • market     │  │ • analyze    │    │
│  │              │  │ • master_*   │  │ • fundamental│  │              │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│                                                                              │
│  Jobs: full_refresh, daily_incremental, cdc_sync                            │
│  Schedules: daily @ 06:00 UTC                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Data Flow

### 1. Ingestion (Sling)

Raw data is replicated from external sources to the `raw` schema:

| Source | Tables | Frequency |
|--------|--------|-----------|
| **Refinitiv QA** | `qa_DS2*`, `qa_RKD*` | Daily CDC |
| **AWS S3** | GICS classifications, themes | On-demand |
| **Seeds** | Reference data (CSV) | On schema change |

### 2. Transformation (DBT)

**Medallion** architecture with 3 layers:

#### Staging (`staging.*`)
- Raw data cleanup and standardization
- Addition of `data_source_id`, `entity_type_id`
- CDC deduplication with `DISTINCT ON`

#### Intermediate (`intermediate.*`)
- **ID resolution**: external → internal via mapping tables
- Business logic application
- Joins with reference tables (currency, country, etc.)

#### Master (`master.*`)
- **Final views** for consumption
- Progressive enrichment (market_data_base → _filled → _adjusted)

### 3. Loading (Python Framework)

The loading framework supports multiple patterns:

| Pattern | Use Case | Template |
|---------|----------|----------|
| **MERGE** | Full reconciliation with soft delete | `merge.sql.j2` |
| **UPSERT** | Fast insert or update | `upsert.sql.j2` |
| **INSERT** | Append-only (timeseries) | `insert.sql.j2` |
| **CDC** | Incremental changes | `cdc_changes.sql.j2` |

#### Idempotency

All templates use `IS DISTINCT FROM` to avoid "phantom updates":

```sql
WHEN MATCHED AND (
    tgt.column1 IS DISTINCT FROM src.column1 OR
    tgt.column2 IS DISTINCT FROM src.column2
)
THEN UPDATE SET ...
```

### 4. Performance Optimizations

#### TimescaleDB Hypertables
- `market_data`: 20 partitions by `quote_id`, 90-day chunks
- `fx_rate`: 4 partitions by `base_currency_id`, 90-day chunks
- Automatic compression after 90/180 days

#### Bulk Load Mode
```python
with bulk_load_mode(engine, schema, table,
                    disable_wal=True,
                    disable_autovacuum=True):
    # Load data without WAL overhead
```

---

## Docker Services

| Service | Port | Role |
|---------|------|------|
| `postgres` | 5432 | PostgreSQL + TimescaleDB (data warehouse) |
| `dagster-postgres` | 5433 | Dagster internal state |
| `dagster-webserver` | 3000 | Dagster UI |
| `dagster-daemon` | - | Background job processor |
| `dagster-code` | 4000 | gRPC code server |

---

## Key Architectural Decisions

### 1. Mapping Tables (External → Internal IDs)

Each entity has a mapping table for:
- Data provenance traceability
- Multi-source support (same external ID can exist at multiple providers)
- Decoupling between source IDs and internal IDs

### 2. Inheritance Pattern (Entity/Company, Instrument/Equity)

Specialized entities inherit from generic entities:
- `Company` extends `Entity` (FK: `company_id` → `entity.entity_id`)
- `Equity` extends `Instrument` (FK: `equity_id` → `instrument.instrument_id`)

Advantages:
- Extensible model for new asset types
- Polymorphic queries possible

### 3. CDC with Version Tracking

Timeseries tables use CDC for incremental updates:
- `sys_change_version` tracked in load tables (`market_data_load`, etc.)
- Watermark prevents reprocessing of same changes

### 4. Soft Delete

Deletions are handled via `deleted_at` timestamp:
- History preserved for audit
- Ability to "resurrect" records
- Compatible with temporal tables

---

## Code Structure

```
master-instrument/
├── src/master_instrument/
│   ├── assets/                 # Dagster assets
│   │   ├── dbt/               # DBT integration
│   │   ├── sling/             # Sling replication
│   │   ├── loading/           # Data loading assets
│   │   │   ├── reference.py   # Reference data (company, equity, etc.)
│   │   │   ├── market.py      # Market data timeseries
│   │   │   └── fundamental.py # Financial statements
│   │   └── infrastructure/    # Indexes, analyze
│   ├── models/                # SQLAlchemy ORM (48 models)
│   ├── loading/               # Loading framework
│   │   ├── loaders.py        # SimpleLoader, BatchLoader
│   │   ├── configs.py        # MergeConfig, UpsertConfig, etc.
│   │   ├── batching.py       # Batch strategies
│   │   └── templates/        # Jinja2 SQL templates
│   ├── resources/            # Dagster resources
│   └── definitions.py        # Main Dagster definitions
├── dbt_project/
│   ├── models/
│   │   ├── staging/          # stg_* models
│   │   ├── intermediate/     # int_* models
│   │   ├── master/           # vw_* views
│   │   └── data_quality/     # dq_* checks
│   ├── seeds/                # Reference data CSV
│   └── macros/               # SQL utilities
├── alembic/
│   └── versions/             # Database migrations
├── scripts/                  # Utility scripts
├── docker/                   # Docker configs
└── docs/                     # Documentation
```

---

## See Also

- [DATA_MODEL.md](DATA_MODEL.md) - Detailed data model
- [TESTING_AND_VALIDATION.md](TESTING_AND_VALIDATION.md) - Testing and validation report
- [Refinitiv QA Mapping Analysis](refinitiv/QA_MAPPING_ANALYSIS.md) - Source data mapping analysis
- [Dagster Documentation](https://docs.dagster.io/)
- [DBT Documentation](https://docs.getdbt.com/)
- [TimescaleDB Documentation](https://docs.timescale.com/)
