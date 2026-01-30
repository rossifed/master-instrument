# Master-Instrument

Financial data warehouse for managing financial instruments (equities, market data, fundamentals) with Medallion architecture.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | [Dagster](https://dagster.io/) |
| Transformation | [DBT](https://www.getdbt.com/) |
| Ingestion | [Sling](https://slingdata.io/) |
| ORM | SQLAlchemy 2.0 |
| Database | PostgreSQL + TimescaleDB |
| Migrations | Alembic |

## Documentation

- **[Architecture](docs/ARCHITECTURE.md)** - Technical overview, data flows, components
- **[Data Model](docs/DATA_MODEL.md)** - Relational schema, tables, relationships

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### 1. Install Dependencies

**Option A: uv (recommended)**
```bash
uv sync
source .venv/bin/activate
```

**Option B: pip**
```bash
python3 -m venv .venv
source .venv/bin/activate  # MacOS/Linux
# .venv\Scripts\activate   # Windows
pip install -e ".[dev]"
```

### 2. Configuration

Copy the environment file and configure the variables:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 3. Full Initialization

```bash
./scripts/init.sh
```

This script performs:
1. Export environment variables
2. Start Docker containers
3. Wait for PostgreSQL to be ready
4. Run Alembic migrations (schema creation)
5. DBT seed (reference data)
6. DBT compile
7. Build and start Dagster

### 4. Access the Interface

Open http://localhost:3000 to access the Dagster UI.

---

## Project Structure

```
master-instrument/
├── src/master_instrument/      # Main Python code
│   ├── assets/                 # Dagster assets
│   │   ├── dbt/               # DBT integration
│   │   ├── sling/             # Sling replication
│   │   ├── loading/           # Data loading
│   │   └── infrastructure/    # Indexes, analyze
│   ├── models/                # SQLAlchemy ORM (48 tables)
│   ├── loading/               # Loading framework
│   └── resources/             # Dagster resources
├── dbt_project/               # DBT project
│   ├── models/
│   │   ├── staging/          # raw → staging transformation
│   │   ├── intermediate/     # Business logic, ID mapping
│   │   ├── master/           # Final views
│   │   └── data_quality/     # Quality checks
│   ├── seeds/                # Reference data (CSV)
│   └── macros/               # SQL utilities
├── alembic/                   # Schema migrations
├── scripts/                   # Utility scripts
├── config/                    # Sling configuration
└── docs/                      # Documentation
```

---

## Utility Scripts

### `scripts/init.sh` - Full Initialization

Initializes the complete environment from scratch:

```bash
./scripts/init.sh
```

**Actions performed:**
1. Export environment variables from `.env`
2. `docker compose up -d` - Start services
3. Wait for PostgreSQL to be ready (healthcheck)
4. `alembic upgrade head` - Apply migrations
5. `alembic revision --autogenerate` - Detect changes
6. `alembic upgrade head` - Apply new migrations
7. Clear DBT cache (`dbt_project/target`)
8. `dbt seed` - Load reference data
9. `dbt compile` - Compile models
10. `docker compose build dagster-code --no-cache` - Rebuild image
11. `docker compose up -d` - Restart services

### `scripts/build_run.sh` - Quick Rebuild

Recompile DBT and restart Dagster (without full reinitialization):

```bash
./scripts/build_run.sh
```

**Actions performed:**
1. Clear DBT cache
2. `dbt compile` - Recompile
3. `docker compose build dagster-code` - Rebuild image
4. `docker compose up -d dagster-code` - Restart

**Use when:** Modifying DBT models or Python code.

### `scripts/export_env.sh` - Export Variables

Load environment variables from `.env`:

```bash
source scripts/export_env.sh
```

**Use for:** Running local commands (alembic, dbt, pytest) with the correct variables.

---

## SQL Validation Scripts

Scripts in `scripts/validation/` allow manual data validation (via DBeaver or psql).

### `scripts/validation/coverage_checks.sql`

Verifies coverage and consistency of master data:

| Check | Description |
|-------|-------------|
| Volume checks | Minimum expected counts (companies ≥ 100K, instruments ≥ 60K) |
| Mapping coverage | 1:1 between tables and mapping tables |
| Type coherence | company = entity(CMPY), equity = instrument(EQU) |
| Primary quote uniqueness | Each instrument has exactly 1 primary quote |
| Market data presence | Primary quotes have price data |

```sql
-- Example result
entity                    | check_name                          | status
companies                 | volume_check                        | OK
instruments              | volume_check                        | OK
entity vs entity_mapping  | mapping_coverage                    | OK
```

### `scripts/validation/validate_cdc_changes.sql`

Validates CDC workflow after incremental loading:

| Section | Description |
|---------|-------------|
| Market Data CDC | Compare `tmp_int_market_data_changes` vs `master.market_data` |
| Company Market Cap CDC | Compare CDC vs master for market cap |
| Std Financial Value CDC | Compare CDC vs master for financial data |

**Use after:** A CDC load to verify that data was correctly applied.

### `scripts/validation/snapshot_table_counts.sql`

Snapshot of row counts for all master tables:

```sql
-- Result
table_name              | row_count | snapshot_time
entity                  | 156789    | 2025-01-19 10:30:00
company                 | 123456    | 2025-01-19 10:30:00
instrument              | 67890     | 2025-01-19 10:30:00
...
```

**Usage:**
1. Run before a job → save the result
2. Run after the job → compare
3. If counts change without new source data = problem

---

## Common Commands

### Docker

```bash
# Start services
docker compose up -d

# View logs
docker compose logs -f dagster-code

# Rebuild an image
docker compose build dagster-code

# Full rebuild (no cache)
docker compose build dagster-code --no-cache

# Service status
docker compose ps

# Stop all
docker compose down
```

### DBT

```bash
# Compile models
dbt compile --project-dir dbt_project --profiles-dir dbt_project

# Load seeds
dbt seed --project-dir dbt_project --profiles-dir dbt_project

# Run models
dbt run --project-dir dbt_project --profiles-dir dbt_project

# Run a specific model
dbt run --select stg_qa_company --project-dir dbt_project --profiles-dir dbt_project

# Tests
dbt test --project-dir dbt_project --profiles-dir dbt_project
```

### Alembic

```bash
# Apply migrations
alembic upgrade head

# Create a new migration
alembic revision --autogenerate -m "description"

# View history
alembic history

# Downgrade
alembic downgrade -1
```

### Dagster

```bash
# Development mode (local)
dg dev

# Or via dagster directly
dagster dev -m master_instrument.definitions
```

---

## Data Architecture

```
Sources (Refinitiv QA, S3)
         │
         ▼
    ┌─────────┐
    │  Sling  │  Replication → raw.*
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │   DBT   │  staging.* → intermediate.* → master views
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ Loading │  Python framework → master tables
    └────┬────┘
         │
         ▼
    ┌─────────────────────┐
    │ PostgreSQL/Timescale│  master.* (golden source)
    └─────────────────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for more details.

---

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `PG_HOST` | PostgreSQL host | ✓ |
| `PG_PORT` | PostgreSQL port | ✓ |
| `PG_USER` | PostgreSQL user | ✓ |
| `PG_PASSWORD` | PostgreSQL password | ✓ |
| `PG_DATABASE` | PostgreSQL database name | ✓ |
| `REFERENTIAL_POSTGRES_CONN` | Full connection string | ✓ |
| `QA_HOST` | Refinitiv QA host | For ingestion |
| `QA_PASSWORD` | Refinitiv QA password | For ingestion |
| `S3_ACCESS_KEY_ID` | AWS S3 access key | For S3 |
| `S3_SECRET_ACCESS_KEY` | AWS S3 secret | For S3 |
| `TIMESERIES_START_DATE` | Timeseries start date | Default: 2000-01-01 |

---

## Tests

```bash
# All tests
pytest

# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# With coverage
pytest --cov=master_instrument
```

---

## Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [DBT Documentation](https://docs.getdbt.com/)
- [TimescaleDB Documentation](https://docs.timescale.com/)
- [SQLAlchemy 2.0 Documentation](https://docs.sqlalchemy.org/)
- [Alembic Documentation](https://alembic.sqlalchemy.org/)
