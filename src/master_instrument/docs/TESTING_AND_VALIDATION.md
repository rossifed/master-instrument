# Testing and Validation Report

## Master-Instrument Data Warehouse

**Test Period**: January 5-19, 2026
**Author**: F. Rossi
**Version**: 1.0

---

## 1. Context and Objectives

### 1.1 Objective

Stabilization and validation of the data loading pipeline from Refinitiv QA sources to the master data warehouse. This phase includes:

- Reconstruction of the target data model with SQLAlchemy/Alembic
- Migration from redundant SQL scripts to Jinja2 templates (DRY principle)
- Centralization of business logic in DBT intermediate views
- Load testing on full production volume
- Data integrity and consistency validation

### 1.2 Pre-Analysis

Prior to implementation, a detailed analysis of the Refinitiv QA schema was conducted to understand mapping constraints and define the appropriate data model granularity.

**See**: [Refinitiv QA Mapping Analysis](refinitiv/QA_MAPPING_ANALYSIS.md) (November 2025)

**Key decisions from analysis**:
- Use `InfoCode` (DS2CtryQtInfo) as instrument-level pivot
- Use `RKDFndCmpRef` as primary company source
- 23 mapping exceptions identified and reported to Refinitiv

### 1.3 Test Environments

| Environment | Infrastructure | Usage |
|-------------|----------------|-------|
| **Local** | Docker Compose (MacOS/Linux) | Development, unit tests, limited volume |
| **Remote Server** | Hetzner VM | Load testing, full production volume, final validation |

### 1.4 Production Volume Tested

| Entity | Volume |
|--------|--------|
| Companies | 138,000 |
| Instruments | 60,000 |
| Quotes | ~350,000 |
| Market Data | 500,000,000 rows |
| Financial Values | > 1,000,000,000 rows |

---

## 2. Architecture and Data Model

### 2.1 Master Schema Reconstruction

**Status**: ✅ Completed

The `master` schema was fully reconstructed with:
- **SQLAlchemy 2.0**: 48 ORM models with type hints
- **Alembic**: 6 versioned migrations
- **TimescaleDB**: Hypertables for `market_data` and `fx_rate`

### 2.2 Constraints and Indexes Review

**Status**: ✅ Completed (evolving)

| Aspect | Validation |
|--------|------------|
| Primary Keys | Verified on all tables |
| Foreign Keys | Cascades configured (CASCADE, RESTRICT, SET NULL) |
| Unique Constraints | Natural keys defined for idempotency |
| Indexes | Filtered indexes, composite indexes, documented strategy |

### 2.3 Financial Value Denormalization

**Status**: ✅ Implemented

The `std_financial_value` table was denormalized to improve direct query performance:

**Denormalized columns**:
- `company_id`, `period_type_id`, `period_end_date`, `filing_end_date`, `statement_type_id`, `calendar_end_date`

**Trade-offs identified**:

| Advantage | Disadvantage |
|----------|--------------|
| Direct queries without multi-level JOINs | More columns to maintain |
| Optimized indexes for cross-company signals | Full load time impacted |
| | Requires FK constraint removal before batch load |

**Mitigations**:
- Constraint management before/after load in framework
- Index optimization on raw tables
- Adapted batch loading strategy

---

## 3. Load Testing

### 3.1 Loading Strategy

**Evolution**: Migrated from `DROP/CREATE` to `TRUNCATE` to preserve indexes on small tables.

| Strategy | Tables | Reason |
|-----------|--------|--------|
| TRUNCATE + INSERT | Reference data, small dimensions | Preserves indexes, avoids view rebuild |
| DROP indexes + bulk INSERT + recreate | Large timeseries (market_data, financial_value) | Optimal performance on large volumes |
| CDC (Change Data Capture) | Incremental updates | Differential loading |

### 3.2 Incremental Load Times (Daily Operations)

> **Environment**: Hetzner VM with indexes and tables already created and populated.
> These timings are representative of daily update operations in production.

#### RAW Layer (from Refinitiv QA)

| Job | Duration | Assets | First Asset |
|-----|----------|--------|-------------|
| Reference Data | 0:09:02 | 23 | `raw / qa / reference / qa_ds2company` |
| Market Data (incremental) | 0:02:05 | 3 | `raw / qa / market / qa_ds2fxrate` |
| Fundamental Data | 0:13:00 | 5 | `raw / qa / fundamental / qa_rkdfndstdfinval_changes` |

**RAW Reference Assets (23)**:
```
qa_ds2company, qa_ds2security, qa_ds2ctryqtinfo, qa_rkdfndcmprefissue,
qa_permorgref, qa_permquoteref, qa_perminstrref, qa_permorginfo,
qa_rkdfndcmpfiling, qa_rkdfndcmpphone, qa_rkdfndcmpdet, qa_rkdfndcmpref,
qa_rkdfndinfo, qa_rkdfndcmpweblink, qa_ds2exchange, qa_ds2exchqtinfo,
qa_ds2xref, qa_rkdfndcode, qa_ds2adj, qa_ds2capevent, qa_ds2div,
qa_ds2fxcode, qa_ds2numshares
```

**RAW Market Assets (3)** (incremental):
```
qa_ds2fxrate, qa_ds2primqtprc_changes, qa_ds2mktval_changes
```

**RAW Fundamental Assets (5)** (incremental):
```
qa_rkdfndstdfinval_changes, qa_rkdfndstditem, qa_rkdfndstdperiod,
qa_rkdfndstdperfiling, qa_rkdfndstdstmt
```

#### Master Layer (from RAW)

| Job | Duration | Assets | First Asset |
|-----|----------|--------|-------------|
| Reference | 0:01:20 | 25 | `master / reference / classification_schemes` |
| Market Data (incremental) | 0:00:58 | 3 | `master / market / fx_rates` |
| Fundamental | 0:04:30 | 6 | `master / fundamental / financial_period_type` |

**Master Reference Assets (25)**:
```
classification_schemes, classification_levels, classification_nodes,
countries, currencies, regions, country_regions, venue_types, venues,
instrument_types, equity_types, entity_types, data_sources,
dividend_types, corpact_types, weblink_types,
companies, equities, quotes, entity_classifications,
corpact_adjustments, corpact_events, dividends, dividend_adjustments,
company_weblinks, share_outstandings
```

**Master Market Assets (3)** (incremental):
```
fx_rates, market_data_changes, company_market_cap_changes
```

**Master Fundamental Assets (6)** (incremental):
```
financial_period_type, financial_statement_type,
std_financial_filing, std_financial_statement, std_financial_item,
std_financial_values_changes
```

### 3.3 Full Load Times (Initial Load / Rebuild)

> **Environment**: Hetzner VM.
> Timings measured with indexes dropped, foreign key constraints removed, and WAL disabled.
> These optimizations are required for acceptable bulk load performance.

#### RAW Layer (from Refinitiv QA)

| Job | Duration (approx.) | Volume |
|-----|--------------------|--------|
| Market Data (ds2primqtprc, ds2mktval) | ~2 hours | 500M rows |
| Financial Values (rkdfndstdfinval) | ~4 hours | >1B rows |

#### Master Layer (from RAW)

| Job | Duration (approx.) | Volume |
|-----|--------------------|--------|
| Market Data + Market Cap | ~1:50 | 500M rows |
| Financial Values | ~3:50 | >1B rows |

> **Important**: These timings do not include post-load operations:
> - Index recreation
> - Foreign key constraint restoration
> - WAL re-enablement
> - ANALYZE/VACUUM operations
>
> For `std_financial_value`, index recreation alone takes approximately **1 hour**.

**Observations**:
- First loads after DB construction are slower (no cache, indexes not populated)
- Once indexes and cache are acquired, successive loads are faster
- Performance sometimes variable depending on server state

### 3.4 Batch Size Optimization

| Table | Batch Strategy | Configuration |
|-------|----------------|---------------|
| Market Data | FixedIntervalStrategy | 90 days per batch |
| Financial Values | VolumeBasedStrategy | ~100,000 rows per batch |
| Hypertables | Compressed chunks | max_tuples_decompressed: 2,000,000 |

**Note**: TimescaleDB hypertables decompress chunks in memory, requiring mandatory batch management.

---

## 4. Bugs Identified and Resolved

### 4.1 Phantom History Records

**Commit**: `c130d8c`, `f0fc253`, `fddadb5` (January 16, 2026)

| Aspect | Detail |
|--------|--------|
| **Symptom** | History tables (temporal tables) growing without actual data modification |
| **Root Cause** | UPDATE executed even when values were identical, triggering temporal triggers |
| **Detection** | Observation of abnormal history table growth after successive loads |
| **Solution** | Added `IS DISTINCT FROM` in MERGE/UPSERT templates |
| **Validation** | Successive loads without history table growth |

**Code fix**:
```sql
WHEN MATCHED AND (
    tgt.column1 IS DISTINCT FROM src.column1 OR
    tgt.column2 IS DISTINCT FROM src.column2
)
THEN UPDATE SET ...
```

### 4.2 Financial Value Join Performance

**Commit**: `c78a1a9` (January 16, 2026)

| Aspect | Detail |
|--------|--------|
| **Symptom** | Load time of 1 minute per batch instead of a few seconds |
| **Root Cause** | Bad join in intermediate view impacting execution plan |
| **Detection** | Observation of batch times in Dagster logs |
| **Solution** | Corrected join order and type |
| **Validation** | Time reduced from **1 min → 3 sec** per batch |

### 4.3 Company Market Cap Mapping

**Commit**: `e6b8c7f` (January 15, 2026)

| Aspect | Detail |
|--------|--------|
| **Symptom** | Inconsistent market cap values |
| **Root Cause** | `mktval` field incorrectly mapped to `company_id` |
| **Detection** | Manual comparison with Yahoo Finance |
| **Solution** | Corrected mapping in staging view |
| **Validation** | Comparison with external sources (Yahoo, Fundy) |

### 4.4 is_primary Incorrectly Replicated

**Detection**: Via SQL data quality script (`dq_instruments_without_primary_quote`)

| Aspect | Detail |
|--------|--------|
| **Symptom** | Instruments without primary quote detected |
| **Root Cause** | `is_primary` field incorrectly returned in staging view |
| **Solution** | Corrected field mapping |
| **Validation** | DQ script returns 0 anomalies |

### 4.5 Duplicate InfoCode per RKD Company

**Commit**: `068d9fe` (January 19, 2026)

| Aspect | Detail |
|--------|--------|
| **Symptom** | Duplicates in company_market_cap table |
| **Root Cause** | Multiple InfoCode for same RKD Company |
| **Detection** | Uniqueness constraint violated during load |
| **Solution** | Deduplication in staging view |
| **Validation** | Load without constraint error |

### 4.6 Index Restore for Hypertables

**Commit**: `068d9fe` (January 19, 2026)

| Aspect | Detail |
|--------|--------|
| **Symptom** | Error when recreating indexes on hypertables |
| **Root Cause** | `CREATE INDEX CONCURRENTLY` not supported on TimescaleDB hypertables |
| **Solution** | Differentiated handling: concurrent mode for normal tables, standard mode for hypertables |
| **Validation** | Full load without error |

---

## 5. Performance Optimization

### 5.1 Staging Views

**Method**: Execution plan analysis (`EXPLAIN ANALYZE`)

**Optimizations applied**:

| Optimization | Impact |
|--------------|--------|
| JOIN order | Significant execution time reduction |
| MATERIALIZED CTE in `stg_qa_quote` | Avoids re-execution of expensive subqueries |
| Column selection for future joins | Optimizes downstream joins |
| Inline views vs CTEs | Variable impact depending on case |

### 5.2 Intermediate Views

**Observations**:
- Performance heavily depends on index state and PostgreSQL cache
- First loads after DB reconstruction significantly slower
- Performance stabilized after index population

### 5.3 Index Strategy

**Actions**:
- Review and cleanup of redundant indexes on raw and master
- Creation of Dagster assets for infrastructure maintenance:
  - `seed_indexes`: Raw index recreation
  - `analyze_assets`: Forced ANALYZE on raw/seed/master tables

**Attention point**:
> ⚠️ Index migration management to be clarified. If ownership in Alembic and manual creation/deletion → risk of inconsistency.

### 5.4 TimescaleDB Hypertables

| Table | Partitions | Chunk Interval | Compression Policy |
|-------|------------|----------------|-------------------|
| market_data | 20 (quote_id) | 90 days | > 90 days |
| fx_rate | 4 (base_currency_id) | 90 days | > 180 days |

---

## 6. Data Validation

### 6.1 SQL Validation Scripts

**Location**: `scripts/validation/`

| Script | Function | Execution Frequency |
|--------|----------|---------------------|
| `coverage_checks.sql` | Volume checks, mapping coverage, type coherence, primary quote uniqueness | After each major load |
| `validate_cdc_changes.sql` | CDC temp tables vs master comparison | After CDC loads |
| `snapshot_table_counts.sql` | Row count snapshot for before/after comparison | Before and after each run |

### 6.2 Manual Comparisons

**Reference instruments tested**: Alphabet (GOOGL) and other representative companies

| Validation | Comparison Source | Period | Result |
|------------|-------------------|--------|--------|
| Market Data (OHLCV) | Yahoo Finance, Refinitiv Workbench | 10 days + history since 2000 | ✅ OK |
| Market Cap | Yahoo Finance, Fundy | Full history | ✅ OK |
| Adjusted Market Data (corpact) | Refinitiv Workbench | Instruments with splits/consolidations | ✅ OK |
| Financial Values | Raw data source | Full history | ✅ OK |

### 6.3 Swiss Universe Validation

| Test | Scope | Period | Result |
|------|-------|--------|--------|
| Financial Values vs Raw | All Swiss companies | Last 25 years | ✅ OK |

**Note**: Not re-tested after financial value normalization correction.

### 6.4 Structural Coherence Validation

| Validation | Method | Result |
|------------|--------|--------|
| Mapping tables + entities coherent after successive loads | SQL scripts + observation | ✅ OK |
| History tables stable if no modification | Row count comparison before/after | ✅ OK (after IS DISTINCT FROM fix) |
| Instruments/companies vs raw (fields and counts) | Comparison queries | ✅ OK |

---

## 7. Automated Tests

### 7.1 Python Unit Tests

**Location**: `tests/unit/`, `tests/integration/`

**Coverage**: ~30 test files written during this period

| Module | Test Files | Coverage |
|--------|------------|----------|
| Loading configs | `test_configs.py` | MergeConfig, UpsertConfig, CDCConfig, BatchConfig |
| Loaders | `test_loaders.py` | SimpleLoader, BatchLoader |
| Batching | `test_batching.py` | FixedIntervalStrategy, VolumeBasedStrategy |
| SQL Templates | `test_merge_core.py`, `test_upsert_core.py`, `test_insert_core.py` | Correct SQL generation |
| Inheritance | `test_inheritance.py`, `test_upsert_inheritance.py` | Entity→Company pattern |
| Mapping | `test_mapping.py` | Mapping column auto-detection |
| Self-reference | `test_self_reference.py` | Circular FK resolution |
| Database utils | `test_database.py`, `test_tables.py`, `test_columns.py` | DB utilities |
| Integration | `test_end_to_end_validation.py`, `test_template_rendering.py` | Full validation |

### 7.2 DBT Tests

**Location**: `dbt_project/tests/`

| Test | Function |
|------|----------|
| `assert_single_primary_quote_per_instrument.sql` | Each instrument has exactly 1 primary quote |
| `assert_quote_currency_matches_latest_market_data.sql` | Currency consistency quote vs market data |
| `assert_currency_stability.sql` | Currency stability |
| `test_company_market_cap_snapshot.sql` | Market cap validation |

### 7.3 Data Quality Models

**Location**: `dbt_project/models/data_quality/`

| Model | Function |
|-------|----------|
| `dq_instruments_without_quote` | Instruments without quote |
| `dq_instruments_without_primary_quote` | Instruments without primary quote |
| `dq_instruments_multiple_primary_quotes` | Instruments with multiple primary quotes |
| `dq_entities_without_instrument` | Orphaned entities |
| `dq_companies_without_filing` | Companies without filing |
| `dq_quotes_without_market_data` | Quotes without market data |
| `dq_cdc_reconciliation` | CDC watermark reconciliation |
| `dq_table_row_counts` | Dynamic row counts |
| `dq_table_indexes` | Index status |
| ... | Other checks |

**Note**: Some tests on large timeseries not enabled due to performance. Sampling required.

---

## 8. Architectural Improvements

### 8.1 SQL Scripts → Jinja2 Templates Migration

**Objective**: DRY (Don't Repeat Yourself)

**Before**: Redundant SQL scripts per table
**After**: Composable templates (`merge.sql.j2`, `upsert.sql.j2`, `insert.sql.j2`, `cdc_changes.sql.j2`)

**Benefits**:
- Centralized maintenance
- Reusable patterns (inheritance, mapping, self-reference)
- Unit tests on SQL generation

### 8.2 Logic Centralization in DBT Intermediate

**Objective**: Testable and auditable business logic

**Before**: Logic scattered across Python scripts and SQL
**After**: `int_*` views centralize:
- ID resolution (external → internal)
- Joins with reference tables
- Business transformations

**Benefits**:
- Easily testable with DBT tests
- Auditable (versioned SQL)
- Clear separation: staging (cleanup) vs intermediate (logic)

---

## 9. Open Items (TODO)

### 9.1 Query/Signal Performance

| Issue | Status | Notes |
|-------|--------|-------|
| Market data performance in signal construction | 🔴 To investigate | Index optimization or pre-aggregation |
| Financial value query performance | 🔴 To investigate | Denormalization helps but may be insufficient |

### 9.2 Dividend Adjustment

| Issue | Status | Notes |
|-------|--------|-------|
| Dividend adjustment integration | 🟡 Partially tested | Join on hypertable very slow |
| Adjustment factors | ✅ Validated | Consistent with QA on test instruments |

### 9.3 Missing Data

| Element | Status |
|---------|--------|
| Estimates (IBES) | 🔴 Not integrated |
| TTM (Trailing Twelve Months) calculations | 🔴 Not implemented |

### 9.4 Forward-Fill Market Data

| Issue | Status | Notes |
|-------|--------|-------|
| `vw_market_data_filled` view underperforming | 🟡 Pending | Strategy to define: golden source raw or client responsible for fill |

### 9.5 FX Rate Gaps

| Issue | Status | Notes |
|-------|--------|-------|
| Price conversion with FX gaps | 🟡 To investigate | Fill strategy to define |

### 9.6 QA DS/RKD Mapping

| Issue | Status | Notes |
|-------|--------|-------|
| DS ↔ RKD mapping inconsistencies | 🟡 Reported to Refinitiv | Can cause missing data or inconsistencies. Ticket in progress. |

**See**: [QA Mapping Analysis](refinitiv/QA_MAPPING_ANALYSIS.md) for detailed analysis.

---

## 10. Appendix: Commits During Period

### Week 1 (January 5-11, 2026)

| Date | Commit | Description |
|------|--------|-------------|
| 01/05 | `9e783a4` | Fix statement type, period type mappings |
| 01/05 | `6d2f9d9` | Fix volume based batching (gaps between dates) |
| 01/05 | `e3922bf` | Work in progress change tracking, testing volume based strategy |
| 01/06 | `15e38bb` | Centralize logic in DBT macro, move to intermediate, batching pipeline |
| 01/07 | `e8e9cdd` | Fundamental: integrating intermediate models |
| 01/07 | `a985849` | Reference: moving load queries to intermediate |
| 01/07 | `5adc503` | Market data: using intermediate models, new batch pipeline |
| 01/07 | `51e4b12` | Remove begin/commit (transaction managed by SQLAlchemy) |
| 01/07 | `d57203e` | Align company market cap to new pipeline |
| 01/08 | `81df3a2` | Using truncate instead of full-refresh to maintain indexes |
| 01/08 | `a68ad91` | Add template for load query building, fix FX coverage |
| 01/09 | `b524fda` | Jinja templates for loading SQL, unit testing, audit columns, soft delete |
| 01/09 | `f049a9a` | Testing assets based on Jinja templates |
| 01/09 | `7706442` | Testing templates and bug fixes |

### Week 2 (January 12-19, 2026)

| Date | Commit | Description |
|------|--------|-------------|
| 01/12 | `187fb89` | Finalize script generation from template, fix models, align DBT |
| 01/13 | `460898f` | Migrating template asset to master |
| 01/13 | `724f087` | Fix upsert, separate SQL generation, add corporate events, denormalize financial_value |
| 01/14 | `04c6aae` | Add type_id to mapping tables unique keys |
| 01/14 | `7748e6b` | Refactor mapping tables + complete asset pipeline |
| 01/14 | `9576074` | Merge PR #1: mapping-type-refactor |
| 01/14 | `92aa260` | Remove SQL load scripts and obsolete files |
| 01/14 | `ac5b603` | Unit testing corpact adjusted market data |
| 01/14 | `45afae3` | Add currency to market cap, align views, add DQ checks |
| 01/15 | `a155507` | Add missing types in DBT sources, currency in market cap migration |
| 01/15 | `e6b8c7f` | **Bug fix**: company market cap mapping |
| 01/15 | `0eea4d6` | Optimizing and aligning market_data views |
| 01/15 | `f2ca65c` | Rollback market data filled |
| 01/15 | `4c0e000` | Clean migration, remove French comments |
| 01/16 | `fddadb5` | **Fix**: prevent phantom history records |
| 01/16 | `c130d8c` | Add IS DISTINCT FROM to MERGE templates |
| 01/16 | `08ffaa9` | Remove "Test" word in asset definitions |
| 01/16 | `28c5c8a` | Remove useless columns on corpact events |
| 01/16 | `e2c1df8` | Bug fix on CDC model |
| 01/16 | `c78a1a9` | **Fix**: bad join impacting finval performance (1min → 3sec) |
| 01/16 | `dbcffed` | Cleaning unwanted files and folders |
| 01/16 | `f0fc253` | Add IS DISTINCT FROM to UPSERT templates |
| 01/18 | `50c93eb` | Remove surrogate key on financial value, optimize staging, WAL deactivation |
| 01/18 | `5595d4b` | Seed indexes, ANALYZE assets, MATERIALIZED CTE, fix std_financial_filing unique_key |
| 01/18 | `d9a0f70` | Seed indexes, analyze assets, optimize views |
| 01/19 | `068d9fe` | **Fix**: duplicate InfoCode, regression on filtered financial item, non-concurrent index restore for hypertables |

---

## 11. Conclusion

This testing and validation phase achieved:

1. **Pipeline stabilization** with predictable and acceptable load times
2. **Identification and resolution** of 6 major bugs impacting data quality
3. **Performance optimization** (e.g., financial value join 1min → 3sec)
4. **Test infrastructure setup** (unit tests, DBT tests, data quality checks)
5. **Trade-off documentation** (denormalization, hypertables, batch strategies)

Loaded data was validated through:
- Manual comparison with external sources (Yahoo Finance, Refinitiv Workbench, Fundy)
- SQL validation scripts executed regularly
- Automated tests (Python + DBT)

**Points of attention for next steps**:
- Signal/query performance to investigate
- Full dividend adjustment integration
- QA DS/RKD mapping resolution (Refinitiv ticket in progress)
