# Master-Instrument Data Model

## Overview

The `master` schema contains **48 tables** organized into several functional domains. All tables are defined via SQLAlchemy ORM in `src/master_instrument/models/`.

---

## Entity-Relationship Diagram

### Core Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CORE HIERARCHY                                     │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────┐
                              │  EntityType  │
                              │ (CMPY, etc.) │
                              └──────┬───────┘
                                     │ 1
                                     │
                                     ▼ *
┌──────────────┐             ┌──────────────┐              ┌──────────────┐
│EntityMapping │◄────────────│    Entity    │─────────────▶│   Country    │
│              │   1:1       │              │              └──────────────┘
│ external_id  │             │  entity_id   │
│ internal_id  │             │  name        │
│ data_source  │             │  entity_type │
└──────────────┘             └──────┬───────┘
                                   │
                   ┌───────────────┼───────────────┐
                   │               │               │
                   ▼               ▼               ▼
            ┌──────────┐   ┌────────────┐   ┌──────────┐
            │ Company  │   │ Instrument │   │  Venue   │
            │ (1:1 FK) │   │  (1:1 FK)  │   │ (1:1 FK) │
            └────┬─────┘   └─────┬──────┘   └──────────┘
                 │               │
                 │               ▼
                 │        ┌──────────┐
                 │        │  Equity  │
                 │        │ (1:1 FK) │
                 │        └────┬─────┘
                 │             │
                 ▼             ▼
          ┌───────────────────────────────────────┐
          │              Quote                     │
          │  instrument_id + venue_id (unique)    │
          └───────────────────────────────────────┘
                          │
                          ▼
          ┌───────────────────────────────────────┐
          │           MarketData                  │
          │  (trade_date, quote_id) - Hypertable  │
          └───────────────────────────────────────┘
```

### Financial Relationships

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FINANCIAL DATA                                       │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│   Company    │
└──────┬───────┘
       │
       │ 1:*
       ▼
┌─────────────────────┐      ┌─────────────────────┐
│ StdFinancialFiling  │      │ FinancialPeriodType │
│                     │◄─────│   (Q, A, S, etc.)   │
│  company_id         │      └─────────────────────┘
│  period_type_id     │
│  period_end_date    │      ┌─────────────────────┐
│  filing_end_date    │◄─────│     Currency        │
└──────────┬──────────┘      │ (reported/converted)│
           │                 └─────────────────────┘
           │ 1:*
           ▼
┌─────────────────────┐      ┌─────────────────────────┐
│StdFinancialStatement│◄─────│ FinancialStatementType  │
│                     │      │  (INC, BAL, CAS, etc.)  │
│  filing_id          │      └─────────────────────────┘
│  statement_type_id  │
└──────────┬──────────┘
           │
           │ 1:*
           ▼
┌─────────────────────┐      ┌─────────────────────┐
│ StdFinancialValue   │◄─────│  StdFinancialItem   │
│                     │      │                     │
│  statement_id       │      │  item_id            │
│  item_id            │      │  name               │
│  value              │      │  statement_type     │
│  (denormalized keys)│      └─────────────────────┘
└─────────────────────┘
```

### Corporate Actions & Dividends

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CORPORATE ACTIONS & DIVIDENDS                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│    Equity    │
└──────┬───────┘
       │
       ├───────────────────────────────────────────────────────┐
       │                                                       │
       ▼                                                       ▼
┌──────────────────┐  ┌────────────────────┐   ┌──────────────────┐  ┌────────────────────┐
│    Dividend      │  │ DividendAdjustment │   │  CorpactEvent    │  │ CorpactAdjustment  │
│                  │  │                    │   │                  │  │                    │
│  equity_id       │  │  equity_id         │   │  equity_id       │  │  equity_id         │
│  dividend_type   │  │  ex_div_date       │   │  corpact_type    │  │  adj_date          │
│  dividend_rate   │  │  div_adj_factor    │   │  effective_date  │  │  adj_factor        │
│  effective_date  │  │  cum_div_factor    │   │  new/old_shares  │  │  cum_adj_factor    │
└──────────────────┘  └────────────────────┘   └──────────────────┘  └────────────────────┘
       │                                              │
       ▼                                              ▼
┌──────────────────┐                          ┌──────────────────┐
│  DividendType    │                          │   CorpactType    │
│  (REG, SPE, etc.)│                          │  (SPL, REV, etc.)│
└──────────────────┘                          └──────────────────┘
```

---

## Tables by Domain

### 1. Base Entities

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `entity` | `entity_id` (auto) | Generic entity (company, venue) | → entity_type |
| `entity_type` | `entity_type_id` | Entity types (CMPY) | |
| `entity_mapping` | `internal_entity_id` | External → internal mapping | → entity, data_source |

### 2. Companies

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `company` | `company_id` (FK → entity) | Company extension of entity | → entity, country, currency |
| `company_weblink` | `company_weblink_id` | Website URLs | → company, weblink_type |
| `company_market_cap` | `(valuation_date, company_id)` | Market cap timeseries | → company, currency |

**Key columns in `company`:**
- `employee_count`, `public_since`, `total_shares_outstanding`
- `estimates_currency_id`, `statements_currency_id`
- `ultimate_organization_id`, `primary_company_id` (self-references)
- `organization_id` (Refinitiv reference)

### 3. Instruments & Equities

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `instrument` | `instrument_id` (auto) | Generic instrument | → entity, instrument_type |
| `instrument_type` | `instrument_type_id` | Types (EQU, BND, FUT, etc.) | |
| `instrument_mapping` | `internal_instrument_id` | External → internal mapping | → instrument, data_source |
| `equity` | `equity_id` (FK → instrument) | Equity extension | → instrument, equity_type, country |
| `equity_type` | `equity_type_id` | Equity types | |
| `share_outstanding` | `(date, equity_id)` | Shares outstanding timeseries | → equity |

**Key columns in `equity`:**
- Identifiers: `isin`, `cusip`, `sedol`, `ric`, `ticker`
- `security_id` (Refinitiv internal)
- `is_major_security`, `delisted_date`
- `split_date`, `split_factor`

### 4. Quotes & Market Data

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `venue` | `venue_id` (auto) | Exchange/marketplace | → venue_type, country |
| `venue_type` | `venue_type_id` | Venue types (EXCH) | |
| `venue_mapping` | `internal_venue_id` | External → internal mapping | → venue, data_source |
| `quote` | `quote_id` (auto) | Instrument/venue listing | → instrument, venue, currency |
| `quote_mapping` | `internal_quote_id` | External → internal mapping | → quote, data_source |
| `market_data` | `(trade_date, quote_id)` | OHLCV timeseries (**Hypertable**) | → quote, currency |

**Key columns in `quote`:**
- `instrument_id`, `venue_id` (unique constraint)
- `is_primary` (boolean, filtered index)
- `ticker`, `ric`, `mic`, `market_name`
- `price_unit` (for bonds, options)

**Key columns in `market_data`:**
- OHLCV: `open`, `high`, `low`, `close`, `volume`
- Spreads: `bid`, `ask`, `vwap`
- `currency_id`, `loaded_at`

### 5. Financial Data

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `std_financial_filing` | `std_financial_filing_id` | Filing header | → company, period_type, currency |
| `std_financial_statement` | `std_financial_statement_id` | Statement within a filing | → filing, statement_type |
| `std_financial_value` | `(statement_id, item_id)` | Financial value | → statement, item, company |
| `std_financial_item` | `std_financial_item_id` | Item definition | → statement_type |
| `std_financial_item_mapping` | `internal_item_id` | External → internal mapping | → item, data_source |
| `financial_period_type` | `financial_period_type_id` | Q, A, S, etc. | |
| `financial_statement_type` | `financial_statement_type_id` | INC, BAL, CAS | |

**Note on `std_financial_value`:**
- Composite PK for MERGE operations
- Denormalized natural key: `(company_id, period_type_id, period_end_date, filing_end_date, statement_type_id, item_id)`
- Indexes optimized for cross-company queries

### 6. Corporate Actions & Dividends

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `dividend` | `dividend_id` | Dividend event | → equity, dividend_type, currency |
| `dividend_type` | `dividend_type_id` | REG, SPE, etc. | |
| `dividend_adjustment` | `(equity_id, ex_div_date)` | Adjustment factor | → equity, currency |
| `corpact_event` | `corpact_event_id` | Corporate action | → equity, corpact_type, res_equity |
| `corpact_type` | `corpact_type_id` | SPL, REV, etc. | |
| `corpact_adjustment` | `(equity_id, adj_date, adj_type)` | Adjustment factor | → equity, corpact_type |

### 7. Reference Data

| Table | PK | Description |
|-------|-----|-------------|
| `currency` | `currency_id` | ISO 4217 currencies |
| `currency_pair` | `currency_pair_id` | Currency pairs |
| `fx_rate` | `(rate_date, base_currency_id, quote_currency_id)` | FX rates (**Hypertable**) |
| `country` | `country_id` | ISO 3166-1 countries |
| `region` | `region_id` | Geographic regions |
| `country_region` | `(country_id, region_id)` | Country/region mapping |
| `data_source` | `data_source_id` | Data sources (QA, S3) |
| `weblink_type` | `weblink_type_id` | Weblink types |

### 8. Classifications

| Table | PK | Description | Relations |
|-------|-----|-------------|-----------|
| `classification_scheme` | `classification_scheme_id` | Scheme (GICS, etc.) | |
| `classification_level` | `(scheme_id, level_number)` | Hierarchical levels | → scheme |
| `classification_node` | `(scheme_id, code)` | Classification nodes | → scheme, parent_node |
| `entity_classification` | `(entity_id, scheme_id)` | Entity classification | → entity, node |

**GICS hierarchical structure:**
```
Sector (level 1)
  └── Industry Group (level 2)
        └── Industry (level 3)
              └── Sub-Industry (level 4)
```

### 9. CDC Load Tracking

| Table | PK | Description |
|-------|-----|-------------|
| `market_data_load` | `market_data_load_id` | CDC market data tracking |
| `company_market_cap_load` | `company_market_cap_load_id` | CDC market cap tracking |
| `std_financial_value_load` | `std_financial_value_load_id` | CDC financial values tracking |

Common columns (via `CDCLoadMixin`):
- `last_source_version` (BigInteger)
- `loaded_at` (timestamp)
- `rows_inserted`, `rows_updated`, `rows_deleted`

---

## TimescaleDB Hypertables

### market_data
```sql
-- 20 partitions by quote_id, 90-day chunks
SELECT create_hypertable('master.market_data', 'trade_date',
    partitioning_column => 'quote_id',
    number_partitions => 20,
    chunk_time_interval => INTERVAL '90 days'
);

-- Compression after 90 days
SELECT add_compression_policy('master.market_data', INTERVAL '90 days');
```

### fx_rate
```sql
-- 4 partitions by base_currency_id, 90-day chunks
SELECT create_hypertable('master.fx_rate', 'rate_date',
    partitioning_column => 'base_currency_id',
    number_partitions => 4,
    chunk_time_interval => INTERVAL '90 days'
);

-- Compression after 180 days
SELECT add_compression_policy('master.fx_rate', INTERVAL '180 days');
```

---

## Index Strategy

### Filtered Indexes
```sql
-- Quote: primary quotes only
CREATE INDEX idx_quote_instrument_id_is_primary
ON master.quote (instrument_id) WHERE is_primary = true;

-- Market data: non-null close prices
CREATE INDEX idx_market_data_quote_id_trade_date_notnull
ON master.market_data (quote_id, trade_date DESC) WHERE close IS NOT NULL;
```

### Composite Indexes
```sql
-- Financial values: signal queries
CREATE INDEX idx_std_financial_value_company_id_item_id_calendar_end_date
ON master.std_financial_value (company_id, std_financial_item_id, calendar_end_date);

-- FX rates: currency pair lookups
CREATE INDEX idx_fx_rate_currencies_rate_date
ON master.fx_rate (base_currency_id, quote_currency_id, rate_date DESC);
```

---

## Mapping Tables Pattern

Each main entity has a mapping table to trace data provenance:

```
┌─────────────────────────────────────────────────────────────────┐
│                    MAPPING PATTERN                               │
└─────────────────────────────────────────────────────────────────┘

External Source         Mapping Table              Master Table
(Refinitiv QA)
                    ┌─────────────────────┐
 ORG123456  ───────▶│  entity_mapping     │───────▶  entity_id = 1
                    │                     │
                    │  data_source_id = 1 │
                    │  external_id = ORG..|│
                    │  internal_id = 1    │
                    └─────────────────────┘

Unique constraint: (data_source_id, entity_type_id, external_entity_id)
```

**Benefits:**
- Complete source → target traceability
- Multi-source support (same external_id at different providers)
- Decoupling between source IDs and internal IDs

---

## Naming Conventions

| Pattern | Convention | Example |
|---------|------------|---------|
| Primary Key | `{table}_id` | `entity_id`, `quote_id` |
| Foreign Key | `{referenced_table}_id` | `instrument_id`, `currency_id` |
| Mapping PK | `internal_{entity}_id` | `internal_entity_id` |
| Mapping External | `external_{entity}_id` | `external_entity_id` |
| Type Tables | `{entity}_type` | `entity_type`, `venue_type` |
| Timestamps | `*_at`, `*_date` | `loaded_at`, `effective_date` |
| Booleans | `is_*`, `has_*` | `is_primary`, `is_major_security` |

---

## See Also

- [ARCHITECTURE.md](ARCHITECTURE.md) - Technical architecture
- `src/master_instrument/models/` - SQLAlchemy code
- `alembic/versions/` - Schema migrations
