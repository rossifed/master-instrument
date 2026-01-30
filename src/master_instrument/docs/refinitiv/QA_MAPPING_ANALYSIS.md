# Refinitiv QA Mapping Analysis

**Date**: November 13, 2025
**Author**: F. Rossi
**Status**: Completed

---

## 1. Objective

Analyze the Refinitiv Quantitative Analytics (QA) schema to determine how its data model can be adapted to feed our master instrument database, while maintaining relational integrity and data consistency.

Our target model uses a three-layer structure:

```
Company → Instrument → Quote
```

- **Company**: Represents the issuer (entity)
- **Instrument**: Identifies a specific financial instrument
- **Quote**: Links an instrument to a specific venue (exchange, OTC, aggregator)

---

## 2. Source Schemas Overview

### 2.1 Reuters Fundamentals (RKD)

Two levels of granularity:
- **RKDFndCmpRef**: Company level (issuer)
- **RKDFndCmpRefIssue**: Issue level (instrument)

No concept of quote or exchange in RKD.

### 2.2 DataStream (DS2)

Four levels of granularity:
- **DS2Company**: Company level
- **DS2Security**: Security level (highest)
- **DS2CtryQtInfo**: Country-level issuance
- **DS2ExchQtInfo**: Exchange-level listing

### 2.3 Volume Comparison

| Table | Record Count |
|-------|--------------|
| RKDFndCmpRef | 138,885 |
| RKDFndCmpRefIssue | 108,257 |
| DS2Company | 162,283 |
| DS2Security | 204,194 |
| DS2CtryQtInfo | 268,230 |

The two schemas have different record counts and do not share common identifiers directly.

---

## 3. Mapping Analysis

### 3.1 Mapping Mechanism

RKD and DS2 are linked via `vw_SecurityMappingX` view, connecting:
- `RKDFndCmpRefIssue.IssueCode` (ventype = 26)
- `DS2CtryQtInfo.InfoCode` (ventype = 33)

Through common `seccode` and `typ` columns.

### 3.2 Mapping Results

| Metric | Count |
|--------|-------|
| Total mapping pairs | 103,953 |
| Distinct InfoCode | 103,953 |
| Distinct IssueCode | 103,884 |
| **Mismatches** | **69** |

#### Issue Identified

69 IssueCode values map to multiple InfoCode values (1:N relationship instead of expected 1:1).

After filtering on `enddate > CURRENT_DATE`:

| Metric | Count |
|--------|-------|
| IssueCode with multiple InfoCode | **23** |

### 3.3 Company-Level Relationship

Analysis of DS2Company ↔ RKDFndCmpRef relationship:

| Relationship | Count |
|--------------|-------|
| RKDFndCmpRef.Code with multiple DS2Company | 16 |
| DS2Company with multiple RKDFndCmpRef.Code | 732 |

**Conclusion**: The relationship between DS2Company and RKDFndCmpRef is **many-to-many**, not 1:1.

### 3.4 Using rel_to_code

Using `RKDFndCmpRef.rel_to_code` (parent company) instead of `Code` reduces mismatches:

| Filter | DS2Security with multiple RKD Company |
|--------|---------------------------------------|
| No filter | 234 |
| Using rel_to_code | 51 |
| Using rel_to_code + enddate filter | **22** |

---

## 4. Key Findings

### 4.1 Problems Identified

1. **IssueCode → InfoCode mapping is not strictly 1:1**
   - 23 active mismatches where one IssueCode maps to multiple InfoCode
   - Root cause: Data quality issues in Refinitiv mapping tables

2. **DS2Company ↔ RKDFndCmpRef is many-to-many**
   - Cannot safely join company information from both sources
   - Would violate 1:1 constraint between instrument and company

3. **DS2Security granularity breaks referential integrity**
   - Using DS2Security as instrument pivot would create 1:N relationship with RKDFndCmpRef
   - 234 cases (or 22 after filtering) where one DS2Security maps to multiple companies

### 4.2 Reported to Refinitiv

The 23 mapping mismatches and cases where no primary DS2CtryQtInfo exists for a given RKD IssueCode have been reported to Refinitiv for investigation.

---

## 5. Design Decision

### 5.1 Chosen Pivot: InfoCode (DS2CtryQtInfo)

**Decision**: Use `InfoCode` from `DS2CtryQtInfo` as the instrument-level pivot.

**Rationale**:
- Most granular non-listing level available
- Enables mapping to all market-related data (market data, market cap, market value)
- Near 1:1 relationship with IssueCode (only 23 exceptions)
- Allows enrichment with country-specific attributes

### 5.2 Company Reference: RKDFndCmpRef

**Decision**: Use `RKDFndCmpRef` as the primary source for company data.

**Rationale**:
- Provides access to fundamental data
- Contains detailed company information
- `rel_to_code` provides parent company aggregation when needed
- `OrgId` enables higher-level organizational grouping

### 5.3 Handling Exceptions

The 23 mapping exceptions are:
- Documented and tracked
- Reported to Refinitiv
- Handled in staging queries with deduplication logic

---

## 6. Implementation Summary

```
Source (QA)                    Target (Master)
───────────────────────────    ───────────────────────
RKDFndCmpRef.Code         →    Entity/Company
     │
     ↓
RKDFndCmpRefIssue         →    Instrument (via mapping)
     │
     ↓ (seccode/typ)
DS2CtryQtInfo.InfoCode    →    Instrument pivot (quote-level data)
     │
     ↓
DS2ExchQtInfo             →    Quote (exchange listing)
     │
     ↓
DS2PrimQtPrc              →    MarketData (prices)
```

---

## 7. Appendix: Exception Cases

### 7.1 IssueCode → Multiple InfoCode (22 active after enddate filter)

Examples include instruments with multiple country-level representations that share the same IssueCode.

### 7.2 DS2Security → Multiple RKDFndCmpRef (22 active after filtering)

| DS2Security | Company Names |
|-------------|---------------|
| ABB LTD N | Abb Ltd, ABB Ltd |
| ANZ GROUP HOLDINGS | ANZ Banking Group (NZ), ANZ Group Holdings Ltd |
| FIRSTRAND | FirstRand Group, FirstRand Ltd |
| LEONARDO DRS | Leonardo DRS Inc (2 entries) |
| NIO 'A' | NIO Inc, Nio Inc - ADR |
| TRANSOCEAN | Transocean Inc, Transocean LTD |
| ... | ... |

These represent corporate restructurings, ADR/ordinary share distinctions, or data quality issues in the source.

---

## 8. References

- LSEG QA Database Schema Documentation
- QA Database Schema - Datastream v1.1.7
- Reuters Fundamentals v1.2.4
- Refinitiv Financials Glossary (February 2023)
