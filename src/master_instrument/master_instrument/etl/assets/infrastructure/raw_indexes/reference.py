"""
Reference data indexes for raw tables.

Tables: qa_DS2Security, qa_DS2CtryQtInfo, qa_DS2Exchange, qa_DS2ExchQtInfo,
        qa_RKDFndCmpRef, qa_RKDFndCmpRefIssue, qa_RKDFndCmpDet, qa_RKDFndCmpFiling,
        qa_RKDFndInfo, qa_PermOrgRef, qa_PermQuoteRef, s3_gics_classification
"""

from dagster import asset, Output, MetadataValue
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from .utils import create_indexes_for_table


DOMAIN = "reference"


# =============================================================================
# qa_DS2Security - Securities (frequently joined)
# =============================================================================
@asset(
    name="qa_DS2Security",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_DS2Security (securities)",
)
def raw_idx_ds2security(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2security_dsseccode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2security_dsseccode ON raw."qa_DS2Security" ("DsSecCode")'
        },
        {
            "name": "idx_ds2security_dscmpycode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2security_dscmpycode ON raw."qa_DS2Security" ("DsCmpyCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2Security", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2CtryQtInfo - Country quote info
# =============================================================================
@asset(
    name="qa_DS2CtryQtInfo",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_DS2CtryQtInfo (country quote info)",
)
def raw_idx_ds2ctryqtinfo(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2ctryqtinfo_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2ctryqtinfo_infocode ON raw."qa_DS2CtryQtInfo" ("InfoCode")'
        },
        {
            "name": "idx_ds2ctryqtinfo_dsseccode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2ctryqtinfo_dsseccode ON raw."qa_DS2CtryQtInfo" ("DsSecCode")'
        },
        {
            "name": "idx_ds2ctryqtinfo_seccode_typ",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2ctryqtinfo_seccode_typ ON raw."qa_DS2CtryQtInfo" ("seccode", "typ")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2CtryQtInfo", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2Exchange - Exchanges/Venues
# =============================================================================
@asset(
    name="qa_DS2Exchange",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_DS2Exchange (exchanges)",
)
def raw_idx_ds2exchange(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2exchange_exchintcode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2exchange_exchintcode ON raw."qa_DS2Exchange" ("ExchIntCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2Exchange", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_DS2ExchQtInfo - Exchange quote info
# =============================================================================
@asset(
    name="qa_DS2ExchQtInfo",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_DS2ExchQtInfo (exchange quote info)",
)
def raw_idx_ds2exchqtinfo(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_ds2exchqtinfo_infocode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ds2exchqtinfo_infocode ON raw."qa_DS2ExchQtInfo" ("InfoCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_DS2ExchQtInfo", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndCmpRef - Company reference
# =============================================================================
@asset(
    name="qa_RKDFndCmpRef",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndCmpRef (company reference)",
)
def raw_idx_rkdfndcmpref(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndcmpref_code",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcmpref_code ON raw."qa_RKDFndCmpRef" ("Code")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndCmpRef", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndCmpRefIssue - Company issues
# =============================================================================
@asset(
    name="qa_RKDFndCmpRefIssue",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndCmpRefIssue (company issues)",
)
def raw_idx_rkdfndcmprefissue(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndcmprefissue_code",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcmprefissue_code ON raw."qa_RKDFndCmpRefIssue" ("Code")'
        },
        {
            "name": "idx_rkdfndcmprefissue_issuecode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcmprefissue_issuecode ON raw."qa_RKDFndCmpRefIssue" ("IssueCode")'
        },
        {
            "name": "idx_rkdfndcmprefissue_seccode_typ",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcmprefissue_seccode_typ ON raw."qa_RKDFndCmpRefIssue" ("seccode", "typ")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndCmpRefIssue", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndCmpDet - Company details
# =============================================================================
@asset(
    name="qa_RKDFndCmpDet",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndCmpDet (company details)",
)
def raw_idx_rkdfndcmpdet(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndcmpdet_code",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcmpdet_code ON raw."qa_RKDFndCmpDet" ("Code")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndCmpDet", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndCmpFiling - Company filings
# =============================================================================
@asset(
    name="qa_RKDFndCmpFiling",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndCmpFiling (company filings)",
)
def raw_idx_rkdfndcmpfiling(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndcmpfiling_code_txtinfotypecode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndcmpfiling_code_txtinfotypecode ON raw."qa_RKDFndCmpFiling" ("Code", "TxtInfoTypeCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndCmpFiling", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_RKDFndInfo - Fund info
# =============================================================================
@asset(
    name="qa_RKDFndInfo",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_RKDFndInfo (fund info)",
)
def raw_idx_rkdfndinfo(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_rkdfndinfo_repno",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rkdfndinfo_repno ON raw."qa_RKDFndInfo" ("RepNo")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_RKDFndInfo", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_PermOrgRef - Organization reference
# =============================================================================
@asset(
    name="qa_PermOrgRef",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_PermOrgRef (organization reference)",
)
def raw_idx_permorgref(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_permorgref_prirptentitycode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permorgref_prirptentitycode ON raw."qa_PermOrgRef" ("PriRptEntityCode")'
        },
        {
            "name": "idx_permorgref_orgpermid",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permorgref_orgpermid ON raw."qa_PermOrgRef" ("OrgPermID")'
        },
        {
            "name": "idx_qa_permorgref_ultparent",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_qa_permorgref_ultparent ON raw."qa_PermOrgRef" ("UltimateParentOrgPermID")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_PermOrgRef", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# qa_PermQuoteRef - Quote reference
# =============================================================================
@asset(
    name="qa_PermQuoteRef",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for qa_PermQuoteRef (quote reference)",
)
def raw_idx_permquoteref(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_permquoteref_dsquotenumber_exchcode",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permquoteref_dsquotenumber_exchcode ON raw."qa_PermQuoteRef" ("DsQuoteNumber", "ExchCode")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "qa_PermQuoteRef", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# s3_gics_classification - S3 GICS data
# =============================================================================
@asset(
    name="s3_gics_classification",
    key_prefix=["infrastructure", "raw_indexes", "reference"],
    group_name="infrastructure",
    description="Indexes for s3_gics_classification (GICS classification)",
)
def raw_idx_s3_gics_classification(engine: SqlAlchemyEngineResource):
    indexes = [
        {
            "name": "idx_s3_gics_classification_isin",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_s3_gics_classification_isin ON raw."s3_gics_classification" ("isin")'
        },
    ]
    results = create_indexes_for_table(engine.get_engine(), "s3_gics_classification", indexes, DOMAIN)
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })
