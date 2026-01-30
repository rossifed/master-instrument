"""
Seed table indexes.

Tables: data_source, entity_type, financial_period_type_mapping
(Seeds managed by dbt, indexes managed via Dagster)
"""

from dagster import asset, Output, MetadataValue
from master_instrument.etl.resources.sqlalchemy_resource import SqlAlchemyEngineResource
from master_instrument.etl.assets.infrastructure.raw_indexes.utils import create_indexes_for_table


DOMAIN = "seed"


# =============================================================================
# data_source - Data source lookups
# =============================================================================
@asset(
    name="data_source",
    key_prefix=["infrastructure", "seed_indexes"],
    group_name="infrastructure",
    description="Indexes for seed.data_source (source lookups)",
)
def seed_idx_data_source(engine: SqlAlchemyEngineResource):
    """Create index for data_source seed table."""
    indexes = [
        {
            "name": "idx_data_source_mnemonic",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_source_mnemonic ON seed."data_source" ("mnemonic")'
        },
    ]
    results = create_indexes_for_table(
        engine.get_engine(), "data_source", indexes, DOMAIN
    )
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_skipped": len(results["skipped"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# entity_type - Entity type lookups
# =============================================================================
@asset(
    name="entity_type",
    key_prefix=["infrastructure", "seed_indexes"],
    group_name="infrastructure",
    description="Indexes for seed.entity_type (entity type lookups)",
)
def seed_idx_entity_type(engine: SqlAlchemyEngineResource):
    """Create index for entity_type seed table."""
    indexes = [
        {
            "name": "idx_entity_type_mnemonic",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entity_type_mnemonic ON seed."entity_type" ("mnemonic")'
        },
    ]
    results = create_indexes_for_table(
        engine.get_engine(), "entity_type", indexes, DOMAIN
    )
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_skipped": len(results["skipped"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })


# =============================================================================
# financial_period_type_mapping - Period type mapping
# =============================================================================
@asset(
    name="financial_period_type_mapping",
    key_prefix=["infrastructure", "seed_indexes"],
    group_name="infrastructure",
    description="Indexes for seed.financial_period_type_mapping (period type lookups)",
)
def seed_idx_financial_period_type_mapping(engine: SqlAlchemyEngineResource):
    """Create index for financial_period_type_mapping seed table."""
    indexes = [
        {
            "name": "idx_fptm_external_source",
            "sql": 'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fptm_external_source ON seed."financial_period_type_mapping" ("external_period_type_id", "source")'
        },
    ]
    results = create_indexes_for_table(
        engine.get_engine(), "financial_period_type_mapping", indexes, DOMAIN
    )
    return Output(value=results, metadata={
        "indexes_created": len(results["created"]),
        "indexes_skipped": len(results["skipped"]),
        "indexes_failed": len(results["failed"]),
        "total_time_seconds": MetadataValue.float(results["total_time_seconds"]),
    })
