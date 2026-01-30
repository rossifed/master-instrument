"""
Convert market_data & market_data_adjusted to TimescaleDB hypertables.

- market_data: hypertable + compression (90 days)
- market_data_adjusted: hypertable only (no compression)
  because adjusted data must be recalculated per instrument
  and compression prevents updates efficiently.

Revision ID: 0006_convert_to_hypertables
Revises: 0005_temporalize_tables
"""

from alembic import op

revision = "0006_convert_to_hypertables"
down_revision = "0005_temporalize_tables"
branch_labels = None
depends_on = None


def upgrade():

    # ----------------------------------------------------------------------
    # Convert market_data (raw) to hypertable
    # With 60K+ quotes, use space partitioning for optimal performance
    # 20 partitions × 90 days = ~3K quotes/partition, ~10.8MB/chunk
    # ----------------------------------------------------------------------
    op.execute("""
        SELECT create_hypertable(
            'master.market_data',
            'trade_date',
            partitioning_column => 'quote_id',
            number_partitions => 20,
            chunk_time_interval => INTERVAL '90 days',
            migrate_data => true,
            if_not_exists => true
        );
    """)

    # Enable compression on market_data
    op.execute("""
        ALTER TABLE master.market_data
        SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'trade_date',
            timescaledb.compress_segmentby = 'quote_id'
        );
    """)

    # Add compression policy: compress chunks older than 90 days, run weekly
    op.execute("""
        SELECT add_compression_policy(
            'master.market_data',
            compress_after => INTERVAL '90 days',
            schedule_interval => INTERVAL '1 week',
            if_not_exists => true
        );
    """)

    # ----------------------------------------------------------------------
    # Convert fx_rate to hypertable
    # 910 currency pairs, use space partitioning for query optimization
    # 4 partitions × 90 days = ~227 pairs/partition, ~1MB/chunk
    # ----------------------------------------------------------------------
    op.execute("""
        SELECT create_hypertable(
            'master.fx_rate',
            'rate_date',
            partitioning_column => 'base_currency_id',
            number_partitions => 4,
            chunk_time_interval => INTERVAL '90 days',
            migrate_data => true,
            if_not_exists => true
        );
    """)

    # Enable compression on fx_rate
    op.execute("""
        ALTER TABLE master.fx_rate
        SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'rate_date',
            timescaledb.compress_segmentby = 'base_currency_id, quote_currency_id'
        );
    """)

    # Add compression policy: compress chunks older than 180 days, run weekly
    op.execute("""
        SELECT add_compression_policy(
            'master.fx_rate',
            compress_after => INTERVAL '180 days',
            schedule_interval => INTERVAL '1 week',
            if_not_exists => true
        );
    """)

    # ----------------------------------------------------------------------
    # Convert total_return to hypertable
    # Similar structure to market_data: daily values per quote
    # 20 partitions × 90 days for optimal performance
    # ----------------------------------------------------------------------
    op.execute("""
        SELECT create_hypertable(
            'master.total_return',
            'value_date',
            partitioning_column => 'quote_id',
            number_partitions => 20,
            chunk_time_interval => INTERVAL '90 days',
            migrate_data => true,
            if_not_exists => true
        );
    """)

    # Enable compression on total_return
    op.execute("""
        ALTER TABLE master.total_return
        SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'value_date',
            timescaledb.compress_segmentby = 'quote_id'
        );
    """)

    # Add compression policy: compress chunks older than 90 days, run weekly
    op.execute("""
        SELECT add_compression_policy(
            'master.total_return',
            compress_after => INTERVAL '90 days',
            schedule_interval => INTERVAL '1 week',
            if_not_exists => true
        );
    """)


def downgrade():

    # Remove compression policy on total_return (if exists from previous migrations)
    op.execute("""
        SELECT remove_compression_policy('master.total_return', if_exists => true);
    """)

    # Disable compression on total_return
    op.execute("""
        ALTER TABLE master.total_return
        SET (timescaledb.compress = false);
    """)

    # Remove compression policy on fx_rate (if exists from previous migrations)
    op.execute("""
        SELECT remove_compression_policy('master.fx_rate', if_exists => true);
    """)

    # Disable compression on fx_rate
    op.execute("""
        ALTER TABLE master.fx_rate
        SET (timescaledb.compress = false);
    """)

    # Remove compression policy on market_data (if exists from previous migrations)
    op.execute("""
        SELECT remove_compression_policy('master.market_data', if_exists => true);
    """)

    # Disable compression flags
    op.execute("""
        ALTER TABLE master.market_data
        SET (timescaledb.compress = false);
    """)

    # TimescaleDB does not support "undo hypertable" cleanly.
    # Downgrade leaves tables as hypertables, which is safe and acceptable.
