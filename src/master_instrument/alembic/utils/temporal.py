"""Temporal tables utilities for Alembic migrations"""
from alembic import op, context
import sqlalchemy as sa


def log(message: str):
    """Safe Alembic-aware logger"""
    try:
        ctx = context.get_context()
        if hasattr(ctx, "log") and hasattr(ctx.log, "info"):
            ctx.log.info(message)
        else:
            print(message)
    except Exception:
        print(message)


class TemporalTable:
    """Helper class to manage temporal versioning on PostgreSQL tables.
    
    This class implements bi-temporal tracking using PostgreSQL's temporal_tables
    extension. It creates a history table and triggers to automatically capture
    all changes (INSERT, UPDATE, DELETE) with timestamp ranges.
    
    Example:
        >>> temporal = TemporalTable("instrument", schema="master")
        >>> temporal.enable()
    
    Args:
        table_name: Name of the table to temporalize
        schema: Schema containing the table (default: "public")
    """

    def __init__(self, table_name: str, schema: str = "public"):
        self.table_name = table_name
        self.schema = schema
        self.history_table = f"{table_name}_history"
        self.qualified_table = f"{schema}.{table_name}"
        self.qualified_history_table = f"{schema}.{self.history_table}"
        self.trigger_name = f"versioning_trigger_{table_name}"
        self.history_index_name = f"{schema}_{self.history_table}_sys_period_idx"

    def enable(self):
        """Enable temporal versioning on this table"""
        log(f"▶ Enabling temporal versioning on {self.qualified_table}")
        self._ensure_sys_period()  # Add sys_period if missing
        self._create_history_table()
        self._create_history_index()  # Index sur la table historique
        self._create_versioning_trigger()

    def disable(self):
        """Disable temporal versioning on this table"""
        log(f"⏹ Disabling temporal versioning on {self.qualified_table}")
        self._drop_trigger()
        self._drop_history_table()
        # Note: sys_period is NOT dropped - keep it for data integrity

    def _ensure_sys_period(self):
        """Add sys_period column to main table if it doesn't exist"""
        conn = op.get_bind()
        result = conn.execute(sa.text(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = '{self.schema}'
            AND table_name = '{self.table_name}'
            AND column_name = 'sys_period'
        """))
        if result.fetchone() is None:
            log(f"  → Adding sys_period column to {self.qualified_table}")
            self._add_sys_period()

    def _add_sys_period(self):
        """Add sys_period column to main table"""
        op.add_column(
            self.table_name,
            sa.Column(
                "sys_period",
                sa.dialects.postgresql.TSTZRANGE(),
                nullable=False,
                server_default=sa.text("tstzrange(current_timestamp, NULL)")
            ),
            schema=self.schema,
        )

    def _create_history_table(self):
        """Create history table based on main table structure"""
        op.execute(f"""
            CREATE TABLE {self.qualified_history_table}
            (LIKE {self.qualified_table});
        """)


    def _create_history_index(self):
        """Create GIST index on sys_period in history table for temporal queries"""
        op.create_index(
            self.history_index_name,
            self.history_table,  # ← Sur la table historique !
            ["sys_period"],
            postgresql_using="gist",
            schema=self.schema,
        )

    def _create_versioning_trigger(self):
        """Create trigger to capture changes to history table"""
        op.execute(f"""
            CREATE TRIGGER {self.trigger_name}
            BEFORE INSERT OR UPDATE OR DELETE ON {self.qualified_table}
            FOR EACH ROW
            EXECUTE FUNCTION versioning(
                'sys_period',
                '{self.qualified_history_table}',
                true
            );
        """)

    def _drop_trigger(self):
        """Drop versioning trigger"""
        op.execute(f"""
            DROP TRIGGER IF EXISTS {self.trigger_name} ON {self.qualified_table};
        """)

    def _drop_history_table(self):
        """Drop history table (CASCADE drops index automatically)"""
        op.execute(f"DROP TABLE IF EXISTS {self.qualified_history_table} CASCADE;")

    def _drop_sys_period(self):
        """Drop sys_period column from main table"""
        op.drop_column(self.table_name, "sys_period", schema=self.schema)


def ensure_temporal_extension():
    """Ensure the 'temporal_tables' extension is installed."""
    op.execute("CREATE EXTENSION IF NOT EXISTS temporal_tables;")
    log("✓ temporal_tables extension enabled")


def temporalize_tables(schema: str, table_names: list[str]):
    """Enable temporal versioning on a list of tables.
    
    Args:
        schema: Database schema name
        table_names: List of table names to temporalize
    """
    ensure_temporal_extension()  # Une seule fois
    for table in table_names:
        TemporalTable(table, schema).enable()


def detemporalize_tables(schema: str, table_names: list[str]):
    """Disable temporal versioning on a list of tables.
    
    Args:
        schema: Database schema name
        table_names: List of table names to detemporalize
    """
    for table in table_names:
        TemporalTable(table, schema).disable()