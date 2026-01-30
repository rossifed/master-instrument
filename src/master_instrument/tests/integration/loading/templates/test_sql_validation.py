"""SQL validation tests - verify generated SQL is syntactically correct."""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine, text


# Setup
TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


@pytest.fixture(scope="module")
def pg_engine():
    """PostgreSQL engine for validation (optional, skip if not available)."""
    try:
        engine = create_engine("postgresql://fundy:fundy@localhost:5432/fundy")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception:
        pytest.skip("PostgreSQL not available for SQL validation")


class TestSQLValidation:
    """Validate that generated SQL is syntactically correct."""
    
    def test_merge_country_sql_valid(self, pg_engine):
        """SQL for country must be EXPLAIN-able (syntactically valid)."""
        # Config EXACTE de production
        params = {
            'target_table': 'master.country',
            'source_view': 'seed.country',
            'unique_key': 'code',
            'columns': ['code', 'name'],
            'data_columns': ['name'],
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False,
            'order_by': 'name'
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # EXPLAIN validates syntax without executing
        with pg_engine.connect() as conn:
            try:
                conn.execute(text(f"EXPLAIN {sql}"))
            except Exception as e:
                pytest.fail(f"Invalid SQL:\n{sql}\n\nError: {e}")
    
    def test_merge_currency_sql_valid(self, pg_engine):
        """SQL for currency must be valid."""
        params = {
            'target_table': 'master.currency',
            'source_view': 'seed.currency',
            'unique_key': 'code',
            'columns': ['code', 'name'],
            'data_columns': ['name'],
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False,
            'order_by': 'name'
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        with pg_engine.connect() as conn:
            try:
                conn.execute(text(f"EXPLAIN {sql}"))
            except Exception as e:
                pytest.fail(f"Invalid SQL:\n{sql}\n\nError: {e}")
