"""Tests for source snippet.

Tests snippets/source.j2 which generates the source CTE.
"""
import pytest
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
import os


@pytest.fixture
def jinja_env():
    """Create Jinja2 environment for template rendering."""
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = current_dir.parent.parent.parent.parent.parent
    template_dir = project_root / "master_instrument" / "etl" / "loading" / "templates"
    return Environment(loader=FileSystemLoader(str(template_dir)))


class TestSourceSnippet:
    """Test snippets/source.j2 snippet."""
    
    def test_simple_source(self, jinja_env):
        """Test basic source CTE without batch filtering."""
        template = jinja_env.from_string("""
{%- set source_table = 'staging.stg_currency' -%}
{%- set batch = None -%}
{%- set order_by = None -%}
{% include 'snippets/source.j2' %}
""")
        sql = template.render()
        
        assert "source_query AS" in sql
        assert "SELECT *" in sql
        assert "FROM staging.stg_currency" in sql
        assert "WHERE" not in sql
    
    def test_with_batch_filtering(self, jinja_env):
        """Test source CTE with batch date filtering."""
        template = jinja_env.from_string("""
{%- set source_table = 'staging.stg_trades' -%}
{%- set batch = {'batch_date_column': 'trade_date'} -%}
{%- set order_by = None -%}
{% include 'snippets/source.j2' %}
""")
        sql = template.render()
        
        assert "WHERE trade_date >= :start_date" in sql
        assert "AND trade_date <= :end_date" in sql
    
    def test_with_order_by(self, jinja_env):
        """Test source CTE with ORDER BY."""
        template = jinja_env.from_string("""
{%- set source_table = 'staging.stg_prices' -%}
{%- set batch = None -%}
{%- set order_by = 'date DESC, instrument_id' -%}
{% include 'snippets/source.j2' %}
""")
        sql = template.render()
        
        assert "ORDER BY date DESC, instrument_id" in sql

    def test_with_batch_and_order_by(self, jinja_env):
        """Test source CTE with both batch and ORDER BY."""
        template = jinja_env.from_string("""
{%- set source_table = 'staging.stg_prices' -%}
{%- set batch = {'batch_date_column': 'price_date'} -%}
{%- set order_by = 'instrument_id' -%}
{% include 'snippets/source.j2' %}
""")
        sql = template.render()
        
        assert "WHERE price_date >= :start_date" in sql
        assert "ORDER BY instrument_id" in sql
