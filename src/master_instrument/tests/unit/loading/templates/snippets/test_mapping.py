"""Tests for mapping snippet.

Tests snippets/mapping.j2 which generates the mapping table INSERT.
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


class TestMappingSnippet:
    """Test snippets/mapping.j2 snippet."""
    
    def test_basic_mapping(self, jinja_env):
        """Test basic mapping INSERT."""
        template = jinja_env.from_string("""
{%- set mapping = {
    'mapping_table': 'master.entity_mapping',
    'data_source_column': 'data_source_id',
    'mapping_external_column': 'external_entity_id',
    'mapping_internal_column': 'internal_entity_id',
    'source_external_id_column': 'external_id'
} -%}
{%- set source_cte = 'merge_result' -%}
{%- set internal_id_column = 'company_id' -%}
{% include 'snippets/mapping.j2' %}
""")
        sql = template.render()
        
        assert "INSERT INTO master.entity_mapping" in sql
        assert "data_source_id" in sql
        assert "external_entity_id" in sql
        assert "internal_entity_id" in sql
        assert "FROM merge_result" in sql
        assert "ON CONFLICT" in sql
        assert "DO NOTHING" in sql

    def test_uses_internal_id_column(self, jinja_env):
        """Test that internal_id_column is used in SELECT."""
        template = jinja_env.from_string("""
{%- set mapping = {
    'mapping_table': 'master.entity_mapping',
    'data_source_column': 'source_id',
    'mapping_external_column': 'ext_id',
    'mapping_internal_column': 'int_id',
    'source_external_id_column': 'external_code'
} -%}
{%- set source_cte = 'upsert_result' -%}
{%- set internal_id_column = 'entity_pk' -%}
{% include 'snippets/mapping.j2' %}
""")
        sql = template.render()
        
        assert "entity_pk" in sql
        assert "FROM upsert_result" in sql

    def test_conflict_columns(self, jinja_env):
        """Test that ON CONFLICT uses correct columns."""
        template = jinja_env.from_string("""
{%- set mapping = {
    'mapping_table': 'master.instrument_mapping',
    'data_source_column': 'ds_id',
    'mapping_external_column': 'ext_instr_id',
    'mapping_internal_column': 'int_instr_id',
    'source_external_id_column': 'ext_id'
} -%}
{%- set source_cte = 'result' -%}
{%- set internal_id_column = 'id' -%}
{% include 'snippets/mapping.j2' %}
""")
        sql = template.render()
        
        assert "ON CONFLICT (ds_id, ext_instr_id)" in sql

    def test_mapping_with_type_column(self, jinja_env):
        """Test mapping INSERT with type column (entity_type_id, etc.)."""
        template = jinja_env.from_string("""
{%- set mapping = {
    'mapping_table': 'master.entity_mapping',
    'data_source_column': 'data_source_id',
    'mapping_type_column': 'entity_type_id',
    'source_type_column': 'entity_type_id',
    'mapping_external_column': 'external_entity_id',
    'mapping_internal_column': 'internal_entity_id',
    'source_external_id_column': 'external_entity_id'
} -%}
{%- set source_cte = 'merge_parent' -%}
{%- set internal_id_column = 'entity_id' -%}
{% include 'snippets/mapping.j2' %}
""")
        sql = template.render()
        
        # Type column should be in INSERT columns
        assert "entity_type_id," in sql
        # Type column should be in ON CONFLICT
        assert "ON CONFLICT (data_source_id, entity_type_id, external_entity_id)" in sql
        # Type column should be in SELECT
        lines = [l.strip() for l in sql.split('\n') if 'entity_type_id' in l]
        assert len(lines) >= 2  # INSERT and SELECT both have it
