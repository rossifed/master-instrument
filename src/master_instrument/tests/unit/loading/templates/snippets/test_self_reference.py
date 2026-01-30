"""Unit tests for self_reference CTE snippet.

Tests the snippets/self_reference.j2 snippet in isolation to ensure correct SQL generation
for both mapping and non-mapping modes.
"""
import pytest
from jinja2 import Environment, FileSystemLoader
from pathlib import Path


@pytest.fixture
def jinja_env():
    """Create Jinja2 environment for template rendering."""
    import os
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = current_dir.parent.parent.parent.parent.parent
    template_dir = project_root / "master_instrument" / "etl" / "loading" / "templates"
    return Environment(loader=FileSystemLoader(str(template_dir)))


class TestSelfReferenceSnippet:
    """Test snippets/self_reference.j2 snippet directly."""
    
    def test_mapping_mode_single_column(self, jinja_env):
        """Test self-reference with mapping for single column."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_table = 'intermediate.int_company' -%}
{%- set unique_key = 'company_id' -%}
{%- set self_reference = {
    'columns': {'primary_company_id': 'primary_company_id'},
    'requires_mapping': True
} -%}
{%- set mapping = {
    'mapping_table': 'master.entity_mapping',
    'mapping_external_column': 'external_entity_id',
    'mapping_internal_column': 'internal_entity_id',
    'source_external_id_column': 'external_id',
    'data_source_column': 'data_source_id'
} -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render()
        
        # Verify UPDATE structure
        assert "UPDATE master.company tgt" in sql
        assert "SET" in sql
        assert "primary_company_id = src.primary_company_id" in sql
        assert "FROM intermediate.int_company src" in sql
        assert "JOIN master.entity_mapping self_map" in sql
        assert "ON self_map.external_entity_id = src.external_id" in sql
        assert "AND self_map.data_source_id = src.data_source_id" in sql
        assert "WHERE tgt.company_id = self_map.internal_entity_id" in sql
    
    def test_mapping_mode_multiple_columns(self, jinja_env):
        """Test self-reference with mapping for multiple columns."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_table = 'intermediate.int_company' -%}
{%- set unique_key = 'company_id' -%}
{%- set self_reference = {
    'columns': {
        'primary_company_id': 'primary_company_id',
        'ultimate_organization_id': 'ultimate_organization_id',
        'parent_company_id': 'parent_company_id'
    },
    'requires_mapping': True
} -%}
{%- set mapping = {
    'mapping_table': 'master.entity_mapping',
    'mapping_external_column': 'external_entity_id',
    'mapping_internal_column': 'internal_entity_id',
    'source_external_id_column': 'external_id',
    'data_source_column': 'data_source_id'
} -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render()
        
        # Verify all columns in SET clause
        assert "primary_company_id = src.primary_company_id," in sql
        assert "ultimate_organization_id = src.ultimate_organization_id," in sql
        assert "parent_company_id = src.parent_company_id" in sql
        
        # Verify single JOIN (not multiple)
        assert sql.count("JOIN master.entity_mapping") == 1
        assert "ON self_map.external_entity_id = src.external_id" in sql
    
    def test_non_mapping_mode(self, jinja_env):
        """Test self-reference without mapping (direct key lookup)."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_table = 'intermediate.int_company' -%}
{%- set unique_key = 'company_id' -%}
{%- set self_reference = {
    'columns': {'primary_company_id': 'primary_natural_key'},
    'requires_mapping': False
} -%}
{%- set mapping = None -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render()
        
        # Verify UPDATE structure for non-mapping mode
        assert "UPDATE master.company tgt" in sql
        assert "SET" in sql
        assert "primary_company_id = src_ref.company_id" in sql
        assert "FROM intermediate.int_company src" in sql
        assert "LEFT JOIN master.company src_ref" in sql
        assert "ON src_ref.company_id = src.primary_natural_key" in sql
        assert "WHERE tgt.company_id = src.company_id" in sql
    
    def test_no_self_reference_empty_output(self, jinja_env):
        """Test that no SQL is generated when self_reference is None."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_table = 'intermediate.int_company' -%}
{%- set unique_key = 'company_id' -%}
{%- set self_reference = None -%}
{%- set mapping = None -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render().strip()
        
        # No SQL should be generated
        assert sql == ""
    
    def test_empty_columns_no_output(self, jinja_env):
        """Test that no SQL is generated when columns is empty."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_table = 'intermediate.int_company' -%}
{%- set unique_key = 'company_id' -%}
{%- set self_reference = {'columns': {}} -%}
{%- set mapping = None -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render().strip()
        
        # No SQL should be generated
        assert sql == ""
    
    def test_requires_mapping_false_uses_lookup(self, jinja_env):
        """Test that requires_mapping=False uses direct lookup pattern."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.entity' -%}
{%- set source_table = 'staging.stg_entity' -%}
{%- set unique_key = 'entity_id' -%}
{%- set self_reference = {
    'columns': {'parent_id': 'parent_code'},
    'requires_mapping': False
} -%}
{%- set mapping = {
    'mapping_table': 'master.entity_mapping',
    'source_external_id_column': 'ext_id'
} -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render()
        
        # Should use LEFT JOIN pattern, not mapping JOIN
        assert "LEFT JOIN master.entity src_ref" in sql
        assert "ON src_ref.entity_id = src.parent_code" in sql
        # Should NOT use mapping table when requires_mapping=False
        assert "master.entity_mapping" not in sql

    def test_composite_unique_key(self, jinja_env):
        """Test self-reference with composite unique key (uses first element)."""
        template = jinja_env.from_string("""
{%- set target_table = 'master.company' -%}
{%- set source_table = 'intermediate.int_company' -%}
{%- set unique_key = ['company_id', 'version'] -%}
{%- set self_reference = {
    'columns': {'parent_id': 'parent_code'},
    'requires_mapping': False
} -%}
{%- set mapping = None -%}
{% include 'snippets/self_reference.j2' %}
""")
        sql = template.render()
        
        # Should use first element of composite key
        assert "src_ref.company_id" in sql
        assert "tgt.company_id = src.company_id" in sql
