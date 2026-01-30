"""Exhaustive tests for upsert.sql.j2"""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class TestUpsertInheritanceBasic:
    """Basic tests."""
    
    def test_basic_upsert_inheritance(self):
        """Basic upsert inheritance test."""
        params = {
            'target_table': 'master.equity',
            'source_table': 'intermediate.int_equity',
            'unique_key': ['equity_id'],
            'columns': ['equity_id', 'ticker', 'shares_outstanding'],
            'data_columns': ['ticker', 'shares_outstanding'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.equity_mapping',
                'mapping_external_column': 'external_equity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_equity_id',
                'source_external_id_column': 'external_id'
            },
            'inheritance': {
                'parent_table': 'master.instrument',
                'parent_unique_key': 'instrument_id',
                'child_table': 'master.equity',
                'child_unique_key': 'equity_id',
                'source_parent_key': 'internal_instrument_id',
                'parent_columns': ['instrument_id', 'name'],
                'child_columns': ['equity_id', 'instrument_id', 'ticker', 'shares_outstanding']
            },
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        # Must have parent upsert
        assert 'INSERT INTO master.instrument' in sql
        assert 'ON CONFLICT' in sql
        
        # Must have child upsert
        assert 'INSERT INTO master.equity' in sql
        assert 'equity_id' in sql


class TestUpsertInheritanceWithSelfReference:
    """Tests with self-reference."""
    
    def test_self_reference(self):
        """Test upsert inheritance with self-reference."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'parent_company_id'],
            'data_columns': ['name', 'parent_company_id'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.entity_mapping',
                'mapping_external_column': 'external_entity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_entity_id',
                'source_external_id_column': 'external_id'
            },
            'inheritance': {
                'parent_table': 'master.entity',
                'parent_unique_key': 'entity_id',
                'child_table': 'master.company',
                'child_unique_key': 'company_id',
                'source_parent_key': 'internal_entity_id',
                'parent_columns': ['entity_id', 'name'],
                'child_columns': ['company_id', 'entity_id', 'parent_company_id']
            },
            'self_reference': type('SelfRef', (), {
                'columns': {'parent_company_id': 'parent_company_id'},
                'requires_mapping': True
            })(),
            'with_audit': False
        }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.entity' in sql
        assert 'INSERT INTO master.company' in sql
        assert 'parent_company_id = src.parent_company_id' in sql


class TestUpsertInheritanceWithAudit:
    """Tests with audit."""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    def test_audit(self, with_audit):
        """Test with audit."""
        params = {
            'target_table': 'master.equity',
            'source_table': 'intermediate.int_equity',
            'unique_key': ['equity_id'],
            'columns': ['equity_id', 'ticker'],
            'data_columns': ['ticker'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.equity_mapping',
                'mapping_external_column': 'external_equity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_equity_id',
                'source_external_id_column': 'external_id'
            },
            'inheritance': {
                'parent_table': 'master.instrument',
                'parent_unique_key': 'instrument_id',
                'child_table': 'master.equity',
                'child_unique_key': 'equity_id',
                'source_parent_key': 'internal_instrument_id',
                'parent_columns': ['instrument_id', 'name'],
                'child_columns': ['equity_id', 'instrument_id', 'ticker']
            },
            'with_audit': with_audit
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        template = jinja_env.get_template('upsert.sql.j2')
        sql = template.render(**params)
        
        assert 'ON CONFLICT' in sql
        
        if with_audit:
            assert 'created_at' in sql or 'updated_at' in sql


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
