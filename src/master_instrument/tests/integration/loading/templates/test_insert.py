"""Exhaustive tests for insert.sql.j2"""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class TestInsertBasic:
    """Basic tests."""
    
    def test_basic_insert(self):
        """Basic insert test."""
        params = {
            'target_table': 'master.price',
            'source_table': 'intermediate.int_price',
            'columns': ['price_id', 'trade_date', 'close_price'],
            'with_audit': False
        }
        
        template = jinja_env.get_template('insert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.price' in sql
        assert 'FROM intermediate.int_price' in sql
        assert 'price_id' in sql


class TestInsertWithBatchDate:
    """Tests with batch_date_column."""
    
    def test_batch_date_filtering(self):
        """Test insert with batch_date_column."""
        params = {
            'target_table': 'master.price',
            'source_table': 'intermediate.int_price',
            'columns': ['price_id', 'trade_date', 'close_price'],
            'batch': type('Batch', (), {'batch_date_column': 'trade_date'})(),
            'with_audit': False
        }
        
        template = jinja_env.get_template('insert.sql.j2')
        sql = template.render(**params)
        
        assert ':start_date' in sql
        assert ':end_date' in sql
        assert 'trade_date' in sql
    
    @pytest.mark.parametrize("with_batch", [True, False])
    def test_batch_combinations(self, with_batch):
        """Test with and without batch."""
        params = {
            'target_table': 'master.price',
            'source_table': 'intermediate.int_price',
            'columns': ['price_id', 'trade_date', 'close_price'],
            'with_audit': False
        }
        
        if with_batch:
            params['batch'] = type('Batch', (), {'batch_date_column': 'trade_date'})()
        
        template = jinja_env.get_template('insert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.price' in sql
        
        if with_batch:
            assert ':start_date' in sql
            assert ':end_date' in sql


class TestInsertWithSelfReference:
    """Tests with self-reference."""
    
    def test_self_reference(self):
        """Test insert with self-reference."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'parent_company_id'],
            'data_source_column': 'data_source_id',
            'mapping': {
                'mapping_table': 'master.entity_mapping',
                'mapping_external_column': 'external_entity_id',
                'data_source_column': 'data_source_id',
                'mapping_internal_column': 'internal_entity_id',
                'source_external_id_column': 'external_id'
            },
            'self_reference': type('SelfRef', (), {
                'columns': {'parent_company_id': 'parent_company_id'},
                'requires_mapping': True
            })(),
            'with_audit': False
        }
        
        template = jinja_env.get_template('insert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.company' in sql
        assert 'parent_company_id = src.parent_company_id' in sql


class TestInsertWithAudit:
    """Tests with audit."""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    def test_audit(self, with_audit):
        """Test with audit."""
        params = {
            'target_table': 'master.price',
            'source_table': 'intermediate.int_price',
            'columns': ['price_id', 'close_price'],
            'with_audit': with_audit
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        template = jinja_env.get_template('insert.sql.j2')
        sql = template.render(**params)
        
        assert 'INSERT INTO master.price' in sql
        
        if with_audit:
            assert 'created_at' in sql or 'CURRENT_TIMESTAMP' in sql


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
