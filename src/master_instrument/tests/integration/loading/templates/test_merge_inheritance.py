"""Exhaustive tests for merge.sql.j2"""

import pytest
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).parent.parent.parent.parent.parent / "master_instrument" / "etl" / "loading" / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


class TestMergeInheritanceBasic:
    """Basic inheritance tests."""
    
    def test_basic_inheritance(self):
        """Basic inheritance test (parent + child tables)."""
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
                'parent_columns': ['instrument_id', 'name', 'description'],
                'child_columns': ['equity_id', 'instrument_id', 'ticker', 'shares_outstanding']
            },
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Must have parent operations
        assert 'MERGE INTO master.instrument' in sql
        assert 'instrument_id' in sql
        
        # Must have child operations
        assert 'MERGE INTO master.equity' in sql
        assert 'equity_id' in sql
        assert 'ticker' in sql


class TestMergeInheritanceCriticalBugs:
    """Tests for critical bugs fixed."""
    
    def test_parent_on_clause_uses_source_parent_key(self):
        """CRITICAL: Parent ON must use source_parent_key, NOT unique_key!
        
        ORIGINAL BUG: ON src.internal_entity_id = tgt.internal_entity_id
        CORRECT: ON src.internal_entity_id = tgt.entity_id
        """
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name'],
            'data_columns': ['name'],
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
                'child_columns': ['company_id', 'entity_id', 'name']
            },
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Parent ON clause: src.internal_entity_id = tgt.entity_id
        assert 'src.internal_entity_id = tgt.entity_id' in sql
    
    def test_child_update_excludes_fk(self):
        """CRITICAL: Child UPDATE SET must NOT include the parent FK.
        
        ORIGINAL BUG: Child UPDATE included entity_id = src.entity_id
        CORRECT: entity_id must only be in INSERT, not UPDATE
        """
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'entity_id', 'name', 'ticker'],
            'data_columns': ['name', 'ticker'],
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
                'child_columns': ['company_id', 'entity_id', 'name', 'ticker']
            },
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Parse child MERGE section
        child_merge_start = sql.find('MERGE INTO master.company')
        assert child_merge_start != -1, "Child MERGE not found"
        
        child_section = sql[child_merge_start:]
        update_start = child_section.find('WHEN MATCHED THEN UPDATE SET')
        update_end = child_section.find('WHEN NOT MATCHED')
        
        if update_start != -1 and update_end != -1:
            update_section = child_section[update_start:update_end]
            
            # ticker must be in UPDATE SET
            assert 'ticker = src.ticker' in update_section
            
            # entity_id must NOT be in UPDATE SET
            assert 'entity_id = src.entity_id' not in update_section
            assert 'entity_id = src.internal_entity_id' not in update_section
    
    def test_child_insert_includes_fk(self):
        """Child INSERT must include the FK (company_id = entity_id via inheritance)."""
        params = {
            'target_table': 'master.company',
            'source_table': 'intermediate.int_company',
            'unique_key': ['company_id'],
            'columns': ['company_id', 'name', 'employee_count'],
            'data_columns': ['name', 'employee_count'],
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
                'child_columns': ['company_id', 'name', 'employee_count']  # company_id = FK to entity_id
            },
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Child INSERT must have company_id (which gets entity_id via inheritance)
        child_section = sql[sql.find('MERGE INTO master.company'):]
        insert_section = child_section[child_section.find('WHEN NOT MATCHED'):]
        
        # company_id must appear in INSERT
        assert 'company_id' in insert_section
        # And the value comes from entity_id via src.entity_id
        assert 'src.entity_id' in insert_section


class TestMergeInheritanceWithSelfReference:
    """Tests inheritance + self-reference combined."""
    
    def test_inheritance_plus_self_reference(self):
        """Test inheritance + self-reference ensemble."""
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
            'with_audit': False,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        # Must have parent and child
        assert 'master.entity' in sql
        assert 'master.company' in sql
        
        # Must have self-reference UPDATE
        assert 'parent_company_id = src.parent_company_id' in sql
        assert 'self_map.external_entity_id = src.external_id' in sql


class TestMergeInheritanceWithAudit:
    """Tests with audit columns."""
    
    @pytest.mark.parametrize("with_audit", [True, False])
    def test_inheritance_with_audit(self, with_audit):
        """Test inheritance with audit."""
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
            'with_audit': with_audit,
            'with_soft_delete': False,
            'hard_delete': False
        }
        
        if with_audit:
            params['audit'] = {
                'with_created_at': True,
                'with_updated_at': True,
                'with_deleted_at': False
            }
        
        template = jinja_env.get_template('merge.sql.j2')
        sql = template.render(**params)
        
        if with_audit:
            assert 'created_at' in sql
            assert 'updated_at' in sql


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
