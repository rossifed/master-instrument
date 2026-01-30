"""Unit tests for InheritanceConfig with parent_config pattern."""

import pytest

from master_instrument.etl.loading.configs import InheritanceConfig, MergeConfig
from master_instrument.db.models import (
    Company,
    Entity,
    Equity,
    Instrument,
)


def test_inheritance_config_with_parent_config_company():
    """Test InheritanceConfig with parent_config for Company/Entity."""
    # 1. Create parent config
    parent_config = MergeConfig.from_model(
        Entity,
        source_table="test.source",
        unique_key="entity_id",
        schema="test",
        
    )
    
    # 2. Create child config with inheritance
    child_config = MergeConfig.from_model(
        Company,
        source_table="test.source",
        unique_key="company_id",
        schema="test",
        exclude_columns=["sys_period", "latest_interim_financial_date"],
        inheritance=InheritanceConfig(
            parent_config=parent_config,
            source_parent_key="internal_company_id"
        )
    )
    
    config = child_config.inheritance
    
    # Verify parent info extracted from parent_config
    assert config.parent_table == "test.entity"
    assert config.parent_unique_key == "entity_id"
    assert config.parent_columns == parent_config.columns  # Directly from parent_config
    assert "name" in config.parent_columns
    assert "description" in config.parent_columns
    assert "entity_type_id" in config.parent_columns
    assert "entity_id" not in config.parent_columns  # SERIAL PK excluded (autoincrement=True)
    assert "sys_period" not in config.parent_columns  # Excluded in parent_config
    assert len(config.parent_columns) == 3  # name, description, entity_type_id
    
    # Verify child info derived from model
    assert config.child_table == "test.company"
    assert config.child_unique_key == "company_id"
    assert config.child_columns == child_config.columns  # From child MergeConfig
    assert "employee_count" in config.child_columns
    assert "country_id" in config.child_columns
    assert "company_id" in config.child_columns  # PK included in child columns
    assert "sys_period" not in config.child_columns
    assert "latest_interim_financial_date" not in config.child_columns
    
    assert config.source_parent_key == "internal_company_id"


def test_inheritance_config_requires_parent_config():
    """Test that InheritanceConfig requires parent_config (simplified API)."""
    parent = MergeConfig.from_model(
        Entity,
        source_table="intermediate.int_company",
        unique_key="entity_id",
        schema="test"
    )
    
    config = InheritanceConfig(
        parent_config=parent,
        source_parent_key="internal_company_id"
    )
    
    assert config.parent_table == "test.entity"
    assert config.parent_unique_key == "entity_id"
