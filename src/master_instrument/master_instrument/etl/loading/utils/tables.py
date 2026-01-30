"""Table and column utilities for loading operations.

Pure functions for:
- Table name parsing, building, and identifier quoting
- Column pattern matching and validation
- Table constraint validation (unique keys, primary keys)
- Table metadata extraction (PKs, unique constraints)
"""

from typing import List, Tuple, Optional, Set, Any
from sqlalchemy.schema import Table


def parse_table_name(qualified_name: str) -> Tuple[str, str]:
    """Parse schema.table format into components.
    
    Args:
        qualified_name: Qualified table name (e.g., 'master.country')
        
    Returns:
        Tuple of (schema, table_name)
        
    Raises:
        ValueError: If format is not 'schema.table'
    """
    parts = qualified_name.split('.')
    if len(parts) != 2:
        raise ValueError(
            f"Table name must be in format 'schema.table', got: '{qualified_name}'"
        )
    return parts[0], parts[1]


parse_target_table = parse_table_name


def build_qualified_name(schema: str, table: str) -> str:
    """Build qualified table name from schema and table.
    
    Args:
        schema: Schema name
        table: Table name
        
    Returns:
        Qualified name in 'schema.table' format
    """
    return f"{schema}.{table}"


def build_qualified_table_name(schema: str | None, table_schema: str | None, table_name: str) -> str:
    """Build schema-qualified table name with optional overrides.
    
    Args:
        schema: Explicit schema override
        table_schema: Schema from table metadata
        table_name: Base table name
        
    Returns:
        Qualified name in format 'schema.table'
    """
    schema_name = schema or table_schema or 'public'
    return f"{schema_name}.{table_name}"


def quote_identifier(identifier: str) -> str:
    """Quote identifier for PostgreSQL.
    
    Args:
        identifier: Column or table identifier
        
    Returns:
        Quoted identifier
    """
    return f'"{identifier}"'


def is_internal_id_pattern(column_name: str) -> bool:
    """Check if column name follows internal_* pattern."""
    return column_name.startswith('internal_')


def find_columns_by_pattern(col_names: List[str], pattern: str, prefix: bool = False) -> List[str]:
    """Find columns matching a pattern.
    
    Args:
        col_names: All column names to search
        pattern: Pattern to match
        prefix: If True, match as prefix; else match anywhere (case-insensitive)
        
    Returns:
        List of matching column names
    """
    if prefix:
        return [c for c in col_names if c.startswith(pattern)]
    return [c for c in col_names if pattern in c.lower()]


def validate_single_column(columns: List[str], description: str) -> str:
    """Validate that exactly one column was found.
    
    Args:
        columns: Found columns
        description: Description for error message
        
    Returns:
        The single column name
        
    Raises:
        ValueError: If not exactly one column found
    """
    if len(columns) != 1:
        raise ValueError(f"Expected exactly 1 {description}, found: {columns}")
    return columns[0]


def get_pk_columns(table: Table) -> Set[str]:  # type: ignore[type-arg]
    """Extract primary key column names."""
    return {col.name for col in table.primary_key.columns}


def get_unique_constraint_columns(table: Table) -> List[Set[str]]:  # type: ignore[type-arg]
    """Extract all unique constraint column sets."""
    from sqlalchemy import UniqueConstraint
    constraints: List[Set[str]] = []
    for constraint in table.constraints:
        if isinstance(constraint, UniqueConstraint):
            constraints.append({col.name for col in constraint.columns})
    return constraints


def get_unique_columns(table: Table) -> Set[str]:  # type: ignore[type-arg]
    """Extract individual columns marked as unique."""
    return {col.name for col in table.columns if col.unique}


def is_pk_match(unique_key_set: Set[str], table: Table) -> bool:  # type: ignore[type-arg]
    """Check if key matches primary key."""
    return unique_key_set == get_pk_columns(table)


def is_unique_constraint_match(unique_key_set: Set[str], table: Table) -> bool:  # type: ignore[type-arg]
    """Check if key matches any unique constraint."""
    return unique_key_set in get_unique_constraint_columns(table)


def is_single_unique_column_match(unique_key: List[str], table: Table) -> bool:  # type: ignore[type-arg]
    """Check if single column key matches unique column."""
    return len(unique_key) == 1 and unique_key[0] in get_unique_columns(table)


def matches_constraint(unique_key: List[str], table: Table) -> bool:  # type: ignore[type-arg]
    """Check if unique_key matches any constraint in the table."""
    unique_key_set = set(unique_key)
    return (
        is_pk_match(unique_key_set, table) or
        is_unique_constraint_match(unique_key_set, table) or
        is_single_unique_column_match(unique_key, table)
    )


def format_constraint_error(table: Table, unique_key: List[str]) -> str:  # type: ignore[type-arg]
    """Format constraint validation error message."""
    pk = list(get_pk_columns(table)) or None
    unique = list(get_unique_columns(table)) or None
    return (
        f"unique_key {unique_key} does not match any constraint on table '{table.name}'. "
        f"Available constraints: PK={pk}, Unique columns={unique}"
    )


def validate_unique_key(table: Table, unique_key: List[str]) -> None:  # type: ignore[type-arg]
    """Validate that unique_key corresponds to an actual constraint.
    
    Args:
        table: SQLAlchemy Table
        unique_key: List of column names to validate
        
    Raises:
        ValueError: If unique_key doesn't match any constraint
    """
    if not matches_constraint(unique_key, table):
        raise ValueError(format_constraint_error(table, unique_key))
