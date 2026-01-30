"""Loading schemes and configuration dataclasses."""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Dict, Type, Any

from master_instrument.etl.loading.utils.tables import (
    parse_table_name,
    build_qualified_table_name,
    find_columns_by_pattern,
    validate_single_column,
    validate_unique_key,
)
from master_instrument.etl.loading.utils.columns import (
    get_all_columns,
    detect_fk_column,
    detect_fk_by_column_pattern,
)



class LoadingScheme(Enum):
    """Available loading schemes (SQL templates)."""
    MERGE = "merge"
    UPSERT = "upsert"
    CDC_CHANGES = "cdc_changes"
    INSERT = "insert"


@dataclass
class SelfReferenceConfig:
    """Configuration for self-referencing FK columns.
    
    Transversal - works with any loading scheme (MERGE, INSERT, UPSERT).
    Used when a table has FK columns pointing to itself.
    
    Example: Company table with primary_company_id → company_id FK.
    Cannot be resolved in initial MERGE because target IDs don't exist yet.
    Requires separate UPDATE statement after merge + mapping resolution.
    
    Args:
        columns: Dict mapping {target_column: source_column}
        requires_mapping: If True, uses mapping table to resolve external IDs
    
    Example:
        SelfReferenceConfig(
            columns={
                "primary_company_id": "primary_external_id",
                "ultimate_organization_id": "ultimate_external_id"
            },
            requires_mapping=True  # Use entity_mapping to resolve IDs
        )
    """
    columns: Dict[str, str]
    requires_mapping: bool = False


@dataclass
class MappingConfig:
    """Configuration for external/internal ID mapping tables.
    
    Can be created from a mapping model for automatic derivation:
        mapping = MappingConfig.from_model(
            EntityMapping,
            source_external_id_column="external_id",  # Source column
            data_source_column="data_source_id"  # Source column (optional if same as model)
        )
        # Auto-derives:
        # - mapping_table from model (master.entity_mapping)
        # - mapping_external_column from model (external_entity_id)
        # - mapping_internal_column from model (internal_entity_id)
        # - data_source_column validated against model
    
    Or created manually:
        mapping = MappingConfig(
            mapping_table="master.entity_mapping",
            source_external_id_column="external_id",
            data_source_column="data_source_id",
            mapping_external_column="external_entity_id",
            mapping_internal_column="internal_entity_id"
        )
    """
    mapping_table: str
    source_external_id_column: str
    data_source_column: str
    mapping_external_column: str
    mapping_internal_column: str
    mapping_type_column: Optional[str] = None  # e.g., entity_type_id, instrument_type_id
    source_type_column: Optional[str] = None   # Source column for type (default: same as mapping)
    
    @classmethod
    def from_model(
        cls,
        model: Type[Any],
        source_external_id_column: Optional[str] = None,
        data_source_column: Optional[str] = None,
        schema: Optional[str] = None
    ) -> "MappingConfig":
        """Create MappingConfig from SQLAlchemy mapping model.
        
        Convention: Source and mapping table use the SAME column names.
        - If mapping has external_company_id, source should also have external_company_id
        - This ensures consistency and eliminates mapping mental overhead
        
        Auto-derives configuration using foreign key inspection:
        - data_source_column: FK to data_source table (default: "data_source_id")
        - mapping_internal_column: FK to target entity table (auto-detected)
        - mapping_external_column: Column containing "external" (auto-detected)
        - source_external_id_column: Same as mapping_external_column (consistent convention)
        
        Args:
            model: SQLAlchemy mapping model (e.g., EntityMapping)
            source_external_id_column: Column name in SOURCE (default: same as mapping)
            data_source_column: Column name in SOURCE (default: "data_source_id")
            schema: Override schema (default: use model's schema)
            
        Returns:
            MappingConfig with auto-derived columns
            
        Example:
            # Minimal usage - source and mapping use same names
            # EntityMapping has external_entity_id → source must have external_entity_id
            mapping = MappingConfig.from_model(EntityMapping)
            
            # With override (if source uses different naming - not recommended)
            mapping = MappingConfig.from_model(
                EntityMapping,
                source_external_id_column="ext_id"
            )
        """
        if not hasattr(model, '__table__'):
            raise ValueError(f"Model {model} must be a SQLAlchemy model with __table__")
        
        table = model.__table__
        mapping_table = build_qualified_table_name(schema, table.schema, table.name)
        col_names = [col.name for col in table.columns]
        
        detected_ds = detect_fk_column(table, 'data_source', 'data_source_id') or 'data_source_id'
        final_data_source_column = data_source_column or detected_ds
        
        detected_internal = detect_fk_by_column_pattern(table, 'internal_')
        
        external_cols = find_columns_by_pattern(col_names, 'external')
        internal_cols = find_columns_by_pattern(col_names, 'internal_', prefix=True)
        
        mapping_external_column = validate_single_column(external_cols, "column containing 'external'")
        mapping_internal_column = detected_internal or validate_single_column(internal_cols, "'internal_*' column")
        
        # Detect type column (entity_type_id, instrument_type_id, venue_type_id)
        mapping_type_cols = [c for c in col_names if c in ('entity_type_id', 'instrument_type_id', 'venue_type_id')]
        mapping_type_column = mapping_type_cols[0] if mapping_type_cols else None
        
        # Default: source uses same column name as mapping table (consistent convention)
        # e.g., if mapping has external_company_id, source should also have external_company_id
        final_source_external_id_column = source_external_id_column or mapping_external_column
        
        return cls(
            mapping_table=mapping_table,
            source_external_id_column=final_source_external_id_column,
            data_source_column=final_data_source_column,
            mapping_external_column=mapping_external_column,
            mapping_internal_column=mapping_internal_column,
            mapping_type_column=mapping_type_column,
            source_type_column=mapping_type_column  # Default: same name in source
        )


def _extract_unique_key_as_string(unique_key: Any) -> str:
    """Extract unique key as single string (first element if list)."""
    return unique_key if isinstance(unique_key, str) else unique_key[0]


def _get_single_pk_column(table: Any) -> str:
    """Extract single primary key column name, raise if multiple."""
    pk_cols = [col.name for col in table.primary_key.columns]
    if len(pk_cols) != 1:
        raise ValueError(f"Child model must have single PK, found: {pk_cols}")
    return pk_cols[0]


def _get_parent_table_name(parent_table: Optional[str]) -> Optional[str]:
    """Extract table name from qualified name."""
    return parent_table.split('.')[-1] if parent_table else None


def _validate_child_pk_is_fk(child_pk_col: Any, pk_name: str) -> None:
    """Validate that child PK has FK constraint."""
    if not list(child_pk_col.foreign_keys):
        raise ValueError(f"Child PK '{pk_name}' must be FK to parent table")


def _validate_fk_points_to_parent(fk: Any, expected_parent: Optional[str]) -> None:
    """Validate FK points to expected parent table."""
    if expected_parent and fk.column.table.name != expected_parent:
        raise ValueError(
            f"Child PK FK points to '{fk.column.table.name}', expected '{expected_parent}'"
        )


@dataclass
class InheritanceConfig:
    """Configuration for parent-child table inheritance.
    
    Requires parent_config (complete MergeConfig for parent table).
    Child fields are auto-derived from the child model via derive_child_from_model().
    
    Example:
        # 1. Define parent merge config
        parent_config = MergeConfig.from_model(
            Entity,
            source_table="intermediate.int_company",
            unique_key="entity_id",
            schema="test"
        )
        
        # 2. Define child with inheritance
        child_config = MergeConfig.from_model(
            Company,
            source_table="intermediate.int_company",
            unique_key="company_id",
            schema="test",
            inheritance=InheritanceConfig(
                parent_config=parent_config,
                source_parent_key="internal_company_id"
            )
        )
    """
    parent_config: "MergeConfig"
    source_parent_key: str
    
    # Derived fields (populated from parent_config and child model)
    parent_table: str = ""
    parent_unique_key: str = ""
    parent_columns: Optional[List[str]] = None
    child_table: Optional[str] = None
    child_unique_key: Optional[str] = None
    child_columns: Optional[List[str]] = None
    
    def __post_init__(self):
        """Extract parent info from parent_config."""
        self.parent_table = self.parent_config.target_table
        self.parent_unique_key = _extract_unique_key_as_string(self.parent_config.unique_key)
        self.parent_columns = self.parent_config.columns
    
    def derive_child_from_model(self, child_model: Any, child_schema: Optional[str] = None):
        """Derive child table info from model.
        
        Called automatically by MergeConfig.from_model() when inheritance is present.
        """
        child_table_obj = child_model.__table__
        
        self.child_table = build_qualified_table_name(child_schema, child_table_obj.schema, child_table_obj.name)
        self.child_unique_key = _get_single_pk_column(child_table_obj)
        
        # Validate FK relationship
        parent_table_name = _get_parent_table_name(self.parent_table)
        child_pk_col = child_table_obj.columns[self.child_unique_key]
        
        _validate_child_pk_is_fk(child_pk_col, self.child_unique_key)
        fk = list(child_pk_col.foreign_keys)[0]
        _validate_fk_points_to_parent(fk, parent_table_name)


@dataclass
class AuditConfig:
    """Configuration for audit columns (created_at, updated_at, deleted_at).
    
    Each flag is independent - choose only what you need:
    - with_created_at: Track when row was first inserted
    - with_updated_at: Track when row was last modified
    - with_deleted_at: Enable soft delete (UPDATE deleted_at instead of DELETE)
    
    Examples:
        # Time-series: only track insertion time
        AuditConfig(with_created_at=True)
        
        # Reference data: track creation and updates
        AuditConfig(with_created_at=True, with_updated_at=True)
        
        # Master data: full audit trail with soft delete
        AuditConfig(with_created_at=True, with_updated_at=True, with_deleted_at=True)
    """
    with_created_at: bool = False
    with_updated_at: bool = False
    with_deleted_at: bool = False
    
    def to_template_params(self) -> Dict[str, bool]:
        """Convert to template parameters dictionary.
        
        Returns:
            Dict with with_created_at, with_updated_at, with_deleted_at flags
        """
        return {
            "with_created_at": self.with_created_at,
            "with_updated_at": self.with_updated_at,
            "with_deleted_at": self.with_deleted_at
        }


@dataclass
class BatchConfig:
    """Configuration for batch loading operations.

    Transversal - applicable to all loading schemes (MERGE, INSERT, CDC, UPSERT).
    Enables incremental loading by date ranges with associated behaviors.

    Examples:
        # Simple batching
        BatchConfig(batch_date_column="trade_date")

        # Full batch reload with optimizations
        BatchConfig(
            batch_date_column="trade_date",
            truncate_before_load=True,
            drop_indexes=True,
            drop_fk=True,  # Drop Foreign Keys during load
            fail_fast=True,
            disable_wal=True,  # For massive bulk loads
            disable_autovacuum=True
        )
        
        # Maximum performance (aggressive)
        BatchConfig(
            batch_date_column="trade_date",
            truncate_before_load=True,
            drop_indexes=True,
            drop_fk=True,
            drop_pk=True,      # Also drop Primary Key
            drop_unique=True,  # Also drop UNIQUE constraints
            disable_wal=True,
            disable_autovacuum=True
        )

    """
    batch_date_column: str

    truncate_before_load: bool = False
    drop_indexes: bool = False  # Drop/recreate indexes for performance
    
    # Constraint management (granular control)
    drop_fk: bool = False      # Drop/recreate Foreign Key constraints
    drop_pk: bool = False      # Drop/recreate Primary Key (use with caution)
    drop_unique: bool = False  # Drop/recreate UNIQUE constraints
    
    fail_fast: bool = True  # Stop on first batch error
    allow_partial_success: bool = False  # Don't raise if some batches fail

    # Bulk load optimizations (for large data volumes)
    disable_wal: bool = False  # Set table UNLOGGED during load (no WAL overhead)
    disable_autovacuum: bool = False  # Disable autovacuum during load
    
    # TimescaleDB hypertables don't support CONCURRENTLY for index creation
    is_hypertable: bool = False


@dataclass
class MergeConfig:
    """Configuration for MERGE loading."""
    target_table: str
    source_table: str
    unique_key: str | List[str]
    columns: List[str]
    scheme: LoadingScheme = LoadingScheme.MERGE
    
    with_soft_delete: bool = False
    hard_delete: bool = False
    
    source_columns: Optional[List[str]] = None
    source_unique_key: Optional[str | List[str]] = None  # If different from unique_key in source
    order_by: Optional[str] = None
    exclude_from_delete: Optional[str] = None
    
    mapping: Optional[MappingConfig] = None
    inheritance: Optional[InheritanceConfig] = None
    self_reference: Optional[SelfReferenceConfig] = None
    audit: Optional[AuditConfig] = None
    batch: Optional[BatchConfig] = None
    
    # TimescaleDB hypertable support
    is_hypertable: bool = False  # If True, sets timescaledb decompression limit
    hypertable_decompression_limit: int = 0  # Max tuples to decompress per DML (0 = unlimited)
    
    @property
    def data_columns(self) -> List[str]:
        """Calculate non-PK columns automatically.
        
        Returns:
            List of column names that are not part of the primary key
        """
        pk_list = [self.unique_key] if isinstance(self.unique_key, str) else self.unique_key
        return [col for col in self.columns if col not in pk_list]
    
    @property
    def target_schema(self) -> str:
        """Extract schema from target_table."""
        schema, _ = parse_table_name(self.target_table)
        return schema
    
    @property
    def target_table_name(self) -> str:
        """Extract table name from target_table."""
        _, table_name = parse_table_name(self.target_table)
        return table_name
    
    def to_template_params(self) -> Dict[str, Any]:
        """Convert config to template parameters.
        
        Single source of truth for template parameter generation.
        Handles both standard and inheritance modes.
        """
        if self.inheritance:
            return self._build_inheritance_params()
        return self._build_standard_params()
    
    def _build_standard_params(self) -> Dict[str, Any]:
        """Build standard (non-inheritance) template parameters."""
        params: Dict[str, Any] = {
            "target_table": self.target_table,
            "source_table": self.source_table,
            "unique_key": self.unique_key,
            "source_unique_key": self.source_unique_key,
            "columns": self.columns,
            "data_columns": self.data_columns,
            "with_soft_delete": self.with_soft_delete,
            "hard_delete": self.hard_delete,
            "audit": self.audit.to_template_params() if self.audit else None,
        }
        
        # Optional params
        if self.source_columns:
            params["source_columns"] = self.source_columns
        if self.order_by:
            params["order_by"] = self.order_by
        if self.exclude_from_delete:
            params["exclude_from_delete"] = self.exclude_from_delete
        if self.mapping:
            params["mapping"] = self.mapping
        if self.batch:
            params["batch"] = self.batch
        if self.self_reference:
            params["self_reference"] = self.self_reference
        
        # TimescaleDB hypertable settings
        params["is_hypertable"] = self.is_hypertable
        params["hypertable_decompression_limit"] = self.hypertable_decompression_limit
        
        return params
    
    def _build_inheritance_params(self) -> Dict[str, Any]:
        """Build inheritance-specific template parameters."""
        return {
            "source_table": self.source_table,
            "unique_key": self.unique_key,
            "inheritance": self.inheritance,
            "mapping": self.mapping,
            "self_reference": self.self_reference,
            "batch": self.batch,
            "with_soft_delete": self.with_soft_delete,
            "hard_delete": self.hard_delete,
            "audit": self.audit.to_template_params() if self.audit else None,
            "order_by": self.order_by,
            "exclude_from_delete": self.exclude_from_delete,
        }
    
    def __post_init__(self):
        """Validate configuration."""
        if self.with_soft_delete and self.hard_delete:
            raise ValueError("with_soft_delete and hard_delete are mutually exclusive")
        
        if self.with_soft_delete and (not self.audit or not self.audit.with_deleted_at):
            raise ValueError(
                "with_soft_delete=True requires audit=AuditConfig(with_deleted_at=True)"
            )
    
    @classmethod
    def from_model(
        cls,
        model: Type[Any],
        source_table: str,
        unique_key: Optional[str | List[str]] = None,
        schema: Optional[str] = None,
        exclude_columns: Optional[List[str]] = None,
        exclude_mixins: bool = True,
        auto_exclude_server_defaults: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
        **kwargs: Any
    ) -> "MergeConfig":
        """Create MergeConfig from SQLAlchemy model.
        
        Auto-derives:
        - target_table (from model.__table__)
        - columns (all columns except exclusions)
        - unique_key (from PK if not provided, see below)
        
        Validates:
        - unique_key matches an actual PK or UniqueConstraint in the model
        
        Args:
            model: SQLAlchemy model class (e.g., MarketData)
            source_table: DBT view or source table (e.g., "intermediate.int_market_data")
            unique_key: Primary/unique key columns (optional).
                If not provided, auto-derived from model's primary key.
                WARNING: Auto-derivation will fail for single-column auto-incremented PKs
                (e.g., SERIAL id) as these are usually not the merge key.
                Examples: ["trade_date", "quote_id"] or "country_code"
            schema: Override schema (default: use model's schema)
            exclude_columns: Explicit columns to exclude (default: None)
            exclude_mixins: Auto-exclude mixin columns like created_at/updated_at (default: True)
            column_mapping: Map target columns to source columns (default: None)
                Format: {"target_col": "source_col"}
                Example: {"valuation_date": "date"} means valuation_date comes from source.date
            **kwargs: Additional config parameters (batch, audit, mapping, etc.)
            
        Returns:
            MergeConfig instance with auto-derived values
            
        Raises:
            ValueError: If unique_key doesn't match any constraint in the model
            ValueError: If unique_key is not provided and PK is auto-incremented
            
        Examples:
            # Explicit unique_key (validated against model)
            config = MergeConfig.from_model(
                MarketData,
                source_view="intermediate.int_market_data",
                unique_key=["trade_date", "quote_id"],  # Must match PK in MarketData
                batch=BatchConfig(batch_date_column="trade_date")
            )
            
            # Single-column unique key
            config = MergeConfig.from_model(
                Currency,
                source_view="intermediate.int_currency",
                unique_key="code"  # Must match PK or unique constraint
            )
            
            # With column mapping
            config = InsertConfig.from_model(
                CompanyMarketCap,
                source_view="intermediate.int_company_market_cap_full",
                unique_key=["valuation_date", "company_id"],
                column_mapping={
                    "valuation_date": "date"  # target <- source
                }
            )
        """
        if not hasattr(model, '__table__'):
            raise ValueError(f"Model {model} must be a SQLAlchemy model with __table__")
        
        table = model.__table__
        
        # Auto-derive unique_key from PK if not provided
        if unique_key is None:
            pk_cols = [col.name for col in table.primary_key.columns]
            if len(pk_cols) == 0:
                raise ValueError(f"Model {model.__name__} has no primary key. unique_key must be provided.")
            if len(pk_cols) == 1:
                # Check if single PK is auto-incremented
                pk_col = table.c[pk_cols[0]]
                if pk_col.autoincrement is True or (hasattr(pk_col, 'identity') and pk_col.identity):
                    raise ValueError(
                        f"Model {model.__name__} has auto-incremented PK '{pk_cols[0]}'. "
                        f"This is likely not the merge key. Please provide unique_key explicitly."
                    )
            unique_key = pk_cols if len(pk_cols) > 1 else pk_cols[0]
        
        # Normalize unique_key to list
        unique_key_list = [unique_key] if isinstance(unique_key, str) else list(unique_key)
        
        validate_unique_key(table, unique_key_list)
        
        schema_name = schema or table.schema or 'public'
        target_table = f"{schema_name}.{table.name}"
        
        columns = get_all_columns(
            table,
            exclude_columns=exclude_columns,
            exclude_mixins=exclude_mixins,
            auto_exclude_server_defaults=auto_exclude_server_defaults,
            model=model
        )
        
        source_columns: Optional[List[str]] = None
        if column_mapping:
            source_columns = []
            for target_col in columns:
                source_col = column_mapping.get(target_col, target_col)
                source_columns.append(source_col)
        
        inheritance = kwargs.get('inheritance')
        if inheritance and hasattr(inheritance, 'parent_config') and inheritance.parent_config is not None:
            inheritance.derive_child_from_model(child_model=model, child_schema=schema_name)
            inheritance.child_columns = columns
        
        kwargs.pop('source_columns', None)
        
        return cls(
            target_table=target_table,
            source_table=source_table,
            columns=columns,
            unique_key=unique_key,
            source_columns=source_columns,
            **kwargs
        )


@dataclass
class UpsertConfig(MergeConfig):
    """Configuration for UPSERT (INSERT ON CONFLICT) loading."""
    scheme: LoadingScheme = LoadingScheme.UPSERT


@dataclass
class CDCConfig(MergeConfig):
    """Configuration for CDC changes loading.
    
    Note: unique_key is automatically normalized to a list for CDC.
    """
    tracking_table: Optional[str] = None
    is_hypertable: bool = False  # If True, sets timescaledb decompression limit
    hypertable_decompression_limit: int = 2000000  # Max tuples to decompress per DML
    scheme: LoadingScheme = LoadingScheme.CDC_CHANGES
    
    def __post_init__(self):
        super().__post_init__()
        
        # Normalize unique_key to list for CDC
        if isinstance(self.unique_key, str):
            self.unique_key = [self.unique_key]
    
    def to_template_params(self) -> Dict[str, Any]:
        """Build CDC-specific template parameters."""
        params = super()._build_standard_params()
        
        # Override unique_key as list for CDC template
        params["unique_key"] = (
            self.unique_key if isinstance(self.unique_key, list) 
            else [self.unique_key]
        )
        
        if self.tracking_table:
            params["tracking_table"] = self.tracking_table
        
        # Hypertable settings
        params["is_hypertable"] = self.is_hypertable
        params["hypertable_decompression_limit"] = self.hypertable_decompression_limit
        
        return params


@dataclass
class InsertConfig(MergeConfig):
    """Configuration for INSERT loading."""
    source_column_mapping: Optional[Dict[str, str]] = None
    scheme: LoadingScheme = LoadingScheme.INSERT
    
    def to_template_params(self) -> Dict[str, Any]:
        """Build INSERT-specific template parameters."""
        params = super()._build_standard_params()
        
        if self.source_column_mapping:
            params["source_column_mapping"] = self.source_column_mapping
        
        return params


