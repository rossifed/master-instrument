"""Data loader services - orchestration for executing data loads."""

from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Protocol, Optional, Generator, Any, TypedDict, Dict, Tuple
from datetime import datetime
from dagster import get_dagster_logger

from master_instrument.etl.loading.sources import DataSource, TemplateSource
from master_instrument.etl.loading.batching import BatchStrategy
from master_instrument.etl.loading.utils.database import (
    EngineType,
    extract_engine,
    get_non_pk_indexes,
    drop_indexes,
    recreate_indexes,
    truncate_table,
    bulk_load_mode,
    get_foreign_keys,
    get_primary_key,
    get_unique_constraints,
    drop_constraints,
    recreate_constraints,
)
from master_instrument.etl.loading.utils.tables import (
    parse_target_table,
    build_qualified_name,
    quote_identifier
)


def _extract_target_info(source: DataSource) -> Tuple[str, str]:
    """Extract target and key info from source config."""
    if not isinstance(source, TemplateSource) or not source.config:
        return "unknown", "unknown"
    
    config = source.config
    
    if hasattr(config, 'inheritance') and config.inheritance:
        target_info = f"{config.inheritance.parent_table} + {config.inheritance.child_table}"
        key_info = f"parent={config.inheritance.parent_unique_key}, child={config.inheritance.child_unique_key}"
    else:
        target_info = config.target_table
        key_info = str(config.unique_key)
    
    return target_info, key_info


def _format_sql_block(sql: str) -> str:
    """Format SQL with separator lines."""
    separator = '-' * 80
    return f"{separator}\n{sql}\n{separator}"


def _format_batch_success(event: Dict[str, Any]) -> str:
    """Format batch success message."""
    return (
        f"✓ Batch {event['batch']}/{event['total']}: "
        f"{event['start_date']}→{event['end_date']} "
        f"({event['duration']:.1f}s)"
    )


def _format_batch_error(event: Dict[str, Any]) -> str:
    """Format batch error message."""
    return (
        f"✗ Batch {event['batch']}/{event['total']}: "
        f"{event['start_date']}→{event['end_date']} - "
        f"{event['error']}"
    )


def _format_batch_range(batches: list[Tuple[str, str]]) -> str:
    """Format batch date range summary."""
    if not batches:
        return "no batches"
    return f"{batches[0][0]} → {batches[-1][1]}"


class BatchResult(TypedDict):
    """Structured result from batch loading operations."""
    status: str
    total: int
    successful: int
    failed: int
    successful_batches: list[dict[str, Any]]
    failed_batches: list[dict[str, Any]]


class Logger(Protocol):
    def info(self, msg: str) -> None: ...
    def error(self, msg: str) -> None: ...


class LoaderProtocol(Protocol):
    def load(self, **kwargs: Any) -> Optional[dict[str, Any]]: ...


class SimpleLoader:
    """Execute SQL from any DataSource (file, template, etc.) once.
    
    Supports both simple file-based loads and template-based loads with full metadata.
    """
    
    def __init__(
        self,
        engine: EngineType,
        source: DataSource,
        logger: Optional[Logger] = None
    ):
        self.engine: Engine = extract_engine(engine)
        self.source = source
        self.logger = logger or get_dagster_logger()
    
    def load(self, params: Optional[Dict[str, Any]] = None, dry_run: bool = False) -> Dict[str, Any]:
        """Execute load from source.
        
        Args:
            params: Optional parameters for SQL generation (e.g., date filters)
            dry_run: If True, return generated SQL without executing
            
        Returns:
            Dict with execution results (sql, rowcount, target, scheme, etc.)
        """
        sql = self.source.read(params or {})
        
        target_info, key_info = _extract_target_info(self.source)
        
        if isinstance(self.source, TemplateSource) and self.source.config:
            config = self.source.config
            self.logger.info(f"Loading {target_info} using {config.scheme.value}")
            self.logger.info(f"Source: {config.source_table}, Key: {key_info}")
        
        self.logger.info(f"Generated SQL:\n{_format_sql_block(sql)}")
        
        if dry_run:
            return {
                "dry_run": True,
                "sql": sql,
                "target": target_info[0]
            }
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(sql), params or {})
                rowcount = result.rowcount if hasattr(result, 'rowcount') else 0
            
            self.logger.info(f"✓ Loaded {rowcount} rows")
            
            result_dict: Dict[str, Any] = {
                "rowcount": rowcount,
                "sql": sql,
                "target": target_info[0]
            }
            
            if isinstance(self.source, TemplateSource) and self.source.config:
                result_dict["scheme"] = self.source.config.scheme.value
                if hasattr(self.source.config, 'columns'):
                    result_dict["columns"] = self.source.config.columns
            
            return result_dict
            
        except Exception as e:
            self.logger.error(f"Failed to execute SQL")
            self.logger.error(f"Error: {e}")
            self.logger.error("SQL was:")
            self.logger.error(sql)
            raise


class BatchLoader:
    """Batch data loader with automatic configuration from source.config.batch.
    
    Executes data loading in batches with automatic:
    - Truncate table (if config.batch.truncate_before_load)
    - Drop/recreate indexes (if config.batch.drop_indexes)
    - Auto-derive source_table and date_column from config.batch
    
    Follows same protocol as SimpleLoader for dependency injection.
    """
    
    def __init__(
        self,
        engine: EngineType,
        source: DataSource,
        strategy: BatchStrategy,
        logger: Optional[Logger] = None,
        fail_fast: bool = True,
        allow_partial_success: bool = False
    ):
        self.engine: Engine = extract_engine(engine)
        self.source = source
        self.strategy = strategy
        self.logger = logger
        self.fail_fast = fail_fast
        self.allow_partial_success = allow_partial_success
    
    @staticmethod
    def _aggregate_results(successful: list[dict[str, Any]], failed: list[dict[str, Any]]) -> BatchResult:
        """Pure: aggregate batch results into summary."""
        total = len(successful) + len(failed)
        return {
            "status": "completed" if len(failed) == 0 else "partial_failure",
            "total": total,
            "successful": len(successful),
            "failed": len(failed),
            "successful_batches": successful,
            "failed_batches": failed
        }
    
    def _execute_batches(
        self,
        batch_periods: list[tuple[str, str]]
    ) -> Generator[dict[str, Any], None, None]:
        """Execute data source in batches and yield results.
        
        Yields dict with keys: status, batch, total, start_date, end_date, duration, [error]
        """
        total = len(batch_periods)
        
        for idx, (start_date, end_date) in enumerate(batch_periods, 1):
            batch_start = datetime.now()
            params = {"start_date": start_date, "end_date": end_date}
            sql: str = ""  # Initialize to avoid unbound error
            
            try:
                sql = self.source.read(params)
                
                if idx == 1:
                    if self.logger:
                        self.logger.info(f"Generated SQL (batch 1/{total}):\n{_format_sql_block(sql)}")
                
                with self.engine.begin() as conn:
                    conn.execute(text(sql), params)
                
                duration = (datetime.now() - batch_start).total_seconds()
                
                yield {
                    "status": "success",
                    "batch": idx,
                    "total": total,
                    "start_date": start_date,
                    "end_date": end_date,
                    "duration": duration
                }
                
            except Exception as e:
                duration = (datetime.now() - batch_start).total_seconds()
                
                if self.logger and sql:
                    self.logger.error(f"Failed SQL (batch {idx}/{total}):\n{_format_sql_block(sql)}")
                    self.logger.error(f"Parameters: start_date={start_date}, end_date={end_date}")
                
                yield {
                    "status": "error",
                    "batch": idx,
                    "total": total,
                    "start_date": start_date,
                    "end_date": end_date,
                    "duration": duration,
                    "error": str(e)
                }
                
                if self.fail_fast:
                    raise
    
    def _get_target_schema_table(self) -> tuple[str, str]:
        """Get target schema and table from source config.
        
        Returns:
            Tuple of (schema, table)
            
        Raises:
            ValueError: If config not available or target_table not in schema.table format
        """
        if not isinstance(self.source, TemplateSource) or not self.source.config:
            raise ValueError("Source has no config - cannot auto-derive target table")
        
        return parse_target_table(self.source.config.target_table)
    
    def _execute_truncate(self) -> None:
        """Truncate target table."""
        schema, table = self._get_target_schema_table()
        truncate_table(self.engine, schema, table, self.logger)
    
    def _execute_drop_indexes(self) -> list[dict[str, str]]:
        """Drop all non-PK indexes on target table. Returns list of dropped indexes."""
        schema, table = self._get_target_schema_table()
        
        indexes = get_non_pk_indexes(self.engine, schema, table)
        
        if not indexes:
            if self.logger:
                self.logger.info(f"No indexes to drop on {build_qualified_name(schema, table)}")
            return []
        
        drop_indexes(self.engine, schema, indexes)
        
        if self.logger:
            self.logger.info(f"✓ Dropped {len(indexes)} indexes on {build_qualified_name(schema, table)}")
        
        return indexes
    
    def _execute_recreate_indexes(self, indexes: list[dict[str, str]]) -> None:
        """Recreate indexes from their definitions."""
        if not indexes:
            return
        
        schema, table = self._get_target_schema_table()
        
        # TimescaleDB hypertables don't support CONCURRENTLY
        is_hypertable = (
            isinstance(self.source, TemplateSource) 
            and self.source.config 
            and self.source.config.batch 
            and self.source.config.batch.is_hypertable
        )
        use_concurrent = not is_hypertable
        
        mode_str = "CONCURRENTLY" if use_concurrent else "(non-concurrent, hypertable)"
        if self.logger:
            self.logger.info(f"Recreating {len(indexes)} indexes on {build_qualified_name(schema, table)} {mode_str}...")

        recreate_indexes(self.engine, indexes, concurrently=use_concurrent)

        if self.logger:
            self.logger.info(f"✓ Recreated {len(indexes)} indexes {mode_str}")
    
    # =========================================================================
    # CONSTRAINT MANAGEMENT
    # =========================================================================
    
    def _execute_drop_constraints(
        self, 
        drop_fk: bool = False, 
        drop_pk: bool = False, 
        drop_unique: bool = False
    ) -> list[dict[str, str]]:
        """Drop constraints on target table based on flags.
        
        Args:
            drop_fk: Drop Foreign Key constraints
            drop_pk: Drop Primary Key constraint
            drop_unique: Drop UNIQUE constraints
        
        Returns:
            List of dropped constraints with name, type, definition
        """
        if not (drop_fk or drop_pk or drop_unique):
            return []
        
        schema, table = self._get_target_schema_table()
        full_table = build_qualified_name(schema, table)
        
        # Collect constraints to drop based on flags
        constraints_to_drop: list[dict[str, str]] = []
        
        if drop_fk:
            fks = get_foreign_keys(self.engine, schema, table)
            constraints_to_drop.extend(fks)
        
        if drop_pk:
            pk = get_primary_key(self.engine, schema, table)
            if pk:
                constraints_to_drop.append(pk)
        
        if drop_unique:
            uniques = get_unique_constraints(self.engine, schema, table)
            constraints_to_drop.extend(uniques)
        
        if not constraints_to_drop:
            if self.logger:
                self.logger.info(f"No constraints to drop on {full_table}")
            return []
        
        # Log what we're about to drop
        if self.logger:
            fk_count = len([c for c in constraints_to_drop if c.get("type") == "f"])
            pk_count = len([c for c in constraints_to_drop if c.get("type") == "p"])
            unique_count = len([c for c in constraints_to_drop if c.get("type") == "u"])
            parts = []
            if fk_count:
                parts.append(f"{fk_count} FK")
            if pk_count:
                parts.append(f"{pk_count} PK")
            if unique_count:
                parts.append(f"{unique_count} UNIQUE")
            self.logger.info(f"⚠️  Dropping constraints on {full_table}: {', '.join(parts)}")
        
        drop_constraints(self.engine, schema, table, constraints_to_drop, self.logger)
        
        if self.logger:
            self.logger.info(f"✓ Dropped {len(constraints_to_drop)} constraints on {full_table}")
        
        return constraints_to_drop
    
    def _execute_recreate_constraints(self, constraints: list[dict[str, str]]) -> bool:
        """Recreate constraints from their definitions.
        
        Args:
            constraints: List of constraint dicts with name, type, definition
            
        Returns:
            True if all constraints recreated successfully, False otherwise
        """
        if not constraints:
            return True
        
        schema, table = self._get_target_schema_table()
        full_table = build_qualified_name(schema, table)
        
        if self.logger:
            self.logger.info(f"Recreating {len(constraints)} constraints on {full_table}...")
        
        try:
            recreate_constraints(self.engine, schema, table, constraints, not_valid=True, logger=self.logger)
            if self.logger:
                self.logger.info(f"✓ Recreated {len(constraints)} constraints on {full_table} (FK as NOT VALID)")
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ FAILED to recreate constraints on {full_table}: {e}")
                self.logger.error(f"⚠️  MANUAL ACTION REQUIRED: Constraints must be recreated manually!")
                self.logger.error(f"   Constraints to recreate: {[c['name'] for c in constraints]}")
            return False
    
    def _log_dropped_constraints_for_recovery(self, constraints: list[dict[str, str]]) -> None:
        """Log constraint definitions for manual recovery if needed."""
        if not constraints or not self.logger:
            return
        
        schema, table = self._get_target_schema_table()
        full_table = build_qualified_name(schema, table)
        
        self.logger.error(f"⚠️  RECOVERY INFO for {full_table}:")
        for c in constraints:
            self.logger.error(f"   ALTER TABLE {full_table} ADD CONSTRAINT {c['name']} {c['definition']};")
    
    def _validate_batch_config(self) -> None:
        """Validate that source has required batch configuration."""
        if not isinstance(self.source, TemplateSource) or not self.source.config:
            raise ValueError("Source has no config - cannot auto-derive batch parameters")
        
        if not self.source.config.batch or not self.source.config.batch.batch_date_column:
            raise ValueError("config.batch with batch_date_column required for batch loading")
    
    def _extract_batch_params(self) -> tuple[str, str, bool, bool, bool, bool, bool]:
        """Extract batch parameters from config.
        
        Returns:
            Tuple of (source_table, date_column, truncate_flag, drop_indexes_flag, 
                      drop_fk_flag, drop_pk_flag, drop_unique_flag)
        """
        cfg = self.source.config
        assert cfg is not None, "Config should not be None after validation"
        batch_config = cfg.batch
        assert batch_config is not None, "Batch config should not be None after validation"
        
        return (
            cfg.source_table,
            quote_identifier(batch_config.batch_date_column),
            batch_config.truncate_before_load,
            batch_config.drop_indexes,
            batch_config.drop_fk,
            batch_config.drop_pk,
            batch_config.drop_unique
        )
    
    def _log_batch_behavior(
        self, 
        truncate: bool, 
        drop_indexes: bool, 
        drop_fk: bool = False,
        drop_pk: bool = False,
        drop_unique: bool = False,
        disable_wal: bool = False, 
        disable_vacuum: bool = False
    ) -> None:
        """Log batch execution behavior."""
        if self.logger:
            opts = []
            if truncate:
                opts.append("truncate")
            if drop_indexes:
                opts.append("drop_indexes")
            if drop_fk:
                opts.append("drop_FK")
            if drop_pk:
                opts.append("drop_PK")
            if drop_unique:
                opts.append("drop_UNIQUE")
            if disable_wal:
                opts.append("WAL_OFF")
            if disable_vacuum:
                opts.append("vacuum_off")

            self.logger.info(
                f"🔍 Batch config: {', '.join(opts) if opts else 'default'}, fail_fast={self.fail_fast}"
            )
    
    def _log_strategy_info(self, source_table: str, date_column: str) -> None:
        """Log strategy and source info (matching BatchLoadPipeline logs)."""
        if self.logger:
            self.logger.info(f"Using strategy: {self.strategy}")
            self.logger.info(f"Source: {source_table}, Date column: {date_column}")
    
    def _prepare_table(
        self, 
        truncate: bool, 
        drop_indexes: bool,
        drop_fk: bool = False,
        drop_pk: bool = False,
        drop_unique: bool = False
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        """Execute pre-load table preparation.
        
        Returns:
            Tuple of (dropped_indexes, dropped_constraints)
        """
        dropped_indexes: list[dict[str, str]] = []
        dropped_constraints: list[dict[str, str]] = []
        
        # Drop constraints FIRST (before indexes, as some indexes back constraints)
        if drop_fk or drop_pk or drop_unique:
            dropped_constraints = self._execute_drop_constraints(
                drop_fk=drop_fk, 
                drop_pk=drop_pk, 
                drop_unique=drop_unique
            )
        
        if truncate:
            self._execute_truncate()
        
        if drop_indexes:
            dropped_indexes = self._execute_drop_indexes()
        
        return dropped_indexes, dropped_constraints
    
    def _generate_batch_periods(self, source_table: str, date_column: str) -> list[tuple[str, str]]:
        """Generate batch periods from source table.
        
        Returns:
            List of (start_date, end_date) tuples
        """
        return self.strategy.generate_batches(self.engine, source_table, date_column)
    
    def _check_empty_batches(self, batch_periods: list[tuple[str, str]]) -> dict[str, str] | None:
        """Check if batches are empty and return skip result if so.
        
        Returns:
            Skip result dict if empty, None otherwise
        """
        if not batch_periods:
            if self.logger:
                self.logger.info("No data to process")
            return {"status": "skipped", "message": "No data to process"}
        return None
    
    def _log_batch_start(self, batch_periods: list[tuple[str, str]]) -> None:
        """Log batch execution start info."""
        if self.logger:
            self.logger.info(
                f"Starting {len(batch_periods)} batches: "
                f"{_format_batch_range(batch_periods)}"
            )
    
    def _collect_batch_results(
        self,
        batch_periods: list[tuple[str, str]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Execute batches and collect results.
        
        Returns:
            Tuple of (successful_batches, failed_batches)
        """
        successful: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        
        for event in self._execute_batches(batch_periods):
            if event["status"] == "success":
                if self.logger:
                    self.logger.info(_format_batch_success(event))
                successful.append(event)
            else:
                if self.logger:
                    self.logger.error(_format_batch_error(event))
                failed.append(event)
        
        return successful, failed
    
    def _log_completion(self, successful: list[dict[str, Any]], failed: list[dict[str, Any]]) -> None:
        """Log batch completion summary."""
        if self.logger:
            total = len(successful) + len(failed)
            self.logger.info(f"Completed: {len(successful)}/{total} batches successful")
    
    def _validate_results(self, result: BatchResult) -> None:
        """Validate results and raise error if needed."""
        if result["failed"] > 0 and not self.allow_partial_success:
            raise RuntimeError(
                f"{result['failed']} batch(es) failed out of {result['total']}"
            )
    
    def load(self) -> BatchResult | dict[str, Any]:
        """Execute batch loading with auto-configuration from source.config.batch.

        Auto-derives source_table and date_column from config.batch.
        Executes truncate and drop/recreate indexes based on config.batch flags.
        Optionally disables WAL and autovacuum during load (config.batch.disable_wal, disable_autovacuum).

        Returns:
            BatchResult with execution summary
        """
        self._validate_batch_config()

        source_table, date_column, truncate, drop_indexes_flag, drop_fk_flag, drop_pk_flag, drop_unique_flag = self._extract_batch_params()

        # Extract bulk optimization flags from config
        config = self.source.config if isinstance(self.source, TemplateSource) else None
        batch_config = config.batch if config and hasattr(config, 'batch') else None
        disable_wal = batch_config.disable_wal if batch_config else False
        disable_vacuum = batch_config.disable_autovacuum if batch_config else False

        self._log_batch_behavior(truncate, drop_indexes_flag, drop_fk_flag, drop_pk_flag, drop_unique_flag, disable_wal, disable_vacuum)
        self._log_strategy_info(source_table, date_column)

        schema, table = self._get_target_schema_table()

        # Track dropped items for recovery
        dropped_indexes: list[dict[str, str]] = []
        dropped_constraints: list[dict[str, str]] = []
        any_constraints_dropped = drop_fk_flag or drop_pk_flag or drop_unique_flag

        def _finalize_table() -> None:
            """Restore indexes and constraints - MUST run even on error."""
            
            # Recreate indexes first
            if drop_indexes_flag and dropped_indexes:
                try:
                    self._execute_recreate_indexes(dropped_indexes)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"❌ Failed to recreate indexes: {e}")
            
            # Then recreate constraints
            if any_constraints_dropped and dropped_constraints:
                success = self._execute_recreate_constraints(dropped_constraints)
                if not success:
                    # Log recovery info if constraints couldn't be restored
                    self._log_dropped_constraints_for_recovery(dropped_constraints)

        def _execute_load() -> BatchResult | dict[str, Any]:
            nonlocal dropped_indexes, dropped_constraints
            
            dropped_indexes, dropped_constraints = self._prepare_table(
                truncate, drop_indexes_flag, drop_fk_flag, drop_pk_flag, drop_unique_flag
            )

            try:
                batch_periods = self._generate_batch_periods(source_table, date_column)

                skip_result = self._check_empty_batches(batch_periods)
                if skip_result:
                    return skip_result

                self._log_batch_start(batch_periods)

                successful, failed = self._collect_batch_results(batch_periods)
                result = self._aggregate_results(successful, failed)

                self._log_completion(successful, failed)
                self._validate_results(result)

                return result
            except Exception as e:
                if self.logger:
                    self.logger.error(f"❌ Batch load failed: {e}")
                raise
            finally:
                _finalize_table()

        if disable_wal or disable_vacuum:
            with bulk_load_mode(self.engine, schema, table, disable_wal, disable_vacuum, self.logger):
                return _execute_load()
        else:
            return _execute_load()






