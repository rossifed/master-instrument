from sqlalchemy import insert, select, text, literal_column, Select
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Type


def load_identifier_mapping_from_view(
    session: Session,
    mapping_model: Type,
    view_name: str,
    source_value: str,
    source_column_name: str = "source_identifier",
    conflict_columns: tuple[str, str] = ("source", "source_identifier"),
    schema: str = "staging",
):
    """
    Bulk-load a surrogate ID mapping table from a staging view.

    This function inserts distinct (source, source_identifier) values from a staging view
    into a referential mapping table (instrument, company, venue, etc.), using ON CONFLICT DO NOTHING.

    Args:
        session (Session): Active SQLAlchemy session.
        mapping_model (Type): SQLAlchemy model for the target mapping table.
        view_name (str): Name of the staging view (e.g. 'stg_instrument_source_id').
        source_value (str): Data source name (e.g. 'QA', 'Refinitiv').
        source_column_name (str): Column name in the view holding the source identifier.
        conflict_columns (tuple): Tuple of column names to detect conflicts (must match a unique constraint).
        schema (str): Schema of the staging view (default: 'staging').
    """
    fq_view = f"{schema}.{view_name}"

    select_stmt: Select = select(
        literal_column(f"'{source_value}'").label("source"),
        literal_column(source_column_name),
        literal_column("now() AT TIME ZONE 'UTC'").label("valid_from")
    ).select_from(text(fq_view))

    insert_stmt = (
        insert(mapping_model)
        .from_select(["source", "source_identifier", "valid_from"], select_stmt)
        .prefix_with('ON CONFLICT (source, source_identifier) DO NOTHING')
    )

    session.execute(insert_stmt)
    session.commit()
