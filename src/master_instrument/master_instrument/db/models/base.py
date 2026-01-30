from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Integer, MetaData
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column
from sqlalchemy.sql import func

NAMING_CONVENTIONS = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s__%(column_0_name)s",
    "ck": "ck_%(table_name)s__%(constraint_name)s",
    "fk": "fk_%(table_name)s__%(column_0_name)s__%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=NAMING_CONVENTIONS)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models (tables managed by Alembic)."""
    metadata = metadata


# Separate metadata for views - NOT managed by Alembic
view_metadata = MetaData()


class ViewBase(DeclarativeBase):
    """Base class for read-only view models.
    
    Views inherit from this class instead of Base to avoid Alembic
    trying to create/manage them as tables. Views are created by dbt.
    """
    metadata = view_metadata


class PKMixin:
    @declared_attr.directive
    def id(cls):
        from sqlalchemy import BigInteger
        from sqlalchemy.orm import mapped_column

        return mapped_column(BigInteger, primary_key=True, autoincrement=True)


class CDCLoadMixin:
    """
    Mixin for CDC (Change Data Capture) load tracking tables.
    
    Provides common columns for tracking:
    - last_source_version: The source version/LSN that was processed
    - loaded_at: Timestamp when the load occurred
    - rows_inserted/updated/deleted: Row counts for the load operation
    """

    @declared_attr
    def last_source_version(cls) -> Mapped[int]:
        return mapped_column(BigInteger, nullable=False)

    @declared_attr
    def loaded_at(cls) -> Mapped[datetime]:
        return mapped_column(
            TIMESTAMP(timezone=True),
            nullable=False,
            server_default=func.current_timestamp()
        )

    @declared_attr
    def rows_inserted(cls) -> Mapped[Optional[int]]:
        return mapped_column(Integer, nullable=True)

    @declared_attr
    def rows_updated(cls) -> Mapped[Optional[int]]:
        return mapped_column(Integer, nullable=True)

    @declared_attr
    def rows_deleted(cls) -> Mapped[Optional[int]]:
        return mapped_column(Integer, nullable=True)
