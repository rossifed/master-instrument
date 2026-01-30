"""StdFinancialItem service - financial item reference data."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import StdFinancialItem


class StdFinancialItemService:
    """Service for StdFinancialItem operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, item_id: int) -> StdFinancialItem | None:
        """Get a financial item by ID."""
        return self.db.get(StdFinancialItem, item_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[StdFinancialItem]:
        """Get all financial items."""
        stmt = select(StdFinancialItem).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_name(self, name: str) -> list[StdFinancialItem]:
        """Search financial items by name (partial match)."""
        stmt = select(StdFinancialItem).where(StdFinancialItem.name.ilike(f"%{name}%"))
        return list(self.db.scalars(stmt).all())

    def get_by_statement_type(
        self, statement_type_id: int, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialItem]:
        """Get financial items by statement type."""
        stmt = (
            select(StdFinancialItem)
            .where(StdFinancialItem.statement_type_id == statement_type_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
