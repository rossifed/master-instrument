"""StdFinancialStatement service - financial statement reference data."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import StdFinancialStatement


class StdFinancialStatementService:
    """Service for StdFinancialStatement operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, statement_id: int) -> StdFinancialStatement | None:
        """Get a financial statement by ID."""
        return self.db.get(StdFinancialStatement, statement_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[StdFinancialStatement]:
        """Get all financial statements."""
        stmt = select(StdFinancialStatement).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_filing_id(
        self, filing_id: int, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialStatement]:
        """Get statements for a filing."""
        stmt = (
            select(StdFinancialStatement)
            .where(StdFinancialStatement.std_financial_filing_id == filing_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_statement_type(
        self, statement_type_id: int, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialStatement]:
        """Get statements by type."""
        stmt = (
            select(StdFinancialStatement)
            .where(StdFinancialStatement.statement_type_id == statement_type_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
