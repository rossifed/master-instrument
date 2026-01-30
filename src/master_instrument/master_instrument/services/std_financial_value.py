"""StdFinancialValue service - standardized financial data access."""

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import StdFinancialValue


class StdFinancialValueService:
    """Service for StdFinancialValue operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_by_company_id(
        self, company_id: int, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialValue]:
        """Get all financial values for a company."""
        stmt = (
            select(StdFinancialValue)
            .where(StdFinancialValue.company_id == company_id)
            .order_by(StdFinancialValue.period_end_date.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_company_and_item(
        self, company_id: int, item_id: int, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialValue]:
        """Get financial values for a company and specific item."""
        stmt = (
            select(StdFinancialValue)
            .where(
                StdFinancialValue.company_id == company_id,
                StdFinancialValue.std_financial_item_id == item_id,
            )
            .order_by(StdFinancialValue.period_end_date.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_company_and_period_date(
        self, company_id: int, period_end_date: date, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialValue]:
        """Get all financial values for a company at a specific period end date."""
        stmt = (
            select(StdFinancialValue)
            .where(
                StdFinancialValue.company_id == company_id,
                StdFinancialValue.period_end_date == period_end_date,
            )
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_item_and_calendar_date(
        self, item_id: int, calendar_end_date: date, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialValue]:
        """Get financial values for an item at a specific calendar date (cross-company)."""
        stmt = (
            select(StdFinancialValue)
            .where(
                StdFinancialValue.std_financial_item_id == item_id,
                StdFinancialValue.calendar_end_date == calendar_end_date,
            )
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_statement_id(
        self, statement_id: int, skip: int = 0, limit: int = 100
    ) -> list[StdFinancialValue]:
        """Get all financial values for a statement."""
        stmt = (
            select(StdFinancialValue)
            .where(StdFinancialValue.std_financial_statement_id == statement_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_pk(self, statement_id: int, item_id: int) -> StdFinancialValue | None:
        """Get a specific financial value by composite PK."""
        return self.db.get(StdFinancialValue, (statement_id, item_id))

    def search(
        self,
        company_id: int | None = None,
        item_id: int | None = None,
        period_end_date: date | None = None,
        period_type_id: int | None = None,
        skip: int = 0,
        limit: int = 100,
    ) -> list[StdFinancialValue]:
        """Search financial values with flexible filters."""
        stmt = select(StdFinancialValue)

        if company_id is not None:
            stmt = stmt.where(StdFinancialValue.company_id == company_id)
        if item_id is not None:
            stmt = stmt.where(StdFinancialValue.std_financial_item_id == item_id)
        if period_end_date is not None:
            stmt = stmt.where(StdFinancialValue.period_end_date == period_end_date)
        if period_type_id is not None:
            stmt = stmt.where(StdFinancialValue.period_type_id == period_type_id)

        stmt = stmt.order_by(
            StdFinancialValue.company_id,
            StdFinancialValue.period_end_date.desc(),
        ).offset(skip).limit(limit)

        return list(self.db.scalars(stmt).all())
