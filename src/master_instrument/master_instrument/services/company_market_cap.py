"""CompanyMarketCap service - historical market cap timeseries."""

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import CompanyMarketCap


class CompanyMarketCapService:
    """Service for CompanyMarketCap operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_by_company_id(
        self, company_id: int, skip: int = 0, limit: int = 100
    ) -> list[CompanyMarketCap]:
        """Get market cap history for a company (most recent first)."""
        stmt = (
            select(CompanyMarketCap)
            .where(CompanyMarketCap.company_id == company_id)
            .order_by(CompanyMarketCap.valuation_date.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_company_and_date(
        self, company_id: int, valuation_date: date
    ) -> CompanyMarketCap | None:
        """Get market cap for a specific company and date (PK lookup)."""
        return self.db.get(CompanyMarketCap, (valuation_date, company_id))

    def get_by_company_date_range(
        self,
        company_id: int,
        start_date: date,
        end_date: date,
        skip: int = 0,
        limit: int = 1000,
    ) -> list[CompanyMarketCap]:
        """Get market cap for a company within a date range."""
        stmt = (
            select(CompanyMarketCap)
            .where(
                CompanyMarketCap.company_id == company_id,
                CompanyMarketCap.valuation_date >= start_date,
                CompanyMarketCap.valuation_date <= end_date,
            )
            .order_by(CompanyMarketCap.valuation_date.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_date(
        self, valuation_date: date, skip: int = 0, limit: int = 100
    ) -> list[CompanyMarketCap]:
        """Get all market caps for a specific date."""
        stmt = (
            select(CompanyMarketCap)
            .where(CompanyMarketCap.valuation_date == valuation_date)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_latest_by_company(self, company_id: int) -> CompanyMarketCap | None:
        """Get the most recent market cap for a company."""
        stmt = (
            select(CompanyMarketCap)
            .where(CompanyMarketCap.company_id == company_id)
            .order_by(CompanyMarketCap.valuation_date.desc())
            .limit(1)
        )
        return self.db.scalar(stmt)
