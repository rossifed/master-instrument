"""Equity service - simple CRUD using ORM relations."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Equity, Instrument, Entity


class EquityService:
    """Service for Equity operations using ORM model and relations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, equity_id: int) -> Equity | None:
        """Get a single equity by ID with ORM relations."""
        return self.db.get(Equity, equity_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[Equity]:
        """Get all equities with ORM relations."""
        stmt = select(Equity).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def search(self, name: str, skip: int = 0, limit: int = 100) -> list[Equity]:
        """Search equities by company name (via instrument -> entity)."""
        stmt = (
            select(Equity)
            .join(Equity.instrument)
            .join(Instrument.entity)
            .where(Entity.name.ilike(f"%{name}%"))
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_isin(self, isin: str) -> Equity | None:
        """Get equity by ISIN."""
        stmt = select(Equity).where(Equity.isin == isin)
        return self.db.scalar(stmt)

    def get_by_ticker(self, ticker: str) -> list[Equity]:
        """Get equities by ticker (can have multiple)."""
        stmt = select(Equity).where(Equity.ticker == ticker)
        return list(self.db.scalars(stmt).all())

    def get_by_ric(self, ric: str) -> Equity | None:
        """Get equity by RIC."""
        stmt = select(Equity).where(Equity.ric == ric)
        return self.db.scalar(stmt)

    def get_by_sedol(self, sedol: str) -> Equity | None:
        """Get equity by SEDOL."""
        stmt = select(Equity).where(Equity.sedol == sedol)
        return self.db.scalar(stmt)

    def get_major_securities(self, skip: int = 0, limit: int = 100) -> list[Equity]:
        """Get only major securities."""
        stmt = (
            select(Equity)
            .where(Equity.is_major_security.is_(True))
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
