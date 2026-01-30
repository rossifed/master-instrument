"""Currency service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Currency


class CurrencyService:
    """Service for Currency operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, currency_id: int) -> Currency | None:
        """Get a single currency by ID."""
        return self.db.get(Currency, currency_id)

    def get_all(self) -> list[Currency]:
        """Get all currencies."""
        stmt = select(Currency)
        return list(self.db.scalars(stmt).all())

    def get_by_code(self, code: str) -> Currency | None:
        """Get currency by code (e.g. 'USD', 'EUR')."""
        stmt = select(Currency).where(Currency.code == code.upper())
        return self.db.scalar(stmt)
