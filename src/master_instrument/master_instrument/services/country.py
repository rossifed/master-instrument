"""Country service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Country


class CountryService:
    """Service for Country operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, country_id: int) -> Country | None:
        """Get a single country by ID."""
        return self.db.get(Country, country_id)

    def get_all(self) -> list[Country]:
        """Get all countries."""
        stmt = select(Country)
        return list(self.db.scalars(stmt).all())

    def get_by_alpha2(self, code: str) -> Country | None:
        """Get country by alpha2 code (e.g. 'US', 'FR')."""
        stmt = select(Country).where(Country.code_alpha2 == code.upper())
        return self.db.scalar(stmt)

    def get_by_alpha3(self, code: str) -> Country | None:
        """Get country by alpha3 code (e.g. 'USA', 'FRA')."""
        stmt = select(Country).where(Country.code_alpha3 == code.upper())
        return self.db.scalar(stmt)
