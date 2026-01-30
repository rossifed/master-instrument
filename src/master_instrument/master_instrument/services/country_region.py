"""CountryRegion service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import CountryRegion


class CountryRegionService:
    """Service for CountryRegion operations."""

    def __init__(self, db: Session):
        self.db = db

    def get_all(self) -> list[CountryRegion]:
        """Get all country-region associations."""
        stmt = select(CountryRegion).order_by(CountryRegion.region_id, CountryRegion.country_id)
        return list(self.db.scalars(stmt).all())

    def get_by_region(self, region_id: int) -> list[CountryRegion]:
        """Get all countries in a region."""
        stmt = (
            select(CountryRegion)
            .where(CountryRegion.region_id == region_id)
            .order_by(CountryRegion.country_id)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_country(self, country_id: int) -> list[CountryRegion]:
        """Get all regions for a country."""
        stmt = (
            select(CountryRegion)
            .where(CountryRegion.country_id == country_id)
        )
        return list(self.db.scalars(stmt).all())
