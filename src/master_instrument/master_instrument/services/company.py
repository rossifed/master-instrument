"""Company service - simple CRUD using ORM relations."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import Company, Entity


class CompanyService:
    """Service for Company operations using ORM model and relations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, company_id: int) -> Company | None:
        """Get a single company by ID with ORM relations."""
        return self.db.get(Company, company_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[Company]:
        """Get all companies with ORM relations."""
        stmt = select(Company).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def search_by_name(self, name_pattern: str, skip: int = 0, limit: int = 100) -> list[Company]:
        """Search companies by name pattern (ILIKE)."""
        stmt = (
            select(Company)
            .join(Company.entity)
            .where(Entity.name.ilike(f"%{name_pattern}%"))
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
