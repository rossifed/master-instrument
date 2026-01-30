"""CompanyWeblink service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import CompanyWeblink


class CompanyWeblinkService:
    """Service for CompanyWeblink operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, weblink_id: int) -> CompanyWeblink | None:
        """Get a company weblink by ID."""
        return self.db.get(CompanyWeblink, weblink_id)

    def get_all(self, skip: int = 0, limit: int = 100) -> list[CompanyWeblink]:
        """Get all company weblinks with pagination."""
        stmt = select(CompanyWeblink).offset(skip).limit(limit)
        return list(self.db.scalars(stmt).all())

    def get_by_company(self, company_id: int) -> list[CompanyWeblink]:
        """Get all weblinks for a company."""
        stmt = (
            select(CompanyWeblink)
            .where(CompanyWeblink.company_id == company_id)
            .order_by(CompanyWeblink.weblink_type_id)
        )
        return list(self.db.scalars(stmt).all())

    def get_by_type(self, weblink_type_id: int, skip: int = 0, limit: int = 100) -> list[CompanyWeblink]:
        """Get all weblinks of a specific type."""
        stmt = (
            select(CompanyWeblink)
            .where(CompanyWeblink.weblink_type_id == weblink_type_id)
            .offset(skip)
            .limit(limit)
        )
        return list(self.db.scalars(stmt).all())
