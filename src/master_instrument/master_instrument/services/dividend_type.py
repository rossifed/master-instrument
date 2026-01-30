"""DividendType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import DividendType


class DividendTypeService:
    """Service for DividendType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, dividend_type_id: int) -> DividendType | None:
        return self.db.get(DividendType, dividend_type_id)

    def get_all(self) -> list[DividendType]:
        stmt = select(DividendType)
        return list(self.db.scalars(stmt).all())

    def get_by_code(self, code: str) -> DividendType | None:
        stmt = select(DividendType).where(DividendType.code == code.upper())
        return self.db.scalar(stmt)
