"""EquityType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import EquityType


class EquityTypeService:
    """Service for EquityType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, equity_type_id: int) -> EquityType | None:
        return self.db.get(EquityType, equity_type_id)

    def get_all(self) -> list[EquityType]:
        stmt = select(EquityType)
        return list(self.db.scalars(stmt).all())

    def get_by_mnemonic(self, mnemonic: str) -> EquityType | None:
        stmt = select(EquityType).where(EquityType.mnemonic == mnemonic.upper())
        return self.db.scalar(stmt)
