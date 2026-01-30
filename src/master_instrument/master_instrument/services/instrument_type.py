"""InstrumentType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import InstrumentType


class InstrumentTypeService:
    """Service for InstrumentType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, instrument_type_id: int) -> InstrumentType | None:
        return self.db.get(InstrumentType, instrument_type_id)

    def get_all(self) -> list[InstrumentType]:
        stmt = select(InstrumentType)
        return list(self.db.scalars(stmt).all())

    def get_by_mnemonic(self, mnemonic: str) -> InstrumentType | None:
        stmt = select(InstrumentType).where(InstrumentType.mnemonic == mnemonic.upper())
        return self.db.scalar(stmt)
