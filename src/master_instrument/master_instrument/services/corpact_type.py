"""CorpactType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import CorpactType


class CorpactTypeService:
    """Service for CorpactType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, corpact_type_id: int) -> CorpactType | None:
        return self.db.get(CorpactType, corpact_type_id)

    def get_all(self) -> list[CorpactType]:
        stmt = select(CorpactType)
        return list(self.db.scalars(stmt).all())

    def get_by_code(self, code: str) -> CorpactType | None:
        stmt = select(CorpactType).where(CorpactType.code == code.upper())
        return self.db.scalar(stmt)
