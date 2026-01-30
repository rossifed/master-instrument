"""WeblinkType service."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from master_instrument.db.models import WeblinkType


class WeblinkTypeService:
    """Service for WeblinkType operations."""

    def __init__(self, db: Session):
        self.db = db

    def get(self, weblink_type_id: int) -> WeblinkType | None:
        return self.db.get(WeblinkType, weblink_type_id)

    def get_all(self) -> list[WeblinkType]:
        stmt = select(WeblinkType)
        return list(self.db.scalars(stmt).all())
