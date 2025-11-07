from typing import List, TYPE_CHECKING
from sqlalchemy import String, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

if TYPE_CHECKING:
    from .company_weblink import CompanyWeblink

class WeblinkType(Base):
    __tablename__ = "weblink_type"
    __table_args__ = {"schema": "ref_data"}

    weblink_type_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=False)
    description: Mapped[str] = mapped_column(String(50), nullable=False)

    weblinks: Mapped[List["CompanyWeblink"]] = relationship(
        back_populates="weblink_type",
        cascade="all, delete-orphan",
        lazy="selectin"
    )

    def __repr__(self) -> str:
        return f"<WeblinkType(id={self.weblink_type_id}, description={self.description})>"
