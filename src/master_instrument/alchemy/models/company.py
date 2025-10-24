from sqlalchemy import ForeignKey, String, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .base import Base

class Company(Base):
    __tablename__ = "company"
    __table_args__ = {"schema": "referential"}

    company_id: Mapped[int] = mapped_column(ForeignKey("referential.entity.entity_id"), primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    valid_from: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    valid_to: Mapped[str] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    entity: Mapped["Entity"] = relationship(back_populates="company")
