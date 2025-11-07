from sqlalchemy import String, Integer, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class Currency(Base):
    __tablename__ = "currency"
    __table_args__ = (
        UniqueConstraint("code"),
        {"schema": "ref_data"},
    )

    currency_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(3), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    def __repr__(self) -> str:
        return f"<Currency(id={self.currency_id}, code={self.code}, name={self.name})>"
