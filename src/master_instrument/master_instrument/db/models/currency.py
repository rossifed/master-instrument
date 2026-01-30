from sqlalchemy import String, SmallInteger, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text

class Currency(Base):
    __tablename__ = "currency"
    __table_args__ = (
        UniqueConstraint("code", name="uq_currency_code"),
        {"schema": "master"},
    )

    currency_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(3), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


    def __repr__(self):
        return f"<Currency(id={self.currency_id}, code={self.code}, name={self.name})>"
