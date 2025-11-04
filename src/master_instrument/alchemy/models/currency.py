from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class Currency(Base):
    __tablename__ = "currency"
    __table_args__ = (UniqueConstraint("code"),
        {"schema": "ref_data"})
 
    currency_id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
