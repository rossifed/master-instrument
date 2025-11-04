from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class Country(Base):
    __tablename__ = "country"
    __table_args__ = (UniqueConstraint("code"),
        {"schema": "ref_data"})

    country_id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
