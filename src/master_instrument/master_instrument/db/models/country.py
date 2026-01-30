from sqlalchemy import String, SmallInteger, UniqueConstraint, Index, CHAR
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base
from sqlalchemy.sql import text

class Country(Base):
    __tablename__ = "country"
    __table_args__ = (
        UniqueConstraint("code_alpha2", name="uq_country_code_alpha2"),
        UniqueConstraint("code_alpha3", name="uq_country_code_alpha3"),
        {"schema": "master"},
    )

    country_id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    code_alpha2: Mapped[str] = mapped_column(CHAR(2), nullable=False)
    code_alpha3: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)


    def __repr__(self):
        return f"<Country(id={self.country_id}, code_alpha2={self.code_alpha2}, code_alpha3={self.code_alpha3}, name={self.name})>"
