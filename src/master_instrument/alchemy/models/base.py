from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase, declared_attr

NAMING_CONVENTIONS = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s__%(column_0_name)s",
    "ck": "ck_%(table_name)s__%(constraint_name)s",
    "fk": "fk_%(table_name)s__%(column_0_name)s__%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=NAMING_CONVENTIONS)


class Base(DeclarativeBase):
    metadata = metadata


class PKMixin:
    @declared_attr.directive
    def id(cls):
        from sqlalchemy import BigInteger
        from sqlalchemy.orm import mapped_column

        return mapped_column(BigInteger, primary_key=True, autoincrement=True)
