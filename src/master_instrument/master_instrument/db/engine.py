"""Database engine configuration."""

import os

from sqlalchemy import create_engine

# Use same connection string as ETL
DATABASE_URL = os.getenv(
    "REFERENTIAL_POSTGRES_CONN",
    "postgresql://postgres:postgres@localhost:5432/master_instrument"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)
