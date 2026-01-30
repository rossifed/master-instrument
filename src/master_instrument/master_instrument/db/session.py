"""Database session management."""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy.orm import Session, sessionmaker

from master_instrument.db.engine import engine

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


@contextmanager
def session_scope():
    """Context manager for ETL/scripts - auto commit/rollback."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """Generator for API - no auto commit."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
