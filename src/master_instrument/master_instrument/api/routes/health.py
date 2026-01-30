"""Health check endpoints for monitoring/K8s."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from master_instrument.db.session import get_db

router = APIRouter()


@router.get("")
def health():
    """Basic health check - always returns OK if the app is running."""
    return {"status": "ok"}


@router.get("/ready")
def ready(db: Session = Depends(get_db)):
    """Readiness check - verifies database connectivity."""
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ready", "database": "connected"}
    except Exception as e:
        return {"status": "not_ready", "database": str(e)}
