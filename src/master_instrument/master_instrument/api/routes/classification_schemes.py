"""ClassificationScheme API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from master_instrument.schemas.classification import ClassificationSchemeResponse
from master_instrument.services.classification import ClassificationSchemeService
from master_instrument.dependencies import get_classification_scheme_service

router = APIRouter()


@router.get("", response_model=list[ClassificationSchemeResponse])
def list_classification_schemes(
    service: Annotated[ClassificationSchemeService, Depends(get_classification_scheme_service)],
):
    """List all classification schemes (e.g. GICS, ICB)."""
    return service.get_all()


@router.get("/{scheme_id}", response_model=ClassificationSchemeResponse)
def get_classification_scheme(
    scheme_id: int,
    service: Annotated[ClassificationSchemeService, Depends(get_classification_scheme_service)],
):
    """Get a classification scheme by ID."""
    scheme = service.get(scheme_id)
    if not scheme:
        raise HTTPException(status_code=404, detail="Classification scheme not found")
    return scheme


@router.get("/mnemonic/{mnemonic}", response_model=ClassificationSchemeResponse)
def get_classification_scheme_by_mnemonic(
    mnemonic: str,
    service: Annotated[ClassificationSchemeService, Depends(get_classification_scheme_service)],
):
    """Get a classification scheme by mnemonic (e.g. 'GICS')."""
    scheme = service.get_by_mnemonic(mnemonic)
    if not scheme:
        raise HTTPException(status_code=404, detail="Classification scheme not found")
    return scheme
