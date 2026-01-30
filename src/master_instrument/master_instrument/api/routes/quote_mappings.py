"""QuoteMapping API routes."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from master_instrument.schemas.mapping import QuoteMappingResponse
from master_instrument.services.mapping import QuoteMappingService
from master_instrument.dependencies import get_quote_mapping_service

router = APIRouter()


@router.get("", response_model=list[QuoteMappingResponse])
def list_quote_mappings(
    service: Annotated[QuoteMappingService, Depends(get_quote_mapping_service)],
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """List all quote mappings."""
    return service.get_all(skip=skip, limit=limit)


@router.get("/{internal_quote_id}", response_model=QuoteMappingResponse)
def get_quote_mapping(
    internal_quote_id: int,
    service: Annotated[QuoteMappingService, Depends(get_quote_mapping_service)],
):
    """Get a quote mapping by internal ID."""
    mapping = service.get(internal_quote_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="QuoteMapping not found")
    return mapping


@router.get("/external/{external_id}", response_model=list[QuoteMappingResponse])
def get_quote_mapping_by_external_id(
    external_id: str,
    service: Annotated[QuoteMappingService, Depends(get_quote_mapping_service)],
    data_source_id: int | None = Query(None),
):
    """Get quote mappings by external ID."""
    return service.get_by_external_id(external_id, data_source_id)
