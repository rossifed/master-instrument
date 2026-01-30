"""FastAPI application entry point for Master Instrument API."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from master_instrument.api.routes import health
from master_instrument.api.routes import instruments
from master_instrument.api.routes import venues
from master_instrument.api.routes import countries
from master_instrument.api.routes import currencies
from master_instrument.api.routes import entities
from master_instrument.api.routes import quotes
from master_instrument.api.routes import companies
from master_instrument.api.routes import equities
from master_instrument.api.routes import company_weblinks
from master_instrument.api.routes import country_regions
from master_instrument.api.routes import corpact_types
from master_instrument.api.routes import dividend_types
from master_instrument.api.routes import entity_types
from master_instrument.api.routes import equity_types
from master_instrument.api.routes import instrument_types
from master_instrument.api.routes import regions
from master_instrument.api.routes import venue_types
from master_instrument.api.routes import weblink_types
from master_instrument.api.routes import venue_mappings
from master_instrument.api.routes import quote_mappings
from master_instrument.api.routes import classification_schemes
from master_instrument.api.routes import classification_nodes
from master_instrument.api.routes import entity_classifications
from master_instrument.api.routes import market_data
from master_instrument.api.routes import adjusted_market_data
from master_instrument.api.routes import company_market_caps
from master_instrument.api.routes import financial_values
from master_instrument.api.routes import financial_items
from master_instrument.api.routes import financial_statements


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup/shutdown events."""
    # Startup
    logging.info("Master Instrument API starting up...")
    yield
    # Shutdown
    logging.info("Master Instrument API shutting down...")


def create_app() -> FastAPI:
    """Factory function to create the FastAPI application."""
    app = FastAPI(
        title="Master Instrument API",
        description="Golden source API for instruments, companies, and market data",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Global exception handler for database errors
    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
        logging.error(f"Database error: {exc}")
        return JSONResponse(
            status_code=503,
            content={"detail": "Database error. Please try again later."},
        )

    # Global exception handler for unexpected errors
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logging.error(f"Unexpected error: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error."},
        )

    # Health check (first for K8s probes)
    app.include_router(
        health.router, prefix="/health", tags=["health"]
    )

    # Include routers - add more as needed
    app.include_router(
        instruments.router, prefix="/instruments", tags=["instruments"]
    )
    app.include_router(
        venues.router, prefix="/venues", tags=["venues"]
    )
    app.include_router(
        countries.router, prefix="/countries", tags=["countries"]
    )
    app.include_router(
        currencies.router, prefix="/currencies", tags=["currencies"]
    )
    app.include_router(
        entities.router, prefix="/entities", tags=["entities"]
    )
    app.include_router(
        quotes.router, prefix="/quotes", tags=["quotes"]
    )
    app.include_router(
        companies.router, prefix="/companies", tags=["companies"]
    )
    app.include_router(
        equities.router, prefix="/equities", tags=["equities"]
    )
    app.include_router(
        company_weblinks.router, prefix="/company-weblinks", tags=["company-weblinks"]
    )
    app.include_router(
        country_regions.router, prefix="/country-regions", tags=["country-regions"]
    )
    # Type endpoints
    app.include_router(
        corpact_types.router, prefix="/corpact-types", tags=["corpact-types"]
    )
    app.include_router(
        dividend_types.router, prefix="/dividend-types", tags=["dividend-types"]
    )
    app.include_router(
        entity_types.router, prefix="/entity-types", tags=["entity-types"]
    )
    app.include_router(
        equity_types.router, prefix="/equity-types", tags=["equity-types"]
    )
    app.include_router(
        instrument_types.router, prefix="/instrument-types", tags=["instrument-types"]
    )
    app.include_router(
        regions.router, prefix="/regions", tags=["regions"]
    )
    app.include_router(
        venue_types.router, prefix="/venue-types", tags=["venue-types"]
    )
    app.include_router(
        weblink_types.router, prefix="/weblink-types", tags=["weblink-types"]
    )
    # Mapping endpoints
    app.include_router(
        venue_mappings.router, prefix="/venue-mappings", tags=["venue-mappings"]
    )
    app.include_router(
        quote_mappings.router, prefix="/quote-mappings", tags=["quote-mappings"]
    )
    # Classification endpoints
    app.include_router(
        classification_schemes.router, prefix="/classification-schemes", tags=["classification-schemes"]
    )
    app.include_router(
        classification_nodes.router, prefix="/classification-nodes", tags=["classification-nodes"]
    )
    app.include_router(
        entity_classifications.router, prefix="/entity-classifications", tags=["entity-classifications"]
    )
    # Timeseries/Data endpoints
    app.include_router(
        market_data.router, prefix="/market-data", tags=["market-data"]
    )
    app.include_router(
        adjusted_market_data.router, prefix="/adjusted-market-data", tags=["adjusted-market-data"]
    )
    app.include_router(
        company_market_caps.router, prefix="/company-market-caps", tags=["company-market-caps"]
    )
    app.include_router(
        financial_values.router, prefix="/financial-values", tags=["financial-values"]
    )
    app.include_router(
        financial_items.router, prefix="/financial-items", tags=["financial-items"]
    )
    app.include_router(
        financial_statements.router, prefix="/financial-statements", tags=["financial-statements"]
    )

    return app


app = create_app()
