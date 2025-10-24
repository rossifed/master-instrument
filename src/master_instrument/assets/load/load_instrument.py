from dagster import asset
from sqlalchemy.orm import Session
from sqlalchemy import insert, select
from master_instrument.alchemy.models import Instrument
from your_project.resources.db import get_sqlalchemy_engine  # ajuster selon ton projet

@asset
def load_instrument_from_dbt(context) -> None:
    engine = get_sqlalchemy_engine()  # construit à partir de REFERENTIAL_POSTGRES_CONN
    with engine.connect() as conn:
        # Lire la vue staging
        results = conn.execute(
            select(
                "entity_id",
                "instrument_type_id",
                "symbol",
                "name",
                "valid_from",
                "valid_to"
            ).select_from("stg_instrument")
        ).fetchall()

        context.log.info(f"Inserting {len(results)} rows into referential.instrument")

        with Session(engine) as session:
            for row in results:
                instrument = Instrument(**dict(row))
                session.add(instrument)
            session.commit()
