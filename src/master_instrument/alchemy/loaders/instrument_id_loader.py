from sqlalchemy import MetaData, Table, select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from master_instrument.alchemy.models.instrument_mapping import InstrumentMapping  # ton modèle ORM cible
from pathlib import Path
from sqlalchemy import text

def load_from_sql(engine: Engine, sql_file: str):
    sql_path = Path(__file__).parent.parent / "sql" / sql_file
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql))


def load_from_orm(engine: Engine):
    metadata = MetaData()

    # Réflexion de la table staging (pas de modèle ORM nécessaire)
    staging_table = Table(
        "stg_qa_instrument_id",
        metadata,
        autoload_with=engine,
        schema="staging"
    )

    # Construction de la requête INSERT ... SELECT ... ON CONFLICT DO NOTHING
    stmt = pg_insert(InstrumentMapping).from_select(
        ["source", "external_instrument_id", "valid_from"],
        select(
            staging_table.c.source,
            staging_table.c.external_instrument_id,
            func.now()
        ).distinct()
    ).on_conflict_do_nothing(
        index_elements=["source", "external_instrument_id"]
    )

    # Exécution dans une transaction
    with engine.begin() as conn:
        conn.execute(stmt)
