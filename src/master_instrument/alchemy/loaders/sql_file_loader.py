from sqlalchemy.engine import Engine
from pathlib import Path
from sqlalchemy import text

def load_from_sql_file(engine: Engine, sql_file: str):
    sql_path = Path(__file__).parent.parent / "sql" / sql_file
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        conn.execute(text(sql))

def Load_insrument_id_mapping(engine: Engine) -> None:
    load_from_sql_file(engine, "insert_instrument_id_mapping.sql")

def Load_company_id_mapping(engine: Engine) -> None:
    load_from_sql_file(engine, "load_companies.sql")