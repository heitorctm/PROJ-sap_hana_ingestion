from datetime import date, timedelta

import pyodbc

from ingestion.config import RAW_SCHEMA
from ingestion.loader import nome_sqlserver


def get_max_watermark(sql_conn: pyodbc.Connection, tabela: str, coluna: str):
    cursor = sql_conn.cursor()
    cursor.execute(
        f"SELECT MAX({nome_sqlserver(coluna)}) FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)}"
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return row[0]


def get_watermark_incremental(sql_conn: pyodbc.Connection, tabela: str, coluna: str) -> date | None:
    """Retorna MAX(coluna) - 1 dia para garantir reprocessamento do dia atual."""
    valor = get_max_watermark(sql_conn, tabela, coluna)
    if valor is None:
        return None
    if isinstance(valor, date):
        return valor - timedelta(days=1)
    try:
        return date.fromisoformat(str(valor)[:10]) - timedelta(days=1)
    except ValueError:
        return None
