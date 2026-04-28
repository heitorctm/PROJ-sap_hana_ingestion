from datetime import date

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
    valor = get_max_watermark(sql_conn, tabela, coluna)
    if valor is None:
        return None
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError:
        return None


def get_watermark_composto(
    sql_conn: pyodbc.Connection,
    tabela: str,
    coluna_date: str,
    coluna_ts: str,
) -> tuple[date, int] | None:
    """Lê o watermark combinando UpdateDate (DATE) + UpdateTS (HHMMSS como inteiro).

    Retorna (data, ts) do registro mais recente, permitindo precisão de segundo
    no filtro incremental — evita re-ingestão de registros já carregados no mesmo dia.
    """
    cursor = sql_conn.cursor()
    cursor.execute(
        f"SELECT TOP 1 {nome_sqlserver(coluna_date)}, {nome_sqlserver(coluna_ts)} "
        f"FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} "
        f"WHERE {nome_sqlserver(coluna_date)} IS NOT NULL "
        f"ORDER BY {nome_sqlserver(coluna_date)} DESC, {nome_sqlserver(coluna_ts)} DESC"
    )
    row = cursor.fetchone()
    if row is None or row[0] is None:
        return None
    wm_date = date.fromisoformat(str(row[0])[:10])
    wm_ts = int(row[1]) if row[1] is not None else 0
    return wm_date, wm_ts
