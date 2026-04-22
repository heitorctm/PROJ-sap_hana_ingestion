import time
from typing import Any

import pyodbc
from sqlalchemy import text

from ingestion.config import CHUNK_SIZE, HANA_SCHEMA, RAW_SCHEMA, TOP_N
from ingestion.metadata import buscar_metadados_tabela, mapear_tipo_hana_para_sqlserver


def nome_sqlserver(nome: str) -> str:
    return "[" + nome.replace("]", "]]") + "]"


def nome_hana(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def normalizar_valor(valor: Any) -> Any:
    if isinstance(valor, memoryview):
        return bytes(valor)
    return valor


def garantir_schema_raw(sql_conn: pyodbc.Connection) -> None:
    cursor = sql_conn.cursor()
    cursor.execute(
        f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{RAW_SCHEMA}')
        BEGIN
            EXEC('CREATE SCHEMA {RAW_SCHEMA}')
        END
        """
    )
    sql_conn.commit()


def recriar_tabela_raw(sql_conn: pyodbc.Connection, tabela: str, metadados: list[dict[str, Any]]) -> None:
    colunas_sql = [
        f"    {nome_sqlserver(col['COLUMN_NAME'])} {mapear_tipo_hana_para_sqlserver(col)} NULL"
        for col in metadados
    ]
    definicao_colunas = ",\n".join(colunas_sql)

    ddl = f"""
    IF OBJECT_ID('{RAW_SCHEMA}.{tabela}', 'U') IS NOT NULL
        DROP TABLE {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)};

    CREATE TABLE {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} (
{definicao_colunas}
    );
    """

    cursor = sql_conn.cursor()
    cursor.execute(ddl)
    sql_conn.commit()


def montar_select_hana(tabela: str, metadados: list[dict[str, Any]]) -> str:
    colunas = ", ".join(nome_hana(col["COLUMN_NAME"]) for col in metadados)
    top = f"TOP {TOP_N} " if TOP_N else ""
    return f"SELECT {top}{colunas} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela)}"


def montar_insert_sqlserver(tabela: str, metadados: list[dict[str, Any]]) -> str:
    colunas = ", ".join(nome_sqlserver(col["COLUMN_NAME"]) for col in metadados)
    placeholders = ", ".join("?" for _ in metadados)
    return f"INSERT INTO {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} ({colunas}) VALUES ({placeholders})"


def carregar_tabela(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str = "tabela",
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)

    if not metadados:
        raise ValueError(f"Tabela sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    recriar_tabela_raw(sql_conn, tabela, metadados)

    sql_select = montar_select_hana(tabela, metadados)
    sql_insert = montar_insert_sqlserver(tabela, metadados)

    total_linhas = 0
    cursor_destino = sql_conn.cursor()
    cursor_destino.fast_executemany = True

    with hana_engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(text(sql_select))

        while True:
            rows = result.fetchmany(CHUNK_SIZE)
            if not rows:
                break

            lote = [tuple(normalizar_valor(v) for v in row) for row in rows]
            cursor_destino.executemany(sql_insert, lote)
            sql_conn.commit()
            total_linhas += len(lote)

    duracao = time.perf_counter() - inicio
    return total_linhas, duracao
