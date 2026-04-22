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
    colunas_sql.append("    [_ingestao_em] DATETIME2 DEFAULT GETDATE()")
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


def truncar_tabela(sql_conn: pyodbc.Connection, tabela: str) -> None:
    cursor = sql_conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)}")
    sql_conn.commit()


def deletar_por_chave(
    sql_conn: pyodbc.Connection,
    tabela: str,
    chaves: list[str],
    valores: list[tuple],
) -> None:
    if not valores:
        return
    cursor = sql_conn.cursor()
    if len(chaves) == 1:
        col = nome_sqlserver(chaves[0])
        placeholders = ", ".join("?" for _ in valores)
        cursor.execute(
            f"DELETE FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} WHERE {col} IN ({placeholders})",
            [v[0] for v in valores],
        )
    else:
        for valor in valores:
            condicoes = " AND ".join(f"{nome_sqlserver(c)} = ?" for c in chaves)
            cursor.execute(
                f"DELETE FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} WHERE {condicoes}",
                list(valor),
            )
    sql_conn.commit()


def montar_select_hana(tabela: str, metadados: list[dict[str, Any]], filtro_watermark: str | None = None) -> str:
    colunas = ", ".join(nome_hana(col["COLUMN_NAME"]) for col in metadados)
    top = f"TOP {TOP_N} " if TOP_N else ""
    sql = f"SELECT {top}{colunas} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela)}"
    if filtro_watermark:
        sql += f" WHERE {filtro_watermark}"
    return sql


def montar_insert_sqlserver(tabela: str, metadados: list[dict[str, Any]]) -> str:
    colunas = ", ".join(nome_sqlserver(col["COLUMN_NAME"]) for col in metadados)
    placeholders = ", ".join("?" for _ in metadados)
    return f"INSERT INTO {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} ({colunas}) VALUES ({placeholders})"


def _executar_carga(hana_engine, sql_conn, sql_select, sql_insert) -> int:
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

    return total_linhas


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
    total_linhas = _executar_carga(hana_engine, sql_conn, sql_select, sql_insert)

    duracao = time.perf_counter() - inicio
    return total_linhas, duracao


def carregar_incremental_upsert(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
    chave_primaria: list[str],
    coluna_watermark: str,
    watermark_valor,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)

    if not metadados:
        raise ValueError(f"Tabela sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    filtro = f"{nome_hana(coluna_watermark)} > '{watermark_valor}'" if watermark_valor else None
    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)

    indices_chave = [
        next(i for i, m in enumerate(metadados) if m["COLUMN_NAME"] == c)
        for c in chave_primaria
    ]

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
            chaves_lote = [tuple(row[i] for i in indices_chave) for row in lote]
            deletar_por_chave(sql_conn, tabela, chave_primaria, chaves_lote)
            cursor_destino.executemany(sql_insert, lote)
            sql_conn.commit()
            total_linhas += len(lote)

    duracao = time.perf_counter() - inicio
    return total_linhas, duracao


def carregar_incremental_append(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
    coluna_watermark: str,
    watermark_valor,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)

    if not metadados:
        raise ValueError(f"Tabela sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    filtro = f"{nome_hana(coluna_watermark)} > '{watermark_valor}'" if watermark_valor else None
    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    total_linhas = _executar_carga(hana_engine, sql_conn, sql_select, sql_insert)

    duracao = time.perf_counter() - inicio
    return total_linhas, duracao


def carregar_full_reload(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)

    if not metadados:
        raise ValueError(f"Tabela sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    truncar_tabela(sql_conn, tabela)

    sql_select = montar_select_hana(tabela, metadados)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    total_linhas = _executar_carga(hana_engine, sql_conn, sql_select, sql_insert)

    duracao = time.perf_counter() - inicio
    return total_linhas, duracao
