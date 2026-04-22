import time
from datetime import date, timedelta

import pyodbc
from dateutil.relativedelta import relativedelta
from sqlalchemy import text

from ingestion.config import CHUNK_SIZE, HANA_SCHEMA
from ingestion.loader import (
    deletar_por_chave,
    executar_carga,
    montar_insert_sqlserver,
    montar_select_hana,
    nome_hana,
    normalizar_valor,
    recriar_tabela_raw,
    truncar_tabela,
)
from ingestion.metadata import buscar_metadados_tabela
from ingestion.watermark import get_max_watermark


def executar_upsert(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
    chave_primaria: list[str],
    coluna_watermark: str,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    watermark = get_max_watermark(sql_conn, tabela, coluna_watermark)
    filtro = f"{nome_hana(coluna_watermark)} > '{watermark}'" if watermark else None
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

    return total_linhas, time.perf_counter() - inicio


def executar_append(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
    coluna_watermark: str,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    watermark = get_max_watermark(sql_conn, tabela, coluna_watermark)
    filtro = f"{nome_hana(coluna_watermark)} > '{watermark}'" if watermark else None
    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    total_linhas = executar_carga(hana_engine, sql_conn, sql_select, sql_insert)

    return total_linhas, time.perf_counter() - inicio


def executar_full_reload(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    truncar_tabela(sql_conn, tabela)
    sql_select = montar_select_hana(tabela, metadados)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    total_linhas = executar_carga(hana_engine, sql_conn, sql_select, sql_insert)

    return total_linhas, time.perf_counter() - inicio


def gerar_janelas(inicio: str, fim: str, janela_meses: int) -> list[tuple[date, date]]:
    d_inicio = date.fromisoformat(inicio)
    d_fim = date.fromisoformat(fim)
    janelas = []
    cursor = d_inicio
    while cursor < d_fim:
        proximo = cursor + relativedelta(months=janela_meses)
        janela_fim = min(proximo - timedelta(days=1), d_fim)
        janelas.append((cursor, janela_fim))
        cursor = proximo
    return janelas


def executar_janela(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    metadados: list[dict],
    coluna_watermark: str,
    janela_inicio: date,
    janela_fim: date,
) -> int:
    filtro = (
        f"{nome_hana(coluna_watermark)} >= '{janela_inicio}' "
        f"AND {nome_hana(coluna_watermark)} <= '{janela_fim}'"
    )
    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    return executar_carga(hana_engine, sql_conn, sql_select, sql_insert)
