import time
from datetime import date, timedelta

import pyodbc
from dateutil.relativedelta import relativedelta
from sqlalchemy import text

from ingestion.config import CHUNK_SIZE, HANA_SCHEMA, RAW_SCHEMA
from ingestion.loader import (
    deletar_por_chave,
    executar_carga,
    montar_insert_sqlserver,
    montar_select_hana,
    nome_hana,
    nome_sqlserver,
    normalizar_valor,
    recriar_tabela_raw,
    truncar_tabela,
)
from ingestion.metadata import buscar_metadados_tabela
from ingestion.watermark import get_max_watermark, get_watermark_incremental


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

    watermark = get_watermark_incremental(sql_conn, tabela, coluna_watermark)
    filtro = f"{nome_hana(coluna_watermark)} >= '{watermark}'" if watermark else None
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
    coluna_watermark_local: str,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    watermark = get_watermark_incremental(sql_conn, tabela, coluna_watermark_local)
    filtro = f"{nome_hana(coluna_watermark)} >= '{watermark}'" if watermark else None
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


def executar_via_cabecalho(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
    chave_primaria: list[str],
    tabela_cabecalho: str,
    coluna_watermark_cabecalho: str,
) -> tuple[int, float]:
    """Incremental para tabelas de linhas sem UpdateDate.

    Passo 1: busca DocEntries do cabeçalho no HANA (filtrado pelo watermark).
    Passo 2: busca as linhas no HANA usando a lista de DocEntries como filtro literal.
    Isso evita subquery no HANA e permite uso de índice em DocEntry.
    """
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    watermark = get_watermark_incremental(sql_conn, tabela_cabecalho, coluna_watermark_cabecalho)

    if watermark:
        sql_cabecalho = (
            f"SELECT DISTINCT {nome_hana('DocEntry')} "
            f"FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela_cabecalho)} "
            f"WHERE {nome_hana(coluna_watermark_cabecalho)} >= '{watermark}'"
        )
        with hana_engine.connect() as conn:
            rows = conn.execute(text(sql_cabecalho)).fetchall()
        doc_entries = [row[0] for row in rows]

        if not doc_entries:
            return 0, time.perf_counter() - inicio

        placeholders = ", ".join(str(d) for d in doc_entries)
        filtro = f"{nome_hana('DocEntry')} IN ({placeholders})"
    else:
        filtro = None

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


def executar_snapshot_diario(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
) -> tuple[int, float]:
    """Snapshot diário: acumula o estado completo do dia, sem apagar histórico.

    Se já rodou hoje, apaga o snapshot do dia antes de reinserir (idempotente).
    Usa _ingestao_em (DEFAULT GETDATE()) como marcador temporal — sem coluna extra.
    """
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    cursor = sql_conn.cursor()
    cursor.execute(
        f"DELETE FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} "
        f"WHERE CAST([_ingestao_em] AS DATE) = CAST(GETDATE() AS DATE)",
    )
    sql_conn.commit()

    sql_select = montar_select_hana(tabela, metadados)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    total_linhas = executar_carga(hana_engine, sql_conn, sql_select, sql_insert)

    return total_linhas, time.perf_counter() - inicio


def executar_janela_via_cabecalho(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    metadados: list[dict],
    chave_primaria: list[str],
    tabela_cabecalho: str,
    coluna_watermark_cabecalho: str,
    janela_inicio: date,
    janela_fim: date,
) -> int:
    """Carga inicial por janela para tabelas incremental_via_cabecalho."""
    filtro = (
        f"{nome_hana('DocEntry')} IN ("
        f"SELECT {nome_hana('DocEntry')} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela_cabecalho)} "
        f"WHERE {nome_hana(coluna_watermark_cabecalho)} >= '{janela_inicio}' "
        f"AND {nome_hana(coluna_watermark_cabecalho)} <= '{janela_fim}'"
        f")"
    )
    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    return executar_carga(hana_engine, sql_conn, sql_select, sql_insert)


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
    cursor = sql_conn.cursor()
    cursor.execute(
        f"DELETE FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} "
        f"WHERE {nome_sqlserver(coluna_watermark)} >= ? AND {nome_sqlserver(coluna_watermark)} <= ?",
        janela_inicio, janela_fim,
    )
    sql_conn.commit()

    filtro = (
        f"{nome_hana(coluna_watermark)} >= '{janela_inicio}' "
        f"AND {nome_hana(coluna_watermark)} <= '{janela_fim}'"
    )
    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)
    return executar_carga(hana_engine, sql_conn, sql_select, sql_insert)
