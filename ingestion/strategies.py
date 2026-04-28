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
    truncar_tabela,
)
from ingestion.metadata import buscar_metadados_tabela
from ingestion.watermark import get_watermark_composto, get_watermark_incremental


def executar_upsert(
    hana_engine,
    sql_conn: pyodbc.Connection,
    tabela: str,
    colunas: list[str],
    tipo: str,
    chave_primaria: list[str],
    coluna_watermark: str,
    coluna_watermark_ts: str | None = None,
    metadados: list[dict] | None = None,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    if metadados is None:
        metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    if coluna_watermark_ts:
        wm = get_watermark_composto(sql_conn, tabela, coluna_watermark, coluna_watermark_ts)
        if wm:
            wm_date, wm_ts = wm
            filtro = (
                f"{nome_hana(coluna_watermark)} > '{wm_date}' OR "
                f"({nome_hana(coluna_watermark)} = '{wm_date}' AND {nome_hana(coluna_watermark_ts)} > {wm_ts})"
            )
        else:
            filtro = None
    else:
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
    coluna_watermark_ts: str | None = None,
    idempotente: bool = False,
    metadados: list[dict] | None = None,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    if metadados is None:
        metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    if coluna_watermark_ts:
        wm = get_watermark_composto(sql_conn, tabela, coluna_watermark, coluna_watermark_ts)
        if wm:
            wm_date, wm_ts = wm
            filtro = (
                f"{nome_hana(coluna_watermark)} > '{wm_date}' OR "
                f"({nome_hana(coluna_watermark)} = '{wm_date}' AND {nome_hana(coluna_watermark_ts)} > {wm_ts})"
            )
        else:
            filtro = None
    else:
        watermark = get_watermark_incremental(sql_conn, tabela, coluna_watermark_local)
        if idempotente and watermark:
            cursor = sql_conn.cursor()
            cursor.execute(
                f"DELETE FROM {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} "
                f"WHERE {nome_sqlserver(coluna_watermark)} = ?",
                watermark,
            )
            sql_conn.commit()
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
    metadados: list[dict] | None = None,
) -> tuple[int, float]:
    inicio = time.perf_counter()
    if metadados is None:
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
    coluna_watermark_cabecalho_ts: str | None = None,
    watermark_cabecalho: tuple | None = None,
    metadados: list[dict] | None = None,
) -> tuple[int, float]:
    """Incremental para tabelas de linha sem UpdateDate próprio.

    Usa subquery no HANA para filtrar por DocEntry — um único round-trip.
    Delete no SQL Server por DocEntry apenas (não por chave composta),
    reduzindo N deletes individuais para um IN batch por chunk.
    Cada DocEntry é deletado apenas uma vez por run para evitar apagar
    linhas já inseridas caso o documento apareça em mais de um chunk.

    watermark_cabecalho: watermark pré-capturado antes da run — evita que o
    cabeçalho já atualizado nessa run avance o watermark antes das linhas rodarem.
    """
    inicio = time.perf_counter()
    if metadados is None:
        metadados = buscar_metadados_tabela(hana_engine, tabela, colunas, tipo)
    if not metadados:
        raise ValueError(f"Sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

    if coluna_watermark_cabecalho_ts:
        wm = watermark_cabecalho if watermark_cabecalho is not None else get_watermark_composto(sql_conn, tabela_cabecalho, coluna_watermark_cabecalho, coluna_watermark_cabecalho_ts)
        if wm:
            wm_date, wm_ts = wm
            filtro_cab = (
                f"{nome_hana(coluna_watermark_cabecalho)} > '{wm_date}' OR "
                f"({nome_hana(coluna_watermark_cabecalho)} = '{wm_date}' AND {nome_hana(coluna_watermark_cabecalho_ts)} > {wm_ts})"
            )
            filtro = (
                f"{nome_hana('DocEntry')} IN ("
                f"SELECT {nome_hana('DocEntry')} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela_cabecalho)} "
                f"WHERE {filtro_cab}"
                f")"
            )
        else:
            filtro = None
    else:
        wm_date = watermark_cabecalho[0] if watermark_cabecalho is not None else get_watermark_incremental(sql_conn, tabela_cabecalho, coluna_watermark_cabecalho)
        if wm_date:
            filtro = (
                f"{nome_hana('DocEntry')} IN ("
                f"SELECT {nome_hana('DocEntry')} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela_cabecalho)} "
                f"WHERE {nome_hana(coluna_watermark_cabecalho)} >= '{wm_date}'"
                f")"
            )
        else:
            filtro = None

    sql_select = montar_select_hana(tabela, metadados, filtro)
    sql_insert = montar_insert_sqlserver(tabela, metadados)

    idx_docentry = next(i for i, m in enumerate(metadados) if m["COLUMN_NAME"] == "DocEntry")
    doc_entries_deletados: set = set()

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
            novos = [(row[idx_docentry],) for row in lote if row[idx_docentry] not in doc_entries_deletados]
            if novos:
                deletar_por_chave(sql_conn, tabela, ["DocEntry"], novos)
                doc_entries_deletados.update(v[0] for v in novos)
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
    metadados: list[dict] | None = None,
) -> tuple[int, float]:
    """Snapshot diário: acumula o estado completo do dia, sem apagar histórico.

    Se já rodou hoje, apaga o snapshot do dia antes de reinserir (idempotente).
    Usa _ingestao_em (DEFAULT GETDATE()) como marcador temporal — sem coluna extra.
    """
    inicio = time.perf_counter()
    if metadados is None:
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
