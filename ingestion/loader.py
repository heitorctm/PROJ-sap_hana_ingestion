from typing import Any

import pyodbc
from sqlalchemy import text

from ingestion.config import CHUNK_SIZE, HANA_SCHEMA, RAW_SCHEMA
from ingestion.metadata import mapear_tipo_hana_para_sqlserver


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


def _ddl_colunas(metadados: list[dict[str, Any]]) -> str:
    colunas_sql = [
        f"    {nome_sqlserver(col['COLUMN_NAME'])} {mapear_tipo_hana_para_sqlserver(col)} NULL"
        for col in metadados
    ]
    colunas_sql.append("    [_ingestao_em] DATETIME2 DEFAULT GETDATE()")
    return ",\n".join(colunas_sql)


def criar_tabela_se_nao_existir(sql_conn: pyodbc.Connection, tabela: str, metadados: list[dict[str, Any]]) -> None:
    colunas_ddl = _ddl_colunas(metadados)
    ddl = f"""
    IF OBJECT_ID('{RAW_SCHEMA}.{tabela}', 'U') IS NULL
        CREATE TABLE {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} (
{colunas_ddl}
        );
    """
    cursor = sql_conn.cursor()
    cursor.execute(ddl)
    sql_conn.commit()


def adicionar_colunas_faltantes(sql_conn: pyodbc.Connection, tabela: str, metadados: list[dict[str, Any]]) -> None:
    cursor = sql_conn.cursor()
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        RAW_SCHEMA, tabela,
    )
    existentes = {row[0].upper() for row in cursor.fetchall()}
    for col in metadados:
        nome = col["COLUMN_NAME"]
        if nome.upper() not in existentes:
            tipo_sql = mapear_tipo_hana_para_sqlserver(col)
            cursor.execute(
                f"ALTER TABLE {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} "
                f"ADD {nome_sqlserver(nome)} {tipo_sql} NULL"
            )
            sql_conn.commit()
            print(f"[schema] coluna adicionada: {tabela}.{nome} ({tipo_sql})")


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


def montar_select_hana(tabela: str, metadados: list[dict[str, Any]], filtro: str | None = None) -> str:
    colunas = ", ".join(nome_hana(col["COLUMN_NAME"]) for col in metadados)
    sql = f"SELECT {colunas} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela)}"
    if filtro:
        sql += f" WHERE {filtro}"
    return sql


def montar_insert_sqlserver(tabela: str, metadados: list[dict[str, Any]]) -> str:
    colunas = ", ".join(nome_sqlserver(col["COLUMN_NAME"]) for col in metadados)
    placeholders = ", ".join("?" for _ in metadados)
    return f"INSERT INTO {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} ({colunas}) VALUES ({placeholders})"


def executar_carga(hana_engine, sql_conn: pyodbc.Connection, sql_select: str, sql_insert: str) -> int:
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
