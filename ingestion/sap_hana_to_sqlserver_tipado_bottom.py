"""
SAP HANA -> SQL Server Local
Carga raw tipada a partir dos metadados do SAP HANA.
Variante: sem recriar tabelas, usa LIMIT/OFFSET para buscar slice alternativo dos dados.

Objetivo:
- extrair dados do SAP HANA usando LIMIT e OFFSET (sem TOP)
- inserir em tabelas raw já existentes no SQL Server
- manter carregamento em lotes para reduzir uso de memória
"""

import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any

import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv(Path(__file__).parent.parent / ".env")

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = os.getenv("HANA_PORT")
HANA_USER = os.getenv("HANA_USER")
HANA_PASSWORD = os.getenv("HANA_PASSWORD")
HANA_SCHEMA = os.getenv("HANA_SCHEMA")

SQLSERVER_SERVER = os.getenv("SQLSERVER_SERVER")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE")
SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")

TOP_N = 5000
OFFSET = 20000
CHUNK_SIZE = 10000
HANA_TIMEOUT = 30000
QUERY_TIMEOUT = 300000
RAW_SCHEMA = "raw"

TABELAS = [
    "OQUT", "QUT1",
    "ORDR", "RDR1",
    "ODLN", "DLN1",
    "OINV", "INV1",
    "ORIN", "RIN1",
    "ODPI", "DPI1",
    "OPOR", "POR1",
    "OPDN", "PDN1",
    "OPCH", "PCH1",
    "ORPC", "RPC1",
    "ODPO", "DPO1",
    "ORCT", "RCT1", "RCT2", "RCT3",
    "OVPM", "VPM1", "VPM2",
    "OCRD", "CRD1", "CRD7", "OCRG", "OCPR",
    "OITM", "ITM1", "OITB", "OITW", "OIVL", "OINM", "OWHS",
    "OIGE", "IGE1", "OIGN", "IGN1", "OWTR", "WTR1", "OWTQ", "WTQ1", "OINC", "INC1",
    "OJDT", "JDT1", "OACT", "OBGT", "BGT1", "OPRC", "OFPR",
    "OSLP", "OHEM", "OBPL", "OMRC", "OPLN", "OSRI", "OBTN", "OCTG", "OPYM", "OUSR", "OSHP", "OPRJ",
    "OWOR", "WOR1",
    "@BDI",
    "@CALC_IMPOSTO_ITEM",
    "@CALENDARIO_SEMANAL",
    "@DEPARTAMENTO",
    "@EIXO_COMISSAO",
    "@ESTOQUE_DIARIO",
    "@IB_CATEG_FIN",
    "@INV_ESTOQUE_DEP",
    "@IN_CCC",
    "@IN_DATAUSER",
    "@ITEM_CLASSE",
    "@ITEM_FAMILIA",
    "@ITEM_SUB_CLASSE",
    "@LOJAS",
    "@LUCRO_RANKING",
    "@META_LOJA",
    "@META_VAREJO",
    "@META_VENDEDOR",
    "@PERMISSAO",
    "@PRECO_S9",
    "@SAUDE_CREDITO",
    "@USER_FATURAMENTO",
    "@USUARIO_PORTAL",
]


def criar_engine_hana():
    return create_engine(
        URL.create(
            "hana",
            username=HANA_USER,
            password=HANA_PASSWORD,
            host=HANA_HOST,
            port=int(HANA_PORT),
        ),
        connect_args={
            "connectTimeout": HANA_TIMEOUT,
            "communicationTimeout": QUERY_TIMEOUT,
        },
    )


def criar_conexao_sqlserver() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{SQLSERVER_DRIVER}}};"
        f"SERVER={SQLSERVER_SERVER};"
        f"DATABASE={SQLSERVER_DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def testar_conexao_hana(hana_engine) -> None:
    try:
        with hana_engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM DUMMY"))
    except Exception as e:
        raise ConnectionError(
            "Falha ao conectar no SAP HANA. Verifique se a VPN está conectada, "
            "se o host/porta estão acessíveis e se as credenciais do .env estão corretas."
        ) from e


def testar_conexao_sqlserver(sql_conn: pyodbc.Connection) -> None:
    try:
        cursor = sql_conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
    except Exception as e:
        raise ConnectionError(
            "Falha ao conectar no SQL Server. Verifique se o SQL Server está ativo, "
            "se o banco existe e se o driver ODBC está instalado corretamente."
        ) from e


def buscar_metadados_tabela(hana_engine, tabela: str) -> list[dict[str, Any]]:
    sql = text(
        """
        SELECT
            COLUMN_NAME AS "COLUMN_NAME",
            DATA_TYPE_NAME AS "DATA_TYPE_NAME",
            LENGTH AS "LENGTH",
            SCALE AS "SCALE",
            POSITION AS "POSITION"
        FROM SYS.TABLE_COLUMNS
        WHERE SCHEMA_NAME = :schema
          AND TABLE_NAME = :tabela
        ORDER BY POSITION
        """
    )

    with hana_engine.connect() as conn:
        rows = conn.execute(sql, {"schema": HANA_SCHEMA, "tabela": tabela}).mappings().all()

    metadados = []
    for row in rows:
        row_dict = dict(row)
        row_normalizado = {str(chave).upper(): valor for chave, valor in row_dict.items()}
        metadados.append(
            {
                "COLUMN_NAME": row_normalizado.get("COLUMN_NAME"),
                "DATA_TYPE_NAME": row_normalizado.get("DATA_TYPE_NAME"),
                "LENGTH": row_normalizado.get("LENGTH"),
                "SCALE": row_normalizado.get("SCALE"),
                "POSITION": row_normalizado.get("POSITION"),
            }
        )

    metadados = [coluna for coluna in metadados if coluna["COLUMN_NAME"]]
    return metadados


def nome_sqlserver(nome: str) -> str:
    return "[" + nome.replace("]", "]]") + "]"


def nome_hana(nome: str) -> str:
    return '"' + nome.replace('"', '""') + '"'


def montar_select_hana(tabela: str, metadados: list[dict[str, Any]]) -> str:
    colunas = ", ".join(nome_hana(coluna["COLUMN_NAME"]) for coluna in metadados)
    return f"SELECT {colunas} FROM {nome_hana(HANA_SCHEMA)}.{nome_hana(tabela)} LIMIT {TOP_N} OFFSET {OFFSET}"


def montar_insert_sqlserver(tabela: str, metadados: list[dict[str, Any]]) -> str:
    colunas = ", ".join(nome_sqlserver(coluna["COLUMN_NAME"]) for coluna in metadados)
    placeholders = ", ".join("?" for _ in metadados)
    return f"INSERT INTO {nome_sqlserver(RAW_SCHEMA)}.{nome_sqlserver(tabela)} ({colunas}) VALUES ({placeholders})"


def normalizar_valor(valor: Any) -> Any:
    if isinstance(valor, memoryview):
        return bytes(valor)
    return valor


def carregar_tabela(hana_engine, sql_conn: pyodbc.Connection, tabela: str) -> tuple[int, float]:
    inicio = time.perf_counter()
    metadados = buscar_metadados_tabela(hana_engine, tabela)

    if not metadados:
        raise ValueError(f"Tabela sem metadados no HANA: {HANA_SCHEMA}.{tabela}")

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

            lote = [tuple(normalizar_valor(valor) for valor in row) for row in rows]
            cursor_destino.executemany(sql_insert, lote)
            sql_conn.commit()
            total_linhas += len(lote)

    duracao = time.perf_counter() - inicio
    return total_linhas, duracao


def main() -> None:
    hana_engine = None
    sql_conn = None

    try:
        print("Criando engine SAP HANA...")
        hana_engine = criar_engine_hana()

        print("Testando conexão SAP HANA...")
        testar_conexao_hana(hana_engine)
        print("SAP HANA conectado com sucesso.")

        print("Conectando ao SQL Server local...")
        sql_conn = criar_conexao_sqlserver()

        print("Testando conexão SQL Server...")
        testar_conexao_sqlserver(sql_conn)
        print("SQL Server conectado com sucesso.")

    except Exception as e:
        print("\nERRO DE CONEXÃO/AMBIENTE")
        print(str(e))
        print("\nDetalhes técnicos:")
        print(traceback.format_exc())

        if sql_conn is not None:
            sql_conn.close()
        if hana_engine is not None:
            hana_engine.dispose()

        sys.exit(1)

    total = len(TABELAS)
    sucesso = 0
    erros: list[tuple[str, str, str]] = []

    print(f"\nIniciando carga raw tipada de {total} tabelas (LIMIT {TOP_N} OFFSET {OFFSET})...\n")

    try:
        for i, tabela in enumerate(TABELAS, 1):
            prefixo = f"[{i:>3}/{total}] {tabela:<30}"
            try:
                print(f"{prefixo} carregando...", end="", flush=True)
                linhas, segundos = carregar_tabela(hana_engine, sql_conn, tabela)
                print(f"\r{prefixo} OK — {linhas:>8} linhas em {segundos:>8.2f}s")
                sucesso += 1
            except Exception as e:
                erro_completo = traceback.format_exc()
                print(f"\r{prefixo} ERRO: {e}")
                erros.append((tabela, str(e), erro_completo))
    finally:
        if sql_conn is not None:
            sql_conn.close()
        if hana_engine is not None:
            hana_engine.dispose()

    print(f"\n{'─' * 70}")
    print(f"Concluído: {sucesso}/{total} tabelas carregadas")

    if erros:
        print(f"\nTabelas com erro ({len(erros)}):")
        for tabela, msg, detalhe in erros:
            print(f"\n  {tabela}: {msg}")
            print("  Detalhes técnicos:")
            print("  " + detalhe.replace("\n", "\n  ").rstrip())


if __name__ == "__main__":
    main()
