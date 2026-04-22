import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from ingestion.config import (
    HANA_HOST,
    HANA_PASSWORD,
    HANA_PORT,
    HANA_TIMEOUT,
    HANA_USER,
    QUERY_TIMEOUT,
    SQLSERVER_DATABASE,
    SQLSERVER_DRIVER,
    SQLSERVER_SERVER,
)


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
