from datetime import datetime
from uuid import UUID

import pyodbc


def registrar_inicio(
    sql_conn: pyodbc.Connection,
    execucao_id: UUID,
    tabela: str,
    estrategia: str,
    frequencia: str,
) -> None:
    try:
        cursor = sql_conn.cursor()
        cursor.execute(
            """
            INSERT INTO audit.log_ingestao (execucao_id, tabela, estrategia, frequencia, inicio_em, status)
            VALUES (?, ?, ?, ?, ?, 'em_andamento')
            """,
            str(execucao_id), tabela, estrategia, frequencia, datetime.now(),
        )
        sql_conn.commit()
    except Exception as e:
        print(f"[audit] falha ao registrar início de {tabela}: {e}")


def registrar_sucesso(
    sql_conn: pyodbc.Connection,
    execucao_id: UUID,
    tabela: str,
    linhas: int,
) -> None:
    try:
        cursor = sql_conn.cursor()
        cursor.execute(
            """
            UPDATE audit.log_ingestao
            SET fim_em = ?, linhas = ?, status = 'sucesso'
            WHERE execucao_id = ? AND tabela = ?
            """,
            datetime.now(), linhas, str(execucao_id), tabela,
        )
        sql_conn.commit()
    except Exception as e:
        print(f"[audit] falha ao registrar sucesso de {tabela}: {e}")


def registrar_erro(
    sql_conn: pyodbc.Connection,
    execucao_id: UUID,
    tabela: str,
    mensagem: str,
) -> None:
    try:
        cursor = sql_conn.cursor()
        cursor.execute(
            """
            UPDATE audit.log_ingestao
            SET fim_em = ?, status = 'erro', mensagem_erro = ?
            WHERE execucao_id = ? AND tabela = ?
            """,
            datetime.now(), mensagem[:4000], str(execucao_id), tabela,
        )
        sql_conn.commit()
    except Exception as e:
        print(f"[audit] falha ao registrar erro de {tabela}: {e}")
