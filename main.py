import sys
import traceback

from ingestion.config import carregar_tabelas
from ingestion.connections import (
    criar_conexao_sqlserver,
    criar_engine_hana,
    testar_conexao_hana,
    testar_conexao_sqlserver,
)
from ingestion.loader import carregar_tabela, garantir_schema_raw


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

        garantir_schema_raw(sql_conn)

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

    tabelas = carregar_tabelas()
    total = len(tabelas)
    sucesso = 0
    erros: list[tuple[str, str, str]] = []

    print(f"\nIniciando carga raw de {total} tabelas...\n")

    try:
        for i, (tabela, colunas) in enumerate(tabelas.items(), 1):
            prefixo = f"[{i:>3}/{total}] {tabela:<30}"
            try:
                print(f"{prefixo} carregando...", end="", flush=True)
                linhas, segundos = carregar_tabela(hana_engine, sql_conn, tabela, colunas)
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
