import argparse
import sys
import traceback

from ingestion.config import carregar_tabelas
from ingestion.connections import (
    criar_conexao_sqlserver,
    criar_engine_hana,
    testar_conexao_hana,
    testar_conexao_sqlserver,
)
from ingestion.strategies import executar_append, executar_full_reload, executar_upsert


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--frequencia",
        choices=["diaria", "semanal"],
        default=None,
        help="Filtrar tabelas por frequência. Sem argumento roda todas.",
    )
    args = parser.parse_args()

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

    tabelas = carregar_tabelas()
    tabelas_filtradas = {
        t: cfg for t, cfg in tabelas.items()
        if args.frequencia is None or cfg["frequencia"] == args.frequencia
    }

    total = len(tabelas_filtradas)
    sucesso = 0
    erros: list[tuple[str, str, str]] = []

    filtro_label = args.frequencia or "todas"
    print(f"\nIniciando carga incremental [{filtro_label}] — {total} tabelas...\n")

    try:
        for i, (tabela, cfg) in enumerate(tabelas_filtradas.items(), 1):
            prefixo = f"[{i:>3}/{total}] {tabela:<30}"
            estrategia = cfg["estrategia"]
            try:
                print(f"{prefixo} [{estrategia}] carregando...", end="", flush=True)

                match estrategia:
                    case "incremental_upsert":
                        linhas, segundos = executar_upsert(
                            hana_engine, sql_conn, tabela,
                            cfg["colunas"], cfg["tipo"],
                            cfg["chave_primaria"], cfg["coluna_watermark"],
                        )
                    case "incremental_append":
                        linhas, segundos = executar_append(
                            hana_engine, sql_conn, tabela,
                            cfg["colunas"], cfg["tipo"], cfg["coluna_watermark"],
                        )
                    case _:
                        linhas, segundos = executar_full_reload(
                            hana_engine, sql_conn, tabela,
                            cfg["colunas"], cfg["tipo"],
                        )

                print(f"\r{prefixo} [{estrategia}] OK — {linhas:>8} linhas em {segundos:>8.2f}s")
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
