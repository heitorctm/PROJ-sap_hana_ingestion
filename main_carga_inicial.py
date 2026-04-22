import sys
import traceback

from ingestion.config import carregar_tabelas
from ingestion.connections import (
    criar_conexao_sqlserver,
    criar_engine_hana,
    testar_conexao_hana,
    testar_conexao_sqlserver,
)
from ingestion.loader import (
    criar_tabela_se_nao_existir,
    executar_carga,
    garantir_schema_raw,
    montar_insert_sqlserver,
    montar_select_hana,
)
from ingestion.metadata import buscar_metadados_tabela
from ingestion.strategies import executar_janela, gerar_janelas


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

    garantir_schema_raw(sql_conn)

    tabelas = carregar_tabelas()
    total = len(tabelas)
    sucesso = 0
    erros: list[tuple[str, str, str]] = []

    print(f"\nIniciando carga inicial de {total} tabelas...\n")

    try:
        for i, (tabela, cfg) in enumerate(tabelas.items(), 1):
            prefixo = f"[{i:>3}/{total}] {tabela:<30}"
            ci = cfg["carga_inicial"]
            inicio = ci.get("inicio")
            fim = ci.get("fim")
            janela_meses = ci.get("janela_meses")
            coluna_watermark = cfg["coluna_watermark"]

            try:
                metadados = buscar_metadados_tabela(
                    hana_engine, tabela, cfg["colunas"], cfg["tipo"]
                )
                if not metadados:
                    raise ValueError(f"Sem metadados no HANA para {tabela}")

                criar_tabela_se_nao_existir(sql_conn, tabela, metadados)

                if inicio and fim and janela_meses and coluna_watermark:
                    janelas = gerar_janelas(inicio, fim, janela_meses)
                    total_linhas = 0
                    for j, (j_inicio, j_fim) in enumerate(janelas, 1):
                        print(
                            f"{prefixo} janela {j:>2}/{len(janelas)} [{j_inicio} → {j_fim}]...",
                            end="", flush=True,
                        )
                        linhas = executar_janela(
                            hana_engine, sql_conn, tabela, metadados,
                            coluna_watermark, j_inicio, j_fim,
                        )
                        total_linhas += linhas
                        print(f"\r{prefixo} janela {j:>2}/{len(janelas)} [{j_inicio} → {j_fim}] OK — {linhas:>8} linhas")

                    print(f"{prefixo} TOTAL: {total_linhas} linhas em {len(janelas)} janelas")

                else:
                    print(f"{prefixo} [full] carregando...", end="", flush=True)
                    sql_select = montar_select_hana(tabela, metadados)
                    sql_insert = montar_insert_sqlserver(tabela, metadados)
                    total_linhas = executar_carga(hana_engine, sql_conn, sql_select, sql_insert)
                    print(f"\r{prefixo} [full] OK — {total_linhas:>8} linhas")

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
