import sys
import time
import traceback
from datetime import date
from uuid import uuid4

from ingestion.audit import registrar_erro, registrar_inicio, registrar_sucesso
from ingestion.config import carregar_tabelas
from ingestion.connections import criar_conexao_sqlserver, criar_engine_hana, testar_conexao_hana, testar_conexao_sqlserver
from ingestion.loader import adicionar_colunas_faltantes, criar_tabela_se_nao_existir, executar_carga, garantir_schema_raw, montar_insert_sqlserver, montar_select_hana
from ingestion.metadata import buscar_metadados_tabela
from ingestion.strategies import executar_janela, executar_janela_via_cabecalho, gerar_janelas


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

    execucao_id = uuid4()
    tabelas = carregar_tabelas()
    total = len(tabelas)
    sucesso = 0
    erros: list[tuple[str, str, str]] = []

    print(f"\nIniciando carga inicial de {total} tabelas...\n")

    inicio_total = time.perf_counter()

    try:
        for i, (tabela, cfg) in enumerate(tabelas.items(), 1):
            prefixo = f"[{i:>3}/{total}] {tabela:<30}"
            ci = cfg["carga_inicial"]
            inicio = ci.get("inicio")
            fim = ci.get("fim") or (date.today().isoformat() if ci.get("inicio") else None)
            janela_meses = ci.get("janela_meses")
            coluna_watermark = cfg["coluna_watermark"]
            tabela_cabecalho = cfg.get("tabela_cabecalho")
            coluna_watermark_cabecalho = cfg.get("coluna_watermark_cabecalho")
            estrategia = cfg["estrategia"]
            frequencia = cfg["frequencia"]

            try:
                metadados = buscar_metadados_tabela(
                    hana_engine, tabela, cfg["colunas"], cfg["tipo"]
                )
                if not metadados:
                    raise ValueError(f"Sem metadados no HANA para {tabela}")

                criar_tabela_se_nao_existir(sql_conn, tabela, metadados)
                adicionar_colunas_faltantes(sql_conn, tabela, metadados)
                registrar_inicio(sql_conn, execucao_id, tabela, estrategia, frequencia)
                inicio_tabela = time.perf_counter()

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

                    segundos = time.perf_counter() - inicio_tabela
                    print(f"{prefixo} TOTAL: {total_linhas:>8} linhas em {len(janelas)} janelas — {segundos:.2f}s")

                elif inicio and fim and janela_meses and tabela_cabecalho and coluna_watermark_cabecalho:
                    janelas = gerar_janelas(inicio, fim, janela_meses)
                    total_linhas = 0
                    for j, (j_inicio, j_fim) in enumerate(janelas, 1):
                        print(
                            f"{prefixo} janela {j:>2}/{len(janelas)} [{j_inicio} → {j_fim}]...",
                            end="", flush=True,
                        )
                        linhas = executar_janela_via_cabecalho(
                            hana_engine, sql_conn, tabela, metadados,
                            cfg["chave_primaria"],
                            tabela_cabecalho, coluna_watermark_cabecalho,
                            j_inicio, j_fim,
                        )
                        total_linhas += linhas
                        print(f"\r{prefixo} janela {j:>2}/{len(janelas)} [{j_inicio} → {j_fim}] OK — {linhas:>8} linhas")

                    segundos = time.perf_counter() - inicio_tabela
                    print(f"{prefixo} TOTAL: {total_linhas:>8} linhas em {len(janelas)} janelas — {segundos:.2f}s")

                else:
                    print(f"{prefixo} [full] carregando...", end="", flush=True)
                    sql_select = montar_select_hana(tabela, metadados)
                    sql_insert = montar_insert_sqlserver(tabela, metadados)
                    total_linhas = executar_carga(hana_engine, sql_conn, sql_select, sql_insert)
                    segundos = time.perf_counter() - inicio_tabela
                    print(f"\r{prefixo} [full] OK — {total_linhas:>8} linhas — {segundos:.2f}s")

                registrar_sucesso(sql_conn, execucao_id, tabela, total_linhas)
                sucesso += 1

            except Exception as e:
                erro_completo = traceback.format_exc()
                registrar_erro(sql_conn, execucao_id, tabela, erro_completo)
                print(f"\r{prefixo} ERRO: {e}")
                erros.append((tabela, str(e), erro_completo))

    finally:
        if sql_conn is not None:
            sql_conn.close()
        if hana_engine is not None:
            hana_engine.dispose()

    total_segundos = time.perf_counter() - inicio_total
    total_minutos = total_segundos / 60

    print(f"\n{'─' * 70}")
    print(f"Concluído: {sucesso}/{total} tabelas carregadas — {total_minutos:.1f} min ({total_segundos:.0f}s)")

    if erros:
        print(f"\nTabelas com erro ({len(erros)}):")
        for tabela, msg, detalhe in erros:
            print(f"\n  {tabela}: {msg}")
            print("  Detalhes técnicos:")
            print("  " + detalhe.replace("\n", "\n  ").rstrip())


if __name__ == "__main__":
    main()
