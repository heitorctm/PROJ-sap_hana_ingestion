"""
SAP HANA -> SQL Server Local
Extrai TOP N linhas das tabelas core do SAP B1 e carrega no SQL Server.

Dependências:
    pip install -r requirements.txt

Configuração:
    Preencha o arquivo .env na raiz do repositório (fast-sap-dw/.env)
"""

import os
from pathlib import Path
from hdbcli import dbapi
from sqlalchemy import create_engine
from dotenv import load_dotenv
import polars as pl

# ──────────────────────────────────────────────
# config: lido do .env
# ──────────────────────────────────────────────

load_dotenv(Path(__file__).parent.parent / ".env")

HANA_HOST     = os.getenv("HANA_HOST")
HANA_PORT     = os.getenv("HANA_PORT")
HANA_USER     = os.getenv("HANA_USER")
HANA_PASSWORD = os.getenv("HANA_PASSWORD")
HANA_SCHEMA   = os.getenv("HANA_SCHEMA")

SQLSERVER_SERVER   = os.getenv("SQLSERVER_SERVER", "localhost\\SQLEXPRESS")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "SAP_MIRROR")

SQLSERVER_CONN = (
    f"mssql+pyodbc://{SQLSERVER_SERVER}/{SQLSERVER_DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

TOP_N         = 1000   # none = todas as linhas | int = limita (ex: 500)
HANA_TIMEOUT  = 30000  # timeout conexão hana (ms) = 30s
QUERY_TIMEOUT = 300000 # timeout por query (ms) = 5min

# ──────────────────────────────────────────────
# tabelas
# ──────────────────────────────────────────────

TABELAS = [
    # ciclo de vendas
    "OQUT", "QUT1",   # cotação de vendas, linhas
    "ORDR", "RDR1",   # pedido de venda, linhas
    "ODLN", "DLN1",   # entrega, linhas
    "OINV", "INV1",   # nota fiscal de saída, linhas
    "ORIN", "RIN1",   # devolução nf saída, linhas
    "ODPI", "DPI1",   # adiantamento de cliente, linhas

    # ciclo de compras
    "OPOR", "POR1",   # pedido de compra, linhas
    "OPDN", "PDN1",   # recebimento de mercadorias, linhas
    "OPCH", "PCH1",   # nota fiscal de entrada, linhas
    "ORPC", "RPC1",   # devolução nf entrada, linhas
    "ODPO", "DPO1",   # adiantamento para fornecedor, linhas

    # pagamentos
    "ORCT",           # contas a receber: cabeçalho
    "RCT1",           # pagamentos recebidos: cheques
    "RCT2",           # pagamentos recebidos: notas fiscais
    "RCT3",           # pagamentos recebidos: cartões
    "OVPM",           # contas a pagar: cabeçalho
    "VPM1",           # pagamentos emitidos: cheques
    "VPM2",           # pagamentos emitidos: notas fiscais

    # parceiros de negócios
    "OCRD",           # business partners
    "CRD1",           # endereços do pn
    "CRD7",           # ids fiscais do pn
    "OCRG",           # grupos de pns
    "OCPR",           # pessoas de contato

    # itens e estoque
    "OITM", "ITM1",   # itens, preços
    "OITB",           # grupos de itens
    "OITW",           # itens por depósito
    "OIVL",           # diário do depósito: valuation
    "OINM",           # movimentações de estoque
    "OWHS",           # depósitos
    "OIGE", "IGE1",   # saída de mercadorias, linhas
    "OIGN", "IGN1",   # entrada de mercadorias, linhas
    "OWTR", "WTR1",   # transferência de estoque, linhas
    "OWTQ", "WTQ1",   # pedido de transferência de estoque, linhas
    "OINC", "INC1",   # contagem de estoque, linhas

    # financeiro e contabilidade
    "OJDT", "JDT1",   # lançamento contábil, linhas
    "OACT",           # plano de contas
    "OBGT", "BGT1",   # orçamento, linhas
    "OPRC",           # centro de custos
    "OFPR",           # períodos contábeis

    # cadastros de apoio
    "OSLP",           # vendedores
    "OHEM",           # colaboradores
    "OBPL",           # filiais
    "OMRC",           # fabricantes
    "OPLN",           # listas de preços
    "OSRI",           # números de série
    "OBTN",           # lotes
    "OCTG",           # condições de pagamento
    "OPYM",           # métodos de pagamento
    "OUSR",           # usuários sap
    "OSHP",           # tipos de envio, transportadoras
    "OPRJ",           # códigos de projeto

    # produção
    "OWOR", "WOR1",   # ordem de produção, linhas

    # tabelas customizadas
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

# ──────────────────────────────────────────────
# ingestão
# ──────────────────────────────────────────────

def main():
    print("Conectando ao SAP HANA...")
    hana = dbapi.connect(
        address=HANA_HOST,
        port=HANA_PORT,
        user=HANA_USER,
        password=HANA_PASSWORD,
        connectTimeout=HANA_TIMEOUT,
        communicationTimeout=QUERY_TIMEOUT,
    )
    cursor = hana.cursor()
    cursor.arraysize = 10000

    print("Conectando ao SQL Server local...")
    engine = create_engine(SQLSERVER_CONN, fast_executemany=True)

    total   = len(TABELAS)
    sucesso = 0
    erros   = []

    modo = f"TOP {TOP_N} mais recentes" if TOP_N else "COMPLETO (todas as linhas)"
    print(f"\nIniciando extração de {total} tabelas ({modo})...\n")

    for i, tabela in enumerate(TABELAS, 1):
        prefixo = f"[{i:>3}/{total}] {tabela:<30}"
        try:
            print(f"{prefixo} extraindo do HANA...", end="", flush=True)
            if TOP_N:
                sql = f'SELECT TOP {TOP_N} * FROM "{HANA_SCHEMA}"."{tabela}"'
            else:
                sql = f'SELECT * FROM "{HANA_SCHEMA}"."{tabela}"'
            cursor.execute(sql)
            colunas_raw = [col[0] for col in cursor.description]
            # deduplica nomes de coluna: sap hana pode retornar duplicatas em select *
            seen: dict[str, int] = {}
            colunas = []
            for c in colunas_raw:
                if c in seen:
                    seen[c] += 1
                    colunas.append(f"{c}_{seen[c]}")
                else:
                    seen[c] = 0
                    colunas.append(c)
            dados = cursor.fetchall()
            n = len(colunas)
            if dados:
                df = pl.DataFrame(
                    {colunas[j]: [None if row[j] is None else str(row[j]) for row in dados] for j in range(n)}
                )
            else:
                df = pl.DataFrame(schema=colunas)

            print(f"\r{prefixo} carregando no SQL Server ({len(df)} linhas)...", end="", flush=True)
            df.write_database(
                f"raw.{tabela}",
                engine,
                if_table_exists="replace",
            )
            print(f"\r{prefixo} OK — {len(df):>5} linhas")
            sucesso += 1

        except Exception as e:
            print(f"\r{prefixo} ERRO: {e}")
            erros.append((tabela, str(e)))

    cursor.close()
    hana.close()

    print(f"\n{'─'*50}")
    print(f"Concluído: {sucesso}/{total} tabelas carregadas")

    if erros:
        print(f"\nTabelas com erro ({len(erros)}):")
        for tabela, msg in erros:
            print(f"  {tabela}: {msg}")


if __name__ == "__main__":
    main()
