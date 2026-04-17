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
# CONFIG — lido do .env
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

TOP_N         = 100   # None = todas as linhas | int = limita (ex: 500)
HANA_TIMEOUT  = 30000  # Timeout conexão HANA (ms) = 30s
QUERY_TIMEOUT = 300000 # Timeout por query (ms) = 5min

# ──────────────────────────────────────────────
# TABELAS
# ──────────────────────────────────────────────

TABELAS = [
    # Ciclo de Vendas
    "OQUT", "QUT1",   # Cotação de vendas + linhas
    "ORDR", "RDR1",   # Pedido de venda + linhas
    "ODLN", "DLN1",   # Entrega + linhas
    "OINV", "INV1",   # Nota Fiscal de Saída + linhas
    "ORIN", "RIN1",   # Devolução NF Saída + linhas
    "ODPI", "DPI1",   # Adiantamento de cliente + linhas

    # Ciclo de Compras
    "OPOR", "POR1",   # Pedido de compra + linhas
    "OPDN", "PDN1",   # Recebimento de mercadorias + linhas
    "OPCH", "PCH1",   # Nota Fiscal de Entrada + linhas
    "ORPC", "RPC1",   # Devolução NF Entrada + linhas
    "ODPO", "DPO1",   # Adiantamento para fornecedor + linhas

    # Pagamentos
    "ORCT",           # Contas a receber (cabeçalho)
    "RCT1",           # Pagamentos recebidos - cheques
    "RCT2",           # Pagamentos recebidos - notas fiscais
    "RCT3",           # Pagamentos recebidos - cartões
    "OVPM",           # Contas a pagar (cabeçalho)
    "VPM1",           # Pagamentos emitidos - cheques
    "VPM2",           # Pagamentos emitidos - notas fiscais

    # Parceiros de Negócios
    "OCRD",           # Business Partners
    "CRD1",           # Endereços do PN
    "CRD7",           # IDs fiscais do PN
    "OCRG",           # Grupos de PNs
    "OCPR",           # Pessoas de contato

    # Itens e Estoque
    "OITM", "ITM1",   # Itens + preços
    "OITB",           # Grupos de itens
    "OITW",           # Itens por depósito
    "OIVL",           # Diário do depósito (valuation)
    "OINM",           # Movimentações de estoque
    "OWHS",           # Depósitos
    "OIGE", "IGE1",   # Saída de mercadorias + linhas
    "OIGN", "IGN1",   # Entrada de mercadorias + linhas
    "OWTR", "WTR1",   # Transferência de estoque + linhas
    "OWTQ", "WTQ1",   # Pedido de transferência de estoque + linhas
    "OINC", "INC1",   # Contagem de estoque + linhas

    # Financeiro / Contabilidade
    "OJDT", "JDT1",   # Lançamento contábil + linhas
    "OACT",           # Plano de contas
    "OBGT", "BGT1",   # Orçamento + linhas
    "OPRC",           # Centro de custos
    "OFPR",           # Períodos contábeis

    # Cadastros de apoio
    "OSLP",           # Vendedores
    "OHEM",           # Colaboradores
    "OBPL",           # Filiais
    "OMRC",           # Fabricantes
    "OPLN",           # Listas de preços
    "OSRI",           # Números de série
    "OBTN",           # Lotes
    "OCTG",           # Condições de pagamento
    "OPYM",           # Métodos de pagamento
    "OUSR",           # Usuários SAP
    "OSHP",           # Tipos de envio / transportadoras
    "OPRJ",           # Códigos de projeto

    # Produção
    "OWOR", "WOR1",   # Ordem de produção + linhas

    # Tabelas customizadas (@)
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
# INGESTÃO
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
            # Deduplica nomes de coluna (SAP HANA pode retornar duplicatas em SELECT *)
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
