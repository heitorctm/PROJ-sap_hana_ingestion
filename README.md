# fast-dw — Ingestão SAP HANA → SQL Server

Pipeline de extração de dados do SAP Business One (SAP HANA) para SQL Server. Responsável pela camada **raw** da arquitetura medallion.

---

## Visão Geral

```
SAP HANA (ECOSISTEMA_FAST_TST_020326)
        ↓  extração via sqlalchemy-hana
SQL Server → schema raw    (espelho das tabelas SAP)
           → schema audit  (log de execução)
```

O pipeline opera em dois modos:

| Script | Quando usar |
|--------|------------|
| `main_carga_inicial.py` | Primeira carga ou reprocessamento de períodos históricos |
| `main_incremental.py` | Carga diária ou semanal em produção |

---

## Estrutura do Projeto

```
fast-sap-dw/
├── ingestion/
│   ├── config.py       # constantes, variáveis de ambiente, leitura do tables.yaml
│   ├── connections.py  # criação e teste de conexões HANA e SQL Server
│   ├── metadata.py     # busca metadados das colunas no HANA e mapeia tipos
│   ├── loader.py       # I/O puro: DDL, SELECT, INSERT, execução em chunks
│   ├── watermark.py    # lê o max(watermark) do raw para carga incremental
│   ├── strategies.py   # orquestra as estratégias de carga
│   └── audit.py        # registra início, sucesso e erro no schema audit
├── main_carga_inicial.py
├── main_incremental.py
├── tables.yaml         # definição de todas as tabelas e suas configurações
├── requirements.txt
└── .env                # credenciais (não versionado)
```

---

## Configuração

### Variáveis de ambiente (.env)

```env
HANA_HOST=
HANA_PORT=
HANA_USER=
HANA_PASSWORD=
HANA_SCHEMA=

SQLSERVER_SERVER=
SQLSERVER_DATABASE=
SQLSERVER_DRIVER=ODBC Driver 17 for SQL Server
```

### Instalação de dependências

```bash
pip install -r requirements.txt
```

`requirements.txt`:
```
hdbcli
sqlalchemy
sqlalchemy-hana
pyodbc
pyyaml
python-dotenv
python-dateutil
```

---

## Pré-requisitos no SQL Server

Criar os schemas antes da primeira execução:

```sql
CREATE SCHEMA raw
CREATE SCHEMA audit
```

Criar a tabela de log:

```sql
CREATE TABLE audit.log_ingestao (
    id                INT IDENTITY(1,1) PRIMARY KEY,
    execucao_id       UNIQUEIDENTIFIER NOT NULL,
    tabela            NVARCHAR(100) NOT NULL,
    estrategia        NVARCHAR(50) NOT NULL,
    frequencia        NVARCHAR(20) NOT NULL,
    inicio_em         DATETIME2 NOT NULL,
    fim_em            DATETIME2,
    linhas            INT,
    status            NVARCHAR(20) NOT NULL,
    mensagem_erro     NVARCHAR(MAX)
)
```

---

## Configuração das Tabelas (tables.yaml)

Cada tabela é declarada com os seguintes campos:

```yaml
OINV:
  tipo: tabela          # tabela | view
  estrategia: incremental_append   # incremental_append | incremental_upsert | full_reload
  frequencia: diaria    # diaria | semanal  (omitir = diaria)
  chave_primaria:
    - DocEntry
  coluna_watermark: UpdateDate
  carga_inicial:
    inicio: "2023-01-01"   # null = sem filtro de data
    fim: "2026-04-22"      # null = sem filtro de data
    janela_meses: 3        # null = carrega tudo de uma vez
  colunas:
    - DocEntry
    - DocDate
    - ...
```

### Estratégias de carga

| Estratégia | Comportamento | Quando usar |
|------------|--------------|-------------|
| `incremental_append` | Busca registros com `watermark > max(raw)` e insere | Tabelas onde registros não mudam retroativamente |
| `incremental_upsert` | Busca por watermark, deleta por PK e reinsere | Tabelas onde registros podem ser alterados após criação |
| `full_reload` | TRUNCATE + INSERT completo | Tabelas sem watermark natural ou views de status |

### Frequência

| Valor | Comportamento |
|-------|--------------|
| `diaria` (padrão) | Incluída na execução diária |
| `semanal` | Ignorada na execução diária — roda apenas na DAG semanal |

Tabelas com `frequencia: semanal`: **ITM1**, **OITW** — tabelas de preço e estoque com alto volume e baixa frequência de atualização.

---

## Execução

### Carga inicial (histórico completo)

```bash
python main_carga_inicial.py
```

- Cria a tabela no raw **se não existir** (não recria se já existir)
- Tabelas com `inicio`/`fim`/`janela_meses` são carregadas em janelas de N meses
- Tabelas sem período definido são carregadas integralmente
- Seguro para rodar em múltiplos dias — acumula sem duplicar

### Carga incremental

```bash
# todas as tabelas
python main_incremental.py

# apenas tabelas diárias (uso em produção — DAG diária)
python main_incremental.py --frequencia diaria

# apenas tabelas semanais (DAG semanal)
python main_incremental.py --frequencia semanal
```

---

## Arquitetura Interna

### Fluxo de dados

```
tables.yaml
    ↓ config.py
    ↓ connections.py  →  HANA engine + SQL Server connection
    ↓ metadata.py     →  busca colunas em SYS.TABLE_COLUMNS / SYS.VIEW_COLUMNS
    ↓ loader.py       →  monta SELECT (HANA) e INSERT (SQL Server)
    ↓ strategies.py   →  executa a estratégia correta por tabela
    ↓ audit.py        →  registra resultado em audit.log_ingestao
```

### Leitura em chunks

A extração usa `stream_results=True` com `fetchmany(10000)` — os dados nunca são carregados inteiros na memória. Cada chunk de 10.000 linhas é inserido no SQL Server via `fast_executemany=True` antes de buscar o próximo.

### Mapeamento de tipos HANA → SQL Server

| HANA | SQL Server |
|------|-----------|
| NVARCHAR | NVARCHAR(n) / NVARCHAR(MAX) |
| VARCHAR | VARCHAR(n) / VARCHAR(MAX) |
| INTEGER | INT |
| SMALLINT | SMALLINT |
| BIGINT | BIGINT |
| DECIMAL | DECIMAL(p,s) — se scale=null usa DECIMAL(38,10) |
| DOUBLE | FLOAT |
| DATE | DATE |
| TIMESTAMP / SECONDDATE | DATETIME2(7) |
| BOOLEAN | BIT |
| CLOB / NCLOB | NVARCHAR(MAX) |
| BLOB / VARBINARY | VARBINARY(MAX) |

> Views no HANA podem ter colunas calculadas com `scale=null`. Nesses casos o tipo é mapeado para `DECIMAL(38,10)` para preservar precisão.

### Coluna de auditoria

Todas as tabelas no raw recebem a coluna `_ingestao_em DATETIME2 DEFAULT GETDATE()`, preenchida automaticamente no momento da inserção.

---

## Monitoramento (audit.log_ingestao)

Cada execução gera um `execucao_id` (UUID) que agrupa todas as tabelas daquela rodada.

```sql
-- resumo da última execução
SELECT tabela, estrategia, linhas,
       DATEDIFF(SECOND, inicio_em, fim_em) AS segundos,
       status
FROM audit.log_ingestao
WHERE execucao_id = (SELECT TOP 1 execucao_id FROM audit.log_ingestao ORDER BY inicio_em DESC)
ORDER BY inicio_em

-- histórico de execuções
SELECT execucao_id,
       MIN(inicio_em) AS inicio,
       MAX(fim_em) AS fim,
       SUM(linhas) AS total_linhas,
       COUNT(*) AS tabelas,
       SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) AS erros
FROM audit.log_ingestao
GROUP BY execucao_id
ORDER BY MIN(inicio_em) DESC

-- tabelas com erro
SELECT tabela, inicio_em, mensagem_erro
FROM audit.log_ingestao
WHERE status = 'erro'
ORDER BY inicio_em DESC
```

---

## Tabelas Configuradas

| Tabela | Tipo | Estratégia | Frequência | Watermark |
|--------|------|-----------|------------|-----------|
| OINV | tabela | incremental_append | diaria | UpdateDate |
| OPCH | tabela | incremental_append | diaria | UpdateDate |
| OCRD | tabela | incremental_append | diaria | UpdateDate |
| OITM | tabela | incremental_append | diaria | UpdateDate |
| JDT1 | tabela | incremental_append | diaria | RefDate |
| INV6 | tabela | full_reload | diaria | — |
| PCH6 | tabela | full_reload | diaria | — |
| BTF1 | tabela | incremental_append | diaria | RefDate |
| ITM1 | tabela | full_reload | **semanal** | — |
| OITW | tabela | full_reload | **semanal** | — |
| OACT | tabela | full_reload | diaria | — |
| OCRG | tabela | full_reload | diaria | — |
| OBPL | tabela | full_reload | diaria | — |
| OWHS | tabela | full_reload | diaria | — |
| NFN1 | tabela | full_reload | diaria | — |
| OSLP | tabela | full_reload | diaria | — |
| OBTF | tabela | full_reload | diaria | — |
| @ITEM_FAMILIA | tabela | full_reload | diaria | — |
| @ITEM_SUB_CLASSE | tabela | full_reload | diaria | — |
| @ITEM_CLASSE | tabela | full_reload | diaria | — |
| @LOJAS | tabela | full_reload | diaria | — |
| VW_PBI_RANKING_VENDAS_V3 | view | incremental_append | diaria | DATA |
| VW_FAROL_MAPA_RELACAO | view | full_reload | diaria | — |

> **VW_FAROL_MAPA_RELACAO**: view de status de documentos (orçamento → pedido → nota). Requer full_reload pois o status de qualquer documento pode mudar a qualquer momento.
>
> **ITM1** e **OITW**: tabelas de preço e estoque por armazém. Sem coluna de data para incremental — full_reload semanal fora do horário comercial.
>
> **INV6** e **PCH6**: tabelas de parcelas de recebimento/pagamento. Não possuem coluna de data de criação ou atualização — `DueDate` é a data de vencimento, não confiável como watermark. `Status` e `Paid` mudam ao longo do tempo (quitação, renegociação). Full_reload diário garante consistência.
