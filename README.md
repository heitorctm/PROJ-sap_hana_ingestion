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
  tipo: tabela                          # tabela | view
  estrategia: incremental_append        # incremental_append | incremental_upsert | incremental_via_cabecalho | full_reload | snapshot_diario
  frequencia: diaria                    # diaria | semanal  (omitir = diaria)
  chave_primaria:
    - DocEntry
  coluna_watermark: UpdateDate          # coluna de data no HANA usada no filtro
  coluna_watermark_ts: UpdateTS         # coluna de hora no HANA (HHMMSS como inteiro) — habilita watermark com precisão de segundo
  coluna_watermark_local: _ingestao_em  # coluna no raw usada como fallback quando coluna_watermark_ts não está definido
  append_idempotente: false             # true = deleta registros do dia-limite antes de inserir (para tabelas sem UpdateTS)
  carga_inicial:
    inicio: "2023-01-01"                # null = sem filtro de data
    fim: null                           # null = usa data de hoje
    janela_meses: 3                     # null = carrega tudo de uma vez
  colunas:
    - DocEntry
    - DocDate
    - ...
```

### Estratégias de carga

| Estratégia | Comportamento | Quando usar |
|------------|--------------|-------------|
| `incremental_append` | Busca e insere registros novos desde o último watermark. Com `coluna_watermark_ts`: precisão de segundo via UpdateDate+UpdateTS. Com `append_idempotente`: deleta o dia-limite antes de inserir | Cabeçalhos transacionais — acumula histórico de versões (OINV, OQUT, ORIN, ORDR, OPCH, JDT1, OJDT) |
| `incremental_upsert` | Busca registros com `coluna_watermark > max(coluna_watermark)`, deleta por PK e reinserere | Dados mestre — raw reflete estado atual com PK única (OITM, OCRD) |
| `incremental_via_cabecalho` | Busca DocEntries do cabeçalho atualizados, deleta+reinserere as linhas afetadas | Tabelas de linha sem UpdateDate próprio (INV1, QUT1, RDR1, RIN1) |
| `full_reload` | TRUNCATE + INSERT completo | Tabelas de referência pequenas e extensões de documento sem watermark |
| `snapshot_diario` | DELETE do dia atual + INSERT completo — acumula histórico via `_ingestao_em` | Tabelas de estado sem coluna de data própria (parcelas, preços, estoque por depósito) |

### Precisão do watermark incremental

`UpdateDate` no SAP tem precisão de dia — sem hora. Rodar o script duas vezes no mesmo dia sem proteção causaria duplicatas. O pipeline resolve isso de três formas dependendo da tabela:

**Watermark composto (`coluna_watermark_ts: UpdateTS`)** — OINV, OQUT, ORIN, ORDR, OPCH, OITM, OCRD

Essas tabelas têm `UpdateTS` (hora da última atualização no formato HHMMSS como inteiro). O pipeline lê `TOP 1 UpdateDate DESC, UpdateTS DESC` do raw e monta o filtro no HANA com precisão de segundo:

```sql
WHERE UpdateDate > '{data}' OR (UpdateDate = '{data}' AND UpdateTS > {ts})
```

Cada run captura apenas registros genuinamente mais novos que o último inserido — sem duplicatas independente de quantas vezes rodar no dia.

**Watermark composto nas tabelas de linha (`coluna_watermark_cabecalho_ts: UpdateTS`)** — INV1, QUT1, RIN1, RDR1

O pipeline captura o watermark do cabeçalho correspondente **antes de iniciar qualquer inserção**. Assim, quando OINV é processado e insere novas notas, INV1 usa o watermark pré-capturado (do estado anterior à run) para buscar no HANA os DocEntries afetados — garantindo que as linhas do cabeçalho atual sejam pegas na mesma run, e não na seguinte.

**Append idempotente (`append_idempotente: true`)** — JDT1, OJDT

Essas tabelas têm só `RefDate` (DATE, sem componente de hora). Antes de inserir, o pipeline deleta do raw todos os registros com `RefDate = MAX(RefDate)` e os reinserere. O dia-limite é sempre recarregado integralmente — idempotente por data.

### Frequência

| Valor | Comportamento |
|-------|--------------|
| `diaria` (padrão) | Incluída na execução diária |
| `semanal` | Ignorada na execução diária — roda apenas na DAG semanal |

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

Todas as tabelas no raw recebem a coluna `_ingestao_em DATETIME2 DEFAULT GETDATE()`, preenchida automaticamente no momento da inserção. Essa coluna serve também como watermark para tabelas com `snapshot_diario`.

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

| Tabela | Tipo | Estratégia | Frequência | Watermark HANA | Watermark Local |
|--------|------|-----------|------------|----------------|-----------------|
| OINV | tabela | incremental_append | diaria | UpdateDate+UpdateTS | _ingestao_em |
| INV1 | tabela | incremental_via_cabecalho | diaria | via OINV.UpdateDate+UpdateTS | — |
| OQUT | tabela | incremental_append | diaria | UpdateDate+UpdateTS | _ingestao_em |
| QUT1 | tabela | incremental_via_cabecalho | diaria | via OQUT.UpdateDate+UpdateTS | — |
| ORIN | tabela | incremental_append | diaria | UpdateDate+UpdateTS | _ingestao_em |
| RIN1 | tabela | incremental_via_cabecalho | diaria | via ORIN.UpdateDate+UpdateTS | — |
| ORDR | tabela | incremental_append | diaria | UpdateDate+UpdateTS | _ingestao_em |
| RDR1 | tabela | incremental_via_cabecalho | diaria | via ORDR.UpdateDate+UpdateTS | — |
| OPCH | tabela | incremental_append | diaria | UpdateDate+UpdateTS | _ingestao_em |
| OITM | tabela | incremental_upsert | diaria | UpdateDate+UpdateTS | UpdateDate+UpdateTS |
| OCRD | tabela | incremental_upsert | diaria | UpdateDate+UpdateTS | UpdateDate+UpdateTS |
| OJDT | tabela | incremental_append | diaria | RefDate | RefDate |
| JDT1 | tabela | incremental_append | diaria | RefDate | RefDate |
| INV3 | tabela | full_reload | diaria | — | — |
| INV12 | tabela | full_reload | diaria | — | — |
| QUT12 | tabela | full_reload | diaria | — | — |
| RIN12 | tabela | full_reload | diaria | — | — |
| OUSG | tabela | full_reload | diaria | — | — |
| OACT | tabela | full_reload | diaria | — | — |
| OCRG | tabela | full_reload | diaria | — | — |
| OBPL | tabela | full_reload | diaria | — | — |
| OWHS | tabela | full_reload | diaria | — | — |
| NFN1 | tabela | full_reload | diaria | — | — |
| OSLP | tabela | full_reload | diaria | — | — |
| @ITEM_FAMILIA | tabela | full_reload | diaria | — | — |
| @ITEM_SUB_CLASSE | tabela | full_reload | diaria | — | — |
| @ITEM_CLASSE | tabela | full_reload | diaria | — | — |
| @LOJAS | tabela | full_reload | diaria | — | — |
| INV6 | tabela | snapshot_diario | diaria | — | _ingestao_em |
| PCH6 | tabela | snapshot_diario | diaria | — | _ingestao_em |
| ITM1 | tabela | snapshot_diario | diaria | — | _ingestao_em |
| OITW | tabela | snapshot_diario | diaria | — | _ingestao_em |
| OMRC | tabela | full_reload | diaria | — | — |
| OPLN | tabela | full_reload | diaria | — | — |
| OEXD | tabela | full_reload | diaria | — | — |
| OUSR | tabela | full_reload | diaria | — | — |
| OVPM | tabela | full_reload | diaria | — | — |
| OPOR | tabela | incremental_append | diaria | UpdateDate | _ingestao_em |
| POR1 | tabela | incremental_via_cabecalho | diaria | via OPOR.UpdateDate | — |
| OPRQ | tabela | incremental_append | diaria | UpdateDate | _ingestao_em |
| PRQ1 | tabela | incremental_via_cabecalho | diaria | via OPRQ.UpdateDate | — |
| ODLN | tabela | incremental_append | diaria | UpdateDate | _ingestao_em |
| DLN1 | tabela | incremental_via_cabecalho | diaria | via ODLN.UpdateDate | — |
| PCH1 | tabela | incremental_via_cabecalho | diaria | via OPCH.UpdateDate+UpdateTS | — |
| PCH12 | tabela | full_reload | diaria | — | — |
| RIN3 | tabela | full_reload | diaria | — | — |
| RIN21 | tabela | full_reload | diaria | — | — |

> **Tabelas de linha (INV1, QUT1, RDR1, RIN1, PCH1, POR1, PRQ1, DLN1):** `incremental_via_cabecalho` — detectam alterações pelo watermark do cabeçalho correspondente, deletam e reinserem apenas as linhas afetadas.
>
> **Dados mestre (OITM, OCRD):** `incremental_upsert` — delete por PK + reinsert garante uma única linha por ItemCode/CardCode no raw.
>
> **Tabelas snapshot (INV6, PCH6, ITM1, OITW):** acumulam histórico diário via `_ingestao_em`. A cada execução, os registros do dia atual são deletados e reinseridos (idempotente). Nunca truncar essas tabelas — o histórico acumulado não existe no SAP HANA.
>
> **OVPM:** `full_reload` — sem campo `UpdateDate`; cancelamentos retroativos seriam invisíveis para qualquer estratégia incremental. 65k linhas em ~22s.
>
> **Pedidos de compra e requisições (OPOR/POR1, OPRQ/PRQ1):** `incremental_append` + `incremental_via_cabecalho` — cabeçalhos guardam histórico de estados (aberto → parcialmente entregue → fechado). Sem `UpdateTS`, o watermark é dia-granulado.
>
> **Entregas (ODLN/DLN1):** mesmo padrão de OPOR/POR1 — `incremental_append` no cabeçalho, `incremental_via_cabecalho` nas linhas.
