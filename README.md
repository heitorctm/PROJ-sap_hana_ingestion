# sap-b1-hana-ingestion

Data pipeline that extracts tables from SAP Business One (SAP HANA) and loads them into SQL Server. Implements the **raw layer** of a medallion architecture.

---

## Overview

```
SAP HANA (production schema)
        ↓  extraction via sqlalchemy-hana
SQL Server → schema raw    (mirror of SAP tables)
           → schema audit  (execution log)
```

Two entry points:

| Script | When to use |
|--------|-------------|
| `main_initial_load.py` | First load or historical reprocessing |
| `main_incremental_load.py` | Daily or weekly production runs |

---

## Project Structure

```
sap-b1-hana-ingestion/
├── ingestion/
│   ├── config.py       # environment variables and tables.yaml loading
│   ├── connections.py  # HANA and SQL Server connection factories
│   ├── metadata.py     # fetches column metadata from HANA and maps types
│   ├── loader.py       # DDL, SELECT, INSERT, chunked execution
│   ├── watermark.py    # reads max(watermark) from raw for incremental loads
│   ├── strategies.py   # orchestrates the correct strategy per table
│   └── audit.py        # logs start, success, and errors to the audit schema
├── main_initial_load.py
├── main_incremental_load.py
├── tables.yaml         # table definitions and ingestion configuration
├── requirements.txt
└── .env                # credentials (not versioned — see .env.example)
```

---

## Setup

### Environment variables

Copy `.env.example` to `.env` and fill in your credentials:

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

### Install dependencies

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

## SQL Server prerequisites

Create the schemas before the first run:

```sql
CREATE SCHEMA raw
CREATE SCHEMA audit
```

Create the audit log table:

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

## Table Configuration (tables.yaml)

Each table entry declares:

```yaml
OINV:
  tipo: tabela                          # type: tabela (table) | view
  estrategia: incremental_append        # ingestion strategy — see strategies below
  frequencia: diaria                    # frequency: diaria (daily) | semanal (weekly) — default: diaria
  chave_primaria:                       # primary key columns
    - DocEntry
  coluna_watermark: UpdateDate          # watermark column in HANA (date)
  coluna_watermark_ts: UpdateTS         # watermark time column in HANA (HHMMSS as integer) — enables second-precision
  coluna_watermark_local: _ingestao_em  # local watermark fallback column in raw
  append_idempotente: false             # if true: deletes boundary day before inserting (for tables with no UpdateTS)
  carga_inicial:                        # initial load config
    inicio: "2023-01-01"               # start date — null = no filter
    fim: null                           # end date — null = today
    janela_meses: 3                     # window size in months — null = load all at once
  colunas:                              # columns to extract
    - DocEntry
    - DocDate
    - ...
```

### Ingestion strategies

| Strategy | Behavior | When to use |
|----------|----------|-------------|
| `incremental_append` | Fetches and inserts records newer than the last watermark. With `coluna_watermark_ts`: second-precision via UpdateDate+UpdateTS. With `append_idempotente`: deletes the boundary day before inserting | Transactional headers that accumulate version history |
| `incremental_upsert` | Fetches records where `watermark > max(watermark)`, deletes by PK, and reinserts | Master data — raw always reflects current state with a single row per PK |
| `incremental_via_cabecalho` | Fetches updated header DocEntries, deletes and reinserts affected line rows | Line tables with no `UpdateDate` of their own |
| `full_reload` | TRUNCATE + full INSERT | Small reference tables and document extensions with no watermark |
| `snapshot_diario` | DELETE current day + full INSERT — accumulates history via `_ingestao_em` | State tables with no date column |

### Watermark precision

`UpdateDate` in SAP has day-level precision — no time component. Running the pipeline twice on the same day without protection would produce duplicates. The pipeline solves this in three ways:

**Composite watermark (`coluna_watermark_ts: UpdateTS`)** — tables like OINV, OITM, OCRD

These tables have `UpdateTS` (last update time as a HHMMSS integer). The pipeline reads `TOP 1 UpdateDate DESC, UpdateTS DESC` from raw and builds the HANA filter with second-level precision:

```sql
WHERE UpdateDate > '{date}' OR (UpdateDate = '{date}' AND UpdateTS > {ts})
```

Each run captures only records genuinely newer than the last inserted row — no duplicates regardless of how many times it runs per day.

**Header-based watermark for line tables (`coluna_watermark_cabecalho_ts: UpdateTS`)** — INV1

The pipeline captures the header watermark **before starting any insert**. This ensures that when OINV is processed and inserts new invoices, INV1 uses the pre-run watermark to fetch the affected DocEntries from HANA — guaranteeing those line rows are captured in the same run, not the next one.

**Idempotent append (`append_idempotente: true`)**

Tables with only a `RefDate` column (DATE, no time component). Before inserting, the pipeline deletes all records from raw where `RefDate = MAX(RefDate)` and reinserts them. The boundary day is always fully reloaded — idempotent by date.

### Frequency

| Value | Behavior |
|-------|----------|
| `diaria` (default) | Included in the daily run |
| `semanal` | Skipped on daily runs — only executes in the weekly DAG |

---

## Running

### Full historical load

```bash
python main_initial_load.py
```

- Creates the raw table **if it does not exist** (does not recreate if it already exists)
- Tables with `inicio`/`fim`/`janela_meses` are loaded in N-month windows
- Tables without a date range are loaded in full
- Safe to run across multiple days — accumulates without duplicating

### Incremental load

```bash
# all tables
python main_incremental_load.py

# daily tables only (production — daily DAG)
python main_incremental_load.py --frequencia diaria

# weekly tables only (weekly DAG)
python main_incremental_load.py --frequencia semanal
```

---

## Internal Architecture

### Data flow

```
tables.yaml
    ↓ config.py
    ↓ connections.py  →  HANA engine + SQL Server connection
    ↓ metadata.py     →  reads columns from SYS.TABLE_COLUMNS / SYS.VIEW_COLUMNS
    ↓ loader.py       →  builds SELECT (HANA) and INSERT (SQL Server)
    ↓ strategies.py   →  executes the correct strategy per table
    ↓ audit.py        →  writes result to audit.log_ingestao
```

### Chunked reading

Extraction uses `stream_results=True` with `fetchmany(10000)` — data is never fully loaded into memory. Each 10,000-row chunk is inserted into SQL Server via `fast_executemany=True` before fetching the next one.

### HANA → SQL Server type mapping

| HANA | SQL Server |
|------|-----------|
| NVARCHAR | NVARCHAR(n) / NVARCHAR(MAX) |
| VARCHAR | VARCHAR(n) / VARCHAR(MAX) |
| INTEGER | INT |
| SMALLINT | SMALLINT |
| BIGINT | BIGINT |
| DECIMAL | DECIMAL(p,s) — if scale=null uses DECIMAL(38,10) |
| DOUBLE | FLOAT |
| DATE | DATE |
| TIMESTAMP / SECONDDATE | DATETIME2(7) |
| BOOLEAN | BIT |
| CLOB / NCLOB | NVARCHAR(MAX) |
| BLOB / VARBINARY | VARBINARY(MAX) |

> HANA views may have calculated columns with `scale=null`. In those cases the type maps to `DECIMAL(38,10)` to preserve precision.

### Audit column

Every table in raw receives the column `_ingestao_em DATETIME2 DEFAULT GETDATE()`, populated automatically at insert time. This column also serves as the watermark for `snapshot_diario` tables.

---

## Monitoring (audit.log_ingestao)

Each execution generates an `execucao_id` (UUID) that groups all tables from that run.

```sql
-- last execution summary
SELECT tabela, estrategia, linhas,
       DATEDIFF(SECOND, inicio_em, fim_em) AS seconds,
       status
FROM audit.log_ingestao
WHERE execucao_id = (SELECT TOP 1 execucao_id FROM audit.log_ingestao ORDER BY inicio_em DESC)
ORDER BY inicio_em

-- execution history
SELECT execucao_id,
       MIN(inicio_em) AS started_at,
       MAX(fim_em)    AS finished_at,
       SUM(linhas)    AS total_rows,
       COUNT(*)       AS tables,
       SUM(CASE WHEN status = 'erro' THEN 1 ELSE 0 END) AS errors
FROM audit.log_ingestao
GROUP BY execucao_id
ORDER BY MIN(inicio_em) DESC

-- tables with errors
SELECT tabela, inicio_em, mensagem_erro
FROM audit.log_ingestao
WHERE status = 'erro'
ORDER BY inicio_em DESC
```

---

## Configured Tables — sales/commercial cycle

Tables from the SAP B1 sales module, covering all five ingestion strategies.

| Table | Description | Strategy | Frequency | HANA Watermark | Local Watermark |
|-------|-------------|----------|-----------|----------------|-----------------|
| OINV | A/R Invoice — header | incremental_append | daily | UpdateDate+UpdateTS | _ingestao_em |
| INV1 | A/R Invoice — lines | incremental_via_cabecalho | daily | via OINV.UpdateDate+UpdateTS | — |
| OCRD | Business Partners | incremental_upsert | daily | UpdateDate+UpdateTS | UpdateDate+UpdateTS |
| OITM | Item master data | incremental_upsert | daily | UpdateDate+UpdateTS | UpdateDate+UpdateTS |
| OUSG | Fiscal usage codes (dimension) | full_reload | daily | — | — |
| OITW | Stock by warehouse | snapshot_diario | daily | — | _ingestao_em |

> **`incremental_append` with composite watermark (OINV):** `UpdateDate+UpdateTS` provides second-level precision — zero duplicates regardless of how many times the pipeline runs per day.
>
> **`incremental_via_cabecalho` (INV1):** line rows have no `UpdateDate` of their own; the pipeline captures the header watermark (OINV) before starting any insert, then deletes and reinserts only the affected rows.
>
> **`incremental_upsert` (OCRD, OITM):** master data — delete by PK + reinsert guarantees exactly one row per CardCode/ItemCode in raw, always reflecting current state.
>
> **`full_reload` (OUSG):** small reference table with no date column — TRUNCATE + full INSERT on every run.
>
> **`snapshot_diario` (OITW):** stock by warehouse has no update column — accumulates daily history via `_ingestao_em`. Never truncate: the accumulated history does not exist in SAP HANA.
