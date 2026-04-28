import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

HANA_HOST = os.getenv("HANA_HOST")
HANA_PORT = os.getenv("HANA_PORT")
HANA_USER = os.getenv("HANA_USER")
HANA_PASSWORD = os.getenv("HANA_PASSWORD")
HANA_SCHEMA = os.getenv("HANA_SCHEMA")

SQLSERVER_SERVER = os.getenv("SQLSERVER_SERVER")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE")
SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD")

CHUNK_SIZE = 10000
HANA_TIMEOUT = 30000
QUERY_TIMEOUT = 300000
RAW_SCHEMA = "raw"


def carregar_tabelas() -> dict[str, dict]:
    caminho = Path(__file__).parent.parent / "tables.yaml"
    with caminho.open(encoding="utf-8") as f:
        dados = yaml.safe_load(f)
    return {
        tabela: {
            "tipo": cfg.get("tipo", "tabela"),
            "estrategia": cfg.get("estrategia", "full_reload"),
            "chave_primaria": cfg.get("chave_primaria") or [],
            "coluna_watermark": cfg.get("coluna_watermark"),
            "coluna_watermark_local": cfg.get("coluna_watermark_local", cfg.get("coluna_watermark")),
            "tabela_cabecalho": cfg.get("tabela_cabecalho"),
            "coluna_watermark_cabecalho": cfg.get("coluna_watermark_cabecalho"),
            "colunas": cfg.get("colunas") or [],
            "carga_inicial": cfg.get("carga_inicial") or {},
            "frequencia": cfg.get("frequencia", "diaria"),
        }
        for tabela, cfg in (dados or {}).items()
    }
