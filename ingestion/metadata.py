from typing import Any

from sqlalchemy import text

from ingestion.config import HANA_SCHEMA


def buscar_metadados_tabela(hana_engine, tabela: str, colunas: list[str], tipo: str = "tabela") -> list[dict[str, Any]]:
    match tipo:
        case "view":
            sys_view = "SYS.VIEW_COLUMNS"
            nome_col = "VIEW_NAME"
        case _:
            sys_view = "SYS.TABLE_COLUMNS"
            nome_col = "TABLE_NAME"

    case_order = " ".join(
        f"WHEN COLUMN_NAME = '{col}' THEN {i}"
        for i, col in enumerate(colunas)
    )
    filtro = ", ".join(f"'{c}'" for c in colunas)

    sql = text(
        f"""
        SELECT
            COLUMN_NAME AS "COLUMN_NAME",
            DATA_TYPE_NAME AS "DATA_TYPE_NAME",
            LENGTH AS "LENGTH",
            SCALE AS "SCALE",
            POSITION AS "POSITION"
        FROM {sys_view}
        WHERE SCHEMA_NAME = :schema
          AND {nome_col} = :tabela
          AND COLUMN_NAME IN ({filtro})
        ORDER BY CASE {case_order} ELSE {len(colunas)} END
        """
    )

    with hana_engine.connect() as conn:
        rows = conn.execute(sql, {"schema": HANA_SCHEMA, "tabela": tabela}).mappings().all()

    metadados = []
    for row in rows:
        row_dict = dict(row)
        row_normalizado = {str(k).upper(): v for k, v in row_dict.items()}
        nome = row_normalizado.get("COLUMN_NAME")
        if not nome:
            continue
        metadados.append(
            {
                "COLUMN_NAME": nome,
                "DATA_TYPE_NAME": row_normalizado.get("DATA_TYPE_NAME"),
                "LENGTH": row_normalizado.get("LENGTH"),
                "SCALE": row_normalizado.get("SCALE"),
                "POSITION": row_normalizado.get("POSITION"),
            }
        )

    return metadados


def mapear_tipo_hana_para_sqlserver(coluna: dict[str, Any]) -> str:
    tipo = str(coluna["DATA_TYPE_NAME"]).upper()
    length = coluna.get("LENGTH")
    scale = coluna.get("SCALE")

    match tipo:
        case "NVARCHAR" | "ALPHANUM" | "SHORTTEXT":
            tamanho = int(length or 255)
            return "NVARCHAR(MAX)" if tamanho > 4000 else f"NVARCHAR({max(tamanho, 1)})"

        case "VARCHAR":
            tamanho = int(length or 255)
            return "VARCHAR(MAX)" if tamanho > 8000 else f"VARCHAR({max(tamanho, 1)})"

        case "NCHAR":
            tamanho = int(length or 1)
            return f"NCHAR({min(max(tamanho, 1), 4000)})"

        case "CHAR":
            tamanho = int(length or 1)
            return f"CHAR({min(max(tamanho, 1), 8000)})"

        case "TINYINT":
            return "TINYINT"

        case "SMALLINT":
            return "SMALLINT"

        case "INTEGER" | "INT":
            return "INT"

        case "BIGINT":
            return "BIGINT"

        case "DECIMAL" | "DEC" | "SMALLDECIMAL":
            if scale is None:
                return "DECIMAL(38,10)"
            precisao = min(max(int(length or 38), 1), 38)
            escala = min(max(int(scale), 0), precisao)
            return f"DECIMAL({precisao},{escala})"

        case "DOUBLE":
            return "FLOAT"

        case "REAL":
            return "REAL"

        case "DATE":
            return "DATE"

        case "TIME":
            return "TIME(7)"

        case "TIMESTAMP" | "SECONDDATE":
            return "DATETIME2(7)"

        case "BOOLEAN":
            return "BIT"

        case "CLOB" | "NCLOB" | "TEXT" | "BINTEXT":
            return "NVARCHAR(MAX)"

        case "BLOB" | "VARBINARY" | "BINARY":
            return "VARBINARY(MAX)"

        case _:
            return "NVARCHAR(MAX)"

