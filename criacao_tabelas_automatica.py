import json
import re
from google.cloud import storage
from google.cloud import bigquery

# =========================================================
# CONFIG
# =========================================================
PROJECT_ID = "dev-autopass-bi-001"
DATASET_ID = "day"
BUCKET_NAME = "autopass-datalake-topdesk"
JSON_PATH = "tabelas/tabelas.json"
LANDING = "landing"
YEAR = "2026"

# =========================================================
# CLASSIFICAÇÃO DE TABELAS
# =========================================================
FACT_TABLES = {
    "Incidents",
    "IncidentDetails",
    "Changes",
    "ChangeDetails",
    "Problems",
    # futuras que também tendem a ser fato:
    # "ProblemDetails",
    # "IncidentSnapshots"
}

DIMENSION_TABLES = {
    "IncidentProcessingStatuses",
    "Categories",
    "Subcategories",
    "AssetEQATMList",
    "AssetEQPOSQRCodeList",
    "Branches",
    "Departments",
    "OperatorGroups",
    "Operators",
    "ChangeProcessingStatuses",
    "Persons",
    "ChangePriorities",
    "ChangeImpacts",
    "ChangeTypes",
}

# =========================================================
# HELPERS
# =========================================================
def normalize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", name).lower()


def get_table_type(table: dict) -> str:
    table_name = table["name"]

    if table_name in FACT_TABLES:
        return "trs"

    if table_name in DIMENSION_TABLES:
        return "mrd"

    raise ValueError(
        f"Tabela '{table_name}' não classificada. "
        f"Adicione em FACT_TABLES ou DIMENSION_TABLES."
    )


def build_table_name(table: dict) -> str:
    name = normalize_name(table["name"])
    table_type = get_table_type(table)
    return f"tb_day_{table_type}_tpd001_{name}"


def build_gcs_uri(table: dict) -> str:
    name = normalize_name(table["name"])
    return f"gs://{BUCKET_NAME}/{LANDING}/{name}/{YEAR}/{name}_*.csv.gz"


def build_schema(columns: list) -> str:
    formatted_cols = []
    for col in columns:
        col = col.strip()
        formatted_cols.append(f"  `{col}` STRING")
    return ",\n".join(formatted_cols)


def build_sql(table: dict) -> str:
    table_name = build_table_name(table)
    uri = build_gcs_uri(table)
    schema = build_schema(table["schema"])

    return f"""
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_ID}.{table_name}`
(
{schema}
)
OPTIONS (
  format = 'CSV',
  uris = ['{uri}'],
  compression = 'GZIP',
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_quoted_newlines = TRUE
);
"""


# =========================================================
# LOAD JSON
# =========================================================
def load_json():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(JSON_PATH)

    content = blob.download_as_text(encoding="utf-8")
    return json.loads(content)


# =========================================================
# MAIN
# =========================================================
def main():
    config = load_json()

    if "tables" not in config or not isinstance(config["tables"], list):
        raise ValueError("JSON inválido: chave 'tables' ausente ou fora do padrão.")

    bq = bigquery.Client(project=PROJECT_ID)

    total = len(config["tables"])
    print(f"[INFO] Total de tabelas: {total}")

    success = []
    failed = []

    for table in config["tables"]:
        name = table["name"]

        try:
            table_type = get_table_type(table)
            final_table_name = build_table_name(table)

            print(f"\n[START] {name}")
            print(f"[INFO] Tipo: {table_type}")
            print(f"[INFO] Destino: {PROJECT_ID}.{DATASET_ID}.{final_table_name}")

            sql = build_sql(table)
            job = bq.query(sql)
            job.result()

            print(f"[OK] {name} criada com sucesso")
            success.append({
                "source_name": name,
                "table_type": table_type,
                "target_name": final_table_name
            })

        except Exception as e:
            print(f"[ERRO] {name}")
            print(str(e))
            failed.append({
                "source_name": name,
                "error": str(e)
            })

    print("\n================ RESUMO ================")
    print(f"[INFO] Sucesso: {len(success)}")
    print(f"[INFO] Falhas : {len(failed)}")

    if success:
        print("\n[TABELAS CRIADAS]")
        for item in success:
            print(f"- {item['source_name']} -> {item['target_name']} ({item['table_type']})")

    if failed:
        print("\n[TABELAS COM ERRO]")
        for item in failed:
            print(f"- {item['source_name']} -> {item['error']}")

    print("\n🔥 FINALIZADO")


if __name__ == "__main__":
    main()