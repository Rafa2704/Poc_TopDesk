import base64
import csv
import gzip
import json
import requests
import pandas as pd
import functions_framework
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from google.cloud import storage, secretmanager, pubsub_v1
from io import StringIO

# --- CONFIGURAÇÕES DE AMBIENTE ---
PROJECT_ID = "dev-autopass-bi-001"
BUCKET_NAME = "autopass-datalake-topdesk"
TOPIC_ID    = "topdesk-errors-topic"

TOPDESK_BASE_ODATA  = "https://autopass.topdesk.net/services/reporting/v2"
TOPDESK_BASE_API    = "https://autopass.topdesk.net"

# ─────────────────────────────────────────────
# HELPERS GERAIS
# ─────────────────────────────────────────────

def get_secrets(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name   = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")


def carregar_status(bucket) -> dict:
    blob = bucket.blob("tabelas/status_execucao.json")
    if blob.exists():
        try:
            return json.loads(blob.download_as_text())
        except Exception:
            return {}
    return {}


def atualizar_status_no_storage(bucket, t_name: str, status: str, registros: int = 0, erro=None):
    blob = bucket.blob("tabelas/status_execucao.json")
    try:
        status_atual = json.loads(blob.download_as_text()) if blob.exists() else {}
    except Exception:
        status_atual = {}

    status_atual[t_name] = {
        "data":      datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status":    status,
        "registros": registros,
        "erro":      str(erro) if erro else None,
    }
    blob.upload_from_string(json.dumps(status_atual, indent=2), content_type='application/json')


def notificar_erro_pubsub(tabela: str, erro):
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    mensagem   = {"tabela": tabela, "erro": str(erro), "data": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    publisher.publish(topic_path, json.dumps(mensagem).encode("utf-8"))


def aplicar_schema_df(df: pd.DataFrame, schema_cols: list) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")].copy()
    for c in schema_cols:
        if c not in df.columns:
            df[c] = None
    return df[schema_cols]


def salvar_gcs(bucket, df: pd.DataFrame, t_name: str, timestamp_exec: str):
    """Serializa DataFrame como CSV gzip e sobe pro GCS."""
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer, index=False, sep=";", encoding="utf-8",
        na_rep="", quotechar='"', quoting=csv.QUOTE_MINIMAL, escapechar="\\"
    )
    csv_gz = gzip.compress(csv_buffer.getvalue().encode("utf-8"))
    ano    = datetime.now().year
    path   = f"landing/{t_name.lower()}/{ano}/{t_name.lower()}_{timestamp_exec}.csv.gz"
    bucket.blob(path).upload_from_string(csv_gz, content_type="application/gzip")
    return path


# ─────────────────────────────────────────────
# ROTA A — OData /services/reporting/v2
# ─────────────────────────────────────────────

def processar_odata(table: dict, user: str, password: str) -> list:
    """
    Endpoint OData padrão com $filter e $format=json.
    Retorna lista de dicts prontos para DataFrame.
    """
    endpoint    = table["endpoint"]
    date_field  = table.get("date_field")
    incremental = table.get("incremental", False)

    filtro = ""
    if incremental and date_field:
        date_limit = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT00:00:00Z')
        campos     = date_field if isinstance(date_field, list) else [date_field]
        clausulas  = [f"{c} ge {date_limit}" for c in campos]
        filtro     = "&$filter=" + " or ".join(clausulas)
        print(f"  ⏳ Incremental (5 dias): {campos}")
    else:
        print(f"  🌍 Full Refresh")

    url      = f"{TOPDESK_BASE_ODATA}{endpoint}?$format=json&dateFormat=iso8601{filtro}"
    response = requests.get(url, auth=(user, password), timeout=120)
    response.raise_for_status()
    return response.json().get("value", [])


# ─────────────────────────────────────────────
# ROTA B — /tas/api com cursor + janelas (operatorChanges e similares)
# ─────────────────────────────────────────────

def _basic_header(user: str, password: str) -> dict:
    encoded = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Accept": "application/json"}


def _gerar_janelas(ano: int) -> list:
    janelas, inicio = [], datetime(ano, 1, 1)
    fim_ano = datetime(ano, 12, 31, 23, 59, 59)
    while inicio <= fim_ano:
        fim = min(inicio + timedelta(days=6, hours=23, minutes=59, seconds=59), fim_ano)
        janelas.append((inicio, fim))
        inicio = fim + timedelta(seconds=1)
    return janelas


def _fetch_ids_janela(janela: tuple, endpoint_url: str, headers: dict, page_size: int = 100) -> list:
    inicio, fim = janela
    start_str   = inicio.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str     = fim.strftime("%Y-%m-%dT%H:%M:%SZ")

    ids, url, params, pagina = [], endpoint_url, {
        "pageSize": page_size,
        "start":    0,
        "query":    f"creationDate>={start_str};creationDate<={end_str}",
    }, 0

    while True:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            items   = payload.get("results", [])

            for item in items:
                cid = item.get("id") if isinstance(item, dict) else item
                if cid:
                    ids.append(cid)

            next_url = payload.get("next")
            if not next_url or not items:
                break

            url, params, pagina = next_url, None, pagina + 1

        except Exception as e:
            print(f"  ❌ Janela {inicio.strftime('%d/%b')} pág {pagina}: {e}")
            break

    return ids


def _fetch_detalhe(change_id: str, endpoint_url: str, headers: dict, schema_cols: list) -> dict | None:
    try:
        resp = requests.get(f"{endpoint_url}/{change_id}", headers=headers, timeout=30)
        if resp.status_code in [200, 206]:
            p = resp.json()
            # Monta dict respeitando exatamente o schema declarado no tabelas.json
            row = {
                "change_id":            change_id,
                "number":               p.get("number"),
                "creationDate":         p.get("creationDate"),
                "lastModificationDate": p.get("lastModificationDate"),
                "briefDescription":     p.get("briefDescription"),
                "status_name":          (p.get("status")      or {}).get("name"),
                "requester_name":       (p.get("requester")   or {}).get("name"),
                "coordinator_name":     (p.get("coordinator") or {}).get("name"),
                "category_name":        (p.get("category")    or {}).get("name"),
                "subcategory_name":     (p.get("subcategory") or {}).get("name"),
                "optionalFields1_json": json.dumps(p.get("optionalFields1"), ensure_ascii=False),
                "raw_json":             json.dumps(p, ensure_ascii=False),
            }
            # Retorna apenas colunas declaradas no schema (flexível para subsets)
            return {k: v for k, v in row.items() if k in schema_cols}
    except Exception as e:
        print(f"  ❌ Detalhe {change_id}: {e}")
    return None


def processar_tas_api(table: dict, user: str, password: str) -> list:
    """
    Endpoint /tas/api com paginação por cursor 'next' e janelas semanais.
    Retorna lista de dicts prontos para DataFrame.
    """
    endpoint_url  = f"{TOPDESK_BASE_API}{table['endpoint']}"
    headers       = _basic_header(user, password)
    schema_cols   = table.get("schema", [])
    ano           = table.get("ano", datetime.now().year)
    janelas       = _gerar_janelas(ano)
    page_size     = table.get("page_size", 1000)
    w_janelas     = table.get("max_workers_janelas", 6)
    w_detalhes    = table.get("max_workers_detalhes", 20)

    print(f"  🗓️  Full Refresh {ano} — {len(janelas)} janelas semanais")

    # FASE 1: coletar IDs
    todos_ids = set()
    with ThreadPoolExecutor(max_workers=w_janelas) as pool:
        futures = {pool.submit(_fetch_ids_janela, j, endpoint_url, headers, page_size): j for j in janelas}
        for fut in as_completed(futures):
            ids = fut.result()
            todos_ids.update(ids)

    print(f"  📋 {len(todos_ids)} IDs únicos encontrados — buscando detalhes...")

    # FASE 2: buscar detalhes
    registros = []
    with ThreadPoolExecutor(max_workers=w_detalhes) as pool:
        futures = {pool.submit(_fetch_detalhe, cid, endpoint_url, headers, schema_cols): cid for cid in todos_ids}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res:
                registros.append(res)
            if i % 200 == 0 or i == len(todos_ids):
                print(f"  🚀 {i}/{len(todos_ids)} detalhes | {len(registros)} válidos")

    return registros


# ─────────────────────────────────────────────
# ROTEADOR — detecta o tipo pelo endpoint
# ─────────────────────────────────────────────

def detectar_rota(table: dict) -> str:
    """
    Regra simples: endpoint começa com /tas/api → usa cursor.
    Qualquer outro → OData padrão.
    Pode ser sobrescrito com "api_type": "tas_api" | "odata" no tabelas.json.
    """
    if "api_type" in table:
        return table["api_type"]
    if table.get("endpoint", "").startswith("/tas/api"):
        return "tas_api"
    return "odata"


# ─────────────────────────────────────────────
# ENTRY POINT — Cloud Run / Cloud Functions
# ─────────────────────────────────────────────

@functions_framework.http
def main(request):
    storage_client  = storage.Client()
    bucket          = storage_client.bucket(BUCKET_NAME)
    hoje            = datetime.now().strftime('%Y-%m-%d')
    timestamp_exec  = datetime.now().strftime('%Y%m%d_%H%M%S')

    blob_config = bucket.blob("tabelas/tabelas.json")
    config      = json.loads(blob_config.download_as_text())
    status_exec = carregar_status(bucket)

    request_json  = request.get_json(silent=True)
    reset_process = request_json.get("reset") if request_json else False

    # Permite rodar só uma tabela específica: {"table": "operatorChanges"}
    filtro_tabela = request_json.get("table") if request_json else None

    creds          = get_secrets("topdesk")
    user, password = creds.split(":")

    relatorio_final = []

    for table in config["tables"]:
        t_name = table["name"]

        # Filtro por tabela específica (opcional na request)
        if filtro_tabela and t_name != filtro_tabela:
            continue

        # Verifica se já processou hoje
        info     = status_exec.get(t_name, {})
        if isinstance(info, str):
            info = {}
        if not reset_process and info.get("data", "")[:10] == hoje and info.get("status") == "sucesso":
            relatorio_final.append(f"⏭️  {t_name}: Já processado hoje ({info.get('data')}).")
            continue

        print(f"\n{'='*50}\n▶️  {t_name} [{detectar_rota(table)}]\n{'='*50}")

        try:
            rota = detectar_rota(table)

            if rota == "tas_api":
                dados = processar_tas_api(table, user, password)
            else:
                dados = processar_odata(table, user, password)

            if not dados:
                atualizar_status_no_storage(bucket, t_name, "sucesso", registros=0)
                relatorio_final.append(f"⚠️  {t_name}: Sem dados.")
                continue

            df          = pd.DataFrame(dados)
            schema_cols = table.get("schema")
            if schema_cols:
                df = aplicar_schema_df(df, schema_cols)

            path = salvar_gcs(bucket, df, t_name, timestamp_exec)
            atualizar_status_no_storage(bucket, t_name, "sucesso", registros=len(df))
            relatorio_final.append(f"✅ {t_name}: {len(df)} registros → {path}")

        except Exception as e:
            print(f"❌ Erro em {t_name}: {e}")
            atualizar_status_no_storage(bucket, t_name, "falha", erro=e)
            notificar_erro_pubsub(t_name, e)
            relatorio_final.append(f"❌ {t_name}: FALHA — {e}")

    return ("\n".join(relatorio_final), 200)