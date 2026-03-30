import base64
import csv
import gzip
import json
import requests
import pandas as pd
import functions_framework

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from google.cloud import storage, secretmanager, pubsub_v1
from io import StringIO

# ==========================================================
# CONFIGURAÇÕES DE AMBIENTE
# ==========================================================
PROJECT_ID = "dev-autopass-bi-001"
BUCKET_NAME = "autopass-datalake-topdesk"
TOPIC_ID = "topdesk-errors-topic"

TOPDESK_BASE_ODATA = "https://autopass.topdesk.net/services/reporting/v2"
TOPDESK_BASE_API = "https://autopass.topdesk.net"


# ==========================================================
# HELPERS GERAIS
# ==========================================================
def get_secrets(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")


def carregar_status(bucket) -> dict:
    blob = bucket.blob("tabelas/status_execucao_api_rest.json")
    if blob.exists():
        try:
            return json.loads(blob.download_as_text())
        except Exception:
            return {}
    return {}


def atualizar_status_no_storage(bucket, t_name: str, status: str, registros: int = 0, erro=None):
    blob = bucket.blob("tabelas/status_execucao_api_rest.json")

    try:
        status_atual = json.loads(blob.download_as_text()) if blob.exists() else {}
    except Exception:
        status_atual = {}

    status_atual[t_name] = {
        "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "registros": registros,
        "erro": str(erro) if erro else None,
    }

    blob.upload_from_string(
        json.dumps(status_atual, indent=2, ensure_ascii=False),
        content_type="application/json"
    )


def notificar_erro_pubsub(tabela: str, erro):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    mensagem = {
        "tabela": tabela,
        "erro": str(erro),
        "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    publisher.publish(topic_path, json.dumps(mensagem).encode("utf-8"))


def aplicar_schema_df(df: pd.DataFrame, schema_cols: list) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")].copy()

    for c in schema_cols:
        if c not in df.columns:
            df[c] = None

    return df[schema_cols]


def salvar_gcs(bucket, df: pd.DataFrame, t_name: str, timestamp_exec: str):
    csv_buffer = StringIO()
    df.to_csv(
        csv_buffer,
        index=False,
        sep=";",
        encoding="utf-8",
        na_rep="",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\"
    )

    csv_gz = gzip.compress(csv_buffer.getvalue().encode("utf-8"))
    ano = datetime.now().year
    path = f"landing/{t_name.lower()}/{ano}/{t_name.lower()}_{timestamp_exec}.csv.gz"

    bucket.blob(path).upload_from_string(csv_gz, content_type="application/gzip")
    return path


# ==========================================================
# ROTA A — ODATA
# ==========================================================
def processar_odata(table: dict, user: str, password: str) -> list:
    endpoint = table["endpoint"]
    date_field = table.get("date_field")
    incremental = table.get("incremental", False)
    days_back = table.get("days_back", 5)

    filtro = ""

    if incremental and date_field:
        date_limit = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
        campos = date_field if isinstance(date_field, list) else [date_field]
        clausulas = [f"{c} ge {date_limit}" for c in campos]
        filtro = "&$filter=" + " or ".join(clausulas)
        print(f"  ⚡ OData incremental ({days_back} dias): {campos}")
    else:
        print("  🌍 OData full refresh")

    url = f"{TOPDESK_BASE_ODATA}{endpoint}?$format=json&dateFormat=iso8601{filtro}"
    response = requests.get(url, auth=(user, password), timeout=120)
    response.raise_for_status()

    return response.json().get("value", [])


# ==========================================================
# ROTA B — /tas/api
# ==========================================================
def _basic_header(user: str, password: str) -> dict:
    encoded = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Accept": "application/json"
    }


def _gerar_janelas(ano: int) -> list:
    janelas = []
    inicio = datetime(ano, 1, 1)
    fim_ano = datetime(ano, 12, 31, 23, 59, 59)

    while inicio <= fim_ano:
        fim = min(inicio + timedelta(days=6, hours=23, minutes=59, seconds=59), fim_ano)
        janelas.append((inicio, fim))
        inicio = fim + timedelta(seconds=1)

    return janelas


def _fetch_ids_janela(janela: tuple, endpoint_url: str, headers: dict, page_size: int = 1000) -> list:
    inicio, fim = janela
    start_str = inicio.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = fim.strftime("%Y-%m-%dT%H:%M:%SZ")

    ids = []
    url = endpoint_url
    params = {
        "pageSize": page_size,
        "start": 0,
        "query": f"creationDate>={start_str};creationDate<={end_str}",
    }
    pagina = 0

    while True:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()

            payload = resp.json()
            items = payload.get("results", [])

            for item in items:
                cid = item.get("id") if isinstance(item, dict) else item
                if cid:
                    ids.append(cid)

            next_url = payload.get("next")
            if not next_url or not items:
                break

            url = next_url
            params = None
            pagina += 1

        except Exception as e:
            print(f"  ❌ Janela {inicio.strftime('%d/%b')} pág {pagina}: {e}")
            break

    return ids


def _fetch_ids_incremental_por_campo(
    endpoint_url: str,
    headers: dict,
    campo: str,
    days_back: int,
    page_size: int = 1000
) -> set:
    agora = datetime.now(timezone.utc)
    inicio = (agora - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    fim = agora.strftime("%Y-%m-%dT%H:%M:%SZ")

    query = f"{campo}>={inicio};{campo}<={fim}"
    print(f"  ⚡ Incremental campo={campo} | days_back={days_back}")
    print(f"  🔎 Query: {query}")

    ids = set()
    url = endpoint_url
    params = {
        "pageSize": page_size,
        "start": 0,
        "query": query
    }
    pagina = 0

    while True:
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()

            payload = resp.json()
            items = payload.get("results", [])

            print(f"    📄 {campo} | página {pagina} | itens: {len(items)}")

            for item in items:
                cid = item.get("id") if isinstance(item, dict) else item
                if cid:
                    ids.add(cid)

            next_url = payload.get("next")
            if not next_url or not items:
                break

            url = next_url
            params = None
            pagina += 1

        except Exception as e:
            print(f"  ❌ Incremental campo={campo} pág {pagina}: {e}")
            break

    return ids


def _fetch_ids_incremental(
    endpoint_url: str,
    headers: dict,
    date_fields,
    days_back: int,
    page_size: int = 1000
) -> set:
    campos = date_fields if isinstance(date_fields, list) else [date_fields]
    todos_ids = set()

    for campo in campos:
        ids_campo = _fetch_ids_incremental_por_campo(
            endpoint_url=endpoint_url,
            headers=headers,
            campo=campo,
            days_back=days_back,
            page_size=page_size
        )
        todos_ids.update(ids_campo)

    print(f"  📋 IDs únicos encontrados no incremental: {len(todos_ids)}")
    return todos_ids


def _fetch_detalhe(change_id: str, endpoint_url: str, headers: dict, schema_cols: list) -> dict | None:
    try:
        resp = requests.get(f"{endpoint_url}/{change_id}", headers=headers, timeout=30)

        if resp.status_code in [200, 206]:
            p = resp.json()

            row = {
                "change_id": change_id,
                "number": p.get("number"),
                "creationDate": p.get("creationDate"),
                "lastModificationDate": p.get("lastModificationDate"),
                "briefDescription": p.get("briefDescription"),
                "status_name": (p.get("status") or {}).get("name"),
                "requester_name": (p.get("requester") or {}).get("name"),
                "coordinator_name": (p.get("coordinator") or {}).get("name"),
                "category_name": (p.get("category") or {}).get("name"),
                "subcategory_name": (p.get("subcategory") or {}).get("name"),
                "optionalFields1_json": json.dumps(p.get("optionalFields1"), ensure_ascii=False),
                "raw_json": json.dumps(p, ensure_ascii=False),
            }

            return {k: v for k, v in row.items() if k in schema_cols}

    except Exception as e:
        print(f"  ❌ Detalhe {change_id}: {e}")

    return None


def processar_tas_api(table: dict, user: str, password: str) -> list:
    endpoint_url = f"{TOPDESK_BASE_API}{table['endpoint']}"
    headers = _basic_header(user, password)
    schema_cols = table.get("schema", [])

    ano = table.get("ano", datetime.now().year)
    page_size = table.get("page_size", 1000)
    w_janelas = table.get("max_workers_janelas", 6)
    w_detalhes = table.get("max_workers_detalhes", 20)

    incremental = table.get("incremental", False)
    date_fields = table.get("date_field")
    days_back = table.get("days_back", 5)

    # ======================================================
    # FASE 1 - COLETAR IDS
    # ======================================================
    if incremental and date_fields:
        print(f"  ⚡ Modo INCREMENTAL habilitado")
        todos_ids = _fetch_ids_incremental(
            endpoint_url=endpoint_url,
            headers=headers,
            date_fields=date_fields,
            days_back=days_back,
            page_size=page_size
        )
    else:
        janelas = _gerar_janelas(ano)
        print(f"  🌍 Modo FULL | ano={ano} | janelas={len(janelas)}")

        todos_ids = set()
        with ThreadPoolExecutor(max_workers=w_janelas) as pool:
            futures = {
                pool.submit(_fetch_ids_janela, j, endpoint_url, headers, page_size): j
                for j in janelas
            }

            for fut in as_completed(futures):
                ids = fut.result()
                todos_ids.update(ids)

        print(f"  📋 IDs únicos encontrados no full: {len(todos_ids)}")

    if not todos_ids:
        print("  ⚠️ Nenhum ID encontrado para processamento.")
        return []

    # ======================================================
    # FASE 2 - BUSCAR DETALHES
    # ======================================================
    print(f"  🚀 Buscando detalhes de {len(todos_ids)} IDs...")

    registros = []
    with ThreadPoolExecutor(max_workers=w_detalhes) as pool:
        futures = {
            pool.submit(_fetch_detalhe, cid, endpoint_url, headers, schema_cols): cid
            for cid in todos_ids
        }

        total = len(todos_ids)
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            if res:
                registros.append(res)

            if i % 200 == 0 or i == total:
                print(f"  🚀 {i}/{total} detalhes | {len(registros)} válidos")

    return registros


# ==========================================================
# ROTEADOR
# ==========================================================
def detectar_rota(table: dict) -> str:
    if "api_type" in table:
        return table["api_type"]

    if table.get("endpoint", "").startswith("/tas/api"):
        return "tas_api"

    return "odata"


# ==========================================================
# ENTRY POINT - CLOUD RUN
# ==========================================================
@functions_framework.http
def main(request):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    hoje = datetime.now().strftime("%Y-%m-%d")
    timestamp_exec = datetime.now().strftime("%Y%m%d_%H%M%S")

    blob_config = bucket.blob("tabelas/tabelas_restapi.json")
    config = json.loads(blob_config.download_as_text())

    status_exec = carregar_status(bucket)

    request_json = request.get_json(silent=True) or {}
    reset_process = request_json.get("reset", False)
    filtro_tabela = request_json.get("table")

    creds = get_secrets("topdesk")
    user, password = creds.split(":", 1)

    relatorio_final = []

    for table in config["tables"]:
        t_name = table["name"]

        if filtro_tabela and t_name != filtro_tabela:
            continue

        info = status_exec.get(t_name, {})
        if isinstance(info, str):
            info = {}

        if not reset_process and info.get("data", "")[:10] == hoje and info.get("status") == "sucesso":
            relatorio_final.append(f"⏭️ {t_name}: Já processado hoje ({info.get('data')}).")
            continue

        print(f"\n{'=' * 60}")
        print(f"▶️ Processando tabela: {t_name} | rota={detectar_rota(table)}")
        print(f"{'=' * 60}")

        try:
            rota = detectar_rota(table)

            if rota == "tas_api":
                dados = processar_tas_api(table, user, password)
            else:
                dados = processar_odata(table, user, password)

            if not dados:
                atualizar_status_no_storage(bucket, t_name, "sucesso", registros=0)
                relatorio_final.append(f"⚠️ {t_name}: Sem dados.")
                continue

            df = pd.DataFrame(dados)

            schema_cols = table.get("schema")
            if schema_cols:
                df = aplicar_schema_df(df, schema_cols)

            path = salvar_gcs(bucket, df, t_name, timestamp_exec)

            atualizar_status_no_storage(
                bucket=bucket,
                t_name=t_name,
                status="sucesso",
                registros=len(df)
            )

            relatorio_final.append(f"✅ {t_name}: {len(df)} registros → {path}")

        except Exception as e:
            print(f"❌ Erro em {t_name}: {e}")

            atualizar_status_no_storage(
                bucket=bucket,
                t_name=t_name,
                status="falha",
                erro=e
            )

            notificar_erro_pubsub(t_name, e)
            relatorio_final.append(f"❌ {t_name}: FALHA — {e}")

    return ("\n".join(relatorio_final), 200)