import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage, secretmanager, pubsub_v1
from io import StringIO
import functions_framework

# --- CONFIGURAÇÕES DE AMBIENTE ---
PROJECT_ID = "dev-autopass-bi-001"
BUCKET_NAME = "autopass-datalake-topdesk"
TOPIC_ID = "topdesk-errors-topic"

def get_secrets(secret_id):
    """Busca credenciais no Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def carregar_status(bucket):
    """Lê o arquivo de estado detalhado para controle e dashboard"""
    blob = bucket.blob("tabelas/status_execucao.json")
    if blob.exists():
        try:
            return json.loads(blob.download_as_text())
        except:
            return {}
    return {}

def atualizar_status_no_storage(bucket, t_name, status, registros=0, erro=None):
    """Grava o progresso detalhado no Storage após cada tabela"""
    blob = bucket.blob("tabelas/status_execucao.json")
    
    try:
        status_atual = json.loads(blob.download_as_text()) if blob.exists() else {}
    except:
        status_atual = {}

    status_atual[t_name] = {
        "data": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status": status,
        "registros": registros,
        "erro": str(erro) if erro else None
    }
    
    blob.upload_from_string(
        json.dumps(status_atual, indent=2), 
        content_type='application/json'
    )

def notificar_erro_pubsub(tabela, erro):
    """Dispara o gatilho para a Cloud Function do Teams via Pub/Sub"""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    mensagem = {
        "tabela": tabela,
        "erro": str(erro),
        "data": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    data = json.dumps(mensagem).encode("utf-8")
    publisher.publish(topic_path, data)

@functions_framework.http
def main(request):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    hoje = datetime.now().strftime('%Y-%m-%d')
    
    # 1. Carrega Metadados e Status
    blob_config = bucket.blob("tabelas/tabelas.json")
    config = json.loads(blob_config.download_as_text())
    status_exec = carregar_status(bucket)
    
    # Verifica reset manual via JSON da requisição
    request_json = request.get_json(silent=True)
    reset_process = request_json.get("reset") if request_json else False
    
    # 2. Credenciais
    creds = get_secrets("topdesk")
    user, password = creds.split(":")
    
    timestamp_exec = datetime.now().strftime('%Y%m%d_%H%M%S')
    relatorio_final = []

    # 3. Loop de Processamento (237 Tabelas)
    for table in config['tables']:
        t_name = table['name']
        status_anterior = status_exec.get(t_name, {})
        
        # Pula se já deu sucesso hoje (baseado no novo formato de status)

        # Pega a informação da tabela e garante que seja um dicionário
        info_tabela = status_exec.get(t_name, {})
        if isinstance(info_tabela, str): info_tabela = {}

        # Pega apenas a DATA (10 primeiros caracteres) para comparar com 'hoje'
        data_sucesso = info_tabela.get('data', '')[:10] 

        # Agora a comparação ignora a hora e foca só no dia
        if not reset_process and data_sucesso == hoje and info_tabela.get('status') == 'sucesso':
            relatorio_final.append(f"⏭️ {t_name}: Já processado hoje ({info_tabela.get('data')}).")
            continue
        if not reset_process and status_anterior.get('data') == hoje and status_anterior.get('status') == 'sucesso':
            relatorio_final.append(f"⏭️ {t_name}: Já processado hoje.")
            continue

        try:
            endpoint = table['endpoint']
            date_field = table.get('date_field')
            is_incremental = table.get('incremental', False) # Pega o valor do JSON

            # --- LÓGICA DE FILTRO SUPORTANDO LISTA OU STRING ---
            filtro = ""
            if is_incremental and date_field:
                # Mantemos o formato de data que você confirmou que funciona (com Z e sem aspas)
                date_limit = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT00:00:00Z')
                
                # Se for lista, montamos o 'or'. Se for string, tratamos como um item só.
                campos = date_field if isinstance(date_field, list) else [date_field]
                
                # Monta as cláusulas: campo1 ge dataZ or campo2 ge dataZ
                clausulas = [f"{c} ge {date_limit}" for c in campos]
                
                # Junta tudo. Se tiver mais de um campo, o 'or' resolve.
                # Nota: Alguns OData preferem parênteses em volta do OR: f"&$filter=({ ' or '.join(clausulas) })"
                # Mas vamos manter o mais simples primeiro:
                filtro = f"&$filter=" + " or ".join(clausulas)
                
                print(f"⏳ {t_name}: Carga Incremental (5 dias) nos campos: {campos}")
            else:
                print(f"🌍 {t_name}: Carga FULL Refresh")
            



            
            
            url = f"https://autopass.topdesk.net/services/reporting/v2{endpoint}?$format=json&dateFormat=iso8601{filtro}"
            
            response = requests.get(url, auth=(user, password), timeout=120)
            response.raise_for_status()
            
            data = response.json().get('value', [])
            
            if data:
                df = pd.DataFrame(data)
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, sep=';', encoding='utf-8')
                
                path = f"landing/{t_name.lower()}/2026/{t_name.lower()}_{timestamp_exec}.csv"
                bucket.blob(path).upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
                
                atualizar_status_no_storage(bucket, t_name, "sucesso", registros=len(data))
                relatorio_final.append(f"✅ {t_name}: {len(data)} registros.")
            else:
                atualizar_status_no_storage(bucket, t_name, "sucesso", registros=0)
                relatorio_final.append(f"⚠️ {t_name}: Sem novos dados.")

        except Exception as e:
            # Em caso de falha, registra no JSON para o dashboard e envia ao Teams
            print(f"Erro em {t_name}: {str(e)}")
            atualizar_status_no_storage(bucket, t_name, "falha", erro=e)
            notificar_erro_pubsub(t_name, e)
            relatorio_final.append(f"❌ {t_name}: FALHA enviada ao Teams.")
            continue

    return ("\n".join(relatorio_final), 200)