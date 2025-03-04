from google.cloud import storage, bigquery
import requests
import re
import time
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import tempfile
import os
import pandas as pd


def generate_gcs_path(symbol, start_time):
    """Gera o caminho e nome do arquivo Parquet no GCS baseado no timestamp exato"""
    dt_utc = pd.to_datetime(start_time, unit='ms', utc=True)
    year, month, day, hour, minute = dt_utc.year, dt_utc.month, dt_utc.day, dt_utc.hour, dt_utc.minute

    # Define a pasta do mês com base no próprio timestamp
    gcs_path = f"binance_klines/{symbol}/{year}/M{month:02d}/binance_klines_{symbol}_{year}-{month:02d}-{day:02d}-{hour:02d}{minute:02d}.parquet"
    
    return gcs_path

def save_dataframe_as_parquet(df):
    """Salva um DataFrame como Parquet em um diretório temporário e retorna o caminho"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_parquet:
        df.to_parquet(temp_parquet.name, index=False)
        return temp_parquet.name

def upload_file_to_gcs(bucket_name, local_file_path, gcs_path):
    """Faz o upload de um arquivo local para o GCS e remove o arquivo local"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    
    blob.upload_from_filename(local_file_path)
    os.remove(local_file_path)

    print(f"✅ Arquivo salvo em {gcs_path}")

def get_last_parquet_from_gcs(symbol, bucket_name):
    """ Retorna o nome do último arquivo Parquet salvo no GCS para o symbol """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=f"binance_klines/{symbol}/"))

    # Filtra apenas arquivos válidos (no novo formato YYYY-MM-DD-HHMM.parquet)
    parquet_files = [
        blob.name for blob in blobs
        if re.search(r'binance_klines_[A-Z]+_\d{4}-\d{2}-\d{2}-\d{4}\.parquet$', blob.name)
    ]

    if not parquet_files:
        return None  # Nenhum arquivo encontrado

    # Ordenação do mais recente para o mais antigo
    parquet_files.sort(reverse=True)

    return parquet_files[0]  # Retorna o arquivo mais recente


def get_last_timestamp(symbol, bucket_name):
    """ Obtém o último timestamp salvo verificando o GCS """
    last_parquet_file = get_last_parquet_from_gcs(symbol, bucket_name)

    if last_parquet_file:
        match = re.search(r'binance_klines_([A-Z]+)_(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})\.parquet', last_parquet_file)
        if match:
            year, month, day, hour, minute = map(int, match.groups()[1:])
            return int(time.mktime((year, month, day, hour, minute, 0, 0, 0, 0)) * 1000)
    return None  # Retorna None se não encontrar nenhum arquivo


def fetch_data(url, max_retries=3):
    """ Faz requisições à API da Binance com retry inteligente """
    retry_delay = 10

    for attempt in range(max_retries):
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()

        print(f"Tentativa {attempt + 1}: Erro {response.status_code} - {response.text}")
        time.sleep(retry_delay)

    raise AirflowFailException("Erro ao obter dados da Binance após múltiplas tentativas.")

def get_parquet_files_from_gcs(bucket_name, prefix="binance_klines/"):
    """ Retorna a lista de arquivos Parquet disponíveis no GCS, sem depender de P1/P2. """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_files = sorted([blob.name for blob in blobs if re.search(r'binance_klines_[A-Z]+_\d{4}-\d{2}-\d{2}-\d{4}\.parquet$', blob.name)])

    return parquet_files

def get_bigquery_client(conn_id):
    """ Retorna um cliente do BigQuery via Airflow Hook. """
    hook = BigQueryHook(gcp_conn_id=conn_id)
    return hook.get_client()

def get_bigquery_job_config():
    """ Retorna a configuração padrão para carga no BigQuery. """
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="open_time_ts"),  # Alterado para `open_time_ts`
        clustering_fields=["symbol"],
        autodetect=True,
    )

def extract_symbol_from_filename(file_path):
    """ Extrai o símbolo do nome do arquivo """
    match = re.search(r'binance_klines_([A-Z]+)_(\d{4})-\d{2}-\d{2}-\d{4}\.parquet', file_path)
    return match.group(1) if match else "UNKNOWN"

def update_bigquery_table(client, gcp_project, dataset_id, table_id, file_path, symbol):
    """ Atualiza a tabela no BigQuery com os campos corrigidos """
    update_query = f"""
        UPDATE `{gcp_project}.{dataset_id}.{table_id}`
        SET 
            open_time_ts = TIMESTAMP_MILLIS(open_time),
            close_time_ts = TIMESTAMP_MILLIS(close_time),
            source_file = "{file_path}",
            symbol = "{symbol}"
        WHERE source_file IS NULL OR source_file = "{file_path}"
    """
    client.query(update_query).result()
