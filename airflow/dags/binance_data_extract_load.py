from airflow.decorators import dag
from datetime import datetime, timedelta
from tasks.fetch_klines import fetch_and_save_klines
from tasks.load_parquets_to_bq import process_and_load_parquets
import yaml
import os

# Configura credenciais do Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Carrega configurações do config.yaml com tratamento de erro
CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "./config/config.yaml"))
try:
    with open(CONFIG_PATH, "r") as config_file:
        config = yaml.safe_load(config_file)
except Exception as e:
    raise RuntimeError(f"Erro ao carregar {CONFIG_PATH}: {e}")

# Variáveis da configuração
BUCKET_NAME = config["gcp"]["bucket_name"]
GCP_PROJECT = config["gcp"]["project"]
DATASET_ID = config["gcp"]["dataset_id"]
TABLE_ID = config["gcp"]["table_id"]
BIGQUERY_CONN_ID = config["gcp"]["bigquery_conn_id"]

BINANCE_BASE_URL = config["binance"]["base_url"]
CRYPTOS = config["binance"]["cryptos"]
INTERVAL = config["binance"]["interval"]
LIMIT = config["binance"]["limit"]
DEFAULT_START_TIME = config["binance"]["default_start_time"]

# Configuração dos parâmetros padrão da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="binance_data_extract_load",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["binance", "gcs", "bigquery", "parquet"],
)
def binance_data_pipeline():
    """ DAG que extrai dados da Binance, armazena no GCS e carrega no BigQuery """

    # Extrai os dados da Binance e salva no GCS para cada criptomoeda
    extracted_data_tasks = {
        crypto: fetch_and_save_klines.override(task_id=f"fetch_klines_{crypto.lower()}")(
            symbol=crypto,
            bucket_name=BUCKET_NAME,
            interval=INTERVAL,
            limit=LIMIT,
            default_start_time=DEFAULT_START_TIME,
            base_url=BINANCE_BASE_URL  # Passando a URL da Binance como argumento
        )
        for crypto in CRYPTOS
    }

    # Após todas as extrações, processa os arquivos novos e carrega no BigQuery
    load_to_bq_task = process_and_load_parquets(
        bucket_name=BUCKET_NAME, dataset_id=DATASET_ID, table_id=TABLE_ID, gcp_project=GCP_PROJECT, conn_id=BIGQUERY_CONN_ID
    )

    # Define a dependência: todas as extrações precisam terminar antes de carregar no BQ
    list(extracted_data_tasks.values()) >> load_to_bq_task

binance_data_pipeline()
