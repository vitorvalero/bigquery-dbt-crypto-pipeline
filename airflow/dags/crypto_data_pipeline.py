from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from tasks.fetch_klines import fetch_and_save_klines
from tasks.load_parquets_to_bq import process_and_load_parquets
from docker.types import Mount
import yaml
import os

# Define a variável de ambiente para credenciais do Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS"
)

# Carrega o arquivo de configuração YAML
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "./config/config.yaml")
)
try:
    with open(CONFIG_PATH, "r") as config_file:
        config = yaml.safe_load(config_file)
except Exception as e:
    raise RuntimeError(f"Erro ao carregar {CONFIG_PATH}: {e}")

# Variáveis do Google Cloud
BUCKET_NAME = config["gcp"]["bucket_name"]
GCP_PROJECT = config["gcp"]["project"]
DATASET_ID = config["gcp"]["dataset_id"]
BIGQUERY_CONN_ID = config["gcp"]["bigquery_conn_id"]
BIGQUERY_LOCATION = config["gcp"]["bigquery_location"]
DATASETS = config["gcp"]["datasets"]

# Variáveis da API Binance
BINANCE_BASE_URL = config["binance"]["base_url"]
CRYPTOS = config["binance"]["cryptos"]
INTERVAL = config["binance"]["interval"]
LIMIT = config["binance"]["limit"]
DEFAULT_START_TIME = config["binance"]["default_start_time"]

# Variáveis para execução do dbt no Docker
DBT_IMAGE = config["dbt"]["image"]
DOCKER_NETWORK = config["dbt"]["docker_network"]
DBT_PROJECT_DIR = config["dbt"]["project_dir"]
DBT_PROFILES_DIR = config["dbt"]["profiles_dir"]
GCLOUD_CREDENTIALS_DIR = config["dbt"]["gcloud_credentials_dir"]

# Definição de parâmetros padrão para o DAG do Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="crypto_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["binance", "gcs", "bigquery", "parquet", "dbt"],
)
def crypto_data_pipeline():

    # Criação de tasks para extração de dados de múltiplas criptomoedas
    extracted_data_tasks = {
        crypto: fetch_and_save_klines.override(
            task_id=f"fetch_klines_{crypto.lower()}"
        )(
            symbol=crypto,
            bucket_name=BUCKET_NAME,
            interval=INTERVAL,
            limit=LIMIT,
            default_start_time=DEFAULT_START_TIME,
            base_url=BINANCE_BASE_URL,
        )
        for crypto in CRYPTOS
    }

    # Task para carregar os arquivos Parquet do GCS para o BigQuery
    load_to_bq_task = process_and_load_parquets(
        bucket_name=BUCKET_NAME,
        dataset_id=DATASET_ID,
        gcp_project=GCP_PROJECT,
        conn_id=BIGQUERY_CONN_ID,
        datasets=DATASETS,
        location=BIGQUERY_LOCATION,
    )

    # Task para rodar transformações dbt usando DockerOperator
    dbt_run_task = DockerOperator(
        task_id="run_dbt_transformations",
        image=DBT_IMAGE,
        auto_remove="force",
        command=["run", "--project-dir", "/usr/app"],
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        mounts=[
            Mount(source=DBT_PROJECT_DIR, target="/usr/app", type="bind"),
            Mount(source=DBT_PROFILES_DIR, target="/root/.dbt", type="bind"),
            Mount(source=GCLOUD_CREDENTIALS_DIR, target="/root/.gcloud", type="bind"),
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
        ],
        working_dir="/usr/app",
        do_xcom_push=True,
    )

    # Task para validar os dados transformados no dbt com dbt test
    dbt_test_task = DockerOperator(
        task_id="validate_data_from_tables",
        image=DBT_IMAGE,
        auto_remove="force",
        command=["test", "--project-dir", "/usr/app"],
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        mounts=[
            Mount(source=DBT_PROJECT_DIR, target="/usr/app", type="bind"),
            Mount(source=DBT_PROFILES_DIR, target="/root/.dbt", type="bind"),
            Mount(source=GCLOUD_CREDENTIALS_DIR, target="/root/.gcloud", type="bind"),
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
        ],
        working_dir="/usr/app",
        do_xcom_push=True,
    )

    # Definição da ordem de execução das tasks
    (
        list(extracted_data_tasks.values())
        >> load_to_bq_task
        >> dbt_run_task
        >> dbt_test_task
    )


crypto_data_pipeline()
