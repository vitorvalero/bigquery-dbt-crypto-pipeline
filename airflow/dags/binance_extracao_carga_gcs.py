from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import time
import os
from google.cloud import storage
from airflow.exceptions import AirflowFailException
from dotenv import load_dotenv
import gcsfs

# Carrega variáveis de ambiente do arquivo .env para tornar a DAG configurável sem alterar código.
load_dotenv()

# Define credenciais do Google Cloud para acesso ao GCS.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Configuração de variáveis principais da DAG, permitindo flexibilidade na definição dos parâmetros.
MOEDAS = os.getenv("MOEDAS", "BTCBRL,ETHBRL,SOLBRL").split(",")
INTERVAL = os.getenv("INTERVAL", "1m")  # Intervalo da API Binance (1m, 5m, 1h, etc.)
LIMIT = int(os.getenv("LIMIT", 720))  # Quantidade máxima de candles retornados por requisição
BUCKET_NAME = os.getenv("BUCKET_NAME")  # Nome do bucket do GCS para armazenar os Parquets
DEFAULT_START_TIME = int(os.getenv("DEFAULT_START_TIME", 1609459200000))  # Timestamp inicial (1º jan 2021)

# Definição de parâmetros padrão para a DAG, seguindo boas práticas do Airflow.
default_args = {
    'owner': os.getenv("AIRFLOW_OWNER", "vitor"),
    'depends_on_past': False,  # Evita que a execução dependa da DAG anterior
    'start_date': datetime.strptime(os.getenv("AIRFLOW_START_DATE", "2025-02-25"), "%Y-%m-%d"),
    'retries': int(os.getenv("AIRFLOW_RETRIES", 3)),  # Número de tentativas antes de falhar
    'retry_delay': timedelta(minutes=int(os.getenv("AIRFLOW_RETRY_DELAY", 5))),  # Tempo de espera entre tentativas
}

# Função de fábrica que cria DAGs dinamicamente para cada moeda configurada.
def dag_factory(symbol):
    @dag(
        dag_id=f"binance_extracao_{symbol.lower()}",
        default_args=default_args,
        description=f"Extrai Klines da Binance para {symbol} e salva como Parquet no GCS",
        schedule_interval="@once",  # Pode ser alterado para execução periódica
        catchup=False,  # Evita execução retroativa automática
        tags=["binance", "gcs", "parquet", symbol]  # Facilita a organização das DAGs no Airflow UI
    )
    def extract_binance_dag():
        
        # Obtém o timestamp do último dado salvo no GCS, evitando duplicação na extração.
        def get_last_timestamp():
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            prefix = f"binance_klines/{symbol}/"
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            # Caso não haja arquivos no bucket, inicia a extração a partir do timestamp padrão.
            if not blobs:
                return DEFAULT_START_TIME
            
            # Obtém o último arquivo salvo no GCS e lê o maior timestamp registrado.
            last_blob = sorted(blobs, key=lambda b: b.name, reverse=True)[0]
            fs = gcsfs.GCSFileSystem()
            with fs.open(f"gs://{BUCKET_NAME}/{last_blob.name}") as f:
                df = pd.read_parquet(f)
            
            return int(df["close_time"].max()) + 1  # Retorna o próximo timestamp a ser coletado.

        # Faz requisição à API Binance, com lógica de re-tentativa para lidar com falhas temporárias.
        def fetch_data(url):
            max_retries = int(os.getenv("API_MAX_RETRIES", 3))
            retry_delay = int(os.getenv("API_RETRY_DELAY", 10))
            
            for _ in range(max_retries):
                response = requests.get(url)
                if response.status_code == 200:
                    return response.json()
                time.sleep(retry_delay)  # Aguarda antes de tentar novamente
            
            # Se a API falhar após todas as tentativas, a DAG é marcada como falha.
            raise AirflowFailException("Erro ao obter dados da Binance após múltiplas tentativas.")
        
        @task()
        def fetch_and_save_klines():
            """
            Extrai os dados da API Binance e salva em formato Parquet no Google Cloud Storage.
            O fluxo evita duplicação e segue boas práticas de particionamento por data.
            """
            now = datetime.now(timezone.utc)
            end_time = int(now.timestamp() * 1000)  # Timestamp atual em milissegundos
            start_time = get_last_timestamp()  # Obtém o próximo timestamp a ser coletado

            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            
            while start_time < end_time:
                # Monta a URL da API Binance para obter os candles da moeda específica.
                url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={INTERVAL}&limit={LIMIT}&startTime={start_time}"
                data = fetch_data(url)

                # Se não houver dados, interrompe o loop.
                if not data:
                    break

                # Converte os dados para um DataFrame Pandas com colunas nomeadas corretamente.
                df = pd.DataFrame(data, columns=[
                    "open_time", "open_price", "high_price", "low_price", "close_price", "volume", 
                    "close_time", "quote_asset_volume", "number_of_trades", 
                    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "_unused"
                ])
                df.drop(columns=["_unused"], inplace=True)  # Remove colunas desnecessárias

                # Define o próximo timestamp a ser extraído.
                start_time = int(df["close_time"].max()) + 1

                # Gera um nome de arquivo para o GCS, organizando os dados por ano/mês/dia.
                first_open_time = datetime.utcfromtimestamp(df["open_time"].min() / 1000)
                date_str = first_open_time.strftime('%Y-%m-%d')
                year_str = first_open_time.strftime('%Y')
                month_str = first_open_time.strftime('%m')
                part = 1 if first_open_time.hour < 12 else 2  # Divide os dados em partes diárias
                file_name = f"binance_klines_{symbol}_{date_str}_parte-{part}.parquet"
                gcs_path = f"binance_klines/{symbol}/{year_str}/{month_str}/{file_name}"

                # Verifica se o arquivo já existe para evitar duplicação.
                blob = bucket.blob(gcs_path)
                if blob.exists():
                    continue

                # Salva o DataFrame como Parquet localmente antes do upload.
                temp_parquet_path = f"/tmp/{file_name}"
                df.to_parquet(temp_parquet_path, index=False)

                # Faz upload do arquivo para o Google Cloud Storage.
                blob.upload_from_filename(temp_parquet_path)
            
        fetch_and_save_klines()
    
    return extract_binance_dag()

# Cria uma DAG separada para cada moeda configurada, garantindo flexibilidade.
for moeda in MOEDAS:
    globals()[f"binance_extracao_{moeda.lower()}"] = dag_factory(moeda)
