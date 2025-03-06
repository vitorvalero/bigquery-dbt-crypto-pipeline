from google.cloud import storage
import requests
import re
import time
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import tempfile
import os
import time
import datetime
import calendar


def delete_parquet_from_gcs(bucket_name, gcs_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    if blob.exists():
        blob.delete()
        print(f"Arquivo removido: {gcs_path}")
    else:
        print(f"Arquivo não encontrado para remoção: {gcs_path}")


import datetime


def get_last_timestamp(symbol, bucket_name):
    last_parquet_file = get_last_parquet_from_gcs(symbol, bucket_name)

    if last_parquet_file:
        match = re.search(
            r"binance_klines/[A-Z0-9]+/\d{4}/M\d{2}/[A-Z0-9]+_binance_klines_(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})\.parquet$",
            last_parquet_file,
        )
        if match:
            year, month, day, hour, minute = map(int, match.groups())

            dt_utc = datetime.datetime(
                year, month, day, hour, minute, 0, tzinfo=datetime.timezone.utc
            )

            last_timestamp = int(calendar.timegm(dt_utc.timetuple()) * 1000)

            print(f"DEBUG {symbol}: Nome do arquivo -> {last_parquet_file}")
            print(
                f"DEBUG {symbol}: Extraído -> {year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d} UTC"
            )
            print(
                f"DEBUG {symbol}: Timestamp correto = {last_timestamp} ({datetime.datetime.utcfromtimestamp(last_timestamp / 1000)} UTC)"
            )

            return last_timestamp

    print(f"DEBUG {symbol}: Nenhum arquivo encontrado no GCS. Retornando None.")
    return None


def generate_gcs_path(symbol, start_time):
    dt_utc = datetime.datetime.utcfromtimestamp(start_time / 1000)

    year = dt_utc.year
    month = dt_utc.month
    day = dt_utc.day
    hour = dt_utc.hour
    minute = dt_utc.minute

    print(f"DEBUG {symbol}: Gerando nome de arquivo com base em {dt_utc} UTC")

    gcs_path = (
        f"binance_klines/{symbol}/{year}/M{month:02d}/"
        f"{symbol}_binance_klines_{year}-{month:02d}-{day:02d}-{hour:02d}{minute:02d}.parquet"
    )

    return gcs_path


def fetch_data(url, max_retries=3):
    retry_delay = 10

    for attempt in range(max_retries):
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()

        print(f"Tentativa {attempt + 1}: Erro {response.status_code} - {response.text}")
        time.sleep(retry_delay)

    raise AirflowFailException(
        "Erro ao obter dados da Binance após múltiplas tentativas."
    )


def save_dataframe_as_parquet(df):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temp_parquet:
        df.to_parquet(temp_parquet.name, index=False)
        return temp_parquet.name


def upload_file_to_gcs(bucket_name, local_file_path, gcs_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    blob.upload_from_filename(local_file_path)
    os.remove(local_file_path)

    print(f"Arquivo salvo em {gcs_path}")
