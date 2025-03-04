from airflow.decorators import task
import pandas as pd
from datetime import datetime, timezone
from utils.utils import get_last_timestamp, fetch_data, generate_gcs_path, save_dataframe_as_parquet, upload_file_to_gcs

@task()
def fetch_and_save_klines(symbol, bucket_name, interval, limit, default_start_time, base_url):
    """ Extrai dados da Binance e salva no GCS """
    end_time = int(datetime.utcnow().timestamp() * 1000) # Timestamp atual em milissegundos

    # Obtém o último timestamp salvo com a nova nomenclatura
    start_time = get_last_timestamp(symbol, bucket_name) or default_start_time

    print(f"{symbol}: Iniciando extração de {start_time} ({datetime.utcfromtimestamp(start_time / 1000)})")

    while start_time < end_time:
        gcs_path = generate_gcs_path(symbol, start_time)

        url = f"{base_url}?symbol={symbol}&interval={interval}&limit={limit}&startTime={start_time}"
        data = fetch_data(url)

        if not data:
            break

        df = pd.DataFrame(data, columns=[
            "open_time", "open_price", "high_price", "low_price", "close_price", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "_unused"
        ])
        df.drop(columns=["_unused"], inplace=True)

        # Converte colunas numéricas explicitamente para FLOAT
        numeric_cols = ["open_price", "high_price", "low_price", "close_price", "volume", "quote_asset_volume", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col])

        if len(df) < limit:
            print(f"{symbol}: Apenas {len(df)} registros retornados. Ignorando e encerrando extração.")
            break

        temp_parquet_path = save_dataframe_as_parquet(df)
        upload_file_to_gcs(bucket_name, temp_parquet_path, gcs_path)

        # Atualiza o timestamp do último registro salvo para o próximo ciclo de extração de dados (720 minutos)
        start_time += (720 * 60 * 1000)
