from airflow.decorators import task
import pandas as pd
from datetime import datetime, timezone
from utils.fetch_klines_utils import (
    get_last_timestamp,
    fetch_data,
    generate_gcs_path,
    save_dataframe_as_parquet,
    upload_file_to_gcs,
    delete_parquet_from_gcs,
)


@task()
def fetch_and_save_klines(
    symbol, bucket_name, interval, limit, default_start_time, base_url
):
    # Obtém o timestamp atual em milissegundos
    present_time = int(datetime.now(timezone.utc).timestamp() * 1000)

    # Recupera o último timestamp disponível para a moeda ou usa o valor padrão
    start_time = get_last_timestamp(symbol, bucket_name) or default_start_time

    print(
        f"{symbol}: Iniciando extração de {start_time} ({datetime.utcfromtimestamp(start_time / 1000)})"
    )

    # Continua a extração enquanto o start_time for menor que o tempo atual
    while start_time < present_time:
        url = f"{base_url}?symbol={symbol}&interval={interval}&limit={limit}&startTime={start_time}"
        data = fetch_data(url)

        # Para a execução caso não haja mais dados a serem extraídos
        if not data:
            break

        # Converte os dados extraídos em um DataFrame do pandas
        df = pd.DataFrame(
            data,
            columns=[
                "open_time",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "_unused",
            ],
        )

        # Remove a coluna '_unused', que não é necessária
        df.drop(columns=["_unused"], inplace=True)

        # Converte colunas numéricas para o tipo correto
        numeric_cols = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "quote_asset_volume",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col])

        # Determina o menor timestamp no DataFrame para nomear o arquivo
        min_timestamp = df["open_time"].iloc[0]
        print(
            f"{symbol}: Nome do arquivo será baseado em {min_timestamp} ({datetime.utcfromtimestamp(min_timestamp / 1000)})"
        )
        gcs_path = generate_gcs_path(symbol, min_timestamp)

        # Se o número de registros for menor que o limite, exclui o Parquet e interrompe a extração
        if len(df) < limit:
            print(
                f"Parquet salvo com {len(df)} registros. Removendo antes de continuar..."
            )
            delete_parquet_from_gcs(bucket_name, gcs_path)
            break

        # Salva os dados em um arquivo Parquet temporário
        temp_parquet_path = save_dataframe_as_parquet(df)

        # Faz o upload do arquivo para o Google Cloud Storage
        upload_file_to_gcs(bucket_name, temp_parquet_path, gcs_path)

        # Atualiza o start_time para continuar a extração a partir do último close_time disponível
        last_close_time = df["close_time"].max()
        if pd.notna(last_close_time):
            start_time = int(last_close_time) + 1
        else:
            break
