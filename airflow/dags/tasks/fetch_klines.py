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
    present_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = get_last_timestamp(symbol, bucket_name) or default_start_time

    print(
        f"{symbol}: Iniciando extração de {start_time} ({datetime.utcfromtimestamp(start_time / 1000)})"
    )

    while start_time < present_time:
        url = f"{base_url}?symbol={symbol}&interval={interval}&limit={limit}&startTime={start_time}"
        data = fetch_data(url)

        if not data:
            break

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
        df.drop(columns=["_unused"], inplace=True)

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

        min_timestamp = df["open_time"].iloc[0]
        print(
            f"{symbol}: Nome do arquivo será baseado em {min_timestamp} ({datetime.utcfromtimestamp(min_timestamp / 1000)})"
        )
        gcs_path = generate_gcs_path(symbol, min_timestamp)

        if len(df) < limit:
            print(
                f"Parquet salvo com {len(df)} registros. Removendo antes de continuar..."
            )
            delete_parquet_from_gcs(bucket_name, gcs_path)
            break

        temp_parquet_path = save_dataframe_as_parquet(df)
        upload_file_to_gcs(bucket_name, temp_parquet_path, gcs_path)

        last_close_time = df["close_time"].max()
        if pd.notna(last_close_time):
            start_time = int(last_close_time) + 1
        else:
            break
