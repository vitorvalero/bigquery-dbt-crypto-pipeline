from google.cloud import storage, bigquery
import re
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def get_loaded_files_from_bq(client, gcp_project, dataset_id):
    query = f"""
        SELECT source_file FROM `{gcp_project}.{dataset_id}.bq_load_tracking`
    """
    result = client.query(query).result()

    return {row[0] for row in result}


def get_last_parquet_from_gcs(symbol, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=f"binance_klines/{symbol}/"))

    parquet_files = [
        blob.name
        for blob in blobs
        if re.search(
            r"binance_klines/[A-Z0-9]+/\d{4}/M\d{2}/[A-Z0-9]+_binance_klines_\d{4}-\d{2}-\d{2}-\d{2}\d{2}\.parquet$",
            blob.name,
        )
    ]

    if not parquet_files:
        return None

    def extract_datetime_key(filename):
        match = re.search(
            r"binance_klines/[A-Z0-9]+/\d{4}/M\d{2}/[A-Z0-9]+_binance_klines_(\d{4})-(\d{2})-(\d{2})-(\d{2})(\d{2})\.parquet$",
            filename,
        )
        if match:
            year, month, day, hour, minute = map(int, match.groups())
            return (year, month, day, hour, minute)
        return (0, 0, 0, 0, 0)

    parquet_files.sort(key=extract_datetime_key, reverse=True)

    return parquet_files[0]


def get_parquet_files_from_gcs(bucket_name, prefix="binance_klines/"):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=prefix))

    print(f"Arquivos encontrados no GCS ({len(blobs)}):")
    for blob in blobs:
        print(f"- {blob.name}")

    regex = re.compile(
        r"binance_klines/[A-Z0-9]+/\d{4}/M\d{2}/[A-Z0-9]+_binance_klines_\d{4}-\d{2}-\d{2}-\d{2}\d{2}\.parquet$",
        re.IGNORECASE,
    )

    parquet_files = sorted([blob.name for blob in blobs if regex.search(blob.name)])

    print(f"Arquivos Parquet encontrados ({len(parquet_files)}):")
    for file in parquet_files:
        print(f"- {file}")

    return parquet_files


def get_bigquery_client(conn_id):
    hook = BigQueryHook(gcp_conn_id=conn_id)
    return hook.get_client()


def get_bigquery_job_config():
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="open_time_ts"
        ),
        autodetect=True,
    )


def extract_symbol_from_filename(file_path):
    match = re.search(
        r"binance_klines/([A-Z0-9]+)/\d{4}/M\d{2}/[A-Z0-9]+_binance_klines_\d{4}-\d{2}-\d{2}-\d{2}\d{2}\.parquet",
        file_path,
    )
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Não foi possível extrair o símbolo do arquivo: {file_path}")


def create_bigquery_table_if_not_exists(client, gcp_project, dataset_id, symbol):
    table_id_full = f"{gcp_project}.{dataset_id}.binance_klines_{symbol}"

    schema = [
        bigquery.SchemaField("open_time", "INTEGER"),
        bigquery.SchemaField("open_time_ts", "TIMESTAMP"),
        bigquery.SchemaField("close_time", "INTEGER"),
        bigquery.SchemaField("close_time_ts", "TIMESTAMP"),
        bigquery.SchemaField("open_price", "FLOAT"),
        bigquery.SchemaField("high_price", "FLOAT"),
        bigquery.SchemaField("low_price", "FLOAT"),
        bigquery.SchemaField("close_price", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("quote_asset_volume", "FLOAT"),
        bigquery.SchemaField("number_of_trades", "INTEGER"),
        bigquery.SchemaField("taker_buy_base_asset_volume", "FLOAT"),
        bigquery.SchemaField("taker_buy_quote_asset_volume", "FLOAT"),
    ]

    table = bigquery.Table(table_id_full, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="open_time_ts"
    )

    try:
        client.get_table(table_id_full)
        print(f"Tabela {table_id_full} já existe.")
    except Exception as e:
        print(f"Tabela {table_id_full} não encontrada. Criando agora...")
        try:
            client.create_table(table)
            print(f"Tabela {table_id_full} criada com sucesso!")
        except Exception as create_error:
            print(f"Erro ao criar a tabela {table_id_full}: {create_error}")


def create_bq_tracking_table(client, gcp_project, dataset_id):
    table_id = f"{gcp_project}.{dataset_id}.bq_load_tracking"

    schema = [
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
        print(f"Tabela de controle {table_id} já existe.")
    except Exception:
        print(f"Criando tabela de controle {table_id}...")
        client.create_table(table)
        print(f"Tabela de controle criada com sucesso!")


def update_bigquery_table(client, gcp_project, dataset_id, symbol):
    table_id_full = f"{gcp_project}.{dataset_id}.binance_klines_{symbol}"

    update_query = f"""
        UPDATE `{table_id_full}`
        SET 
            open_time_ts = TIMESTAMP_MILLIS(CAST(open_time AS INT64)),
            close_time_ts = TIMESTAMP_MILLIS(CAST(close_time AS INT64))
        WHERE open_time_ts IS NULL OR close_time_ts IS NULL
    """
    client.query(update_query).result()


def check_if_file_exists(client, gcp_project, dataset_id, file_path):
    query = f"""
        SELECT COUNT(*) FROM `{gcp_project}.{dataset_id}.bq_load_tracking`
        WHERE source_file = "{file_path}"
    """
    result = client.query(query).result()
    return list(result)[0][0] > 0


def mark_file_as_loaded(client, gcp_project, dataset_id, file_path):
    query = f"""
        INSERT INTO `{gcp_project}.{dataset_id}.bq_load_tracking` (source_file, loaded_at)
        VALUES ("{file_path}", CURRENT_TIMESTAMP())
    """
    client.query(query).result()


def remove_failed_file(client, gcp_project, dataset_id, file_path):
    query = f"""
        DELETE FROM `{gcp_project}.{dataset_id}.bq_load_tracking`
        WHERE source_file = "{file_path}"
    """
    client.query(query).result()
