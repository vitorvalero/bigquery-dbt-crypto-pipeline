from airflow.decorators import task
from utils.utils import get_parquet_files_from_gcs, get_bigquery_client, get_bigquery_job_config, extract_symbol_from_filename, update_bigquery_table

@task()
def process_and_load_parquets(bucket_name, dataset_id, table_id, gcp_project, conn_id):
    """ Identifica arquivos Parquet novos no GCS e os carrega no BigQuery, aplicando partition, cluster e update """

    # Busca todos os arquivos Parquet disponíveis no GCS
    all_files = get_parquet_files_from_gcs(bucket_name)

    if not all_files:
        print("✅ Nenhum novo arquivo para processar.")
        return []

    client = get_bigquery_client(conn_id)
    job_config = get_bigquery_job_config()

    successfully_loaded_files = []

    for file_path in all_files:
        gcs_uri = f"gs://{bucket_name}/{file_path}"
        table_id_full = f"{gcp_project}.{dataset_id}.{table_id}"

        print(f"🚀 Carregando {gcs_uri} para {table_id_full}...")

        # Executa a carga no BigQuery
        job = client.load_table_from_uri(gcs_uri, table_id_full, job_config=job_config)
        job.result()

        # Obtém o símbolo e faz o update na tabela
        symbol = extract_symbol_from_filename(file_path)
        update_bigquery_table(client, gcp_project, dataset_id, table_id, file_path, symbol)

        successfully_loaded_files.append(file_path)

    print(f"✅ Arquivos carregados no BigQuery: {successfully_loaded_files}")
    return successfully_loaded_files
