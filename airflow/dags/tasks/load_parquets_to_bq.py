from airflow.decorators import task
from concurrent.futures import ThreadPoolExecutor
from utils.load_parquets_to_bq_utils import (
    get_parquet_files_from_gcs,
    create_raw_dataset,
    get_bigquery_client,
    get_bigquery_job_config,
    extract_symbol_from_filename,
    create_bigquery_table_if_not_exists,
    create_bq_tracking_table,
    mark_file_as_loaded,
    remove_failed_file,
    update_bigquery_table,
    get_loaded_files_from_bq,
)


@task()
def process_and_load_parquets(
    bucket_name, dataset_id, gcp_project, conn_id, datasets, location
):
    # Obtém a lista de arquivos Parquet disponíveis no GCS
    all_files = get_parquet_files_from_gcs(bucket_name)

    # Se não houver arquivos novos, finaliza a execução
    if not all_files:
        print("Nenhum novo arquivo para processar.")
        return []

    # Inicializa o cliente do BigQuery
    client = get_bigquery_client(conn_id)

    # Cria o dataset no BigQuery caso ainda não exista
    create_raw_dataset(conn_id, datasets, gcp_project, location)

    # Cria a tabela de controle de arquivos carregados
    create_bq_tracking_table(client, gcp_project, dataset_id)

    # Obtém a lista de arquivos já carregados para evitar duplicidade
    loaded_files_set = get_loaded_files_from_bq(client, gcp_project, dataset_id)

    # Agrupa os arquivos por símbolo para processamento
    symbol_files = {}
    for file in all_files:
        symbol = extract_symbol_from_filename(file)
        if symbol not in symbol_files:
            symbol_files[symbol] = []
        symbol_files[symbol].append(file)

    # Cria tabelas no BigQuery para cada símbolo, se ainda não existirem
    for symbol in symbol_files.keys():
        create_bigquery_table_if_not_exists(client, gcp_project, dataset_id, symbol)

    # Configuração do job de carregamento no BigQuery
    job_config = get_bigquery_job_config()
    successfully_loaded_files = []

    def process_symbol_files(symbol, files):
        # Processa os arquivos de um determinado símbolo
        loaded_files = []
        table_id_full = f"{gcp_project}.{dataset_id}.raw_binance_klines_{symbol}"

        print(f"Iniciando processamento do símbolo {symbol}...")

        for file_path in sorted(files):
            try:
                gcs_uri = f"gs://{bucket_name}/{file_path}"

                # Pula arquivos que já foram carregados anteriormente
                if file_path in loaded_files_set:
                    continue

                print(f"Carregando {gcs_uri} para {table_id_full}...")

                # Executa o job de carregamento no BigQuery
                job = client.load_table_from_uri(
                    gcs_uri, table_id_full, job_config=job_config
                )
                job.result()

                # Registra o arquivo como carregado na tabela de controle
                mark_file_as_loaded(client, gcp_project, dataset_id, file_path)

                print(f"Concluído: {file_path}")
                loaded_files.append(file_path)
            except Exception as e:
                print(f"Erro ao carregar {file_path}: {e}")
                remove_failed_file(client, gcp_project, dataset_id, file_path)

        try:
            # Atualiza os timestamps nas tabelas do BigQuery
            print(f"Atualizando timestamps para {symbol}...")
            update_bigquery_table(client, gcp_project, dataset_id, symbol)
            print(f"Timestamps atualizados para {symbol}.")
        except Exception as e:
            print(f"Erro ao atualizar timestamps para {symbol}: {e}")

        print(f"✅ Processamento do símbolo {symbol} finalizado.")
        return loaded_files

    # Executa o processamento dos símbolos em paralelo usando ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(
            process_symbol_files, symbol_files.keys(), symbol_files.values()
        )

    # Coleta todos os arquivos carregados com sucesso
    for res in results:
        successfully_loaded_files.extend(res)

    print(f"Arquivos carregados no BigQuery: {successfully_loaded_files}")
    return successfully_loaded_files
