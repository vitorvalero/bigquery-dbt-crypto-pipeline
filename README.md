# 🚀 Binance Crypto Pipeline - ELT com Airflow, BigQuery e DBT

![Badge](https://img.shields.io/badge/Status-Concluído-green?style=for-the-badge)
![Badge](https://img.shields.io/badge/Versão-1.0-blue?style=for-the-badge)

## 📌 Sobre o Projeto

Este é um projeto autoral de pipeline de **ELT (Extract, Load, Transform)** para coleta, armazenamento e processamento de dados de trading da **API** da Binance. O pipeline armazena os dados brutos no **Google Cloud Storage (GCS)**, carrega no **BigQuery** e realiza transformações utilizando **dbt**.

O objetivo é construir uma solução escalável e eficiente para análise do mercado de criptomoedas, seguindo boas práticas de engenharia de dados. Toda a arquitetura e implementação foram desenvolvidas do zero.

---

## 🛠 Ferramentas Utilizadas

- ✅ **Docker** - Containerização dos serviços para garantir reprodutibilidade e escalabilidade.
- ✅ **Airflow** - Orquestração das DAGs para extração, carga e processamento dos dados.
- ✅ **Google Cloud Storage (GCS)** - Armazenamento dos dados brutos em formato Parquet.
- ✅ **BigQuery** - Data Warehouse utilizado para armazenar e processar grandes volumes de dados.
- ✅ **dbt** - Modelagem e transformação dos dados, estruturando as camadas STG (Stage) e Analytics.

---

## 🏗 Arquitetura do Pipeline

- **Extração de Dados**
    - O Airflow agenda e executa a coleta de dados da API da Binance em tempo real.
    - Os dados são extraídos no formato JSON e convertidos para Parquet.

- **Armazenamento no Google Cloud Storage (GCS)**
    - Os arquivos Parquet são armazenados no GCS, mantendo assim cópia dos dados com seu conteúdo e estrutura original .

- **Carga no BigQuery**
    - O Airflow carrega os arquivos Parquet para a camada raw do BigQuery.
    - Os dados são armazenados com particionamento por data para otimizar consultas.

- **Transformação e Modelagem**
    - Utilizando dbt, os dados passam por tratamento e transformação nas camadas:
        - STG (Staging) → Normaliza e padroniza os dados extraídos.
        - Analytics → Modelos agregados para análise e visualização.
