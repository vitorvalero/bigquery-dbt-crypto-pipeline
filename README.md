# 🚀 Binance Crypto Pipeline - ELT with Airflow, BigQuery, and DBT

![Badge](https://img.shields.io/badge/Status-Completed-green?style=for-the-badge)
![Badge](https://img.shields.io/badge/Version-1.0-blue?style=for-the-badge)

## 📌 About the Project

This is an **ELT (Extract, Load, Transform)** pipeline project for collecting, storing, and processing trading data from Binance’s **API**. The pipeline stores raw data in **Google Cloud Storage (GCS)**, loads it into **BigQuery**, and performs transformations using **dbt**.

The goal is to build a scalable and efficient solution for cryptocurrency market analysis, following best practices in data engineering. The entire architecture and implementation were developed from scratch.

---

## 🛠 Tools Used

- ✅ **Docker** - Containerization of services to ensure reproducibility and scalability.
- ✅ **Airflow** - Orchestration of DAGs for data extraction, loading, and processing.
- ✅ **Google Cloud Storage (GCS)** - Storage of raw data in Parquet format.
- ✅ **BigQuery** - Data warehouse used to store and process large volumes of data.
- ✅ **dbt** - Data modeling and transformation, structuring the STG (Stage) and Analytics layers.

---

## 🏗 Pipeline Architecture

- **Data Extraction**
    - Airflow schedules and executes real-time data collection from the Binance API.
    - Data is extracted in JSON format and converted to Parquet.

- **Storage in Google Cloud Storage (GCS)**
    - Parquet files are stored in GCS, preserving the content and structure of the original data.

- **Loading into BigQuery**
    - Airflow loads the Parquet files into the raw layer of BigQuery.
    - Data is stored with date partitioning to optimize queries.

- **Transformation and Modeling**
    - Using dbt, data undergoes cleaning and transformation in the following layers:
        - STG (Staging) → Normalizes and standardizes the extracted data.
        - Analytics → Aggregated models for analysis and visualization.
