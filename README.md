# 🚀 Binance Crypto Pipeline - ELT com Airflow, BigQuery e DBT

![Badge](https://img.shields.io/badge/Status-Em%20Desenvolvimento-yellow?style=for-the-badge)
![Badge](https://img.shields.io/badge/Versão-0.1-blue?style=for-the-badge)

## 📌 Sobre o Projeto

Este é um projeto autoral de desenvolvimento de **pipeline de ELT (Extract, Load, Transform)** para coletar dados de trading da **API da Binance**, armazenar no **GCS**, carregar no **BigQuery**, e visualizar no **Metabase**. O objetivo é criar uma solução robusta e escalável para análise de dados do mercado de criptomoedas.

A estrutura do projeto foi elaborada do zero, bem como seu planejamento, arquitetura e implementação. Não houve uso de repositórios clonados, apenas de templates de arquivos de configuração **Docker Compose** de fontes oficiais para garantir o bom funcionamento e gerenciamento dos containers **Docker**.

---

## 🛠 Tecnologias Utilizadas

- **Fonte dos Dados**: [Binance API](https://api.binance.com)
- **Orquestração**: [Apache Airflow](https://airflow.apache.org/)
- **Armazenamento**: [Google Cloud Storage (GCS)](https://cloud.google.com/storage)
- **Data Warehouse**: [Google BigQuery](https://cloud.google.com/bigquery)
- **Transformação (futuro)**: [DBT](https://www.getdbt.com/)
- **Visualização**: [Metabase](https://www.metabase.com/)
- **Infraestrutura**: [Docker](https://www.docker.com/) + [Docker Compose](https://docs.docker.com/compose/)

---

## 🚧 Status do Projeto

⚠ **O projeto ainda está em construção!** Até agora, foram configurados os seguintes componentes:

- ✅ **Airflow** - Orquestração das DAGs de extração e carga de dados.
- ✅ **Google Cloud Storage (GCS)** - Armazenamento dos arquivos em formato Parquet.
- ✅ **BigQuery** - Armazenamento e estruturação dos dados como Data Warehouse.
- ✅ **Metabase** - Visualização inicial dos dados diretamente do BigQuery.

---

## 🏗 Arquitetura do Projeto

1. **Extração de Dados**: DAGs no **Airflow** fazem chamadas à API da **Binance** para obter dados de mercado.
2. **Armazenamento**: Os dados brutos são armazenados no **Google Cloud Storage (GCS)** em formato **Parquet**.
3. **Carga no BigQuery**: DAGs no **Airflow** carregam os arquivos Parquet no **BigQuery**.
4. **Transformação (em desenvolvimento)**: **DBT** será usado para modelagem e agregação dos dados.
5. **Visualização (em desenvolvimento)**: **Metabase** consome os dados diretamente do **BigQuery** para criar dashboards interativos.

---

## 🗂 Estrutura do Projeto

- **📂 airflow/** → Configuração do Airflow e DAGs do pipeline.  
- **📂 dags/** → DAGs do Airflow para extração e carga de dados.  
  - **📂 dags/tasks/** → Tarefas individuais que compõem as DAGs.  
  - **📂 dags/utils/** → Funções auxiliares para extração e transformação.  
- **📂 dbt/** → (Futuro) Diretório reservado para os modelos do DBT.  
- **📂 metabase/** → Configuração do Metabase para visualização de dados.  
- **📂 scripts/** → Scripts para ligar/desligar serviços.  

---

## 🔜 Próximos Desenvolvimentos

Este projeto está em constante evolução. As próximas etapas incluirão:

- **Implementação do DBT** para transformação de dados no BigQuery.
- **Criação de modelos analíticos e agregações** para otimizar consultas.
- **Desenvolvimento dos dashboards no Metabase** para visualização e insights mais detalhados.
- **Otimização das DAGs do Airflow** para melhorar a eficiência do pipeline.
- **Documentação detalhada das transformações e arquitetura final**.

Fique atento para futuras atualizações! 🚀
