#!/bin/bash

echo "Iniciando serviços do Airflow..."
cd airflow && docker compose up -d
cd ..

echo "Iniciando serviços do Metabase..."
cd metabase && docker compose up -d
cd ..

echo "Todos os serviços foram iniciados."
