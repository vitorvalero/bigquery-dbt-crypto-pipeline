#!/bin/bash

echo "Parando serviços do Airflow..."
cd airflow && docker compose down
cd ..

echo "Parando serviços do Metabase..."
cd metabase && docker compose down
cd ..

echo "Todos os serviços foram desligados."
