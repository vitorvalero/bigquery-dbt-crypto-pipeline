#!/bin/bash

echo "Parando serviços do Airflow..."
cd airflow && docker compose down
cd ..

echo "Parando serviços do Metabase..."
cd metabase && docker compose down
cd ..

echo "Parando serviços do Airbyte..."
docker stop airbyte-abctl-control-plane

echo "Todos os serviços foram desligados."
