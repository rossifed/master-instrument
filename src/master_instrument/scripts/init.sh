#!/usr/bin/env bas
echo "Exporting ENV..."
source scripts/export_env.sh

docker compose up -d

echo "Waiting for PostgreSQL to be ready..."
until docker exec postgres-master-instrument pg_isready -U postgres -d master_instrument > /dev/null 2>&1; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "PostgreSQL is ready!"

echo "Running alembic upgrade head..."
alembic upgrade head

echo "Running alembic revision --autogenerate..."
alembic revision --autogenerate

echo "Running alembic upgrade head..."
alembic upgrade head

echo "Deleting dbt_project/target..."
rm -rf dbt_project/target

echo "Running dbt seed..."
dbt seed --project-dir dbt_project --profiles-dir dbt_project

echo "Running dbt compile..."
dbt compile --project-dir dbt_project --profiles-dir dbt_project

echo "Building dagster-code image..."
docker compose build dagster-code --no-cache

echo "Starting containers..."
docker compose up -d

echo "Done."
