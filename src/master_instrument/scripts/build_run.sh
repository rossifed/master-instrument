#!/bin/bash
# Build and run script for dagster-code
# Automates: clean target → dbt compile → docker build → docker up

set -e  # Exit on error

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

ENV_FILE="$PROJECT_DIR/../../.env"

echo "Exporting ENV from $ENV_FILE..."
# Use set -a to auto-export all variables from .env
set -a
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    source "$SCRIPT_DIR/export_env.sh"
fi
set +a

DBT_PROJECT_DIR="$PROJECT_DIR/dbt_project"

echo "🧹 Cleaning dbt target folder..."
rm -rf "$DBT_PROJECT_DIR/target"

echo "🔨 Compiling dbt models..."
dbt compile --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR"

echo "🐳 Building dagster-code image..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" --env-file "$ENV_FILE" build dagster-code

echo "🚀 Starting dagster-code container..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" --env-file "$ENV_FILE" up -d dagster-code

echo "✅ Done! Dagster should be running."
echo "📊 Check status: docker compose ps"
echo "📜 View logs: docker compose logs -f dagster-code"
