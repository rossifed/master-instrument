#!/bin/bash

# Load environment variables from .env file (at project root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
set -a
source "$PROJECT_ROOT/.env"
set +a

echo "PostgreSQL environment variables exported from .env"
