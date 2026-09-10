#!/bin/sh
set -e

echo "=== [start_dagster] pip install dagster 1.8.12 ==="
pip install "dagster==1.8.12" "dagster-webserver==1.8.12" "dagster-pipes==1.8.12" --upgrade 2>&1 | tail -5

echo "=== [start_dagster] pip install psycopg2-binary + requests ==="
pip install psycopg2-binary requests 2>&1 | tail -5

echo "=== [start_dagster] verification imports ==="
python -c "import psycopg2; print('psycopg2 OK', psycopg2.__version__)"
python -c "import requests; print('requests OK', requests.__version__)"

mkdir -p /app/dagster_home
echo "=== [start_dagster] demarrage: $* ==="
exec "$@"
