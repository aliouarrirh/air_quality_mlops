#!/bin/sh
set -e
pip install "dagster==1.8.12" "dagster-webserver==1.8.12" "dagster-pipes==1.8.12" "dbt-core==1.8.7" "dbt-duckdb==1.8.7" -q --upgrade
mkdir -p /app/dagster_home
exec "$@"
