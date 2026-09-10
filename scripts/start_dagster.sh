#!/bin/sh
set -e
pip install "dagster==1.8.12" "dagster-webserver==1.8.12" "dagster-pipes==1.8.12" psycopg2-binary requests -q --upgrade
mkdir -p /app/dagster_home
exec "$@"
