FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl git && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] pydantic \
    mlflow scikit-learn xgboost lightgbm \
    duckdb numpy pandas python-dotenv joblib \
    "dagster==1.8.12" "dagster-webserver==1.8.12" "dagster-pipes==1.8.12" \
    psycopg2-binary \
    dlt[duckdb] dbt-duckdb

# Copie tout le projet
COPY . .

EXPOSE 8000 3000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
