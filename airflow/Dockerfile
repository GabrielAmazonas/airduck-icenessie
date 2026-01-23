# =============================================================================
# Airflow with dbt Integration
# =============================================================================
# Includes:
# - Apache Airflow 2.8.0
# - dbt-core with Trino adapter
# - DuckDB for vector index building
# - sentence-transformers for embedding generation
# =============================================================================

FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create dbt directory
RUN mkdir -p /opt/airflow/dbt && chown -R airflow:root /opt/airflow/dbt

USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Verify dbt installation
RUN dbt --version

# Set working directory
WORKDIR /opt/airflow
