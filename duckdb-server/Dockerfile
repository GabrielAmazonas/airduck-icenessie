# =============================================================================
# DuckDB FlightSQL Server
# =============================================================================
# Provides centralized DuckDB access via Apache Arrow Flight SQL protocol.
# Includes VSS (vector search), httpfs (S3), and iceberg extensions.
# =============================================================================

FROM python:3.11-slim-bookworm

LABEL maintainer="polary-team"
LABEL description="DuckDB FlightSQL Server with Vector Search Extensions"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages with pinned versions
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-install DuckDB extensions (cached in image)
RUN python -c "\
import duckdb; \
conn = duckdb.connect(':memory:'); \
conn.execute('INSTALL vss'); \
conn.execute('INSTALL httpfs'); \
conn.execute('INSTALL iceberg'); \
print('Extensions installed: vss, httpfs, iceberg')"

# Copy server code
COPY server.py .
COPY health.py .

# Create non-root user
RUN useradd -m -u 1000 duckdb && \
    mkdir -p /mnt/efs && \
    chown -R duckdb:duckdb /app /mnt/efs

USER duckdb

# Expose ports
EXPOSE 8815 8816

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8816/health || exit 1

# Start server
CMD ["python", "server.py"]


