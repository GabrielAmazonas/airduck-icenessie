# Airduck-IceNessie

A modern data stack for Vector Search & RAG (Retrieval-Augmented Generation), combining open-source data lakehouse technologies with embedded vector search.

## Overview

This architecture demonstrates how to build a production-ready RAG system without proprietary vector databases:

- **DuckDB + VSS** for embedded vector search with HNSW indexing
- **Apache Iceberg + Nessie** for open table format data lakehouse
- **Trino** as federated SQL query engine
- **dbt** for medallion architecture transformations (Bronze → Silver → Gold)
- **Apache Airflow** for orchestration
- **MinIO** as S3-compatible object storage

> For detailed architecture documentation, see [ARCHITECTURE.md](./ARCHITECTURE.md)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            PRESENTATION LAYER                                │
│  ┌─────────────────┐     ┌─────────────────┐                                │
│  │   Next.js UI    │────▶│  FastAPI + RAG  │◀──── Vector Search API         │
│  │    (3000)       │     │     (8000)      │      /search, /index, /embed   │
│  └─────────────────┘     └────────┬────────┘                                │
├────────────────────────────────────┼────────────────────────────────────────┤
│                            VECTOR STORE                                      │
│                                    ▼                                         │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                      DuckDB + VSS Extension                            │  │
│  │  • HNSW Index          • 384-dim embeddings      • all-MiniLM-L6-v2   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│                            ORCHESTRATION                                     │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         Apache Airflow                                 │  │
│  │  • daily_reindex DAG           • dbt_manual_transforms DAG            │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│                            DATA LAKEHOUSE                                    │
│  ┌────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐   │
│  │     Trino      │──│     Nessie      │──│         MinIO (S3)          │   │
│  │    (8085)      │  │ Iceberg Catalog │  │  iceberg-warehouse/         │   │
│  │  dbt-trino     │  │    (19120)      │  │  rag-data/                  │   │
│  └────────────────┘  └─────────────────┘  └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 3000 | Next.js search UI |
| FastAPI | 8000 | Vector search API with embedding generation |
| Airflow | 8080 | Workflow orchestrator |
| Trino | 8085 | Federated SQL query engine |
| Nessie | 19120 | Iceberg catalog with git-like versioning |
| MinIO | 9000/9001 | S3-compatible object storage |
| PostgreSQL | 5432 | Airflow metadata DB |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- 8GB+ RAM recommended

### Running the Stack

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Stop all services
docker compose down
```

### Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Frontend | http://localhost:3000 | - |
| API Docs | http://localhost:8000/docs | - |
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| Trino UI | http://localhost:8085 | - |
| Nessie UI | http://localhost:19120 | - |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |

## Embedding Model

The system uses **`all-MiniLM-L6-v2`** for semantic embeddings:

| Metric | Value |
|--------|-------|
| Dimensions | 384 |
| Model size | ~22 MB |
| Container memory | ~370 MB |
| Inference | CPU-only (no GPU required) |

Embeddings are generated automatically when indexing documents via the API.

## API Endpoints

### Vector Search (FastAPI)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/status` | Index status with embedding info |
| POST | `/search` | Vector similarity search |
| POST | `/search/s3` | Search directly on S3 parquet files |
| POST | `/index` | Index single document (auto-embeds) |
| POST | `/index/batch` | Batch index documents |
| POST | `/embed` | Generate embedding for text |
| DELETE | `/documents/{id}` | Delete document |
| DELETE | `/documents` | Clear all documents |

### Example: Index a Document

```bash
curl -X POST http://localhost:8000/index \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc-001",
    "content": "This is a sample document about vector search.",
    "metadata": {"source": "manual", "category": "tutorial"}
  }'
```

### Example: Semantic Search

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "how does vector similarity work?", "top_k": 5}'
```

## Data Pipeline

The system uses a **medallion architecture** for data quality:

```
Bronze (raw)  →  Silver (cleaned)  →  Gold (enriched)
     ↓                ↓                    ↓
 Raw ingestion   Deduplicated        Chunked, scored,
                 SHA256 hashed       ready for embedding
```

dbt transformations run on Trino, with Iceberg tables stored in MinIO.

## Project Structure

```
airduck-icenessie/
├── docker-compose.yml       # Service orchestration
├── ARCHITECTURE.md          # Detailed architecture docs
├── app/                     # FastAPI + DuckDB vector search
│   ├── main.py             # API endpoints + embedding
│   └── requirements.txt    # CPU-only PyTorch
├── airflow/                 # Workflow orchestrator
│   ├── dags/               # DAG definitions
│   └── dbt/                # dbt project
│       ├── models/
│       │   ├── bronze/     # Raw layer
│       │   ├── silver/     # Cleaned layer
│       │   └── gold/       # Enriched layer
│       └── profiles.yml    # Trino connection
├── frontend/                # Next.js UI
│   └── src/app/
├── trino/                   # Trino configuration
│   ├── catalog/            # Iceberg + Nessie config
│   └── etc/                # Server config
└── minio-data/              # Persistent S3 storage
```

## Development

### Local Development (without Docker)

#### FastAPI

```bash
cd app
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

#### Frontend

```bash
cd frontend
npm install
npm run dev
```

## Configuration

Key environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `EMBEDDING_MODEL` | Sentence transformer model | `all-MiniLM-L6-v2` |
| `S3_ENDPOINT` | MinIO/S3 endpoint | `http://minio:9000` |
| `EFS_PATH` | Shared storage path | `/mnt/efs` |
| `NEXT_PUBLIC_API_URL` | API URL for frontend | `http://localhost:8000` |

## Further Reading

- [ARCHITECTURE.md](./ARCHITECTURE.md) - Detailed architecture documentation
- [DuckDB VSS Extension](https://duckdb.org/docs/extensions/vss)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Project Nessie](https://projectnessie.org/)
- [dbt-trino](https://github.com/starburstdata/dbt-trino)

## License

MIT
