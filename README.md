# RAG Stack - Vector Search Engine

A complete RAG (Retrieval-Augmented Generation) stack with vector search capabilities, built with DuckDB, FastAPI, Airflow, and Next.js.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Frontend   │────▶│   FastAPI    │────▶│   DuckDB     │
│   (Next.js)  │     │   (Search)   │     │ (Vector DB)  │
└──────────────┘     └──────────────┘     └──────────────┘
                            ▲                     ▲
                            │                     │
                     ┌──────┴──────┐       ┌──────┴──────┐
                     │   Airflow   │──────▶│   MinIO     │
                     │ (Scheduler) │       │    (S3)     │
                     └─────────────┘       └─────────────┘
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 3000 | Next.js search UI |
| FastAPI | 8000 | Search engine API |
| Airflow | 8080 | Workflow orchestrator |
| MinIO | 9000/9001 | S3-compatible storage |
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

- **Frontend**: http://localhost:3000
- **API Docs**: http://localhost:8000/docs
- **Airflow UI**: http://localhost:8080 (user: `airflow`, pass: `airflow`)
- **MinIO Console**: http://localhost:9001 (user: `minioadmin`, pass: `minioadmin`)

## API Endpoints

### Search Engine (FastAPI)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/status` | Index status |
| POST | `/search` | Search documents |
| POST | `/index` | Index single document |
| POST | `/index/batch` | Batch index documents |
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

### Example: Search

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "vector search", "top_k": 5}'
```

## Project Structure

```
rag-stack/
├── docker-compose.yml      # Service orchestration
├── .env                    # Environment variables
├── app/                    # FastAPI + DuckDB
│   ├── Dockerfile
│   ├── main.py            # API endpoints
│   ├── requirements.txt
│   └── start.sh
├── airflow/                # Workflow orchestrator
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── reindex_dag.py # Reindexing pipeline
└── frontend/               # Next.js UI
    ├── Dockerfile
    ├── package.json
    └── src/
        └── app/
            └── page.tsx   # Search interface
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

Key environment variables (see `.env`):

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_ENDPOINT` | MinIO/S3 endpoint | `http://minio:9000` |
| `EFS_PATH` | Shared storage path | `/mnt/efs` |
| `NEXT_PUBLIC_API_URL` | API URL for frontend | `http://localhost:8000` |

## License

MIT

