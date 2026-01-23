# RAG Stack: Vector Search Architecture Summary

## Overview

This project implements a **production-ready RAG (Retrieval-Augmented Generation) infrastructure** using DuckDB for vector storage, FastAPI for the search API, Airflow for orchestration, and MinIO (S3-compatible) for durable storage.

The architecture provides **two distinct vector search paths** optimized for different use cases, enabling both high-performance production queries and resilient disaster recovery scenarios.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER / APPLICATION                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FASTAPI SEARCH ENGINE                              │
│  ┌─────────────────────────────┐    ┌─────────────────────────────────────┐ │
│  │      /search (HNSW)         │    │         /search/s3 (Direct)         │ │
│  │  • O(log n) complexity      │    │  • O(n) complexity                  │ │
│  │  • Pre-built index          │    │  • Query-time computation           │ │
│  │  • Sub-millisecond          │    │  • Seconds for large datasets       │ │
│  └──────────────┬──────────────┘    └──────────────────┬──────────────────┘ │
│                 │                                       │                    │
│  ┌──────────────▼──────────────┐    ┌──────────────────▼──────────────────┐ │
│  │   sentence-transformers     │    │      sentence-transformers          │ │
│  │   (all-MiniLM-L6-v2)        │    │      (all-MiniLM-L6-v2)             │ │
│  │   384-dim embeddings        │    │      384-dim embeddings             │ │
│  └─────────────────────────────┘    └─────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                 │                                       │
                 ▼                                       ▼
┌────────────────────────────────┐    ┌─────────────────────────────────────┐
│        DuckDB + HNSW           │    │         MinIO (S3)                  │
│   /mnt/efs/index_*.duckdb      │    │   s3://rag-data/backup/             │
│                                │    │   └── documents/                    │
│   • Vector index (HNSW)        │    │       └── year=2026/                │
│   • Fast approximate search    │    │           └── month=1/              │
│   • Atomic index swaps         │    │               └── day=11/           │
│                                │    │                   └── *.parquet     │
└────────────────────────────────┘    └─────────────────────────────────────┘
                 ▲                                       ▲
                 │                                       │
                 └───────────────────┬───────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────────┐
│                           AIRFLOW ORCHESTRATOR                               │
│  ┌──────────────┐    ┌──────────────────┐    ┌────────────────────────────┐ │
│  │ build_index  │───▶│   backup_to_s3   │───▶│   cleanup_old_indexes      │ │
│  │              │    │                  │    │                            │ │
│  │ • Load data  │    │ • Export to      │    │ • Remove old index files   │ │
│  │ • Generate   │    │   partitioned    │    │ • Keep last 3 versions     │ │
│  │   embeddings │    │   parquet        │    │                            │ │
│  │ • Build HNSW │    │ • With embeddings│    │                            │ │
│  │ • Atomic swap│    │                  │    │                            │ │
│  └──────────────┘    └──────────────────┘    └────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Vector Search Comparison

### `/search` - DuckDB with HNSW Index

| Aspect | Details |
|--------|---------|
| **Algorithm** | HNSW (Hierarchical Navigable Small World) |
| **Complexity** | O(log n) - Approximate Nearest Neighbor |
| **Data Source** | Pre-built DuckDB index on EFS |
| **Latency** | Sub-millisecond to milliseconds |
| **Accuracy** | Approximate (configurable recall) |
| **Best For** | Production real-time queries |

**How it works:**
1. Query text is converted to 384-dim embedding
2. HNSW graph is traversed to find approximate nearest neighbors
3. Results returned with cosine similarity scores

**Example:**
```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning APIs", "top_k": 5}'
```

---

### `/search/s3` - Direct S3 Parquet Query

| Aspect | Details |
|--------|---------|
| **Algorithm** | Brute-force cosine similarity |
| **Complexity** | O(n) - Full table scan |
| **Data Source** | Parquet files on S3/MinIO |
| **Latency** | Seconds to minutes (depends on data size) |
| **Accuracy** | Exact (100% recall) |
| **Best For** | DR, historical queries, validation |

**How it works:**
1. Query text is converted to 384-dim embedding
2. DuckDB reads parquet files directly from S3
3. Cosine similarity computed for every document
4. Results sorted and returned

**Example:**
```bash
curl -X POST http://localhost:8000/search/s3 \
  -H "Content-Type: application/json" \
  -d '{"query": "machine learning APIs", "top_k": 5, "path": "backup/documents"}'
```

---

## Document Indexing with Automatic Embeddings

### `/index` - Single Document Indexing

The `/index` endpoint provides **automatic embedding generation** for documents. When you submit a document, it:

1. ✅ **Generates embeddings automatically** using `all-MiniLM-L6-v2` (384 dimensions)
2. ✅ **Stores both content and embedding** in DuckDB for vector search
3. ✅ **Supports upserts** - update existing documents by ID
4. ✅ **Returns confirmation** with embedding details

**Request:**
```bash
curl -X POST http://localhost:8000/index \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc-001",
    "content": "Your document text goes here",
    "metadata": {"source": "manual", "category": "test"}
  }'
```

**Response:**
```json
{
    "status": "indexed",
    "id": "doc-001",
    "embedding_generated": true,
    "embedding_dim": 384
}
```

### `/index/batch` - Batch Document Indexing

For bulk ingestion, the `/index/batch` endpoint accepts an array of documents and generates embeddings for all of them in a single batch operation (more efficient than individual calls).

**Request:**
```bash
curl -X POST http://localhost:8000/index/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"id": "doc-001", "content": "First document"},
    {"id": "doc-002", "content": "Second document"}
  ]'
```

**Response:**
```json
{
    "status": "indexed",
    "count": 2,
    "embeddings_generated": 2
}
```

### Embedding Model Details

| Property | Value |
|----------|-------|
| **Model** | `all-MiniLM-L6-v2` |
| **Dimensions** | 384 |
| **Library** | sentence-transformers |
| **Similarity** | Cosine similarity |
| **Loaded at** | FastAPI startup (lifespan handler) |

> **Note:** The model is loaded once at startup and reused for all requests, ensuring fast embedding generation without model loading overhead.

---

## When to Use Each

| Scenario | Use `/search` | Use `/search/s3` |
|----------|---------------|------------------|
| Real-time user queries | ✅ | ❌ |
| Production search | ✅ | ❌ |
| Disaster recovery | ❌ | ✅ |
| Historical data queries | ❌ | ✅ |
| Exact nearest neighbors | ❌ | ✅ |
| Index validation | ❌ | ✅ |
| Debugging/testing | ❌ | ✅ |
| Cross-partition queries | ❌ | ✅ |

---

## RAG Use Cases This Architecture Enables

### 1. **Enterprise Knowledge Base Search**
Build internal search engines for company documentation, wikis, and policies with semantic understanding.

```
User: "What's our policy on remote work?"
→ Returns relevant HR documents even if they don't contain "remote work" exactly
```

### 2. **Customer Support Chatbots**
Power AI assistants that retrieve relevant support articles, FAQs, and documentation to answer customer questions.

```
User: "My order hasn't arrived"
→ Retrieves: shipping policies, order tracking guides, refund procedures
→ LLM generates contextual response
```

### 3. **Legal Document Analysis**
Search through contracts, legal briefs, and regulatory documents using semantic similarity.

```
Query: "liability clauses for data breaches"
→ Finds relevant sections across thousands of contracts
```

### 4. **Research Paper Discovery**
Enable researchers to find relevant papers based on concepts, not just keywords.

```
Query: "neural networks for time series forecasting"
→ Returns papers about LSTMs, Transformers for sequences, etc.
```

### 5. **E-commerce Product Search**
Improve product discovery with semantic search that understands user intent.

```
Query: "comfortable shoes for standing all day"
→ Returns: nursing shoes, chef clogs, orthopedic footwear
```

### 6. **Code Search & Documentation**
Search codebases and documentation semantically.

```
Query: "how to handle authentication errors"
→ Returns: auth middleware, error handlers, retry logic
```

---

## Key Architectural Benefits

### 🔄 **Zero-Downtime Updates**
- Airflow builds new indexes atomically
- Pointer file swap ensures readers never see partial data
- Old indexes kept for rollback

### 🛡️ **Disaster Recovery**
- S3 backup contains full data with embeddings
- Can query S3 directly if primary index fails
- Partitioned by date for efficient historical queries

### 📈 **Scalability**
- DuckDB handles millions of vectors efficiently
- S3 storage is virtually unlimited
- HNSW index scales logarithmically

### 🔌 **Extensibility**
- Swap embedding models easily
- Add new data sources via Airflow DAGs
- Integrate with any LLM for RAG

---

## Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Source    │────▶│   Airflow   │────▶│   DuckDB    │────▶│   FastAPI   │
│   Data      │     │   DAG       │     │   + HNSW    │     │   /search   │
└─────────────┘     └──────┬──────┘     └─────────────┘     └─────────────┘
                           │
                           │ Backup
                           ▼
                    ┌─────────────┐     ┌─────────────┐
                    │    MinIO    │────▶│   FastAPI   │
                    │   Parquet   │     │  /search/s3 │
                    └─────────────┘     └─────────────┘
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Vector DB** | DuckDB + VSS | Vector storage and HNSW indexing |
| **Embeddings** | sentence-transformers | all-MiniLM-L6-v2 (384 dimensions) |
| **API** | FastAPI | RESTful search endpoints |
| **Orchestration** | Airflow | Scheduled reindexing and backups |
| **Object Storage** | MinIO (S3) | Durable parquet backup |
| **Frontend** | Next.js | Search UI with document indexing |

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with embedding model status |
| `/status` | GET | Index status, document count, vector index info |
| `/index` | POST | Index single document with auto-embedding |
| `/index/batch` | POST | Batch index multiple documents |
| `/search` | POST | Vector search using HNSW index |
| `/search/s3` | POST | Direct S3 vector search (brute-force) |
| `/s3/list` | GET | List documents in S3 backup |
| `/documents/{id}` | DELETE | Delete a document by ID |
| `/documents` | DELETE | Clear all documents |
| `/index/current` | GET | Current active index information |
| `/embed` | POST | Generate embedding for text (utility) |

---

## Getting Started

```bash
# Start the stack
docker compose up -d

# Index a document
curl -X POST http://localhost:8000/index \
  -H "Content-Type: application/json" \
  -d '{"id": "doc-1", "content": "Your document content here"}'

# Search (vector)
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "your search query"}'

# Trigger reindex DAG
# Visit http://localhost:8080 (airflow/airflow)
```

---

## Summary

This RAG stack provides a **complete, production-ready infrastructure** for semantic search:

- **Fast production queries** via HNSW-indexed DuckDB
- **Resilient fallback** via S3 parquet direct queries
- **Automatic embedding generation** for both indexing and search
- **Scheduled maintenance** via Airflow DAGs
- **Beautiful UI** for testing and document management

The dual-path architecture ensures you're never locked out of your data while maintaining the performance needed for real-time applications.

