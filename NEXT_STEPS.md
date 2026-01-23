# Next Steps: Airduck-IceNessie Platform Roadmap

This document outlines future enhancements and production-readiness tasks for the Airduck-IceNessie data platform.

---

## Current State (Implemented)

The POC currently includes:

| Component | Status | Description |
|-----------|--------|-------------|
| MinIO | ✅ Running | S3-compatible object storage |
| DuckDB + VSS | ✅ Running | Embedded vector database with HNSW |
| FastAPI | ✅ Running | Search API with embedding generation |
| Next.js Frontend | ✅ Running | Document management UI |
| Airflow | ✅ Running | DAG orchestration |
| Nessie | ✅ Running | Iceberg catalog (no auth for POC) |
| Trino | ✅ Running | Query engine for dbt |
| dbt | ✅ Running | Bronze → Silver → Gold transforms |
| PostgreSQL | ✅ Running | Airflow metadata store |

---

## Phase 1: Production Hardening

### 1.1 Nessie Persistence

Current: In-memory storage (data lost on restart)

**Target:** Persistent backend

```yaml
# docker-compose.yml - Updated Nessie config
nessie:
  environment:
    NESSIE_VERSION_STORE_TYPE: JDBC
    QUARKUS_DATASOURCE_DB_KIND: postgresql
    QUARKUS_DATASOURCE_USERNAME: nessie
    QUARKUS_DATASOURCE_PASSWORD: nessie_secret
    QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://postgres:5432/nessie
```

Tasks:
- [ ] Add dedicated Nessie PostgreSQL database
- [ ] Configure JDBC version store
- [ ] Test catalog persistence across restarts
- [ ] Implement backup strategy for Nessie metadata

### 1.2 Authentication & Security

Current: No authentication (POC mode)

**Target:** Secured endpoints

Tasks:
- [ ] Enable Nessie authentication (`NESSIE_SERVER_AUTHENTICATION_ENABLED=true`)
- [ ] Add API key authentication to FastAPI
- [ ] Configure Trino user authentication
- [ ] Set up MinIO IAM policies
- [ ] Rotate default credentials

### 1.3 High Availability

Current: Single container per service

**Target:** Resilient deployment

Tasks:
- [ ] Airflow: CeleryExecutor with Redis + multiple workers
- [ ] PostgreSQL: Primary-replica setup
- [ ] MinIO: Distributed mode (4+ nodes)
- [ ] DuckDB: Index replication to standby

---

## Phase 2: Performance Optimization

### 2.1 GPU-Accelerated Embeddings

Current: CPU-based embedding generation (~100 docs/sec)

**Target:** GPU acceleration (~10k docs/sec)

```python
# Example: GPU worker with sentence-transformers
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2", device="cuda")
```

Tasks:
- [ ] Add GPU worker container (NVIDIA runtime)
- [ ] Implement async embedding queue (Redis/RabbitMQ)
- [ ] Batch embedding requests
- [ ] Add embedding cache layer

### 2.2 HNSW Index Tuning

Current: Default HNSW parameters

**Target:** Tuned for scale

```sql
-- Example: Tuned HNSW for 1M+ vectors
CREATE INDEX embedding_idx ON documents 
USING HNSW (embedding) 
WITH (
    metric = 'cosine',
    M = 32,                    -- More connections (default: 16)
    ef_construction = 200,     -- Higher build quality (default: 100)
    ef = 100                   -- Search quality (default: 10)
);
```

Tasks:
- [ ] Benchmark with production-scale data
- [ ] Tune M and ef_construction based on recall requirements
- [ ] Implement index sharding for >10M vectors

### 2.3 Trino Performance

Current: Single coordinator

**Target:** Distributed query engine

Tasks:
- [ ] Add Trino worker nodes
- [ ] Configure query memory limits
- [ ] Enable result caching
- [ ] Optimize Iceberg table compaction

---

## Phase 3: Observability

### 3.1 Metrics & Dashboards

Tasks:
- [ ] Deploy Prometheus
- [ ] Add Grafana dashboards:
  - Vector search latency (p50, p95, p99)
  - Embedding generation throughput
  - Index size and document count
  - dbt run duration and failures
  - Airflow DAG success rates
- [ ] Configure alerting rules

### 3.2 Logging & Tracing

Tasks:
- [ ] Centralized logging (Loki or ELK)
- [ ] Distributed tracing (Jaeger/Tempo)
- [ ] Query auditing for compliance

---

## Phase 4: Advanced Features

### 4.1 DuckDB FlightSQL Service

Extract DuckDB from embedded mode to shared service.

```
┌─────────────────┐     ┌─────────────────┐
│    FastAPI      │     │    Airflow      │
│  (FlightSQL)    │     │  (FlightSQL)    │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
            ┌─────────────────┐
            │  DuckDB Service │
            │  (FlightSQL)    │
            └─────────────────┘
```

Tasks:
- [ ] Implement DuckDB FlightSQL server
- [ ] Update FastAPI to use ADBC client
- [ ] Hot-reload index without service restart

### 4.2 Real-Time Ingestion

Current: Batch-only ingestion

**Target:** Stream processing

```
Sources → Kafka → Flink → Bronze (Iceberg) → dbt → Gold → Vector Index
```

Tasks:
- [ ] Add Kafka for event streaming
- [ ] Implement Flink jobs for Bronze ingestion
- [ ] Configure incremental dbt models
- [ ] Near-real-time index updates

### 4.3 Hybrid Search

Current: Vector-only search

**Target:** Combined vector + keyword + filters

```sql
SELECT * FROM documents
WHERE 
    array_cosine_similarity(embedding, ?) > 0.7
    AND content ILIKE '%keyword%'
    AND metadata->>'category' = 'technical'
ORDER BY similarity DESC
LIMIT 10;
```

Tasks:
- [ ] Add full-text search index (DuckDB FTS or Tantivy)
- [ ] Implement hybrid ranking (RRF or weighted)
- [ ] Metadata filter pushdown optimization

---

## Implementation Priority

| Priority | Task | Impact | Effort |
|----------|------|--------|--------|
| P0 | Nessie persistence | Critical for prod | Low |
| P0 | Authentication | Security requirement | Medium |
| P1 | Monitoring/alerting | Operational visibility | Medium |
| P1 | HNSW tuning | Performance | Low |
| P2 | GPU embeddings | Throughput | High |
| P2 | FlightSQL service | Architecture | High |
| P3 | Streaming ingestion | Real-time capability | High |
| P3 | Hybrid search | Feature | Medium |

---

## Reference Architecture (Target State)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              PRODUCTION PLATFORM                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐│
│  │   Kafka     │  │   Flink     │  │   Airflow   │  │    Prometheus/Grafana   ││
│  │  (Ingest)   │──│  (Stream)   │──│   (Batch)   │  │      (Monitoring)       ││
│  └─────────────┘  └──────┬──────┘  └──────┬──────┘  └─────────────────────────┘│
│                          │                │                                      │
│                          ▼                ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                    Nessie (JDBC) + Trino Cluster                          │ │
│  │                    ┌─────────────────────────────────────────────────┐    │ │
│  │                    │   Bronze → Silver → Gold (Iceberg Tables)       │    │ │
│  │                    └─────────────────────────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                          │
│                                      ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────────────┐ │
│  │                      DuckDB FlightSQL Service                             │ │
│  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │ │
│  │   │ GPU Embed   │  │ HNSW Index  │  │ FTS Index   │  │ Hybrid Search   │  │ │
│  │   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────────┘  │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                      │                                          │
│                          ┌───────────┴───────────┐                             │
│                          ▼                       ▼                             │
│                   ┌──────────────┐       ┌──────────────┐                      │
│                   │   FastAPI    │       │   Frontend   │                      │
│                   │    (LB)      │       │    (CDN)     │                      │
│                   └──────────────┘       └──────────────┘                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

*Last updated: 2026-01-23*
