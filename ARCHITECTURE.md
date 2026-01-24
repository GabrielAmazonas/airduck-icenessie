# Airduck-IceNessie: A Modern Data Stack for Vector Search & RAG

## Executive Summary

**Airduck-IceNessie** is a proof-of-concept demonstrating how modern open-source data infrastructure can power vector search and Retrieval-Augmented Generation (RAG) workloads without relying on proprietary vector databases or cloud-specific services.

This architecture combines:
- **DuckDB** as an embedded analytical database with native vector search (optional HNSW)
- **Apache Iceberg** for open table format data lakehouse semantics
- **Trino** as a federated query engine with vector search capability
- **dbt** for data transformation (Bronze → Silver → Gold medallion architecture)
- **Apache Airflow** for orchestration
- **MinIO** as S3-compatible object storage

### Why This Matters for Data Organizations

| Challenge | Traditional Approach | This Architecture |
|-----------|---------------------|-------------------|
| Vector Search | Proprietary DBs (Pinecone, Weaviate) | DuckDB + Trino (open source, flexible) |
| Data Lakehouse | Databricks/Snowflake lock-in | Iceberg + Nessie (open formats) |
| ETL/ELT | Separate tools, fragmented lineage | dbt on Trino (unified transforms) |
| Cost at Scale | Per-query pricing, egress fees | Self-hosted, predictable costs |
| Portability | Vendor lock-in | Fully containerized, cloud-agnostic |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              PRESENTATION LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐     ┌─────────────────┐                                    │
│  │   Next.js UI    │────▶│  FastAPI + RAG  │◀──── Vector Search API            │
│  │    (3000)       │     │     (8000)      │      /search, /search/iceberg     │
│  └─────────────────┘     └────────┬────────┘                                    │
│                                   │                                              │
├───────────────────────────────────┼──────────────────────────────────────────────┤
│                        VECTOR SEARCH LAYER                                       │
├───────────────────────────────────┼──────────────────────────────────────────────┤
│                                   ▼                                              │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │                         SEARCH PATH SELECTION                               │ │
│  │  ┌─────────────────────────┐       ┌─────────────────────────────────────┐ │ │
│  │  │      DuckDB (EFS)       │       │         Trino → Iceberg             │ │ │
│  │  │ ┌─────────────────────┐ │       │  • Cosine similarity in SQL         │ │ │
│  │  │ │ Brute Force (O(n))  │ │       │  • Query Gold layer embeddings      │ │ │
│  │  │ │ array_cosine_sim()  │ │       │  • Scales with Trino workers        │ │ │
│  │  │ ├─────────────────────┤ │       │  • Source of truth queries          │ │ │
│  │  │ │ HNSW Index (O(logn))│ │       └─────────────────────────────────────┘ │ │
│  │  │ │ array_cosine_dist() │ │                                               │ │
│  │  │ │ (optional, via DAG) │ │                                               │ │
│  │  │ └─────────────────────┘ │                                               │ │
│  │  └─────────────────────────┘                                               │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                   ▲                                              │
│                                   │ Pointer-based atomic swap                    │
│                                   │ (zero-downtime updates)                      │
├───────────────────────────────────┼──────────────────────────────────────────────┤
│                           ORCHESTRATION                                          │
├───────────────────────────────────┼──────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                          Apache Airflow                                  │    │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │    │
│  │  │ daily_reindex    │  │ embed_iceberg    │  │ dbt_transforms       │   │    │
│  │  │ • Build HNSW     │  │ • Gen embeddings │  │ • Bronze → Silver    │   │    │
│  │  │ • S3 Backup      │  │ • Update Iceberg │  │ • Silver → Gold      │   │    │
│  │  │ • Atomic swap    │  │ • Verify coverage│  │ • Quality scoring    │   │    │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                           DATA LAKEHOUSE                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐   │
│  │     Trino      │──│     Nessie      │──│         MinIO (S3)              │   │
│  │   (8085)       │  │ Iceberg Catalog │  │  ┌───────────────────────────┐  │   │
│  │                │  │    (19120)      │  │  │ iceberg-warehouse/        │  │   │
│  │ dbt-trino      │  │                 │  │  │   bronze/ silver/ gold/   │  │   │
│  │ adapter        │  │ Branch: main    │  │  ├───────────────────────────┤  │   │
│  └────────────────┘  └─────────────────┘  │  │ rag-data/                 │  │   │
│                                           │  │   backup/documents/       │  │   │
│                                           │  └───────────────────────────┘  │   │
│                                           └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Deep Dive

### 1. Vector Search Layer

The architecture provides **three search paths** optimized for different use cases:

#### Path A: DuckDB Brute Force (Default)

```sql
-- O(n) scan with cosine similarity
SELECT id, content, 
       array_cosine_similarity(embedding, ?::FLOAT[384]) as score
FROM documents
WHERE embedding IS NOT NULL
ORDER BY score DESC
LIMIT 10;
```

**Use when:** Real-time indexing needed, dataset <100k vectors, simplicity preferred.

#### Path B: DuckDB HNSW Index (Optional)

```sql
-- O(log n) approximate nearest neighbor
SELECT id, content,
       1 - array_cosine_distance(embedding, ?::FLOAT[384]) as score
FROM documents
WHERE embedding IS NOT NULL
ORDER BY array_cosine_distance(embedding, ?::FLOAT[384])
LIMIT 10;
```

**Use when:** Dataset >100k vectors, batch indexing acceptable, low latency required.

**Enabling HNSW:**
1. Run `daily_reindex` DAG to build the HNSW index
2. Set `use_hnsw=true` in search requests

#### Path C: Trino → Iceberg (Data Lake Query)

```sql
-- Cosine similarity computed in Trino SQL
SELECT id, content,
    reduce(zip_with(embedding, query_vec, (a,b) -> a*b), 0.0, (s,x) -> s+x, s -> s)
    / NULLIF(sqrt(...) * sqrt(...), 0.0) as similarity
FROM iceberg.silver_gold.gold_documents
WHERE embedding IS NOT NULL
ORDER BY similarity DESC
LIMIT 10;
```

**Use when:** Querying source of truth, audit requirements, distributed compute needed.

### 2. Data Lakehouse (Iceberg + Nessie + Trino)

**Medallion Architecture with Embeddings**

| Layer | Purpose | Embeddings | Location |
|-------|---------|------------|----------|
| **Bronze** | Raw ingestion | ❌ | `iceberg.bronze.raw_documents` |
| **Silver** | Deduplicated, cleaned | ❌ | `iceberg.silver_silver.silver_documents` |
| **Gold** | Chunked, quality-scored | ✅ | `iceberg.silver_gold.gold_documents` |

The `embed_iceberg_gold` DAG generates embeddings and writes them directly to the Iceberg Gold layer, enabling vector search via Trino.

### 3. Embedding Pipeline

**Embedding Model**: `all-MiniLM-L6-v2` (384 dimensions)

| Metric | Value | Notes |
|--------|-------|-------|
| Model file size | ~22 MB | Small enough for container images |
| Parameters | ~22 million | Distilled from larger models |
| Embedding dimension | 384 | Good for cosine similarity search |
| Container memory | ~370 MB | Total FastAPI container footprint |
| Inference speed | ~5ms/query | CPU-only, no GPU required |

**Three Embedding Paths:**

1. **Real-time (FastAPI)**: Embeddings generated immediately on `/index`
2. **Batch to DuckDB (Airflow)**: `daily_reindex` DAG generates for HNSW index
3. **Batch to Iceberg (Airflow)**: `embed_iceberg_gold` DAG writes to Gold layer

**Trino Array Limit Workaround:**

Trino's `ARRAY[]` constructor has a limit on arguments. For 384-dim vectors, we use:

```sql
-- Instead of ARRAY[v1, v2, ...] (fails with 384 elements)
CAST(json_parse('[0.1, 0.2, ...]') AS ARRAY(REAL))
```

### 4. Storage Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                        MinIO (S3-Compatible)                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  iceberg-warehouse/                  rag-data/                  │
│  ├── bronze/                         └── backup/                │
│  │   └── raw_documents/                  └── documents/         │
│  │       └── data/*.parquet                  └── year=2026/     │
│  ├── silver_silver/                              └── month=1/   │
│  │   └── silver_documents/                           └── day=23/│
│  │       └── data/*.parquet                              └── *.parquet
│  └── silver_gold/                                                │
│      └── gold_documents/                                         │
│          └── data/*.parquet  (includes 384-dim embeddings)      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5. Iceberg Table Maintenance (Small Files Problem)

**The Problem:** Frequent writes, updates, and streaming ingestion create many small files:

| Root Cause | Result |
|------------|--------|
| Frequent INSERTs | 1 file per write batch |
| UPDATE statements | Copy-on-write creates new files |
| Many partitions | Few rows per partition file |
| Streaming ingestion | Micro-batches = micro-files |

**Impact on Performance:**

| Files per Table | Query Planning | S3 API Calls | Query Time |
|-----------------|----------------|--------------|------------|
| 10 | Fast | Low | Fast |
| 1,000 | Slow | High | 10-100x slower |
| 10,000+ | Very slow | Very high | Unusable |

**Solution: Iceberg Maintenance Procedures**

The `iceberg_maintenance` DAG runs these procedures:

```sql
-- 1. Compact small files into target size (128MB)
ALTER TABLE iceberg.silver_gold.gold_documents EXECUTE optimize
WHERE file_size_in_bytes < 134217728;

-- Alternative for older Trino versions:
CALL iceberg.system.rewrite_data_files(
    table => 'iceberg.silver_gold.gold_documents'
);

-- 2. Expire old snapshots (enables garbage collection)
CALL iceberg.system.expire_snapshots(
    table => 'iceberg.silver_gold.gold_documents',
    older_than => TIMESTAMP '2026-01-17 00:00:00',
    retain_last => 2
);

-- 3. Remove orphan files (unreferenced data)
CALL iceberg.system.remove_orphan_files(
    table => 'iceberg.silver_gold.gold_documents',
    older_than => TIMESTAMP '2026-01-21 00:00:00'
);
```

**Maintenance Schedule:**

| DAG | Compaction | When |
|-----|------------|------|
| `dbt_manual_transforms` | After dbt run | On transform |
| `embed_iceberg_gold` | After embedding update | On embedding |
| `iceberg_maintenance` | Full optimization | Daily 3 AM |

**Target File Sizes:**

| File Size | Assessment | Action |
|-----------|------------|--------|
| < 10 MB | Too small | Compact immediately |
| 10-100 MB | Acceptable | Compact opportunistically |
| 100-256 MB | Optimal | No action needed |
| > 256 MB | Large but OK | May slow writes |

---

## Scaling Analysis: A Principal Engineer's Perspective

> **Design Principle:** Storage is cheap, compute is expensive. Optimize for compute efficiency at the cost of storage redundancy.

### Understanding Documents vs. Vectors

A critical distinction: **source documents ≠ vectors**. The Gold layer chunks documents before embedding.

**Vector Storage Math:**
- 384 dimensions × 4 bytes (float32) = **1,536 bytes per vector** (~1.5 KB)
- Plus metadata, content, overhead ≈ **2-3 KB per vector** in practice

**Realistic Enterprise Knowledge Base Scenario:**

| Document Type | Avg Size | Chunk Size | Vectors/Doc | Distribution |
|---------------|----------|------------|-------------|--------------|
| Chat messages | 200 chars | 500 chars | 1 | 40% |
| Support tickets | 1,500 chars | 500 chars | 3 | 25% |
| KB articles | 5,000 chars | 500 chars | 10 | 20% |
| Product docs | 15,000 chars | 500 chars | 30 | 10% |
| PDFs/Manuals | 100,000 chars | 500 chars | 200 | 5% |

**Weighted average: ~12 vectors per source document**

This means:
- 100K source documents → **1.2M vectors**
- 1M source documents → **12M vectors**
- 10M source documents → **120M vectors**

### Current Architecture Limits

| Component | Document Limit | Vector Limit | Bottleneck |
|-----------|----------------|--------------|------------|
| DuckDB (single node) | ~800K docs | ~10M vectors | Memory for HNSW graph |
| Trino brute force | ~4M docs | ~50M vectors | CPU time per query |
| Embedding generation | ~80 docs/min | ~1000 chunks/min | CPU inference |

### Scaling Strategies

#### Phase 1: Vertical Scaling (0 → 800K documents / 10M vectors)

**Current architecture handles this well.** Optimizations:

```yaml
# Increase Trino resources
trino:
  environment:
    - JAVA_TOOL_OPTIONS=-Xmx16g  # More heap for larger queries

# Add memory for DuckDB HNSW
fastapi:
  deploy:
    resources:
      limits:
        memory: 8G  # HNSW graph ~1GB for 10M vectors
```

**Cost model (800K documents / 10M vectors):**
- Storage: ~25GB vectors + ~10GB content (~$0.80/month on S3)
- Compute: 1 node with 8GB RAM (~$60/month)
- Embedding backlog: 800K docs ÷ 80 docs/min = **~7 days** initial indexing

**Realistic startup scenario:**
| Metric | Value |
|--------|-------|
| Source documents | 800,000 |
| Total vectors | 10,000,000 |
| Storage (S3) | 35 GB |
| Memory (HNSW) | 8 GB |
| Initial embed time | 7 days (CPU) / 4 hours (GPU) |

#### Phase 2: Horizontal Read Scaling (800K → 8M documents / 100M vectors)

**Strategy: Read replicas + query routing**

```
                    ┌─────────────────────────────────────┐
                    │          Load Balancer              │
                    └──────────────┬──────────────────────┘
                                   │
           ┌───────────────────────┼───────────────────────┐
           ▼                       ▼                       ▼
    ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
    │ FastAPI R1  │         │ FastAPI R2  │         │ FastAPI R3  │
    │ DuckDB (RO) │         │ DuckDB (RO) │         │ DuckDB (RO) │
    └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
           │                       │                       │
           └───────────────────────┴───────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │     Shared EFS / S3 Storage         │
                    │     (Same DuckDB file, read-only)   │
                    └─────────────────────────────────────┘
```

**Implementation:**
- DuckDB supports concurrent readers on same file
- Each replica loads HNSW graph into memory (~10GB per 100M vectors)
- Airflow remains single writer with atomic pointer swap

**Cost model (8M documents / 100M vectors):**
- Storage: ~250GB vectors + ~100GB content (~$8/month on S3) - **CHEAP**
- Compute: 3 replicas × 16GB RAM = 48GB (~$360/month) - **EXPENSIVE**
- **Trade-off:** Pay for memory N times to get N× throughput

**Realistic mid-scale scenario:**
| Metric | Value |
|--------|-------|
| Source documents | 8,000,000 |
| Total vectors | 100,000,000 |
| Storage (S3) | 350 GB |
| Memory (per replica) | 16 GB |
| Replicas needed | 3 (for 1000 QPS) |
| Monthly cost | ~$400 |

#### Phase 3: Data Sharding (8M → 80M documents / 1B vectors)

**Strategy: Shard by partition key, fan-out queries**

```
┌─────────────────────────────────────────────────────────────────┐
│                         Query Router                             │
│   1. Receive query                                               │
│   2. Generate embedding once                                     │
│   3. Fan-out to all shards in parallel                          │
│   4. Merge top-k from each shard                                │
│   5. Re-rank and return global top-k                            │
└───────────┬─────────────────┬─────────────────┬─────────────────┘
            │                 │                 │
            ▼                 ▼                 ▼
     ┌────────────┐    ┌────────────┐    ┌────────────┐
     │  Shard 0   │    │  Shard 1   │    │  Shard 2   │
     │  ~27M docs │    │  ~27M docs │    │  ~27M docs │
     │  ~333M vec │    │  ~333M vec │    │  ~333M vec │
     │ DuckDB/EFS │    │ DuckDB/EFS │    │ DuckDB/EFS │
     └────────────┘    └────────────┘    └────────────┘
```

**Sharding strategies:**

| Strategy | Pros | Cons |
|----------|------|------|
| Hash(doc_id) | Even distribution | Cross-shard for related docs |
| Range(timestamp) | Time-based queries fast | Uneven if bursty |
| Tenant ID | Multi-tenant isolation | Varying shard sizes |
| Document type | Optimize chunk ratios per shard | Requires routing logic |

**Query router implementation:**

```python
async def search_sharded(query: str, top_k: int):
    embedding = generate_embedding(query)
    
    # Fan-out to all shards in parallel
    tasks = [
        query_shard(shard_url, embedding, top_k)
        for shard_url in SHARD_URLS
    ]
    shard_results = await asyncio.gather(*tasks)
    
    # Merge and re-rank
    all_results = flatten(shard_results)
    return sorted(all_results, key=lambda x: x.score, reverse=True)[:top_k]
```

**Cost model (80M documents / 1B vectors):**
- Storage: ~2.5TB vectors + ~1TB content (~$80/month on S3) - **CHEAP**
- Compute: 10 shards × 16GB memory = 160GB RAM (~$1,200/month) - **EXPENSIVE**
- **Trade-off:** Linear storage cost, linear compute cost, but queries parallelize

**Realistic large-scale scenario:**
| Metric | Value |
|--------|-------|
| Source documents | 80,000,000 |
| Total vectors | 1,000,000,000 |
| Storage (S3) | 3.5 TB |
| Shards | 10 |
| Memory (per shard) | 16 GB |
| Monthly cost | ~$1,300 |
| Query latency (HNSW) | ~20ms |

#### Phase 4: Tiered Architecture (80M+ documents / 1B+ vectors, cost-optimized)

**Strategy: Hot/warm/cold tiers with different search characteristics**

```
┌─────────────────────────────────────────────────────────────────┐
│                          HOT TIER                                │
│  Recent 30 days: ~8M docs / ~100M vectors                       │
│  DuckDB + HNSW, <10ms latency, memory-resident                  │
│  Cost: High compute, low storage                                │
└───────────────────────────────┬─────────────────────────────────┘
                                │ Cache miss
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          WARM TIER                               │
│  30-365 days: ~40M docs / ~500M vectors                         │
│  Trino → Iceberg, ~500ms latency, compute-on-demand             │
│  Cost: Pay per query, storage-optimized                         │
└───────────────────────────────┬─────────────────────────────────┘
                                │ Rare access
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          COLD TIER                               │
│  >1 year: ~750M docs / ~9B vectors                              │
│  S3 + Glacier, seconds latency, batch queries only              │
│  Cost: $0.004/GB/month storage                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Routing logic:**

```python
def route_query(query: str, time_range: Optional[DateRange]):
    if time_range is None or time_range.end > days_ago(30):
        return search_hot_tier(query)  # DuckDB HNSW
    elif time_range.end > days_ago(365):
        return search_warm_tier(query)  # Trino Iceberg
    else:
        return search_cold_tier(query)  # Batch S3 scan
```

**Cost model at 800M documents / 10B vectors:**

| Tier | Documents | Vectors | Storage | Compute | Total |
|------|-----------|---------|---------|---------|-------|
| Hot | 8M | 100M | $8/mo | $500/mo (2×32GB) | $508 |
| Warm | 40M | 500M | $45/mo | $100/mo (on-demand) | $145 |
| Cold | 750M | 9.4B | $150/mo (Glacier) | $0 (batch) | $150 |
| **Total** | **800M** | **10B** | **$203/mo** | **$600/mo** | **$803** |

Vs. managed vector DB at this scale:
- **Pinecone**: ~$50,000+/month (p2 pods for 10B vectors)
- **Weaviate Cloud**: ~$25,000+/month
- **This architecture**: ~$800/month (**98% savings**)

**Realistic enterprise scenario (800M documents):**
| Metric | Value |
|--------|-------|
| Source documents | 800,000,000 |
| Total vectors | 10,000,000,000 |
| Hot tier storage | 350 GB |
| Warm tier storage | 1.8 TB |
| Cold tier storage | 35 TB |
| Hot tier memory | 64 GB (2 nodes) |
| Monthly cost | ~$800 |
| Hot query latency | <10ms |
| Warm query latency | ~500ms |

### Compute Optimization Strategies

Since **compute is expensive**, optimize aggressively:

#### 1. Embedding Caching

```python
# Cache embeddings by content hash
@lru_cache(maxsize=100000)
def get_cached_embedding(content_hash: str) -> List[float]:
    return generate_embedding(content_by_hash[content_hash])
```

**Impact:** Avoid re-embedding duplicate or similar queries.

#### 2. Approximate Search Everywhere

| Documents | Vectors | Brute Force | HNSW | Savings |
|-----------|---------|-------------|------|---------|
| ~80K | 1M | 500ms | 5ms | 99% |
| ~800K | 10M | 5s | 10ms | 99.8% |
| ~8M | 100M | 50s | 15ms | 99.97% |

**Trade-off:** <5% recall loss for 99%+ compute savings.

#### 3. Query Result Caching

```python
# Cache search results by query embedding hash
RESULT_CACHE_TTL = 3600  # 1 hour

async def search_with_cache(query: str, top_k: int):
    cache_key = hash(tuple(generate_embedding(query)))
    
    cached = redis.get(cache_key)
    if cached:
        return cached
    
    results = await search_backend(query, top_k)
    redis.setex(cache_key, RESULT_CACHE_TTL, results)
    return results
```

**Impact:** Popular queries served from cache, zero compute.

#### 4. Batch Embedding Generation

```python
# GPU batch processing (if available)
# 32 docs at once vs 1 at a time = 10x throughput

embeddings = model.encode(
    documents,
    batch_size=32,
    show_progress_bar=True,
    device='cuda'  # or 'cpu' with threading
)
```

**Impact:** Amortize model loading, maximize throughput.

### Scaling Decision Matrix

| Documents | Vectors | QPS | Strategy | Monthly Cost |
|-----------|---------|-----|----------|--------------|
| <80K | <1M | <10 | Single DuckDB (brute force) | ~$50 |
| 80K-800K | 1-10M | <100 | Single DuckDB + HNSW | ~$100 |
| 800K-8M | 10-100M | <1000 | Read replicas (3 nodes) | ~$400 |
| 8M-80M | 100M-1B | <10000 | Sharded DuckDB (10 shards) | ~$1,300 |
| 80M-800M | 1B-10B | Any | Tiered + Sharded | ~$800-2,000 |

**Note:** Document counts assume ~12 vectors per document (weighted average across document types).

### Recommended Scaling Path

1. **Start simple**: Single DuckDB with brute force (<80K docs)
2. **Add HNSW** when P95 latency exceeds 100ms (~80K docs)
3. **Add read replicas** when throughput limits hit (~800K docs)
4. **Shard** when memory per replica exceeds 32GB (~8M docs)
5. **Tier** when storage costs approach compute costs (~80M+ docs)

---

## Trade-offs & Considerations

### What This Architecture Does Well

✅ Sub-second vector search at moderate scale (up to ~800K documents / 10M vectors)
✅ Full control over data and infrastructure
✅ Open formats (Parquet, Iceberg) prevent lock-in
✅ Familiar tools for data engineers (dbt, Airflow, SQL)
✅ Zero-downtime index updates
✅ Multiple search paths (DuckDB, Trino, S3)
✅ Storage-optimized design (cheap at scale)

### Where It Has Limitations

⚠️ **Horizontal scaling**: Requires manual sharding beyond 100M vectors
⚠️ **Real-time streaming**: Batch-oriented; for true streaming, add Kafka + Flink
⚠️ **GPU acceleration**: Embedding generation is CPU-bound; add GPU workers for throughput
⚠️ **Managed services**: No built-in HA/DR; requires ops investment

### When to Use This vs. Managed Services

| Use Case | Recommendation |
|----------|----------------|
| POC / Prototype | ✅ This architecture |
| <800K docs / 10M vectors, cost-sensitive | ✅ This architecture |
| Storage >> Compute budget | ✅ This architecture |
| >8M docs / 100M vectors, zero ops | ⚠️ Consider Milvus, Qdrant |
| Strict SLAs (<5ms p99) | ⚠️ Consider dedicated vector DB |
| No engineering capacity | ⚠️ Consider managed services |

---

## Production Readiness Checklist

To move from POC to production:

- [ ] **Nessie**: Replace in-memory with persistent backend (JDBC/RocksDB)
- [ ] **Auth**: Enable Nessie authentication, add API keys for FastAPI
- [ ] **HA**: Deploy Airflow with CeleryExecutor + Redis, multiple workers
- [ ] **Monitoring**: Add Prometheus metrics, Grafana dashboards
- [ ] **Backup**: Configure cross-region replication for MinIO
- [ ] **Embedding**: Add GPU workers for batch embedding generation
- [ ] **Index**: Tune HNSW parameters (M, ef_construction) for scale
- [ ] **Caching**: Add Redis for query result caching
- [ ] **Testing**: Add integration tests, load tests

---

## Getting Started

```bash
# Clone and start
git clone <repo>
cd airduck-icenessie
docker compose up -d

# Access points
# - Frontend: http://localhost:3000
# - API: http://localhost:8000
# - Airflow: http://localhost:8080 (airflow/airflow)
# - Trino: http://localhost:8085
# - MinIO: http://localhost:9001 (minioadmin/minioadmin)
```

---

## References

- [DuckDB VSS Extension](https://duckdb.org/docs/extensions/vss)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Project Nessie](https://projectnessie.org/)
- [dbt-trino](https://github.com/starburstdata/dbt-trino)
- [HNSW Algorithm Paper](https://arxiv.org/abs/1603.09320)
- [Scaling Vector Search (Pinecone Engineering)](https://www.pinecone.io/learn/vector-database/)

---

*Architecture documented: January 2026*
*Last updated: 2026-01-24*
*Scaling analysis assumes ~12 vectors per document (enterprise knowledge base mix)*
