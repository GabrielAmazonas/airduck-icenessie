"""
FastAPI + DuckDB Vector Search Engine

Uses a pointer-based index loading pattern:
- Airflow builds new indexes and atomically swaps a pointer file
- FastAPI reads the pointer to find the current index
- This enables zero-downtime index updates

Embedding generation:
- Uses sentence-transformers to generate embeddings
- Embeddings are generated on indexing and stored in DuckDB
- Search queries are also embedded for vector similarity
"""
import json
import os
from contextlib import asynccontextmanager
from typing import List, Optional

import duckdb
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer


# ============================================================================
# Configuration
# ============================================================================

EFS_PATH = os.getenv("EFS_PATH", "/mnt/efs")
POINTER_FILE = os.path.join(EFS_PATH, "latest_pointer.txt")
FALLBACK_DB = os.path.join(EFS_PATH, "vector_store.duckdb")

# Embedding model configuration
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 produces 384-dim embeddings

# S3 Configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000").replace("http://", "").replace("https://", "")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_BUCKET = "rag-data"

# Global embedding model (loaded on startup)
embedding_model: Optional[SentenceTransformer] = None


# ============================================================================
# Helper Functions
# ============================================================================

def parse_metadata(metadata_value) -> Optional[dict]:
    """Parse metadata from DuckDB which may be a string or dict."""
    if metadata_value is None:
        return None
    if isinstance(metadata_value, dict):
        return metadata_value
    if isinstance(metadata_value, str):
        try:
            return json.loads(metadata_value)
        except (json.JSONDecodeError, TypeError):
            return {"raw": metadata_value}
    return None


def generate_embedding(text: str) -> List[float]:
    """Generate embedding for a text using the loaded model."""
    global embedding_model
    if embedding_model is None:
        raise HTTPException(status_code=503, detail="Embedding model not loaded")
    
    embedding = embedding_model.encode(text, convert_to_numpy=True)
    return embedding.tolist()


def generate_embeddings_batch(texts: List[str]) -> List[List[float]]:
    """Generate embeddings for multiple texts."""
    global embedding_model
    if embedding_model is None:
        raise HTTPException(status_code=503, detail="Embedding model not loaded")
    
    embeddings = embedding_model.encode(texts, convert_to_numpy=True)
    return [emb.tolist() for emb in embeddings]


# ============================================================================
# Models
# ============================================================================

class Document(BaseModel):
    """Document model for indexing."""
    id: str
    content: str
    metadata: Optional[dict] = None
    embedding: Optional[List[float]] = None  # Optional - will be auto-generated if not provided


class SearchQuery(BaseModel):
    """Search query model."""
    query: str
    top_k: int = 10
    use_vector: bool = True  # Use vector search by default
    use_hnsw: bool = False  # Use HNSW index if available (requires reindex DAG to have run)


class SearchResult(BaseModel):
    """Search result model."""
    id: str
    content: str
    score: float
    metadata: Optional[dict] = None


class IndexStatus(BaseModel):
    """Index status model."""
    total_documents: int
    last_updated: Optional[str] = None
    status: str
    index_file: Optional[str] = None
    has_vector_index: bool = False
    embedding_model: Optional[str] = None
    embedding_dim: int = EMBEDDING_DIM


class S3SearchQuery(BaseModel):
    """S3 search query model."""
    query: str
    top_k: int = 10
    path: str = "backup/documents"
    use_vector: bool = True  # Use vector search by default


# ============================================================================
# Database Connection
# ============================================================================

def get_current_index_path() -> str:
    """Get the path to the current active index."""
    if os.path.exists(POINTER_FILE):
        with open(POINTER_FILE, "r") as f:
            index_name = f.read().strip()
            index_path = os.path.join(EFS_PATH, index_name)
            if os.path.exists(index_path):
                return index_path
    return FALLBACK_DB


def get_db_connection(read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the current index."""
    db_path = get_current_index_path()
    os.makedirs(EFS_PATH, exist_ok=True)
    
    if not os.path.exists(db_path):
        conn = duckdb.connect(db_path)
        # Install and load VSS extension for vector operations
        conn.execute("INSTALL vss; LOAD vss;")
        init_database(conn)
        return conn
    
    conn = duckdb.connect(db_path, read_only=read_only)
    # Install and load VSS extension for vector operations
    conn.execute("INSTALL vss; LOAD vss;")
    return conn


def init_database(conn: duckdb.DuckDBPyConnection):
    """Initialize the database schema for fallback mode."""
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS documents (
            id VARCHAR PRIMARY KEY,
            content TEXT NOT NULL,
            embedding FLOAT[{EMBEDDING_DIM}],
            metadata JSON,
            source_file VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_id ON documents(id)")
    conn.commit()


def check_vector_index_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    """Check if HNSW vector index exists."""
    try:
        result = conn.execute("""
            SELECT COUNT(*) FROM duckdb_indexes() 
            WHERE index_name = 'embedding_idx'
        """).fetchone()
        return result[0] > 0 if result else False
    except Exception:
        return False


def get_s3_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection configured for S3 access."""
    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        INSTALL httpfs; LOAD httpfs;
        SET s3_endpoint='{S3_ENDPOINT}';
        SET s3_access_key_id='{S3_ACCESS_KEY}';
        SET s3_secret_access_key='{S3_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return conn


# ============================================================================
# Application Lifecycle
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global embedding_model
    
    # Startup: Load embedding model
    print(f"Loading embedding model: {EMBEDDING_MODEL}...")
    embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    print(f"Embedding model loaded. Dimension: {EMBEDDING_DIM}")
    
    # Ensure database exists
    os.makedirs(EFS_PATH, exist_ok=True)
    db_path = get_current_index_path()
    
    if not os.path.exists(db_path):
        conn = duckdb.connect(db_path)
        init_database(conn)
        conn.close()
    
    yield
    
    # Shutdown
    embedding_model = None


# Create FastAPI app
app = FastAPI(
    title="RAG Vector Search Engine",
    description="Vector search engine using DuckDB with HNSW index and automatic embedding generation",
    version="2.0.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    global embedding_model
    try:
        conn = get_db_connection()
        conn.execute("SELECT 1")
        conn.close()
        return {
            "status": "healthy",
            "service": "search-engine",
            "embedding_model_loaded": embedding_model is not None,
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/status", response_model=IndexStatus)
async def get_index_status():
    """Get the current index status."""
    conn = get_db_connection()
    try:
        result = conn.execute("SELECT COUNT(*) FROM documents").fetchone()
        count = result[0] if result else 0
        
        last_updated = conn.execute(
            "SELECT MAX(created_at) FROM documents"
        ).fetchone()
        last_updated_str = str(last_updated[0]) if last_updated and last_updated[0] else None
        
        has_vector_idx = check_vector_index_exists(conn)
        current_index = get_current_index_path()
        
        return IndexStatus(
            total_documents=count,
            last_updated=last_updated_str,
            status="ready" if has_vector_idx else "no_vector_index",
            index_file=os.path.basename(current_index),
            has_vector_index=has_vector_idx,
            embedding_model=EMBEDDING_MODEL,
            embedding_dim=EMBEDDING_DIM,
        )
    finally:
        conn.close()


@app.post("/search", response_model=List[SearchResult])
async def search_documents(query: SearchQuery):
    """
    Search for documents using vector similarity.
    
    - Automatically generates embedding from query text
    - use_hnsw=True: Uses HNSW index for O(log n) approximate nearest neighbor search
    - use_hnsw=False (default): Uses brute-force cosine similarity O(n)
    - use_vector=False: Falls back to text search
    
    Note: HNSW requires the daily_reindex DAG to have run. Documents added via API
    after the last reindex will NOT appear in HNSW results.
    """
    conn = get_db_connection()
    try:
        if query.use_vector:
            # Generate embedding for query
            query_embedding = generate_embedding(query.query)
            embedding_str = str(query_embedding)
            
            if query.use_hnsw:
                # HNSW index search using array_cosine_distance
                # This query pattern allows DuckDB to use the HNSW index
                # Note: cosine distance = 1 - cosine_similarity (lower = more similar)
                # so we convert to similarity score: 1 - distance
                has_index = check_vector_index_exists(conn)
                if not has_index:
                    raise HTTPException(
                        status_code=400,
                        detail="HNSW index not found. Run the daily_reindex DAG first, or use use_hnsw=false"
                    )
                
                results = conn.execute(f"""
                    SELECT 
                        id, 
                        content, 
                        metadata,
                        1 - array_cosine_distance(embedding, {embedding_str}::FLOAT[{EMBEDDING_DIM}]) as score
                    FROM documents
                    WHERE embedding IS NOT NULL
                    ORDER BY array_cosine_distance(embedding, {embedding_str}::FLOAT[{EMBEDDING_DIM}])
                    LIMIT ?
                """, [query.top_k]).fetchall()
            else:
                # Brute-force cosine similarity (works without HNSW index)
                results = conn.execute(f"""
                    SELECT 
                        id, 
                        content, 
                        metadata,
                        array_cosine_similarity(embedding, {embedding_str}::FLOAT[{EMBEDDING_DIM}]) as score
                    FROM documents
                    WHERE embedding IS NOT NULL
                    ORDER BY score DESC
                    LIMIT ?
                """, [query.top_k]).fetchall()
            
            return [
                SearchResult(
                    id=row[0],
                    content=row[1],
                    metadata=parse_metadata(row[2]),
                    score=float(row[3]) if row[3] else 0.0,
                )
                for row in results
            ]
        else:
            # Text search fallback
            results = conn.execute("""
                SELECT id, content, metadata
                FROM documents
                WHERE content ILIKE ?
                LIMIT ?
            """, [f"%{query.query}%", query.top_k]).fetchall()
            
            return [
                SearchResult(
                    id=row[0],
                    content=row[1],
                    score=1.0,
                    metadata=parse_metadata(row[2]),
                )
                for row in results
            ]
    finally:
        conn.close()


@app.post("/index")
async def index_document(document: Document):
    """
    Index a single document with automatic embedding generation.
    
    If embedding is not provided, it will be automatically generated.
    """
    conn = get_db_connection(read_only=False)
    try:
        # Generate embedding if not provided
        if document.embedding is None or len(document.embedding) == 0:
            document.embedding = generate_embedding(document.content)
        
        existing = conn.execute(
            "SELECT id FROM documents WHERE id = ?", [document.id]
        ).fetchone()
        
        if existing:
            conn.execute(f"""
                UPDATE documents 
                SET content = ?, metadata = ?, embedding = ?::FLOAT[{EMBEDDING_DIM}], updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, [document.content, json.dumps(document.metadata) if document.metadata else None, document.embedding, document.id])
        else:
            conn.execute(f"""
                INSERT INTO documents (id, content, metadata, embedding)
                VALUES (?, ?, ?, ?::FLOAT[{EMBEDDING_DIM}])
            """, [document.id, document.content, json.dumps(document.metadata) if document.metadata else None, document.embedding])
        
        conn.commit()
        return {
            "status": "indexed",
            "id": document.id,
            "embedding_generated": True,
            "embedding_dim": len(document.embedding),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/index/batch")
async def index_documents_batch(documents: List[Document]):
    """Index multiple documents with automatic embedding generation."""
    conn = get_db_connection(read_only=False)
    try:
        # Generate embeddings for documents that don't have them
        docs_needing_embeddings = [
            (i, doc) for i, doc in enumerate(documents)
            if doc.embedding is None or len(doc.embedding) == 0
        ]
        
        if docs_needing_embeddings:
            texts = [doc.content for _, doc in docs_needing_embeddings]
            embeddings = generate_embeddings_batch(texts)
            
            for (i, doc), embedding in zip(docs_needing_embeddings, embeddings):
                documents[i].embedding = embedding
        
        indexed_count = 0
        for doc in documents:
            existing = conn.execute(
                "SELECT id FROM documents WHERE id = ?", [doc.id]
            ).fetchone()
            
            metadata_json = json.dumps(doc.metadata) if doc.metadata else None
            
            if existing:
                conn.execute(f"""
                    UPDATE documents 
                    SET content = ?, metadata = ?, embedding = ?::FLOAT[{EMBEDDING_DIM}], updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, [doc.content, metadata_json, doc.embedding, doc.id])
            else:
                conn.execute(f"""
                    INSERT INTO documents (id, content, metadata, embedding)
                    VALUES (?, ?, ?, ?::FLOAT[{EMBEDDING_DIM}])
                """, [doc.id, doc.content, metadata_json, doc.embedding])
            indexed_count += 1
        
        conn.commit()
        return {"status": "indexed", "count": indexed_count, "embeddings_generated": len(docs_needing_embeddings)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.delete("/documents/{document_id}")
async def delete_document(document_id: str):
    """Delete a document by ID."""
    conn = get_db_connection(read_only=False)
    try:
        result = conn.execute(
            "DELETE FROM documents WHERE id = ? RETURNING id", [document_id]
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Document not found")
        
        conn.commit()
        return {"status": "deleted", "id": document_id}
    finally:
        conn.close()


@app.delete("/documents")
async def clear_all_documents():
    """Clear all documents from the index."""
    conn = get_db_connection(read_only=False)
    try:
        conn.execute("DELETE FROM documents")
        conn.commit()
        return {"status": "cleared"}
    finally:
        conn.close()


@app.get("/index/current")
async def get_current_index():
    """Get information about the currently active index."""
    index_path = get_current_index_path()
    pointer_exists = os.path.exists(POINTER_FILE)
    
    return {
        "index_path": index_path,
        "index_name": os.path.basename(index_path),
        "using_pointer": pointer_exists,
        "exists": os.path.exists(index_path),
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dim": EMBEDDING_DIM,
    }


# ============================================================================
# S3 Direct Query (with Vector Search)
# ============================================================================

@app.post("/search/s3", response_model=List[SearchResult])
async def search_s3_direct(query: S3SearchQuery):
    """
    Search directly on S3 parquet files using vector similarity.
    
    This bypasses the HNSW index and queries the raw parquet backup.
    Uses brute-force cosine similarity (O(n) but always available).
    """
    conn = get_s3_connection()
    try:
        s3_path = f"s3://{S3_BUCKET}/{query.path}/**/*.parquet"
        
        # Check if data exists
        try:
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM '{s3_path}'
            """).fetchone()
            
            if not count_result or count_result[0] == 0:
                return []
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail=f"No data found at {s3_path}: {str(e)}"
            )
        
        if query.use_vector:
            # Generate embedding for query
            query_embedding = generate_embedding(query.query)
            embedding_str = str(query_embedding)
            
            # Vector similarity search on S3 (brute force)
            # Cast embedding column to proper array type for compatibility
            results = conn.execute(f"""
                SELECT 
                    id, 
                    content, 
                    metadata,
                    array_cosine_similarity(
                        embedding::FLOAT[{EMBEDDING_DIM}], 
                        {embedding_str}::FLOAT[{EMBEDDING_DIM}]
                    ) as score
                FROM '{s3_path}'
                WHERE embedding IS NOT NULL
                ORDER BY score DESC
                LIMIT ?
            """, [query.top_k]).fetchall()
            
            return [
                SearchResult(
                    id=str(row[0]),
                    content=str(row[1]),
                    metadata=parse_metadata(row[2]),
                    score=float(row[3]) if row[3] else 0.0,
                )
                for row in results
            ]
        else:
            # Text search fallback
            results = conn.execute(f"""
                SELECT id, content, metadata
                FROM '{s3_path}'
                WHERE content ILIKE ?
                LIMIT ?
            """, [f"%{query.query}%", query.top_k]).fetchall()
            
            return [
                SearchResult(
                    id=str(row[0]),
                    content=str(row[1]),
                    score=1.0,
                    metadata=parse_metadata(row[2]),
                )
                for row in results
            ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 query failed: {str(e)}")
    finally:
        conn.close()


@app.get("/s3/list")
async def list_s3_documents(path: str = "backup/documents", limit: int = 20):
    """List documents available in S3 backup."""
    conn = get_s3_connection()
    try:
        s3_path = f"s3://{S3_BUCKET}/{path}/**/*.parquet"
        
        results = conn.execute(f"""
            SELECT id, LEFT(content, 100) as content_preview, metadata
            FROM '{s3_path}'
            LIMIT ?
        """, [limit]).fetchall()
        
        return {
            "path": s3_path,
            "count": len(results),
            "documents": [
                {
                    "id": str(row[0]),
                    "content_preview": str(row[1]),
                    "metadata": parse_metadata(row[2]),
                }
                for row in results
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 list failed: {str(e)}")
    finally:
        conn.close()


@app.post("/embed")
async def generate_embedding_endpoint(text: str):
    """Generate embedding for a text (utility endpoint)."""
    embedding = generate_embedding(text)
    return {
        "text": text,
        "embedding": embedding,
        "dim": len(embedding),
        "model": EMBEDDING_MODEL,
    }


# ============================================================================
# Iceberg/Trino Direct Query (Vector Search on Data Lake)
# ============================================================================

class IcebergSearchQuery(BaseModel):
    """Iceberg search query model."""
    query: str
    top_k: int = 10
    catalog: str = "iceberg"
    schema_name: str = "silver_gold"  # dbt-trino creates schemas as {target}_{model_schema}
    table: str = "gold_documents"


def get_trino_connection():
    """Get a Trino connection for Iceberg queries."""
    import trino
    
    trino_host = os.getenv("TRINO_HOST", "trino")
    trino_port = int(os.getenv("TRINO_PORT", "8080"))
    
    # Note: dbt-trino creates schemas as {target_schema}_{model_schema}
    # So gold models end up in silver_gold schema
    return trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user="fastapi",
        catalog="iceberg",
        schema="silver_gold",
    )


@app.post("/search/iceberg", response_model=List[SearchResult])
async def search_iceberg_direct(query: IcebergSearchQuery):
    """
    Search directly on Iceberg tables via Trino using vector similarity.
    
    This queries the Gold layer in Iceberg, which has embeddings generated
    by the embed_iceberg_gold DAG. Uses brute-force cosine similarity
    computed in Trino SQL.
    
    Benefits:
    - No separate DuckDB index needed
    - Queries the source of truth (Iceberg)
    - Scales with Trino workers
    """
    try:
        # Generate embedding for query
        query_embedding = generate_embedding(query.query)
        
        # Use JSON parse to avoid Trino ARRAY[] argument limit (384 dims is too many)
        import json as json_module
        embedding_json = json_module.dumps(query_embedding)
        query_vec = f"CAST(json_parse('{embedding_json}') AS ARRAY(REAL))"
        
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        try:
            # Cosine similarity in Trino SQL
            # dot_product / (magnitude_a * magnitude_b)
            sql = f"""
                SELECT 
                    id,
                    content,
                    CAST(metadata AS VARCHAR) as metadata,
                    (
                        reduce(
                            zip_with(embedding, {query_vec}, (a, b) -> CAST(a AS DOUBLE) * CAST(b AS DOUBLE)),
                            0.0,
                            (s, x) -> s + x,
                            s -> s
                        )
                    ) / NULLIF(
                        sqrt(reduce(embedding, 0.0, (s, x) -> s + CAST(x AS DOUBLE) * CAST(x AS DOUBLE), s -> s)) *
                        sqrt(reduce({query_vec}, 0.0, (s, x) -> s + CAST(x AS DOUBLE) * CAST(x AS DOUBLE), s -> s)),
                        0.0
                    ) as score
                FROM {query.catalog}.{query.schema_name}.{query.table}
                WHERE embedding IS NOT NULL
                  AND cardinality(embedding) > 0
                ORDER BY score DESC
                LIMIT {query.top_k}
            """
            
            cursor.execute(sql)
            results = cursor.fetchall()
            
            return [
                SearchResult(
                    id=str(row[0]),
                    content=str(row[1]),
                    metadata=parse_metadata(row[2]),
                    score=float(row[3]) if row[3] else 0.0,
                )
                for row in results
            ]
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Iceberg search failed: {str(e)}"
        )


@app.get("/iceberg/status")
async def get_iceberg_status():
    """
    Get the status of the Iceberg Gold table and embedding coverage.
    """
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN embedding IS NOT NULL 
                               AND cardinality(embedding) > 0 
                          THEN 1 END) as with_embeddings
                FROM iceberg.silver_gold.gold_documents
            """)
            
            result = cursor.fetchone()
            total, with_embeddings = result[0], result[1]
            
            return {
                "status": "ready" if with_embeddings > 0 else "no_embeddings",
                "total_documents": total,
                "documents_with_embeddings": with_embeddings,
                "embedding_coverage": f"{with_embeddings/total*100:.1f}%" if total > 0 else "0%",
                "table": "iceberg.silver_gold.gold_documents",
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "message": "Iceberg table may not exist. Run dbt transforms first.",
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
