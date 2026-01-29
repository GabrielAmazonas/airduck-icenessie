"""
Daily Reindex DAG - Builds vector index and maintains S3 backups.

Architecture:
- Reindexing: Required for HNSW vector search (can't query parquet directly for ANN)
- S3 Backup: For durability, disaster recovery, and data lake queries

Data Sources (configurable via USE_ICEBERG_SOURCE):
- Legacy: Read from S3 parquet files directly (s3://rag-data/documents/)
- Iceberg: Read from Iceberg Gold layer via DuckDB's iceberg extension
"""
from datetime import datetime, timedelta
import os

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator

# =============================================================================
# Configuration
# =============================================================================
EFS_PATH = os.getenv("EFS_PATH", "/mnt/efs")

# Strip http:// or https:// from S3_ENDPOINT for DuckDB
_raw_endpoint = os.getenv("S3_ENDPOINT", "minio:9000")
S3_ENDPOINT = _raw_endpoint.replace("http://", "").replace("https://", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = "rag-data"

# Iceberg configuration
ICEBERG_WAREHOUSE = "iceberg-warehouse"
# Default to true - Iceberg is the source of truth for embeddings
USE_ICEBERG_SOURCE = os.getenv("USE_ICEBERG_SOURCE", "true").lower() == "true"

# Embedding configuration (must match FastAPI)
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# DuckDB configuration SQL with all extensions
DUCKDB_CONFIG_SQL = f"""
    INSTALL httpfs; LOAD httpfs;
    INSTALL vss; LOAD vss;
    INSTALL iceberg; LOAD iceberg;
    SET s3_endpoint='{S3_ENDPOINT}';
    SET s3_access_key_id='{S3_ACCESS_KEY}';
    SET s3_secret_access_key='{S3_SECRET_KEY}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET hnsw_enable_experimental_persistence=true;
"""

# Legacy S3-only config (for backwards compatibility)
S3_CONFIG_SQL = DUCKDB_CONFIG_SQL

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_current_index_path() -> str | None:
    """Get the path to the current active index."""
    pointer_path = os.path.join(EFS_PATH, "latest_pointer.txt")
    if os.path.exists(pointer_path):
        with open(pointer_path, "r") as f:
            return os.path.join(EFS_PATH, f.read().strip())
    return None


def _ingest_from_current_index(con) -> int:
    """
    Ingest documents from the current active DuckDB index.
    
    This ensures documents added via the UI (FastAPI /index endpoint)
    are preserved during reindexing.
    
    Returns the number of documents ingested.
    """
    current_index = get_current_index_path()
    if not current_index or not os.path.exists(current_index):
        print("No current index found, skipping current index ingestion")
        return 0
    
    print(f"Reading documents from current index: {current_index}")
    
    try:
        # Open the current index in read-only mode
        current_con = duckdb.connect(current_index, read_only=True)
        
        # Check if documents table exists and has data
        result = current_con.execute("""
            SELECT COUNT(*) FROM documents
        """).fetchone()
        
        if not result or result[0] == 0:
            print("Current index has no documents")
            current_con.close()
            return 0
        
        doc_count = result[0]
        print(f"Found {doc_count} documents in current index, ingesting...")
        
        # Read all documents from the current index
        docs = current_con.execute("""
            SELECT id, content, embedding, metadata, source_file, created_at
            FROM documents
        """).fetchall()
        
        current_con.close()
        
        # Insert into the new index
        for doc in docs:
            doc_id, content, embedding, metadata, source_file, created_at = doc
            con.execute(f"""
                INSERT OR REPLACE INTO documents (id, content, embedding, metadata, source_file, created_at)
                VALUES (?, ?, ?::FLOAT[{EMBEDDING_DIM}], ?, ?, ?)
            """, [doc_id, content, embedding, metadata, source_file, created_at])
        
        print(f"Ingested {doc_count} documents from current index")
        return doc_count
        
    except Exception as e:
        print(f"Current index read failed: {e}")
        return 0


def _ingest_from_iceberg_gold(con) -> int:
    """
    Ingest documents WITH embeddings from Iceberg Gold layer.
    
    The Gold layer contains chunked, scored, vector-ready documents
    processed by dbt transformations via Trino, with embeddings generated
    by the embed_iceberg_gold DAG.
    
    IMPORTANT: This copies embeddings from Iceberg (single source of truth)
    rather than regenerating them. This ensures DuckDB and Iceberg have
    identical embeddings for consistent search results across both lanes.
    
    Returns the number of documents ingested.
    """
    print("Attempting to read from Iceberg Gold layer...")
    
    # Path to the Iceberg Gold table metadata
    # DuckDB's iceberg extension reads the metadata.json to find data files
    # Note: dbt-trino creates schemas as {target_schema}_{model_schema}
    # So gold models end up in silver_gold schema
    iceberg_gold_path = f"s3://{ICEBERG_WAREHOUSE}/silver_gold/gold_documents"
    
    try:
        # Try to read from Iceberg table
        result = con.execute(f"""
            SELECT COUNT(*) FROM iceberg_scan('{iceberg_gold_path}')
        """).fetchone()
        
        if result and result[0] > 0:
            doc_count = result[0]
            print(f"Found {doc_count} documents in Iceberg Gold, ingesting...")
            
            # Check how many have embeddings
            emb_result = con.execute(f"""
                SELECT COUNT(*) FROM iceberg_scan('{iceberg_gold_path}')
                WHERE embedding IS NOT NULL
            """).fetchone()
            emb_count = emb_result[0] if emb_result else 0
            print(f"  - {emb_count} documents have embeddings in Iceberg")
            
            # Insert from Iceberg Gold - INCLUDING embeddings (single source of truth)
            con.execute(f"""
                INSERT INTO documents (id, content, embedding, metadata, source_file, created_at)
                SELECT 
                    id,
                    content,
                    embedding::FLOAT[{EMBEDDING_DIM}],
                    metadata::JSON,
                    COALESCE(source, 'iceberg_gold') as source_file,
                    COALESCE(ready_at, CURRENT_TIMESTAMP) as created_at
                FROM iceberg_scan('{iceberg_gold_path}')
                WHERE content IS NOT NULL
            """)
            
            print(f"Ingested {doc_count} documents with embeddings from Iceberg Gold")
            return doc_count
        else:
            print("Iceberg Gold table is empty")
            return 0
            
    except Exception as e:
        print(f"Iceberg Gold read failed: {e}")
        return 0


def _ingest_from_s3_parquet(con) -> int:
    """
    Ingest documents from legacy S3 parquet files.
    
    This is the original data source before Iceberg integration.
    
    Returns the number of documents ingested.
    """
    print("Attempting to read from S3 parquet files...")
    
    try:
        result = con.execute(f"""
            SELECT COUNT(*) FROM 's3://{S3_BUCKET}/documents/**/*.parquet'
        """).fetchone()
        
        if result and result[0] > 0:
            doc_count = result[0]
            print(f"Found {doc_count} documents in S3 parquet, ingesting...")
            
            con.execute(f"""
                INSERT INTO documents 
                SELECT * FROM 's3://{S3_BUCKET}/documents/**/*.parquet'
            """)
            
            return doc_count
        else:
            print("No documents found in S3 parquet files")
            return 0
            
    except Exception as e:
        print(f"S3 parquet read failed: {e}")
        return 0


def build_and_swap_index(**context):
    """
    Build a new vector index and atomically swap it.
    
    This is REQUIRED for vector search because:
    1. HNSW index enables O(log n) approximate nearest neighbor search
    2. Without it, every query would scan all embeddings (O(n))
    3. DuckDB VSS extension needs the index in the database file
    
    Data Source Priority (when USE_ICEBERG_SOURCE=true):
    1. Iceberg Gold layer (dbt-processed, chunked, scored)
    2. Fallback to S3 parquet (legacy)
    3. Fallback to sample data
    """
    os.makedirs(EFS_PATH, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_index_name = f"index_{timestamp}.duckdb"
    new_index_path = os.path.join(EFS_PATH, new_index_name)
    
    print(f"Building new index: {new_index_name}")
    print(f"USE_ICEBERG_SOURCE: {USE_ICEBERG_SOURCE}")
    
    # 1. Create new database and configure extensions
    con = duckdb.connect(new_index_path)
    con.execute(DUCKDB_CONFIG_SQL)
    
    # 2. Create schema for documents with embeddings
    con.execute(f"""
        CREATE TABLE documents (
            id VARCHAR PRIMARY KEY,
            content TEXT NOT NULL,
            embedding FLOAT[{EMBEDDING_DIM}],
            metadata JSON,
            source_file VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 3. Ingest data from all sources
    # Priority: 
    #   1. Current DuckDB index (preserves UI-added documents)
    #   2. Iceberg Gold (if USE_ICEBERG_SOURCE=true)
    #   3. S3 parquet (legacy/fallback)
    #   4. Sample data (if nothing else found)
    
    total_doc_count = 0
    
    # Always start by ingesting from current index to preserve UI-added documents
    current_index_count = _ingest_from_current_index(con)
    total_doc_count += current_index_count
    print(f"Ingested {current_index_count} documents from current index")
    
    if USE_ICEBERG_SOURCE:
        # Try Iceberg Gold, then fallback to S3 parquet
        iceberg_count = _ingest_from_iceberg_gold(con)
        total_doc_count += iceberg_count
        
        if iceberg_count == 0:
            print("Falling back to S3 parquet source...")
            s3_count = _ingest_from_s3_parquet(con)
            total_doc_count += s3_count
    else:
        # Legacy behavior: try S3 parquet
        s3_count = _ingest_from_s3_parquet(con)
        total_doc_count += s3_count
    
    # Get actual document count (handles duplicates from INSERT OR REPLACE)
    doc_count = con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    
    # Fallback to sample data if no documents found
    if doc_count == 0:
        print("No documents found in any source, creating sample data...")
        _insert_sample_data(con)
        doc_count = con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    
    # 4. Generate embeddings ONLY for documents without them
    # This is a fallback - embeddings should come from Iceberg (single source of truth)
    # Only generates locally if:
    #   - Document was added via UI (not in Iceberg yet)
    #   - Iceberg document didn't have embedding (embed_iceberg_gold DAG not run)
    _generate_missing_embeddings(con)
    
    # 5. Build HNSW vector index for fast similarity search
    doc_count = con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    print(f"Building HNSW index for {doc_count} documents...")
    
    if doc_count > 0:
        con.execute("""
            CREATE INDEX embedding_idx ON documents 
            USING HNSW (embedding)
            WITH (metric = 'cosine')
        """)
    
    # 6. Create additional indexes for filtering
    con.execute("CREATE INDEX idx_doc_id ON documents(id)")
    
    con.close()
    print(f"Index built successfully: {new_index_path}")
    
    # 7. Atomic pointer swap
    pointer_path = os.path.join(EFS_PATH, "latest_pointer.txt")
    temp_pointer = pointer_path + ".tmp"
    
    with open(temp_pointer, "w") as f:
        f.write(new_index_name)
    os.rename(temp_pointer, pointer_path)
    
    print(f"Pointer updated to: {new_index_name}")
    
    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="new_index_name", value=new_index_name)
    context["ti"].xcom_push(key="doc_count", value=doc_count)
    context["ti"].xcom_push(key="source", value="iceberg" if USE_ICEBERG_SOURCE else "s3_parquet")
    
    return new_index_name


def _generate_missing_embeddings(con):
    """Generate embeddings for documents that don't have them."""
    from sentence_transformers import SentenceTransformer
    
    # Check for documents without embeddings
    result = con.execute("""
        SELECT id, content FROM documents 
        WHERE embedding IS NULL
    """).fetchall()
    
    if not result:
        print("All documents already have embeddings")
        return
    
    print(f"Generating embeddings for {len(result)} documents...")
    print(f"Loading embedding model: {EMBEDDING_MODEL}...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Generate embeddings in batches
    batch_size = 32
    for i in range(0, len(result), batch_size):
        batch = result[i:i + batch_size]
        ids = [row[0] for row in batch]
        contents = [row[1] for row in batch]
        
        embeddings = model.encode(contents, convert_to_numpy=True)
        
        for doc_id, embedding in zip(ids, embeddings):
            embedding_list = embedding.tolist()
            con.execute(f"""
                UPDATE documents 
                SET embedding = ?::FLOAT[{EMBEDDING_DIM}]
                WHERE id = ?
            """, [embedding_list, doc_id])
        
        print(f"Generated embeddings for batch {i // batch_size + 1}")
    
    print(f"Completed embedding generation for {len(result)} documents")


def _insert_sample_data(con):
    """Insert sample documents with real embeddings."""
    from sentence_transformers import SentenceTransformer
    
    print(f"Loading embedding model: {EMBEDDING_MODEL}...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    sample_docs = [
        ("doc-001", "Introduction to vector databases and similarity search"),
        ("doc-002", "DuckDB is an in-process analytical database"),
        ("doc-003", "HNSW algorithm for approximate nearest neighbor search"),
        ("doc-004", "Building RAG applications with embeddings"),
        ("doc-005", "FastAPI for high-performance Python APIs"),
    ]
    
    # Generate embeddings for all documents
    contents = [content for _, content in sample_docs]
    embeddings = model.encode(contents, convert_to_numpy=True)
    
    for (doc_id, content), embedding in zip(sample_docs, embeddings):
        embedding_list = embedding.tolist()
        con.execute(f"""
            INSERT INTO documents (id, content, embedding, metadata, source_file)
            VALUES (?, ?, ?::FLOAT[{EMBEDDING_DIM}], ?, ?)
        """, [doc_id, content, embedding_list, {"type": "sample"}, "sample_data"])
    
    print(f"Inserted {len(sample_docs)} sample documents with {EMBEDDING_DIM}-dim embeddings")


def backup_to_s3_partitioned(**context):
    """
    Backup current index to S3 with partitioning.
    
    This is for:
    - Disaster recovery
    - Data lake queries (non-vector SQL queries can hit S3 directly)
    - Re-ingestion if index needs to be rebuilt
    """
    current_index = get_current_index_path()
    if not current_index or not os.path.exists(current_index):
        print("No current index found, skipping backup")
        return
    
    print(f"Backing up index: {current_index}")
    
    con = duckdb.connect(current_index, read_only=True)
    con.execute(S3_CONFIG_SQL)
    
    # Get current date for partitioning
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    
    # Export to S3 with date partitioning
    con.execute(f"""
        COPY (
            SELECT 
                id, content, embedding, metadata, source_file, created_at,
                {year} as year, 
                {month} as month,
                {day} as day
            FROM documents
        ) 
        TO 's3://{S3_BUCKET}/backup/documents/' 
        (FORMAT PARQUET, PARTITION_BY (year, month, day), OVERWRITE_OR_IGNORE 1)
    """)
    
    con.close()
    print(f"Backup complete to s3://{S3_BUCKET}/backup/documents/year={year}/month={month}/day={day}/")


def cleanup_old_indexes(**context):
    """Remove old index files, keeping the last 3 versions."""
    import glob
    
    pointer_path = os.path.join(EFS_PATH, "latest_pointer.txt")
    if not os.path.exists(pointer_path):
        return
    
    with open(pointer_path, "r") as f:
        current_index = f.read().strip()
    
    # Find all index files
    index_files = glob.glob(os.path.join(EFS_PATH, "index_*.duckdb"))
    index_files.sort(reverse=True)  # Newest first
    
    # Keep the 3 most recent, delete the rest
    files_to_keep = set(index_files[:3])
    files_to_keep.add(os.path.join(EFS_PATH, current_index))  # Always keep current
    
    for index_file in index_files:
        if index_file not in files_to_keep:
            try:
                os.remove(index_file)
                # Also remove WAL file if exists
                wal_file = index_file + ".wal"
                if os.path.exists(wal_file):
                    os.remove(wal_file)
                print(f"Removed old index: {index_file}")
            except Exception as e:
                print(f"Failed to remove {index_file}: {e}")


# Define the DAG
with DAG(
    dag_id="daily_reindex",
    default_args=default_args,
    description="Build vector index from Iceberg Gold or S3 parquet, backup to S3",
    schedule_interval="0 2 * * *",  # Run at 2 AM (after dbt at 1 AM)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rag", "vector-search", "duckdb", "iceberg"],
    doc_md="""
    ## Daily Reindex Pipeline
    
    This DAG maintains the vector search infrastructure by building HNSW indexes.
    
    ### Tasks
    
    1. **build_index**: Creates new DuckDB with HNSW vector index
    2. **backup_to_s3**: Exports to partitioned parquet for DR
    3. **cleanup**: Removes old index versions (keeps last 3)
    
    ### Data Sources
    
    Controlled by `USE_ICEBERG_SOURCE` environment variable:
    
    - **`true` (default)**: Read from Iceberg Gold layer (`s3://iceberg-warehouse/silver_gold/gold_documents`)
    - **`false`**: Read from S3 parquet files (`s3://rag-data/documents/`) - legacy mode
    
    ### Embedding Strategy (Single Source of Truth)
    
    When `USE_ICEBERG_SOURCE=true`, embeddings are **copied from Iceberg**, not regenerated:
    
    ```
    embed_iceberg_gold DAG    →    Iceberg Gold (S3)    →    daily_reindex DAG    →    DuckDB (EFS)
         (generate)                 (source of truth)           (copy)                 (replica)
    ```
    
    This ensures:
    - **Identical embeddings** across both search lanes (DuckDB and Trino)
    - **Consistent results** for `/search` and `/search/iceberg` endpoints
    - **No drift** from model version differences or timing
    
    Local embedding generation only happens as a fallback for:
    - Documents added via UI (not yet in Iceberg)
    - Documents in Iceberg without embeddings
    
    ### Data Flow with Iceberg
    
    ```
    Bronze (raw) → Silver (dedupe) → Gold (chunked/scored + embeddings) → HNSW Index → FastAPI
    ```
    
    ### Why Reindexing is Required
    
    Vector similarity search using HNSW cannot query raw parquet files directly.
    The index must be pre-built in DuckDB for O(log n) approximate nearest neighbor search.
    
    ### Triggering
    
    This DAG is typically triggered by `dbt_pipeline_with_reindex` after dbt completes,
    or runs on its daily schedule at 2 AM (after dbt runs at 1 AM).
    """,
) as dag:

    build_task = PythonOperator(
        task_id="build_index",
        python_callable=build_and_swap_index,
    )

    backup_task = PythonOperator(
        task_id="backup_to_s3",
        python_callable=backup_to_s3_partitioned,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_old_indexes",
        python_callable=cleanup_old_indexes,
    )

    # Task dependencies
    build_task >> backup_task >> cleanup_task
