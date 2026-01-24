"""
Embed Iceberg DAG - Generates embeddings for Gold layer documents in Iceberg.

This DAG enables vector search directly on Iceberg tables via Trino,
eliminating the need for a separate DuckDB index.

Flow:
    1. Read Gold documents without embeddings from Iceberg (via Trino)
    2. Generate embeddings via FastAPI /embed endpoint (offloads compute to FastAPI container)
    3. Write embeddings back to Iceberg (via Trino UPDATE)
    4. Compact small files created by updates
    5. Verify embedding coverage

Architecture Note:
    Embedding generation runs on the FastAPI container, not Airflow.
    This avoids loading the model twice and centralizes ML compute.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

# Configuration
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
EMBEDDING_DIM = 384

# FastAPI endpoint for embedding generation
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://fastapi:8000")

# Batch size for embedding requests
EMBEDDING_BATCH_SIZE = 32


def get_trino_connection():
    """Get a Trino connection."""
    from trino.dbapi import connect
    
    # Note: dbt-trino creates schemas as {target_schema}_{model_schema}
    # So gold models end up in silver_gold schema (silver is the target, gold is the model schema)
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        schema="silver_gold",
    )


def get_documents_without_embeddings(**context):
    """
    Fetch Gold documents that don't have embeddings yet.
    """
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    try:
        # Check if table exists and has the embedding column
        # Note: dbt-trino creates schemas as {target_schema}_{model_schema}
        cursor.execute("""
            SELECT id, content 
            FROM iceberg.silver_gold.gold_documents 
            WHERE embedding IS NULL 
               OR cardinality(embedding) = 0
            LIMIT 1000
        """)
        
        documents = cursor.fetchall()
        print(f"Found {len(documents)} documents without embeddings")
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='documents', value=documents)
        return len(documents)
        
    except Exception as e:
        print(f"Error fetching documents: {e}")
        # Table might not exist yet
        return 0
    finally:
        cursor.close()
        conn.close()


def generate_embeddings(**context):
    """
    Generate embeddings via FastAPI /embed/batch endpoint.
    
    This offloads embedding computation to the FastAPI container,
    which already has the model loaded. Benefits:
    - No model loading in Airflow container
    - Centralized ML compute
    - Reduced Airflow memory requirements
    - Model stays warm in FastAPI for real-time requests
    - Batch endpoint is more efficient than individual requests
    """
    import requests
    import time
    
    documents = context['task_instance'].xcom_pull(
        key='documents', 
        task_ids='get_documents_without_embeddings'
    )
    
    if not documents:
        print("No documents to embed")
        return []
    
    # Extract content
    ids = [doc[0] for doc in documents]
    contents = [doc[1] for doc in documents]
    
    print(f"Generating embeddings for {len(contents)} documents via FastAPI...")
    print(f"Endpoint: {FASTAPI_URL}/embed/batch")
    
    results = []
    failed_batches = 0
    
    # Process in batches using the batch endpoint
    for i in range(0, len(contents), EMBEDDING_BATCH_SIZE):
        batch_ids = ids[i:i + EMBEDDING_BATCH_SIZE]
        batch_contents = contents[i:i + EMBEDDING_BATCH_SIZE]
        batch_num = i // EMBEDDING_BATCH_SIZE + 1
        total_batches = (len(contents) + EMBEDDING_BATCH_SIZE - 1) // EMBEDDING_BATCH_SIZE
        
        print(f"Processing batch {batch_num}/{total_batches} ({len(batch_contents)} docs)...")
        
        try:
            response = requests.post(
                f"{FASTAPI_URL}/embed/batch",
                json={"texts": batch_contents},
                timeout=120  # Longer timeout for batch
            )
            
            if response.status_code == 200:
                data = response.json()
                embeddings = data["embeddings"]
                
                for doc_id, embedding in zip(batch_ids, embeddings):
                    results.append({
                        "id": doc_id,
                        "embedding": embedding
                    })
                    
                print(f"  ✓ Batch {batch_num}: {len(embeddings)} embeddings generated")
            else:
                print(f"  ⚠ Batch {batch_num} failed: HTTP {response.status_code}")
                failed_batches += 1
                
        except requests.exceptions.Timeout:
            print(f"  ⚠ Batch {batch_num} timed out")
            failed_batches += 1
        except requests.exceptions.RequestException as e:
            print(f"  ⚠ Batch {batch_num} error: {e}")
            failed_batches += 1
        
        # Small delay between batches
        if i + EMBEDDING_BATCH_SIZE < len(contents):
            time.sleep(0.2)
    
    print(f"✓ Generated {len(results)} embeddings, {failed_batches} batches failed")
    context['task_instance'].xcom_push(key='embeddings', value=results)
    return len(results)


def write_embeddings_to_iceberg(**context):
    """
    Write embeddings back to Iceberg Gold table.
    
    Uses Trino to update the embedding column.
    """
    embeddings_data = context['task_instance'].xcom_pull(
        key='embeddings',
        task_ids='generate_embeddings'
    )
    
    if not embeddings_data:
        print("No embeddings to write")
        return 0
    
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    try:
        updated_count = 0
        batch_size = 50
        
        for i in range(0, len(embeddings_data), batch_size):
            batch = embeddings_data[i:i + batch_size]
            
            for item in batch:
                doc_id = item['id']
                embedding = item['embedding']
                
                # Use JSON cast to avoid ARRAY[] argument limit in Trino
                # json_parse + CAST is the workaround for large arrays
                import json
                embedding_json = json.dumps(embedding)
                
                try:
                    cursor.execute(f"""
                        UPDATE iceberg.silver_gold.gold_documents
                        SET embedding = CAST(json_parse('{embedding_json}') AS ARRAY(REAL))
                        WHERE id = '{doc_id}'
                    """)
                    updated_count += 1
                except Exception as e:
                    print(f"Failed to update {doc_id}: {e}")
            
            print(f"Updated batch {i // batch_size + 1}, total: {updated_count}")
        
        print(f"Successfully updated {updated_count} documents with embeddings")
        return updated_count
        
    finally:
        cursor.close()
        conn.close()


def verify_embeddings(**context):
    """
    Verify that embeddings were written correctly.
    """
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
        
        print(f"Total documents: {total}")
        print(f"Documents with embeddings: {with_embeddings}")
        print(f"Coverage: {with_embeddings/total*100:.1f}%" if total > 0 else "No documents")
        
        return {"total": total, "with_embeddings": with_embeddings}
        
    finally:
        cursor.close()
        conn.close()


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'embed_iceberg_gold',
    default_args=default_args,
    description='Generate embeddings for Iceberg Gold documents',
    schedule_interval=None,  # Manual trigger or after dbt
    catchup=False,
    tags=['embeddings', 'iceberg', 'vector-search'],
    doc_md="""
    ## Embed Iceberg Gold Documents
    
    This DAG generates embeddings for documents in the Iceberg Gold layer,
    enabling vector search directly via Trino without needing DuckDB.
    
    ### Architecture
    
    **Embedding compute runs on FastAPI container, not Airflow.**
    
    ```
    Airflow                    FastAPI
    ┌──────────────────┐      ┌──────────────────┐
    │ 1. Query Trino   │      │                  │
    │ 2. Get docs      │─────▶│ 3. /embed/batch  │
    │ 4. Write to      │◀─────│    (ML compute)  │
    │    Iceberg       │      │                  │
    └──────────────────┘      └──────────────────┘
    ```
    
    Benefits:
    - No model loading in Airflow
    - Centralized ML compute
    - Model stays warm for real-time requests
    
    ### Tasks
    
    1. **get_documents_without_embeddings**: Query Iceberg via Trino
    2. **generate_embeddings**: Call FastAPI /embed/batch endpoint
    3. **write_embeddings_to_iceberg**: Update Iceberg via Trino
    4. **compact_gold_table**: Compact small files from UPDATEs
    5. **verify_embeddings**: Confirm embedding coverage
    
    ### Configuration
    
    - `FASTAPI_URL`: FastAPI endpoint (default: http://fastapi:8000)
    - `EMBEDDING_BATCH_SIZE`: Docs per batch request (default: 32)
    """,
) as dag:
    
    def compact_gold_table(**context):
        """
        Compact Gold table after embedding updates to address small files problem.
        
        UPDATEs in Iceberg create new data files. Frequent embedding updates
        can create many small files. This compacts them for better performance.
        """
        conn = get_trino_connection()
        cursor = conn.cursor()
        
        table = "iceberg.silver_gold.gold_documents"
        
        try:
            # Check file count and average size
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as file_count,
                    AVG(file_size_in_bytes) / 1024 / 1024 as avg_size_mb
                FROM "{table}$files"
            """)
            row = cursor.fetchone()
            
            if row and row[0] > 0:
                file_count = row[0]
                avg_size = round(row[1], 2) if row[1] else 0
                
                # Skip if already optimal
                if avg_size >= 100 or file_count < 5:
                    print(f"⏭ {table}: {file_count} files, avg {avg_size}MB - already optimal")
                    return {"skipped": True, "reason": "already optimal"}
                
                print(f"🔄 {table}: {file_count} files, avg {avg_size}MB - compacting...")
                
                # Try optimize first
                try:
                    cursor.execute(f"""
                        ALTER TABLE {table} EXECUTE optimize
                        WHERE file_size_in_bytes < 134217728
                    """)
                    print(f"✓ {table}: Compaction complete")
                except Exception:
                    # Fallback to rewrite_data_files
                    try:
                        cursor.execute(f"CALL iceberg.system.rewrite_data_files(table => '{table}')")
                        print(f"✓ {table}: Compaction via rewrite_data_files complete")
                    except Exception as e2:
                        print(f"⚠ {table}: Compaction skipped - {e2}")
                        return {"skipped": True, "reason": str(e2)}
                
                return {"compacted": True}
            else:
                print(f"⏭ {table}: No files to compact")
                return {"skipped": True, "reason": "no files"}
                
        except Exception as e:
            print(f"⚠ {table}: Could not analyze/compact - {e}")
            return {"error": str(e)}
        finally:
            cursor.close()
            conn.close()
    
    get_docs = PythonOperator(
        task_id='get_documents_without_embeddings',
        python_callable=get_documents_without_embeddings,
    )
    
    gen_embeddings = PythonOperator(
        task_id='generate_embeddings',
        python_callable=generate_embeddings,
    )
    
    write_embeddings = PythonOperator(
        task_id='write_embeddings_to_iceberg',
        python_callable=write_embeddings_to_iceberg,
    )
    
    compact = PythonOperator(
        task_id='compact_gold_table',
        python_callable=compact_gold_table,
    )
    
    verify = PythonOperator(
        task_id='verify_embeddings',
        python_callable=verify_embeddings,
    )
    
    get_docs >> gen_embeddings >> write_embeddings >> compact >> verify
