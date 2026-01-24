"""
Embed Iceberg DAG - Generates embeddings for Gold layer documents in Iceberg.

This DAG enables vector search directly on Iceberg tables via Trino,
eliminating the need for a separate DuckDB index.

Flow:
    1. Read Gold documents without embeddings from Iceberg (via Trino)
    2. Generate embeddings using sentence-transformers
    3. Write embeddings back to Iceberg (via PyIceberg)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

# Configuration
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
EMBEDDING_DIM = 384

# S3/MinIO configuration for PyIceberg
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")


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
    Generate embeddings for documents using sentence-transformers.
    """
    from sentence_transformers import SentenceTransformer
    
    documents = context['task_instance'].xcom_pull(
        key='documents', 
        task_ids='get_documents_without_embeddings'
    )
    
    if not documents:
        print("No documents to embed")
        return []
    
    print(f"Loading embedding model: {EMBEDDING_MODEL}...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Extract content and generate embeddings
    ids = [doc[0] for doc in documents]
    contents = [doc[1] for doc in documents]
    
    print(f"Generating embeddings for {len(contents)} documents...")
    embeddings = model.encode(contents, convert_to_numpy=True, show_progress_bar=True)
    
    # Prepare results
    results = [
        {"id": doc_id, "embedding": emb.tolist()}
        for doc_id, emb in zip(ids, embeddings)
    ]
    
    print(f"Generated {len(results)} embeddings")
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
    
    ### Tasks
    
    1. **get_documents_without_embeddings**: Query Iceberg for docs missing embeddings
    2. **generate_embeddings**: Use sentence-transformers to create 384-dim vectors
    3. **write_embeddings_to_iceberg**: Update Iceberg table with embeddings
    4. **verify_embeddings**: Confirm embedding coverage
    
    ### Usage
    
    Run this DAG after `dbt_manual_transforms` completes to add embeddings
    to the Gold layer. Then query directly via Trino:
    
    ```sql
    -- Cosine similarity search in Trino
    SELECT id, content,
           reduce(
               zip_with(embedding, query_vec, (a, b) -> a * b),
               0.0, (s, x) -> s + x, s -> s
           ) / (
               sqrt(reduce(embedding, 0.0, (s, x) -> s + x*x, s -> s)) *
               sqrt(reduce(query_vec, 0.0, (s, x) -> s + x*x, s -> s))
           ) as similarity
    FROM iceberg.gold.gold_documents
    WHERE embedding IS NOT NULL
    ORDER BY similarity DESC
    LIMIT 10;
    ```
    """,
) as dag:
    
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
    
    verify = PythonOperator(
        task_id='verify_embeddings',
        python_callable=verify_embeddings,
    )
    
    get_docs >> gen_embeddings >> write_embeddings >> verify
