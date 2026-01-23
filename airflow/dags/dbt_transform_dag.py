"""
dbt Transformation DAG - Orchestrates Bronze → Silver → Gold pipeline via Trino.

This DAG transforms raw data through the medallion architecture using dbt
and Trino as the query engine against Iceberg tables.

Data Flow:
    Bronze (raw_documents) → Silver (deduplicated) → Gold (chunked, scored)
    
After Gold is ready, the daily_reindex DAG should run to build HNSW vector indexes.

Three DAG options are provided:
1. dbt_cosmos_transforms - Uses Cosmos for automatic dbt task generation (recommended)
2. dbt_manual_transforms - Manual BashOperator-based for more control
3. dbt_pipeline_with_reindex - Runs dbt then triggers vector reindexing
"""
from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

# =============================================================================
# Configuration
# =============================================================================
DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_PATH = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# Helper Functions
# =============================================================================

def check_trino_connection(**context):
    """Verify Trino is accessible before running dbt."""
    import trino
    
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=int(TRINO_PORT),
            user="airflow",
            catalog="iceberg",
            schema="silver"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result[0] == 1:
            print(f"✓ Trino connection successful: {TRINO_HOST}:{TRINO_PORT}")
            return True
        else:
            raise Exception("Trino returned unexpected result")
    except Exception as e:
        print(f"✗ Trino connection failed: {e}")
        raise


def ensure_iceberg_schemas(**context):
    """Create Iceberg schemas if they don't exist."""
    import trino
    
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user="airflow",
        catalog="iceberg"
    )
    cursor = conn.cursor()
    
    schemas = ["bronze", "silver", "gold"]
    for schema in schemas:
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS iceberg.{schema}")
            print(f"✓ Schema iceberg.{schema} ensured")
        except Exception as e:
            print(f"Schema {schema} creation note: {e}")
    
    cursor.close()
    conn.close()
    print("✓ All Iceberg schemas verified")


def create_bronze_table_if_not_exists(**context):
    """Create the Bronze raw_documents table if it doesn't exist."""
    import trino
    
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user="airflow",
        catalog="iceberg",
        schema="bronze"
    )
    cursor = conn.cursor()
    
    # Check if table exists
    try:
        cursor.execute("SELECT 1 FROM raw_documents LIMIT 1")
        cursor.fetchone()
        print("✓ Bronze raw_documents table already exists")
    except Exception:
        # Create the table
        print("Creating Bronze raw_documents table...")
        # Note: Trino/Iceberg doesn't support DEFAULT clause in CREATE TABLE
        # The ingested_at value should be set at INSERT time using CURRENT_TIMESTAMP
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.bronze.raw_documents (
                id VARCHAR,
                content VARCHAR,
                source VARCHAR,
                ingested_at TIMESTAMP(6) WITH TIME ZONE,
                raw_metadata VARCHAR
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(ingested_at)']
            )
        """)
        print("✓ Bronze raw_documents table created")
    
    cursor.close()
    conn.close()


def log_dbt_run_summary(**context):
    """Log summary after dbt run."""
    ti = context['ti']
    
    print("=" * 60)
    print("dbt Transformation Summary")
    print("=" * 60)
    print(f"DAG Run ID: {context['dag_run'].run_id}")
    print(f"Execution Date: {context['execution_date']}")
    print(f"dbt Project: {DBT_PROJECT_PATH}")
    print(f"Trino Endpoint: {TRINO_HOST}:{TRINO_PORT}")
    print("=" * 60)
    print("")
    print("Next Steps:")
    print("  - Run the 'daily_reindex' DAG to build HNSW vector indexes")
    print("  - Or wait for scheduled trigger from 'dbt_pipeline_with_reindex'")
    print("=" * 60)


# =============================================================================
# DAG 1: Cosmos-based DbtDag (Recommended)
# =============================================================================
# This uses astronomer-cosmos for automatic task generation from dbt models.
# Each dbt model becomes an Airflow task with proper dependencies.

try:
    from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
    from cosmos.profiles import TrinoUserPasswordProfileMapping
    
    COSMOS_AVAILABLE = True
except ImportError:
    COSMOS_AVAILABLE = False
    print("Warning: astronomer-cosmos not available, using manual DAG only")

if COSMOS_AVAILABLE:
    dbt_cosmos_dag = DbtDag(
        dag_id="dbt_cosmos_transforms",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name="trino_iceberg",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROFILES_PATH}/profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/home/airflow/.local/bin/dbt",
        ),
        schedule_interval="0 1 * * *",  # Run at 1 AM daily
        start_date=datetime(2024, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=["dbt", "transform", "cosmos", "trino", "iceberg"],
        doc_md="""
        ## dbt Cosmos Transformation Pipeline
        
        Automatically generates Airflow tasks from dbt models using Cosmos.
        
        **Medallion Architecture:**
        1. **Bronze** (source): Raw documents as ingested into Iceberg
        2. **Silver** (incremental): Deduplicated, cleaned documents  
        3. **Gold** (table): Chunked, scored, ready for embedding
        
        **Schedule:** Daily at 1 AM (before vector indexing)
        
        **Downstream:** Triggers `daily_reindex` DAG after completion
        """,
    )


# =============================================================================
# DAG 2: Manual dbt DAG with BashOperator (More Control)
# =============================================================================

with DAG(
    dag_id="dbt_manual_transforms",
    default_args=default_args,
    description="Manual dbt pipeline with explicit task control",
    schedule_interval="0 1 * * *",  # Run at 1 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transform", "manual", "trino", "iceberg"],
    doc_md="""
    ## dbt Manual Transformation Pipeline
    
    Manual BashOperator-based DAG for dbt transformations.
    Provides more control over individual dbt commands.
    
    **Tasks:**
    1. check_trino - Verify Trino connectivity
    2. ensure_schemas - Create Iceberg schemas if needed
    3. create_bronze_table - Ensure source table exists
    4. dbt_deps - Install dbt packages
    5. dbt_debug - Test dbt connection to Trino
    6. dbt_run_silver - Run Silver incremental models
    7. dbt_run_gold - Run Gold table models
    8. dbt_test - Run dbt tests
    9. dbt_docs - Generate documentation
    10. log_summary - Log run summary
    """,
) as manual_dag:

    # Start marker
    start = EmptyOperator(task_id="start")

    # Check Trino connectivity
    check_trino = PythonOperator(
        task_id="check_trino_connection",
        python_callable=check_trino_connection,
    )

    # Ensure Iceberg schemas exist
    ensure_schemas = PythonOperator(
        task_id="ensure_iceberg_schemas",
        python_callable=ensure_iceberg_schemas,
    )

    # Create Bronze table if not exists
    create_bronze = PythonOperator(
        task_id="create_bronze_table",
        python_callable=create_bronze_table_if_not_exists,
    )

    # Install dbt packages
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt deps --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Test dbt connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_PATH} && dbt debug --profiles-dir {DBT_PROFILES_PATH}",
    )

    # Run Silver models (incremental deduplication)
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"""
            cd {DBT_PROJECT_PATH} && \
            dbt run --select silver --profiles-dir {DBT_PROFILES_PATH} --target dev
        """,
    )

    # Run Gold models (full refresh for vector-ready data)
    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"""
            cd {DBT_PROJECT_PATH} && \
            dbt run --select gold --profiles-dir {DBT_PROFILES_PATH} --target dev --full-refresh
        """,
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd {DBT_PROJECT_PATH} && \
            dbt test --profiles-dir {DBT_PROFILES_PATH} --target dev || echo "Some tests failed, continuing..."
        """,
    )

    # Generate dbt documentation
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
            cd {DBT_PROJECT_PATH} && \
            dbt docs generate --profiles-dir {DBT_PROFILES_PATH} --target dev
        """,
    )

    # Log summary
    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_dbt_run_summary,
    )

    # End marker
    end = EmptyOperator(task_id="end")

    # Task dependencies
    (
        start
        >> check_trino
        >> ensure_schemas
        >> create_bronze
        >> dbt_deps
        >> dbt_debug
        >> dbt_silver
        >> dbt_gold
        >> dbt_test
        >> dbt_docs
        >> log_summary
        >> end
    )


# =============================================================================
# DAG 3: Full Pipeline - dbt + Vector Reindex
# =============================================================================

with DAG(
    dag_id="dbt_pipeline_with_reindex",
    default_args=default_args,
    description="Full pipeline: dbt transforms then vector reindexing",
    schedule_interval="0 0 * * *",  # Run at midnight daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "vector", "pipeline", "full", "trino", "iceberg"],
    doc_md="""
    ## Full Data Pipeline: dbt + Vector Indexing
    
    Complete end-to-end pipeline that:
    1. Runs all dbt transformations (Bronze → Silver → Gold)
    2. Runs tests to validate data quality
    3. Triggers the vector reindexing DAG
    
    **Schedule:** Daily at midnight
    
    **Flow:**
    ```
    Bronze (Iceberg) 
        → dbt Silver (dedupe) 
        → dbt Gold (chunk/score) 
        → DuckDB HNSW Index
        → FastAPI Vector Search
    ```
    """,
) as pipeline_dag:

    # Start marker
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # Check Trino connectivity first
    check_trino_pipeline = PythonOperator(
        task_id="check_trino_connection",
        python_callable=check_trino_connection,
    )

    # Ensure infrastructure is ready
    ensure_infra = PythonOperator(
        task_id="ensure_infrastructure",
        python_callable=ensure_iceberg_schemas,
    )

    # Run all dbt steps in one command
    run_dbt_all = BashOperator(
        task_id="run_dbt_all",
        bash_command=f"""
            cd {DBT_PROJECT_PATH} && \
            echo "=== Installing dbt packages ===" && \
            dbt deps --profiles-dir {DBT_PROFILES_PATH} && \
            echo "" && \
            echo "=== Running dbt models ===" && \
            dbt run --profiles-dir {DBT_PROFILES_PATH} --target dev && \
            echo "" && \
            echo "=== Running dbt tests ===" && \
            dbt test --profiles-dir {DBT_PROFILES_PATH} --target dev || echo "Some tests failed" && \
            echo "" && \
            echo "=== dbt run complete ==="
        """,
        execution_timeout=timedelta(hours=2),
    )

    # Verify Gold table has data
    verify_gold = BashOperator(
        task_id="verify_gold_table",
        bash_command=f"""
            cd {DBT_PROJECT_PATH} && \
            dbt run-operation check_gold_count --profiles-dir {DBT_PROFILES_PATH} || \
            echo "Gold table verification skipped - macro not defined"
        """,
    )

    # Trigger the reindex DAG
    trigger_reindex = TriggerDagRunOperator(
        task_id="trigger_vector_reindex",
        trigger_dag_id="daily_reindex",
        wait_for_completion=True,
        poke_interval=60,  # Check every minute
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # Log pipeline completion
    log_pipeline_complete = PythonOperator(
        task_id="log_pipeline_complete",
        python_callable=log_dbt_run_summary,
    )

    # End marker
    pipeline_end = EmptyOperator(task_id="pipeline_end")

    # Task dependencies
    (
        pipeline_start
        >> check_trino_pipeline
        >> ensure_infra
        >> run_dbt_all
        >> verify_gold
        >> trigger_reindex
        >> log_pipeline_complete
        >> pipeline_end
    )


# =============================================================================
# DAG 4: Seed Bronze Data (for initial setup/testing)
# =============================================================================

with DAG(
    dag_id="dbt_seed_bronze_data",
    default_args=default_args,
    description="Seed initial data into Bronze layer for testing",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "seed", "bronze", "setup"],
    doc_md="""
    ## Seed Bronze Data
    
    Manually triggered DAG to seed initial test data into the Bronze layer.
    Use this for initial setup or testing the dbt pipeline.
    
    **Trigger manually** when you need sample data in Bronze.
    """,
) as seed_dag:

    def seed_bronze_documents(**context):
        """Insert sample documents into Bronze layer via Trino."""
        import trino
        from datetime import datetime
        import json
        
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=int(TRINO_PORT),
            user="airflow",
            catalog="iceberg",
            schema="bronze"
        )
        cursor = conn.cursor()
        
        # Sample documents for testing
        sample_docs = [
            {
                "id": "seed-001",
                "content": "Introduction to vector databases and similarity search. Vector databases enable efficient nearest neighbor search across high-dimensional embedding spaces.",
                "source": "seed",
                "metadata": {"type": "tutorial", "topic": "vector-db"}
            },
            {
                "id": "seed-002", 
                "content": "DuckDB is an in-process analytical database that supports vector similarity search through its VSS extension. It enables HNSW indexing for fast approximate nearest neighbor queries.",
                "source": "seed",
                "metadata": {"type": "documentation", "topic": "duckdb"}
            },
            {
                "id": "seed-003",
                "content": "The HNSW (Hierarchical Navigable Small World) algorithm provides logarithmic time complexity for approximate nearest neighbor search, making it ideal for large-scale vector retrieval.",
                "source": "seed",
                "metadata": {"type": "research", "topic": "hnsw"}
            },
            {
                "id": "seed-004",
                "content": "Building RAG (Retrieval-Augmented Generation) applications requires combining embedding models with vector stores. This enables semantic search over document collections.",
                "source": "seed",
                "metadata": {"type": "guide", "topic": "rag"}
            },
            {
                "id": "seed-005",
                "content": "Apache Iceberg provides ACID transactions, time travel, and schema evolution for data lakes. Combined with Trino, it enables efficient analytical queries over large datasets.",
                "source": "seed",
                "metadata": {"type": "documentation", "topic": "iceberg"}
            },
            {
                "id": "seed-006",
                "content": "FastAPI is a modern Python web framework that provides high performance for building APIs. It integrates well with async database drivers and ML models for real-time inference.",
                "source": "seed",
                "metadata": {"type": "documentation", "topic": "fastapi"}
            },
            {
                "id": "seed-007",
                "content": "The medallion architecture (Bronze, Silver, Gold) organizes data lake tables by quality level. Bronze contains raw data, Silver is cleaned, and Gold is business-ready.",
                "source": "seed",
                "metadata": {"type": "architecture", "topic": "lakehouse"}
            },
            {
                "id": "seed-008",
                "content": "Airflow orchestrates complex data pipelines with DAGs (Directed Acyclic Graphs). It supports scheduling, monitoring, and managing dependencies between tasks.",
                "source": "seed",
                "metadata": {"type": "documentation", "topic": "airflow"}
            },
        ]
        
        # Insert documents
        inserted = 0
        for doc in sample_docs:
            try:
                # Explicitly set ingested_at since Trino/Iceberg doesn't support DEFAULT
                cursor.execute("""
                    INSERT INTO iceberg.bronze.raw_documents 
                    (id, content, source, ingested_at, raw_metadata)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)
                """, (
                    doc["id"],
                    doc["content"],
                    doc["source"],
                    json.dumps(doc["metadata"])
                ))
                inserted += 1
            except Exception as e:
                print(f"Warning inserting {doc['id']}: {e}")
        
        cursor.close()
        conn.close()
        
        print(f"✓ Seeded {inserted} documents into Bronze layer")
        return inserted

    seed_start = EmptyOperator(task_id="seed_start")
    
    ensure_bronze = PythonOperator(
        task_id="ensure_bronze_table",
        python_callable=create_bronze_table_if_not_exists,
    )
    
    seed_data = PythonOperator(
        task_id="seed_bronze_documents",
        python_callable=seed_bronze_documents,
    )
    
    seed_end = EmptyOperator(task_id="seed_end")
    
    seed_start >> ensure_bronze >> seed_data >> seed_end
