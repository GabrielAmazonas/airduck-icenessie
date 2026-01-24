"""
Iceberg Table Maintenance DAG - Addresses the Small Files Problem.

This DAG runs Iceberg maintenance procedures to optimize table storage:
1. rewrite_data_files - Compacts small files into larger ones
2. expire_snapshots - Removes old snapshots (enables garbage collection)
3. remove_orphan_files - Cleans up orphaned data files
4. rewrite_manifests - Optimizes manifest files for faster planning

The Small Files Problem:
- Many small files = slow queries (many file opens, high S3 API costs)
- Caused by: frequent small writes, streaming ingestion, many partitions
- Solution: Periodic compaction to merge small files into larger ones

Schedule: Daily at 3 AM (after dbt transforms and embedding generation)
"""
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# =============================================================================
# Configuration
# =============================================================================
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")

# Tables to maintain (schema.table format)
ICEBERG_TABLES = [
    "iceberg.bronze.raw_documents",
    "iceberg.silver_silver.silver_documents",
    "iceberg.silver_gold.gold_documents",
]

# Maintenance settings
SNAPSHOT_RETENTION_DAYS = 7  # Keep snapshots for 7 days (time travel)
TARGET_FILE_SIZE_MB = 128  # Target file size after compaction
MIN_FILES_TO_COMPACT = 5  # Minimum small files to trigger compaction

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# =============================================================================
# Maintenance Functions
# =============================================================================

def get_trino_connection():
    """Create Trino connection for maintenance operations."""
    import trino
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user="airflow",
        catalog="iceberg",
    )


def analyze_table_files(**context):
    """Analyze current file distribution for each table."""
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    results = {}
    
    for table in ICEBERG_TABLES:
        try:
            # Get file statistics from Iceberg metadata
            # Trino uses "schema.table$files" syntax for metadata tables
            # We need to quote the schema.table$files part properly
            parts = table.split('.')
            if len(parts) == 3:
                catalog, schema, tbl = parts
                metadata_table = f'"{catalog}"."{schema}"."{tbl}$files"'
            else:
                metadata_table = f'"{table}$files"'
            
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as file_count,
                    COALESCE(SUM(file_size_in_bytes), 0) / 1024 / 1024 as total_size_mb,
                    COALESCE(AVG(file_size_in_bytes), 0) / 1024 / 1024 as avg_size_mb,
                    COALESCE(MIN(file_size_in_bytes), 0) / 1024 / 1024 as min_size_mb,
                    COALESCE(MAX(file_size_in_bytes), 0) / 1024 / 1024 as max_size_mb
                FROM {metadata_table}
            """)
            row = cursor.fetchone()
            
            if row and row[0] > 0:
                results[table] = {
                    "file_count": row[0],
                    "total_size_mb": round(row[1], 2) if row[1] else 0,
                    "avg_size_mb": round(row[2], 2) if row[2] else 0,
                    "min_size_mb": round(row[3], 2) if row[3] else 0,
                    "max_size_mb": round(row[4], 2) if row[4] else 0,
                }
                print(f"✓ {table}:")
                print(f"    Files: {results[table]['file_count']}")
                print(f"    Total: {results[table]['total_size_mb']} MB")
                print(f"    Avg size: {results[table]['avg_size_mb']} MB")
                print(f"    Range: {results[table]['min_size_mb']} - {results[table]['max_size_mb']} MB")
            else:
                results[table] = {"file_count": 0, "total_size_mb": 0}
                print(f"⏭ {table}: No files found (table may be empty or not exist)")
                
        except Exception as e:
            print(f"⚠ {table}: Error analyzing - {e}")
            results[table] = {"error": str(e), "file_count": 0}
    
    cursor.close()
    conn.close()
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='file_analysis', value=results)
    return results


def compact_small_files(**context):
    """
    Rewrite data files to compact small files into larger ones.
    
    This is the primary solution to the small files problem.
    Iceberg rewrites files to meet target size while preserving data.
    """
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    file_analysis = context['ti'].xcom_pull(key='file_analysis', task_ids='analyze_files')
    
    # Handle case where XCom data is not available
    if file_analysis is None:
        print("⚠ No file analysis data available from previous task")
        file_analysis = {}
    
    compacted = 0
    skipped = 0
    errors = 0
    
    for table in ICEBERG_TABLES:
        try:
            analysis = file_analysis.get(table, {})
            
            # Skip if there was an error analyzing this table
            if 'error' in analysis:
                print(f"⏭ {table}: Skipping - previous analysis error: {analysis['error']}")
                skipped += 1
                continue
            
            file_count = analysis.get('file_count', 0)
            avg_size = analysis.get('avg_size_mb', 0)
            
            # Skip if no files
            if file_count == 0:
                print(f"⏭ {table}: Skipping - no files found")
                skipped += 1
                continue
            
            # Skip if not enough files or already optimal
            if file_count < MIN_FILES_TO_COMPACT:
                print(f"⏭ {table}: Skipping - only {file_count} files (min: {MIN_FILES_TO_COMPACT})")
                skipped += 1
                continue
            
            if avg_size >= TARGET_FILE_SIZE_MB * 0.8:  # Within 80% of target
                print(f"⏭ {table}: Skipping - avg size {avg_size}MB already optimal")
                skipped += 1
                continue
            
            print(f"🔄 {table}: Compacting {file_count} files (avg {avg_size}MB → target {TARGET_FILE_SIZE_MB}MB)...")
            
            # Try Trino optimize command first (simpler syntax)
            try:
                cursor.execute(f"ALTER TABLE {table} EXECUTE optimize")
                print(f"✓ {table}: Compaction complete via optimize")
                compacted += 1
            except Exception as e:
                print(f"⚠ {table}: optimize failed ({e}), trying rewrite_data_files...")
                try:
                    # Alternative: Use Iceberg's rewrite_data_files procedure
                    cursor.execute(f"CALL iceberg.system.rewrite_data_files(table => '{table}')")
                    print(f"✓ {table}: Compaction complete via rewrite_data_files")
                    compacted += 1
                except Exception as e2:
                    print(f"✗ {table}: Compaction failed - {e2}")
                    errors += 1
            
        except Exception as e:
            print(f"✗ {table}: Unexpected error - {e}")
            errors += 1
    
    cursor.close()
    conn.close()
    
    print(f"\n📊 Compaction summary: {compacted} compacted, {skipped} skipped, {errors} errors")
    return {"compacted": compacted, "skipped": skipped, "errors": errors}


def expire_old_snapshots(**context):
    """
    Expire old snapshots to enable garbage collection.
    
    Iceberg keeps snapshots for time travel. Old snapshots reference
    old data files, preventing their deletion. Expiring snapshots
    marks old data files as eligible for removal.
    """
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    # Calculate expiration timestamp
    from datetime import datetime, timedelta
    expire_before = datetime.utcnow() - timedelta(days=SNAPSHOT_RETENTION_DAYS)
    expire_timestamp = expire_before.strftime("%Y-%m-%d %H:%M:%S.000")
    
    print(f"🗓 Expiring snapshots older than {expire_timestamp} ({SNAPSHOT_RETENTION_DAYS} days)")
    
    expired = 0
    
    for table in ICEBERG_TABLES:
        try:
            # First, check how many snapshots exist
            cursor.execute(f"""
                SELECT COUNT(*) as snapshot_count
                FROM "{table}$snapshots"
            """)
            row = cursor.fetchone()
            snapshot_count = row[0] if row else 0
            
            if snapshot_count <= 1:
                print(f"⏭ {table}: Only {snapshot_count} snapshot(s), keeping all")
                continue
            
            print(f"🔄 {table}: {snapshot_count} snapshots, expiring old ones...")
            
            # Expire old snapshots
            cursor.execute(f"""
                CALL iceberg.system.expire_snapshots(
                    table => '{table}',
                    older_than => TIMESTAMP '{expire_timestamp}',
                    retain_last => 2
                )
            """)
            
            print(f"✓ {table}: Snapshots expired (keeping last 2 and those within {SNAPSHOT_RETENTION_DAYS} days)")
            expired += 1
            
        except Exception as e:
            print(f"⚠ {table}: Snapshot expiration note - {e}")
    
    cursor.close()
    conn.close()
    
    print(f"\n📊 Snapshot expiration: {expired} tables processed")
    return {"tables_processed": expired}


def remove_orphan_files(**context):
    """
    Remove orphan files that are no longer referenced by any snapshot.
    
    After compaction and snapshot expiration, old data files may remain
    on storage but not referenced by any snapshot. This cleans them up.
    """
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    # Only remove files orphaned for at least 3 days (safety buffer)
    from datetime import datetime, timedelta
    orphan_before = datetime.utcnow() - timedelta(days=3)
    orphan_timestamp = orphan_before.strftime("%Y-%m-%d %H:%M:%S.000")
    
    print(f"🧹 Removing orphan files older than {orphan_timestamp}")
    
    removed = 0
    
    for table in ICEBERG_TABLES:
        try:
            print(f"🔄 {table}: Checking for orphan files...")
            
            cursor.execute(f"""
                CALL iceberg.system.remove_orphan_files(
                    table => '{table}',
                    older_than => TIMESTAMP '{orphan_timestamp}'
                )
            """)
            
            print(f"✓ {table}: Orphan files removed")
            removed += 1
            
        except Exception as e:
            print(f"⚠ {table}: Orphan removal note - {e}")
    
    cursor.close()
    conn.close()
    
    print(f"\n📊 Orphan removal: {removed} tables processed")
    return {"tables_processed": removed}


def verify_maintenance(**context):
    """Verify maintenance results by re-analyzing tables."""
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    before = context['ti'].xcom_pull(key='file_analysis', task_ids='analyze_files')
    if before is None:
        before = {}
    
    print("=" * 60)
    print("Maintenance Results Summary")
    print("=" * 60)
    
    for table in ICEBERG_TABLES:
        try:
            # Build proper metadata table reference
            parts = table.split('.')
            if len(parts) == 3:
                catalog, schema, tbl = parts
                metadata_table = f'"{catalog}"."{schema}"."{tbl}$files"'
            else:
                metadata_table = f'"{table}$files"'
            
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as file_count,
                    COALESCE(SUM(file_size_in_bytes), 0) / 1024 / 1024 as total_size_mb,
                    COALESCE(AVG(file_size_in_bytes), 0) / 1024 / 1024 as avg_size_mb
                FROM {metadata_table}
            """)
            row = cursor.fetchone()
            
            if row and row[0] > 0:
                before_data = before.get(table, {})
                before_count = before_data.get('file_count', 0)
                after_count = row[0]
                before_avg = before_data.get('avg_size_mb', 0)
                after_avg = round(row[2], 2) if row[2] else 0
                
                file_reduction = before_count - after_count if before_count > 0 else 0
                size_improvement = after_avg - before_avg if before_avg > 0 else 0
                
                print(f"\n{table}:")
                print(f"  Files: {before_count} → {after_count} ({file_reduction:+d})")
                print(f"  Avg size: {before_avg}MB → {after_avg}MB ({size_improvement:+.1f}MB)")
                
        except Exception as e:
            print(f"\n{table}: Could not verify - {e}")
    
    print("=" * 60)
    
    cursor.close()
    conn.close()


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="iceberg_maintenance",
    default_args=default_args,
    description="Iceberg table maintenance - compaction, snapshot expiration, orphan cleanup",
    schedule_interval="0 3 * * *",  # Run at 3 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["iceberg", "maintenance", "compaction", "optimization"],
    doc_md="""
    ## Iceberg Table Maintenance
    
    Addresses the **Small Files Problem** in data lakes.
    
    ### Why This Matters
    
    | Problem | Impact | Solution |
    |---------|--------|----------|
    | Many small files | Slow queries, high S3 API costs | `compact_small_files` |
    | Old snapshots | Storage bloat, orphan files | `expire_old_snapshots` |
    | Orphan files | Wasted storage costs | `remove_orphan_files` |
    
    ### Tasks
    
    1. **analyze_files** - Inventory current file distribution
    2. **compact_small_files** - Merge small files into target size (128MB)
    3. **expire_old_snapshots** - Remove snapshots older than 7 days
    4. **remove_orphan_files** - Clean up unreferenced data files
    5. **verify_maintenance** - Report before/after comparison
    
    ### Configuration
    
    - `SNAPSHOT_RETENTION_DAYS`: 7 (keep for time travel)
    - `TARGET_FILE_SIZE_MB`: 128 (optimal for cloud storage)
    - `MIN_FILES_TO_COMPACT`: 5 (avoid unnecessary compaction)
    
    ### Schedule
    
    Daily at 3 AM (after transforms and embeddings complete)
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    
    analyze = PythonOperator(
        task_id="analyze_files",
        python_callable=analyze_table_files,
    )
    
    compact = PythonOperator(
        task_id="compact_small_files",
        python_callable=compact_small_files,
        execution_timeout=timedelta(hours=2),  # Compaction can be slow
    )
    
    expire_snapshots = PythonOperator(
        task_id="expire_old_snapshots",
        python_callable=expire_old_snapshots,
    )
    
    remove_orphans = PythonOperator(
        task_id="remove_orphan_files",
        python_callable=remove_orphan_files,
    )
    
    verify = PythonOperator(
        task_id="verify_maintenance",
        python_callable=verify_maintenance,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Task dependencies
    # Compact first, then expire snapshots, then remove orphans
    start >> analyze >> compact >> expire_snapshots >> remove_orphans >> verify >> end
