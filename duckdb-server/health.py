"""
Standalone health check script for DuckDB server.

Can be used independently for debugging.
"""
import os
import duckdb

EFS_PATH = os.getenv("EFS_PATH", "/mnt/efs")


def get_db_path() -> str:
    """Get current index path from pointer file."""
    pointer_file = os.path.join(EFS_PATH, "latest_pointer.txt")
    if os.path.exists(pointer_file):
        with open(pointer_file) as f:
            index_name = f.read().strip()
            index_path = os.path.join(EFS_PATH, index_name)
            if os.path.exists(index_path):
                return index_path
    return os.path.join(EFS_PATH, "vector_store.duckdb")


def check_health():
    """Check database health."""
    try:
        db_path = get_db_path()
        print(f"Database path: {db_path}")
        print(f"Exists: {os.path.exists(db_path)}")
        
        conn = duckdb.connect(db_path, read_only=True)
        conn.execute("INSTALL vss; LOAD vss;")
        
        result = conn.execute("SELECT COUNT(*) FROM documents").fetchone()
        print(f"Document count: {result[0] if result else 0}")
        
        # Check HNSW index
        try:
            idx_result = conn.execute("""
                SELECT COUNT(*) FROM duckdb_indexes() 
                WHERE index_name = 'embedding_idx'
            """).fetchone()
            print(f"HNSW index exists: {idx_result[0] > 0 if idx_result else False}")
        except:
            print("HNSW index check failed")
        
        conn.close()
        print("Health: OK")
        return True
        
    except Exception as e:
        print(f"Health: FAILED - {e}")
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if check_health() else 1)


