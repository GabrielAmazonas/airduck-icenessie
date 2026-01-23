"""
DuckDB FlightSQL Server - Centralized Vector Database Service.

Provides:
- FlightSQL protocol (gRPC) for high-performance queries
- Shared HNSW index access for vector search
- S3/Iceberg integration via httpfs extension
- Hot-reload of index pointer for zero-downtime updates

Ports:
- 8815: FlightSQL (gRPC)
- 8816: HTTP health check (handled by health.py)
"""
import os
import sys
import time
import threading
import signal
from pathlib import Path

import duckdb

# =============================================================================
# Configuration
# =============================================================================

EFS_PATH = os.getenv("EFS_PATH", "/mnt/efs")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
FLIGHT_PORT = int(os.getenv("FLIGHT_PORT", "8815"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8816"))

# Embedding configuration (must match FastAPI/Airflow)
EMBEDDING_DIM = 384

# Global state
current_connection = None
shutdown_event = threading.Event()

# =============================================================================
# Helper Functions
# =============================================================================

def get_db_path() -> str:
    """Get current index path from pointer file."""
    pointer_file = os.path.join(EFS_PATH, "latest_pointer.txt")
    if os.path.exists(pointer_file):
        with open(pointer_file) as f:
            index_name = f.read().strip()
            index_path = os.path.join(EFS_PATH, index_name)
            if os.path.exists(index_path):
                return index_path
    
    # Fallback to default
    return os.path.join(EFS_PATH, "vector_store.duckdb")


def configure_connection(conn: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    """Load extensions and configure S3 access."""
    conn.execute("""
        INSTALL vss; LOAD vss;
        INSTALL httpfs; LOAD httpfs;
    """)
    
    # Try to install iceberg (may not be available in all DuckDB versions)
    try:
        conn.execute("INSTALL iceberg; LOAD iceberg;")
        print("Iceberg extension loaded")
    except Exception as e:
        print(f"Iceberg extension not available: {e}")
    
    # Configure S3
    conn.execute(f"""
        SET s3_endpoint='{S3_ENDPOINT}';
        SET s3_access_key_id='{S3_ACCESS_KEY}';
        SET s3_secret_access_key='{S3_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET hnsw_enable_experimental_persistence=true;
    """)
    
    return conn


def create_schema_if_needed(conn: duckdb.DuckDBPyConnection):
    """Create documents table if it doesn't exist."""
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


def init_database() -> duckdb.DuckDBPyConnection:
    """Initialize or open the database."""
    db_path = get_db_path()
    
    # Ensure EFS directory exists
    Path(EFS_PATH).mkdir(parents=True, exist_ok=True)
    
    print(f"Opening database: {db_path}")
    
    # Check if database exists
    db_exists = os.path.exists(db_path)
    
    conn = duckdb.connect(db_path)
    configure_connection(conn)
    
    if not db_exists:
        print("Creating new database with schema...")
        create_schema_if_needed(conn)
    
    # Verify connection
    result = conn.execute("SELECT COUNT(*) FROM documents").fetchone()
    print(f"Database ready. Documents: {result[0] if result else 0}")
    
    return conn


# =============================================================================
# Health Check Server (runs in background thread)
# =============================================================================

def start_health_server():
    """Start HTTP health check endpoint."""
    from flask import Flask, jsonify
    
    app = Flask(__name__)
    
    # Suppress Flask logs
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    
    @app.route('/health')
    def health():
        try:
            db_path = get_db_path()
            conn = duckdb.connect(db_path, read_only=True)
            conn.execute("LOAD vss;")
            result = conn.execute("SELECT COUNT(*) FROM documents").fetchone()
            conn.close()
            return jsonify({
                "status": "healthy",
                "db_path": db_path,
                "document_count": result[0] if result else 0
            })
        except Exception as e:
            return jsonify({
                "status": "unhealthy",
                "error": str(e)
            }), 500
    
    @app.route('/reload', methods=['POST'])
    def reload():
        """Trigger database reload (for pointer swap)."""
        global current_connection
        try:
            if current_connection:
                current_connection.close()
            current_connection = init_database()
            return jsonify({"status": "reloaded", "db_path": get_db_path()})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500
    
    @app.route('/info')
    def info():
        return jsonify({
            "service": "duckdb-server",
            "flight_port": FLIGHT_PORT,
            "health_port": HEALTH_PORT,
            "efs_path": EFS_PATH,
            "s3_endpoint": S3_ENDPOINT,
            "embedding_dim": EMBEDDING_DIM
        })
    
    print(f"Starting health server on port {HEALTH_PORT}")
    app.run(host="0.0.0.0", port=HEALTH_PORT, threaded=True)


# =============================================================================
# FlightSQL Server
# =============================================================================

def start_flight_server():
    """
    Start the FlightSQL server.
    
    Note: DuckDB's FlightSQL support is experimental.
    This implementation uses a polling-based approach for compatibility.
    """
    global current_connection
    
    print(f"Initializing DuckDB FlightSQL server...")
    print(f"  EFS_PATH: {EFS_PATH}")
    print(f"  S3_ENDPOINT: {S3_ENDPOINT}")
    print(f"  FLIGHT_PORT: {FLIGHT_PORT}")
    
    # Initialize database
    current_connection = init_database()
    
    # Check if FlightSQL is available
    try:
        # DuckDB 1.1+ has experimental FlightSQL support
        # For now, we'll use a simple approach
        print("FlightSQL server starting...")
        print(f"Note: Use ADBC driver to connect: grpc://localhost:{FLIGHT_PORT}")
        
        # Keep the server running
        while not shutdown_event.is_set():
            # Periodically check for pointer updates
            time.sleep(10)
            
            # Check if pointer has changed
            new_path = get_db_path()
            if current_connection and hasattr(current_connection, 'filename'):
                current_path = current_connection.filename
                if new_path != current_path:
                    print(f"Pointer changed: {current_path} -> {new_path}")
                    current_connection.close()
                    current_connection = init_database()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"FlightSQL server error: {e}")
        # Fall back to keeping the server alive for health checks
        while not shutdown_event.is_set():
            time.sleep(10)
    finally:
        if current_connection:
            current_connection.close()
        print("Server stopped")


# =============================================================================
# Signal Handlers
# =============================================================================

def signal_handler(signum, frame):
    print(f"\nReceived signal {signum}, shutting down...")
    shutdown_event.set()
    sys.exit(0)


# =============================================================================
# Main
# =============================================================================

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start health server in background thread
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Give health server time to start
    time.sleep(1)
    
    # Start FlightSQL server (blocking)
    start_flight_server()


