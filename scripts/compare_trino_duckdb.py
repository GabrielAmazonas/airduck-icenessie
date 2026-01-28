#!/usr/bin/env python3
"""
Compare query results between Trino (Iceberg) and DuckDB.

This script:
1. Checks what tables/data exist in both engines
2. Runs identical queries on both
3. Compares the results for consistency
"""

import os
import sys
import json
from typing import List, Tuple, Optional
import requests

# Configuration
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))


def check_fastapi_health() -> dict:
    """Check FastAPI service health."""
    try:
        resp = requests.get(f"{FASTAPI_URL}/health", timeout=10)
        return resp.json()
    except Exception as e:
        return {"status": "error", "error": str(e)}


def check_duckdb_status() -> dict:
    """Check DuckDB index status."""
    try:
        resp = requests.get(f"{FASTAPI_URL}/status", timeout=10)
        return resp.json()
    except Exception as e:
        return {"status": "error", "error": str(e)}


def check_iceberg_status() -> dict:
    """Check Iceberg/Trino Gold table status."""
    try:
        resp = requests.get(f"{FASTAPI_URL}/iceberg/status", timeout=30)
        return resp.json()
    except Exception as e:
        return {"status": "error", "error": str(e)}


def search_duckdb(query: str, top_k: int = 10, use_hnsw: bool = False) -> List[dict]:
    """Search using DuckDB."""
    try:
        resp = requests.post(
            f"{FASTAPI_URL}/search",
            json={"query": query, "top_k": top_k, "use_hnsw": use_hnsw, "use_vector": True},
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"DuckDB search error: {e}")
        return []


def search_trino(query: str, top_k: int = 10) -> List[dict]:
    """Search using Trino/Iceberg."""
    try:
        resp = requests.post(
            f"{FASTAPI_URL}/search/iceberg",
            json={"query": query, "top_k": top_k},
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Trino search error: {e}")
        return []


def get_trino_tables() -> List[str]:
    """List all tables in Trino Iceberg catalog."""
    import trino
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user="compare_script",
            catalog="iceberg",
        )
        cursor = conn.cursor()
        
        # Get all schemas
        cursor.execute("SHOW SCHEMAS IN iceberg")
        schemas = [row[0] for row in cursor.fetchall()]
        
        tables = []
        for schema in schemas:
            if schema in ('information_schema',):
                continue
            try:
                cursor.execute(f"SHOW TABLES IN iceberg.{schema}")
                for row in cursor.fetchall():
                    tables.append(f"iceberg.{schema}.{row[0]}")
            except Exception:
                pass
        
        cursor.close()
        conn.close()
        return tables
    except Exception as e:
        print(f"Error listing Trino tables: {e}")
        return []


def get_iceberg_documents_count(table: str) -> Tuple[int, int]:
    """Get total count and count with embeddings from an Iceberg table."""
    import trino
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user="compare_script",
            catalog="iceberg",
        )
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN embedding IS NOT NULL 
                           AND cardinality(embedding) > 0 
                      THEN 1 END) as with_embeddings
            FROM {table}
        """)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0], result[1]
    except Exception as e:
        print(f"Error counting documents in {table}: {e}")
        return 0, 0


def compare_search_results(duckdb_results: List[dict], trino_results: List[dict]) -> dict:
    """Compare search results from both engines."""
    comparison = {
        "duckdb_count": len(duckdb_results),
        "trino_count": len(trino_results),
        "matching_ids": 0,
        "matching_order": 0,
        "score_differences": [],
        "id_overlap": [],
        "only_in_duckdb": [],
        "only_in_trino": [],
    }
    
    duckdb_ids = {r["id"]: r for r in duckdb_results}
    trino_ids = {r["id"]: r for r in trino_results}
    
    # Check ID overlap
    common_ids = set(duckdb_ids.keys()) & set(trino_ids.keys())
    comparison["matching_ids"] = len(common_ids)
    comparison["id_overlap"] = list(common_ids)
    comparison["only_in_duckdb"] = list(set(duckdb_ids.keys()) - set(trino_ids.keys()))
    comparison["only_in_trino"] = list(set(trino_ids.keys()) - set(duckdb_ids.keys()))
    
    # Compare scores for matching IDs
    for doc_id in common_ids:
        duckdb_score = duckdb_ids[doc_id]["score"]
        trino_score = trino_ids[doc_id]["score"]
        diff = abs(duckdb_score - trino_score)
        comparison["score_differences"].append({
            "id": doc_id,
            "duckdb_score": round(duckdb_score, 6),
            "trino_score": round(trino_score, 6),
            "difference": round(diff, 6),
        })
    
    # Check if order matches
    if len(duckdb_results) > 0 and len(trino_results) > 0:
        min_len = min(len(duckdb_results), len(trino_results))
        matching_order = sum(
            1 for i in range(min_len) 
            if duckdb_results[i]["id"] == trino_results[i]["id"]
        )
        comparison["matching_order"] = matching_order
    
    return comparison


def print_banner(text: str):
    """Print a banner."""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)


def main():
    """Main comparison function."""
    print_banner("Trino vs DuckDB Query Comparison")
    
    # 1. Check service health
    print("\n📊 Service Status:")
    print("-" * 40)
    
    health = check_fastapi_health()
    print(f"FastAPI: {health.get('status', 'unknown')}")
    
    duckdb_status = check_duckdb_status()
    print(f"DuckDB: {duckdb_status.get('total_documents', 0)} documents, "
          f"HNSW index: {duckdb_status.get('has_vector_index', False)}")
    
    iceberg_status = check_iceberg_status()
    print(f"Iceberg: {iceberg_status.get('status', 'unknown')}")
    if iceberg_status.get('status') == 'error':
        print(f"  Error: {iceberg_status.get('error', 'unknown')}")
    else:
        print(f"  Documents: {iceberg_status.get('total_documents', 0)}, "
              f"With embeddings: {iceberg_status.get('documents_with_embeddings', 0)}")
    
    # 2. List Trino tables
    print("\n📋 Available Iceberg Tables:")
    print("-" * 40)
    tables = get_trino_tables()
    if tables:
        for table in tables:
            print(f"  - {table}")
    else:
        print("  No tables found or error connecting to Trino")
    
    # 3. Check if we can compare
    can_compare = (
        duckdb_status.get('total_documents', 0) > 0 and
        iceberg_status.get('status') not in ('error', 'no_embeddings') and
        iceberg_status.get('documents_with_embeddings', 0) > 0
    )
    
    if not can_compare:
        print_banner("Cannot Compare - Missing Data")
        print("\nTo run a comparison, ensure:")
        print("  1. DuckDB has documents with embeddings (currently: "
              f"{duckdb_status.get('total_documents', 0)})")
        print("  2. Iceberg Gold table exists with embeddings (currently: "
              f"{iceberg_status.get('status', 'error')})")
        print("\nSteps to populate data:")
        print("  1. Run dbt transforms: dbt run (creates Silver/Gold tables)")
        print("  2. Run embed_iceberg_gold DAG (generates embeddings in Iceberg)")
        print("  3. Run daily_reindex DAG (builds DuckDB HNSW index from Iceberg)")
        
        # If only DuckDB has data, show what we can
        if duckdb_status.get('total_documents', 0) > 0:
            print("\n📝 DuckDB Sample Documents:")
            print("-" * 40)
            # List documents in DuckDB
            try:
                import duckdb
                conn = duckdb.connect("/mnt/efs/vector_store.duckdb", read_only=True)
                conn.execute("INSTALL vss; LOAD vss;")
                results = conn.execute("""
                    SELECT id, LEFT(content, 80) as preview 
                    FROM documents 
                    LIMIT 5
                """).fetchall()
                for row in results:
                    print(f"  [{row[0]}] {row[1]}...")
                conn.close()
            except Exception as e:
                print(f"  Could not read DuckDB: {e}")
        
        return 1
    
    # 4. Run comparison queries
    print_banner("Running Comparison Queries")
    
    test_queries = [
        "What is data engineering?",
        "How to build a data pipeline?",
        "Machine learning",
    ]
    
    for query in test_queries:
        print(f"\n🔍 Query: \"{query}\"")
        print("-" * 40)
        
        # Search both engines
        duckdb_results = search_duckdb(query, top_k=5, use_hnsw=False)
        trino_results = search_trino(query, top_k=5)
        
        # Compare results
        comparison = compare_search_results(duckdb_results, trino_results)
        
        print(f"  DuckDB results: {comparison['duckdb_count']}")
        print(f"  Trino results:  {comparison['trino_count']}")
        print(f"  Matching IDs:   {comparison['matching_ids']}")
        print(f"  Matching order: {comparison['matching_order']}")
        
        if comparison['score_differences']:
            print("\n  Score Comparison:")
            for diff in comparison['score_differences'][:5]:
                match = "✅" if diff['difference'] < 0.001 else "⚠️"
                print(f"    {match} {diff['id']}: "
                      f"DuckDB={diff['duckdb_score']:.6f}, "
                      f"Trino={diff['trino_score']:.6f}, "
                      f"diff={diff['difference']:.6f}")
        
        if comparison['only_in_duckdb']:
            print(f"\n  Only in DuckDB: {comparison['only_in_duckdb']}")
        if comparison['only_in_trino']:
            print(f"\n  Only in Trino: {comparison['only_in_trino']}")
    
    print_banner("Comparison Complete")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
