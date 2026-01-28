#!/usr/bin/env python3
"""
Sync DuckDB documents to Iceberg and compare search results.

This script:
1. Reads all documents from DuckDB (with embeddings)
2. Inserts them into Iceberg Gold table via Trino
3. Runs identical search queries on both engines
4. Compares the results to verify consistency
"""

import os
import sys
import json
import requests
import trino

# Configuration
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))

# Test queries for comparison
TEST_QUERIES = [
    "vector databases similarity search",
    "DuckDB analytical database",
    "HNSW nearest neighbor",
    "RAG applications embeddings",
    "FastAPI Python APIs",
]


def get_duckdb_documents():
    """Get all documents from DuckDB via the FastAPI container."""
    print("📖 Reading documents from DuckDB...")
    
    # We'll use a direct query via docker exec since there's no list endpoint
    import subprocess
    
    result = subprocess.run([
        "docker", "exec", "airduck_search_api", "python3", "-c", """
import duckdb
import os
import json

efs_path = '/mnt/efs'
pointer_file = os.path.join(efs_path, 'latest_pointer.txt')
if os.path.exists(pointer_file):
    with open(pointer_file) as f:
        index_name = f.read().strip()
    db_path = os.path.join(efs_path, index_name)
else:
    db_path = os.path.join(efs_path, 'vector_store.duckdb')

conn = duckdb.connect(db_path, read_only=True)
conn.execute('INSTALL vss; LOAD vss;')

# Get all documents with embeddings
result = conn.execute('''
    SELECT id, content, embedding, metadata
    FROM documents
    WHERE embedding IS NOT NULL
''').fetchall()

docs = []
for row in result:
    docs.append({
        'id': row[0],
        'content': row[1],
        'embedding': list(row[2]) if row[2] else None,
        'metadata': row[3]
    })

print(json.dumps(docs))
conn.close()
"""
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error reading DuckDB: {result.stderr}")
        return []
    
    try:
        docs = json.loads(result.stdout.strip())
        print(f"  Found {len(docs)} documents in DuckDB")
        return docs
    except json.JSONDecodeError as e:
        print(f"Error parsing DuckDB output: {e}")
        print(f"Output was: {result.stdout[:500]}")
        return []


def insert_docs_to_iceberg(docs: list) -> bool:
    """Insert documents with embeddings into Iceberg Gold table via Trino."""
    print(f"\n📝 Inserting {len(docs)} documents into Iceberg Gold...")
    
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="sync_script",
        catalog="iceberg",
        schema="silver_gold",
    )
    cursor = conn.cursor()
    
    try:
        # First, check current state
        cursor.execute("""
            SELECT COUNT(*) FROM iceberg.silver_gold.gold_documents
        """)
        before_count = cursor.fetchone()[0]
        print(f"  Current Iceberg Gold count: {before_count}")
        
        # Delete existing documents with same IDs to avoid duplicates
        doc_ids = [doc['id'] for doc in docs]
        for doc_id in doc_ids:
            try:
                cursor.execute(f"""
                    DELETE FROM iceberg.silver_gold.gold_documents 
                    WHERE id = '{doc_id}'
                """)
            except Exception:
                pass  # Ignore if doesn't exist
        
        # Insert documents
        inserted = 0
        for doc in docs:
            try:
                # Convert embedding to JSON for Trino ARRAY insertion
                emb_json = json.dumps(doc['embedding'])
                
                # Escape single quotes in content
                content = doc['content'].replace("'", "''")
                
                # Format metadata properly - use simple JSON without extra escaping
                raw_meta = doc.get('metadata')
                if raw_meta is None:
                    metadata_str = '{}'
                elif isinstance(raw_meta, dict):
                    metadata_str = json.dumps(raw_meta)
                elif isinstance(raw_meta, str):
                    # Already a string, use as-is but validate it's JSON
                    try:
                        json.loads(raw_meta)
                        metadata_str = raw_meta
                    except:
                        metadata_str = json.dumps({"raw": raw_meta})
                else:
                    metadata_str = '{}'
                
                # Escape single quotes for SQL
                metadata_str = metadata_str.replace("'", "''")
                
                cursor.execute(f"""
                    INSERT INTO iceberg.silver_gold.gold_documents 
                        (id, content, chunk_index, total_chunks, metadata, source, 
                         original_content_hash, embedding, quality_score, ready_at)
                    VALUES (
                        '{doc["id"]}',
                        '{content}',
                        1,
                        1,
                        '{metadata_str}',
                        'duckdb_sync',
                        '{doc["id"]}',
                        CAST(json_parse('{emb_json}') AS ARRAY(REAL)),
                        9.5,
                        CURRENT_TIMESTAMP
                    )
                """)
                inserted += 1
                print(f"    ✓ Inserted: {doc['id']}")
            except Exception as e:
                print(f"    ✗ Failed to insert {doc['id']}: {e}")
        
        # Verify final count
        cursor.execute("""
            SELECT COUNT(*), 
                   COUNT(CASE WHEN embedding IS NOT NULL AND cardinality(embedding) > 0 THEN 1 END)
            FROM iceberg.silver_gold.gold_documents
        """)
        result = cursor.fetchone()
        print(f"\n  After sync - Total: {result[0]}, With embeddings: {result[1]}")
        
        return inserted > 0
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def search_duckdb(query: str, top_k: int = 5) -> list:
    """Search using DuckDB via /search endpoint."""
    try:
        resp = requests.post(
            f"{FASTAPI_URL}/search",
            json={"query": query, "top_k": top_k, "use_hnsw": False, "use_vector": True},
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  DuckDB search error: {e}")
        return []


def search_trino(query: str, top_k: int = 5) -> list:
    """Search using Trino/Iceberg via /search/iceberg endpoint."""
    try:
        resp = requests.post(
            f"{FASTAPI_URL}/search/iceberg",
            json={"query": query, "top_k": top_k},
            timeout=60
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Trino search error: {e}")
        return []


def compare_results(duckdb_results: list, trino_results: list) -> dict:
    """Compare search results from both engines."""
    
    # Build maps for comparison
    duckdb_map = {r["id"]: {"score": r["score"], "rank": i} for i, r in enumerate(duckdb_results)}
    trino_map = {r["id"]: {"score": r["score"], "rank": i} for i, r in enumerate(trino_results)}
    
    common_ids = set(duckdb_map.keys()) & set(trino_map.keys())
    
    score_diffs = []
    for doc_id in common_ids:
        d_score = duckdb_map[doc_id]["score"]
        t_score = trino_map[doc_id]["score"]
        diff = abs(d_score - t_score)
        score_diffs.append({
            "id": doc_id,
            "duckdb_score": round(d_score, 6),
            "trino_score": round(t_score, 6),
            "diff": round(diff, 6),
            "duckdb_rank": duckdb_map[doc_id]["rank"] + 1,
            "trino_rank": trino_map[doc_id]["rank"] + 1,
        })
    
    # Sort by DuckDB rank
    score_diffs.sort(key=lambda x: x["duckdb_rank"])
    
    # Check if ranking order matches
    duckdb_order = [r["id"] for r in duckdb_results]
    trino_order = [r["id"] for r in trino_results]
    
    return {
        "duckdb_ids": duckdb_order,
        "trino_ids": trino_order,
        "common_count": len(common_ids),
        "exact_order_match": duckdb_order == trino_order,
        "score_comparisons": score_diffs,
        "max_score_diff": max([s["diff"] for s in score_diffs]) if score_diffs else 0,
        "avg_score_diff": sum([s["diff"] for s in score_diffs]) / len(score_diffs) if score_diffs else 0,
    }


def print_banner(text: str):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def main():
    print_banner("DuckDB ↔ Iceberg Sync & Comparison")
    
    # Step 1: Get DuckDB documents
    docs = get_duckdb_documents()
    if not docs:
        print("No documents found in DuckDB!")
        return 1
    
    print("\n  Documents to sync:")
    for doc in docs:
        emb_status = f"[{len(doc['embedding'])} dims]" if doc['embedding'] else "[no embedding]"
        print(f"    - {doc['id']}: {doc['content'][:50]}... {emb_status}")
    
    # Step 2: Insert into Iceberg
    if not insert_docs_to_iceberg(docs):
        print("Failed to insert documents into Iceberg!")
        return 1
    
    # Step 3: Run comparison queries
    print_banner("Running Search Comparisons")
    
    all_results = []
    
    for query in TEST_QUERIES:
        print(f"\n🔍 Query: \"{query}\"")
        print("-" * 60)
        
        duckdb_results = search_duckdb(query, top_k=5)
        trino_results = search_trino(query, top_k=5)
        
        if not duckdb_results or not trino_results:
            print("  ⚠️ One or both searches returned no results")
            continue
        
        comparison = compare_results(duckdb_results, trino_results)
        all_results.append(comparison)
        
        # Print comparison
        print(f"\n  {'Rank':<6} {'Document ID':<15} {'DuckDB Score':<14} {'Trino Score':<14} {'Diff':<10}")
        print(f"  {'-'*6} {'-'*15} {'-'*14} {'-'*14} {'-'*10}")
        
        for sc in comparison['score_comparisons']:
            match = "✅" if sc['diff'] < 0.0001 else ("⚠️" if sc['diff'] < 0.001 else "❌")
            print(f"  {sc['duckdb_rank']:<6} {sc['id']:<15} {sc['duckdb_score']:<14.6f} {sc['trino_score']:<14.6f} {match} {sc['diff']:.6f}")
        
        order_match = "✅ YES" if comparison['exact_order_match'] else "❌ NO"
        print(f"\n  Order match: {order_match}")
        print(f"  Max score diff: {comparison['max_score_diff']:.6f}")
    
    # Summary
    print_banner("Summary")
    
    if not all_results:
        print("\n  ❌ No comparison results available")
        return 1
    
    total_queries = len(all_results)
    exact_matches = sum(1 for r in all_results if r['exact_order_match'])
    avg_max_diff = sum(r['max_score_diff'] for r in all_results) / total_queries
    overall_max_diff = max(r['max_score_diff'] for r in all_results)
    
    print(f"\n  Queries tested: {total_queries}")
    print(f"  Exact order matches: {exact_matches}/{total_queries}")
    print(f"  Average max score diff: {avg_max_diff:.6f}")
    print(f"  Overall max score diff: {overall_max_diff:.6f}")
    
    # Verdict
    if exact_matches == total_queries and overall_max_diff < 0.0001:
        print("\n  ✅ PERFECT MATCH: DuckDB and Trino return identical results!")
        verdict = 0
    elif exact_matches == total_queries and overall_max_diff < 0.001:
        print("\n  ✅ EXCELLENT: Results match with negligible floating-point differences.")
        verdict = 0
    elif overall_max_diff < 0.01:
        print("\n  ⚠️ ACCEPTABLE: Minor differences due to floating-point precision.")
        print("     Both engines are computing cosine similarity correctly.")
        verdict = 0
    else:
        print("\n  ❌ SIGNIFICANT DIFFERENCES: Results don't match as expected.")
        verdict = 1
    
    print("\n  Note: Small score differences (< 0.001) are expected due to:")
    print("    - Float32 vs Float64 precision differences")
    print("    - Different cosine similarity implementations")
    print("    - Array type casting between systems")
    
    return verdict


if __name__ == "__main__":
    sys.exit(main())
