#!/usr/bin/env python3
"""
Populate both Trino (Iceberg) and DuckDB with identical test documents,
then compare query results to verify consistency.
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

# Test documents - same ones that will be inserted into both engines
TEST_DOCUMENTS = [
    {
        "id": "compare-001",
        "content": "Vector databases enable efficient similarity search across high-dimensional data. "
                   "They use algorithms like HNSW (Hierarchical Navigable Small World) to achieve "
                   "logarithmic search complexity instead of linear scans.",
    },
    {
        "id": "compare-002", 
        "content": "DuckDB is an embedded analytical database that provides blazing fast OLAP queries. "
                   "It supports columnar storage, vectorized execution, and can process data directly "
                   "from Parquet files without loading them into memory.",
    },
    {
        "id": "compare-003",
        "content": "Apache Trino is a distributed SQL query engine designed for interactive analytics. "
                   "It can query data from multiple sources including Iceberg, Hive, PostgreSQL, and "
                   "object storage systems like S3.",
    },
    {
        "id": "compare-004",
        "content": "Retrieval Augmented Generation (RAG) combines large language models with external "
                   "knowledge retrieval. The system first searches for relevant documents using semantic "
                   "similarity, then provides them as context to the LLM for generating accurate responses.",
    },
    {
        "id": "compare-005",
        "content": "Cosine similarity measures the angle between two vectors, producing a value between -1 "
                   "and 1. For normalized embeddings, this is equivalent to the dot product. It's commonly "
                   "used in NLP for comparing document and query embeddings.",
    },
]

# Test queries for comparison
TEST_QUERIES = [
    "How does vector similarity search work?",
    "What is DuckDB used for?",
    "distributed SQL query engine",
    "RAG and language models",
    "cosine similarity embeddings",
]


def generate_embeddings(texts: list) -> list:
    """Generate embeddings using FastAPI endpoint."""
    resp = requests.post(
        f"{FASTAPI_URL}/embed/batch",
        json={"texts": texts},
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()["embeddings"]


def clear_comparison_docs_duckdb():
    """Clear comparison documents from DuckDB."""
    print("  Clearing existing comparison docs from DuckDB...")
    for doc in TEST_DOCUMENTS:
        try:
            requests.delete(f"{FASTAPI_URL}/documents/{doc['id']}", timeout=10)
        except:
            pass


def insert_docs_to_duckdb(docs: list, embeddings: list) -> bool:
    """Insert documents with embeddings into DuckDB."""
    print("  Inserting documents into DuckDB...")
    
    batch = []
    for doc, emb in zip(docs, embeddings):
        batch.append({
            "id": doc["id"],
            "content": doc["content"],
            "embedding": emb,
            "metadata": {"source": "comparison_test"}
        })
    
    resp = requests.post(
        f"{FASTAPI_URL}/index/batch",
        json=batch,
        timeout=60
    )
    
    if resp.status_code == 200:
        print(f"    ✅ Inserted {len(batch)} documents into DuckDB")
        return True
    else:
        print(f"    ❌ Failed: {resp.text}")
        return False


def insert_docs_to_iceberg(docs: list, embeddings: list) -> bool:
    """Insert documents with embeddings into Iceberg Gold table via Trino."""
    print("  Inserting documents into Iceberg Gold...")
    
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="comparison_script",
        catalog="iceberg",
        schema="silver_gold",
    )
    cursor = conn.cursor()
    
    try:
        # First, delete any existing comparison docs
        cursor.execute("""
            DELETE FROM iceberg.silver_gold.gold_documents 
            WHERE id LIKE 'compare-%'
        """)
        
        # Insert new documents
        for doc, emb in zip(docs, embeddings):
            # Convert embedding to JSON for Trino ARRAY insertion
            emb_json = json.dumps(emb)
            
            cursor.execute(f"""
                INSERT INTO iceberg.silver_gold.gold_documents 
                    (id, content, chunk_index, total_chunks, metadata, source, 
                     original_content_hash, embedding, quality_score, ready_at)
                VALUES (
                    '{doc["id"]}',
                    '{doc["content"].replace("'", "''")}',
                    1,
                    1,
                    '{{"source": "comparison_test"}}',
                    'comparison_test',
                    '{doc["id"]}',
                    CAST(json_parse('{emb_json}') AS ARRAY(REAL)),
                    9.0,
                    CURRENT_TIMESTAMP
                )
            """)
        
        print(f"    ✅ Inserted {len(docs)} documents into Iceberg Gold")
        return True
        
    except Exception as e:
        print(f"    ❌ Failed: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def search_duckdb(query: str, top_k: int = 5) -> list:
    """Search using DuckDB."""
    resp = requests.post(
        f"{FASTAPI_URL}/search",
        json={"query": query, "top_k": top_k, "use_hnsw": False, "use_vector": True},
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()


def search_trino(query: str, top_k: int = 5) -> list:
    """Search using Trino/Iceberg."""
    resp = requests.post(
        f"{FASTAPI_URL}/search/iceberg",
        json={"query": query, "top_k": top_k},
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()


def compare_results(query: str, duckdb_results: list, trino_results: list) -> dict:
    """Compare search results from both engines."""
    
    # Extract IDs and scores
    duckdb_map = {r["id"]: r["score"] for r in duckdb_results}
    trino_map = {r["id"]: r["score"] for r in trino_results}
    
    # Compare
    common_ids = set(duckdb_map.keys()) & set(trino_map.keys())
    
    score_diffs = []
    for doc_id in common_ids:
        diff = abs(duckdb_map[doc_id] - trino_map[doc_id])
        score_diffs.append({
            "id": doc_id,
            "duckdb": round(duckdb_map[doc_id], 6),
            "trino": round(trino_map[doc_id], 6),
            "diff": round(diff, 6)
        })
    
    # Sort by DuckDB score
    score_diffs.sort(key=lambda x: x["duckdb"], reverse=True)
    
    return {
        "query": query,
        "duckdb_count": len(duckdb_results),
        "trino_count": len(trino_results),
        "common_ids": len(common_ids),
        "duckdb_ids": [r["id"] for r in duckdb_results],
        "trino_ids": [r["id"] for r in trino_results],
        "score_comparisons": score_diffs,
        "exact_match": [r["id"] for r in duckdb_results] == [r["id"] for r in trino_results],
        "max_score_diff": max([s["diff"] for s in score_diffs]) if score_diffs else 0,
    }


def print_banner(text: str):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def main():
    print_banner("Trino vs DuckDB Query Comparison Test")
    
    # Step 1: Generate embeddings for test documents
    print("\n📊 Step 1: Generating embeddings for test documents...")
    texts = [doc["content"] for doc in TEST_DOCUMENTS]
    
    try:
        embeddings = generate_embeddings(texts)
        print(f"  ✅ Generated {len(embeddings)} embeddings (dim={len(embeddings[0])})")
    except Exception as e:
        print(f"  ❌ Failed to generate embeddings: {e}")
        return 1
    
    # Step 2: Insert into DuckDB
    print("\n📊 Step 2: Populating DuckDB...")
    clear_comparison_docs_duckdb()
    if not insert_docs_to_duckdb(TEST_DOCUMENTS, embeddings):
        return 1
    
    # Step 3: Insert into Iceberg
    print("\n📊 Step 3: Populating Iceberg Gold via Trino...")
    if not insert_docs_to_iceberg(TEST_DOCUMENTS, embeddings):
        print("  ⚠️ Iceberg insertion failed - may need dbt transforms first")
        return 1
    
    # Step 4: Verify counts match
    print("\n📊 Step 4: Verifying document counts...")
    
    duckdb_status = requests.get(f"{FASTAPI_URL}/status").json()
    iceberg_status = requests.get(f"{FASTAPI_URL}/iceberg/status").json()
    
    print(f"  DuckDB documents: {duckdb_status.get('total_documents', 0)}")
    print(f"  Iceberg documents with embeddings: {iceberg_status.get('documents_with_embeddings', 0)}")
    
    # Step 5: Run comparison queries
    print_banner("Running Comparison Queries")
    
    all_results = []
    all_match = True
    
    for query in TEST_QUERIES:
        print(f"\n🔍 Query: \"{query}\"")
        print("-" * 60)
        
        try:
            duckdb_results = search_duckdb(query, top_k=5)
            trino_results = search_trino(query, top_k=5)
            
            comparison = compare_results(query, duckdb_results, trino_results)
            all_results.append(comparison)
            
            # Print results
            print(f"  DuckDB returned: {comparison['duckdb_ids']}")
            print(f"  Trino returned:  {comparison['trino_ids']}")
            print(f"  Order match: {'✅ YES' if comparison['exact_match'] else '❌ NO'}")
            
            if comparison['score_comparisons']:
                print("\n  Score comparison (same documents):")
                for sc in comparison['score_comparisons']:
                    match_symbol = "✅" if sc['diff'] < 0.0001 else ("⚠️" if sc['diff'] < 0.001 else "❌")
                    print(f"    {match_symbol} {sc['id']}: DuckDB={sc['duckdb']:.6f}, Trino={sc['trino']:.6f}, diff={sc['diff']:.6f}")
            
            if not comparison['exact_match']:
                all_match = False
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
            all_match = False
    
    # Summary
    print_banner("Summary")
    
    avg_max_diff = sum(r['max_score_diff'] for r in all_results) / len(all_results) if all_results else 0
    exact_matches = sum(1 for r in all_results if r['exact_match'])
    
    print(f"\n  Total queries tested: {len(TEST_QUERIES)}")
    print(f"  Exact result order matches: {exact_matches}/{len(TEST_QUERIES)}")
    print(f"  Average max score difference: {avg_max_diff:.6f}")
    
    if all_match:
        print("\n  ✅ ALL QUERIES RETURNED IDENTICAL RESULTS")
        print("  Trino and DuckDB produce consistent results for the same embeddings.")
    else:
        print("\n  ⚠️ SOME DIFFERENCES DETECTED")
        print("  This may be due to floating-point precision differences between engines.")
        print("  Small differences (< 0.001) in scores are generally acceptable.")
    
    # Check if differences are within acceptable tolerance
    max_diff_overall = max(r['max_score_diff'] for r in all_results) if all_results else 0
    if max_diff_overall < 0.001:
        print(f"\n  ✅ Max score difference ({max_diff_overall:.6f}) is within acceptable tolerance (< 0.001)")
        return 0
    else:
        print(f"\n  ❌ Max score difference ({max_diff_overall:.6f}) exceeds acceptable tolerance (< 0.001)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
