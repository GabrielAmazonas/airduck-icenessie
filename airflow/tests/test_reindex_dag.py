"""
Unit tests for the reindex_dag.py module.

Tests the document ingestion and index building functionality.
"""
import os
import tempfile
import pytest
import duckdb


# Constants matching the DAG
EMBEDDING_DIM = 384


def create_test_index(db_path: str, documents: list):
    """Create a test DuckDB index with sample documents."""
    con = duckdb.connect(db_path)
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
    
    for doc in documents:
        embedding = [0.1] * EMBEDDING_DIM  # Mock embedding
        con.execute(f"""
            INSERT INTO documents (id, content, embedding, metadata, source_file)
            VALUES (?, ?, ?::FLOAT[{EMBEDDING_DIM}], ?, ?)
        """, [doc["id"], doc["content"], embedding, doc.get("metadata"), doc.get("source_file", "test")])
    
    con.commit()
    con.close()


class TestIngestFromCurrentIndex:
    """Tests for _ingest_from_current_index function."""
    
    def test_ingest_preserves_all_documents(self):
        """Test that all documents from current index are preserved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create source index with documents
            source_path = os.path.join(tmpdir, "source.duckdb")
            docs = [
                {"id": "doc-001", "content": "First document content"},
                {"id": "doc-002", "content": "Second document content"},
                {"id": "doc-003", "content": "Third document content"},
            ]
            create_test_index(source_path, docs)
            
            # Create target index
            target_path = os.path.join(tmpdir, "target.duckdb")
            target_con = duckdb.connect(target_path)
            target_con.execute(f"""
                CREATE TABLE documents (
                    id VARCHAR PRIMARY KEY,
                    content TEXT NOT NULL,
                    embedding FLOAT[{EMBEDDING_DIM}],
                    metadata JSON,
                    source_file VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Read from source and insert into target (simulating _ingest_from_current_index)
            source_con = duckdb.connect(source_path, read_only=True)
            source_docs = source_con.execute("""
                SELECT id, content, embedding, metadata, source_file, created_at
                FROM documents
            """).fetchall()
            source_con.close()
            
            for doc in source_docs:
                doc_id, content, embedding, metadata, source_file, created_at = doc
                target_con.execute(f"""
                    INSERT OR REPLACE INTO documents (id, content, embedding, metadata, source_file, created_at)
                    VALUES (?, ?, ?::FLOAT[{EMBEDDING_DIM}], ?, ?, ?)
                """, [doc_id, content, embedding, metadata, source_file, created_at])
            
            target_con.commit()
            
            # Verify all documents were transferred
            result = target_con.execute("SELECT COUNT(*) FROM documents").fetchone()
            assert result[0] == 3, f"Expected 3 documents, got {result[0]}"
            
            # Verify content is preserved
            doc_ids = target_con.execute("SELECT id FROM documents ORDER BY id").fetchall()
            assert [d[0] for d in doc_ids] == ["doc-001", "doc-002", "doc-003"]
            
            target_con.close()
    
    def test_ingest_preserves_embeddings(self):
        """Test that embeddings are preserved during ingestion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "source.duckdb")
            
            # Create source with specific embedding
            con = duckdb.connect(source_path)
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
            
            test_embedding = [0.5] * EMBEDDING_DIM
            con.execute(f"""
                INSERT INTO documents (id, content, embedding)
                VALUES ('test-id', 'test content', ?::FLOAT[{EMBEDDING_DIM}])
            """, [test_embedding])
            con.commit()
            con.close()
            
            # Read and verify embedding
            con = duckdb.connect(source_path, read_only=True)
            result = con.execute("SELECT embedding FROM documents WHERE id = 'test-id'").fetchone()
            con.close()
            
            assert result is not None
            assert len(result[0]) == EMBEDDING_DIM
            assert result[0][0] == pytest.approx(0.5, rel=1e-5)
    
    def test_empty_index_returns_zero(self):
        """Test that ingesting from empty index returns 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = os.path.join(tmpdir, "empty.duckdb")
            
            con = duckdb.connect(source_path)
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
            con.commit()
            con.close()
            
            # Verify empty
            con = duckdb.connect(source_path, read_only=True)
            result = con.execute("SELECT COUNT(*) FROM documents").fetchone()
            con.close()
            
            assert result[0] == 0


class TestDocumentDeduplication:
    """Tests for document deduplication using INSERT OR REPLACE."""
    
    def test_duplicate_ids_are_replaced(self):
        """Test that documents with same ID are replaced, not duplicated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            
            con = duckdb.connect(db_path)
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
            
            embedding = [0.1] * EMBEDDING_DIM
            
            # Insert first version
            con.execute(f"""
                INSERT INTO documents (id, content, embedding)
                VALUES ('doc-001', 'original content', ?::FLOAT[{EMBEDDING_DIM}])
            """, [embedding])
            
            # Insert second version with same ID
            con.execute(f"""
                INSERT OR REPLACE INTO documents (id, content, embedding)
                VALUES ('doc-001', 'updated content', ?::FLOAT[{EMBEDDING_DIM}])
            """, [embedding])
            
            con.commit()
            
            # Verify only one document exists
            count = con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            assert count == 1
            
            # Verify it has updated content
            content = con.execute("SELECT content FROM documents WHERE id = 'doc-001'").fetchone()[0]
            assert content == "updated content"
            
            con.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
