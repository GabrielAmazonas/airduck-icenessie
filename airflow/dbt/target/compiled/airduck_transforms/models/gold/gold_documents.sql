

/*
 * Gold Layer: Vector-Ready Documents
 * ===================================
 * - Chunks long documents into smaller pieces
 * - Computes quality scores for filtering
 * - Ready for embedding generation by DuckDB/FastAPI
 */

WITH silver AS (
    SELECT * FROM "iceberg"."silver_silver"."silver_documents"
),

-- Generate chunk indices for documents
chunk_indices AS (
    SELECT 
        id as original_id,
        content,
        source,
        content_hash,
        metadata,
        processed_at,
        CAST(CEIL(LENGTH(content) / 1000.0) AS INTEGER) as num_chunks
    FROM silver
    WHERE LENGTH(content) > 0
),

-- Expand into chunks (simplified chunking - production should use overlap)
chunked AS (
    SELECT 
        original_id || '-' || CAST(chunk_idx AS VARCHAR) as id,
        CASE 
            WHEN num_chunks = 1 THEN content
            ELSE SUBSTR(
                content, 
                (chunk_idx - 1) * 1000 + 1, 
                1000
            )
        END as content,
        chunk_idx as chunk_index,
        num_chunks as total_chunks,
        metadata,
        source,
        content_hash as original_content_hash,
        processed_at
    FROM chunk_indices
    CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(1, num_chunks))) AS t(chunk_idx)
),

-- Compute quality scores
scored AS (
    SELECT 
        *,
        -- Quality score based on content characteristics (0.0 - 1.0)
        (
            -- Length score (0.0 - 0.3)
            CASE 
                WHEN LENGTH(content) > 200 THEN 0.3
                WHEN LENGTH(content) > 100 THEN 0.2
                ELSE 0.1 
            END +
            -- Has proper case (0.0 - 0.2)
            CASE WHEN REGEXP_LIKE(content, '[A-Z]') THEN 0.2 ELSE 0.0 END +
            -- Word count (0.0 - 0.3)
            CASE 
                WHEN CARDINALITY(SPLIT(content, ' ')) > 20 THEN 0.3
                WHEN CARDINALITY(SPLIT(content, ' ')) > 10 THEN 0.2
                ELSE 0.1 
            END +
            -- Not blank (0.0 - 0.2)
            CASE WHEN NOT REGEXP_LIKE(content, '^\s*$') THEN 0.2 ELSE 0.0 END
        ) as quality_score
    FROM chunked
    WHERE LENGTH(TRIM(content)) > 0
)

SELECT 
    id,
    content,
    chunk_index,
    total_chunks,
    metadata,
    source,
    original_content_hash,
    -- Embedding placeholder (computed by DuckDB/FastAPI later)
    CAST(NULL AS ARRAY(REAL)) as embedding,
    quality_score,
    CURRENT_TIMESTAMP as ready_at
FROM scored
WHERE quality_score >= 0.5