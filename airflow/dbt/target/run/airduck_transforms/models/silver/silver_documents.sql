
  
    

    create table "iceberg"."silver_silver"."silver_documents"
      
      
    as (
      

/*
 * Silver Layer: Deduplicated and Cleaned Documents
 * ================================================
 * - Removes duplicate content via SHA256 hash
 * - Filters by content length
 * - Parses metadata from JSON string
 */

WITH raw AS (
    SELECT 
        id,
        content,
        source,
        raw_metadata as metadata,
        ingested_at,
        -- Deduplication hash (case-insensitive, trimmed)
        TO_HEX(SHA256(TO_UTF8(LOWER(TRIM(content))))) as content_hash
    FROM "iceberg"."bronze"."raw_documents"
    
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY content_hash 
            ORDER BY ingested_at DESC
        ) as rn
    FROM raw
)

SELECT 
    id,
    content,
    content_hash,
    source,
    metadata,
    CURRENT_TIMESTAMP as processed_at
FROM deduplicated
WHERE rn = 1
  AND LENGTH(content) >= 50
  AND LENGTH(content) <= 10000
    );

  