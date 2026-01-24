/*
 * Cosine Similarity Macro for Trino
 * ==================================
 * Computes cosine similarity between two ARRAY<REAL> vectors.
 * 
 * Formula: dot_product(a, b) / (magnitude(a) * magnitude(b))
 * 
 * Usage in dbt model:
 *   SELECT id, content,
 *          {{ cosine_similarity('embedding', 'query_vec') }} as score
 *   FROM {{ ref('gold_documents') }}
 *   ORDER BY score DESC
 *   LIMIT 10
 */

{% macro cosine_similarity(vec_column, query_vector) %}
    (
        -- Dot product: SUM(a[i] * b[i])
        reduce(
            zip_with(
                {{ vec_column }}, 
                {{ query_vector }}, 
                (a, b) -> CAST(a AS DOUBLE) * CAST(b AS DOUBLE)
            ),
            CAST(0.0 AS DOUBLE),
            (s, x) -> s + x,
            s -> s
        )
    ) / NULLIF(
        (
            -- Magnitude of vec_column: SQRT(SUM(a[i]^2))
            sqrt(
                reduce(
                    {{ vec_column }},
                    CAST(0.0 AS DOUBLE),
                    (s, x) -> s + CAST(x AS DOUBLE) * CAST(x AS DOUBLE),
                    s -> s
                )
            )
            *
            -- Magnitude of query_vector: SQRT(SUM(b[i]^2))
            sqrt(
                reduce(
                    {{ query_vector }},
                    CAST(0.0 AS DOUBLE),
                    (s, x) -> s + CAST(x AS DOUBLE) * CAST(x AS DOUBLE),
                    s -> s
                )
            )
        ),
        0.0
    )
{% endmacro %}


/*
 * Example: Raw SQL for Trino (without dbt)
 * ========================================
 * 
 * SELECT id, content,
 *     reduce(
 *         zip_with(embedding, ARRAY[0.1, 0.2, ...], (a, b) -> CAST(a AS DOUBLE) * CAST(b AS DOUBLE)),
 *         0.0, (s, x) -> s + x, s -> s
 *     ) / NULLIF(
 *         sqrt(reduce(embedding, 0.0, (s, x) -> s + CAST(x AS DOUBLE) * CAST(x AS DOUBLE), s -> s)) *
 *         sqrt(reduce(ARRAY[0.1, 0.2, ...], 0.0, (s, x) -> s + CAST(x AS DOUBLE) * CAST(x AS DOUBLE), s -> s)),
 *         0.0
 *     ) as similarity
 * FROM iceberg.gold.gold_documents
 * WHERE embedding IS NOT NULL
 *   AND cardinality(embedding) > 0
 * ORDER BY similarity DESC
 * LIMIT 10;
 */
