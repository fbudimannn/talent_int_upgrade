WITH source AS (
    SELECT * FROM {{ source('raw_source', 'papi_scores') }}
),

medians AS (
    SELECT DISTINCT
        scale_code, 
        -- Syntax BigQuery untuk Median
        PERCENTILE_CONT(score, 0.5) OVER(PARTITION BY scale_code) as median_score
    FROM source
    WHERE score IS NOT NULL
),

cleaned AS (
    SELECT 
        s.employee_id,
        s.scale_code,
        COALESCE(s.score, m.median_score) as score_imputed
    FROM source s
    LEFT JOIN medians m ON s.scale_code = m.scale_code
)

SELECT * FROM cleaned