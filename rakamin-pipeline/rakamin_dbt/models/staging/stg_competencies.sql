WITH source AS (
    SELECT * FROM {{ source('raw_source', 'competencies_yearly') }}
),

medians AS (
    SELECT DISTINCT
        pillar_code, 
        -- Syntax BigQuery untuk Median (Window Function)
        PERCENTILE_CONT(score, 0.5) OVER(PARTITION BY pillar_code) as median_score
    FROM source
    WHERE year = 2025 
      AND score IS NOT NULL 
      AND score NOT IN (0, 6, 99) 
      
),

cleaned AS (
    SELECT 
        c.employee_id,
        c.pillar_code,
        c.year,
        COALESCE(
            CASE 
                WHEN c.score IN (0, 6, 99) THEN NULL 
                ELSE c.score 
            END, 
            m.median_score
        ) as score_imputed
    FROM source c
    LEFT JOIN medians m ON c.pillar_code = m.pillar_code
    WHERE c.year = 2025
)

SELECT * FROM cleaned