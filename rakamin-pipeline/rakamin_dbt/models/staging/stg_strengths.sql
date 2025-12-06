WITH source AS (
    SELECT * FROM {{ source('raw_source', 'strengths') }}
),

pre_cleaned AS (
    SELECT
        employee_id,
        `rank`, -- PENTING: Pakai backtick (`) untuk BigQuery, bukan (")
        CASE 
            WHEN theme IS NULL THEN NULL
            -- BigQuery aman dengan LOWER/TRIM
            WHEN LOWER(TRIM(theme)) IN ('', 'nan', 'none') THEN NULL
            ELSE theme
        END AS theme_clean
    FROM source
),

ranked AS (
    SELECT
        employee_id,
        theme_clean AS theme,
        `rank`,
        -- Window function BigQuery standar
        ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY `rank`) AS rn
    FROM pre_cleaned
    WHERE theme_clean IS NOT NULL
)

SELECT 
    employee_id,
    theme
FROM ranked
WHERE rn <= 5