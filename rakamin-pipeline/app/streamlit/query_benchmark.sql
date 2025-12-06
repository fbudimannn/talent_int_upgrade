DECLARE benchmark_ids ARRAY<STRING> DEFAULT [{benchmark_ids_list}];

WITH 
-- =================================================================================
-- PHASE 1: SOURCE & PREPARATION
-- Mengambil data yang sudah unpivoted dari DBT (Sudah bersih/imputed di level dbt)
-- =================================================================================
source_data AS (
    SELECT * FROM `{project}.{dataset}.int_employees_unpivoted` 
    WHERE tv_name IN (
        'SEA', 'QDD', 'FTC', 'IDS', 'VCU', 'STO_LIE', 'CSI', 'CEX_GDR', 
        'IQ', 'GTQ', 'Pauli', 'Faxtor', 'Tiki', 
        'MBTI', 'DISC', 'Papi_P', 'Papi_S', 'Papi_G', 'Papi_T', 'Papi_W', 
        'Strength', 
        'Education', 'Major', 'Position', 'Area', 'Tenure'
    )
),

-- Ambil data khusus 3 orang benchmark
benchmark_subset AS (
    SELECT * FROM source_data
    WHERE employee_id IN UNNEST(benchmark_ids)
),

-- =================================================================================
-- PHASE 2: BENCHMARK CALCULATION (THE NEW HYBRID LOGIC)
-- Menggantikan Phase 2 & 4 di script lama.
-- =================================================================================

-- 2.1. Hitung Frekuensi Jawaban (Khusus Categorical)
cat_freq_calc AS (
    SELECT 
        tv_name, 
        user_score,
        COUNT(*) as freq
    FROM benchmark_subset
    WHERE tv_type = 'categorical' 
    GROUP BY 1, 2
),

-- 2.2. Cari Angka Tertinggi (Max Count) per Test
cat_winner_filter AS (
    SELECT 
        tv_name,
        MAX(freq) as max_freq
    FROM cat_freq_calc
    GROUP BY 1
),

-- 2.3. Gabungkan (Hybrid Rule: Jika Seri ambil Semua, Jika Dominan ambil Satu)
cat_target_ready AS (
    SELECT 
        f.tv_name,
        -- Simpan sebagai JSON String array ['A', 'B']
        TO_JSON_STRING(ARRAY_AGG(DISTINCT f.user_score)) as hybrid_categorical_target
    FROM cat_freq_calc f
    JOIN cat_winner_filter m ON f.tv_name = m.tv_name
    WHERE f.freq = m.max_freq 
    GROUP BY 1
),

-- 2.4. Hitung Numeric Median (EXACT METHOD)
-- Menggunakan PERCENTILE_CONT agar hasil sama persis dengan SQL Lama (Interpolasi)
numeric_target_calc AS (
    SELECT DISTINCT
        tv_name,
        CAST(
            PERCENTILE_CONT(SAFE_CAST(user_score AS FLOAT64), 0.5) 
            OVER(PARTITION BY tv_name) 
        AS STRING) as numeric_target_val
    FROM benchmark_subset
    WHERE tv_type = 'numeric'
),

-- 2.5. Final Benchmark Profile (Gabungan Semuanya)
benchmark_profile_final AS (
    SELECT 
        base.tv_name,
        ANY_VALUE(base.tv_type) as tv_type,
        
        -- A. STRENGTH (Array Aggregation)
        TO_JSON_STRING(ARRAY_AGG(DISTINCT s_item IGNORE NULLS)) as strength_target,
        
        -- B. NUMERIC (Ambil dari CTE numeric_target_calc)
        ANY_VALUE(num.numeric_target_val) as numeric_target,
        
        -- C. CATEGORICAL (Ambil dari CTE cat_target_ready)
        ANY_VALUE(c.hybrid_categorical_target) as categorical_target

    FROM benchmark_subset base
    LEFT JOIN cat_target_ready c ON base.tv_name = c.tv_name
    LEFT JOIN numeric_target_calc num ON base.tv_name = num.tv_name
    LEFT JOIN UNNEST(JSON_EXTRACT_STRING_ARRAY(base.user_score)) s_item 
        ON base.tv_name = 'Strength'
    GROUP BY base.tv_name
),

-- =================================================================================
-- PHASE 3: SCORING (CALCULATE MATCH SCORE)
-- Menggabungkan Logic 'Individual Scores' dari script lama
-- =================================================================================
scoring_calc AS (
    SELECT
        u.employee_id,
        u.tgv_name,
        u.tv_name,
        u.tv_type,
        u.user_score,
        
        -- Ambil Target Value yang sesuai tipe-nya untuk display
        CASE 
            WHEN u.tv_name = 'Strength' THEN b.strength_target
            WHEN u.tv_type = 'numeric' THEN b.numeric_target
            ELSE b.categorical_target
        END as target_val_display,

        CASE
            -- 1. STRENGTH (Overlap Logic)
            WHEN u.tv_name = 'Strength' THEN (
                SELECT IFNULL((COUNT(DISTINCT u_item) / 5.0) * 100, 0)
                FROM UNNEST(JSON_EXTRACT_STRING_ARRAY(u.user_score)) u_item
                JOIN UNNEST(JSON_EXTRACT_STRING_ARRAY(b.strength_target)) b_item 
                ON TRIM(UPPER(u_item)) = TRIM(UPPER(b_item))
            )

            -- 2. NUMERIC (Formula Jarak)
            WHEN u.tv_type = 'numeric' THEN
                CASE 
                    -- Inverse Logic (Papi S/G/T)
                    WHEN u.tv_name IN ('Papi_S', 'Papi_G', 'Papi_T') THEN 
                        GREATEST(0, LEAST(100, ((2 * SAFE_CAST(b.numeric_target AS FLOAT64) - SAFE_CAST(u.user_score AS FLOAT64)) / NULLIF(SAFE_CAST(b.numeric_target AS FLOAT64),0)) * 100))
                    -- Normal Logic
                    ELSE 
                        GREATEST(0, LEAST(100, (SAFE_CAST(u.user_score AS FLOAT64) / NULLIF(SAFE_CAST(b.numeric_target AS FLOAT64),0)) * 100))
                END
            
            -- 3. CATEGORICAL (Hybrid Check: Is user value inside the list?)
            ELSE
                CASE 
                    WHEN TRIM(UPPER(u.user_score)) IN (
                        SELECT TRIM(UPPER(val)) 
                        FROM UNNEST(JSON_EXTRACT_STRING_ARRAY(b.categorical_target)) val
                    ) THEN 100 
                    ELSE 0 
                END
        END as match_score

    FROM source_data u
    LEFT JOIN benchmark_profile_final b ON u.tv_name = b.tv_name
),

-- =================================================================================
-- PHASE 5: WEIGHTING (MAPPING)
-- Sama persis dengan script lama
-- =================================================================================
weights_mapping AS (
    SELECT * FROM UNNEST([
        STRUCT('SEA' as tv_name, 0.084375 as weight),
        ('QDD', 0.084375), ('FTC', 0.084375), ('IDS', 0.084375),
        ('VCU', 0.084375), ('STO_LIE', 0.084375), ('CSI', 0.084375), ('CEX_GDR', 0.084375),
        
        ('IQ', 0.01), ('GTQ', 0.01), ('Pauli', 0.01), ('Faxtor', 0.01), ('Tiki', 0.01),
        
        ('MBTI', 0.00714), ('DISC', 0.00714), 
        ('Papi_P', 0.00714), ('Papi_S', 0.00714), ('Papi_G', 0.00714), ('Papi_T', 0.00714), ('Papi_W', 0.00714),
        
        ('Strength', 0.05),
        
        ('Education', 0.035), ('Major', 0.035), ('Position', 0.035), ('Area', 0.035), ('Tenure', 0.035)
    ])
),

weighted_scores AS (
    SELECT
        s.*,
        w.weight,
        (s.match_score * w.weight) as weighted_score
    FROM scoring_calc s
    JOIN weights_mapping w ON s.tv_name = w.tv_name
),

-- =================================================================================
-- PHASE 6: AGGREGATION & DETAILS
-- Sama persis dengan script lama (Summing & Ratios)
-- =================================================================================
aggregated_scores AS (
    SELECT
        employee_id,
        SUM(weighted_score) AS final_match_rate,
        
        -- Raw Sums per TGV
        SUM(CASE WHEN tgv_name = 'Competency' THEN weighted_score ELSE 0 END) AS competency_raw,
        SUM(CASE WHEN tgv_name = 'Psychometric (Cognitive)' THEN weighted_score ELSE 0 END) AS cognitive_raw,
        SUM(CASE WHEN tgv_name = 'Psychometric (Personality)' THEN weighted_score ELSE 0 END) AS personality_raw,
        SUM(CASE WHEN tgv_name = 'Behavioral (Strengths)' THEN weighted_score ELSE 0 END) AS strengths_raw,
        SUM(CASE WHEN tgv_name = 'Contextual (Background)' THEN weighted_score ELSE 0 END) AS contextual_raw
    FROM weighted_scores
    GROUP BY employee_id
),

detailed_scores_with_ratio AS (
    SELECT
        ws.employee_id,
        ws.tgv_name,
        ws.tv_name,
        ws.user_score,
        ws.target_val_display as baseline_score, -- JSON String / Numeric String
        ws.match_score,
        agg.final_match_rate,
        
        -- TGV Ratio Logic (Normalized)
        CASE 
            WHEN ws.tgv_name = 'Competency' THEN agg.competency_raw / 0.675
            WHEN ws.tgv_name = 'Psychometric (Cognitive)' THEN agg.cognitive_raw / 0.05
            WHEN ws.tgv_name = 'Psychometric (Personality)' THEN agg.personality_raw / 0.05
            WHEN ws.tgv_name = 'Behavioral (Strengths)' THEN agg.strengths_raw / 0.05
            WHEN ws.tgv_name = 'Contextual (Background)' THEN agg.contextual_raw / 0.175
            ELSE 0 
        END AS tgv_match_ratio
        
    FROM weighted_scores ws
    LEFT JOIN aggregated_scores agg ON ws.employee_id = agg.employee_id
)

-- =================================================================================
-- FINAL OUTPUT
-- Struktur kolom disamakan dengan script lama
-- =================================================================================

SELECT 
    f.employee_id,
    e.directorate,
    e.position as role,
    e.grade,
    
    f.tgv_name,
    f.tv_name,
    
    -- Format Baseline Score (Bersihkan JSON characters untuk display)
    CASE 
        WHEN f.tv_name = 'Strength' THEN (
            -- Convert JSON Array String to Comma Separated String
            SELECT STRING_AGG(val, ', ') 
            FROM UNNEST(JSON_EXTRACT_STRING_ARRAY(f.baseline_score)) val
        )
        WHEN f.tv_name IN ('DISC', 'Education', 'Major', 'Position', 'Area', 'MBTI') THEN (
             -- Convert JSON Array String to Comma Separated (misal: "DS, DI")
            SELECT STRING_AGG(val, ', ') 
            FROM UNNEST(JSON_EXTRACT_STRING_ARRAY(f.baseline_score)) val
        )
        ELSE f.baseline_score -- Numeric biarkan apa adanya
    END as baseline_score,
    
    -- Format User Score (Khusus Strength yg JSON)
    CASE 
        WHEN f.tv_name = 'Strength' THEN (
            SELECT STRING_AGG(val, ', ') 
            FROM UNNEST(JSON_EXTRACT_STRING_ARRAY(f.user_score)) val
        )
        ELSE f.user_score
    END as user_score,
    
    f.match_score as tv_match_rate,
    ROUND(CAST(f.tgv_match_ratio AS NUMERIC), 2) as tgv_match_rate,
    ROUND(CAST(f.final_match_rate AS NUMERIC), 2) as final_match_rate,
    
    CASE WHEN f.employee_id IN UNNEST(benchmark_ids) THEN TRUE ELSE FALSE END as is_benchmark

FROM detailed_scores_with_ratio f
LEFT JOIN `{project}.{dataset}.stg_employees` e -- Pastikan join ke metadata employee
    ON f.employee_id = e.employee_id
ORDER BY 
    is_benchmark DESC, 
    final_match_rate DESC, 
    f.employee_id, 
    f.tgv_name
