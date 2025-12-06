-- Panggil model staging yang sudah kamu buat di atas
WITH stg_comp AS (
    SELECT * FROM {{ ref('stg_competencies') }} -- Asumsi nama file staging competency kamu
),
stg_psych AS (
    SELECT * FROM {{ ref('stg_employees') }} -- Karena IQ, GTQ, Pauli, dll ada di tabel employees hasil join
),
stg_papi AS (
    SELECT * FROM {{ ref('stg_papi') }}
),
stg_strength AS (
    SELECT * FROM {{ ref('stg_strengths') }}
),

-- LOGIC KHUSUS STRENGTH: JADIKAN ARRAY (Satu baris per karyawan)
strength_agg AS (
    SELECT 
        employee_id,
        -- Menggabungkan 5 theme menjadi satu Array/List
        ARRAY_AGG(theme) AS strength_array 
    FROM stg_strength
    GROUP BY employee_id
),

unpivoted AS (
    -- 1. COMPETENCY (Ambil Pillar Codes)
    SELECT 
        employee_id, 
        'Competency' AS tgv_name, 
        pillar_code AS tv_name,
        CAST(score_imputed AS STRING) AS user_score, 
        'numeric' AS tv_type
    FROM stg_comp
    WHERE pillar_code NOT IN ('STO', 'LIE', 'CEX', 'GDR') 

    UNION ALL

    SELECT 
        employee_id, 
        'Competency' AS tgv_name, 
        'STO_LIE' AS tv_name,
        CAST(AVG(score_imputed) AS STRING) AS user_score, 
        'numeric' AS tv_type
    FROM stg_comp
    WHERE pillar_code IN ('STO', 'LIE')
    GROUP BY employee_id

    UNION ALL

    SELECT 
        employee_id, 
        'Competency' AS tgv_name, 
        'CEX_GDR' AS tv_name,
        CAST(AVG(score_imputed) AS STRING) AS user_score, 
        'numeric' AS tv_type
    FROM stg_comp
    WHERE pillar_code IN ('CEX', 'GDR')
    GROUP BY employee_id


    UNION ALL

    -- 2. PSYCHOMETRIC (Cognitive) - Dari stg_employees
    SELECT employee_id, 'Psychometric (Cognitive)', 'IQ', CAST(iq_imputed AS STRING), 'numeric' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Psychometric (Cognitive)', 'GTQ', CAST(gtq_imputed AS STRING), 'numeric' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Psychometric (Cognitive)', 'Pauli', CAST(pauli_score AS STRING), 'numeric' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Psychometric (Cognitive)', 'Faxtor', CAST(faxtor_score AS STRING), 'numeric' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Psychometric (Cognitive)', 'Tiki', CAST(tiki_score AS STRING), 'numeric' FROM stg_psych

    UNION ALL

    -- 3. PSYCHOMETRIC (Personality) - MBTI & DISC dari stg_employees
    SELECT employee_id, 'Psychometric (Personality)', 'MBTI', mbti_final, 'categorical' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Psychometric (Personality)', 'DISC', disc, 'categorical' FROM stg_psych

    UNION ALL

    -- 3b. PSYCHOMETRIC (Personality) - PAPI (P, S, G, T, W)
    SELECT 
        employee_id, 
        'Psychometric (Personality)', 
        scale_code, 
        CAST(score_imputed AS STRING), 
        'numeric'
    FROM stg_papi
    WHERE scale_code IN ('Papi_P', 'Papi_S', 'Papi_G', 'Papi_T', 'Papi_W')

    UNION ALL

    -- 4. BEHAVIORAL (Strengths) - Ambil dari CTE strength_agg
    -- Disini kita simpan Array sebagai String dulu biar bisa masuk UNION
    SELECT 
        employee_id, 
        'Behavioral (Strengths)', 
        'Strength', 
        TO_JSON_STRING(strength_array), -- Konversi Array ke String JSON biar aman
        'array'
    FROM strength_agg

    UNION ALL

    -- 5. CONTEXTUAL (Background)
    SELECT employee_id, 'Contextual (Background)', 'Education', education_level, 'categorical' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Contextual (Background)', 'Major', major, 'categorical' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Contextual (Background)', 'Position', position, 'categorical' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Contextual (Background)', 'Area', area, 'categorical' FROM stg_psych
    UNION ALL
    SELECT employee_id, 'Contextual (Background)', 'Tenure', CAST(tenure_months AS STRING), 'numeric' FROM stg_psych
)

SELECT * FROM unpivoted