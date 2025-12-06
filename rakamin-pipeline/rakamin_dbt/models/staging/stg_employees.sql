WITH employees AS (
    SELECT * FROM {{ source('raw_source', 'employees') }}
),
psych AS (
    SELECT * FROM {{ source('raw_source', 'profiles_psych') }}
),
performance AS (
    SELECT * FROM {{ source('raw_source', 'performance_yearly') }}
),
-- Dimensions (Ambil semua tabel dimensi)
dim_dept AS (SELECT * FROM {{ source('raw_source', 'dim_departments') }}),
dim_pos AS (SELECT * FROM {{ source('raw_source', 'dim_positions') }}),
dim_grade AS (SELECT * FROM {{ source('raw_source', 'dim_grades') }}),
dim_edu AS (SELECT * FROM {{ source('raw_source', 'dim_education') }}),
dim_major AS (SELECT * FROM {{ source('raw_source', 'dim_majors') }}),
dim_comp AS (SELECT * FROM {{ source('raw_source', 'dim_companies') }}),
dim_area AS (SELECT * FROM {{ source('raw_source', 'dim_areas') }}),
dim_dir AS (SELECT * FROM {{ source('raw_source', 'dim_directorates') }}),

-- Helpers: Median IQ per Dept
cog_medians AS (
    SELECT DISTINCT
        e.department_id,
        -- Window function menghitung median dari partisi department
        PERCENTILE_CONT(p.iq, 0.5) OVER(PARTITION BY e.department_id) as median_iq,
        PERCENTILE_CONT(p.gtq, 0.5) OVER(PARTITION BY e.department_id) as median_gtq
    FROM employees e
    LEFT JOIN psych p ON e.employee_id = p.employee_id
    
    -- JOIN JUGA KE PERFORMANCE UNTUK FILTER
    LEFT JOIN performance py ON e.employee_id = py.employee_id
    
    WHERE 
        -- Filter Dept 4
        e.department_id != 4
        -- Pastikan hanya mengambil populasi yang SAMA dengan tabel Final (Tahun 2025 & Rating Valid)
        AND py.year = 2025 
        AND py.rating BETWEEN 1 AND 5
        -- Filter IQ/GTQ null agar tidak memberatkan query (opsional, krn percentile ignore null)
        AND (p.iq IS NOT NULL OR p.gtq IS NOT NULL)
),

-- Helpers: MBTI Cleaning
mbti_clean AS (
    SELECT employee_id, 
        CASE 
            WHEN LOWER(TRIM(mbti)) = 'inftj' THEN 'UNKNOWN' 
            ELSE UPPER(TRIM(mbti)) 
        END as mbti_val
    FROM psych
),

-- Helpers: MBTI Mode (Logic BigQuery: APPROX_TOP_COUNT)
mbti_mode_calc AS (
    SELECT 
        APPROX_TOP_COUNT(mbti_val, 1)[OFFSET(0)].value as val
    FROM mbti_clean 
    WHERE mbti_val IS NOT NULL
),

final AS (
    SELECT 
        e.employee_id, e.fullname, e.nip, e.years_of_service_months as tenure_months, e.department_id,
        dim_comp.name as company, dim_area.name as area, dim_dept.name as department,
        dim_pos.name as position, dim_dir.name as directorate, dim_grade.name as grade,
        dim_edu.name as education_level, dim_major.name as major,
        
        -- Imputed Cognitive (Logic BigQuery)
        CASE 
            WHEN e.department_id = 4 THEN NULL 
            ELSE COALESCE(p.iq, cog.median_iq) 
        END as iq_imputed,
        
        CASE 
            WHEN e.department_id = 4 THEN NULL 
            ELSE COALESCE(p.gtq, cog.median_gtq) 
        END as gtq_imputed,
        
        -- Imputed MBTI & DISC
        COALESCE(mc.mbti_val, mm.val) as mbti_final,
        UPPER(TRIM(COALESCE(p.disc, 
        CASE 
            -- Mapping Combinations
            WHEN TRIM(p.disc_word) = 'Dominant-Influencer'    THEN 'DI'
            WHEN TRIM(p.disc_word) = 'Dominant-Steadiness'    THEN 'DS'
            WHEN TRIM(p.disc_word) = 'Dominant-Conscientious' THEN 'DC'
            WHEN TRIM(p.disc_word) = 'Influencer-Dominant'    THEN 'ID'
            WHEN TRIM(p.disc_word) = 'Influencer-Steadiness'  THEN 'IS'
            WHEN TRIM(p.disc_word) = 'Influencer-Conscientious' THEN 'IC'
            WHEN TRIM(p.disc_word) = 'Steadiness-Dominant'    THEN 'SD'
            WHEN TRIM(p.disc_word) = 'Steadiness-Influencer'  THEN 'SI'
            WHEN TRIM(p.disc_word) = 'Steadiness-Conscientious' THEN 'SC'
            WHEN TRIM(p.disc_word) = 'Conscientious-Dominant' THEN 'CD'
            WHEN TRIM(p.disc_word) = 'Conscientious-Influencer' THEN 'CI'
            WHEN TRIM(p.disc_word) = 'Conscientious-Steadiness' THEN 'CS'
            
            -- Mapping Single Types (Jaga-jaga ada data single)
            WHEN TRIM(p.disc_word) = 'Dominance' THEN 'D'
            WHEN TRIM(p.disc_word) = 'Influence' THEN 'I'
            WHEN TRIM(p.disc_word) = 'Steadiness' THEN 'S'
            WHEN TRIM(p.disc_word) = 'Conscientiousness' THEN 'C'
            
            ELSE NULL 
        END
    ))) as disc,
        
        p.disc_word, p.pauli as pauli_score, p.tiki as tiki_score, p.faxtor as faxtor_score,
        py.rating
        
    FROM employees e
    LEFT JOIN dim_dept ON e.department_id = dim_dept.department_id
    LEFT JOIN dim_pos ON e.position_id = dim_pos.position_id
    LEFT JOIN dim_grade ON e.grade_id = dim_grade.grade_id
    LEFT JOIN dim_edu ON e.education_id = dim_edu.education_id
    LEFT JOIN dim_major ON e.major_id = dim_major.major_id
    LEFT JOIN dim_comp ON e.company_id = dim_comp.company_id
    LEFT JOIN dim_area ON e.area_id = dim_area.area_id
    LEFT JOIN dim_dir ON e.directorate_id = dim_dir.directorate_id
    LEFT JOIN psych p ON e.employee_id = p.employee_id
    LEFT JOIN cog_medians cog ON e.department_id = cog.department_id
    LEFT JOIN mbti_clean mc ON e.employee_id = mc.employee_id
    CROSS JOIN mbti_mode_calc mm
    LEFT JOIN performance py ON e.employee_id = py.employee_id
    WHERE py.rating BETWEEN 1 AND 5 AND py.year = 2025
)

SELECT * FROM final