-- models/staging/stg_performance_clean.sql
SELECT 
    employee_id,
    year,
    rating
FROM {{ source('raw_source', 'performance_yearly') }}
WHERE rating IS NOT NULL  -- Buang yang rating kosong
  AND rating BETWEEN 1 AND 5 -- Hanya ambil rating 1-5
  AND year < 2025         -- HANYA ambil history (sebelum 2025)
  -- Data 2025 kan sudah ada di Profil saat ini