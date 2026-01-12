{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('italian_clean') }} -- This creates the dependency
)

select * from clean_data

WHERE TRIM(LOWER(mood)) = 'indicative' 
 AND TRIM(LOWER(tense)) IN ('present', 'imperfect', 'preterite', 'past', 'future')

UNION ALL

/* 2. SUBJUNCTIVE TENSES */
SELECT * FROM public.italian_clean 
WHERE TRIM(LOWER(mood)) = 'subjunctive' 
 AND TRIM(LOWER(tense)) IN ('present', 'imperfect')

UNION ALL

/* 3. STANDALONE MOODS */
SELECT * FROM public.italian_clean 
WHERE TRIM(LOWER(mood)) IN ('conditional', 'imperative', 'future')

UNION ALL

/* 4. NON-FINITE FORMS */
SELECT * FROM public.italian_clean 
WHERE TRIM(LOWER(verbform)) IN ('infinitive', 'gerund')