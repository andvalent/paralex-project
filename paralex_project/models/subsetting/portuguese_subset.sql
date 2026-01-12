{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('portuguese_clean') }} -- This creates the dependency
)

select * from clean_data
WHERE (
   /* 1. Indicative */
   (mood = 'indicative' AND (tense IN ('present', 'past', 'future') OR aspect = 'imperfective'))
   OR 
   /* 2. Subjunctive */
   (mood = 'subjunctive' AND (tense = 'present' OR aspect = 'imperfective'))
   OR 
   /* 3. Standalone Moods */
   (mood IN ('conditional', 'imperative'))
   OR 
   /* 4. Non-finite Forms */
   (verbform IN ('infinitive', 'gerund'))
)