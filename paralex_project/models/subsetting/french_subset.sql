{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('french_clean') }} -- This creates the dependency
)

select *
from clean_data
WHERE 
    -- 1. Indicative
    (
        tense IN ('present', 'imperfect', 'past', 'future') 
        AND "mode" = 'indicative'
    )
    -- 2. Subjunctive
    OR (
        tense IN ('present', 'imperfect') 
        AND "mode" = 'subjunctive'
    )
    -- 3. Others (Adding 'participle' here for French)
    OR "mode" IN ('conditional', 'imperative', 'infinitive', 'participle')