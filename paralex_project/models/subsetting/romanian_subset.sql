{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('romanian_clean') }} -- This creates the dependency
)

select *
from clean_data
where 
    (mood = 'indicative' and tense in ('present', 'imperfect', 'preterite'))
    or (mood = 'subjunctive' and tense = 'present')
    or (mood in ('imperative', 'infinitive', 'conditional'))
    or (tense = 'future')
    or (verbform = 'gerund')