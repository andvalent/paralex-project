{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('romanian_clean') }} -- This creates the dependency
)

SELECT *
FROM clean_data
WHERE (
    /* 1. Indicative Mood */
    (mood = 'indicative' AND (
        tense = 'present'      -- Present Indicative
        OR tense = 'imperfect' -- Imperfect Indicative
        OR tense = 'preterite' -- Preterite Indicative
    ))
    OR
    /* 2. Subjunctive Mood */
    (mood = 'subjunctive' AND (
        tense = 'present'      -- Present Subjunctive
    ))
    OR
    /* 3. Independent Moods & Forms */
    mood = 'imperative'        -- Imperative
    OR mood = 'infinitive'     -- Infinitive
    OR verbform = 'gerund'     -- Gerund
)