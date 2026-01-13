{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('portuguese_clean') }} -- This creates the dependency
)

SELECT *
FROM clean_data
WHERE (
    /* 1. Indicative Mood */
    (mood = 'indicative' AND (
        tense = 'present'                             -- Present Indicative
        OR (tense = 'past' AND aspect = 'imperfective') -- Imperfect Indicative
        OR (tense = 'past' AND aspect = 'perfective')  -- Preterite Indicative
        OR tense = 'future'                            -- Future Indicative
    ))
    OR
    /* 2. Subjunctive Mood */
    (mood = 'subjunctive' AND (
        tense = 'present'                             -- Present Subjunctive
        OR tense = 'past'                             -- Imperfect Subjunctive
    ))
    OR
    /* 3. Independent Moods */
    mood = 'conditional'                              -- Conditional
    OR mood = 'imperative'                            -- Imperative
    OR verbform = 'infinitive'                        -- Infinitive
    OR verbform = 'gerund'                            -- Gerund
)