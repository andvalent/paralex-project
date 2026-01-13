{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('french_clean') }} -- This creates the dependency
)

SELECT *
FROM clean_data
WHERE (
    /* 1. Indicative Mood */
    (mode = 'indicative' AND (
        tense = 'present'      -- Present Indicative
        OR tense = 'imperfect' -- Imperfect Indicative
        OR tense = 'past'      -- Preterite Indicative (Passé simple)
        OR tense = 'future'    -- Future Indicative
    ))
    OR
    /* 2. Subjunctive Mood */
    (mode = 'subjunctive' AND (
        tense = 'present'      -- Present Subjunctive
        OR tense = 'past'      -- Imperfect Subjunctive (Imparfait du subjonctif)
    ))
    OR
    /* 3. Other Moods & Forms */
    (mode = 'conditional' AND tense = 'present') -- Conditional
    OR (mode = 'imperative' AND tense = 'present') -- Imperative
    OR mode = 'infinitive'                         -- Infinitive
)