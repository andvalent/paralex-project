{{ config(materialized='view') }}

with clean_data as (
    select * from {{ ref('spanish_clean') }}
)

SELECT *
FROM clean_data
WHERE (
    /* 1. Indicative Mood */
    (mood = 'indicative' AND (
        tense = 'present'                          -- Present Indicative
        OR (tense = 'past' AND aspect = 'imperfect') -- Imperfect Indicative
        OR (tense = 'past' AND aspect = 'perfect')   -- Preterite Indicative
        OR tense = 'future'                         -- Future Indicative
    ))
    OR
    /* 2. Subjunctive Mood */
    (mood = 'subjunctive' AND (
        tense = 'present'                          -- Present Subjunctive
        OR tense = 'past'                          -- Imperfect Subjunctive (Both -ra and -se)
    ))
    OR
    /* 3. Other Forms */
    mood = 'conditional'                           -- Conditional
    OR mood = 'imperative'                         -- Imperative
    OR mood = 'infinitive'                         -- Infinitive
    OR verbform = 'gerund'                         -- Gerund
)