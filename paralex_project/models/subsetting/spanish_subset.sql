{{ config(materialized='view') }} -- Materialized as view for flexibility

with clean_data as (
    select * from {{ ref('spanish_clean') }} -- This creates the dependency
)

select * from clean_data
WHERE (
   /* 1. Indicative Mood Tenses */
   (mood = 'indicative' AND (
       tense = 'present'                /* Present Indicative [prs.ind] */
       OR aspect = 'imperfect'          /* Imperfect Indicative [iprf.ind] */
       OR tense = 'past'                /* Preterite Indicative [pret.ind.] */
       OR tense = 'future'              /* Future Indicative [fut.ind] */
   ))
   
   OR 
   
   /* 2. Subjunctive Mood Tenses */
   (mood = 'subjunctive' AND (
       tense = 'present'                /* Present Subjunctive [prs.sbjv] */
       OR aspect = 'imperfect'          /* Imperfect Subjunctive [iprf.sbjv] */
   ))
   
   OR 
   
   /* 3. Independent Moods & Forms */
   (mood = 'conditional')               /* Conditional [cond] */
   OR (mood = 'imperative')              /* Imperative [imp] */
   OR (mood = 'infinitive')              /* Infinitive [inf] */
   OR (verbform = 'gerund')              /* Gerund [ger] */
)