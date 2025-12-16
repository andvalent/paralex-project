{{ config(materialized="table") }}

with base as (
    select
        form_id,
        lexeme,
        cell,
        phon_form,
        verbform,
        tense,
        mood,
        "person/number" as person_number,   -- original combined column
        aspect,
        valency,
        gender,
        number                                  -- existing number column (keep)
    from {{ source('paralex_raw', 'portuguese') }}
),

split_person_number as (
    select
        *,
        -- Extract person (first / second / third)
        case
            when person_number ilike 'first %' then 'first'
            when person_number ilike 'second %' then 'second'
            when person_number ilike 'third %' then 'third'
        end as person,

        -- Extract number (singular / plural)
        case
            when person_number ilike '% singular' then 'singular'
            when person_number ilike '% plural' then 'plural'
        end as extracted_number
    from base
),

ordered as (
    select
        *,
        case person
            when 'first' then 1
            when 'second' then 2
            when 'third' then 3
        end as person_order,

        case 
            when coalesce(extracted_number, number) = 'singular' then 1
            when coalesce(extracted_number, number) = 'plural' then 2
        end as number_order
    from split_person_number
)

select
    form_id,
    lexeme,
    cell,
    phon_form,
    verbform,
    tense,
    mood,
    person_number,              -- keep original
    aspect,
    valency,
    gender,
    number,                     -- keep original
    person,                     -- new clean person column
    extracted_number as clean_number
from ordered
order by
    lexeme,
    mood,
    tense,
    number_order,
    person_order
