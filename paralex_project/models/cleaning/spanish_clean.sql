{{ config(materialized="table") }}

with base as (
    select
        form_id,
        lexeme,
        cell,
        phon_form,
        mood,
        person,
        number,
        tense,
        aspect,
        lexical,
        verbform,
        gender
    from {{ source('paralex_raw', 'spanish_processed') }}
),

ordered as (
    select
        *,
        case person
            when 'first' then 1
            when 'second' then 2
            when 'third' then 3
        end as person_order,
        case number
            when 'singular' then 1
            when 'plural' then 2
        end as number_order
    from base
)

select
    form_id,
    lexeme,
    cell,
    phon_form,
    mood,
    person,
    number,
    tense,
    aspect,
    lexical,
    verbform,
    gender
from ordered
order by
    lexeme,          
    mood,
    tense,
    number_order,
    person_order
