{{ config(materialized="table") }}

with base as (
    select
        form_id,
        lexeme,
        cell,
        phon_form,
        verbform,
        tense,
        number,
        gender,
        mood,
        person
    from {{ source('paralex_raw', 'italian') }}
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
    verbform,
    tense,
    number,
    gender,
    mood,
    person
from ordered
order by
    lexeme,
    mood,
    tense,
    number_order,
    person_order


