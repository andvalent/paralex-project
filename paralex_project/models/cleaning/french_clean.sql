{{ config(materialized="table") }}

with base as (
    select
        frequency_per_million,
        frequency,
        cell,
        phon_form,
        orth_form,
        overabundance_tag,
        mode,
        tense,
        person,
        number,
        form_id,
        gender,
        lexeme
    from {{ source('paralex_raw', 'french_processed') }}
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
    frequency_per_million,
    frequency,
    cell,
    phon_form,
    orth_form,
    overabundance_tag,
    mode,
    tense,
    person,
    number,
    form_id,
    gender,
    lexeme
from ordered
order by
    lexeme,          
    mode,            
    tense,          
    number_order,   
    person_order     
