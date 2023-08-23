with vitals_unpivot as(
    select
        id
        , encounter_id
        , patient_id
        , document_date as observation_date
        , id as panel_id
        , null as source_code_type
        , lower(source_code) as source_code
        , null as source_description
        , result
        , units
    from
    (
        select 
            id
            , visit_note_id as encounter_id
            , patient_id
            , document_date
            , height
            , height_units as units
    from {{ source('elation','vital')}}
    where deletion_time is null
    and nullif(height,'') is not null
    ) x 
    unpivot( result for source_code in (height)
        )piv

    union all

    select
        id
        , encounter_id
        , patient_id
        , document_date as observation_date
        , id as panel_id
        , null as source_code_type
        , lower(source_code) as source_code
        , null as source_description
        , result
        , units
    from
    (
        select 
            id
            , visit_note_id as encounter_id
            , patient_id
            , document_date
            , weight
            , weight_units as units
    from {{ source('elation','vital')}}
    where deletion_time is null
    and nullif(weight,'') is not null
    ) x 
    unpivot( result for source_code in (weight)
        )piv

    union all

    select
        id
        , encounter_id
        , patient_id
        , document_date as observation_date
        , id as panel_id
        , null as source_code_type
        , lower(source_code) as source_code
        , null as source_description
        , result
        , units
    from
    (
        select 
            id
            , visit_note_id as encounter_id
            , patient_id
            , document_date
            , temperature
            , temperature_units as units
    from {{ source('elation','vital')}}
    where deletion_time is null
    and nullif(temperature,'') is not null
    ) x 
    unpivot( result for source_code in (temperature)
        )piv
        
    union all 

    select
        id
        , encounter_id
        , patient_id
        , document_date as observation_date
        , id as panel_id
        , null as source_code_type
        , lower(source_code) as source_code
        , null as source_description
        , result
        , null as units
    from
    (
        select 
            id
            , visit_note_id as encounter_id
            , patient_id
            , document_date
            , bp_d
            , bp_s
            , hc
            , height
            , cast(bmi as varchar) as bmi
            , hr
            , o2_percent
            , pain
            , rr
    from {{ source('elation','vital')}}
    ) x 
    unpivot( result for source_code in (bp_d, bp_s, hc, bmi, hr, o2_percent, pain, rr)
        )piv
)

select
    id as observation_id
    , id as source_id
    , patient_id
    , encounter_id
    , observation_date
    , panel_id
    , 'vital signs' as observation_type
    , source_code_type
    , source_code
    , case source_code
        when 'bp_d' then 'diastolic blood pressure'
        when 'bp_s' then 'systolic blood pressure'
        when 'hc' then 'head circumference'
        when 'bmi' then 'body mass index'
        when 'hr' then 'heart rate'
        when 'pain' then 'pain'
        when 'rr' then 'respiratory rate'
        when 'weight' then 'weight'
        when 'temperature' then 'temperature'
        when 'height' then 'height'
    end as source_description
    , result
    , units as source_units
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from vitals_unpivot
