with distinct_tags as(
    select 
        rdt.report_id
        , dt.code_type
        , dt.code
        , dt.description
        , dt.value
        , row_number() over (partition by rdt.report_id order by case when code_type = 'LOINC' then 1 else 2 end) as duplicate_row_number 
    from {{ source('elation','report_document_tag')}} as rdt
    inner join {{ source('elation','document_tag')}} as dt
    on rdt.document_tag_id = dt.id
)



select 
    r.id as observation_id
    , r.id as source_id
    , r.patient_id 
    , null as encounter_id
    , r.document_date as observation_date
    , null as panel_id
    , report_type as observation_type
    , tags.code_type as source_code_type
    , tags.code as source_code
    , tags.description as source_description
    , case 
        when hcpcs_ii.hcpcs is not null then 'hcpcs'
    end as normalized_code_type
    , hcpcs_ii.hcpcs as normalized_code
    , hcpcs_ii.long_description as normalized_description
    , tags.value as result
    , null as source_units
    , null as normalized_units
    , null as source_reference_range_low
    , null as source_reference_range_high
    , null as normalized_reference_range_low
    , null as normalized_reference_range_high
    , 'elation' as data_source
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','report')}} as r
inner join distinct_tags tags
    on r.id = tags.report_id
    and tags.duplicate_row_number = 1
left join {{ ref('terminology__hcpcs_level_2')}} hcpcs_ii
    on tags.code = hcpcs_ii.hcpcs
    and tags.code_type = 'HCPCS'
where r.deletion_time is null
