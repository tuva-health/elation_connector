select
    hm.id as observation_id
    , hm.id as source_id
    , hm.patient_id
    , null as encounter_id
    , hm.date as observation_date
    , null as panel_id
    , hm.measure_code as observation_type
    , dt.code_type as source_code_type
    , dt.code as source_code
    , hm.name as source_description
    , case 
        when hcpcs_ii.hcpcs is not null then 'hcpcs'
    end as normalized_code_type
    , hcpcs_ii.hcpcs as normalized_code
    , hcpcs_ii.long_description as normalized_description
    , dt.value as result
    , null as source_units
    , null as normalized_units
    , null as source_reference_range_low
    , null as source_reference_range_high
    , null as normalized_reference_range_low
    , null as normalized_reference_range_high
    , 'elation' as data_source
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','health_maintenance') }} as hm
left join {{ source('elation','document_tag') }} as dt
on hm.doc_tag_id = dt.id
left join {{ ref('terminology__hcpcs_level_2')}} hcpcs_ii
    on dt.code = hcpcs_ii.hcpcs
    and dt.code_type = 'HCPCS'
where hm.deleted_date is null