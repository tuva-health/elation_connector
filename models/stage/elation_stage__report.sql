select 
    r.id as observation_id
    , r.id as source_id
    , r.patient_id 
    , null as encounter_id
    , r.document_date as observation_date
    , null as panel_id
    , report_type as observation_type
    , dt.code_type as source_code_type
    , dt.code as source_code
    , dt.description as source_description
    , case 
        when hcpcs_ii.hcpcs is not null then 'hcpcs'
    end as normalized_code_type
    , hcpcs_ii.hcpcs as normalized_code
    , hcpcs_ii.long_description as normalized_description
    , dt.value as result
    , null as source_units
    , null as normalized_units
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','report')}} as r
inner join {{ source('elation','report_document_tag')}} as rdt
  on r.id = rdt.report_id
inner join {{ source('elation','document_tag')}} as dt
  on rdt.document_tag_id = dt.id
left join {{ ref('terminology__hcpcs_level_2')}} hcpcs_ii
    on dt.code = hcpcs_ii.hcpcs
    and dt.code_type = 'HCPCS'
where r.deletion_time is null
