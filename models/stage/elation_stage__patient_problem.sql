with icd_10_unique as(
  select distinct 
    id
    , code
    , description
  from {{ source('elation','icd10') }}
)

select distinct
      {{sha_hash_512('patient_id||prob.creation_time||rank||source_icd10.code')}} as condition_id
    , prob.id as source_id
    , prob.patient_id
    , null as encounter_id
    , null as claim_id
    , cast(prob.creation_time as date) as recorded_date
    , cast(prob.start_date as date) as onset_date
    , cast(prob.resolved_date as date) as resolved_date
    , case lower(prob.status) 
            when 'active' then 'active'
            when 'resolved' then 'resolved'
            when 'controlled' then 'active'
    end as status
    , 'problem' as condition_type
    , 'icd-10-cm' as source_code_type
    , source_icd10.code as source_code
    , source_icd10.description as source_description
    , case
        when norm_icd10.icd_10_cm is not null then 'icd-10-cm'
    end as normalized_code_type
    , norm_icd10.icd_10_cm as normalized_code
    , norm_icd10.description as normalized_description
    , prob.rank as condition_rank
    , null as present_on_admit_code
    , null as present_on_admit_description
    , 'elation' as data_source
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
    , prob.last_modified as source_last_modified
from {{ source('elation','patient_problem') }} prob
left join {{ source('elation','patient_problem_code') }} pro_code
    on prob.id = pro_code.patient_problem_id
left join icd_10_unique source_icd10
    on pro_code.icd10_id = source_icd10.id
left join {{ ref('terminology__icd_10_cm') }} norm_icd10
    on replace(source_icd10.code,'.','') = norm_icd10.icd_10_cm
where prob.deletion_time is null
and source_icd10.code is not null