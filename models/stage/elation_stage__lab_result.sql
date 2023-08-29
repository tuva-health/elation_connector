with deduplicate_requisition as(
    select 
        req_number
        , ordering_provider
        , row_number() over (partition by req_number order by last_modified desc) as duplicate_row_number
    from {{ source('elation','lab_order')}}
)

select 
    {{sha_hash_512('report.patient_id||result.resulted_date||result.test_name||result.value||result.id')}} as lab_result_id
    , result.id as source_id
    , report.patient_id as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , result.accession_number
    , case 
        when result.loinc is not null then 'loinc'
    end as source_code_type
    , result.loinc as source_code
    , result.test_category as source_description
    , result.test_name as source_component
    , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_description
    , cast(null as {{ dbt.type_string() }} ) as normalized_component
    , result.accession_status as status
    , result.value as result
    , result.resulted_date as result_date
    , result.collected_date as collection_date
    , result.units as source_units
    , cast(null as {{ dbt.type_string() }} ) as normalized_units
    , result.reference_min as source_reference_range_low
    , result.reference_max as source_reference_range_high
    , cast(null as {{ dbt.type_string() }} ) as normalized_reference_range_low
    , cast(null as {{ dbt.type_string() }} ) as normalized_reference_range_high
    , result.is_abnormal as source_abnormal_flag
    , cast(null as {{ dbt.type_string() }} ) as normalized_abnormal_flag
    , ord.ordering_provider as ordering_practitioner_id
    , cast(null as {{ dbt.type_string() }} ) as specimen
    , 'elation' as data_source
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','lab_result')}} result
inner join {{ source('elation','report')}} report
    on result.lab_report_id = report.id
left join deduplicate_requisition ord
    on report.requisition_number = ord.req_number
    and ord.duplicate_row_number = 1
where result.is_deleted = FALSE
