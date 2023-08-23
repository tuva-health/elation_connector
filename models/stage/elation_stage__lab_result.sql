<<<<<<< HEAD
with deduplicate_requisition as(
    select 
        req_number
        , ordering_provider
        , row_number() over (partition by req_number order by last_modified desc) as duplicate_row_number
    from elation.herself_health.lab_order
)

select 
    sha2(report.patient_id||result.resulted_date||result.test_name||result.value||result.id, 512) as lab_result_id
    , result.id as source_id
    , report.patient_id as patient_id
    , null as encounter_id
    , result.accession_number
    , case 
        when result.loinc is not null then 'loinc'
    end as source_code_type
=======
select 
    result.id as lab_result_id
    , report.patient_id as patient_id
    , null as encounter_id
    , result.accession_number
    , 'loinc' as source_code_type
>>>>>>> 54b2bbd9702f9c2541feb47eb7d5b147181d57f0
    , result.loinc as source_code
    , result.test_category as source_description
    , result.test_name as source_component
    , null as normalized_code_type
    , null as normalized_code
    , null as normalized_description
    , null as normalized_component
    , result.accession_status as status
    , result.value as result
    , result.resulted_date as result_date
    , result.collected_date as collection_date
    , result.units as source_units
    , null as normalized_units
    , result.reference_min as source_reference_range_low
    , result.reference_max as source_reference_range_high
    , null as normalized_reference_range_low
    , null as normalized_reference_range_high
    , result.is_abnormal as source_abnormal_flag
    , null as normalized_abnormal_flag
    , ord.ordering_provider as ordering_practitioner_id
    , null as specimen
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','lab_result')}} result
inner join {{ source('elation','report')}} report
    on result.lab_report_id = report.id
<<<<<<< HEAD
left join deduplicate_requisition ord
    on report.requisition_number = ord.req_number
    and ord.duplicate_row_number = 1
=======
left join {{ source('elation','lab_order')}} ord
    on report.requisition_number = ord.req_number
>>>>>>> 54b2bbd9702f9c2541feb47eb7d5b147181d57f0
where result.is_deleted = 'FALSE'
