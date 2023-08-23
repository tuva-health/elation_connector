select 
    id as observation_id
    , patient_id
    , null as encounter_id
    , null as panel_id
    , coalesce(date_for_test, creation_time) as observation_date
    , 'misc orders' as observation_type
    , 'icd-10-cm' as source_code_type
    , icd10_code as source_code
    , test_name as source_description
    , null as normalized_code_type
    , null as normalized_code
    , null as normalized_description
    , test_score as result
    , null as source_units
    , null as normalized_units
    , null as source_reference_range_low
    , null as source_reference_range_high
    , null as normalized_reference_range_low
    , null as normalized_reference_range_high
from {{ source('elation','misc_orders') }}
where deletion_time is null
    or deleted_by_user_id is null