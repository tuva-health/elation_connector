select
    id as condition_id
    , patient_id
    , null as encounter_id
    , null as claim_id
    , creation_time as recorded_date
    , null as onset_date
    , null as resolved_date
    , null as status
    , null as note
    , type as condition_type
    , value as code
    , null as description
    , rank
    , null as present_on_admit_code
    , null as present_on_admit_description
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','patient_history')}}
where deletion_time is null