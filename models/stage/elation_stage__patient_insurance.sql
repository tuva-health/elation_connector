select
    patient_id
    , carrier
    , rank as insurance_rank
    , last_modified
from {{ source('elation','patient_insurance') }}
where is_deleted = false