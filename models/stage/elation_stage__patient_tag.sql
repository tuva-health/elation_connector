
select
    patient_id
    , tag_value
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','patient_tag') }}
where deletion_time is null