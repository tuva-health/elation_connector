select
    users.id as practitioner_id
    , prov.npi
    , first_name
    , last_name
    , user_type
    , prac.name as practice_affiliation
    , users.specialty
    , null as sub_specialty
/** columns for daily huddle  **/
    , credentials
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','user')}} users
left join {{ source('elation','practice')}} prac
    on users.practice_id = prac.id
left join {{ ref('terminology__provider')}} prov
    on users.npi = prov.npi
