select
    immun.id as immunization_id
    , null as encounter_id
    , patient_id
    , cvx
    , name
    , admin_id.npi as administering_physician_npi
    , order_id.npi ordering_physician_mpi
    , administered_date
    , qty as quantity
    , qty_units as units
    , lot_number
    , manufacturer_name
    , vis
    , method
    , site
    , notes
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','patient_immunization') }} immun
left join {{ source('elation','user') }} admin_id
    on admin_id.physician_id = immun.administering_physician_id
left join {{ source('elation','user') }} order_id
    on order_id.physician_id = immun.ordering_physician_id
where deletion_time is null