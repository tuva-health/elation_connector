select
    app.id as appointment_id
    , app.patient_id as patient_id
    , app.appt_time as appointment_datetime
    , app.appt_type as source_appointment_type
    , null as normalized_appointment_type
    , app.duration as duration
    , loc.name as location
    , app.physician_user_id as practitioner_id
    , app.description
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','appointment') }} app
left join {{ source('elation','user') }} users
    on app.physician_user_id = users.id
left join {{ source('elation','service_location') }} loc
    on app.service_location_id = loc.id
where app.deletion_time is null
and patient_id is not null