select 
    allergy.id as allergy_id
    , null as encounter_id
    , allergy.patient_id
    , date(allergy.start_date) as onset_date
    , rx.cui as source_code
    , allergy.name as source_name
    , allergy.status
    , null as normalized_code
    , null as normalized_name
    , allergy.allergen_type
    , allergy.reaction
    , allergy.last_modified as last_update
    , allergy.created_by_user_id as practitioner_id
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','patient_allergy')}} allergy
left join {{ source('elation','patient_allergy_rxnorm')}} rx
    on allergy.id = rx.allergy_id
where 1=1
and allergy.deletion_time is null