select
    id as allergy_id
    , null as encounter_id
    , patient_id
    , date(start_date) as onset_date
    , rxnorm.cui as source_code
    , name as source_name
    , status
    , null as normalized_code
    , null as normalized_name
    , allergen_type
    , reaction
    , last_modified as last_update
    , created_by_user_id as practitioner_id
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','patient_drug_intolerance')}} drug
left join {{ source('elation','patient_drug_intolerance_rxnorm')}} rxnorm
    on drug.id = rxnorm.intolerance_id
where deletion_time is null