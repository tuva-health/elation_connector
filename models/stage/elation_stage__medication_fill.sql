with rxnorm as(
    select distinct
        medication_id
        , cui as rxnorm_code
        , row_number() over (partition by medication_id order by medication_id) as duplicate_row_number
    from {{ source('elation','medication_rxnorm') }}

)

select 
    {{sha_hash_512('fill.patient_id||fill.id||rxnorm.rxnorm_code') }} as medication_id
    , fill.id as source_id
    , fill.patient_id
    , null as encounter_id
    , last_fill_date as dispensing_date
    , written_date as prescribing_date
    , 'rxnorm' as source_code_type
    , rxnorm.rxnorm_code as source_code
    , medication_name as source_description
    , null as ndc_code
    , null as ndc_description
    , null as rxnorm_code
    , null as rxnorm_description
    , null as atc_code
    , null as atc_description
    , medication_route as route
    , medication_strength as strength
    , fill.medication_id as code
    , quantity
    , quantity_unit
    , days_supply
    , users.id as practitioner_id
    , null as charge_amount
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','med_order_fill') }} fill
inner join {{ source('elation','med_order') }} med_order
    on med_order.id = fill.med_order_id
left join rxnorm rxnorm
    on fill.medication_id = rxnorm.medication_id
left join {{ source('elation','user') }} users
    on med_order.prescribing_user_id = users.id
where is_deleted = FALSE
and rxnorm.duplicate_row_number = 1